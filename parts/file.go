package parts

import (
	"fmt"
	"io"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/parquet-go/parquet-go"

	"github.com/polarsignals/frostdb/dynparquet"
)

// fileParquetPart implements the Part interface. Is is backed by a Parquet file.
type fileParquetPart struct {
	basePart

	rdr     io.ReaderAt
	size    int64
	release func()
}

func NewFileParquetPart(tx uint64, rdr io.ReaderAt, size int64, release func(), options ...Option) Part {
	p := &fileParquetPart{
		basePart: basePart{
			tx: tx,
		},
		rdr:     rdr,
		size:    size,
		release: release,
	}

	for _, option := range options {
		option(&p.basePart)
	}

	return p
}

func (p *fileParquetPart) NumRows() int64 {
	file, err := p.initFile()
	if err != nil {
		panic("error initializing file")
	}
	return file.NumRows()
}

func (p *fileParquetPart) Record() arrow.Record { return nil }

func (p *fileParquetPart) Release() {
	p.release()
}

func (p *fileParquetPart) SerializeBuffer(_ *dynparquet.Schema, _ dynparquet.ParquetWriter) error {
	return fmt.Errorf("not a record part")
}

func (p *fileParquetPart) AsSerializedBuffer(_ *dynparquet.Schema) (*dynparquet.SerializedBuffer, error) {
	file, err := p.initFile()
	if err != nil {
		return nil, err
	}
	return dynparquet.NewSerializedBuffer(file)
}

func (p *fileParquetPart) Size() int64 { return p.size }

func (p *fileParquetPart) Least() (*dynparquet.DynamicRow, error) {
	if p.minRow != nil {
		return p.minRow, nil
	}

	file, err := p.initFile()
	if err != nil {
		return nil, err
	}

	buf, err := dynparquet.NewSerializedBuffer(file)
	if err != nil {
		return nil, err
	}

	minRow, err := min(buf)
	if err != nil {
		return nil, err
	}

	p.minRow = minRow
	return p.minRow, nil
}

func (p *fileParquetPart) Most() (*dynparquet.DynamicRow, error) {
	if p.maxRow != nil {
		return p.maxRow, nil
	}

	file, err := p.initFile()
	if err != nil {
		return nil, err
	}

	buf, err := dynparquet.NewSerializedBuffer(file)
	if err != nil {
		return nil, err
	}

	maxRow, err := max(buf)
	if err != nil {
		return nil, err
	}

	p.maxRow = maxRow
	return p.maxRow, nil
}

func (p *fileParquetPart) OverlapsWith(schema *dynparquet.Schema, otherPart Part) (bool, error) {
	a, err := p.Least()
	if err != nil {
		return false, err
	}
	b, err := p.Most()
	if err != nil {
		return false, err
	}
	c, err := otherPart.Least()
	if err != nil {
		return false, err
	}
	d, err := otherPart.Most()
	if err != nil {
		return false, err
	}

	return schema.Cmp(a, d) <= 0 && schema.Cmp(c, b) <= 0, nil
}

func (p *fileParquetPart) initFile() (*parquet.File, error) {
	file, err := parquet.OpenFile(p.rdr, p.size) // TODO options?
	if err != nil {
		return nil, fmt.Errorf("failed to open Parquet file: %w", err)
	}

	return file, nil
}
