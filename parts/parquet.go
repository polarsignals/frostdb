package parts

import (
	"fmt"
	"io"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/parquet-go/parquet-go"

	"github.com/polarsignals/frostdb/dynparquet"
)

// This file contains the implementation of the Part interface backed by a Parquet Buffer.
type parquetPart struct {
	basePart

	ref *atomic.Int32
	buf *dynparquet.SerializedBuffer
}

func (p *parquetPart) Record() arrow.Record {
	return nil
}

func (p *parquetPart) Retain() { p.ref.Add(1) }

func (p *parquetPart) Release() {
	ref := p.ref.Add(-1)
	if ref <= 0 && p.release != nil {
		p.release()
	}
}

func (p *parquetPart) Write(w io.Writer) error {
	buf, err := p.AsSerializedBuffer(nil)
	if err != nil {
		return err
	}

	f := buf.ParquetFile()
	_, err = io.Copy(w, io.NewSectionReader(f, 0, f.Size()))
	return err
}

func (p *parquetPart) SerializeBuffer(_ *dynparquet.Schema, _ dynparquet.ParquetWriter) error {
	return fmt.Errorf("not a record part")
}

func (p *parquetPart) AsSerializedBuffer(_ *dynparquet.Schema) (*dynparquet.SerializedBuffer, error) {
	return p.buf, nil
}

func NewParquetPart(tx uint64, buf *dynparquet.SerializedBuffer, options ...Option) Part {
	p := &parquetPart{
		basePart: basePart{
			tx: tx,
		},
		ref: &atomic.Int32{},
		buf: buf,
	}

	for _, opt := range options {
		opt(&p.basePart)
	}

	p.ref.Add(1)
	return p
}

func (p *parquetPart) NumRows() int64 {
	return p.buf.NumRows()
}

func (p *parquetPart) Size() int64 {
	return p.buf.ParquetFile().Size()
}

// Least returns the least row  in the part.
func (p *parquetPart) Least() (*dynparquet.DynamicRow, error) {
	if p.minRow != nil {
		return p.minRow, nil
	}

	minRow, err := minRow(p.buf)
	if err != nil {
		return nil, err
	}

	p.minRow = minRow
	return p.minRow, nil
}

func (p *parquetPart) Most() (*dynparquet.DynamicRow, error) {
	if p.maxRow != nil {
		return p.maxRow, nil
	}

	maxRow, err := maxRow(p.buf)
	if err != nil {
		return nil, err
	}
	p.maxRow = maxRow
	return p.maxRow, nil
}

func (p *parquetPart) OverlapsWith(schema *dynparquet.Schema, otherPart Part) (bool, error) {
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

func maxRow(buf *dynparquet.SerializedBuffer) (*dynparquet.DynamicRow, error) {
	rowBuf := &dynparquet.DynamicRows{Rows: make([]parquet.Row, 1)}
	rg := buf.DynamicRowGroup(buf.NumRowGroups() - 1)
	reader := rg.DynamicRows()
	defer reader.Close()

	if err := reader.SeekToRow(rg.NumRows() - 1); err != nil {
		return nil, fmt.Errorf("seek to last row of part: %w", err)
	}

	if n, err := reader.ReadRows(rowBuf); err != nil {
		return nil, fmt.Errorf("read last row of part: %w", err)
	} else if n != 1 {
		return nil, fmt.Errorf("expected to read exactly 1 row, but read %d", n)
	}

	// Copy here so that this reference does not prevent the decompressed page
	// from being GCed.
	return rowBuf.GetCopy(0), nil
}

func minRow(buf *dynparquet.SerializedBuffer) (*dynparquet.DynamicRow, error) {
	rowBuf := &dynparquet.DynamicRows{Rows: make([]parquet.Row, 1)}
	reader := buf.DynamicRowGroup(0).DynamicRows()
	defer reader.Close()

	if n, err := reader.ReadRows(rowBuf); err != nil {
		return nil, fmt.Errorf("read first row of part: %w", err)
	} else if n != 1 {
		return nil, fmt.Errorf("expected to read exactly 1 row, but read %d", n)
	}

	// Copy here so that this reference does not prevent the decompressed page
	// from being GCed.
	return rowBuf.GetCopy(0), nil
}
