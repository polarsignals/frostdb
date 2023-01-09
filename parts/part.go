package parts

import (
	"bytes"
	"fmt"
	"io"
	"math"

	"github.com/apache/arrow/go/v7/arrow"
	"github.com/segmentio/parquet-go"

	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/pqarrow"
)

type CompactionLevel uint8

const (
	// CompactionLevel0 is the default compaction level for new Parts. This
	// means that the Part contains multiple variable-length row groups.
	CompactionLevel0 CompactionLevel = iota
	// CompactionLevel1 is the compaction level for Parts that are the result of
	// a compaction. Parts with this compaction level contain multiple row
	// groups with the row group size specified on table creation.
	CompactionLevel1
)

type Part struct {
	buf    *dynparquet.SerializedBuffer
	schema *dynparquet.Schema

	// tx is the id of the transaction that created this part.
	tx uint64

	compactionLevel CompactionLevel

	minRow *dynparquet.DynamicRow
	maxRow *dynparquet.DynamicRow
}

func (p *Part) SerializeBuffer(schema *dynparquet.Schema, w *parquet.Writer) error {
	if p.record == nil {
		return fmt.Errorf("not a record part")
	}

	return pqarrow.RecordToFile(schema, w, p.record)
}

type SchemaWriter interface {
	GetWriter(io.Writer, map[string][]string) (*dynparquet.PooledWriter, error)
	PutWriter(io.Writer, map[string][]string) (*dynparquet.PooledWriter, error)
	Schema() *dynparquet.Schema
}

func (p *Part) AsSerializedBuffer(schema *dynparquet.Schema) (*dynparquet.SerializedBuffer, error) {
	if p.buf != nil {
		return p.buf, nil
	}

	// If this is a Arrow record part, convert the record into a serialized buffer
	b := &bytes.Buffer{}

	w, err := schema.GetWriter(b, pqarrow.RecordDynamicCols(p.Record()))
	if err != nil {
		return nil, err
	}
	defer schema.PutWriter(w)
	if err := p.SerializeBuffer(schema, w.ParquetWriter()); err != nil {
		return nil, err
	}

	f, err := parquet.OpenFile(bytes.NewReader(b.Bytes()), int64(b.Len()))
	if err != nil {
		return nil, err
	}

	buf, err := dynparquet.NewSerializedBuffer(f)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

type Option func(*Part)

func WithCompactionLevel(level CompactionLevel) Option {
	return func(p *Part) {
		p.compactionLevel = level
	}
}

// NewArrowPart returns a new Arrow part.
func NewArrowPart(tx uint64, record arrow.Record, schema *dynparquet.Schema, options ...Option) *Part {
	p := &Part{
		tx:     tx,
		record: record,
		schema: schema,
	}

	for _, opt := range options {
		opt(p)
	}

	return p
}

func NewPart(tx uint64, buf *dynparquet.SerializedBuffer, options ...Option) *Part {
	p := &Part{
		tx:  tx,
		buf: buf,
	}

	for _, opt := range options {
		opt(p)
	}

	return p
}

func (p *Part) NumRows() int64 {
	return p.buf.NumRows()
}

func (p *Part) Size() int64 {
	return p.buf.ParquetFile().Size()
}

func (p *Part) CompactionLevel() CompactionLevel {
	return p.compactionLevel
}

// TX returns the transaction id for the part.
func (p *Part) TX() uint64 { return p.tx }

// Least returns the least row  in the part.
func (p *Part) Least() (*dynparquet.DynamicRow, error) {
	if p.minRow != nil {
		return p.minRow, nil
	}

	rowBuf := &dynparquet.DynamicRows{Rows: make([]parquet.Row, 1)}
	reader := p.buf.DynamicRowGroup(0).DynamicRows()
	defer reader.Close()

	if n, err := reader.ReadRows(rowBuf); err != nil {
		return nil, fmt.Errorf("read first row of part: %w", err)
	} else if n != 1 {
		return nil, fmt.Errorf("expected to read exactly 1 row, but read %d", n)
	}

	// Copy here so that this reference does not prevent the decompressed page
	// from being GCed.
	p.minRow = rowBuf.GetCopy(0)
	return p.minRow, nil
}

func (p *Part) most() (*dynparquet.DynamicRow, error) {
	if p.maxRow != nil {
		return p.maxRow, nil
	}

	rowBuf := &dynparquet.DynamicRows{Rows: make([]parquet.Row, 1)}
	rg := p.buf.DynamicRowGroup(p.buf.NumRowGroups() - 1)
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
	p.maxRow = rowBuf.GetCopy(0)
	return p.maxRow, nil
}

func (p Part) OverlapsWith(schema *dynparquet.Schema, otherPart *Part) (bool, error) {
	a, err := p.Least()
	if err != nil {
		return false, err
	}
	b, err := p.most()
	if err != nil {
		return false, err
	}
	c, err := otherPart.Least()
	if err != nil {
		return false, err
	}
	d, err := otherPart.most()
	if err != nil {
		return false, err
	}

	return schema.Cmp(a, d) <= 0 && schema.Cmp(c, b) <= 0, nil
}

// Tombstone marks all the parts with the max tx id to ensure they aren't
// included in reads. Tombstoned parts will be eventually be dropped from the
// database during compaction.
func Tombstone(parts []*Part) {
	for _, part := range parts {
		part.tx = math.MaxUint64
	}
}

func (p Part) HasTombstone() bool {
	return p.tx == math.MaxUint64
}

type PartSorter struct {
	schema *dynparquet.Schema
	parts  []*Part
	err    error
}

func NewPartSorter(schema *dynparquet.Schema, parts []*Part) *PartSorter {
	return &PartSorter{
		schema: schema,
		parts:  parts,
	}
}

func (p PartSorter) Len() int {
	return len(p.parts)
}

func (p *PartSorter) Less(i, j int) bool {
	a, err := p.parts[i].Least()
	if err != nil {
		p.err = err
		return false
	}
	b, err := p.parts[j].Least()
	if err != nil {
		p.err = err
		return false
	}
	return p.schema.RowLessThan(a, b)
}

func (p *PartSorter) Swap(i, j int) {
	p.parts[i], p.parts[j] = p.parts[j], p.parts[i]
}

func (p *PartSorter) Err() error {
	return p.err
}
