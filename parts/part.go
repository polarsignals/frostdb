package parts

import (
	"bytes"
	"fmt"
	"math"
	"sort"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/parquet-go/parquet-go"

	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/pqarrow"
)

type Part struct {
	buf    *dynparquet.SerializedBuffer
	record arrow.Record
	// record relative size is how many bytes are roughly attributed to this record.
	// It's considered rough because it may be sharing underlying dictionaries with other records.
	recordRelativeSize int
	schema             *dynparquet.Schema

	// tx is the id of the transaction that created this part.
	tx uint64

	compactionLevel int

	minRow *dynparquet.DynamicRow
	maxRow *dynparquet.DynamicRow
}

func (p *Part) Record() arrow.Record {
	return p.record
}

func (p *Part) SerializeBuffer(schema *dynparquet.Schema, w dynparquet.ParquetWriter) error {
	if p.record == nil {
		return fmt.Errorf("not a record part")
	}

	return pqarrow.RecordToFile(schema, w, p.record)
}

func (p *Part) AsSerializedBuffer(schema *dynparquet.Schema) (*dynparquet.SerializedBuffer, error) {
	if p.buf != nil {
		return p.buf, nil
	}

	// If this is a Arrow record part, convert the record into a serialized buffer
	b := &bytes.Buffer{}

	w, err := schema.GetWriter(b, pqarrow.RecordDynamicCols(p.record))
	if err != nil {
		return nil, err
	}
	defer schema.PutWriter(w)
	if err := p.SerializeBuffer(schema, w.ParquetWriter); err != nil {
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

func WithCompactionLevel(level int) Option {
	return func(p *Part) {
		p.compactionLevel = level
	}
}

// NewArrowPart returns a new Arrow part.
func NewArrowPart(tx uint64, record arrow.Record, size int, schema *dynparquet.Schema, options ...Option) *Part {
	p := &Part{
		tx:                 tx,
		record:             record,
		recordRelativeSize: size,
		schema:             schema,
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
	if p.buf != nil {
		return p.buf.NumRows()
	}

	return p.record.NumRows()
}

func (p *Part) Size() int64 {
	if p.buf != nil {
		return p.buf.ParquetFile().Size()
	}

	return int64(p.recordRelativeSize)
}

func (p *Part) CompactionLevel() int {
	return p.compactionLevel
}

// TX returns the transaction id for the part.
func (p *Part) TX() uint64 { return p.tx }

// Least returns the least row  in the part.
func (p *Part) Least() (*dynparquet.DynamicRow, error) {
	if p.minRow != nil {
		return p.minRow, nil
	}

	if p.record != nil {
		dynCols := pqarrow.RecordDynamicCols(p.record)
		pooledSchema, err := p.schema.GetDynamicParquetSchema(dynCols)
		if err != nil {
			return nil, err
		}
		defer p.schema.PutPooledParquetSchema(pooledSchema)
		p.minRow, err = pqarrow.RecordToDynamicRow(p.schema, pooledSchema.Schema, p.record, dynCols, 0)
		if err != nil {
			return nil, err
		}

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

	if p.record != nil {
		dynCols := pqarrow.RecordDynamicCols(p.record)
		pooledSchema, err := p.schema.GetDynamicParquetSchema(dynCols)
		if err != nil {
			return nil, err
		}
		defer p.schema.PutPooledParquetSchema(pooledSchema)
		p.maxRow, err = pqarrow.RecordToDynamicRow(p.schema, pooledSchema.Schema, p.record, dynCols, int(p.record.NumRows()-1))
		if err != nil {
			return nil, err
		}

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

func (p *PartSorter) Len() int {
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

// FindMaximumNonOverlappingSet removes the minimum number of parts from the
// given slice in order to return the maximum non-overlapping set of parts.
// The function returns the non-overlapping parts first and any overlapping
// parts second. The parts returned are in sorted order according to their Least
// row.
func FindMaximumNonOverlappingSet(schema *dynparquet.Schema, parts []*Part) ([]*Part, []*Part, error) {
	if len(parts) < 2 {
		return parts, nil, nil
	}
	sorter := NewPartSorter(schema, parts)
	sort.Sort(sorter)
	if sorter.Err() != nil {
		return nil, nil, sorter.Err()
	}

	// Parts are now sorted according to their Least row.
	prev := 0
	prevEnd, err := parts[0].most()
	if err != nil {
		return nil, nil, err
	}
	nonOverlapping := make([]*Part, 0, len(parts))
	overlapping := make([]*Part, 0, len(parts))
	for i := 1; i < len(parts); i++ {
		start, err := parts[i].Least()
		if err != nil {
			return nil, nil, err
		}
		curEnd, err := parts[i].most()
		if err != nil {
			return nil, nil, err
		}
		if schema.Cmp(prevEnd, start) < 0 {
			// No overlap, append the previous part and update end for the next
			// iteration.
			nonOverlapping = append(nonOverlapping, parts[prev])
			prevEnd = curEnd
			prev = i
			continue
		}

		// This part overlaps with the previous part. Remove the part with
		// the highest end row.
		if schema.Cmp(prevEnd, curEnd) >= 0 {
			overlapping = append(overlapping, parts[prev])
			prevEnd = curEnd
			prev = i
		} else {
			// The current part must be removed. Don't update prevEnd or prev,
			// this will be used in the next iteration and must stay the same.
			overlapping = append(overlapping, parts[i])
		}
	}
	if len(overlapping) == 0 || overlapping[len(overlapping)-1] != parts[len(parts)-1] {
		// The last part either did not overlap with its previous part, or
		// overlapped but had a smaller end row than its previous part (so the
		// previous part is in the overlapping slice). The last part must be
		// appended to nonOverlapping.
		nonOverlapping = append(nonOverlapping, parts[len(parts)-1])
	}
	return nonOverlapping, overlapping, nil
}
