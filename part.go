package frostdb

import (
	"fmt"
	"math"

	"github.com/segmentio/parquet-go"

	"github.com/polarsignals/frostdb/dynparquet"
)

type compactionLevel uint8

const (
	// compactionLevel0 is the default compaction level for new Parts. This
	// means that the Part contains multiple variable-length row groups.
	compactionLevel0 compactionLevel = iota
	// compactionLevel1 is the compaction level for Parts that are the result of
	// a compaction. Parts with this compaction level contain multiple row
	// groups with the row group size specified on table creation.
	compactionLevel1
)

type Part struct {
	Buf *dynparquet.SerializedBuffer

	// tx is the id of the transaction that created this part.
	tx uint64

	compactionLevel compactionLevel

	minRow *dynparquet.DynamicRow
	maxRow *dynparquet.DynamicRow
}

func NewPart(tx uint64, buf *dynparquet.SerializedBuffer) *Part {
	return &Part{
		tx:  tx,
		Buf: buf,
	}
}

// Least returns the least row  in the part.
func (p *Part) Least() (*dynparquet.DynamicRow, error) {
	if p.minRow != nil {
		return p.minRow, nil
	}

	rowBuf := &dynparquet.DynamicRows{Rows: make([]parquet.Row, 1)}
	reader := p.Buf.DynamicRowGroup(0).DynamicRows()
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
	rg := p.Buf.DynamicRowGroup(p.Buf.NumRowGroups() - 1)
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

func (p Part) overlapsWith(schema *dynparquet.Schema, otherPart *Part) (bool, error) {
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

// tombstone marks all the parts with the max tx id to ensure they aren't
// included in reads. Tombstoned parts will be eventually be dropped from the
// database during compaction.
func tombstone(parts []*Part) {
	for _, part := range parts {
		part.tx = math.MaxUint64
	}
}

func (p Part) hasTombstone() bool {
	return p.tx == math.MaxUint64
}

type partSorter struct {
	schema *dynparquet.Schema
	parts  []*Part
	err    error
}

func (p partSorter) Len() int {
	return len(p.parts)
}

func (p *partSorter) Less(i, j int) bool {
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

func (p *partSorter) Swap(i, j int) {
	p.parts[i], p.parts[j] = p.parts[j], p.parts[i]
}
