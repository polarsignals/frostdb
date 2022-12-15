package frostdb

import (
	"fmt"
	"sync/atomic"

	"github.com/google/btree"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/parquet-go"

	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/parts"
)

type Granule struct {
	metadata GranuleMetadata

	parts       *parts.List
	tableConfig *TableConfig

	granulesCreated prometheus.Counter

	// newGranules are the granules that were created after a split
	newGranules []*Granule
}

// GranuleMetadata is the metadata for a granule.
type GranuleMetadata struct {
	// least is the row that exists within the Granule that is the least.
	// This is used for quick insertion into the btree, without requiring an
	// iterator.
	least atomic.Pointer[dynparquet.DynamicRow]

	// size is the raw commited, and uncommited size of the granule. It is used as a suggestion for potential compaction
	size atomic.Uint64

	// pruned indicates if this Granule is longer found in the index
	pruned atomic.Uint64
}

func NewGranule(granulesCreated prometheus.Counter, tableConfig *TableConfig, firstPart *parts.Part) (*Granule, error) {
	g := &Granule{
		granulesCreated: granulesCreated,
		parts:           parts.NewList(&atomic.Pointer[parts.Node]{}, parts.None),
		tableConfig:     tableConfig,

		metadata: GranuleMetadata{
			least: atomic.Pointer[dynparquet.DynamicRow]{},
		},
	}

	// Find the "smallest" row
	if firstPart != nil {
		g.metadata.size.Store(uint64(firstPart.Buf.ParquetFile().Size()))
		g.parts.Prepend(firstPart)
		least, err := firstPart.Least()
		if err != nil {
			return nil, err
		}
		g.metadata.least.Store(least)
	}

	granulesCreated.Inc()
	return g, nil
}

func (g *Granule) addPart(p *parts.Part, r *dynparquet.DynamicRow) (uint64, error) {
	rows := p.Buf.NumRows()
	if rows == 0 {
		return g.metadata.size.Load(), nil
	}
	node := g.parts.Prepend(p)

	newSize := g.metadata.size.Add(uint64(p.Buf.ParquetFile().Size()))

	for {
		least := g.metadata.least.Load()
		if least == nil || g.tableConfig.schema.RowLessThan(r, (*dynparquet.DynamicRow)(least)) {
			if g.metadata.least.CompareAndSwap(least, r) {
				break
			}
		} else {
			break
		}
	}

	// If the prepend returned that we're adding to the compacted list; then we
	// need to propagate the Part to the new granules.
	if node.Compacted() {
		err := addPartToGranule(g.newGranules, p)
		if err != nil {
			return 0, err
		}
	}

	return newSize, nil
}

// AddPart returns the new size of the Granule.
func (g *Granule) AddPart(p *parts.Part) (uint64, error) {
	rowBuf := &dynparquet.DynamicRows{Rows: make([]parquet.Row, 1)}
	reader := p.Buf.DynamicRowGroup(0).DynamicRows()
	n, err := reader.ReadRows(rowBuf)
	if err != nil {
		return 0, fmt.Errorf("read first row of part: %w", err)
	}
	if n != 1 {
		return 0, fmt.Errorf("expected to read exactly 1 row, but read %d", n)
	}
	r := rowBuf.GetCopy(0)
	if err := reader.Close(); err != nil {
		return 0, err
	}

	return g.addPart(p, r)
}

// PartBuffersForTx returns the PartBuffers for the given transaction constraints.
func (g *Granule) PartBuffersForTx(watermark uint64, iterator func(*dynparquet.SerializedBuffer) bool) {
	g.parts.Iterate(func(p *parts.Part) bool {
		// Don't iterate over parts from an uncompleted transaction
		if p.TX() > watermark {
			return true
		}

		return iterator(p.Buf)
	})
}

// Less implements the btree.Item interface.
func (g *Granule) Less(than btree.Item) bool {
	return g.tableConfig.schema.RowLessThan(g.Least(), than.(*Granule).Least())
}

// Least returns the least row in a Granule.
func (g *Granule) Least() *dynparquet.DynamicRow {
	return (*dynparquet.DynamicRow)(g.metadata.least.Load())
}
