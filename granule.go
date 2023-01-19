package frostdb

import (
	"sync/atomic"

	"github.com/google/btree"
	"github.com/prometheus/client_golang/prometheus"

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
	least *dynparquet.DynamicRow

	// size is the raw commited, and uncommited size of the granule. It is used as a suggestion for potential compaction
	size atomic.Uint64

	// pruned indicates if this Granule is longer found in the index
	pruned atomic.Uint64
}

func NewGranule(granulesCreated prometheus.Counter, tableConfig *TableConfig, prts ...*parts.Part) (*Granule, error) {
	g := &Granule{
		granulesCreated: granulesCreated,
		parts:           parts.NewList(&atomic.Pointer[parts.Node]{}, parts.None),
		tableConfig:     tableConfig,
		metadata:        GranuleMetadata{},
	}

	for _, p := range prts {
		if err := g.addPart(p); err != nil {
			return nil, err
		}
	}

	granulesCreated.Inc()
	return g, nil
}

func (g *Granule) addPart(p *parts.Part) error {
	if p.NumRows() == 0 {
		return nil
	}

	r, err := p.Least()
	if err != nil {
		return err
	}

	_ = g.parts.Prepend(p)
	g.metadata.size.Add(uint64(p.Size()))

	if g.metadata.least == nil || g.tableConfig.schema.RowLessThan(r, g.metadata.least) {
		g.metadata.least = r
	}

	return nil
}

// Append adds a part into the Granule. It returns the new size of the Granule.
func (g *Granule) Append(p *parts.Part) (uint64, error) {
	node := g.parts.Prepend(p)
	newSize := g.metadata.size.Add(uint64(p.Size()))

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

// PartBuffersForTx returns the PartBuffers for the given transaction constraints.
func (g *Granule) PartBuffersForTx(watermark uint64, iterator func(*dynparquet.SerializedBuffer) bool) {
	g.parts.Iterate(func(p *parts.Part) bool {
		// Don't iterate over parts from an uncompleted transaction
		if p.TX() > watermark {
			return true
		}

		return iterator(p.Buf())
	})
}

// Less implements the btree.Item interface.
func (g *Granule) Less(than btree.Item) bool {
	return g.tableConfig.schema.RowLessThan(g.Least(), than.(*Granule).Least())
}

// Least returns the least row in a Granule.
func (g *Granule) Least() *dynparquet.DynamicRow {
	return g.metadata.least
}
