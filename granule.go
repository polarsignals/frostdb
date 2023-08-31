package frostdb

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/google/btree"

	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/parts"
)

type Granule struct {
	metadata GranuleMetadata

	parts  *parts.List
	schema *dynparquet.Schema

	// newGranules are the granules that were created after a split
	newGranules *atomic.Pointer[[]*Granule]
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

func NewGranule(schema *dynparquet.Schema, prts ...*parts.Part) (*Granule, error) {
	g := &Granule{
		parts:       parts.NewList(&atomic.Pointer[parts.Node]{}, parts.None),
		schema:      schema,
		metadata:    GranuleMetadata{},
		newGranules: &atomic.Pointer[[]*Granule]{},
	}
	g.newGranules.Store(&[]*Granule{})

	for _, p := range prts {
		if err := g.addPart(p); err != nil {
			return nil, err
		}
	}

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

	if g.metadata.least == nil || g.schema.RowLessThan(r, g.metadata.least) {
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
		err := addPartToGranule(*g.newGranules.Load(), p)
		if err != nil {
			return 0, err
		}
	}

	return newSize, nil
}

// PartsForTx returns the parts for the given transaction constraints.
func (g *Granule) PartsForTx(watermark uint64, iterator func(*parts.Part) bool) {
	g.parts.Iterate(func(p *parts.Part) bool {
		// Don't iterate over parts from an uncompleted transaction
		if p.TX() > watermark {
			return true
		}

		return iterator(p)
	})
}

// Less implements the btree.Item interface.
func (g *Granule) Less(than btree.Item) bool {
	var otherRow *dynparquet.DynamicRow
	switch v := than.(type) {
	case *Granule:
		otherRow = v.Least()
	case btreeComparableDynamicRow:
		otherRow = v.DynamicRow
	default:
		panic(fmt.Sprintf("cannot compare against %T", v))
	}
	return g.schema.RowLessThan(g.Least(), otherRow)
}

// Least returns the least row in a Granule.
func (g *Granule) Least() *dynparquet.DynamicRow {
	return g.metadata.least
}

// Collect will filter row groups or arrow records into the collector. Arrow records passed to the collector must be Released().
func (g *Granule) Collect(ctx context.Context, tx uint64, filter TrueNegativeFilter, collector chan<- any) {
	records := []arrow.Record{}
	g.PartsForTx(tx, func(p *parts.Part) bool {
		if r := p.Record(); r != nil {
			r.Retain()
			records = append(records, r)
			return true
		}

		var buf *dynparquet.SerializedBuffer
		var err error
		buf, err = p.AsSerializedBuffer(g.schema)
		if err != nil {
			return false
		}
		f := buf.ParquetFile()
		for i := range f.RowGroups() {
			rg := buf.DynamicRowGroup(i)
			var mayContainUsefulData bool
			mayContainUsefulData, err = filter.Eval(rg)
			if err != nil {
				return false
			}
			if mayContainUsefulData {
				select {
				case <-ctx.Done():
					return false
				case collector <- rg:
				}
			}
		}

		return true
	})

	if len(*g.newGranules.Load()) != 0 && len(records) != 0 { // This granule was pruned while we were retaining Records; it's not safe to use them anymore
		for _, r := range records {
			r.Release()
		}
		for _, newGran := range *g.newGranules.Load() {
			newGran.Collect(ctx, tx, filter, collector)
		}
	} else {
		for _, r := range records {
			select {
			case <-ctx.Done():
				return
			case collector <- r:
			}
		}
	}
}
