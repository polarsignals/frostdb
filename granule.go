package columnstore

import (
	"fmt"
	"io"
	"sync/atomic"
	"unsafe"

	"github.com/apache/arrow/go/v7/arrow"
	"github.com/apache/arrow/go/v7/arrow/memory"
	"github.com/google/btree"
	"github.com/parca-dev/parca/pkg/columnstore/dynparquet"
	"github.com/parca-dev/parca/pkg/columnstore/pqarrow"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/parquet-go"
)

type Granule struct {

	// least is the row that exists within the Granule that is the least.
	// This is used for quick insertion into the btree, without requiring an iterator
	least *dynparquet.DynamicRow
	parts *PartList

	// card is the raw commited, and uncommited cardinality of the granule. It is used as a suggestion for potential compaction
	card uint64

	tableConfig *TableConfig

	granulesCreated prometheus.Counter

	// pruned indicates if this Granule is longer found in the index
	pruned uint64

	// newGranules are the granules that were created after a split
	newGranules []*Granule
}

func NewGranule(granulesCreated prometheus.Counter, tableConfig *TableConfig, firstPart *Part) *Granule {
	g := &Granule{
		granulesCreated: granulesCreated,
		parts:           &PartList{},
		tableConfig:     tableConfig,
	}

	// Find the least column
	if firstPart != nil {
		g.card = uint64(firstPart.Buf.NumRows())
		g.parts.Prepend(firstPart)
		// Since we assume a part is sorted, we need only to look at the first row in each Part
		row, err := firstPart.Buf.DynamicRows().ReadRow(nil)
		if err != nil {
			// TODO(brancz): handle error
			panic(err)
		}
		g.least = row
	}

	granulesCreated.Inc()
	return g
}

// AddPart returns the new cardinality of the Granule
func (g *Granule) AddPart(p *Part) uint64 {
	node := g.parts.Prepend(p)
	newcard := atomic.AddUint64(&g.card, uint64(p.Buf.NumRows()))
	r, err := p.Buf.DynamicRows().ReadRow(nil)
	if err != nil {
		// TODO(brancz): handle error
		panic(err)
	}

	for {
		least := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&g.least)))
		if least == nil || g.tableConfig.schema.RowLessThan(r, (*dynparquet.DynamicRow)(least)) {
			if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&g.least)), least, unsafe.Pointer(r)) {
				break
			}
		} else {
			break
		}
	}

	// If the prepend returned that we're adding to the compacted list; then we need to propogate the Part to the new granules
	if node.sentinel == Compacted {
		addPartToGranule(g.newGranules, p)
	}

	return newcard
}

// split a granule into n sized granules. With the last granule containing the remainder.
// Returns the granules in order.
// This assumes the Granule has had it's parts merged into a single part
func (g *Granule) split(tx uint64, n int) ([]*Granule, error) {
	// Get the first part in the granule's part list.
	var p *Part
	g.parts.Iterate(func(part *Part) bool {
		// Since all parts are already merged into one, this iterator will only
		// iterate over the one and only part.
		p = part
		return false
	})
	// How many granules we'll need to build
	count := int(p.Buf.NumRows()) / n

	// Build all the new granules
	granules := make([]*Granule, 0, count)

	// TODO: Buffers should be able to efficiently slice themselves.
	var row parquet.Row
	buf, err := g.tableConfig.schema.NewBuffer(p.Buf.DynamicColumns())
	if err != nil {
		return nil, err
	}
	rows := p.Buf.Rows()
	for {
		row, err := rows.ReadRow(row)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		err = buf.WriteRow(row)
		if err != nil {
			return nil, err
		}

		if int(buf.NumRows()) == n && len(granules) != count-1 { // If we have n rows, and aren't on the last granule, create the n-sized granule
			granules = append(granules, NewGranule(g.granulesCreated, g.tableConfig, NewPart(tx, buf)))
			buf, err = g.tableConfig.schema.NewBuffer(p.Buf.DynamicColumns())
			if err != nil {
				return nil, err
			}
		}
	}

	if buf.NumRows() > 0 {
		// Save the remaining Granule
		granules = append(granules, NewGranule(g.granulesCreated, g.tableConfig, NewPart(tx, buf)))
	}

	return granules, nil
}

// ArrowRecord merges all parts in a Granule before returning an ArrowRecord over that part
func (g *Granule) ArrowRecord(tx uint64, txCompleted func(uint64) uint64, pool memory.Allocator) (arrow.Record, error) {
	bufs := []dynparquet.DynamicRowGroup{}

	// Convert all the parts into a set of rows
	g.parts.Iterate(func(p *Part) bool {
		// Don't merge parts from an newer tx, or from an uncompleted tx, or a completed tx that finished after this tx started
		if p.tx > tx || txCompleted(p.tx) > tx {
			return true
		}

		bufs = append(bufs, p.Buf)
		return true
	})

	if len(bufs) == 0 {
		// Nothing readable for this transaction in this Granule.
		return nil, nil
	}

	merge, err := g.tableConfig.schema.MergeDynamicRowGroups(bufs)
	if err != nil {
		return nil, fmt.Errorf("merge part row groups: %w", err)
	}

	record, err := pqarrow.ParquetRowGroupToArrowRecord(pool, merge)
	if err != nil {
		return nil, fmt.Errorf("convert merged row groups to arrow: %w", err)
	}

	return record, nil
}

// Less implements the btree.Item interface
func (g *Granule) Less(than btree.Item) bool {
	return g.tableConfig.schema.RowLessThan(g.Least(), than.(*Granule).Least())
}

// Least returns the least row in a Granule
func (g *Granule) Least() *dynparquet.DynamicRow {
	return (*dynparquet.DynamicRow)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&g.least))))
}
