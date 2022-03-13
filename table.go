package columnstore

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/apache/arrow/go/v7/arrow"
	"github.com/apache/arrow/go/v7/arrow/memory"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/google/btree"
	"github.com/parca-dev/parca/pkg/columnstore/dynparquet"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var ErrNoSchema = fmt.Errorf("no schema")

type TableConfig struct {
	schema      *dynparquet.Schema
	granuleSize int
}

func NewTableConfig(schema *dynparquet.Schema, granuleSize int) *TableConfig {
	return &TableConfig{
		schema:      schema,
		granuleSize: granuleSize,
	}
}

type Table struct {
	db      *DB
	metrics *tableMetrics
	logger  log.Logger

	config *TableConfig
	index  *btree.BTree

	sync.WaitGroup
	sync.Mutex
}

type tableMetrics struct {
	granulesCreated  prometheus.Counter
	granulesSplits   prometheus.Counter
	rowsInserted     prometheus.Counter
	zeroRowsInserted prometheus.Counter
	rowInsertSize    prometheus.Histogram
}

func newTable(
	db *DB,
	name string,
	tableConfig *TableConfig,
	reg prometheus.Registerer,
	logger log.Logger,
) *Table {
	reg = prometheus.WrapRegistererWith(prometheus.Labels{"table": name}, reg)

	t := &Table{
		db:     db,
		config: tableConfig,
		index:  btree.New(2), // TODO make the degree a setting
		logger: logger,
		metrics: &tableMetrics{
			granulesCreated: promauto.With(reg).NewCounter(prometheus.CounterOpts{
				Name: "granules_created",
				Help: "Number of granules created.",
			}),
			granulesSplits: promauto.With(reg).NewCounter(prometheus.CounterOpts{
				Name: "granules_splits",
				Help: "Number of granules splits executed.",
			}),
			rowsInserted: promauto.With(reg).NewCounter(prometheus.CounterOpts{
				Name: "rows_inserted",
				Help: "Number of rows inserted into table.",
			}),
			zeroRowsInserted: promauto.With(reg).NewCounter(prometheus.CounterOpts{
				Name: "zero_rows_inserted",
				Help: "Number of times it was attempted to insert zero rows into the table.",
			}),
			rowInsertSize: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
				Name:    "row_insert_size",
				Help:    "Size of batch inserts into table.",
				Buckets: prometheus.ExponentialBuckets(1, 2, 10),
			}),
		},
	}

	promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "index_size",
		Help: "Number of granules in the table index currently.",
	}, func() float64 {
		return float64(t.Index().Len())
	})

	g := NewGranule(t.metrics.granulesCreated, t.config, nil)
	t.index.ReplaceOrInsert(g)

	return t
}

// Sync the table. This will return once all split operations have completed.
// Currently it does not prevent new inserts from happening, so this is only
// safe to rely on if you control all writers. In the future we may need to add a way to
// block new writes as well.
func (t *Table) Sync() {
	t.Wait()
}

func (t *Table) Schema() *dynparquet.Schema {
	return t.config.schema
}

func (t *Table) Insert(buf *dynparquet.Buffer) error {
	defer func() {
		t.metrics.rowsInserted.Add(float64(buf.NumRows()))
		t.metrics.rowInsertSize.Observe(float64(buf.NumRows()))
	}()

	if buf.NumRows() == 0 {
		t.metrics.zeroRowsInserted.Add(float64(buf.NumRows()))
		return nil
	}

	tx, commit := t.db.begin()
	defer commit()

	rowsToInsertPerGranule := t.splitRowsByGranule(buf)
	for granule, buf := range rowsToInsertPerGranule {
		if granule.AddPart(NewPart(tx, buf)) >= uint64(t.config.granuleSize) {
			t.Add(1)
			go t.compact(granule)
		}
	}

	return nil
}

func (t *Table) splitGranule(granule *Granule) {

	// Recheck to ensure the granule still needs to be split
	if !atomic.CompareAndSwapUint64(&granule.pruned, 0, 1) {
		return
	}

	// Obtain a new tx for this compaction
	tx, commit := t.db.begin()

	// Start compaction by adding sentinel node to parts list
	parts := granule.parts.Sentinel(Compacting)

	bufs := []dynparquet.DynamicRowGroup{}
	remain := []*Part{}

	// Convert all the parts into a set of rows
	parts.Iterate(func(p *Part) bool {
		// Don't merge parts from an newer tx, or from an uncompleted tx, or a completed tx that finished after this tx started
		if p.tx > tx || t.db.txCompleted(p.tx) > tx {
			remain = append(remain, p)
			return true
		}

		bufs = append(bufs, p.Buf)
		return true
	})

	merge, err := t.config.schema.MergeDynamicRowGroups(bufs)
	if err != nil {
		panic(err)
	}

	buf, err := t.config.schema.NewBuffer(merge.DynamicColumns())
	if err != nil {
		panic(err)
	}

	_, err = buf.WriteRowGroup(merge)
	if err != nil {
		panic(err)
	}

	if buf.NumRows() < int64(t.config.granuleSize) { // It's possible to have a Granule marked for compaction but all the parts in it aren't completed tx's yet
		for {
			if atomic.CompareAndSwapUint64(&granule.pruned, 1, 0) { // unmark pruned, so that we can compact it in the future
				return
			}
		}
	}
	g := NewGranule(t.metrics.granulesCreated, t.config, NewPart(tx, buf))

	granules, err := g.split(tx, t.config.granuleSize/2) // TODO magic numbers
	if err != nil {
		level.Error(t.logger).Log("msg", "granule split failed after add part", "error", err)
	}

	// add remaining parts onto new granules
	for _, p := range remain {
		addPartToGranule(granules, p)
	}

	// set the newGranules pointer, so new writes will propogate into these new granules
	granule.newGranules = granules

	// Mark compaction complete in the granule; this will cause new writes to start using the newGranules pointer
	parts = granule.parts.Sentinel(Compacted)

	// Now we need to copy any new parts that happened while we were compacting
	parts.Iterate(func(p *Part) bool {
		addPartToGranule(granules, p)
		return true
	})

	// commit our compacted writes.
	// Do this here to avoid a small race condition where we swap the index, and before what was previously a defer commit() would allow a read
	// to not find the compacted parts
	commit()

	for {
		curIndex := t.Index()
		t.Lock()
		index := curIndex.Clone() // TODO(THOR): we can't clone concurrently
		t.Unlock()

		deleted := index.Delete(granule)
		if deleted == nil {
			level.Error(t.logger).Log("msg", "failed to delete granule during split")
		}

		for _, g := range granules {
			if dupe := index.ReplaceOrInsert(g); dupe != nil {
				level.Error(t.logger).Log("duplicate insert performed")
			}
		}

		// Point to the new index
		if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&t.index)), unsafe.Pointer(t.index), unsafe.Pointer(index)) {
			return
		}
	}
}

// Iterator iterates in order over all granules in the table. It stops iterating when the iterator function returns false.
func (t *Table) Iterator(pool memory.Allocator, iterator func(r arrow.Record) error) error {
	index := t.Index()
	tx := t.db.beginRead()

	var err error
	index.Ascend(func(i btree.Item) bool {
		g := i.(*Granule)

		var r arrow.Record
		r, err = g.ArrowRecord(tx, t.db.txCompleted, pool)
		if err != nil {
			return false
		}
		err = iterator(r)
		r.Release()
		return err == nil
	})
	return err
}

// Index provides atomic access to the table index
func (t *Table) Index() *btree.BTree {
	ptr := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&t.index)))
	return (*btree.BTree)(ptr)
}

func (t *Table) granuleIterator(iterator func(g *Granule) bool) {
	t.Index().Ascend(func(i btree.Item) bool {
		g := i.(*Granule)
		return iterator(g)
	})
}

func (t *Table) splitRowsByGranule(buf *dynparquet.Buffer) map[*Granule]*dynparquet.Buffer {
	rowsByGranule := map[*Granule]*dynparquet.Buffer{}

	// Special case: if there is only one granule, insert parts into it until full.
	index := t.Index()
	if index.Len() == 1 {
		rowsByGranule[index.Min().(*Granule)] = buf
		return rowsByGranule
	}

	// TODO: we might be able to do ascend less than or ascend greater than here?
	rows := buf.DynamicRows()
	var prev *Granule
	exhaustedAllRows := false

	row, err := rows.ReadRow(nil)
	if err != nil {
		panic(err)
	}

	index.Ascend(func(i btree.Item) bool {
		g := i.(*Granule)

		for {
			isLess := t.config.schema.RowLessThan(row, g.Least())
			if isLess {
				if prev != nil {
					gbuf, ok := rowsByGranule[prev]
					if !ok {
						gbuf, err = t.config.schema.NewBuffer(buf.DynamicColumns())
						if err != nil {
							panic(err)
						}
						rowsByGranule[prev] = gbuf
					}
					err = gbuf.WriteRow(row.Row)
					if err != nil {
						panic(err)
					}
					row, err = rows.ReadRow(row)
					if err == io.EOF {
						// All rows accounted for
						exhaustedAllRows = true
						return false
					}
					if err != nil {
						panic(err)
					}
					continue
				}
			}

			// stop at the first granule where this is not the least
			// this might be the correct granule, but we need to check that it isn't the next granule
			prev = g
			return true // continue btree iteration
		}
	})

	if !exhaustedAllRows {
		gbuf, ok := rowsByGranule[prev]
		if !ok {
			gbuf, err = t.config.schema.NewBuffer(buf.DynamicColumns())
			if err != nil {
				panic(err)
			}
			rowsByGranule[prev] = gbuf
		}

		// Save any remaining rows that belong into prev
		for {
			err = gbuf.WriteRow(row.Row)
			if err != nil {
				panic(err)
			}
			row, err = rows.ReadRow(row)
			if err == io.EOF {
				break
			}
			if err != nil {
				panic(err)
			}
		}
	}

	return rowsByGranule
}

// compact will compact a Granule; should be performed as a background go routine
func (t *Table) compact(g *Granule) {
	defer t.Done()
	t.splitGranule(g)
}

// addPartToGranule finds the corresponding granule it belongs to in a sorted list of Granules
func addPartToGranule(granules []*Granule, p *Part) {
	row, err := p.Buf.DynamicRows().ReadRow(nil)
	if err == io.EOF {
		return
	}
	if err != nil {
		panic(err)
	}

	var prev *Granule
	for _, g := range granules {
		if g.tableConfig.schema.RowLessThan(row, g.Least()) {
			if prev != nil {
				prev.AddPart(p)
				return
			}
		}
		prev = g
	}

	// Save part to prev
	prev.AddPart(p)
}
