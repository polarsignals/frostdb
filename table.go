package arcticdb

import (
	"bytes"
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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/segmentio/parquet-go"

	"github.com/polarsignals/arcticdb/dynparquet"
	"github.com/polarsignals/arcticdb/pqarrow"
	"github.com/polarsignals/arcticdb/query/logicalplan"
)

var ErrNoSchema = fmt.Errorf("no schema")

type ErrWriteRow struct{ err error }

func (e ErrWriteRow) Error() string { return "failed to write row: " + e.err.Error() }

type ErrReadRow struct{ err error }

func (e ErrReadRow) Error() string { return "failed to read row: " + e.err.Error() }

type ErrCreateSchemaWriter struct{ err error }

func (e ErrCreateSchemaWriter) Error() string {
	return "failed to create schema write: " + e.err.Error()
}

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

	rowsToInsertPerGranule, err := t.splitRowsByGranule(buf)
	if err != nil {
		return fmt.Errorf("failed to split rows by granule: %w", err)
	}

	for granule, serBuf := range rowsToInsertPerGranule {
		if granule.AddPart(NewPart(tx, serBuf)) >= uint64(t.config.granuleSize) {
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

		for i := 0; i < p.Buf.NumRowGroups(); i++ {
			bufs = append(bufs, p.Buf.DynamicRowGroup(i))
		}
		return true
	})

	merge, err := t.config.schema.MergeDynamicRowGroups(bufs)
	if err != nil {
		panic(err)
	}

	b := bytes.NewBuffer(nil)
	w, err := t.config.schema.NewWriter(b, merge.DynamicColumns())
	if err != nil {
		panic(err)
	}

	rows := merge.Rows()
	n := 0
	for {
		row, err := rows.ReadRow(nil)
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		err = w.WriteRow(row)
		if err != nil {
			panic(err)
		}
		n++
	}

	err = w.Close()
	if err != nil {
		panic(err)
	}

	if n < t.config.granuleSize { // It's possible to have a Granule marked for compaction but all the parts in it aren't completed tx's yet
		for {
			if atomic.CompareAndSwapUint64(&granule.pruned, 1, 0) { // unmark pruned, so that we can compact it in the future
				return
			}
		}
	}

	serBuf, err := dynparquet.ReaderFromBytes(b.Bytes())
	if err != nil {
		panic(err)
	}

	g := NewGranule(t.metrics.granulesCreated, t.config, NewPart(tx, serBuf))

	granules, err := g.split(tx, t.config.granuleSize/2) // TODO magic numbers
	if err != nil {
		panic(err)
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
func (t *Table) Iterator(
	pool memory.Allocator,
	projections []logicalplan.ColumnMatcher,
	filterExpr logicalplan.Expr,
	iterator func(r arrow.Record) error,
) error {
	index := t.Index()
	tx := t.db.beginRead()

	filter, err := booleanExpr(filterExpr)
	if err != nil {
		return err
	}

	index.Ascend(func(i btree.Item) bool {
		g := i.(*Granule)

		rowGroups := []dynparquet.DynamicRowGroup{}
		g.PartBuffersForTx(tx, t.db.txCompleted, func(buf *dynparquet.SerializedBuffer) bool {
			f := buf.ParquetFile()
			for i := 0; i < f.NumRowGroups(); i++ {
				rg := buf.DynamicRowGroup(i)
				var mayContainUsefulData bool
				mayContainUsefulData, err = filter.Eval(rg)
				if err != nil {
					return false
				}
				if mayContainUsefulData {
					rowGroups = append(rowGroups, rg)
				}
			}
			return true
		})

		if len(rowGroups) == 0 {
			// Granule had no readable parts for this transaction.
			return true
		}

		// Previously we sorted all row groups into a single row group here,
		// but it turns out that none of the downstream uses actually rely on
		// the sorting so it's not worth it in the general case. Physical plans
		// can decide to sort if they need to in order to exploit the
		// characteristics of sorted data.
		for _, rg := range rowGroups {
			var record arrow.Record
			record, err = pqarrow.ParquetRowGroupToArrowRecord(pool, rg, projections)
			if err != nil {
				return false
			}
			err = iterator(record)
			record.Release()
			if err != nil {
				return false
			}
		}

		return true
	})
	return err
}

// Index provides atomic access to the table index.
func (t *Table) Index() *btree.BTree {
	ptr := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&t.index)))
	return (*btree.BTree)(ptr)
}

func (t *Table) splitRowsByGranule(buf *dynparquet.Buffer) (map[*Granule]*dynparquet.SerializedBuffer, error) {
	// Special case: if there is only one granule, insert parts into it until full.
	index := t.Index()
	if index.Len() == 1 {
		b := bytes.NewBuffer(nil)
		w, err := t.config.schema.NewWriter(b, buf.DynamicColumns())
		if err != nil {
			return nil, ErrCreateSchemaWriter{err}
		}

		rows := buf.Rows()
		for {
			row, err := rows.ReadRow(nil)
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, ErrReadRow{err}
			}
			err = w.WriteRow(row)
			if err != nil {
				return nil, ErrWriteRow{err}
			}
		}

		err = w.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to close schema writer: %w", err)
		}

		serBuf, err := dynparquet.ReaderFromBytes(b.Bytes())
		if err != nil {
			return nil, fmt.Errorf("failed to create dynparquet reader: %w", err)
		}

		return map[*Granule]*dynparquet.SerializedBuffer{
			index.Min().(*Granule): serBuf,
		}, nil
	}

	writerByGranule := map[*Granule]*parquet.Writer{}
	bufByGranule := map[*Granule]*bytes.Buffer{}

	// TODO: we might be able to do ascend less than or ascend greater than here?
	rows := buf.DynamicRows()
	var prev *Granule
	exhaustedAllRows := false

	row, err := rows.ReadRow(nil)
	if err != nil {
		return nil, ErrReadRow{err}
	}

	var ascendErr error

	index.Ascend(func(i btree.Item) bool {
		g := i.(*Granule)

		for {
			isLess := t.config.schema.RowLessThan(row, g.Least())
			if isLess {
				if prev != nil {
					w, ok := writerByGranule[prev]
					if !ok {
						b := bytes.NewBuffer(nil)
						w, err = t.config.schema.NewWriter(b, buf.DynamicColumns())
						if err != nil {
							ascendErr = ErrCreateSchemaWriter{err}
							return false
						}
						writerByGranule[prev] = w
						bufByGranule[prev] = b
					}
					err = w.WriteRow(row.Row)
					if err != nil {
						ascendErr = ErrWriteRow{err}
						return false
					}
					row, err = rows.ReadRow(row)
					if err == io.EOF {
						// All rows accounted for
						exhaustedAllRows = true
						return false
					}
					if err != nil {
						ascendErr = ErrReadRow{err}
						return false
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
	if ascendErr != nil {
		return nil, ascendErr
	}

	if !exhaustedAllRows {
		w, ok := writerByGranule[prev]
		if !ok {
			b := bytes.NewBuffer(nil)
			w, err = t.config.schema.NewWriter(b, buf.DynamicColumns())
			if err != nil {
				return nil, ErrCreateSchemaWriter{err}
			}
			writerByGranule[prev] = w
			bufByGranule[prev] = b
		}

		// Save any remaining rows that belong into prev
		for {
			err = w.WriteRow(row.Row)
			if err != nil {
				return nil, ErrWriteRow{err}
			}
			row, err = rows.ReadRow(row)
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, ErrReadRow{err}
			}
		}
	}

	res := map[*Granule]*dynparquet.SerializedBuffer{}
	for g, w := range writerByGranule {
		err := w.Close()
		if err != nil {
			return nil, err
		}
		res[g], err = dynparquet.ReaderFromBytes(bufByGranule[g].Bytes())
		if err != nil {
			return nil, fmt.Errorf("failed to read from granule buffer: %w", err)
		}
	}

	return res, nil
}

// compact will compact a Granule; should be performed as a background go routine.
func (t *Table) compact(g *Granule) {
	defer t.Done()
	t.splitGranule(g)
}

// addPartToGranule finds the corresponding granule it belongs to in a sorted list of Granules.
func addPartToGranule(granules []*Granule, p *Part) {
	row, err := p.Buf.DynamicRowGroup(0).DynamicRows().ReadRow(nil)
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

	if prev != nil {
		// Save part to prev
		prev.AddPart(p)
	}
}
