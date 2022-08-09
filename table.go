package frostdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"
	"unsafe"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/apache/arrow/go/v8/arrow/scalar"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/google/btree"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/segmentio/parquet-go"
	"go.uber.org/atomic"

	"github.com/polarsignals/frostdb/dynparquet"
	walpb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/wal/v1alpha1"
	"github.com/polarsignals/frostdb/pqarrow"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

const (

	// schemasPrefix is the prefix for the schema files
	schemasPrefix = "schemas"

	// schemaFileNameFormat is the format string of the schema file name that is written for each table if persistence is enabled.
	schemaFileNameFormat = "schema_%v.json"

	// dataFileName is the name of the parquet blocks that are written to storage if persistence is enabled.
	dataFileName = "data.parquet"
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
	schemas []*dynparquet.Schema
}

// Schema returns the latest schema in the table config
func (t *TableConfig) Schema() *dynparquet.Schema {
	return t.schemas[0] // TODO THOR
}

func NewTableConfig(schemas ...*dynparquet.Schema) *TableConfig {
	return &TableConfig{
		schemas: schemas,
	}
}

type completedBlock struct {
	prevTx uint64
	tx     uint64
}

type Table struct {
	db      *DB
	name    string
	metrics *tableMetrics
	logger  log.Logger

	config *TableConfig

	pendingBlocks   map[*TableBlock]struct{}
	completedBlocks []completedBlock
	lastCompleted   uint64

	mtx    *sync.RWMutex
	active *TableBlock

	wal WAL
}

type WAL interface {
	Close() error
	Log(tx uint64, record *walpb.Record) error
	Replay(handler func(tx uint64, record *walpb.Record) error) error
	Truncate(tx uint64) error
	FirstIndex() (uint64, error)
}

type TableBlock struct {
	table  *Table
	logger log.Logger

	ulid   ulid.ULID
	minTx  uint64
	prevTx uint64

	size  *atomic.Int64
	index *atomic.UnsafePointer // *btree.BTree

	pendingWritersWg sync.WaitGroup

	wg  *sync.WaitGroup
	mtx *sync.RWMutex
}

type tableMetrics struct {
	blockRotated              prometheus.Counter
	granulesCreated           prometheus.Counter
	granulesSplits            prometheus.Counter
	rowsInserted              prometheus.Counter
	zeroRowsInserted          prometheus.Counter
	granulesCompactionAborted prometheus.Counter
	rowInsertSize             prometheus.Histogram
	lastCompletedBlockTx      prometheus.Gauge
}

func newTable(
	db *DB,
	name string,
	tableConfig *TableConfig,
	reg prometheus.Registerer,
	logger log.Logger,
	wal WAL,
) (*Table, error) {
	if db.columnStore.indexDegree <= 0 {
		msg := fmt.Sprintf("Table's columnStore index degree must be a positive integer (received %d)", db.columnStore.indexDegree)
		return nil, errors.New(msg)
	}

	if db.columnStore.splitSize < 2 {
		msg := fmt.Sprintf("Table's columnStore splitSize must be a positive integer > 1 (received %d)", db.columnStore.splitSize)
		return nil, errors.New(msg)
	}

	reg = prometheus.WrapRegistererWith(prometheus.Labels{"table": name}, reg)

	t := &Table{
		db:     db,
		config: tableConfig,
		name:   name,
		logger: logger,
		mtx:    &sync.RWMutex{},
		wal:    wal,
		metrics: &tableMetrics{
			blockRotated: promauto.With(reg).NewCounter(prometheus.CounterOpts{
				Name: "blocks_rotated_total",
				Help: "Number of table blocks that have been rotated.",
			}),
			granulesCreated: promauto.With(reg).NewCounter(prometheus.CounterOpts{
				Name: "granules_created_total",
				Help: "Number of granules created.",
			}),
			granulesSplits: promauto.With(reg).NewCounter(prometheus.CounterOpts{
				Name: "granules_splits_total",
				Help: "Number of granules splits executed.",
			}),
			granulesCompactionAborted: promauto.With(reg).NewCounter(prometheus.CounterOpts{
				Name: "granules_compaction_aborted_total",
				Help: "Number of aborted granules compaction.",
			}),
			rowsInserted: promauto.With(reg).NewCounter(prometheus.CounterOpts{
				Name: "rows_inserted_total",
				Help: "Number of rows inserted into table.",
			}),
			zeroRowsInserted: promauto.With(reg).NewCounter(prometheus.CounterOpts{
				Name: "zero_rows_inserted_total",
				Help: "Number of times it was attempted to insert zero rows into the table.",
			}),
			rowInsertSize: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
				Name:    "row_insert_size",
				Help:    "Size of batch inserts into table.",
				Buckets: prometheus.ExponentialBuckets(1, 2, 10),
			}),
			lastCompletedBlockTx: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
				Name: "last_completed_block_tx",
				Help: "Last completed block transaction.",
			}),
		},
	}

	t.pendingBlocks = make(map[*TableBlock]struct{})

	promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "index_size",
		Help: "Number of granules in the table index currently.",
	}, func() float64 {
		return float64(t.ActiveBlock().Index().Len())
	})

	promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "active_table_block_size",
		Help: "Size of the active table block in bytes.",
	}, func() float64 {
		return float64(t.ActiveBlock().Size())
	})

	return t, nil
}

func (t *Table) newTableBlock(prevTx, tx uint64, id ulid.ULID) error {
	b, err := id.MarshalBinary()
	if err != nil {
		return err
	}

	if err := t.wal.Log(tx, &walpb.Record{
		Entry: &walpb.Entry{
			EntryType: &walpb.Entry_NewTableBlock_{
				NewTableBlock: &walpb.Entry_NewTableBlock{
					TableName: t.name,
					BlockId:   b,
					Schema:    t.Schema().Definition(),
				},
			},
		},
	}); err != nil {
		return err
	}

	t.active, err = newTableBlock(t, prevTx, tx, id)
	if err != nil {
		return err
	}

	return nil
}

func (t *Table) writeBlock(block *TableBlock) {
	level.Debug(t.logger).Log("msg", "syncing block")
	block.pendingWritersWg.Wait()
	block.wg.Wait()

	// from now on, the block will no longer be modified, we can persist it to disk

	level.Debug(t.logger).Log("msg", "done syncing block")

	// Persist the block
	err := block.Persist()
	t.mtx.Lock()
	delete(t.pendingBlocks, block)
	t.mtx.Unlock()
	if err != nil {
		level.Error(t.logger).Log("msg", "failed to persist block")
		level.Error(t.logger).Log("msg", err.Error())
		return
	}

	tx, _, commit := t.db.begin()
	defer commit()

	buf, err := block.ulid.MarshalBinary()
	if err != nil {
		level.Error(t.logger).Log("msg", "failed to record block persistence in WAL: marshal ulid", "err", err)
		return
	}

	if err := t.wal.Log(tx, &walpb.Record{
		Entry: &walpb.Entry{
			EntryType: &walpb.Entry_TableBlockPersisted_{
				TableBlockPersisted: &walpb.Entry_TableBlockPersisted{
					TableName: t.name,
					BlockId:   buf,
				},
			},
		},
	}); err != nil {
		level.Error(t.logger).Log("msg", "failed to record block persistence in WAL", "err", err)
		return
	}

	t.mtx.Lock()
	t.completedBlocks = append(t.completedBlocks, completedBlock{prevTx: block.prevTx, tx: block.minTx})
	sort.Slice(t.completedBlocks, func(i, j int) bool {
		return t.completedBlocks[i].prevTx < t.completedBlocks[j].prevTx
	})
	for {
		if len(t.completedBlocks) == 0 {
			break
		}
		if t.completedBlocks[0].prevTx != t.lastCompleted {
			break
		}

		t.lastCompleted = t.completedBlocks[0].tx
		t.metrics.lastCompletedBlockTx.Set(float64(t.lastCompleted))
		t.completedBlocks = t.completedBlocks[1:]
	}
	t.mtx.Unlock()
	t.db.maintainWAL()
}

func (t *Table) RotateBlock(block *TableBlock) error {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	// Need to check that we haven't already rotated this block.
	if t.active != block {
		return nil
	}

	level.Debug(t.logger).Log("msg", "rotating block", "blockSize", block.Size())
	defer func() {
		level.Debug(t.logger).Log("msg", "done rotating block")
	}()

	tx, _, commit := t.db.begin()
	defer commit()

	id := generateULID()
	if err := t.newTableBlock(t.active.minTx, tx, id); err != nil {
		return err
	}
	t.metrics.blockRotated.Inc()

	t.pendingBlocks[block] = struct{}{}
	go t.writeBlock(block)
	return nil
}

func (t *Table) ActiveBlock() *TableBlock {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	return t.active
}

func (t *Table) ActiveWriteBlock() (*TableBlock, func()) {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	t.active.pendingWritersWg.Add(1)
	return t.active, t.active.pendingWritersWg.Done
}

func (t *Table) Schema() *dynparquet.Schema {
	return t.config.schemas[0] // TODO THOR determine the latest schema
}

func (t *Table) Sync() {
	t.ActiveBlock().Sync()
}

func (t *Table) InsertBuffer(ctx context.Context, buf *dynparquet.Buffer) (uint64, error) {
	b, err := t.Schema().SerializeBuffer(buf) // TODO should we abort this function? If a large buffer is passed this could get long potentially...
	if err != nil {
		return 0, fmt.Errorf("serialize buffer: %w", err)
	}

	return t.Insert(ctx, b)
}

func (t *Table) Insert(ctx context.Context, buf []byte) (uint64, error) {
	return t.insert(ctx, buf)
}

func (t *Table) appendToLog(ctx context.Context, tx uint64, buf []byte) error {
	if err := t.wal.Log(tx, &walpb.Record{
		Entry: &walpb.Entry{
			EntryType: &walpb.Entry_Write_{
				Write: &walpb.Entry_Write{
					Data:      buf,
					TableName: t.name,
				},
			},
		},
	}); err != nil {
		return err
	}
	return nil
}

func (t *Table) appender() (*TableBlock, func(), error) {
	for {
		// Using active write block is important because it ensures that we don't
		// miss pending writers when synchronizing the block.
		block, close := t.ActiveWriteBlock()
		if block.Size() < t.db.columnStore.activeMemorySize {
			return block, close, nil
		}

		// We need to rotate the block and the writer won't actually be used.
		close()

		err := t.RotateBlock(block)
		if err != nil {
			return nil, nil, fmt.Errorf("rotate block: %w", err)
		}
	}
}

func (t *Table) insert(ctx context.Context, buf []byte) (uint64, error) {
	block, close, err := t.appender()
	if err != nil {
		return 0, fmt.Errorf("get appender: %w", err)
	}
	defer close()

	tx, _, commit := t.db.begin()
	defer commit()

	if err := t.appendToLog(ctx, tx, buf); err != nil {
		return tx, fmt.Errorf("append to log: %w", err)
	}

	serBuf, err := dynparquet.ReaderFromBytes(buf)
	if err != nil {
		return tx, fmt.Errorf("deserialize buffer: %w", err)
	}

	err = block.Insert(ctx, tx, serBuf)
	if err != nil {
		return tx, fmt.Errorf("insert buffer into block: %w", err)
	}

	return tx, nil
}

func (t *Table) View(fn func(tx uint64) error) error {
	return fn(t.db.beginRead())
}

// Iterator iterates in order over all granules in the table. It stops iterating when the iterator function returns false.
func (t *Table) Iterator(
	ctx context.Context,
	tx uint64,
	pool memory.Allocator,
	schema *arrow.Schema,
	physicalProjections []logicalplan.Expr,
	projections []logicalplan.Expr,
	filterExpr logicalplan.Expr,
	distinctColumns []logicalplan.Expr,
	iterator func(r arrow.Record) error,
) error {
	rowGroups, err := t.collectRowGroups(ctx, tx, filterExpr)
	if err != nil {
		return err
	}

	// Previously we sorted all row groups into a single row group here,
	// but it turns out that none of the downstream uses actually rely on
	// the sorting so it's not worth it in the general case. Physical plans
	// can decide to sort if they need to in order to exploit the
	// characteristics of sorted data.
	for _, rg := range rowGroups {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if schema == nil {
				schema, err = pqarrow.ParquetRowGroupToArrowSchema(
					ctx,
					t.Schema(),
					rg,
					physicalProjections,
					projections,
					filterExpr,
					distinctColumns,
				)
				if err != nil {
					return err
				}
			}

			var record arrow.Record
			record, err = pqarrow.ParquetRowGroupToArrowRecord(
				ctx,
				pool,
				rg,
				schema,
				filterExpr,
				distinctColumns,
			)
			if err != nil {
				return fmt.Errorf("failed to convert row group to arrow record: %v", err)
			}
			err = iterator(record)
			record.Release()
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// SchemaIterator iterates in order over all granules in the table and returns
// all the schemas seen across the table.
func (t *Table) SchemaIterator(
	ctx context.Context,
	tx uint64,
	pool memory.Allocator,
	physicalProjections []logicalplan.Expr,
	projections []logicalplan.Expr,
	filterExpr logicalplan.Expr,
	distinctColumns []logicalplan.Expr,
	iterator func(r arrow.Record) error,
) error {
	rowGroups, err := t.collectRowGroups(ctx, tx, filterExpr)
	if err != nil {
		return err
	}

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "name", Type: arrow.BinaryTypes.String},
		},
		nil,
	)
	for _, rg := range rowGroups {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			b := array.NewRecordBuilder(pool, schema)

			parquetFields := rg.Schema().Fields()
			fieldNames := make([]string, 0, len(parquetFields))
			for _, f := range parquetFields {
				fieldNames = append(fieldNames, f.Name())
			}

			b.Field(0).(*array.StringBuilder).AppendValues(fieldNames, nil)

			record := b.NewRecord()
			err = iterator(record)
			record.Release()
			b.Release()
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (t *Table) ArrowSchema(
	ctx context.Context,
	tx uint64,
	pool memory.Allocator,
	physicalProjections []logicalplan.Expr,
	projections []logicalplan.Expr,
	filterExpr logicalplan.Expr,
	distinctColumns []logicalplan.Expr,
) (*arrow.Schema, error) {
	rowGroups, err := t.collectRowGroups(ctx, tx, filterExpr)
	if err != nil {
		return nil, err
	}

	// TODO: We should be able to figure out if dynamic columns are even queried.
	// If not, then we can simply return the first schema.

	fieldNames := make([]string, 0, 16)
	fieldsMap := make(map[string]arrow.Field)

	for _, rg := range rowGroups {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			schema, err := pqarrow.ParquetRowGroupToArrowSchema(
				ctx,
				t.Schema(),
				rg,
				physicalProjections,
				projections,
				filterExpr,
				distinctColumns,
			)
			if err != nil {
				return nil, err
			}

			for _, f := range schema.Fields() {
				if _, ok := fieldsMap[f.Name]; !ok {
					fieldNames = append(fieldNames, f.Name)
					fieldsMap[f.Name] = f
				}
			}
		}
	}

	sort.Strings(fieldNames)

	fields := make([]arrow.Field, 0, len(fieldNames))
	for _, name := range fieldNames {
		fields = append(fields, fieldsMap[name])
	}

	return arrow.NewSchema(fields, nil), nil
}

func generateULID() ulid.ULID {
	t := time.Now()
	entropy := ulid.Monotonic(rand.New(rand.NewSource(t.UnixNano())), 0)
	return ulid.MustNew(ulid.Timestamp(t), entropy)
}

func newTableBlock(table *Table, prevTx, tx uint64, id ulid.ULID) (*TableBlock, error) {
	index := btree.New(table.db.columnStore.indexDegree)
	tb := &TableBlock{
		table:  table,
		index:  atomic.NewUnsafePointer(unsafe.Pointer(index)),
		wg:     &sync.WaitGroup{},
		mtx:    &sync.RWMutex{},
		ulid:   id,
		size:   atomic.NewInt64(0),
		logger: table.logger,
		minTx:  tx,
		prevTx: prevTx,
	}

	g, err := NewGranule(tb.table.metrics.granulesCreated, tb.table.config, nil)
	if err != nil {
		return nil, fmt.Errorf("new granule failed: %w", err)
	}
	(*btree.BTree)(tb.index.Load()).ReplaceOrInsert(g)

	return tb, nil
}

// Sync the table block. This will return once all writes have completed and
// all potentially started split operations have completed.
func (t *TableBlock) Sync() {
	t.wg.Wait()
}

func (t *TableBlock) Insert(ctx context.Context, tx uint64, buf *dynparquet.SerializedBuffer) error {
	defer func() {
		t.table.metrics.rowsInserted.Add(float64(buf.NumRows()))
		t.table.metrics.rowInsertSize.Observe(float64(buf.NumRows()))
	}()

	if buf.NumRows() == 0 {
		t.table.metrics.zeroRowsInserted.Add(float64(buf.NumRows()))
		return nil
	}

	rowsToInsertPerGranule, err := t.splitRowsByGranule(buf)
	if err != nil {
		return fmt.Errorf("failed to split rows by granule: %w", err)
	}

	parts := []*Part{}
	for granule, serBuf := range rowsToInsertPerGranule {
		select {
		case <-ctx.Done():
			tombstone(parts)
			return ctx.Err()
		default:
			part := NewPart(tx, serBuf)
			card, err := granule.AddPart(part)
			if err != nil {
				return fmt.Errorf("failed to add part to granule: %w", err)
			}
			parts = append(parts, part)
			if card >= uint64(t.table.db.columnStore.granuleSize) {
				t.wg.Add(1)
				go t.compact(granule)
			}
			t.size.Add(serBuf.ParquetFile().Size())
		}
	}

	return nil
}

func (t *TableBlock) splitGranule(granule *Granule) {
	// Recheck to ensure the granule still needs to be split
	if !granule.metadata.pruned.CAS(0, 1) {
		return
	}

	// Use the latest watermark as the tx id
	tx := t.table.db.tx.Load()

	// Start compaction by adding sentinel node to parts list
	parts := granule.parts.Sentinel(Compacting)

	bufs := []dynparquet.DynamicRowGroup{}
	remain := []*Part{}

	sizeBefore := int64(0)
	// Convert all the parts into a set of rows
	parts.Iterate(func(p *Part) bool {
		// Don't merge uncompleted transactions
		if p.tx > tx {
			if p.tx < math.MaxUint64 { // drop tombstoned parts
				remain = append(remain, p)
			}
			return true
		}

		for i, n := 0, p.Buf.NumRowGroups(); i < n; i++ {
			bufs = append(bufs, p.Buf.DynamicRowGroup(i))
		}

		sizeBefore += p.Buf.ParquetFile().Size()
		return true
	})

	if len(bufs) == 0 { // aborting; nothing to do
		t.abort(granule)
		return
	}

	merge, err := t.table.Schema().MergeDynamicRowGroups(bufs)
	if err != nil {
		t.abort(granule)
		level.Error(t.logger).Log("msg", "failed to merge dynamic row groups", "err", err)
		return
	}

	b := bytes.NewBuffer(nil)
	cols := merge.DynamicColumns()
	w, err := t.table.Schema().GetWriter(b, cols)
	if err != nil {
		t.abort(granule)
		level.Error(t.logger).Log("msg", "failed to create new schema writer", "err", err)
		return
	}
	defer t.table.Schema().PutWriter(w)

	rowBuf := make([]parquet.Row, 1)
	rows := merge.Rows()
	n := 0
	for {
		_, err := rows.ReadRows(rowBuf)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.abort(granule)
			level.Error(t.logger).Log("msg", "error reading rows", "err", err)
			return
		}
		_, err = w.WriteRows(rowBuf)
		if err != nil {
			t.abort(granule)
			level.Error(t.logger).Log("msg", "error writing rows", "err", err)
			return
		}
		n++
	}

	if err := rows.Close(); err != nil {
		t.abort(granule)
		level.Error(t.logger).Log("msg", "error closing schema writer", "err", err)
		return
	}

	if err := w.Close(); err != nil {
		t.abort(granule)
		level.Error(t.logger).Log("msg", "error closing schema writer", "err", err)
		return
	}

	if n < t.table.db.columnStore.granuleSize { // It's possible to have a Granule marked for compaction but all the parts in it aren't completed tx's yet
		t.abort(granule)
		return
	}

	serBuf, err := dynparquet.ReaderFromBytes(b.Bytes())
	if err != nil {
		t.abort(granule)
		level.Error(t.logger).Log("msg", "failed to create reader from bytes", "err", err)
		return
	}

	g, err := NewGranule(t.table.metrics.granulesCreated, t.table.config, NewPart(tx, serBuf))
	if err != nil {
		t.abort(granule)
		level.Error(t.logger).Log("msg", "failed to create granule", "err", err)
		return
	}

	granules, err := g.split(tx, t.table.db.columnStore.granuleSize/t.table.db.columnStore.splitSize)
	if err != nil {
		t.abort(granule)
		level.Error(t.logger).Log("msg", "failed to split granule", "err", err)
		return
	}

	// add remaining parts onto new granules
	for _, p := range remain {
		err := addPartToGranule(granules, p)
		if err != nil {
			t.abort(granule)
			level.Error(t.logger).Log("msg", "failed to add part to granule", "err", err)
			return
		}
	}

	// we disable compaction for new granules before allowing new insert to be propagated to them
	for _, childGranule := range granules {
		childGranule.metadata.pruned.Store(1)
	}

	// we restore the possibility to trigger compaction after we exited the function
	defer func() {
		for _, childGranule := range granules {
			childGranule.metadata.pruned.Store(0)
		}
	}()

	// set the newGranules pointer, so new writes will propogate into these new granules
	granule.newGranules = granules

	// Mark compaction complete in the granule; this will cause new writes to start using the newGranules pointer
	parts = granule.parts.Sentinel(Compacted)

	// Now we need to copy any new parts that happened while we were compacting
	parts.Iterate(func(p *Part) bool {
		err = addPartToGranule(granules, p)
		if err != nil {
			return false
		}
		return true
	})
	if err != nil {
		t.abort(granule)
		level.Error(t.logger).Log("msg", "failed to add part to granule", "err", err)
		return
	}

	for {
		curIndex := t.Index()
		t.mtx.Lock()
		index := curIndex.Clone() // TODO(THOR): we can't clone concurrently
		t.mtx.Unlock()

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
		if t.index.CAS(unsafe.Pointer(curIndex), unsafe.Pointer(index)) {
			sizeDiff := serBuf.ParquetFile().Size() - sizeBefore
			t.size.Add(sizeDiff)
			return
		}
	}
}

// RowGroupIterator iterates in order over all granules in the table.
// It stops iterating when the iterator function returns false.
func (t *TableBlock) RowGroupIterator(
	ctx context.Context,
	tx uint64,
	filterExpr logicalplan.Expr,
	filter TrueNegativeFilter,
	iterator func(rg dynparquet.DynamicRowGroup) bool,
) error {
	index := t.Index()

	var err error
	index.Ascend(func(i btree.Item) bool {
		g := i.(*Granule)

		// Check if the entire granule can be skipped due to the filter expr
		if !filterGranule(t.logger, filterExpr, g) {
			return true
		}

		g.PartBuffersForTx(tx, func(buf *dynparquet.SerializedBuffer) bool {
			f := buf.ParquetFile()
			for i := range f.RowGroups() {
				rg := buf.DynamicRowGroup(i)
				var mayContainUsefulData bool
				mayContainUsefulData, err = filter.Eval(rg)
				if err != nil {
					return false
				}
				if mayContainUsefulData {
					if continu := iterator(rg); !continu {
						return false
					}
				}
			}
			return true
		})

		return true
	})

	return err
}

// Size returns the cumulative size of all buffers in the table. This is roughly the size of the table in bytes.
func (t *TableBlock) Size() int64 {
	return t.size.Load()
}

// Index provides atomic access to the table index.
func (t *TableBlock) Index() *btree.BTree {
	return (*btree.BTree)(t.index.Load())
}

func (t *TableBlock) splitRowsByGranule(buf *dynparquet.SerializedBuffer) (map[*Granule]*dynparquet.SerializedBuffer, error) {
	// Special case: if there is only one granule, insert parts into it until full.
	index := t.Index()
	if index.Len() == 1 {
		b := bytes.NewBuffer(nil)

		cols := buf.DynamicColumns()
		w, err := t.table.Schema().GetWriter(b, cols)
		if err != nil {
			return nil, ErrCreateSchemaWriter{err}
		}
		defer t.table.Schema().PutWriter(w)

		rowBuf := make([]parquet.Row, 1)
		rows := buf.Reader()
		for {
			_, err := rows.ReadRows(rowBuf)
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, ErrReadRow{err}
			}
			_, err = w.WriteRows(rowBuf)
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

	writerByGranule := map[*Granule]*dynparquet.PooledWriter{}
	bufByGranule := map[*Granule]*bytes.Buffer{}
	defer func() {
		for _, w := range writerByGranule {
			t.table.Schema().PutWriter(w)
		}
	}()

	// TODO: we might be able to do ascend less than or ascend greater than here?
	rows := buf.DynamicRows()
	var prev *Granule
	exhaustedAllRows := false

	rowBuf := &dynparquet.DynamicRows{Rows: make([]parquet.Row, 1)}
	_, err := rows.ReadRows(rowBuf)
	if err != nil {
		return nil, ErrReadRow{err}
	}
	row := rowBuf.Get(0)

	var ascendErr error

	index.Ascend(func(i btree.Item) bool {
		g := i.(*Granule)

		for {
			least := g.Least()
			isLess := t.table.Schema().RowLessThan(row, least)
			if isLess {
				if prev != nil {
					w, ok := writerByGranule[prev]
					if !ok {
						b := bytes.NewBuffer(nil)
						w, err = t.table.Schema().GetWriter(b, buf.DynamicColumns())
						if err != nil {
							ascendErr = ErrCreateSchemaWriter{err}
							return false
						}
						writerByGranule[prev] = w
						bufByGranule[prev] = b
					}
					_, err = w.WriteRows([]parquet.Row{row.Row})
					if err != nil {
						ascendErr = ErrWriteRow{err}
						return false
					}
					n, err := rows.ReadRows(rowBuf)
					if err == io.EOF && n == 0 {
						// All rows accounted for
						exhaustedAllRows = true
						return false
					}
					if err != nil && err != io.EOF {
						ascendErr = ErrReadRow{err}
						return false
					}
					row = rowBuf.Get(0)
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
			w, err = t.table.Schema().GetWriter(b, buf.DynamicColumns())
			if err != nil {
				return nil, ErrCreateSchemaWriter{err}
			}
			writerByGranule[prev] = w
			bufByGranule[prev] = b
		}

		// Save any remaining rows that belong into prev
		for {
			_, err = w.WriteRows([]parquet.Row{row.Row})
			if err != nil {
				return nil, ErrWriteRow{err}
			}
			n, err := rows.ReadRows(rowBuf)
			if err == io.EOF && n == 0 {
				break
			}
			if err != nil && err != io.EOF {
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
func (t *TableBlock) compact(g *Granule) {
	defer t.wg.Done()
	t.splitGranule(g)
}

// addPartToGranule finds the corresponding granule it belongs to in a sorted list of Granules.
func addPartToGranule(granules []*Granule, p *Part) error {
	row, err := p.Least()
	if err != nil {
		return err
	}

	var prev *Granule
	for _, g := range granules {
		if g.tableConfig.Schema().RowLessThan(row, g.Least()) {
			if prev != nil {
				if _, err := prev.addPart(p, row); err != nil {
					return err
				}
				return nil
			}
		}
		prev = g
	}

	if prev != nil {
		// Save part to prev
		if _, err := prev.addPart(p, row); err != nil {
			return err
		}
	}

	return nil
}

// abort a compaction transaction.
func (t *TableBlock) abort(granule *Granule) {
	t.table.metrics.granulesCompactionAborted.Inc()
	for {
		if granule.metadata.pruned.CAS(1, 0) { // unmark pruned, so that we can compact it in the future
			return
		}
	}
}

func (t *TableBlock) Serialize() ([]byte, error) {
	ctx := context.Background()

	// Read all row groups
	rowGroups := []dynparquet.DynamicRowGroup{}
	// math.MaxUint64 is passed because we want to serialize everything.
	err := t.RowGroupIterator(ctx, math.MaxUint64, nil, &AlwaysTrueFilter{}, func(rg dynparquet.DynamicRowGroup) bool {
		rowGroups = append(rowGroups, rg)
		return true
	})
	if err != nil {
		return nil, err
	}

	merged, err := t.table.Schema().MergeDynamicRowGroups(rowGroups)
	if err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(nil)
	cols := merged.DynamicColumns()
	w, err := t.table.Schema().GetWriter(buf, cols)
	if err != nil {
		return nil, err
	}
	defer t.table.Schema().PutWriter(w)

	rows := merged.Rows()
	n := 0
	for {
		rowsBuf := make([]parquet.Row, 1)
		_, err := rows.ReadRows(rowsBuf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		_, err = w.WriteRows(rowsBuf)
		if err != nil {
			return nil, err
		}
		n++
	}

	err = w.Close()

	level.Info(t.logger).Log("msg", "writing rows to disk", "rows_count", n)
	return buf.Bytes(), err
}

// filterGranule returns false if this granule does not contain useful data.
func filterGranule(logger log.Logger, filterExpr logicalplan.Expr, g *Granule) bool {
	if filterExpr == nil {
		return true
	}

	switch expr := filterExpr.(type) {
	default: // unsupported filter
		level.Info(logger).Log("msg", "unsupported filter")
		return true
	case *logicalplan.BinaryExpr:
		var (
			min, max   *parquet.Value
			v          scalar.Scalar
			leftresult bool
			leftfound  bool
		)
		switch left := expr.Left.(type) {
		case *logicalplan.BinaryExpr:
			leftresult = filterGranule(logger, left, g)
		case *logicalplan.Column:
			min, max, leftfound = findColumnValues(left.ColumnsUsedExprs(), g)
		case *logicalplan.LiteralExpr:
			switch left.Value.(type) {
			case *scalar.Int64:
				v = left.Value.(*scalar.Int64)
			case *scalar.String:
				v = left.Value.(*scalar.String)
			}
		}

		switch right := expr.Right.(type) {
		case *logicalplan.BinaryExpr:
			switch expr.Op {
			case logicalplan.OpAnd:
				if !leftresult {
					return false
				}
				rightresult := filterGranule(logger, right, g)
				return leftresult && rightresult
			}
		case *logicalplan.Column:
			var found bool
			min, max, found = findColumnValues(right.ColumnsUsedExprs(), g)
			if !found {
				// If we fallthrough to here, than we didn't find any columns that match so we can skip this granule
				return false
			}

			switch val := v.(type) {
			case *scalar.Int64:
				switch expr.Op {
				case logicalplan.OpLt:
					return val.Value < max.Int64()
				case logicalplan.OpGt:
					return val.Value > min.Int64()
				case logicalplan.OpEq:
					return val.Value >= min.Int64() && val.Value <= max.Int64()
				}
			case *scalar.String:
				s := string(val.Value.Bytes())
				if len(val.Value.Bytes()) > dynparquet.ColumnIndexSize {
					s = string(val.Value.Bytes()[:dynparquet.ColumnIndexSize])
				}
				switch expr.Op {
				case logicalplan.OpLt:
					return string(val.Value.Bytes()[:dynparquet.ColumnIndexSize]) < max.String()
				case logicalplan.OpGt:
					return string(val.Value.Bytes()[:dynparquet.ColumnIndexSize]) > min.String()
				case logicalplan.OpEq:
					return s >= min.String() && s <= max.String()
				}
			}

		case *logicalplan.LiteralExpr:
			switch v := right.Value.(type) {
			case *scalar.Int64:
				if !leftfound {
					return false
				}
				switch expr.Op {
				case logicalplan.OpLt:
					return min.Int64() < v.Value
				case logicalplan.OpGt:
					return max.Int64() > v.Value
				case logicalplan.OpEq:
					return v.Value >= min.Int64() && v.Value <= max.Int64()
				}
			case *scalar.String:
				s := string(v.Value.Bytes())
				if len(v.Value.Bytes()) > dynparquet.ColumnIndexSize {
					s = string(v.Value.Bytes()[:dynparquet.ColumnIndexSize])
				}
				if !leftfound {
					switch {
					case expr.Op == logicalplan.OpEq && len(s) == 0:
						return true
					case expr.Op == logicalplan.OpNotEq && len(s) != 0:
						return true
					case expr.Op == logicalplan.OpRegexNotMatch || expr.Op == logicalplan.OpRegexMatch:
						// todo: run the regex on empty string to see if it matches
						// this would allow to fully reject a granule faster.
						// However it requires a bigger refactoring of the code to compile the regexp only once.
						return true
					default:
						return false
					}
				}
				switch expr.Op {
				case logicalplan.OpLt:
					return min.String() < s
				case logicalplan.OpGt:
					return max.String() > s
				case logicalplan.OpEq:
					return s >= min.String() && s <= max.String()
				}
			}
		}
	}

	return true
}

func findColumnValues(matchers []logicalplan.Expr, g *Granule) (*parquet.Value, *parquet.Value, bool) {
	findMinColumn := func() (*parquet.Value, string) {
		g.metadata.minlock.RLock()
		defer g.metadata.minlock.RUnlock()
		for _, matcher := range matchers {
			for column, min := range g.metadata.min {
				if matcher.MatchColumn(column) {
					return min, column
				}
			}
		}
		return nil, ""
	}

	min, column := findMinColumn()
	if min == nil {
		return nil, nil, false
	}

	g.metadata.maxlock.RLock()
	max := g.metadata.max[column]
	g.metadata.maxlock.RUnlock()

	return min, max, true
}

// tombstone marks all the parts with the max tx id to ensure they aren't included in reads.
// Tombstoned parts will be eventually dropped from the database during compaction.
func tombstone(parts []*Part) {
	for _, part := range parts {
		part.tx = math.MaxUint64
	}
}

func (t *Table) memoryBlocks() ([]*TableBlock, uint64) {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	lastReadBlockTimestamp := t.active.ulid.Time()
	memoryBlocks := []*TableBlock{t.active}
	for block := range t.pendingBlocks {
		memoryBlocks = append(memoryBlocks, block)

		if block.ulid.Time() < lastReadBlockTimestamp {
			lastReadBlockTimestamp = block.ulid.Time()
		}
	}

	return memoryBlocks, lastReadBlockTimestamp
}

// collectRowGroups collects all the row groups from the table for the given filter.
func (t *Table) collectRowGroups(ctx context.Context, tx uint64, filterExpr logicalplan.Expr) ([]dynparquet.DynamicRowGroup, error) {
	filter, err := booleanExpr(filterExpr)
	if err != nil {
		return nil, err
	}

	rowGroups := []dynparquet.DynamicRowGroup{}
	iteratorFunc := func(rg dynparquet.DynamicRowGroup) bool {
		rowGroups = append(rowGroups, rg)
		return true
	}

	// pending blocks could be uploaded to the bucket while we iterate on them.
	// to avoid to iterate on them again while reading the block file
	// we keep the last block timestamp to be read from the bucket and pass it to the IterateBucketBlocks() function
	// so that every block with a timestamp >= lastReadBlockTimestamp is discarded while being read.
	memoryBlocks, lastReadBlockTimestamp := t.memoryBlocks()
	for _, block := range memoryBlocks {
		if err := block.RowGroupIterator(ctx, tx, filterExpr, filter, iteratorFunc); err != nil {
			return nil, err
		}
	}

	if err := t.IterateBucketBlocks(ctx, t.logger, filter, iteratorFunc, lastReadBlockTimestamp); err != nil {
		return nil, err
	}

	return rowGroups, nil
}
