package frostdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/google/btree"
	"github.com/google/uuid"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/segmentio/parquet-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	"github.com/polarsignals/frostdb/dynparquet"
	walpb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/wal/v1alpha1"
	"github.com/polarsignals/frostdb/pqarrow"
	"github.com/polarsignals/frostdb/query/logicalplan"
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
	schema *dynparquet.Schema
}

func NewTableConfig(
	schema *dynparquet.Schema,
) *TableConfig {
	return &TableConfig{
		schema: schema,
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
	tracer  trace.Tracer

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
	index *atomic.Pointer[btree.BTree] // *btree.BTree

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
	numParts                  prometheus.Gauge
}

func newTable(
	db *DB,
	name string,
	tableConfig *TableConfig,
	reg prometheus.Registerer,
	logger log.Logger,
	tracer trace.Tracer,
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
		tracer: tracer,
		mtx:    &sync.RWMutex{},
		wal:    wal,
		metrics: &tableMetrics{
			numParts: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
				Name: "num_parts",
				Help: "Number of parts currently active.",
			}),
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
					Schema:    t.config.schema.Definition(),
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
	t.metrics.numParts.Set(float64(0))

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
	if t.config == nil {
		return nil
	}
	return t.config.schema
}

func (t *Table) Sync() {
	t.ActiveBlock().Sync()
}

// Write objects into the table.
func (t *Table) Write(ctx context.Context, vals ...any) (uint64, error) {
	b, err := ValuesToBuffer(t.Schema(), vals...)
	if err != nil {
		return 0, err
	}

	return t.InsertBuffer(ctx, b)
}

var (
	matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
	matchAllCap   = regexp.MustCompile("([a-z0-9])([A-Z])")
)

func ToSnakeCase(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}

func dedupe(s map[string][]string) map[string][]string {
	final := map[string][]string{}
	set := map[string]map[string]struct{}{}
	for k, v := range s {
		if set[k] == nil {
			set[k] = map[string]struct{}{}
		}
		for _, i := range v {
			if _, ok := set[k][i]; !ok {
				set[k][i] = struct{}{}
				final[k] = append(final[k], i)
			}
		}
	}

	for _, s := range final {
		sort.Strings(s)
	}
	return final
}

func ValuesToBuffer(schema *dynparquet.Schema, vals ...any) (*dynparquet.Buffer, error) {
	dynamicColumns := map[string][]string{}
	rows := make([]parquet.Row, 0, len(vals))

	findColumn := func(val reflect.Value, col string, v any) any {
		for i := 0; i < val.NumField(); i++ {
			if ToSnakeCase(val.Type().Field(i).Name) == col {
				return val.Field(i).Interface()
			}
		}
		return nil
	}

	// Collect dynamic columns
	for _, v := range vals {
		val := reflect.ValueOf(v)
		for _, col := range schema.Columns() {
			cv := findColumn(val, col.Name, v)
			switch col.Dynamic {
			case true:
				switch reflect.TypeOf(cv).Kind() {
				case reflect.Struct:
					dynVals := reflect.ValueOf(cv)
					for j := 0; j < dynVals.NumField(); j++ {
						dynamicColumns[col.Name] = append(dynamicColumns[col.Name], ToSnakeCase(dynVals.Type().Field(j).Name))
					}
				case reflect.Slice:
					dynVals := reflect.ValueOf(cv)
					for j := 0; j < dynVals.Len(); j++ {
						pair := reflect.ValueOf(dynVals.Index(j).Interface())
						dynamicColumns[col.Name] = append(dynamicColumns[col.Name], ToSnakeCase(fmt.Sprintf("%v", pair.Field(0))))
					}
				default:
					return nil, fmt.Errorf("unsupported dynamic type")
				}
			}
		}
	}

	dynamicColumns = dedupe(dynamicColumns)

	// Create all rows
	for _, v := range vals {
		row := []parquet.Value{}
		val := reflect.ValueOf(v)

		colIdx := 0
		for _, col := range schema.Columns() {
			cv := findColumn(val, col.Name, v)
			switch col.Dynamic {
			case true:
				switch reflect.TypeOf(cv).Kind() {
				case reflect.Struct:
					dynVals := reflect.ValueOf(cv)
					for _, dyncol := range dynamicColumns[col.Name] {
						found := false
						for j := 0; j < dynVals.NumField(); j++ {
							if ToSnakeCase(dynVals.Type().Field(j).Name) == dyncol {
								row = append(row, parquet.ValueOf(dynVals.Field(j).Interface()).Level(0, 1, colIdx))
								colIdx++
								found = true
								break
							}
						}
						if !found {
							row = append(row, parquet.ValueOf(nil).Level(0, 0, colIdx))
							colIdx++
						}
					}
				case reflect.Slice:
					dynVals := reflect.ValueOf(cv)
					for _, dyncol := range dynamicColumns[col.Name] {
						found := false
						for j := 0; j < dynVals.Len(); j++ {
							pair := reflect.ValueOf(dynVals.Index(j).Interface())
							if ToSnakeCase(fmt.Sprintf("%v", pair.Field(0).Interface())) == dyncol {
								row = append(row, parquet.ValueOf(pair.Field(1).Interface()).Level(0, 1, colIdx))
								colIdx++
								found = true
								break
							}
						}
						if !found {
							row = append(row, parquet.ValueOf(nil).Level(0, 0, colIdx))
							colIdx++
						}
					}
				default:
					return nil, fmt.Errorf("unsupported dynamic type")
				}
			default:
				switch t := cv.(type) {
				case []uuid.UUID: // Special handling for this type
					row = append(row, parquet.ValueOf(dynparquet.ExtractLocationIDs(t)).Level(0, 0, colIdx))
				default:
					row = append(row, parquet.ValueOf(cv).Level(0, 0, colIdx))
				}
				colIdx++
			}
		}

		rows = append(rows, row)
	}

	pb, err := schema.NewBuffer(dynamicColumns)
	if err != nil {
		return nil, err
	}

	_, err = pb.WriteRows(rows)
	if err != nil {
		return nil, err
	}

	return pb, nil
}

func (t *Table) InsertBuffer(ctx context.Context, buf *dynparquet.Buffer) (uint64, error) {
	b, err := t.config.schema.SerializeBuffer(buf) // TODO should we abort this function? If a large buffer is passed this could get long potentially...
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

func (t *Table) View(ctx context.Context, fn func(ctx context.Context, tx uint64) error) error {
	ctx, span := t.tracer.Start(ctx, "Table/View")
	tx := t.db.beginRead()
	span.SetAttributes(attribute.Int64("tx", int64(tx))) // Attributes don't support uint64...
	defer span.End()
	return fn(ctx, tx)
}

// prepareForFlush sets the nullability on a Record column if the type is a
// ListType.
// TODO: Is this a bug in arrow? We already set the nullability in
// parquetColumnToArrowArray, but it doesn't appear to transfer into the
// resulting array's type. Needs to be investigated.
func prepareForFlush(r arrow.Record, schema *parquet.Schema) {
	for i, c := range r.Columns() {
		switch t := c.DataType().(type) {
		case *arrow.ListType:
			t.SetElemNullable(schema.Fields()[i].Optional())
		}
	}
}

// Iterator iterates in order over all granules in the table. It stops iterating when the iterator function returns false.
func (t *Table) Iterator(
	ctx context.Context,
	tx uint64,
	pool memory.Allocator,
	schema *arrow.Schema,
	iterOpts logicalplan.IterOptions,
	callbacks []logicalplan.Callback,
) error {
	ctx, span := t.tracer.Start(ctx, "Table/Iterator")
	span.SetAttributes(attribute.Int("physicalProjections", len(iterOpts.PhysicalProjection)))
	span.SetAttributes(attribute.Int("projections", len(iterOpts.Projection)))
	span.SetAttributes(attribute.Int("distinct", len(iterOpts.DistinctColumns)))
	defer span.End()

	if len(callbacks) == 0 {
		return errors.New("no callbacks provided")
	}
	if schema == nil {
		return errors.New("arrow output schema not provided")
	}
	if len(schema.Fields()) == 0 {
		// An empty schema might be surprising, but this can happen in cases
		// where the table was empty or all row groups were filtered out when
		// calling ArrowSchema. In any case, the semantics of an empty schema
		// are to emit zero rows.
		return nil
	}

	rowGroups := make(chan dynparquet.DynamicRowGroup, len(callbacks)*4) // buffer up to 4 row groups per callback

	// Previously we sorted all row groups into a single row group here,
	// but it turns out that none of the downstream uses actually rely on
	// the sorting so it's not worth it in the general case. Physical plans
	// can decide to sort if they need to in order to exploit the
	// characteristics of sorted data.

	// bufferSize specifies a threshold of records past which the
	// buffered results are flushed to the next operator.
	const bufferSize = 1024

	errg, ctx := errgroup.WithContext(ctx)
	for _, callback := range callbacks {
		callback := callback
		errg.Go(func() error {
			converter := pqarrow.NewParquetConverter(pool, schema, iterOpts.Filter, iterOpts.DistinctColumns)
			defer converter.Close()

			var rgSchema *parquet.Schema

			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case rg, ok := <-rowGroups:
					if !ok {
						r := converter.NewRecord()
						prepareForFlush(r, rgSchema)
						if r.NumRows() == 0 {
							return nil
						}
						if err := callback(ctx, r); err != nil {
							return err
						}
						r.Release()
						return nil
					}
					rgSchema = rg.Schema()
					if err := converter.Convert(ctx, rg); err != nil {
						return fmt.Errorf("failed to convert row group to arrow record: %v", err)
					}
					if converter.NumRows() >= bufferSize {
						r := converter.NewRecord()
						prepareForFlush(r, rgSchema)
						if err := callback(ctx, r); err != nil {
							return err
						}
						r.Release()
					}
				}
			}
		})
	}

	errg.Go(func() error {
		if err := t.collectRowGroups(ctx, tx, iterOpts.Filter, rowGroups); err != nil {
			return err
		}
		close(rowGroups)
		return nil
	})

	return errg.Wait()
}

// SchemaIterator iterates in order over all granules in the table and returns
// all the schemas seen across the table.
func (t *Table) SchemaIterator(
	ctx context.Context,
	tx uint64,
	pool memory.Allocator,
	iterOpts logicalplan.IterOptions,
	callbacks []logicalplan.Callback,
) error {
	ctx, span := t.tracer.Start(ctx, "Table/SchemaIterator")
	span.SetAttributes(attribute.Int("physicalProjections", len(iterOpts.PhysicalProjection)))
	span.SetAttributes(attribute.Int("projections", len(iterOpts.Projection)))
	span.SetAttributes(attribute.Int("distinct", len(iterOpts.DistinctColumns)))
	defer span.End()

	if len(callbacks) == 0 {
		return errors.New("no callbacks provided")
	}

	rowGroups := make(chan dynparquet.DynamicRowGroup, len(callbacks)*4) // buffer up to 4 row groups per callback

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "name", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	errg, ctx := errgroup.WithContext(ctx)
	for _, callback := range callbacks {
		callback := callback
		errg.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case rg, ok := <-rowGroups:
					if !ok {
						return nil // we're done
					}
					if rg == nil {
						return errors.New("received nil rowGroup") // shouldn't happen, but anyway
					}
					b := array.NewRecordBuilder(pool, schema)

					parquetFields := rg.Schema().Fields()
					fieldNames := make([]string, 0, len(parquetFields))
					for _, f := range parquetFields {
						fieldNames = append(fieldNames, f.Name())
					}

					b.Field(0).(*array.StringBuilder).AppendValues(fieldNames, nil)

					record := b.NewRecord()
					if err := callback(ctx, record); err != nil {
						return err
					}
					record.Release()
					b.Release()
				}
			}
		})
	}

	errg.Go(func() error {
		if err := t.collectRowGroups(ctx, tx, iterOpts.Filter, rowGroups); err != nil {
			return err
		}
		close(rowGroups)
		return nil
	})

	return errg.Wait()
}

func (t *Table) ArrowSchema(
	ctx context.Context,
	tx uint64,
	pool memory.Allocator,
	iterOpts logicalplan.IterOptions,
) (*arrow.Schema, error) {
	ctx, span := t.tracer.Start(ctx, "Table/ArrowSchema")
	span.SetAttributes(attribute.Int("physicalProjections", len(iterOpts.PhysicalProjection)))
	span.SetAttributes(attribute.Int("projections", len(iterOpts.Projection)))
	span.SetAttributes(attribute.Int("distinct", len(iterOpts.DistinctColumns)))
	defer span.End()

	rowGroups := make(chan dynparquet.DynamicRowGroup)

	// TODO: We should be able to figure out if dynamic columns are even queried.
	// If not, then we can simply return the first schema.

	fieldNames := make([]string, 0, 16)
	fieldsMap := make(map[string]arrow.Field)

	// Since we only ever create one goroutine here there is no concurrency issue with the above maps.
	errg, ctx := errgroup.WithContext(ctx)
	errg.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case rg, ok := <-rowGroups:
				if !ok {
					return nil // we're done
				}
				if rg == nil {
					return nil // shouldn't happen, but anyway
				}

				schema, err := pqarrow.ParquetRowGroupToArrowSchema(
					ctx,
					rg,
					iterOpts.PhysicalProjection,
					iterOpts.Projection,
					iterOpts.Filter,
					iterOpts.DistinctColumns,
				)
				if err != nil {
					return err
				}

				for _, f := range schema.Fields() {
					if _, ok := fieldsMap[f.Name]; !ok {
						fieldNames = append(fieldNames, f.Name)
						fieldsMap[f.Name] = f
					}
				}
			}
		}
	})

	err := t.collectRowGroups(ctx, tx, iterOpts.Filter, rowGroups)
	if err != nil {
		return nil, err
	}

	close(rowGroups)
	if err := errg.Wait(); err != nil {
		return nil, err
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
	index := atomic.Pointer[btree.BTree]{}
	index.Store(btree.New(table.db.columnStore.indexDegree))

	tb := &TableBlock{
		table:  table,
		index:  &index,
		wg:     &sync.WaitGroup{},
		mtx:    &sync.RWMutex{},
		ulid:   id,
		size:   &atomic.Int64{},
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

	b := bytes.NewBuffer(nil)
	w, err := t.table.config.schema.GetWriter(b, buf.DynamicColumns())
	if err != nil {
		return fmt.Errorf("failed to get writer: %w", err)
	}
	defer t.table.config.schema.PutWriter(w)

	parts := []*Part{}
	for granule, indicies := range rowsToInsertPerGranule {
		select {
		case <-ctx.Done():
			tombstone(parts)
			return ctx.Err()
		default:

			rowBuf := make([]parquet.Row, 1)
			rows := buf.Reader()
			for index := 0; ; index++ {
				_, err := rows.ReadRows(rowBuf)
				if err == io.EOF {
					break
				}
				if err != nil {
					return err
				}

				// Check if this index belongs in this granule
				if _, ok := indicies[index]; !ok {
					continue
				}

				_, err = w.WriteRows(rowBuf)
				if err != nil {
					return fmt.Errorf("failed to write rows: %w", err)
				}
			}
			if err := w.Close(); err != nil {
				return fmt.Errorf("failed to close writer: %w", err)
			}

			serBuf, err := dynparquet.ReaderFromBytes(b.Bytes())
			if err != nil {
				return fmt.Errorf("failed to get reader from bytes: %w", err)
			}

			part := NewPart(tx, serBuf)
			size, err := granule.AddPart(part)
			if err != nil {
				return fmt.Errorf("failed to add part to granule: %w", err)
			}
			parts = append(parts, part)
			if size >= uint64(t.table.db.columnStore.granuleSizeBytes) {
				t.wg.Add(1)
				go t.compact(granule)
			}
			t.size.Add(serBuf.ParquetFile().Size())

			b = bytes.NewBuffer(nil)
			w.Reset(b)
		}
	}

	t.table.metrics.numParts.Add(float64(len(parts)))
	return nil
}

func (t *TableBlock) compactGranule(granule *Granule) error {
	// Recheck to ensure the granule still needs to be split
	if !granule.metadata.pruned.CompareAndSwap(0, 1) {
		return nil
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
		return nil
	}

	merge, err := t.table.config.schema.MergeDynamicRowGroups(bufs)
	if err != nil {
		return fmt.Errorf("failed to merge dynamic row groups: %w", err)
	}

	b := bytes.NewBuffer(nil)
	cols := merge.DynamicColumns()
	w, err := t.table.config.schema.GetWriter(b, cols)
	if err != nil {
		return fmt.Errorf("failed to create new schema writer: %w", err)
	}
	defer t.table.config.schema.PutWriter(w)

	rowBuf := make([]parquet.Row, 1)
	rows := merge.Rows()
	n := 0
	for {
		_, err := rows.ReadRows(rowBuf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading rows: %w", err)
		}
		_, err = w.WriteRows(rowBuf)
		if err != nil {
			return fmt.Errorf("writing rows: %w", err)
		}
		n++
	}

	if err := rows.Close(); err != nil {
		return fmt.Errorf("closing schema reader: %w", err)
	}

	if err := w.Close(); err != nil {
		return fmt.Errorf("closing schema writer: %w", err)
	}

	serBuf, err := dynparquet.ReaderFromBytes(b.Bytes())
	if err != nil {
		return fmt.Errorf("reader from bytes: %w", err)
	}

	g, err := NewGranule(t.table.metrics.granulesCreated, t.table.config, NewPart(tx, serBuf))
	if err != nil {
		return fmt.Errorf("new granule: %w", err)
	}

	granules := []*Granule{g}

	// only split the granule if it still exceeds the size
	if serBuf.ParquetFile().Size() > t.table.db.columnStore.granuleSizeBytes {
		granules, err = g.split(tx, 2)
		if err != nil {
			return fmt.Errorf("splitting granule: %w", err)
		}
	}

	// add remaining parts onto new granules
	for _, p := range remain {
		err := addPartToGranule(granules, p)
		if err != nil {
			return fmt.Errorf("add parts to granules: %w", err)
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
		return fmt.Errorf("add part to granules: %w", err)
	}

	for {
		curIndex := t.Index()
		t.mtx.Lock()
		index := curIndex.Clone() // TODO(THOR): we can't clone concurrently
		t.mtx.Unlock()

		deleted := index.Delete(granule)
		if deleted == nil {
			level.Error(t.logger).Log("msg", "failed to delete granule during split")
			continue
		}

		for _, g := range granules {
			if dupe := index.ReplaceOrInsert(g); dupe != nil {
				level.Error(t.logger).Log("duplicate insert performed")
			}
		}

		// Point to the new index
		if t.index.CompareAndSwap(curIndex, index) {
			sizeDiff := serBuf.ParquetFile().Size() - sizeBefore
			t.size.Add(sizeDiff)

			change := len(bufs) - len(granules) - len(remain)
			t.table.metrics.numParts.Sub(float64(change))
			return nil
		}
	}
}

// RowGroupIterator iterates in order over all granules in the table.
// It stops iterating when the iterator function returns false.
func (t *TableBlock) RowGroupIterator(
	ctx context.Context,
	tx uint64,
	filter TrueNegativeFilter,
	rowGroups chan<- dynparquet.DynamicRowGroup,
) error {
	index := t.Index()

	var err error
	index.Ascend(func(i btree.Item) bool {
		g := i.(*Granule)

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
					select {
					case <-ctx.Done():
						err = ctx.Err()
						return false
					case rowGroups <- rg:
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

func (t *TableBlock) splitRowsByGranule(buf *dynparquet.SerializedBuffer) (map[*Granule]map[int]struct{}, error) {
	index := t.Index()
	if index.Len() == 1 {
		rows := map[int]struct{}{}
		for i := 0; i < int(buf.NumRows()); i++ {
			rows[i] = struct{}{}
		}

		return map[*Granule]map[int]struct{}{
			index.Min().(*Granule): rows,
		}, nil
	}

	rowsByGranule := map[*Granule]map[int]struct{}{}

	// TODO: we might be able to do ascend less than or ascend greater than here?
	rows := buf.DynamicRows()
	defer rows.Close()
	var prev *Granule
	exhaustedAllRows := false

	rowBuf := &dynparquet.DynamicRows{Rows: make([]parquet.Row, 1)}
	_, err := rows.ReadRows(rowBuf)
	if err != nil {
		return nil, ErrReadRow{err}
	}
	idx := 0
	row := rowBuf.Get(0)

	var ascendErr error

	index.Ascend(func(i btree.Item) bool {
		g := i.(*Granule)

		for {
			least := g.Least()
			isLess := t.table.config.schema.RowLessThan(row, least)
			if isLess {
				if prev != nil {
					_, ok := rowsByGranule[prev]
					if !ok {
						rowsByGranule[prev] = map[int]struct{}{idx: {}}
					} else {
						rowsByGranule[prev][idx] = struct{}{}
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
					idx++
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
		_, ok := rowsByGranule[prev]
		if !ok {
			rowsByGranule[prev] = map[int]struct{}{}
		}

		// Save any remaining rows that belong into prev
		for {
			rowsByGranule[prev][idx] = struct{}{}
			n, err := rows.ReadRows(rowBuf)
			if err == io.EOF && n == 0 {
				break
			}
			if err != nil && err != io.EOF {
				return nil, ErrReadRow{err}
			}
			row = rowBuf.Get(0)
			idx++
		}
	}

	return rowsByGranule, nil
}

// compact will compact a Granule; should be performed as a background go routine.
func (t *TableBlock) compact(g *Granule) {
	defer t.wg.Done()
	if err := t.compactGranule(g); err != nil {
		t.abort(g)
		level.Error(t.logger).Log("msg", "failed to compact granule", "err", err)
	}
}

// addPartToGranule finds the corresponding granule it belongs to in a sorted list of Granules.
func addPartToGranule(granules []*Granule, p *Part) error {
	row, err := p.Least()
	if err != nil {
		return err
	}

	var prev *Granule
	for _, g := range granules {
		if g.tableConfig.schema.RowLessThan(row, g.Least()) {
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
		if granule.metadata.pruned.CompareAndSwap(1, 0) { // unmark pruned, so that we can compact it in the future
			return
		}
	}
}

func (t *TableBlock) Serialize(writer io.Writer) error {
	ctx := context.Background()

	// Read all row groups
	rowGroups := []dynparquet.DynamicRowGroup{}

	rowGroupsChan := make(chan dynparquet.DynamicRowGroup)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for rg := range rowGroupsChan {
			rowGroups = append(rowGroups, rg)
		}
		wg.Done()
	}()

	// Collect all the row groups just to determine the dynamic cols
	err := t.RowGroupIterator(ctx, math.MaxUint64, &AlwaysTrueFilter{}, rowGroupsChan)
	if err != nil {
		return err
	}

	close(rowGroupsChan)
	wg.Wait()

	merged, err := t.table.config.schema.MergeDynamicRowGroups(rowGroups)
	if err != nil {
		return err
	}

	cols := merged.DynamicColumns()
	w, err := t.table.config.schema.GetWriter(writer, cols)
	if err != nil {
		return err
	}
	defer t.table.config.schema.PutWriter(w)
	defer w.Close()

	// Iterate over all the row groups, and write them to storage
	maxRows := 2 // TODO expose this setting
	return t.writeRows(w, maxRows, rowGroups)
}

// writeRows writes a set of dynamic row groups to a writer.
func (t *TableBlock) writeRows(w *dynparquet.PooledWriter, count int, rowGroups []dynparquet.DynamicRowGroup) error {
	merged, err := t.table.config.schema.MergeDynamicRowGroups(rowGroups)
	if err != nil {
		return err
	}

	rows := merged.Rows()
	defer rows.Close()
	total := 0
	for {
		rowsBuf := make([]parquet.Row, 1)
		n, err := rows.ReadRows(rowsBuf)
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 {
			break
		}

		if _, err = w.WriteRows(rowsBuf); err != nil {
			return err
		}
		total++
		if count > 0 && total >= count {
			if err := w.Flush(); err != nil {
				return err
			}
			total = 0
		}
	}

	return nil
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

	if t.active == nil { // this is currently a read only table
		return nil, 0
	}

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
func (t *Table) collectRowGroups(
	ctx context.Context,
	tx uint64,
	filterExpr logicalplan.Expr,
	rowGroups chan<- dynparquet.DynamicRowGroup,
) error {
	ctx, span := t.tracer.Start(ctx, "Table/collectRowGroups")
	defer span.End()

	filter, err := booleanExpr(filterExpr)
	if err != nil {
		return err
	}

	// pending blocks could be uploaded to the bucket while we iterate on them.
	// to avoid to iterate on them again while reading the block file
	// we keep the last block timestamp to be read from the bucket and pass it to the IterateBucketBlocks() function
	// so that every block with a timestamp >= lastReadBlockTimestamp is discarded while being read.
	memoryBlocks, lastBlockTimestamp := t.memoryBlocks()
	for _, block := range memoryBlocks {
		span.AddEvent("memoryBlock")
		if err := block.RowGroupIterator(ctx, tx, filter, rowGroups); err != nil {
			return err
		}
	}

	if err := t.IterateBucketBlocks(ctx, t.logger, lastBlockTimestamp, filter, rowGroups); err != nil {
		return err
	}

	return nil
}
