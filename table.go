package frostdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/dustin/go-humanize"
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
	"google.golang.org/protobuf/proto"

	"github.com/polarsignals/frostdb/bufutils"
	"github.com/polarsignals/frostdb/dynparquet"
	schemapb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/schema/v1alpha1"
	schemav2pb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/schema/v1alpha2"
	tablepb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/table/v1alpha1"
	walpb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/wal/v1alpha1"
	"github.com/polarsignals/frostdb/parts"
	"github.com/polarsignals/frostdb/pqarrow"
	"github.com/polarsignals/frostdb/pqarrow/arrowutils"
	"github.com/polarsignals/frostdb/query/logicalplan"
	"github.com/polarsignals/frostdb/wal"
	walpkg "github.com/polarsignals/frostdb/wal"
)

var (
	ErrNoSchema     = fmt.Errorf("no schema")
	ErrTableClosing = fmt.Errorf("table closing")
)

type ErrWriteRow struct{ err error }

func (e ErrWriteRow) Error() string { return "failed to write row: " + e.err.Error() }

type ErrReadRow struct{ err error }

func (e ErrReadRow) Error() string { return "failed to read row: " + e.err.Error() }

type ErrCreateSchemaWriter struct{ err error }

func (e ErrCreateSchemaWriter) Error() string {
	return "failed to create schema write: " + e.err.Error()
}

type TableOption func(*tablepb.TableConfig) error

// WithRowGroupSize sets the size in number of rows for each row group for parquet files. A <= 0 value indicates no limit.
func WithRowGroupSize(numRows int) TableOption {
	return func(config *tablepb.TableConfig) error {
		config.RowGroupSize = uint64(numRows)
		return nil
	}
}

// WithBlockReaderLimit sets the limit of go routines that will be used to read persisted block files. A negative number indicates no limit.
func WithBlockReaderLimit(n int) TableOption {
	return func(config *tablepb.TableConfig) error {
		config.BlockReaderLimit = uint64(n)
		return nil
	}
}

// WithoutWAL disables the WAL for this table.
func WithoutWAL() TableOption {
	return func(config *tablepb.TableConfig) error {
		config.DisableWal = true
		return nil
	}
}

// FromConfig sets the table configuration from the given config.
// NOTE: that this does not override the schema even though that is included in the passed in config.
func FromConfig(config *tablepb.TableConfig) TableOption {
	return func(cfg *tablepb.TableConfig) error {
		if config.BlockReaderLimit != 0 { // the zero value is not a valid block reader limit
			cfg.BlockReaderLimit = config.BlockReaderLimit
		}
		cfg.DisableWal = config.DisableWal
		cfg.RowGroupSize = config.RowGroupSize
		return nil
	}
}

func defaultTableConfig() *tablepb.TableConfig {
	return &tablepb.TableConfig{
		BlockReaderLimit: uint64(runtime.GOMAXPROCS(0)),
	}
}

func NewTableConfig(
	schema proto.Message,
	options ...TableOption,
) *tablepb.TableConfig {
	t := defaultTableConfig()

	switch v := schema.(type) {
	case *schemapb.Schema:
		t.Schema = &tablepb.TableConfig_DeprecatedSchema{
			DeprecatedSchema: v,
		}
	case *schemav2pb.Schema:
		t.Schema = &tablepb.TableConfig_SchemaV2{
			SchemaV2: v,
		}
	default:
		panic(fmt.Sprintf("unsupported schema type: %T", v))
	}

	for _, opt := range options {
		_ = opt(t)
	}

	return t
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

	config *tablepb.TableConfig
	schema *dynparquet.Schema

	pendingBlocks   map[*TableBlock]struct{}
	completedBlocks []completedBlock
	lastCompleted   uint64

	mtx    *sync.RWMutex
	active *TableBlock

	wal     WAL
	closing bool
}

type WAL interface {
	Close() error
	Log(tx uint64, record *walpb.Record) error
	LogRecord(tx uint64, table string, record arrow.Record) error
	// Replay replays WAL records from the given first index. If firstIndex is
	// 0, the first index read from the WAL is used (i.e. given a truncation,
	// using 0 is still valid). If the given firstIndex is less than the WAL's
	// first index on disk, the replay happens from the first index on disk.
	// If the handler panics, the WAL implementation will truncate the WAL up to
	// the last valid index.
	Replay(tx uint64, handler wal.ReplayHandlerFunc) error
	Truncate(tx uint64) error
	FirstIndex() (uint64, error)
	LastIndex() (uint64, error)
}

type TableBlock struct {
	table  *Table
	logger log.Logger
	tracer trace.Tracer

	ulid   ulid.ULID
	minTx  uint64
	prevTx uint64

	size *atomic.Int64
	// lastSnapshotSize keeps track of the size of the block when it last
	// triggered a snapshot.
	lastSnapshotSize atomic.Int64
	index            *atomic.Pointer[btree.BTree] // *btree.BTree

	pendingWritersWg sync.WaitGroup
	pendingReadersWg sync.WaitGroup

	mtx *sync.RWMutex
}

type tableMetrics struct {
	blockRotated              prometheus.Counter
	granulesCreated           prometheus.Counter
	compactions               prometheus.Counter
	granulesSplits            prometheus.Counter
	rowsInserted              prometheus.Counter
	zeroRowsInserted          prometheus.Counter
	granulesCompactionAborted prometheus.Counter
	rowInsertSize             prometheus.Histogram
	lastCompletedBlockTx      prometheus.Gauge
	numParts                  prometheus.Gauge
	unsortedInserts           prometheus.Counter
	compactionMetrics         *compactionMetrics
}

func schemaFromTableConfig(tableConfig *tablepb.TableConfig) (*dynparquet.Schema, error) {
	switch schema := tableConfig.Schema.(type) {
	case *tablepb.TableConfig_DeprecatedSchema:
		return dynparquet.SchemaFromDefinition(schema.DeprecatedSchema)
	case *tablepb.TableConfig_SchemaV2:
		return dynparquet.SchemaFromDefinition(schema.SchemaV2)
	default:
		// No schema defined for table; read/only table
		return nil, nil
	}
}

func newTable(
	db *DB,
	name string,
	tableConfig *tablepb.TableConfig,
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

	if tableConfig == nil {
		tableConfig = defaultTableConfig()
	}

	s, err := schemaFromTableConfig(tableConfig)
	if err != nil {
		return nil, err
	}

	t := &Table{
		db:     db,
		config: tableConfig,
		name:   name,
		logger: logger,
		tracer: tracer,
		mtx:    &sync.RWMutex{},
		wal:    wal,
		schema: s,
		metrics: &tableMetrics{
			numParts: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
				Name: "frostdb_table_num_parts",
				Help: "Number of parts currently active.",
			}),
			blockRotated: promauto.With(reg).NewCounter(prometheus.CounterOpts{
				Name: "frostdb_table_blocks_rotated_total",
				Help: "Number of table blocks that have been rotated.",
			}),
			granulesCreated: promauto.With(reg).NewCounter(prometheus.CounterOpts{
				Name: "frostdb_table_granules_created_total",
				Help: "Number of granules created.",
			}),
			compactions: promauto.With(reg).NewCounter(prometheus.CounterOpts{
				Name: "frostdb_table_granules_compactions_total",
				Help: "Number of granule compactions.",
			}),
			granulesSplits: promauto.With(reg).NewCounter(prometheus.CounterOpts{
				Name: "frostdb_table_granules_splits_total",
				Help: "Number of granules splits executed.",
			}),
			granulesCompactionAborted: promauto.With(reg).NewCounter(prometheus.CounterOpts{
				Name: "frostdb_table_granules_compaction_aborted_total",
				Help: "Number of aborted granules compaction.",
			}),
			rowsInserted: promauto.With(reg).NewCounter(prometheus.CounterOpts{
				Name: "frostdb_table_rows_inserted_total",
				Help: "Number of rows inserted into table.",
			}),
			zeroRowsInserted: promauto.With(reg).NewCounter(prometheus.CounterOpts{
				Name: "frostdb_table_zero_rows_inserted_total",
				Help: "Number of times it was attempted to insert zero rows into the table.",
			}),
			rowInsertSize: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
				Name:    "frostdb_table_row_insert_size",
				Help:    "Size of batch inserts into table.",
				Buckets: prometheus.ExponentialBuckets(1, 2, 10),
			}),
			lastCompletedBlockTx: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
				Name: "frostdb_table_last_completed_block_tx",
				Help: "Last completed block transaction.",
			}),
			unsortedInserts: promauto.With(reg).NewCounter(prometheus.CounterOpts{
				Name: "frostdb_table_unsorted_inserts_total",
				Help: "The number of times a buffer to insert was not in sorted order.",
			}),
			compactionMetrics: newCompactionMetrics(reg, float64(db.columnStore.granuleSizeBytes)),
		},
	}

	// Disable the WAL for this table by replacing any given WAL with a nop wal
	if tableConfig.DisableWal {
		t.wal = &walpkg.NopWAL{}
	}

	t.pendingBlocks = make(map[*TableBlock]struct{})

	promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "frostdb_table_index_size",
		Help: "Number of granules in the table index currently.",
	}, func() float64 {
		if active := t.ActiveBlock(); active != nil {
			return float64(active.Index().Len())
		}
		return 0
	})

	promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "frostdb_table_active_block_size",
		Help: "Size of the active table block in bytes.",
	}, func() float64 {
		if active := t.ActiveBlock(); active != nil {
			return float64(active.Size())
		}
		return 0
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
					Config:    t.config,
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

func (t *Table) dropPendingBlock(block *TableBlock) {
	t.mtx.Lock()
	defer t.mtx.Unlock()
	delete(t.pendingBlocks, block)

	// Wait for outstanding readers to finish with the block before releasing underlying resources.
	block.pendingReadersWg.Wait()

	block.Index().Ascend(func(i btree.Item) bool {
		g := i.(*Granule)
		g.PartsForTx(math.MaxUint64, func(p *parts.Part) bool {
			if r := p.Record(); r != nil {
				r.Release()
			}
			return true
		})
		return true
	})
}

func (t *Table) writeBlock(block *TableBlock, snapshotDB bool) {
	level.Debug(t.logger).Log("msg", "syncing block")
	block.pendingWritersWg.Wait()

	// from now on, the block will no longer be modified, we can persist it to disk

	level.Debug(t.logger).Log("msg", "done syncing block")

	// Persist the block
	err := block.Persist()
	t.dropPendingBlock(block)
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
	if snapshotDB && t.db.columnStore.snapshotTriggerSize != 0 && t.db.columnStore.enableWAL {
		func() {
			if !t.db.snapshotInProgress.CompareAndSwap(false, true) {
				// Snapshot already in progress. This could lead to duplicate
				// data when replaying (refer to the snapshot design document),
				// but discarding this data on recovery is better than a
				// potential additional CPU spike caused by another snapshot.
				return
			}
			defer t.db.snapshotInProgress.Store(false)
			// This snapshot snapshots the new, active, table block. Refer to
			// the snapshot design document for more details as to why this
			// snapshot is necessary.
			// context.Background is used here for the snapshot since callers
			// might cancel the context when the write is finished but the
			// snapshot is not. Note that block.Persist does the same.
			// TODO(asubiotto): Eventually we should register a cancel function
			// that is called with a grace period on db.Close.
			ctx := context.Background()
			if err := t.db.snapshotAtTX(ctx, tx); err != nil {
				level.Error(t.logger).Log(
					"msg", "failed to write snapshot on block rotation",
					"err", err,
				)
			}
			if err := t.db.reclaimDiskSpace(ctx); err != nil {
				level.Error(t.logger).Log(
					"msg", "failed to reclaim disk space after snapshot on block rotation",
					"err", err,
				)
				return
			}
		}()
	}
}

func (t *Table) RotateBlock(ctx context.Context, block *TableBlock) error {
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
	go t.writeBlock(block, true /* snapshotDB */)

	return nil
}

func (t *Table) ActiveBlock() *TableBlock {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	return t.active
}

func (t *Table) ActiveWriteBlock() (*TableBlock, func(), error) {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	if t.closing {
		return nil, nil, ErrTableClosing
	}

	t.active.pendingWritersWg.Add(1)
	return t.active, t.active.pendingWritersWg.Done, nil
}

func (t *Table) Schema() *dynparquet.Schema {
	if t.config == nil {
		return nil
	}
	return t.schema
}

func (t *Table) EnsureCompaction() error {
	return t.ActiveBlock().EnsureCompaction()
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

	dynamicColumns = bufutils.Dedupe(dynamicColumns)

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

func (t *Table) InsertRecord(ctx context.Context, record arrow.Record) (uint64, error) {
	block, close, err := t.appender(ctx)
	if err != nil {
		return 0, fmt.Errorf("get appender: %w", err)
	}
	defer close()

	tx, _, commit := t.db.begin()
	defer commit()

	if err := t.wal.LogRecord(tx, t.name, record); err != nil {
		return tx, fmt.Errorf("append to log: %w", err)
	}

	if err := block.InsertRecord(ctx, tx, record); err != nil {
		return tx, fmt.Errorf("insert buffer into block: %w", err)
	}

	return tx, nil
}

func (t *Table) InsertBuffer(ctx context.Context, buf *dynparquet.Buffer) (uint64, error) {
	b := bytes.NewBuffer(nil)
	err := t.schema.SerializeBuffer(b, buf) // TODO should we abort this function? If a large buffer is passed this could get long potentially...
	if err != nil {
		return 0, fmt.Errorf("serialize buffer: %w", err)
	}

	return t.Insert(ctx, b.Bytes())
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

func (t *Table) appender(ctx context.Context) (*TableBlock, func(), error) {
	for {
		// Using active write block is important because it ensures that we don't
		// miss pending writers when synchronizing the block.
		block, close, err := t.ActiveWriteBlock()
		if err != nil {
			return nil, nil, err
		}

		blockSize := block.Size()
		if t.db.columnStore.snapshotTriggerSize != 0 &&
			// If size-lastSnapshotSize > snapshotTriggerSize (a column store
			// option), a new snapshot is triggered. This is basically the size
			// of the new data in this block since the last snapshot.
			blockSize-block.lastSnapshotSize.Load() > t.db.columnStore.snapshotTriggerSize {
			// context.Background is used here for the snapshot since callers
			// might cancel the context when the write is finished but the
			// snapshot is not.
			// TODO(asubiotto): Eventually we should register a cancel function
			// that is called with a grace period on db.Close.
			t.db.asyncSnapshot(context.Background(), func() {
				level.Debug(t.logger).Log(
					"msg", "successful snapshot on block size trigger",
					"block_size", humanize.IBytes(uint64(blockSize)),
					"last_snapshot_size", humanize.IBytes(uint64(block.lastSnapshotSize.Load())),
				)
				block.lastSnapshotSize.Store(blockSize)
				if err := t.db.reclaimDiskSpace(ctx); err != nil {
					level.Error(t.logger).Log(
						"msg", "failed to reclaim disk space after snapshot",
						"err", err,
					)
					return
				}
			})
		}
		if blockSize < t.db.columnStore.activeMemorySize {
			return block, close, nil
		}

		// We need to rotate the block and the writer won't actually be used.
		close()

		err = t.RotateBlock(ctx, block)
		if err != nil {
			return nil, nil, fmt.Errorf("rotate block: %w", err)
		}
	}
}

func (t *Table) insert(ctx context.Context, buf []byte) (uint64, error) {
	block, close, err := t.appender(ctx)
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

// Iterator iterates in order over all granules in the table. It stops iterating when the iterator function returns false.
func (t *Table) Iterator(
	ctx context.Context,
	tx uint64,
	pool memory.Allocator,
	callbacks []logicalplan.Callback,
	options ...logicalplan.Option,
) error {
	iterOpts := &logicalplan.IterOptions{}
	for _, opt := range options {
		opt(iterOpts)
	}
	ctx, span := t.tracer.Start(ctx, "Table/Iterator")
	span.SetAttributes(attribute.Int("physicalProjections", len(iterOpts.PhysicalProjection)))
	span.SetAttributes(attribute.Int("projections", len(iterOpts.Projection)))
	span.SetAttributes(attribute.Int("distinct", len(iterOpts.DistinctColumns)))
	defer span.End()

	if len(callbacks) == 0 {
		return errors.New("no callbacks provided")
	}
	rowGroups := make(chan any, len(callbacks)*4) // buffer up to 4 row groups per callback

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
			converter := pqarrow.NewParquetConverter(pool, *iterOpts)
			defer converter.Close()

			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case rg, ok := <-rowGroups:
					if !ok {
						r := converter.NewRecord()
						if r == nil || r.NumRows() == 0 {
							return nil
						}
						if err := callback(ctx, r); err != nil {
							return err
						}
						r.Release()
						return nil
					}

					switch t := rg.(type) {
					case arrow.Record:
						err := callback(ctx, t)
						t.Release()
						if err != nil {
							return err
						}
					case dynparquet.DynamicRowGroup:
						if err := converter.Convert(ctx, t); err != nil {
							return fmt.Errorf("failed to convert row group to arrow record: %v", err)
						}
						// This RowGroup had no relevant data. Ignore it.
						if len(converter.Fields()) == 0 {
							continue
						}
						if converter.NumRows() >= bufferSize {
							r := converter.NewRecord()
							if err := callback(ctx, r); err != nil {
								return err
							}
							r.Release()
						}
					default:
						return fmt.Errorf("unknown row group type: %T", t)
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
	callbacks []logicalplan.Callback,
	options ...logicalplan.Option,
) error {
	iterOpts := &logicalplan.IterOptions{}
	for _, opt := range options {
		opt(iterOpts)
	}
	ctx, span := t.tracer.Start(ctx, "Table/SchemaIterator")
	span.SetAttributes(attribute.Int("physicalProjections", len(iterOpts.PhysicalProjection)))
	span.SetAttributes(attribute.Int("projections", len(iterOpts.Projection)))
	span.SetAttributes(attribute.Int("distinct", len(iterOpts.DistinctColumns)))
	defer span.End()

	if len(callbacks) == 0 {
		return errors.New("no callbacks provided")
	}

	rowGroups := make(chan any, len(callbacks)*4) // buffer up to 4 row groups per callback

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

					b := array.NewRecordBuilder(pool, schema)

					switch t := rg.(type) {
					case arrow.Record:
						for _, f := range t.Schema().Fields() {
							b.Field(0).(*array.StringBuilder).Append(f.Name)
						}
						record := b.NewRecord()
						err := callback(ctx, record)
						record.Release()
						t.Release()
						if err != nil {
							return err
						}
					case dynparquet.DynamicRowGroup:
						if rg == nil {
							return errors.New("received nil rowGroup") // shouldn't happen, but anyway
						}
						parquetFields := t.Schema().Fields()
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
					default:
						return fmt.Errorf("unknown row group type: %T", t)
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

func generateULID() ulid.ULID {
	t := time.Now()
	entropy := ulid.Monotonic(rand.New(rand.NewSource(t.UnixNano())), 0)
	return ulid.MustNew(ulid.Timestamp(t), entropy)
}

func newTableBlock(table *Table, prevTx, tx uint64, id ulid.ULID) (*TableBlock, error) {
	index := atomic.Pointer[btree.BTree]{}
	index.Store(btree.New(table.db.columnStore.indexDegree))

	return &TableBlock{
		table:  table,
		index:  &index,
		mtx:    &sync.RWMutex{},
		ulid:   id,
		size:   &atomic.Int64{},
		logger: table.logger,
		tracer: table.tracer,
		minTx:  tx,
		prevTx: prevTx,
	}, nil
}

// EnsureCompaction forces a TableBlock compaction.
func (t *TableBlock) EnsureCompaction() error {
	return t.compact(t.table.db.columnStore.compactionConfig)
}

func (t *TableBlock) InsertRecord(ctx context.Context, tx uint64, record arrow.Record) error {
	defer func() {
		t.table.metrics.rowsInserted.Add(float64(record.NumRows()))
		t.table.metrics.rowInsertSize.Observe(float64(record.NumRows()))
	}()

	if record.NumRows() == 0 {
		t.table.metrics.zeroRowsInserted.Add(1)
		return nil
	}

	if err := t.insertRecordToGranules(tx, record); err != nil {
		return fmt.Errorf("failed to insert record into granules: %w", err)
	}

	return nil
}

func (t *TableBlock) Insert(ctx context.Context, tx uint64, buf *dynparquet.SerializedBuffer) error {
	defer func() {
		t.table.metrics.rowsInserted.Add(float64(buf.NumRows()))
		t.table.metrics.rowInsertSize.Observe(float64(buf.NumRows()))
	}()

	numRows := buf.NumRows()
	if numRows == 0 {
		t.table.metrics.zeroRowsInserted.Add(float64(buf.NumRows()))
		return nil
	}

	var dynRows *dynparquet.DynamicRows
	{
		rowBuf := make([]parquet.Row, numRows)
		rows := buf.Reader()
		defer rows.Close()

		// TODO(asubiotto): Add utility method to read all rows.
		n := 0
		for int64(n) < numRows {
			readN, err := rows.ReadRows(rowBuf[n:])
			for i := n; i < n+readN; i++ {
				rowBuf[i] = rowBuf[i].Clone()
			}
			n += readN
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return err
			}
		}

		dynRows = dynparquet.NewDynamicRows(
			rowBuf,
			buf.ParquetFile().Schema(),
			buf.DynamicColumns(),
			buf.ParquetFile().Schema().Fields(),
		)

		if !dynRows.IsSorted(t.table.schema) {
			// Input rows should be sorted. Eventually, we should return an error.
			// However, for caution, we just increment a metric and sort the rows.
			t.table.metrics.unsortedInserts.Inc()
			sorter := dynparquet.NewDynamicRowSorter(t.table.schema, dynRows)
			sort.Sort(sorter)
		}
	}

	rowsToInsertPerGranule, err := t.splitRowsByGranule(dynRows)
	if err != nil {
		return fmt.Errorf("failed to split rows by granule: %w", err)
	}

	b := bytes.NewBuffer(nil)
	w, err := t.table.schema.GetWriter(b, buf.DynamicColumns())
	if err != nil {
		return fmt.Errorf("failed to get writer: %w", err)
	}
	defer t.table.schema.PutWriter(w)

	list := make([]*parts.Part, 0)
	for granule, indices := range rowsToInsertPerGranule {
		select {
		case <-ctx.Done():
			parts.Tombstone(list)
			return ctx.Err()
		default:

			for idx := range dynRows.Rows {
				// Check if this index belongs in this granule
				if _, ok := indices[idx]; !ok {
					continue
				}

				if _, err = w.WriteRows(dynRows.Rows[idx : idx+1]); err != nil {
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

			part := parts.NewPart(tx, serBuf)
			if granule == nil { // insert new granule with part
				g, err := NewGranule(t.table.schema, part)
				if err != nil {
					return fmt.Errorf("failed to create granule: %w", err)
				}
				t.table.metrics.granulesCreated.Inc()

				if err := t.updateIndex(func(index *btree.BTree) error {
					index.ReplaceOrInsert(g)
					return nil
				}); err != nil {
					return err
				}
				t.table.metrics.numParts.Add(float64(1))
			} else {
				if _, err := granule.Append(part); err != nil {
					return fmt.Errorf("failed to add part to granule: %w", err)
				}
			}
			list = append(list, part)
			t.size.Add(serBuf.ParquetFile().Size())

			b = bytes.NewBuffer(nil)
			w.Reset(b)
		}
	}

	t.table.metrics.numParts.Add(float64(len(list)))
	return nil
}

// updateIndex updates that TableBlock's index in a thread safe manner to the result of calling the update fn with the current index.
func (t *TableBlock) updateIndex(update func(index *btree.BTree) error) error {
	var replaceErr error
	for ok := false; !ok; {
		ok = func() bool {
			old := t.Index()
			t.mtx.Lock()
			newIndex := old.Clone()
			t.mtx.Unlock()

			if err := update(newIndex); err != nil {
				replaceErr = err
				return true
			}

			return t.index.CompareAndSwap(old, newIndex)
		}()
	}
	return replaceErr
}

// RowGroupIterator iterates in order over all granules in the table.
// It stops iterating when the iterator function returns false.
func (t *TableBlock) RowGroupIterator(
	ctx context.Context,
	tx uint64,
	filter TrueNegativeFilter,
	rowGroups chan<- any,
) error {
	ctx, span := t.tracer.Start(ctx, "TableBlock/RowGroupIterator")
	span.SetAttributes(attribute.Int64("tx", int64(tx))) // Attributes don't support uint64...
	defer span.End()

	index := t.Index()

	var err error
	index.Ascend(func(i btree.Item) bool {
		g := i.(*Granule)
		g.Collect(ctx, tx, filter, rowGroups)
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

func (t *TableBlock) insertRecordToGranules(tx uint64, record arrow.Record) error {
	ps := t.table.schema
	numRows := record.NumRows()
	idx := numRows - 1
	pooledSchema, err := ps.GetDynamicParquetSchema(pqarrow.RecordDynamicCols(record))
	if err != nil {
		return err
	}
	defer ps.PutPooledParquetSchema(pooledSchema)
	row, err := pqarrow.RecordToDynamicRow(ps, pooledSchema.Schema, record, int(idx))
	if err != nil {
		if err == io.EOF {
			level.Debug(t.logger).Log("msg", "inserted record with no rows")
			return nil
		}
		return err
	}

	// recordSizePerRow is the rough estimate of the size of each row in the record.
	recordSizePerRow := int(arrowutils.RecordSize(record) / int64(numRows))

	var ascendErr error
	t.Index().DescendLessOrEqual(
		btreeComparableDynamicRow{
			schema:     ps,
			DynamicRow: row,
		},
		func(i btree.Item) bool {
			g := i.(*Granule)
			// Descend rows to insert until we find a row that does not belong in this granule.
			n := 0
			for ; idx >= 0; idx-- {
				row, err := pqarrow.RecordToDynamicRow(ps, pooledSchema.Schema, record, int(idx))
				if err != nil {
					ascendErr = err
					return false
				}
				if ps.RowLessThan(row, g.Least()) {
					if n > 0 {
						if _, err := g.Append(parts.NewArrowPart(tx, record.NewSlice(idx+1, idx+1+int64(n)), recordSizePerRow*n, ps)); err != nil {
							ascendErr = err
							return false
						}
						t.table.metrics.numParts.Add(float64(1))
					}
					// Go on to the next granule.
					return true
				}
				n++
			}
			// If we got here, all rows were exhaused in the loop above.
			if n > 0 {
				if _, err := g.Append(parts.NewArrowPart(tx, record.NewSlice(idx+1, idx+1+int64(n)), recordSizePerRow*n, ps)); err != nil {
					ascendErr = err
					return false
				}
				t.table.metrics.numParts.Add(float64(1))
			}
			return false
		},
	)
	if ascendErr != nil {
		return err
	}

	if idx < 0 {
		// All rows exhausted.
		t.size.Add(arrowutils.RecordSize(record))
		return nil
	}

	g, err := NewGranule(ps, parts.NewArrowPart(tx, record.NewSlice(0, idx+1), recordSizePerRow*int(idx+1), ps))
	if err != nil {
		return fmt.Errorf("new granule failed: %w", err)
	}

	if err := t.updateIndex(func(index *btree.BTree) error {
		index.ReplaceOrInsert(g)
		return nil
	}); err != nil {
		return err
	}
	t.table.metrics.granulesCreated.Inc()
	t.table.metrics.numParts.Add(float64(1))
	t.size.Add(arrowutils.RecordSize(record))
	return nil
}

type btreeComparableDynamicRow struct {
	schema *dynparquet.Schema
	*dynparquet.DynamicRow
}

func (r btreeComparableDynamicRow) Less(than btree.Item) bool {
	return r.schema.RowLessThan(r.DynamicRow, than.(*Granule).Least())
}

func (t *TableBlock) splitRowsByGranule(parquetRows *dynparquet.DynamicRows) (map[*Granule]map[int]struct{}, error) {
	index := t.Index()
	if index.Len() == 0 {
		rows := map[int]struct{}{}
		for i := 0; i < len(parquetRows.Rows); i++ {
			rows[i] = struct{}{}
		}

		return map[*Granule]map[int]struct{}{
			nil: rows, // NOTE: nil pointer to a granule indicates a new granule must be greated for insertion
		}, nil
	}

	var (
		rowsByGranule = map[*Granule]map[int]struct{}{}
		idx           = len(parquetRows.Rows) - 1
	)

	// Imagine our index looks like (in sorted order):
	// [a, c) [c, h) [h, inf)
	// Note that the "end" range bounds are implicit and defined by the least
	// row of the next granule.
	// If we insert 2 rows, [d, k], the DescendLessOrEqual call will return
	// granule [h, inf) as a starting point for our descent. The rows to insert
	// are iterated in reverse order until a row is found that does not belong
	// to the current granule (e.g. d, since it's less than h). At this point,
	// the iteration is continued to granule [c, h) into which d is inserted.
	index.DescendLessOrEqual(
		btreeComparableDynamicRow{
			schema:     t.table.schema,
			DynamicRow: parquetRows.Get(idx),
		},
		func(i btree.Item) bool {
			g := i.(*Granule)
			// Descend the rows to insert until we find a row that does not belong
			// in this granule.
			for ; idx >= 0; idx-- {
				if t.table.schema.RowLessThan(parquetRows.Get(idx), g.Least()) {
					// Go on to the next granule.
					return true
				}
				if _, ok := rowsByGranule[g]; !ok {
					rowsByGranule[g] = map[int]struct{}{}
				}
				rowsByGranule[g][idx] = struct{}{}
			}
			// If we got here, all rows were exhausted in the loop above.
			return false
		})

	if idx < 0 {
		// All rows exhausted.
		return rowsByGranule, nil
	}

	if _, ok := rowsByGranule[nil]; ok {
		return nil, errors.New(
			"unexpectedly found rows that do not belong to any granule before exhausting search",
		)
	}

	// Add remaining rows to a new granule.
	rowsByGranule[nil] = map[int]struct{}{}
	for ; idx >= 0; idx-- {
		rowsByGranule[nil][idx] = struct{}{}
	}

	return rowsByGranule, nil
}

// addPartToGranule finds the corresponding granule it belongs to in a sorted list of Granules.
func addPartToGranule(granules []*Granule, p *parts.Part) error {
	row, err := p.Least()
	if err != nil {
		return err
	}

	var prev *Granule
	for _, g := range granules {
		if g.schema.RowLessThan(row, g.Least()) {
			if prev != nil {
				if _, err := prev.Append(p); err != nil {
					return err
				}
				return nil
			}
		} else {
			prev = g
		}
	}

	if prev != nil {
		// Save part to prev
		if _, err := prev.Append(p); err != nil {
			return err
		}
	} else {
		// NOTE: this should never happen
		panic("programming error; unable to find granule for part")
	}

	return nil
}

// abortCompaction resets state set on compaction so that a granule may be
// compacted again.
func (t *TableBlock) abortCompaction(granule *Granule) {
	t.table.metrics.granulesCompactionAborted.Inc()
	for {
		// Unmark pruned, so that we can compact the granule in the future.
		if granule.metadata.pruned.CompareAndSwap(1, 0) {
			return
		}
	}
}

// Serialize the table block into a single Parquet file.
// The Serialize function will walk all Granules in the block, compact each Granule into a sorted set of Row groups, then write those
// row groups to the final Parquet file, repeating for each Granule. This leverages the fact that the index has alreayd sorted the Granules
// so no need to sort them further than the intra-granule parts.
func (t *TableBlock) Serialize(writer io.Writer) error {
	var ascendErr error
	dynCols := map[string][]string{} // NOTE: it would be nice if we didn't have to go back and find all the dynamic columns...
	index := t.Index()
	index.Ascend(func(i btree.Item) bool {
		g := i.(*Granule)

		g.PartsForTx(math.MaxUint64, func(p *parts.Part) bool {
			buf, err := p.AsSerializedBuffer(t.table.schema)
			if err != nil {
				ascendErr = err
				return false
			}

			for key, vals := range buf.DynamicColumns() {
				dynCols[key] = append(dynCols[key], vals...)
			}
			return true
		})

		return true
	})
	if ascendErr != nil {
		return ascendErr
	}

	dynCols = bufutils.Dedupe(dynCols)
	rowWriter, err := t.rowWriter(writer, dynCols)
	if err != nil {
		return fmt.Errorf("create row writer: %w", err)
	}
	defer rowWriter.close()

	cols := t.table.schema.ParquetSortingColumns(dynCols)
	pooledSchema, err := t.table.schema.GetDynamicParquetSchema(dynCols)
	if err != nil {
		return err
	}
	defer t.table.schema.PutPooledParquetSchema(pooledSchema)
	ps := pooledSchema.Schema

	// Merge all parts in a Granule and write that granule to the file
	index.Ascend(func(i btree.Item) bool {
		g := i.(*Granule)

		// collect parts based on compaction level; we do this because level1 parts are already non-overlapping.
		lvl0, lvl1, _, stats, err := collectPartsForCompaction(math.MaxUint64, g.parts)
		if err != nil {
			ascendErr = err
			return false
		}

		// Compact the level 0 parts
		if len(lvl0) > 0 {
			lvl1, err = compactLevel0IntoLevel1(t, math.MaxUint64, lvl0, lvl1, 1.0, &stats)
			if err != nil {
				ascendErr = err
				return false
			}
		}

		sorter := parts.NewPartSorter(t.table.schema, lvl1)
		sort.Sort(sorter)
		if err := sorter.Err(); err != nil {
			ascendErr = err
			return false
		}

		// Write the sorted parts
		for _, p := range lvl1 {
			buf, err := p.AsSerializedBuffer(t.table.schema)
			if err != nil {
				ascendErr = err
				return false
			}

			// Use the dynamic row group merge adapter for each row group to ensure the missing dynamic columns are written
			for i := 0; i < buf.NumRowGroups(); i++ {
				rows := dynparquet.NewDynamicRowGroupMergeAdapter(ps, cols, dynCols, buf.DynamicRowGroup(i)).Rows()
				defer rows.Close()

				// Write the merged row groups to the writer
				if _, err := rowWriter.writeRows(rows); err != nil {
					ascendErr = err
					return false
				}
			}
		}

		return true
	})
	if ascendErr != nil {
		return ascendErr
	}

	return nil
}

// parquetRowWriter is a stateful parquet row group writer.
type parquetRowWriter struct {
	schema *dynparquet.Schema
	w      *dynparquet.PooledWriter

	rowGroupSize int
	maxNumRows   int

	rowGroupRowsWritten int
	totalRowsWritten    int
	rowsBuf             []parquet.Row
}

type parquetRowWriterOption func(p *parquetRowWriter)

func withMaxRows(max int) parquetRowWriterOption {
	return func(p *parquetRowWriter) {
		p.maxNumRows = max
	}
}

// rowWriter returns a new Parquet row writer with the given dynamic columns.
func (t *TableBlock) rowWriter(writer io.Writer, dynCols map[string][]string, options ...parquetRowWriterOption) (*parquetRowWriter, error) {
	w, err := t.table.schema.GetWriter(writer, dynCols)
	if err != nil {
		return nil, err
	}

	buffSize := 256
	if t.table.config.RowGroupSize > 0 {
		buffSize = int(t.table.config.RowGroupSize)
	}

	p := &parquetRowWriter{
		w:            w,
		schema:       t.table.schema,
		rowsBuf:      make([]parquet.Row, buffSize),
		rowGroupSize: int(t.table.config.RowGroupSize),
	}

	for _, option := range options {
		option(p)
	}

	return p, nil
}

// WriteRows will write the given rows to the underlying Parquet writer. It returns the number of rows written.
func (p *parquetRowWriter) writeRows(rows parquet.Rows) (int, error) {
	written := 0
	for p.maxNumRows == 0 || p.totalRowsWritten < p.maxNumRows {
		if p.rowGroupSize > 0 && p.rowGroupRowsWritten+len(p.rowsBuf) > p.rowGroupSize {
			// Read only as many rows as we need to complete the row group size limit.
			p.rowsBuf = p.rowsBuf[:p.rowGroupSize-p.rowGroupRowsWritten]
		}
		if p.maxNumRows != 0 && p.totalRowsWritten+len(p.rowsBuf) > p.maxNumRows {
			// Read only as many rows as we need to write if they would bring
			// us over the limit.
			p.rowsBuf = p.rowsBuf[:p.maxNumRows-p.totalRowsWritten]
		}
		n, err := rows.ReadRows(p.rowsBuf)
		if err != nil && err != io.EOF {
			return 0, err
		}
		if n == 0 {
			break
		}

		if _, err = p.w.WriteRows(p.rowsBuf[:n]); err != nil {
			return 0, err
		}
		written += n
		p.rowGroupRowsWritten += n
		p.totalRowsWritten += n
		if p.rowGroupSize > 0 && p.rowGroupRowsWritten >= p.rowGroupSize {
			if err := p.w.Flush(); err != nil {
				return 0, err
			}
			p.rowGroupRowsWritten = 0
		}
	}

	return written, nil
}

func (p *parquetRowWriter) close() error {
	defer p.schema.PutWriter(p.w)
	return p.w.Close()
}

// memoryBlocks collects the active and pending blocks that are currently resident in memory.
// The pendingReadersWg.Done() function must be called on all blocks returned once processing is finished.
func (t *Table) memoryBlocks() ([]*TableBlock, uint64) {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	if t.active == nil { // this is currently a read only table
		return nil, 0
	}

	lastReadBlockTimestamp := t.active.ulid.Time()
	t.active.pendingReadersWg.Add(1)
	memoryBlocks := []*TableBlock{t.active}
	for block := range t.pendingBlocks {
		block.pendingReadersWg.Add(1)
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
	rowGroups chan<- any,
) error {
	ctx, span := t.tracer.Start(ctx, "Table/collectRowGroups")
	defer span.End()

	filter, err := BooleanExpr(filterExpr)
	if err != nil {
		return err
	}

	// pending blocks could be uploaded to the bucket while we iterate on them.
	// to avoid to iterate on them again while reading the block file
	// we keep the last block timestamp to be read from the bucket and pass it to the IterateBucketBlocks() function
	// so that every block with a timestamp >= lastReadBlockTimestamp is discarded while being read.
	memoryBlocks, lastBlockTimestamp := t.memoryBlocks()
	defer func() {
		for _, block := range memoryBlocks {
			block.pendingReadersWg.Done()
		}
	}()
	for _, block := range memoryBlocks {
		span.AddEvent("memoryBlock")
		if err := block.RowGroupIterator(ctx, tx, filter, rowGroups); err != nil {
			return err
		}
	}

	// Collect from all other data sources.
	for _, source := range t.db.sources {
		span.AddEvent(fmt.Sprintf("source/%s", source.String()))
		if err := source.Scan(ctx, filepath.Join(t.db.name, t.name), t.schema, filterExpr, lastBlockTimestamp, func(ctx context.Context, v any) error {
			rowGroups <- v
			return nil
		}); err != nil {
			return err
		}
	}

	return nil
}

// close notifies a table to stop accepting writes.
func (t *Table) close() {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	t.active.pendingWritersWg.Wait()
	t.closing = true
}
