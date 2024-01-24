package frostdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/arrow/util"
	"github.com/dustin/go-humanize"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/polarsignals/frostdb/dynparquet"
	schemapb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/schema/v1alpha1"
	schemav2pb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/schema/v1alpha2"
	tablepb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/table/v1alpha1"
	walpb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/wal/v1alpha1"
	"github.com/polarsignals/frostdb/index"
	"github.com/polarsignals/frostdb/internal/records"
	"github.com/polarsignals/frostdb/parts"
	"github.com/polarsignals/frostdb/pqarrow"
	"github.com/polarsignals/frostdb/query/logicalplan"
	"github.com/polarsignals/frostdb/query/physicalplan"
	"github.com/polarsignals/frostdb/recovery"
	"github.com/polarsignals/frostdb/wal"
	walpkg "github.com/polarsignals/frostdb/wal"
)

var (
	ErrNoSchema     = fmt.Errorf("no schema")
	ErrTableClosing = fmt.Errorf("table closing")
)

// DefaultIndexConfig returns the default level configs used. This is a function
// So that any modifications to the result will not affect the default config.
func DefaultIndexConfig() []*index.LevelConfig {
	return []*index.LevelConfig{
		{Level: index.L0, MaxSize: 1024 * 1024 * 15, Type: index.CompactionTypeParquetMemory},  // Compact to in-memory Parquet buffer after 15MiB of data
		{Level: index.L1, MaxSize: 1024 * 1024 * 128, Type: index.CompactionTypeParquetMemory}, // Compact to a single in-memory Parquet buffer after 128MiB of Parquet files
		{Level: index.L2, MaxSize: 1024 * 1024 * 512},                                          // Final level. Rotate after 512MiB of Parquet files
	}
}

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

func WithUniquePrimaryIndex(unique bool) TableOption {
	return func(config *tablepb.TableConfig) error {
		switch e := config.Schema.(type) {
		case *tablepb.TableConfig_DeprecatedSchema:
			e.DeprecatedSchema.UniquePrimaryIndex = unique
		case *tablepb.TableConfig_SchemaV2:
			e.SchemaV2.UniquePrimaryIndex = unique
		}
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

// GenericTable is a wrapper around *Table that writes structs of type T. It
// consist of  a generic arrow.Record builder that ingests structs of type T.
// The generated record is then  passed to (*Table).InsertRecord.
//
// Struct tag `frostdb` is used to pass options for the schema for T.
//
// This api is opinionated.
//
//   - Nested Columns are not supported
//
// # Tags
//
// Use `frostdb` to define tags that customizes field values. You can express
// everything needed to construct schema v1alpha1.
//
// Tags are defined as a comma separated list. The first item is the column
// name. Column name is optional, when omitted it is derived from the field name
// (snake_cased)
//
// Supported Tags
//
//	    delta_binary_packed | Delta binary packed encoding.
//	                 brotli | Brotli compression.
//	                    asc | Sorts in ascending order.Use asc(n) where n is an integer for sorting order.
//	                   gzip | GZIP compression.
//	                 snappy | Snappy compression.
//	delta_length_byte_array | Delta Length Byte Array encoding.
//	       delta_byte_array | Delta Byte Array encoding.
//	                   desc | Sorts in descending order.Use desc(n) where n is an integer for sorting order
//	                lz4_raw | LZ4_RAW compression.
//	               pre_hash | Prehash the column before storing it.
//	             null_first | When used wit asc nulls are smallest and with des nulls are largest.
//	                   zstd | ZSTD compression.
//	               rle_dict | Dictionary run-length encoding.
//	                  plain | Plain encoding.
//
// Example tagged Sample struct
//
//	type Sample struct {
//		ExampleType string      `frostdb:"example_type,rle_dict,asc(0)"`
//		Labels      []Label     `frostdb:"labels,rle_dict,null,dyn,asc(1),null_first"`
//		Stacktrace  []uuid.UUID `frostdb:"stacktrace,rle_dict,asc(3),null_first"`
//		Timestamp   int64       `frostdb:"timestamp,asc(2)"`
//		Value       int64       `frostdb:"value"`
//	}
//
// # Dynamic columns
//
// Field of type map<string, T> is a dynamic column by default.
//
//	type Example struct {
//		// Use supported tags to customize the column value
//		Labels map[string]string `frostdb:"labels"`
//	}
//
// # Repeated columns
//
// Fields of type []int64, []float64, []bool, and []string are supported. These
// are represented as arrow.LIST.
//
// Generated schema for the repeated columns applies all supported tags. By
// default repeated fields are nullable. You can safely pass nil slices for
// repeated columns.
type GenericTable[T any] struct {
	*Table
	mu    sync.Mutex
	build *records.Build[T]
}

func (t *GenericTable[T]) Release() {
	t.build.Release()
}

// Write builds arrow.Record directly from values and calls (*Table).InsertRecord.
func (t *GenericTable[T]) Write(ctx context.Context, values ...T) (uint64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	err := t.build.Append(values...)
	if err != nil {
		return 0, err
	}
	return t.InsertRecord(ctx, t.build.NewRecord())
}

func NewGenericTable[T any](db *DB, name string, mem memory.Allocator, options ...TableOption) (*GenericTable[T], error) {
	build := records.NewBuild[T](mem)
	table, err := db.Table(name, NewTableConfig(build.Schema(name), options...))
	if err != nil {
		return nil, err
	}
	return &GenericTable[T]{build: build, Table: table}, nil
}

type Table struct {
	db      *DB
	name    string
	metrics *tableMetrics
	logger  log.Logger
	tracer  trace.Tracer

	config atomic.Pointer[tablepb.TableConfig]
	schema *dynparquet.Schema

	pendingBlocks   map[*TableBlock]struct{}
	completedBlocks []completedBlock
	lastCompleted   uint64

	mtx    *sync.RWMutex
	active *TableBlock

	wal     WAL
	closing bool
}

type Sync interface {
	Sync() error
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
	Reset(nextTx uint64) error
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

	// uncompressedInsertsSize keeps track of the cumulative L0 size. This is
	// not the size of the block, since these inserts are eventually compressed.
	// However, it serves to determine when to perform a snapshot, since these
	// uncompressed inserts are stored in the WAL, and if the node crashes, it
	// is obliged to re-read all of these uncompressed inserts into memory,
	// potentially causing OOMs.
	uncompressedInsertsSize atomic.Int64
	// lastSnapshotSize keeps track of the uncompressedInsertsSize when a
	// snapshot was last triggered.
	lastSnapshotSize atomic.Int64

	index *index.LSM

	pendingWritersWg sync.WaitGroup
	pendingReadersWg sync.WaitGroup

	mtx *sync.RWMutex
}

type Closer interface {
	Close(cleanup bool) error
}

type tableMetrics struct {
	blockPersisted       prometheus.Counter
	blockRotated         prometheus.Counter
	rowsInserted         prometheus.Counter
	rowBytesInserted     prometheus.Counter
	zeroRowsInserted     prometheus.Counter
	rowInsertSize        prometheus.Histogram
	lastCompletedBlockTx prometheus.Gauge
	numParts             prometheus.Gauge

	indexMetrics *index.LSMMetrics
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
			blockPersisted: promauto.With(reg).NewCounter(prometheus.CounterOpts{
				Name: "frostdb_table_blocks_persisted_total",
				Help: "Number of table blocks that have been persisted.",
			}),
			blockRotated: promauto.With(reg).NewCounter(prometheus.CounterOpts{
				Name: "frostdb_table_blocks_rotated_total",
				Help: "Number of table blocks that have been rotated.",
			}),
			rowsInserted: promauto.With(reg).NewCounter(prometheus.CounterOpts{
				Name: "frostdb_table_rows_inserted_total",
				Help: "Number of rows inserted into table.",
			}),
			rowBytesInserted: promauto.With(reg).NewCounter(prometheus.CounterOpts{
				Name: "frostdb_table_row_bytes_inserted_total",
				Help: "Number of bytes inserted into table.",
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
			indexMetrics: index.NewLSMMetrics(reg),
		},
	}

	// Store the table config
	t.config.Store(tableConfig)

	// Disable the WAL for this table by replacing any given WAL with a nop wal
	if tableConfig.DisableWal {
		t.wal = &walpkg.NopWAL{}
	}

	t.pendingBlocks = make(map[*TableBlock]struct{})

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
					Config:    t.config.Load(),
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

	// Wait for outstanding readers/writers to finish with the block before releasing underlying resources.
	block.pendingReadersWg.Wait()
	block.pendingWritersWg.Wait()

	if err := block.index.Close(); err != nil {
		level.Error(t.logger).Log("msg", "failed to close index", "err", err)
	}
}

func (t *Table) writeBlock(block *TableBlock, skipPersist, snapshotDB bool) {
	level.Debug(t.logger).Log("msg", "syncing block")
	block.pendingWritersWg.Wait()

	// from now on, the block will no longer be modified, we can persist it to disk

	level.Debug(t.logger).Log("msg", "done syncing block")

	// Persist the block
	var err error
	if !skipPersist {
		err = block.Persist()
	}
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
			if err := t.db.snapshotAtTX(ctx, tx, t.db.snapshotWriter(tx)); err != nil {
				level.Error(t.logger).Log(
					"msg", "failed to write snapshot on block rotation",
					"err", err,
				)
			}
			if err := t.db.reclaimDiskSpace(ctx, nil); err != nil {
				level.Error(t.logger).Log(
					"msg", "failed to reclaim disk space after snapshot on block rotation",
					"err", err,
				)
				return
			}
		}()
	}
}

func (t *Table) RotateBlock(_ context.Context, block *TableBlock, skipPersist bool) error {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	// Need to check that we haven't already rotated this block.
	if t.active != block {
		return nil
	}

	level.Debug(t.logger).Log("msg", "rotating block", "blockSize", block.Size(), "skipPersist", skipPersist)
	defer func() {
		level.Debug(t.logger).Log("msg", "done rotating block")
	}()

	tx, _, commit := t.db.begin()
	defer commit()

	id := generateULID()
	for id.Time() == block.ulid.Time() { // Ensure the new block has a different timestamp.
		runtime.Gosched()
		id = generateULID()
	}
	if err := t.newTableBlock(t.active.minTx, tx, id); err != nil {
		return err
	}
	t.metrics.blockRotated.Inc()
	t.metrics.numParts.Set(float64(0))

	t.pendingBlocks[block] = struct{}{}
	// We don't check t.db.columnStore.manualBlockRotation here because this is
	// the entry point for users to trigger a manual block rotation and they
	// will specify through skipPersist if they want the block to be persisted.
	go t.writeBlock(block, skipPersist, true)

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
	if t.config.Load() == nil {
		return nil
	}
	return t.schema
}

func (t *Table) EnsureCompaction() error {
	return t.ActiveBlock().EnsureCompaction()
}

func (t *Table) InsertRecord(ctx context.Context, record arrow.Record) (uint64, error) {
	block, finish, err := t.appender(ctx)
	if err != nil {
		return 0, fmt.Errorf("get appender: %w", err)
	}
	defer finish()

	tx, _, commit := t.db.begin()
	defer commit()

	preHashedRecord := dynparquet.PrehashColumns(t.schema, record)
	defer preHashedRecord.Release()

	if err := t.wal.LogRecord(tx, t.name, preHashedRecord); err != nil {
		return tx, fmt.Errorf("append to log: %w", err)
	}

	if err := block.InsertRecord(ctx, tx, preHashedRecord); err != nil {
		return tx, fmt.Errorf("insert buffer into block: %w", err)
	}

	return tx, nil
}

func (t *Table) appender(ctx context.Context) (*TableBlock, func(), error) {
	for {
		// Using active write block is important because it ensures that we don't
		// miss pending writers when synchronizing the block.
		block, finish, err := t.ActiveWriteBlock()
		if err != nil {
			return nil, nil, err
		}

		uncompressedInsertsSize := block.uncompressedInsertsSize.Load()
		if t.db.columnStore.snapshotTriggerSize != 0 &&
			// If size-lastSnapshotSize > snapshotTriggerSize (a column store
			// option), a new snapshot is triggered. This is basically the size
			// of the new data in this block since the last snapshot.
			uncompressedInsertsSize-block.lastSnapshotSize.Load() > t.db.columnStore.snapshotTriggerSize {
			// context.Background is used here for the snapshot since callers
			// might cancel the context when the write is finished but the
			// snapshot is not.
			// TODO(asubiotto): Eventually we should register a cancel function
			// that is called with a grace period on db.Close.
			t.db.asyncSnapshot(context.Background(), func() {
				level.Debug(t.logger).Log(
					"msg", "successful snapshot on block size trigger",
					"block_size", humanize.IBytes(uint64(uncompressedInsertsSize)),
					"last_snapshot_size", humanize.IBytes(uint64(block.lastSnapshotSize.Load())),
				)
				block.lastSnapshotSize.Store(uncompressedInsertsSize)
				if err := t.db.reclaimDiskSpace(context.Background(), nil); err != nil {
					level.Error(t.logger).Log(
						"msg", "failed to reclaim disk space after snapshot",
						"err", err,
					)
					return
				}
			})
		}
		blockSize := block.Size()
		if blockSize < t.db.columnStore.activeMemorySize || t.db.columnStore.manualBlockRotation {
			return block, finish, nil
		}

		// We need to rotate the block and the writer won't actually be used.
		finish()

		err = t.RotateBlock(ctx, block, false)
		if err != nil {
			return nil, nil, fmt.Errorf("rotate block: %w", err)
		}
	}
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
		errg.Go(recovery.Do(func() error {
			converter := pqarrow.NewParquetConverter(pool, *iterOpts)
			defer converter.Close()

			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case rg, ok := <-rowGroups:
					if !ok {
						r := converter.NewRecord()
						if r == nil {
							return nil
						}
						defer r.Release()
						if r.NumRows() == 0 {
							return nil
						}
						return callback(ctx, r)
					}

					switch t := rg.(type) {
					case arrow.Record:
						defer t.Release()
						r := pqarrow.Project(t, iterOpts.PhysicalProjection)
						defer r.Release()
						err := callback(ctx, r)
						if err != nil {
							return err
						}
					case index.ReleaseableRowGroup:
						defer t.Release()
						if err := converter.Convert(ctx, t); err != nil {
							return fmt.Errorf("failed to convert row group to arrow record: %v", err)
						}
						// This RowGroup had no relevant data. Ignore it.
						if len(converter.Fields()) == 0 {
							continue
						}
						if converter.NumRows() >= bufferSize {
							err := func() error {
								r := converter.NewRecord()
								defer r.Release()
								converter.Reset() // Reset the converter to drop any dictionaries that were built.
								return callback(ctx, r)
							}()
							if err != nil {
								return err
							}
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
							err := func() error {
								r := converter.NewRecord()
								defer r.Release()
								converter.Reset() // Reset the converter to drop any dictionaries that were built.
								return callback(ctx, r)
							}()
							if err != nil {
								return err
							}
						}
					default:
						return fmt.Errorf("unknown row group type: %T", t)
					}
				}
			}
		}))
	}

	errg.Go(func() error {
		if err := t.collectRowGroups(ctx, tx, iterOpts.Filter, iterOpts.InMemoryOnly, rowGroups); err != nil {
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
		errg.Go(recovery.Do(func() error {
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
						for i := 0; i < t.Schema().NumFields(); i++ {
							b.Field(0).(*array.StringBuilder).Append(t.Schema().Field(i).Name)
						}
						record := b.NewRecord()
						err := callback(ctx, record)
						record.Release()
						t.Release()
						if err != nil {
							return err
						}
					case index.ReleaseableRowGroup:
						if rg == nil {
							return errors.New("received nil rowGroup") // shouldn't happen, but anyway
						}
						defer t.Release()
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
		}))
	}

	errg.Go(func() error {
		if err := t.collectRowGroups(ctx, tx, iterOpts.Filter, iterOpts.InMemoryOnly, rowGroups); err != nil {
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
	tb := &TableBlock{
		table:  table,
		mtx:    &sync.RWMutex{},
		ulid:   id,
		logger: table.logger,
		tracer: table.tracer,
		minTx:  tx,
		prevTx: prevTx,
	}

	var err error
	tb.index, err = index.NewLSM(
		filepath.Join(table.db.indexDir(), table.name, id.String()), // Any index files are found at <db.indexDir>/<table.name>/<block.id>
		table.schema,
		table.IndexConfig(),
		table.db.Wait,
		index.LSMWithMetrics(table.metrics.indexMetrics),
		index.LSMWithLogger(table.logger),
	)
	if err != nil {
		return nil, fmt.Errorf("new LSM: %w", err)
	}

	return tb, nil
}

// EnsureCompaction forces a TableBlock compaction.
func (t *TableBlock) EnsureCompaction() error {
	return t.index.EnsureCompaction()
}

func (t *TableBlock) InsertRecord(_ context.Context, tx uint64, record arrow.Record) error {
	recordSize := util.TotalRecordSize(record)
	defer func() {
		t.table.metrics.rowsInserted.Add(float64(record.NumRows()))
		t.table.metrics.rowInsertSize.Observe(float64(record.NumRows()))
		t.table.metrics.rowBytesInserted.Add(float64(recordSize))
	}()

	if record.NumRows() == 0 {
		t.table.metrics.zeroRowsInserted.Add(1)
		return nil
	}

	t.index.Add(tx, record)
	t.table.metrics.numParts.Inc()
	t.uncompressedInsertsSize.Add(recordSize)
	return nil
}

// Size returns the cumulative size of all buffers in the table. This is roughly the size of the table in bytes.
func (t *TableBlock) Size() int64 {
	return t.index.Size()
}

// Index provides atomic access to the table index.
func (t *TableBlock) Index() *index.LSM {
	return t.index
}

// Serialize the table block into a single Parquet file.
func (t *TableBlock) Serialize(writer io.Writer) error {
	return t.index.Rotate(t.table.externalParquetCompaction(writer))
}

type ParquetWriter interface {
	Flush() error
	WriteRows([]parquet.Row) (int, error)
	io.Closer
}

// parquetRowWriter is a stateful parquet row group writer.
type parquetRowWriter struct {
	schema *dynparquet.Schema
	w      ParquetWriter

	rowGroupSize int
	maxNumRows   int

	rowGroupRowsWritten int
	totalRowsWritten    int
	rowsBuf             []parquet.Row
}

type parquetRowWriterOption func(p *parquetRowWriter)

// rowWriter returns a new Parquet row writer with the given dynamic columns.
// TODO(asubiotto): Can we delete this parquetRowWriter?
func (t *TableBlock) rowWriter(w ParquetWriter, options ...parquetRowWriterOption) (*parquetRowWriter, error) {
	buffSize := 256
	config := t.table.config.Load()
	if config.RowGroupSize > 0 {
		buffSize = int(config.RowGroupSize)
	}

	p := &parquetRowWriter{
		w:            w,
		schema:       t.table.schema,
		rowsBuf:      make([]parquet.Row, buffSize),
		rowGroupSize: int(config.RowGroupSize),
	}

	for _, option := range options {
		option(p)
	}

	return p, nil
}

// WriteRows will write the given rows to the underlying Parquet writer. It returns the number of rows written.
func (p *parquetRowWriter) writeRows(rows parquet.RowReader) (int, error) {
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
	skipSources bool,
	rowGroups chan<- any,
) error {
	ctx, span := t.tracer.Start(ctx, "Table/collectRowGroups")
	defer span.End()

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
		if err := block.index.Scan(ctx, "", t.schema, filterExpr, tx, func(ctx context.Context, v any) error {
			select {
			case rowGroups <- v:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}); err != nil {
			return err
		}
	}

	if skipSources {
		return nil
	}

	// Collect from all other data sources.
	for _, source := range t.db.sources {
		span.AddEvent(fmt.Sprintf("source/%s", source.String()))
		if err := source.Scan(ctx, filepath.Join(t.db.name, t.name), t.schema, filterExpr, lastBlockTimestamp, func(ctx context.Context, v any) error {
			select {
			case rowGroups <- v:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
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
	t.active.index.WaitForPendingCompactions()
}

func (t *Table) externalParquetCompaction(writer io.Writer) func(compact []parts.Part) (parts.Part, int64, int64, error) {
	return func(compact []parts.Part) (parts.Part, int64, int64, error) {
		size, err := t.compactParts(writer, compact)
		if err != nil {
			return nil, 0, 0, err
		}

		return nil, size, 0, nil
	}
}

// compactParts will compact the given parts into a Parquet file written to w.
// It returns the size in bytes of the compacted parts.
func (t *Table) compactParts(w io.Writer, compact []parts.Part, options ...parquet.WriterOption) (int64, error) {
	if len(compact) == 0 {
		return 0, nil
	}

	preCompactionSize := int64(0)
	for _, p := range compact {
		preCompactionSize += p.Size()
	}

	if t.schema.UniquePrimaryIndex {
		distinctRecords, err := t.distinctRecordsForCompaction(compact)
		if err != nil {
			return 0, err
		}
		if distinctRecords != nil {
			// Arrow distinction was successful.
			defer func() {
				for _, r := range distinctRecords {
					r.Release()
				}
			}()
			// Note that the records must be sorted (sortInput=true) because
			// there is no guarantee that order is maintained.
			return preCompactionSize, t.writeRecordsToParquet(w, distinctRecords, true)
		}
	}

	bufs, err := t.buffersForCompaction(w, compact)
	if err != nil {
		return 0, err
	}
	if bufs == nil {
		// Optimization.
		return preCompactionSize, nil
	}

	merged, err := t.schema.MergeDynamicRowGroups(bufs)
	if err != nil {
		return 0, err
	}
	err = func() error {
		var writer dynparquet.ParquetWriter
		if len(options) > 0 {
			writer, err = t.schema.NewWriter(w, merged.DynamicColumns(), false, options...)
			if err != nil {
				return err
			}
		} else {
			pw, err := t.schema.GetWriter(w, merged.DynamicColumns(), false)
			if err != nil {
				return err
			}
			defer t.schema.PutWriter(pw)
			writer = pw.ParquetWriter
		}
		p, err := t.active.rowWriter(writer)
		if err != nil {
			return err
		}
		defer p.close()

		rows := merged.Rows()
		defer rows.Close()

		var rowReader parquet.RowReader = rows
		if t.schema.UniquePrimaryIndex {
			// Given all inputs are sorted, we can deduplicate the rows using
			// DedupeRowReader, which deduplicates consecutive rows that are
			// equal on the sorting columns.
			rowReader = parquet.DedupeRowReader(rows, merged.Schema().Comparator(merged.SortingColumns()...))
		}

		if _, err := p.writeRows(rowReader); err != nil {
			return err
		}

		return nil
	}()
	if err != nil {
		return 0, err
	}

	return preCompactionSize, nil
}

// buffersForCompaction, given a slice of possibly overlapping parts, returns
// the minimum slice of dynamic row groups to be merged together for compaction.
// If nil, nil is returned, the resulting serialized buffer is written directly
// to w as an optimization.
func (t *Table) buffersForCompaction(w io.Writer, inputParts []parts.Part) ([]dynparquet.DynamicRowGroup, error) {
	nonOverlappingParts, overlappingParts, err := parts.FindMaximumNonOverlappingSet(t.schema, inputParts)
	if err != nil {
		return nil, err
	}
	result := make([]dynparquet.DynamicRowGroup, 0, len(inputParts))
	for _, p := range overlappingParts {
		buf, err := p.AsSerializedBuffer(t.schema)
		if err != nil {
			return nil, err
		}
		result = append(result, buf.MultiDynamicRowGroup())
	}
	if len(nonOverlappingParts) == 0 {
		return result, nil
	}

	allArrow := true
	for _, p := range nonOverlappingParts {
		if p.Record() == nil {
			allArrow = false
			break
		}
	}
	if len(nonOverlappingParts) == 1 || !allArrow {
		// Not worth doing anything if only one part does not overlap. If there
		// is at least one non-arrow part then optimizations cannot be made.
		nonOverlappingRowGroups := make([]dynparquet.DynamicRowGroup, 0, len(nonOverlappingParts))
		for _, p := range nonOverlappingParts {
			buf, err := p.AsSerializedBuffer(t.schema)
			if err != nil {
				return nil, err
			}
			nonOverlappingRowGroups = append(nonOverlappingRowGroups, buf.MultiDynamicRowGroup())
		}
		merged := nonOverlappingRowGroups[0]
		if len(nonOverlappingRowGroups) > 1 {
			// WithAlreadySorted ensures that a parquet.MultiRowGroup is created
			// here, which is much cheaper than actually merging all these row
			// groups.
			merged, err = t.schema.MergeDynamicRowGroups(nonOverlappingRowGroups, dynparquet.WithAlreadySorted())
			if err != nil {
				return nil, err
			}
		}
		result = append(result, merged)
		return result, nil
	}

	// All the non-overlapping parts are arrow records, and can therefore be
	// directly written to a parquet file. If there are no overlapping parts,
	// write directly to w.
	var b bytes.Buffer
	if len(overlappingParts) > 0 {
		w = &b
	}

	records := make([]arrow.Record, 0, len(nonOverlappingParts))
	for _, p := range nonOverlappingParts {
		records = append(records, p.Record())
	}

	if err := t.writeRecordsToParquet(w, records, false); err != nil {
		return nil, err
	}

	if len(overlappingParts) == 0 {
		// Result was written directly to w.
		return nil, nil
	}

	buf, err := dynparquet.ReaderFromBytes(b.Bytes())
	if err != nil {
		return nil, err
	}
	result = append(result, buf.MultiDynamicRowGroup())
	return result, nil
}

func (t *Table) writeRecordsToParquet(w io.Writer, records []arrow.Record, sortInput bool) error {
	dynColSets := make([]map[string][]string, 0, len(records))
	for _, r := range records {
		dynColSets = append(dynColSets, pqarrow.RecordDynamicCols(r))
	}
	dynCols := dynparquet.MergeDynamicColumnSets(dynColSets)
	pw, err := t.schema.GetWriter(w, dynCols, sortInput)
	if err != nil {
		return err
	}
	defer t.schema.PutWriter(pw)

	return pqarrow.RecordsToFile(t.schema, pw, records)
}

// distinctRecordsForCompaction performs a distinct on the given parts. If at
// least one non-arrow part is found, nil, nil is returned in which case, the
// caller should fall back to normal compaction. On success, the caller is
// responsible for releasing the returned records.
func (t *Table) distinctRecordsForCompaction(compact []parts.Part) ([]arrow.Record, error) {
	sortingCols := t.schema.SortingColumns()
	columnExprs := make([]logicalplan.Expr, 0, len(sortingCols))
	for _, col := range sortingCols {
		var expr logicalplan.Expr
		if col.Dynamic {
			expr = logicalplan.DynCol(col.Name)
		} else {
			expr = logicalplan.Col(col.Name)
		}
		columnExprs = append(columnExprs, expr)
	}

	d := physicalplan.Distinct(memory.NewGoAllocator(), t.tracer, columnExprs)
	output := physicalplan.OutputPlan{}
	newRecords := make([]arrow.Record, 0)
	output.SetNextCallback(func(ctx context.Context, r arrow.Record) error {
		r.Retain()
		newRecords = append(newRecords, r)
		return nil
	})
	d.SetNext(&output)

	if ok, err := func() (bool, error) {
		ctx := context.TODO()
		for _, p := range compact {
			if p.Record() == nil {
				// Caller should fall back to parquet distinction.
				return false, nil
			}
			if err := d.Callback(ctx, p.Record()); err != nil {
				return false, err
			}
		}
		if err := d.Finish(ctx); err != nil {
			return false, err
		}
		return true, nil
	}(); !ok || err != nil {
		for _, r := range newRecords {
			r.Release()
		}
		return nil, err
	}
	return newRecords, nil
}

// IndexConfig returns the index configuration for the table. It makes a copy of the column store index config and injects it's compactParts method.
func (t *Table) IndexConfig() []*index.LevelConfig {
	config := make([]*index.LevelConfig, 0, len(t.db.columnStore.indexConfig))
	for i, c := range t.db.columnStore.indexConfig {
		compactFunc := t.compactParts
		if i == len(t.db.columnStore.indexConfig)-1 {
			// The last level is the in-memory level, which is never compacted.
			compactFunc = nil
		}
		config = append(config, &index.LevelConfig{
			Level:   c.Level,
			MaxSize: c.MaxSize,
			Type:    c.Type,
			Compact: compactFunc, // TODO: this is bad and it should feel bad. We shouldn't need the table object to define how parts are compacted. Refactor needed.
		})
	}

	return config
}
