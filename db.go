package frostdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow/go/v14/arrow/ipc"
	"github.com/apache/arrow/go/v14/arrow/util"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/polarsignals/frostdb/dynparquet"
	tablepb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/table/v1alpha1"
	walpb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/wal/v1alpha1"
	"github.com/polarsignals/frostdb/index"
	"github.com/polarsignals/frostdb/parts"
	"github.com/polarsignals/frostdb/query/logicalplan"
	"github.com/polarsignals/frostdb/wal"
)

const (
	B   = 1
	KiB = 1024 * B
	MiB = 1024 * KiB
	GiB = 1024 * MiB
	TiB = 1024 * GiB
)

type ColumnStore struct {
	mtx                 sync.RWMutex
	dbs                 map[string]*DB
	dbReplaysInProgress map[string]chan struct{}
	reg                 prometheus.Registerer
	logger              log.Logger
	tracer              trace.Tracer
	activeMemorySize    int64
	storagePath         string
	enableWAL           bool
	manualBlockRotation bool
	snapshotTriggerSize int64
	metrics             metrics
	recoveryConcurrency int

	// indexDegree is the degree of the btree index (default = 2)
	indexDegree int
	// splitSize is the number of new granules that are created when granules are split (default =2)
	splitSize int
	// indexConfig is the configuration settings for the lsm index
	indexConfig []*index.LevelConfig

	sources []DataSource
	sinks   []DataSink

	compactAfterRecovery           bool
	compactAfterRecoveryTableNames []string

	// testingOptions are options only used for testing purposes.
	testingOptions struct {
		disableReclaimDiskSpaceOnSnapshot bool
	}
}

type metrics struct {
	shutdownDuration  prometheus.Histogram
	shutdownStarted   prometheus.Counter
	shutdownCompleted prometheus.Counter
}

type Option func(*ColumnStore) error

func New(
	options ...Option,
) (*ColumnStore, error) {
	s := &ColumnStore{
		dbs:                 make(map[string]*DB),
		dbReplaysInProgress: make(map[string]chan struct{}),
		reg:                 prometheus.NewRegistry(),
		logger:              log.NewNopLogger(),
		tracer:              trace.NewNoopTracerProvider().Tracer(""),
		indexConfig:         DefaultIndexConfig(),
		indexDegree:         2,
		splitSize:           2,
		activeMemorySize:    512 * MiB,
	}

	for _, option := range options {
		if err := option(s); err != nil {
			return nil, err
		}
	}

	s.metrics = metrics{
		shutdownDuration: promauto.With(s.reg).NewHistogram(prometheus.HistogramOpts{
			Name: "frostdb_shutdown_duration",
			Help: "time it takes for the columnarstore to complete a full shutdown.",
		}),
		shutdownStarted: promauto.With(s.reg).NewCounter(prometheus.CounterOpts{
			Name: "frostdb_shutdown_started",
			Help: "Indicates a shutdown of the columnarstore has started.",
		}),
		shutdownCompleted: promauto.With(s.reg).NewCounter(prometheus.CounterOpts{
			Name: "frostdb_shutdown_completed",
			Help: "Indicates a shutdown of the columnarstore has completed.",
		}),
	}

	if s.enableWAL && s.storagePath == "" {
		return nil, fmt.Errorf("storage path must be configured if WAL is enabled")
	}

	for _, cfg := range s.indexConfig {
		if cfg.Type == index.CompactionTypeParquetDisk {
			if !s.enableWAL || s.storagePath == "" {
				return nil, fmt.Errorf("persistent disk compaction requires WAL and storage path to be enabled")
			}
		}
	}

	if err := s.recoverDBsFromStorage(context.Background()); err != nil {
		return nil, err
	}

	return s, nil
}

func WithLogger(logger log.Logger) Option {
	return func(s *ColumnStore) error {
		s.logger = logger
		return nil
	}
}

func WithTracer(tracer trace.Tracer) Option {
	return func(s *ColumnStore) error {
		s.tracer = tracer
		return nil
	}
}

func WithRegistry(reg prometheus.Registerer) Option {
	return func(s *ColumnStore) error {
		s.reg = reg
		return nil
	}
}

func WithActiveMemorySize(size int64) Option {
	return func(s *ColumnStore) error {
		s.activeMemorySize = size
		return nil
	}
}

func WithIndexDegree(indexDegree int) Option {
	return func(s *ColumnStore) error {
		s.indexDegree = indexDegree
		return nil
	}
}

func WithSplitSize(size int) Option {
	return func(s *ColumnStore) error {
		s.splitSize = size
		return nil
	}
}

func WithReadWriteStorage(ds DataSinkSource) Option {
	return func(s *ColumnStore) error {
		s.sources = append(s.sources, ds)
		s.sinks = append(s.sinks, ds)
		return nil
	}
}

func WithReadOnlyStorage(ds DataSource) Option {
	return func(s *ColumnStore) error {
		s.sources = append(s.sources, ds)
		return nil
	}
}

func WithWriteOnlyStorage(ds DataSink) Option {
	return func(s *ColumnStore) error {
		s.sinks = append(s.sinks, ds)
		return nil
	}
}

func WithManualBlockRotation() Option {
	return func(s *ColumnStore) error {
		s.manualBlockRotation = true
		return nil
	}
}

func WithWAL() Option {
	return func(s *ColumnStore) error {
		s.enableWAL = true
		return nil
	}
}

func WithStoragePath(path string) Option {
	return func(s *ColumnStore) error {
		s.storagePath = path
		return nil
	}
}

func WithIndexConfig(indexConfig []*index.LevelConfig) Option {
	return func(s *ColumnStore) error {
		s.indexConfig = indexConfig
		return nil
	}
}

func WithCompactionAfterRecovery(tableNames []string) Option {
	return func(s *ColumnStore) error {
		s.compactAfterRecovery = true
		s.compactAfterRecoveryTableNames = tableNames
		return nil
	}
}

// WithSnapshotTriggerSize specifies a size in bytes of uncompressed inserts
// that will trigger a snapshot of the whole database. This can be larger than
// the active memory size given that the active memory size tracks the size of
// *compressed* data, while snapshots are triggered based on the *uncompressed*
// data inserted into the database. The reason this choice was made is that
// if a database instance crashes, it is forced to reread all uncompressed
// inserts since the last snapshot from the WAL, which could potentially lead
// to unrecoverable OOMs on startup. Defining the snapshot trigger in terms of
// uncompressed bytes limits the memory usage on recovery to at most the
// snapshot trigger size (as long as snapshots were successful).
// If 0, snapshots are disabled. Note that snapshots (if enabled) are also
// triggered on block rotation of any database table.
// Snapshots are complementary to the WAL and will also be disabled if the WAL
// is disabled.
func WithSnapshotTriggerSize(size int64) Option {
	return func(s *ColumnStore) error {
		s.snapshotTriggerSize = size
		return nil
	}
}

// WithRecoveryConcurrency limits the number of databases that are recovered
// simultaneously when calling frostdb.New. This helps limit memory usage on
// recovery.
func WithRecoveryConcurrency(concurrency int) Option {
	return func(s *ColumnStore) error {
		s.recoveryConcurrency = concurrency
		return nil
	}
}

// Close persists all data from the columnstore to storage.
// It is no longer valid to use the coumnstore for reads or writes, and the object should not longer be reused.
func (s *ColumnStore) Close() error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.metrics.shutdownStarted.Inc()
	defer s.metrics.shutdownCompleted.Inc()
	defer func(ts time.Time) {
		s.metrics.shutdownDuration.Observe(float64(time.Since(ts)))
	}(time.Now())

	errg := &errgroup.Group{}
	errg.SetLimit(runtime.GOMAXPROCS(0))
	for _, db := range s.dbs {
		toClose := db
		errg.Go(func() error {
			err := toClose.Close()
			if err != nil {
				level.Error(s.logger).Log("msg", "error closing DB", "db", toClose.name, "err", err)
			}
			return err
		})
	}

	return errg.Wait()
}

func (s *ColumnStore) DatabasesDir() string {
	return filepath.Join(s.storagePath, "databases")
}

// recoverDBsFromStorage replays the snapshots and write-ahead logs for each database.
func (s *ColumnStore) recoverDBsFromStorage(ctx context.Context) error {
	if !s.enableWAL {
		return nil
	}

	dir := s.DatabasesDir()
	if _, err := os.Stat(dir); err != nil {
		if os.IsNotExist(err) {
			level.Debug(s.logger).Log("msg", "WAL directory does not exist, no WAL to replay")
			return nil
		}
		return err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	g, ctx := errgroup.WithContext(ctx)
	// Limit this operation since WAL recovery could be very memory intensive.
	if s.recoveryConcurrency == 0 {
		s.recoveryConcurrency = runtime.GOMAXPROCS(0)
	}
	g.SetLimit(s.recoveryConcurrency)
	for _, f := range files {
		databaseName := f.Name()
		g.Go(func() error {
			// Open the DB for the side effect of the snapshot and WALs being loaded as part of the open operation.
			_, err := s.DB(ctx,
				databaseName,
				WithCompactionAfterOpen(
					s.compactAfterRecovery, s.compactAfterRecoveryTableNames,
				),
			)
			return err
		})
	}

	return g.Wait()
}

type dbMetrics struct {
	txHighWatermark prometheus.GaugeFunc
	snapshotMetrics *snapshotMetrics
}

type DB struct {
	columnStore *ColumnStore
	reg         prometheus.Registerer
	logger      log.Logger
	tracer      trace.Tracer
	name        string

	mtx      *sync.RWMutex
	roTables map[string]*Table
	tables   map[string]*Table

	storagePath string
	wal         WAL

	// The database supports multiple data sources and sinks.
	sources []DataSource
	sinks   []DataSink

	// Databases monotonically increasing transaction id
	tx atomic.Uint64
	// highWatermark maintains the highest consecutively completed txn.
	highWatermark atomic.Uint64

	// TxPool is a waiting area for finished transactions that haven't been added to the watermark
	txPool *TxPool

	compactAfterRecovery           bool
	compactAfterRecoveryTableNames []string

	snapshotInProgress atomic.Bool

	metrics *dbMetrics
}

// DataSinkSource is a convenience interface for a data source and sink.
type DataSinkSource interface {
	DataSink
	DataSource
}

// DataSource is remote source of data that can be queried.
type DataSource interface {
	fmt.Stringer
	Scan(ctx context.Context, prefix string, schema *dynparquet.Schema, filter logicalplan.Expr, lastBlockTimestamp uint64, callback func(context.Context, any) error) error
	Prefixes(ctx context.Context, prefix string) ([]string, error)
}

// DataSink is a remote destination for data.
type DataSink interface {
	fmt.Stringer
	Upload(ctx context.Context, name string, r io.Reader) error
	Delete(ctx context.Context, name string) error
}

type DBOption func(*DB) error

func WithCompactionAfterOpen(compact bool, tableNames []string) DBOption {
	return func(db *DB) error {
		db.compactAfterRecovery = compact
		db.compactAfterRecoveryTableNames = tableNames
		return nil
	}
}

// DB gets or creates a database on the given ColumnStore with the given
// options. Note that if the database already exists, the options will be
// applied cumulatively to the database.
func (s *ColumnStore) DB(ctx context.Context, name string, opts ...DBOption) (*DB, error) {
	if !validateName(name) {
		return nil, errors.New("invalid database name")
	}
	applyOptsToDB := func(db *DB) error {
		db.mtx.Lock()
		defer db.mtx.Unlock()
		for _, opt := range opts {
			if err := opt(db); err != nil {
				return err
			}
		}
		return nil
	}
	s.mtx.RLock()
	db, ok := s.dbs[name]
	s.mtx.RUnlock()
	if ok {
		if err := applyOptsToDB(db); err != nil {
			return nil, err
		}
		return db, nil
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()

	// Need to double-check that in the meantime a database with the same name
	// wasn't concurrently created.
	for {
		db, ok = s.dbs[name]
		if ok {
			if err := applyOptsToDB(db); err != nil {
				return nil, err
			}
			return db, nil
		}

		// DB has not yet been created. However, another goroutine might be
		// replaying the WAL in the background (the store mutex is released
		// during replay.).
		waitForReplay, ok := s.dbReplaysInProgress[name]
		if !ok {
			// No replay in progress, it is safe to create the DB.
			break
		}
		s.mtx.Unlock()
		<-waitForReplay
		s.mtx.Lock()
	}

	reg := prometheus.WrapRegistererWith(prometheus.Labels{"db": name}, s.reg)
	logger := log.WithPrefix(s.logger, "db", name)
	db = &DB{
		columnStore: s,
		name:        name,
		mtx:         &sync.RWMutex{},
		tables:      map[string]*Table{},
		roTables:    map[string]*Table{},
		reg:         reg,
		logger:      logger,
		tracer:      s.tracer,
		wal:         &wal.NopWAL{},
		sources:     s.sources,
		sinks:       s.sinks,
	}

	if s.storagePath != "" {
		db.storagePath = filepath.Join(s.DatabasesDir(), name)
	}

	if err := applyOptsToDB(db); err != nil {
		return nil, err
	}

	if dbSetupErr := func() error {
		if err := os.RemoveAll(db.trashDir()); err != nil {
			return err
		}
		db.txPool = NewTxPool(&db.highWatermark)
		// Wait to start the compactor pool since benchmarks show that WAL
		// replay is a lot more efficient if it is not competing against
		// compaction. Additionally, if the CompactAfterRecovery option is
		// specified, we don't want the user-specified compaction to race with
		// our compactor pool.
		if len(db.sources) != 0 {
			for _, source := range db.sources {
				prefixes, err := source.Prefixes(ctx, name)
				if err != nil {
					return err
				}

				for _, prefix := range prefixes {
					_, err := db.readOnlyTable(prefix)
					if err != nil {
						return err
					}
				}
			}
		}

		if s.enableWAL {
			if err := func() error {
				// Unlock the store mutex while the WAL is replayed, otherwise
				// if multiple DBs are opened in parallel, WAL replays will not
				// happen in parallel. However, create a channel for any
				// goroutines that might concurrently try to open the same DB
				// to listen on.
				s.dbReplaysInProgress[name] = make(chan struct{})
				s.mtx.Unlock()
				defer func() {
					s.mtx.Lock()
					close(s.dbReplaysInProgress[name])
					delete(s.dbReplaysInProgress, name)
				}()
				var err error
				db.wal, err = db.openWAL(ctx)
				return err
			}(); err != nil {
				return err
			}
			// WAL pointers of tables need to be updated to the DB WAL since
			// they are loaded from object storage and snapshots with a no-op
			// WAL by default.
			for _, table := range db.tables {
				if !table.config.Load().DisableWal {
					table.wal = db.wal
				}
			}
			for _, table := range db.roTables {
				if !table.config.Load().DisableWal {
					table.wal = db.wal
				}
			}
		}

		// Register metrics last to avoid duplicate registration should and of the WAL or storage replay errors occur
		db.metrics = &dbMetrics{
			txHighWatermark: promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
				Name: "frostdb_tx_high_watermark",
				Help: "The highest transaction number that has been released to be read",
			}, func() float64 {
				return float64(db.highWatermark.Load())
			}),
			snapshotMetrics: newSnapshotMetrics(reg),
		}
		return nil
	}(); dbSetupErr != nil {
		level.Warn(s.logger).Log(
			"msg", "error setting up db",
			"name", name,
			"err", dbSetupErr,
		)
		// closeInternal handles closing partially set fields in the db without
		// rotating blocks etc... that the public Close method does.
		_ = db.closeInternal()
		return nil, dbSetupErr
	}

	// Compact tables after recovery if requested.
	if db.compactAfterRecovery {
		tables := db.compactAfterRecoveryTableNames
		if len(tables) == 0 {
			// Run compaction on all tables.
			tables = maps.Keys(db.tables)
		}
		for _, name := range tables {
			tbl, err := db.GetTable(name)
			if err != nil {
				level.Warn(db.logger).Log("msg", "get table during db setup", "err", err)
				continue
			}

			start := time.Now()
			if err := tbl.EnsureCompaction(); err != nil {
				level.Warn(db.logger).Log("msg", "compaction during setup", "err", err)
			}
			level.Info(db.logger).Log(
				"msg", "compacted table after recovery", "table", name, "took", time.Since(start),
			)
		}
	}

	s.dbs[name] = db
	return db, nil
}

// DBs returns all the DB names of this column store.
func (s *ColumnStore) DBs() []string {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return maps.Keys(s.dbs)
}

func (s *ColumnStore) GetDB(name string) (*DB, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	db, ok := s.dbs[name]
	if !ok {
		return nil, fmt.Errorf("db %s not found", name)
	}
	return db, nil
}

func (s *ColumnStore) DropDB(name string) error {
	db, err := s.GetDB(name)
	if err != nil {
		return err
	}
	if err := db.Close(WithClearStorage()); err != nil {
		return err
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()
	delete(s.dbs, name)
	return os.Remove(filepath.Join(s.DatabasesDir(), name))
}

func (db *DB) openWAL(ctx context.Context) (WAL, error) {
	wal, err := wal.Open(
		db.logger,
		db.reg,
		db.walDir(),
	)
	if err != nil {
		return nil, err
	}

	if err := db.recover(ctx, wal); err != nil {
		return nil, err
	}

	wal.RunAsync()
	return wal, nil
}

const (
	walPath       = "wal"
	snapshotsPath = "snapshots"
)

func (db *DB) walDir() string {
	return filepath.Join(db.storagePath, walPath)
}

func (db *DB) snapshotsDir() string {
	return filepath.Join(db.storagePath, snapshotsPath)
}

func (db *DB) trashDir() string {
	return filepath.Join(db.storagePath, "trash")
}

func (db *DB) indexDir() string {
	return filepath.Join(db.storagePath, "index")
}

// recover attempts to recover database state from a combination of snapshots and the WAL.
//
// The recovery process is as follows:
// 1. Load the latest snapshot (if one should exist).
// 1.a. If on-disk LSM index files exist: Upon table creation during snapshot loading, the index files shall be recovered from, inserting parts into the index.
// 2. Replay the WAL starting from the latest snapshot transaction.
// 2.a. If on-disk LSM index files were loaded: Insertion into the index may drop the insertion if a part with a higher transaction already exists in the WAL.
func (db *DB) recover(ctx context.Context, wal WAL) error {
	level.Info(db.logger).Log(
		"msg", "recovering db",
		"name", db.name,
	)
	snapshotLoadStart := time.Now()
	snapshotTx, err := db.loadLatestSnapshot(ctx)
	if err != nil {
		level.Info(db.logger).Log(
			"msg", "failed to load latest snapshot", "db", db.name, "err", err,
		)
		snapshotTx = 0
	}
	snapshotLogArgs := make([]any, 0)
	if snapshotTx != 0 {
		snapshotLogArgs = append(
			snapshotLogArgs,
			"snapshot_tx", snapshotTx,
			"snapshot_load_duration", time.Since(snapshotLoadStart),
		)
		if err := db.cleanupSnapshotDir(ctx, snapshotTx); err != nil {
			// Truncation is best-effort. If it fails, move on.
			level.Info(db.logger).Log(
				"msg", "failed to truncate snapshots not equal to loaded snapshot",
				"err", err,
				"snapshot_tx", snapshotTx,
			)
		}
		if err := wal.Truncate(snapshotTx); err != nil {
			level.Info(db.logger).Log(
				"msg", "failed to truncate WAL after loading snapshot",
				"err", err,
				"snapshot_tx", snapshotTx,
			)
		}
	}

	// persistedTables is a map from a table name to the last transaction
	// persisted.
	persistedTables := map[string]uint64{}
	var lastTx uint64

	start := time.Now()
	if err := wal.Replay(snapshotTx, func(tx uint64, record *walpb.Record) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		switch e := record.Entry.EntryType.(type) {
		case *walpb.Entry_TableBlockPersisted_:
			persistedTables[e.TableBlockPersisted.TableName] = tx
			if tx > snapshotTx {
				// The loaded snapshot has data in a table that has been
				// persisted. Delete all data in this table, since it has
				// already been persisted.
				db.mtx.Lock()
				if table, ok := db.tables[e.TableBlockPersisted.TableName]; ok {
					table.ActiveBlock().index, err = index.NewLSM(
						filepath.Join(table.db.indexDir(), table.name, table.ActiveBlock().ulid.String()), // Any index files are found at <db.indexDir>/<table.name>/<block.id>
						table.schema,
						table.IndexConfig(),
						db.Wait,
						index.LSMWithMetrics(table.metrics.indexMetrics),
						index.LSMWithLogger(table.logger),
					)
					if err != nil {
						return fmt.Errorf("create new lsm index: %w", err)
					}
				}
				db.mtx.Unlock()
			}
			return nil
		default:
			return nil
		}
	}); err != nil {
		return err
	}

	// performSnapshot is set to true if a snapshot should be performed after
	// replay. This is set in cases where there could be "dead bytes" in the
	// WAL (i.e. entries that occupy space on disk but are useless).
	performSnapshot := false

	if err := wal.Replay(snapshotTx, func(tx uint64, record *walpb.Record) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		lastTx = tx
		switch e := record.Entry.EntryType.(type) {
		case *walpb.Entry_NewTableBlock_:
			entry := e.NewTableBlock
			schema := entry.Config.Schema
			var id ulid.ULID
			if err := id.UnmarshalBinary(entry.BlockId); err != nil {
				return err
			}

			if lastPersistedTx, ok := persistedTables[entry.TableName]; ok && tx < lastPersistedTx {
				// This block has already been successfully persisted, so we can
				// skip it.
				return nil
			}

			tableName := entry.TableName
			table, err := db.GetTable(tableName)
			var tableErr ErrTableNotFound
			if errors.As(err, &tableErr) {
				return func() error {
					db.mtx.Lock()
					defer db.mtx.Unlock()
					config := NewTableConfig(schema, FromConfig(entry.Config))
					if _, ok := db.roTables[tableName]; ok {
						table, err = db.promoteReadOnlyTableLocked(tableName, config)
						if err != nil {
							return fmt.Errorf("promoting read only table: %w", err)
						}
					} else {
						table, err = newTable(
							db,
							tableName,
							config,
							db.reg,
							db.logger,
							db.tracer,
							wal,
						)
						if err != nil {
							return fmt.Errorf("instantiate table: %w", err)
						}
					}

					table.active, err = newTableBlock(table, 0, tx, id)
					if err != nil {
						return err
					}
					db.tables[tableName] = table
					return nil
				}()
			}
			if err != nil {
				return fmt.Errorf("get table: %w", err)
			}

			// If we get to this point it means a block was finished but did
			// not get persisted.
			level.Info(db.logger).Log(
				"msg", "writing unfinished block in recovery",
				"table", tableName,
				"tx", tx,
			)
			table.pendingBlocks[table.active] = struct{}{}
			go table.writeBlock(table.active, db.columnStore.manualBlockRotation, false)

			protoEqual := proto.Equal(schema, table.config.Load().GetSchema())
			if !protoEqual {
				// If schemas are identical from block to block we should we
				// reuse the previous schema in order to retain pooled memory
				// for it.
				schema, err := dynparquet.SchemaFromDefinition(schema)
				if err != nil {
					return fmt.Errorf("initialize schema: %w", err)
				}

				table.schema = schema
			}

			table.active, err = newTableBlock(table, table.active.minTx, tx, id)
			if err != nil {
				return err
			}
		case *walpb.Entry_Write_:
			entry := e.Write
			tableName := entry.TableName
			if lastPersistedTx, ok := persistedTables[tableName]; ok && tx < lastPersistedTx {
				// This write has already been successfully persisted, so we can
				// skip it.
				return nil
			}

			table, err := db.GetTable(tableName)
			var tableErr ErrTableNotFound
			if errors.As(err, &tableErr) {
				// This means the WAL was truncated at a point where this write
				// was already successfully persisted to disk in more optimized
				// form than the WAL.
				return nil
			}
			if err != nil {
				return fmt.Errorf("get table: %w", err)
			}

			switch e.Write.Arrow {
			case true:
				reader, err := ipc.NewReader(bytes.NewReader(entry.Data))
				if err != nil {
					return fmt.Errorf("create ipc reader: %w", err)
				}
				record, err := reader.Read()
				if err != nil {
					return fmt.Errorf("read record: %w", err)
				}
				defer reader.Release()
				size := util.TotalRecordSize(record)
				table.active.index.InsertPart(parts.NewArrowPart(tx, record, uint64(size), table.schema, parts.WithCompactionLevel(int(index.L0))))
			default:
				panic("parquet writes are deprecated")
			}
			return nil
		case *walpb.Entry_TableBlockPersisted_:
			// If a block was persisted but the entry still exists in the WAL,
			// a snapshot was not performed after persisting the block. Perform
			// one now to clean up the WAL.
			performSnapshot = true
			return nil
		case *walpb.Entry_Snapshot_:
			return nil
		default:
			return fmt.Errorf("unexpected WAL entry type: %t", e)
		}
		return nil
	}); err != nil {
		return err
	}

	resetTxn := snapshotTx
	if lastTx > resetTxn {
		resetTxn = lastTx
	}

	db.mtx.Lock()
	for _, table := range db.tables {
		block := table.ActiveBlock()
		block.uncompressedInsertsSize.Store(block.Index().LevelSize(index.L0))
	}
	db.mtx.Unlock()

	db.resetToTxn(resetTxn, nil)
	if performSnapshot && db.columnStore.snapshotTriggerSize != 0 {
		level.Info(db.logger).Log(
			"msg", "performing snapshot after recovery",
		)
		db.snapshot(ctx, false, func() {
			if err := db.reclaimDiskSpace(ctx, wal); err != nil {
				level.Error(db.logger).Log(
					"msg", "failed to reclaim disk space after snapshot during recovery",
					"err", err,
				)
			}
		})
	}
	level.Info(db.logger).Log(
		append(
			[]any{
				"msg", "db recovered",
				"wal_replay_duration", time.Since(start),
				"watermark", resetTxn,
			},
			snapshotLogArgs...,
		)...,
	)
	return nil
}

type CloseOption func(*closeOptions)

type closeOptions struct {
	clearStorage bool
}

func WithClearStorage() CloseOption {
	return func(o *closeOptions) {
		o.clearStorage = true
	}
}

func (db *DB) Close(options ...CloseOption) error {
	opts := &closeOptions{}
	for _, opt := range options {
		opt(opts)
	}
	level.Info(db.logger).Log("msg", "closing DB")
	shouldPersist := len(db.sinks) > 0 && !db.columnStore.manualBlockRotation
	for _, table := range db.tables {
		table.close()
		if shouldPersist {
			// Write the blocks but no snapshots since they are long-running
			// jobs.
			// TODO(asubiotto): Maybe we should snapshot in any case since it
			// should be faster to write to local disk than upload to object
			// storage. This would avoid a slow WAL replay on startup if we
			// don't manage to persist in time.
			table.writeBlock(table.ActiveBlock(), false, false)
		}
	}
	level.Info(db.logger).Log("msg", "closed all tables")

	if !shouldPersist && db.columnStore.snapshotTriggerSize != 0 && !opts.clearStorage {
		start := time.Now()
		db.snapshot(context.Background(), false, func() {
			level.Info(db.logger).Log("msg", "snapshot on close completed", "duration", time.Since(start))
			if err := db.reclaimDiskSpace(context.Background(), nil); err != nil {
				level.Error(db.logger).Log(
					"msg", "failed to reclaim disk space after snapshot",
					"err", err,
				)
			}
		})
	}

	if err := db.closeInternal(); err != nil {
		return err
	}

	if (shouldPersist || opts.clearStorage) && db.storagePath != "" {
		if err := db.dropStorage(); err != nil {
			return err
		}
		level.Info(db.logger).Log("msg", "cleaned up wal & snapshots")
	}
	return nil
}

func (db *DB) closeInternal() error {
	if db.columnStore.enableWAL && db.wal != nil {
		if err := db.wal.Close(); err != nil {
			return err
		}
	}
	if db.txPool != nil {
		db.txPool.Stop()
	}

	return nil
}

func (db *DB) maintainWAL() {
	if minTx := db.getMinTXPersisted(); minTx > 0 {
		if err := db.wal.Truncate(minTx); err != nil {
			return
		}
	}
}

// reclaimDiskSpace attempts to read the latest valid snapshot txn and removes
// any snapshots/wal entries that are older than the snapshot tx. Since this can
// be called before db.wal is set, the caller may optionally pass in a WAL to
// truncate.
func (db *DB) reclaimDiskSpace(ctx context.Context, wal WAL) error {
	if db.columnStore.testingOptions.disableReclaimDiskSpaceOnSnapshot {
		return nil
	}
	validSnapshotTxn, err := db.getLatestValidSnapshotTxn(ctx)
	if err != nil {
		return err
	}
	if validSnapshotTxn == 0 {
		return nil
	}
	if err := db.cleanupSnapshotDir(ctx, validSnapshotTxn); err != nil {
		return err
	}
	if wal == nil {
		wal = db.wal
	}
	return wal.Truncate(validSnapshotTxn)
}

func (db *DB) getMinTXPersisted() uint64 {
	db.mtx.RLock()
	defer db.mtx.RUnlock()
	minTx := uint64(math.MaxUint64)
	for _, table := range db.tables {
		table.mtx.RLock()
		tableMinTxPersisted := table.lastCompleted
		table.mtx.RUnlock()
		if tableMinTxPersisted < minTx {
			minTx = tableMinTxPersisted
		}
	}
	return minTx
}

func (db *DB) readOnlyTable(name string) (*Table, error) {
	table, ok := db.tables[name]
	if ok {
		return table, nil
	}

	table, err := newTable(
		db,
		name,
		nil,
		db.reg,
		db.logger,
		db.tracer,
		db.wal,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	db.roTables[name] = table
	return table, nil
}

// promoteReadOnlyTableLocked promotes a read-only table to a read-write table.
// The read-write table is returned but not added to the database. Callers must
// do so.
// db.mtx must be held while calling this method.
func (db *DB) promoteReadOnlyTableLocked(name string, config *tablepb.TableConfig) (*Table, error) {
	table, ok := db.roTables[name]
	if !ok {
		return nil, fmt.Errorf("read only table %s not found", name)
	}
	schema, err := schemaFromTableConfig(config)
	if err != nil {
		return nil, err
	}
	table.config.Store(config)
	table.schema = schema
	delete(db.roTables, name)
	return table, nil
}

// Table will get or create a new table with the given name and config. If a table already exists with the given name, it will have it's configuration updated.
func (db *DB) Table(name string, config *tablepb.TableConfig) (*Table, error) {
	return db.table(name, config, generateULID())
}

func (db *DB) table(name string, config *tablepb.TableConfig, id ulid.ULID) (*Table, error) {
	if config == nil {
		return nil, fmt.Errorf("table config cannot be nil")
	}
	if !validateName(name) {
		return nil, errors.New("invalid table name")
	}
	db.mtx.RLock()
	table, ok := db.tables[name]
	db.mtx.RUnlock()
	if ok {
		table.config.Store(config)
		return table, nil
	}

	db.mtx.Lock()
	defer db.mtx.Unlock()

	// Need to double-check that in the meantime another table with the same
	// name wasn't concurrently created.
	table, ok = db.tables[name]
	if ok {
		return table, nil
	}

	// Check if this table exists as a read only table
	if _, ok := db.roTables[name]; ok {
		var err error
		table, err = db.promoteReadOnlyTableLocked(name, config)
		if err != nil {
			return nil, err
		}
	} else {
		var err error
		table, err = newTable(
			db,
			name,
			config,
			db.reg,
			db.logger,
			db.tracer,
			db.wal,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create table: %w", err)
		}
	}

	tx, _, commit := db.begin()
	defer commit()

	if err := table.newTableBlock(0, tx, id); err != nil {
		return nil, err
	}

	db.tables[name] = table
	return table, nil
}

type ErrTableNotFound struct {
	TableName string
}

func (e ErrTableNotFound) Error() string {
	return fmt.Sprintf("table %s not found", e.TableName)
}

func (db *DB) GetTable(name string) (*Table, error) {
	db.mtx.RLock()
	table, ok := db.tables[name]
	db.mtx.RUnlock()
	if !ok {
		return nil, ErrTableNotFound{TableName: name}
	}
	return table, nil
}

func (db *DB) TableProvider() *DBTableProvider {
	return NewDBTableProvider(db)
}

// TableNames returns the names of all the db's tables.
func (db *DB) TableNames() []string {
	db.mtx.RLock()
	tables := maps.Keys(db.tables)
	db.mtx.RUnlock()
	return tables
}

type DBTableProvider struct {
	db *DB
}

func NewDBTableProvider(db *DB) *DBTableProvider {
	return &DBTableProvider{
		db: db,
	}
}

func (p *DBTableProvider) GetTable(name string) (logicalplan.TableReader, error) {
	p.db.mtx.RLock()
	defer p.db.mtx.RUnlock()
	tbl, ok := p.db.tables[name]
	if ok {
		return tbl, nil
	}

	tbl, ok = p.db.roTables[name]
	if ok {
		return tbl, nil
	}

	return nil, fmt.Errorf("table %v not found", name)
}

// beginRead returns the high watermark. Reads can safely access any write that has a lower or equal tx id than the returned number.
func (db *DB) beginRead() uint64 {
	return db.highWatermark.Load()
}

// begin is an internal function that Tables call to start a transaction for writes.
// It returns:
//
//	the write tx id
//	The current high watermark
//	A function to complete the transaction
func (db *DB) begin() (uint64, uint64, func()) {
	txn := db.tx.Add(1)
	watermark := db.highWatermark.Load()
	return txn, watermark, func() {
		if mark := db.highWatermark.Load(); mark+1 == txn {
			// This is the next consecutive transaction; increase the watermark.
			db.highWatermark.Store(txn)
		}

		// place completed transaction in the waiting pool
		db.txPool.Insert(txn)
	}
}

// Wait is a blocking function that returns once the high watermark has equaled or exceeded the transaction id.
// Wait makes no differentiation between completed and aborted transactions.
func (db *DB) Wait(tx uint64) {
	for {
		if db.highWatermark.Load() >= tx {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// HighWatermark returns the current high watermark.
func (db *DB) HighWatermark() uint64 {
	return db.highWatermark.Load()
}

// resetToTxn resets the DB's internal state to resume from the given
// transaction. If the given wal is non-nil, it is also reset so that the next
// expected transaction will log correctly to the WAL. Note that db.wal is not
// used since callers might be calling resetToTxn before db.wal has been
// initialized or might not want the WAL to be reset.
func (db *DB) resetToTxn(txn uint64, wal WAL) {
	db.tx.Store(txn)
	db.highWatermark.Store(txn)
	if wal != nil {
		// This call resets the WAL to a zero state so that new records can be
		// logged.
		if err := wal.Reset(txn + 1); err != nil {
			level.Warn(db.logger).Log(
				"msg", "failed to reset WAL when resetting DB to txn",
				"txnID", txn,
				"err", err,
			)
		}
	}
}

// validateName ensures that the passed in name doesn't violate any constrainsts.
func validateName(name string) bool {
	return !strings.Contains(name, "/")
}

// dropStorage removes snapshots and WAL data from the storage directory.
func (db *DB) dropStorage() error {
	trashDir := db.trashDir()

	if moveErr := func() error {
		if err := os.MkdirAll(trashDir, os.FileMode(0o755)); err != nil {
			return fmt.Errorf("making trash dir: %w", err)
		}
		// Create a temporary directory in the trash dir to avoid clashing
		// with other wal/snapshot dirs that might not have been removed
		// previously.
		tmpPath, err := os.MkdirTemp(trashDir, "")
		if err != nil {
			return err
		}
		if err := os.Rename(db.snapshotsDir(), filepath.Join(tmpPath, snapshotsPath)); err != nil && !os.IsNotExist(err) {
			return err
		}
		if err := os.Rename(db.walDir(), filepath.Join(tmpPath, walPath)); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}(); moveErr != nil {
		// If we failed to move the wal/snapshots to the trash dir, fall
		// back to attempting to remove them with RemoveAll.
		if err := os.RemoveAll(db.snapshotsDir()); err != nil {
			return fmt.Errorf("%v: %v", moveErr, err)
		}
		if err := os.RemoveAll(db.walDir()); err != nil {
			return fmt.Errorf("%v: %v", moveErr, err)
		}
		return moveErr
	}
	return os.RemoveAll(trashDir)
}
