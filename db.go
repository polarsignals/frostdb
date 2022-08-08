package frostdb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/segmentio/parquet-go"
	"github.com/thanos-io/objstore"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/polarsignals/frostdb/dynparquet"
	walpb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/wal/v1alpha1"
	"github.com/polarsignals/frostdb/query/logicalplan"
	"github.com/polarsignals/frostdb/wal"
)

type ColumnStore struct {
	mtx                  *sync.RWMutex
	dbs                  map[string]*DB
	reg                  prometheus.Registerer
	logger               log.Logger
	granuleSize          int
	activeMemorySize     int64
	storagePath          string
	bucket               objstore.Bucket
	ignoreStorageOnQuery bool
	enableWAL            bool

	// indexDegree is the degree of the btree index (default = 2)
	indexDegree int
	// splitSize is the number of new granules that are created when granules are split (default =2)
	splitSize int
}

type Option func(*ColumnStore) error

func New(
	logger log.Logger,
	reg prometheus.Registerer,
	options ...Option,
) (*ColumnStore, error) {
	if reg == nil {
		reg = prometheus.NewRegistry()
	}

	s := &ColumnStore{
		mtx:              &sync.RWMutex{},
		dbs:              map[string]*DB{},
		reg:              reg,
		logger:           logger,
		indexDegree:      2,
		splitSize:        2,
		granuleSize:      8192,
		activeMemorySize: 512 * 1024 * 1024, // 512MB
	}

	for _, option := range options {
		if err := option(s); err != nil {
			return nil, err
		}
	}

	if s.enableWAL && s.storagePath == "" {
		return nil, fmt.Errorf("storage path must be configured if WAL is enabled")
	}

	return s, nil
}

func WithGranuleSize(size int) Option {
	return func(s *ColumnStore) error {
		s.granuleSize = size
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

func WithBucketStorage(bucket objstore.Bucket) Option {
	return func(s *ColumnStore) error {
		s.bucket = bucket
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

// WithIgnoreStorageOnQuery storage paths aren't included in queries.
func WithIgnoreStorageOnQuery() Option {
	return func(s *ColumnStore) error {
		s.ignoreStorageOnQuery = true
		return nil
	}
}

// Close persists all data from the columnstore to storage.
// It is no longer valid to use the coumnstore for reads or writes, and the object should not longer be reused.
func (s *ColumnStore) Close() error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	for _, db := range s.dbs {
		if err := db.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (s *ColumnStore) DatabasesDir() string {
	return filepath.Join(s.storagePath, "databases")
}

// ReplayWALs replays the write-ahead log for each database.
func (s *ColumnStore) ReplayWALs(ctx context.Context) error {
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
	for _, f := range files {
		databaseName := f.Name()
		g.Go(func() error {
			db, err := s.DB(context.Background(), databaseName)
			if err != nil {
				return err
			}
			return db.replayWAL(ctx)
		})
	}

	return g.Wait()
}

type dbMetrics struct {
	txHighWatermark prometheus.GaugeFunc
}

type DB struct {
	columnStore *ColumnStore
	logger      log.Logger
	name        string

	mtx    *sync.RWMutex
	tables map[string]*Table
	reg    prometheus.Registerer

	storagePath          string
	wal                  WAL
	bucket               objstore.Bucket
	ignoreStorageOnQuery bool
	// Databases monotonically increasing transaction id
	tx *atomic.Uint64

	// TxPool is a waiting area for finished transactions that haven't been added to the watermark
	txPool *TxPool

	// highWatermark maintains the highest consecutively completed tx number
	highWatermark *atomic.Uint64

	metrics *dbMetrics
}

func (s *ColumnStore) DB(ctx context.Context, name string) (*DB, error) {
	s.mtx.RLock()
	db, ok := s.dbs[name]
	s.mtx.RUnlock()
	if ok {
		return db, nil
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()

	// Need to double-check that in the meantime a database with the same name
	// wasn't concurrently created.
	db, ok = s.dbs[name]
	if ok {
		return db, nil
	}

	highWatermark := atomic.NewUint64(0)
	reg := prometheus.WrapRegistererWith(prometheus.Labels{"db": name}, s.reg)
	db = &DB{
		columnStore:          s,
		name:                 name,
		mtx:                  &sync.RWMutex{},
		tables:               map[string]*Table{},
		reg:                  reg,
		tx:                   atomic.NewUint64(0),
		highWatermark:        highWatermark,
		storagePath:          filepath.Join(s.DatabasesDir(), name),
		logger:               s.logger,
		wal:                  &wal.NopWAL{},
		ignoreStorageOnQuery: s.ignoreStorageOnQuery,
		metrics: &dbMetrics{
			txHighWatermark: promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
				Name: "tx_high_watermark",
				Help: "The highest transaction number that has been released to be read",
			}, func() float64 {
				return float64(highWatermark.Load())
			}),
		},
	}

	if s.bucket != nil {
		db.bucket = NewPrefixedBucket(s.bucket, db.name)
	}

	if s.enableWAL {
		var err error
		db.wal, err = db.openWAL()
		if err != nil {
			return nil, err
		}
	}

	db.txPool = NewTxPool(db.highWatermark)

	// If bucket storage is configured; scan for existing tables in the database
	if db.bucket != nil {
		if err := db.bucket.Iter(ctx, "", func(block string) error {
			attr, err := db.bucket.Attributes(ctx, block)
			if err != nil {
				return err
			}

			// grab table name
			tableName := filepath.Dir(filepath.Dir(block))

			b := &BucketReaderAt{
				name:   block,
				ctx:    ctx,
				Bucket: db.bucket,
			}

			f, err := parquet.OpenFile(b, attr.Size)
			if err != nil {
				return err
			}

			schema, err := dynparquet.SchemaFromParquetFile(f)
			if err != nil {
				return err
			}

			tbl, err := db.Table(tableName, NewTableConfig(schema))
			if err != nil {
				return err
			}
			db.tables[tableName] = tbl

			return nil
		}, objstore.WithRecursiveIter); err != nil {
			return nil, err
		}
	}

	s.dbs[name] = db
	return db, nil
}

func (db *DB) openWAL() (WAL, error) {
	return wal.Open(
		db.logger,
		db.reg,
		db.walDir(),
	)
}

func (db *DB) walDir() string {
	return filepath.Join(db.storagePath, "wal")
}

func (db *DB) replayWAL(ctx context.Context) error {
	persistedBlocks := map[ulid.ULID]struct{}{}
	if err := db.wal.Replay(func(tx uint64, record *walpb.Record) error {
		switch e := record.Entry.EntryType.(type) {
		case *walpb.Entry_TableBlockPersisted_:
			entry := e.TableBlockPersisted
			var id ulid.ULID
			if err := id.UnmarshalBinary(entry.BlockId); err != nil {
				return err
			}

			persistedBlocks[id] = struct{}{}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("first WAL replay: %w", err)
	}

	lastTx := uint64(0)
	if err := db.wal.Replay(func(tx uint64, record *walpb.Record) error {
		lastTx = tx
		switch e := record.Entry.EntryType.(type) {
		case *walpb.Entry_NewTableBlock_:
			entry := e.NewTableBlock

			var id ulid.ULID
			if err := id.UnmarshalBinary(entry.BlockId); err != nil {
				return err
			}

			if _, ok := persistedBlocks[id]; ok {
				// This block has already been successfully persisted, so we can skip it.
				return nil
			}

			tableName := entry.TableName
			table, err := db.GetTable(tableName)
			var tableErr ErrTableNotFound
			if errors.As(err, &tableErr) {
				schema, err := dynparquet.SchemaFromDefinition(entry.Schema)
				if err != nil {
					return fmt.Errorf("initialize schema: %w", err)
				}
				table, err = newTable(
					db,
					tableName,
					NewTableConfig(schema),
					db.reg,
					db.logger,
					db.wal,
				)
				if err != nil {
					return fmt.Errorf("instantiate table: %w", err)
				}

				db.tables[tableName] = table

				table.active, err = newTableBlock(table, 0, tx, id)
				if err != nil {
					return err
				}
				return nil
			}
			if err != nil {
				return fmt.Errorf("get table: %w", err)
			}

			// If we get to this point it means a block was finished but did
			// not get persisted.
			table.pendingBlocks[table.active] = struct{}{}
			go table.writeBlock(table.active)

			if !proto.Equal(entry.Schema, table.config.schema.Definition()) {
				// If schemas are identical from block to block we should we
				// reuse the previous schema in order to retain pooled memory
				// for it.

				schema, err := dynparquet.SchemaFromDefinition(entry.Schema)
				if err != nil {
					return fmt.Errorf("instantiate schema: %w", err)
				}

				table.config.schema = schema
			}

			table.active, err = newTableBlock(table, table.active.minTx, tx, id)
			if err != nil {
				return err
			}
		case *walpb.Entry_Write_:
			entry := e.Write
			tableName := entry.TableName
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

			serBuf, err := dynparquet.ReaderFromBytes(entry.Data)
			if err != nil {
				return fmt.Errorf("deserialize buffer: %w", err)
			}

			if err := table.active.Insert(ctx, tx, serBuf); err != nil {
				return fmt.Errorf("insert buffer into block: %w", err)
			}
		case *walpb.Entry_TableBlockPersisted_:
			return nil
		default:
			return fmt.Errorf("unexpected WAL entry type: %t", e)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("second WAL replay: %w", err)
	}

	db.tx.Store(lastTx)
	db.highWatermark.Store(lastTx)

	return nil
}

func (db *DB) Close() error {
	switch {
	case db.bucket != nil:
		// Persist blocks to storage
		for _, table := range db.tables {
			table.writeBlock(table.ActiveBlock())
		}

		// If we've successfully persisted all the table blocks we can remove the wal
		if err := os.RemoveAll(db.walDir()); err != nil {
			return err
		}

	case db.columnStore.enableWAL:
		if err := db.wal.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) maintainWAL() {
	db.mtx.RLock()
	defer db.mtx.RUnlock()
	minTx := uint64(0)
	for _, table := range db.tables {
		table.mtx.RLock()
		tableMinTxPersisted := table.lastCompleted
		table.mtx.RUnlock()
		if minTx == 0 || tableMinTxPersisted < minTx {
			minTx = tableMinTxPersisted
		}
	}

	if minTx > 0 {
		if err := db.wal.Truncate(minTx); err != nil {
			return
		}
	}
}

func (db *DB) Table(name string, config *TableConfig) (*Table, error) {
	db.mtx.RLock()
	table, ok := db.tables[name]
	db.mtx.RUnlock()
	if ok {
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

	table, err := newTable(
		db,
		name,
		config,
		db.reg,
		db.logger,
		db.wal,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	tx, _, commit := db.begin()
	defer commit()

	id := generateULID()
	if err := table.newTableBlock(0, tx, id); err != nil {
		return nil, err
	}

	db.tables[name] = table
	return table, nil
}

type ErrTableNotFound struct {
	tableName string
}

func (e ErrTableNotFound) Error() string {
	return fmt.Sprintf("table %q not found", e.tableName)
}

func (db *DB) GetTable(name string) (*Table, error) {
	db.mtx.RLock()
	table, ok := db.tables[name]
	db.mtx.RUnlock()
	if !ok {
		return nil, ErrTableNotFound{tableName: name}
	}
	return table, nil
}

func (db *DB) TableProvider() *DBTableProvider {
	return NewDBTableProvider(db)
}

type DBTableProvider struct {
	db *DB
}

func NewDBTableProvider(db *DB) *DBTableProvider {
	return &DBTableProvider{
		db: db,
	}
}

func (p *DBTableProvider) GetTable(name string) logicalplan.TableReader {
	p.db.mtx.RLock()
	defer p.db.mtx.RUnlock()
	return p.db.tables[name]
}

// beginRead returns the high watermark. Reads can safely access any write that has a lower or equal tx id than the returned number.
func (db *DB) beginRead() uint64 {
	return db.highWatermark.Load()
}

// begin is an internal function that Tables call to start a transaction for writes.
// It returns:
//   the write tx id
//   The current high watermark
//   A function to complete the transaction
func (db *DB) begin() (uint64, uint64, func()) {
	tx := db.tx.Inc()
	watermark := db.highWatermark.Load()
	return tx, watermark, func() {
		if mark := db.highWatermark.Load(); mark+1 == tx { // This is the next consecutive transaction; increate the watermark
			db.highWatermark.Inc()
		}

		// place completed transaction in the waiting pool
		db.txPool.Prepend(tx)
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
