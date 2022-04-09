package arcticdb

import (
	"fmt"
	"sync"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"

	"github.com/polarsignals/arcticdb/query/logicalplan"
)

type ColumnStore struct {
	mtx *sync.RWMutex
	dbs map[string]*DB
	reg prometheus.Registerer
}

func New(reg prometheus.Registerer) *ColumnStore {
	if reg == nil {
		reg = prometheus.NewRegistry()
	}

	return &ColumnStore{
		mtx: &sync.RWMutex{},
		dbs: map[string]*DB{},
		reg: reg,
	}
}

type DB struct {
	name string

	mtx    *sync.RWMutex
	tables map[string]*Table
	reg    prometheus.Registerer

	// Databases monotonically increasing transaction id
	tx *atomic.Uint64

	// TxPool is a waiting area for finished transactions that haven't been added to the watermark
	txPool *TxPool

	// highWatermark maintains the highest consecutively completed tx number
	highWatermark *atomic.Uint64
}

func (s *ColumnStore) DB(name string) *DB {
	s.mtx.RLock()
	db, ok := s.dbs[name]
	s.mtx.RUnlock()
	if ok {
		return db
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()

	// Need to double-check that in the meantime a database with the same name
	// wasn't concurrently created.
	db, ok = s.dbs[name]
	if ok {
		return db
	}

	db = &DB{
		name:          name,
		mtx:           &sync.RWMutex{},
		tables:        map[string]*Table{},
		reg:           prometheus.WrapRegistererWith(prometheus.Labels{"db": name}, s.reg),
		tx:            atomic.NewUint64(0),
		highWatermark: atomic.NewUint64(0),
	}

	db.txPool = NewTxPool(db.highWatermark)

	s.dbs[name] = db
	return db
}

func (db *DB) Table(name string, config *TableConfig, logger log.Logger) (*Table, error) {
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

	var err error
	table, err = newTable(db, name, config, db.reg, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}
	db.tables[name] = table
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
