package frostdb

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/polarsignals/frostdb/dynparquet"
)

type TableTx interface {
	InsertRecord(context.Context, arrow.Record) error
}

type Transaction interface {
	GetTable(string) (TableTx, error)
	Abort()
	Commit() error
}

type transaction struct {
	db *DB

	// commit is the function to call when this transaction is committed or aborted. It is set by the DB.Begin() function
	commit func()
	// tx is the current transaction number for this transaction
	tx uint64
	// err is set if this transaction has encountered an erorr. All future operations in the transaction will return this error
	err error

	// tablesWritten is a set of table names that have been written to in this transaction.
	// This is used to remove writes from the tables if the transaction is aborted.
	tablesWritten map[string]struct{}
}

type tableTx struct {
	// tx is the parent transaction this table transaction is a part of
	tx    *transaction
	table *Table
}

func (t *tableTx) InsertRecord(ctx context.Context, record arrow.Record) error {
	if t.tx.err != nil {
		return t.tx.err
	}

	block, finish, err := t.table.appender(ctx)
	if err != nil {
		return fmt.Errorf("get appender: %w", err)
	}
	defer finish()

	preHashedRecord := dynparquet.PrehashColumns(t.table.schema, record)
	defer preHashedRecord.Release()

	if err := t.table.wal.LogRecord(t.tx.tx, t.table.name, preHashedRecord); err != nil {
		return fmt.Errorf("append to log: %w", err)
	}

	if err := block.InsertRecord(ctx, t.tx.tx, preHashedRecord); err != nil {
		return fmt.Errorf("insert buffer into block: %w", err)
	}

	t.tx.tablesWritten[t.table.name] = struct{}{}
	return nil
}

func (t *transaction) GetTable(name string) (TableTx, error) {
	if t.err != nil {
		return nil, t.err
	}

	table, err := t.db.GetTable(name)
	if err != nil {
		t.err = err
		return nil, err
	}

	return &tableTx{
		tx:    t,
		table: table,
	}, nil
}

func (t *transaction) Abort() {
	defer t.commit()

	for table := range t.tablesWritten {
		tbl, err := t.db.GetTable(table)
		if err != nil { // TODO: do something smarter here
			t.err = err
			continue
		}

		blk, done, err := tbl.ActiveWriteBlock() // TODO: do we need a write block here?
		if err != nil {
			t.err = err
			continue
		}
		defer done()
		blk.index.Remove(t.tx)
	}
}

func (t *transaction) Commit() error {
	t.commit()
	return t.err
}

func (db *DB) Begin() Transaction {
	tx, _, commit := db.begin()

	return &transaction{
		db:            db,
		commit:        commit,
		tx:            tx,
		tablesWritten: make(map[string]struct{}),
	}
}
