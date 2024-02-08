package frostdb

import (
	"context"
	"testing"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/query"
	"github.com/stretchr/testify/require"
)

func Test_Transaction(t *testing.T) {
	tests := map[string]func(db *DB, validate func(string, int64), t *testing.T){
		"read_isolation": func(db *DB, validateRows func(string, int64), t *testing.T) {
			ctx := context.Background()

			table, err := db.GetTable("test")
			require.NoError(t, err)

			// Write 3 rows to the table
			samples := dynparquet.GenerateTestSamples(3)
			r, err := samples.ToRecord()
			require.NoError(t, err)
			_, err = table.InsertRecord(ctx, r)
			require.NoError(t, err)

			validateRows("test", 3)

			// Start a transaction
			tx := db.Begin()
			tbl, err := tx.GetTable("test")
			require.NoError(t, err)
			tbl.InsertRecord(ctx, r)

			// Even though we've inserted the record, the table should still have 3 rows until we commit the transaction
			validateRows("test", 3)

			require.NoError(t, tx.Commit())

			// Now that the transaction is committed, the table should have 6 rows
			validateRows("test", 6)

		},
		"abort": func(db *DB, validateRows func(string, int64), t *testing.T) {
			ctx := context.Background()

			table, err := db.GetTable("test")
			require.NoError(t, err)

			// Write 3 rows to the table
			samples := dynparquet.GenerateTestSamples(3)
			r, err := samples.ToRecord()
			require.NoError(t, err)
			_, err = table.InsertRecord(ctx, r)
			require.NoError(t, err)

			validateRows("test", 3)

			// Start a transaction
			tx := db.Begin()
			tbl, err := tx.GetTable("test")
			require.NoError(t, err)
			tbl.InsertRecord(ctx, r)

			// Even though we've inserted the record, the table should still have 3 rows until we commit the transaction
			validateRows("test", 3)

			tx.Abort()

			// Now that the transaction is committed, the table should have 6 rows
			validateRows("test", 3)

		},
		"multi table isolation": func(db *DB, validateRows func(string, int64), t *testing.T) {
			ctx := context.Background()

			table, err := db.GetTable("test")
			require.NoError(t, err)

			// Write 3 rows to the table
			samples := dynparquet.GenerateTestSamples(3)
			r, err := samples.ToRecord()
			require.NoError(t, err)
			_, err = table.InsertRecord(ctx, r)
			require.NoError(t, err)

			validateRows("test", 3)

			// Insert 3 rows into table 2
			table2, err := db.Table("test2", NewTableConfig(dynparquet.SampleDefinition()))
			require.NoError(t, err)
			_, err = table2.InsertRecord(ctx, r)
			require.NoError(t, err)

			validateRows("test2", 3)

			// Start a transaction
			tx := db.Begin()
			tbl, err := tx.GetTable("test")
			require.NoError(t, err)
			tbl.InsertRecord(ctx, r)
			tbl2, err := tx.GetTable("test2")
			tbl2.InsertRecord(ctx, r)

			// Even though we've inserted the record, the tables should still have 3 rows until we commit the transaction
			validateRows("test", 3)
			validateRows("test2", 3)

			require.NoError(t, tx.Commit())

			// Now that the transaction is committed, the table should have 6 rows
			validateRows("test", 6)
			validateRows("test2", 6)
		},
		"multi table abort": func(db *DB, validateRows func(string, int64), t *testing.T) {
			ctx := context.Background()

			table, err := db.GetTable("test")
			require.NoError(t, err)

			// Write 3 rows to the table
			samples := dynparquet.GenerateTestSamples(3)
			r, err := samples.ToRecord()
			require.NoError(t, err)
			_, err = table.InsertRecord(ctx, r)
			require.NoError(t, err)

			validateRows("test", 3)

			// Insert 3 rows into table 2
			table2, err := db.Table("test2", NewTableConfig(dynparquet.SampleDefinition()))
			require.NoError(t, err)
			_, err = table2.InsertRecord(ctx, r)
			require.NoError(t, err)

			validateRows("test2", 3)

			// Start a transaction
			tx := db.Begin()
			tbl, err := tx.GetTable("test")
			require.NoError(t, err)
			tbl.InsertRecord(ctx, r)
			tbl2, err := tx.GetTable("test2")
			tbl2.InsertRecord(ctx, r)

			// Even though we've inserted the record, the tables should still have 3 rows until we commit the transaction
			validateRows("test", 3)
			validateRows("test2", 3)

			tx.Abort()

			// Now that the transaction is committed, the table should have 6 rows
			validateRows("test", 3)
			validateRows("test2", 3)
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			config := NewTableConfig(
				dynparquet.SampleDefinition(),
			)

			logger := newTestLogger(t)
			c, err := New(
				WithLogger(logger),
				WithActiveMemorySize(100*KiB),
			)
			require.NoError(t, err)
			t.Cleanup(func() {
				require.NoError(t, c.Close())
			})
			db, err := c.DB(context.Background(), "test")
			require.NoError(t, err)
			_, err = db.Table("test", config)
			require.NoError(t, err)

			validateRows := func(name string, expected int64) {
				pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
				defer pool.AssertSize(t, 0)
				rows := int64(0)
				engine := query.NewEngine(pool, db.TableProvider())
				err = engine.ScanTable(name).
					Execute(context.Background(), func(ctx context.Context, r arrow.Record) error {
						rows += r.NumRows()
						return nil
					})
				require.NoError(t, err)
				require.Equal(t, expected, rows)
			}

			test(db, validateRows, t)
		})
	}
}
