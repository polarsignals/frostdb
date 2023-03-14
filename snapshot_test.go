package frostdb

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/query"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

func TestSnapshot(t *testing.T) {
	ctx := context.Background()
	// Create a new DB with multiple tables and granules with
	// compacted/uncompacted parts that have a mixture of arrow/parquet records.
	t.Run("Empty", func(t *testing.T) {
		c, err := New(
			WithStoragePath(t.TempDir()),
			WithWAL(),
		)
		require.NoError(t, err)
		defer c.Close()

		db, err := c.DB(ctx, "test")
		require.NoError(t, err)

		require.NoError(t, db.snapshotAtTX(ctx, db.highWatermark.Load()))

		txBefore := db.highWatermark.Load()
		tx, err := db.loadLatestSnapshot(ctx)
		require.NoError(t, err)
		require.Equal(t, txBefore, tx)
	})

	t.Run("WithData", func(t *testing.T) {
		c, err := New(
			WithStoragePath(t.TempDir()),
			WithWAL(),
		)
		require.NoError(t, err)
		defer c.Close()

		db, err := c.DB(ctx, "test")
		require.NoError(t, err)

		config := NewTableConfig(dynparquet.SampleDefinition())

		// Pause compactor pool to have control over compactions.
		db.compactorPool.pause()
		table, err := db.Table("table1", config)
		require.NoError(t, err)
		insertSampleRecords(ctx, t, table, 1, 2, 3)
		require.NoError(t, table.EnsureCompaction())
		insertSamples(ctx, t, table, 4, 5, 6)
		insertSampleRecords(ctx, t, table, 7, 8, 9)

		const overrideConfigVal = 1234
		config.RowGroupSize = overrideConfigVal
		table, err = db.Table("table2", config)
		require.NoError(t, err)
		insertSamples(ctx, t, table, 1, 2, 3)
		insertSampleRecords(ctx, t, table, 4, 5, 6)

		config.BlockReaderLimit = overrideConfigVal
		_, err = db.Table("empty", config)
		require.NoError(t, err)

		highWatermark := db.highWatermark.Load()

		// Insert a sample that should not be snapshot.
		insertSamples(ctx, t, table, 10)
		require.NoError(t, db.snapshotAtTX(ctx, highWatermark))

		// Create another db and verify.
		snapshotDB, err := c.DB(ctx, "testsnapshot")
		require.NoError(t, err)

		snapshotDB.compactorPool.pause()
		// Load the other db's latest snapshot.
		tx, err := snapshotDB.loadLatestSnapshotFromDir(ctx, db.snapshotsDir())
		require.NoError(t, err)
		require.Equal(t, highWatermark, tx)
		require.Equal(t, highWatermark, snapshotDB.highWatermark.Load())

		require.Equal(t, len(db.tables), len(snapshotDB.tables))

		snapshotEngine := query.NewEngine(memory.DefaultAllocator, snapshotDB.TableProvider())

		for _, testCase := range []struct {
			name            string
			expMaxTimestamp int
		}{
			{
				name:            "table1",
				expMaxTimestamp: 9,
			},
			{
				name:            "table2",
				expMaxTimestamp: 6,
			},
			{
				name: "empty",
			},
		} {
			if testCase.expMaxTimestamp != 0 {
				max := []logicalplan.Expr{
					logicalplan.Max(logicalplan.Col("timestamp")),
				}
				require.NoError(
					t,
					snapshotEngine.ScanTable(testCase.name).Aggregate(max, nil).Execute(ctx,
						func(_ context.Context,
							r arrow.Record,
						) error {
							require.Equal(
								t, testCase.expMaxTimestamp, int(r.Column(0).(*array.Int64).Int64Values()[0]),
							)
							return nil
						}),
				)
			}
			// Reset sync.Maps so reflect.DeepEqual can be used below.
			db.tables[testCase.name].schema.ResetWriters()
			db.tables[testCase.name].schema.ResetBuffers()
			require.Equal(t, db.tables[testCase.name].config, snapshotDB.tables[testCase.name].config)
		}
	})

	t.Run("WithConcurrentWrites", func(t *testing.T) {
		cancelCtx, cancelWrites := context.WithCancel(ctx)

		c, err := New(
			WithStoragePath(t.TempDir()),
			WithWAL(),
		)
		require.NoError(t, err)
		defer c.Close()

		db, err := c.DB(ctx, "test")
		require.NoError(t, err)

		config := NewTableConfig(dynparquet.SampleDefinition())
		const tableName = "table"
		table, err := db.Table(tableName, config)
		require.NoError(t, err)

		highWatermarkAtStart := db.highWatermark.Load()
		shouldStartSnapshotChan := make(chan struct{})
		var errg errgroup.Group
		errg.Go(func() error {
			ts := int64(highWatermarkAtStart)
			for cancelCtx.Err() == nil {
				tx := insertSamples(ctx, t, table, ts)
				// This check simply ensures that the assumption that inserting
				// timestamp n corresponds to the n+1th transaction (the +1
				// corresponding to table creation). This assumption is required
				// by the snapshot.
				require.Equal(t, uint64(ts+1), tx)
				ts++
				if ts == 10 {
					close(shouldStartSnapshotChan)
				}
			}
			return nil
		})
		// Wait until some writes have happened.
		<-shouldStartSnapshotChan
		defer cancelWrites()
		snapshotDB, err := c.DB(ctx, "testsnapshot")
		require.NoError(t, err)
		require.NoError(t, db.snapshotAtTX(ctx, db.highWatermark.Load()))
		snapshotTx, err := snapshotDB.loadLatestSnapshotFromDir(ctx, db.snapshotsDir())
		require.NoError(t, err)
		require.NoError(
			t,
			query.NewEngine(
				memory.DefaultAllocator, snapshotDB.TableProvider(),
			).ScanTable(tableName).Aggregate(
				[]logicalplan.Expr{logicalplan.Max(logicalplan.Col("timestamp"))}, nil,
			).Execute(ctx, func(ctx context.Context, r arrow.Record) error {
				require.Equal(
					t, int(snapshotTx-highWatermarkAtStart), int(r.Column(0).(*array.Int64).Int64Values()[0]),
				)
				return nil
			}),
		)
		cancelWrites()
		require.NoError(t, errg.Wait())
	})
}

// TestSnapshotWithWAL verifies that the interaction between snapshots and WAL
// entries works as expected. In general, snapshots should occur when a table
// block is rotated out.
func TestSnapshotWithWAL(t *testing.T) {
	const dbAndTableName = "test"
	var (
		ctx                 = context.Background()
		dir                 = t.TempDir()
		snapshotTx          uint64
		firstWriteTimestamp int64
	)
	func() {
		c, err := New(
			WithWAL(),
			WithStoragePath(dir),
			WithSnapshotTriggerSize(1),
		)
		require.NoError(t, err)
		defer c.Close()

		db, err := c.DB(ctx, dbAndTableName)
		require.NoError(t, err)

		schema := dynparquet.SampleDefinition()
		table, err := db.Table(dbAndTableName, NewTableConfig(schema))
		require.NoError(t, err)

		samples := dynparquet.NewTestSamples()
		firstWriteTimestamp = samples[0].Timestamp
		for i := range samples {
			samples[i].Timestamp = firstWriteTimestamp
		}
		ctx := context.Background()

		buf, err := samples.ToBuffer(table.schema)
		require.NoError(t, err)
		_, err = table.InsertBuffer(ctx, buf)
		require.NoError(t, err)

		// No snapshots should have happened yet.
		_, err = os.ReadDir(db.snapshotsDir())
		require.ErrorIs(t, err, os.ErrNotExist)

		for i := range samples {
			samples[i].Timestamp = firstWriteTimestamp + 1
		}
		buf, err = samples.ToBuffer(table.schema)
		require.NoError(t, err)
		// With this new insert, a snapshot should be triggered.
		_, err = table.InsertBuffer(ctx, buf)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			files, err := os.ReadDir(db.snapshotsDir())
			require.NoError(t, err)
			if len(files) != 1 {
				return false
			}
			snapshotTx, err = strconv.ParseUint(files[0].Name()[:20], 10, 64)
			require.NoError(t, err)
			return true
		}, 1*time.Second, 10*time.Millisecond, "expected a snapshot on disk")
	}()

	verifyC, err := New(
		WithWAL(),
		WithStoragePath(dir),
		// Snapshot trigger size is not needed here, we only want to use this
		// column store to verify correctness.
	)
	require.NoError(t, err)
	defer verifyC.Close()

	verifyDB, err := verifyC.DB(ctx, dbAndTableName)
	require.NoError(t, err)

	// Truncate all entries from the WAL up to but not including the second
	// insert.
	require.NoError(t, verifyDB.wal.Truncate(snapshotTx+1))

	engine := query.NewEngine(memory.DefaultAllocator, verifyDB.TableProvider())
	require.NoError(
		t,
		engine.ScanTable(dbAndTableName).
			Aggregate(
				[]logicalplan.Expr{logicalplan.Min(logicalplan.Col("timestamp"))},
				nil,
			).Execute(ctx, func(_ context.Context, r arrow.Record) error {
			// This check verifies that the snapshot data (i.e. the first
			// write) is correctly loaded.
			require.Equal(t, firstWriteTimestamp, r.Column(0).(*array.Int64).Value(0))
			return nil
		}),
	)
	require.NoError(
		t,
		engine.ScanTable(dbAndTableName).
			Aggregate(
				[]logicalplan.Expr{logicalplan.Max(logicalplan.Col("timestamp"))},
				nil,
			).Execute(ctx, func(_ context.Context, r arrow.Record) error {
			// This check verifies that the write that is only represented in
			// WAL entries is still replayed (i.e. the second write) in the
			// presence of a snapshot.
			require.Equal(t, firstWriteTimestamp+1, r.Column(0).(*array.Int64).Value(0))
			return nil
		}),
	)
}
