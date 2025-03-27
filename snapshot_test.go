package frostdb

import (
	"context"
	"math"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/query"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

// insertSampleRecords is the same helper function as insertSamples but it inserts arrow records instead.
func insertSampleRecords(ctx context.Context, t *testing.T, table *Table, timestamps ...int64) uint64 {
	t.Helper()
	var samples dynparquet.Samples
	samples = make([]dynparquet.Sample, 0, len(timestamps))
	for _, ts := range timestamps {
		samples = append(samples, dynparquet.Sample{
			ExampleType: "ex",
			Labels: map[string]string{
				"label1": "value1",
			},
			Stacktrace: []uuid.UUID{
				{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			},
			Timestamp: ts,
		})
	}

	ar, err := samples.ToRecord()
	require.NoError(t, err)

	tx, err := table.InsertRecord(ctx, ar)
	require.NoError(t, err)
	return tx
}

func TestSnapshot(t *testing.T) {
	ctx := context.Background()
	// Create a new DB with multiple tables and granules with
	// compacted/uncompacted parts that have a mixture of arrow/parquet records.
	t.Run("Empty", func(t *testing.T) {
		c, err := New(
			WithStoragePath(t.TempDir()),
			WithWAL(),
			WithSnapshotTriggerSize(math.MaxInt64),
		)
		require.NoError(t, err)
		defer c.Close()

		db, err := c.DB(ctx, "test")
		require.NoError(t, err)

		// Complete a txn so that the snapshot is created at txn 1, snapshots at
		// txn 0 are considered empty so ignored.
		_, _, commit := db.begin()
		commit()

		tx := db.highWatermark.Load()
		require.NoError(t, db.snapshotAtTX(ctx, tx, db.snapshotWriter(tx)))

		txBefore := db.highWatermark.Load()
		tx, err = db.loadLatestSnapshot(ctx)
		require.NoError(t, err)
		require.Equal(t, txBefore, tx)
	})

	t.Run("WithData", func(t *testing.T) {
		c, err := New(
			WithStoragePath(t.TempDir()),
			WithWAL(),
			WithSnapshotTriggerSize(math.MaxInt64),
		)
		require.NoError(t, err)
		defer c.Close()

		db, err := c.DB(ctx, "test")
		require.NoError(t, err)

		config := NewTableConfig(dynparquet.SampleDefinition())

		table, err := db.Table("table1", config)
		require.NoError(t, err)
		insertSampleRecords(ctx, t, table, 1, 2, 3)
		require.NoError(t, table.EnsureCompaction())
		insertSampleRecords(ctx, t, table, 4, 5, 6)
		insertSampleRecords(ctx, t, table, 7, 8, 9)

		const overrideConfigVal = 1234
		config.RowGroupSize = overrideConfigVal
		table, err = db.Table("table2", config)
		require.NoError(t, err)
		insertSampleRecords(ctx, t, table, 1, 2, 3)
		insertSampleRecords(ctx, t, table, 4, 5, 6)

		config.BlockReaderLimit = overrideConfigVal
		_, err = db.Table("empty", config)
		require.NoError(t, err)

		highWatermark := db.highWatermark.Load()

		// Insert a sample that should not be snapshot.
		insertSampleRecords(ctx, t, table, 10)
		require.NoError(t, db.snapshotAtTX(ctx, highWatermark, db.snapshotWriter(highWatermark)))

		// Create another db and verify.
		snapshotDB, err := c.DB(ctx, "testsnapshot")
		require.NoError(t, err)

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
				aggrMax := []*logicalplan.AggregationFunction{
					logicalplan.Max(logicalplan.Col("timestamp")),
				}
				require.NoError(
					t,
					snapshotEngine.ScanTable(testCase.name).Aggregate(aggrMax, nil).Execute(ctx,
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
			require.Equal(t, db.tables[testCase.name].config.Load(), snapshotDB.tables[testCase.name].config.Load())
		}
	})

	t.Run("WithConcurrentWrites", func(t *testing.T) {
		cancelCtx, cancelWrites := context.WithCancel(ctx)

		c, err := New(
			WithStoragePath(t.TempDir()),
			WithWAL(),
			WithSnapshotTriggerSize(math.MaxInt64),
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
				tx := insertSampleRecords(ctx, t, table, ts)
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
		tx := db.highWatermark.Load()
		require.NoError(t, db.snapshotAtTX(ctx, tx, db.snapshotWriter(tx)))
		snapshotTx, err := snapshotDB.loadLatestSnapshotFromDir(ctx, db.snapshotsDir())
		require.NoError(t, err)
		require.NoError(
			t,
			query.NewEngine(
				memory.DefaultAllocator, snapshotDB.TableProvider(),
			).ScanTable(tableName).Aggregate(
				[]*logicalplan.AggregationFunction{logicalplan.Max(logicalplan.Col("timestamp"))}, nil,
			).Execute(ctx, func(_ context.Context, r arrow.Record) error {
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

		r, err := samples.ToRecord()
		require.NoError(t, err)
		_, err = table.InsertRecord(ctx, r)
		require.NoError(t, err)

		// No snapshots should have happened yet.
		_, err = os.ReadDir(db.snapshotsDir())
		require.ErrorIs(t, err, os.ErrNotExist)

		for i := range samples {
			samples[i].Timestamp = firstWriteTimestamp + 1
		}
		r, err = samples.ToRecord()
		require.NoError(t, err)
		// With this new insert, a snapshot should be triggered.
		_, err = table.InsertRecord(ctx, r)
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
				[]*logicalplan.AggregationFunction{logicalplan.Min(logicalplan.Col("timestamp"))},
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
				[]*logicalplan.AggregationFunction{logicalplan.Max(logicalplan.Col("timestamp"))},
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

func TestSnapshotIsTakenOnUncompressedInserts(t *testing.T) {
	const dbAndTableName = "test"
	var (
		ctx = context.Background()
		dir = t.TempDir()
	)
	const (
		numInserts                     = 100
		expectedUncompressedInsertSize = 2000
	)
	c, err := New(
		WithWAL(),
		WithStoragePath(dir),
		WithSnapshotTriggerSize(expectedUncompressedInsertSize),
	)
	require.NoError(t, err)
	defer c.Close()

	db, err := c.DB(ctx, dbAndTableName)
	require.NoError(t, err)

	type model struct {
		Bytes string `frostdb:",rle_dict"`
	}
	table, err := NewGenericTable[model](
		db, dbAndTableName, memory.NewGoAllocator(),
	)
	require.NoError(t, err)
	defer table.Release()

	for i := 0; i < numInserts; i++ {
		_, err = table.Write(ctx, model{Bytes: "test"})
		require.NoError(t, err)
	}
	activeBlock := table.ActiveBlock()
	require.True(t, activeBlock.Size() == expectedUncompressedInsertSize, "expected uncompressed insert size is wrong. The test should be updated.")
	// This will force a compaction of all the inserts we have so far.
	require.NoError(t, table.EnsureCompaction())
	require.True(
		t,
		activeBlock.Size() < activeBlock.uncompressedInsertsSize.Load(),
		"expected uncompressed inserts to be larger than compressed inserts. Did the compaction run?",
	)
	require.Zero(t, activeBlock.lastSnapshotSize.Load(), "expected no snapshots to be taken so far")

	// These writes should now trigger a snapshot even though the active block
	// size is much lower than the uncompressed insert size.
	for i := 0; i < 2; i++ {
		_, err = table.Write(ctx, model{Bytes: "test"})
		require.NoError(t, err)
	}

	require.Eventually(
		t,
		func() bool {
			return activeBlock.lastSnapshotSize.Load() > 0
		},
		1*time.Second,
		10*time.Millisecond,
		"expected snapshot to be taken",
	)
}
