package frostdb

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/util"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"

	"github.com/polarsignals/frostdb/dynparquet"
	schemapb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/schema/v1alpha1"
	"github.com/polarsignals/frostdb/index"
	"github.com/polarsignals/frostdb/pqarrow"
	"github.com/polarsignals/frostdb/query"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

type TestLogHelper interface {
	Helper()
	Log(args ...any)
}

type testOutput struct {
	t TestLogHelper
}

func (l *testOutput) Write(p []byte) (n int, err error) {
	l.t.Helper()
	l.t.Log(string(p))
	return len(p), nil
}

func newTestLogger(t TestLogHelper) log.Logger {
	t.Helper()
	logger := log.NewLogfmtLogger(log.NewSyncWriter(&testOutput{t: t}))
	logger = level.NewFilter(logger, level.AllowDebug())
	return logger
}

func basicTable(t *testing.T, options ...Option) (*ColumnStore, *Table) {
	config := NewTableConfig(
		dynparquet.SampleDefinition(),
	)

	logger := newTestLogger(t)

	c, err := New(
		append([]Option{WithLogger(logger)}, options...)...,
	)
	require.NoError(t, err)

	db, err := c.DB(context.Background(), "test")
	require.NoError(t, err)
	table, err := db.Table("test", config)
	require.NoError(t, err)

	return c, table
}

// This test issues concurrent writes to the database, and expects all of them to be recorded successfully.
func Test_Table_Concurrency(t *testing.T) {
	c, table := basicTable(t)
	defer c.Close()

	generateRows := func(n int) arrow.Record {
		rows := make(dynparquet.Samples, 0, n)
		for i := 0; i < n; i++ {
			rows = append(rows, dynparquet.Sample{
				Labels: map[string]string{ // TODO would be nice to not have all the same column
					"label1": "value1",
					"label2": "value2",
				},
				Stacktrace: []uuid.UUID{
					{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
					{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
				},
				Timestamp: rand.Int63(),
				Value:     rand.Int63(),
			})
		}
		r, err := rows.ToRecord()
		require.NoError(t, err)

		return r
	}

	// Spawn n workers that will insert values into the table
	maxTxID := &atomic.Uint64{}
	n := 8
	inserts := 100
	rows := 10
	wg := &sync.WaitGroup{}
	ctx := context.Background()
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < inserts; i++ {
				tx, err := table.InsertRecord(ctx, generateRows(rows))
				if err != nil {
					fmt.Println("Received error on insert: ", err)
				}

				//	 Set the max tx id that we've seen
				if maxTX := maxTxID.Load(); tx > maxTX {
					maxTxID.CompareAndSwap(maxTX, tx)
				}
			}
		}()
	}

	// Wait for all our writes to exit
	wg.Wait()

	// Wait for our last tx to be marked as complete
	table.db.Wait(maxTxID.Load())

	pool := memory.NewGoAllocator()

	err := table.View(ctx, func(ctx context.Context, tx uint64) error {
		totalrows := int64(0)
		err := table.Iterator(
			ctx,
			tx,
			pool,
			[]logicalplan.Callback{func(_ context.Context, ar arrow.Record) error {
				totalrows += ar.NumRows()

				return nil
			}},
		)

		require.NoError(t, err)
		require.Equal(t, int64(n*inserts*rows), totalrows)
		return nil
	})
	require.NoError(t, err)
}

func Benchmark_Table_Insert_1000Rows_10Iters_10Writers(b *testing.B) {
	benchmarkTableInserts(b, 1000, 10, 10)
}

func Benchmark_Table_Insert_100Rows_1000Iters_1Writers(b *testing.B) {
	benchmarkTableInserts(b, 100, 1000, 1)
}

func Benchmark_Table_Insert_100Rows_100Iters_100Writers(b *testing.B) {
	benchmarkTableInserts(b, 100, 100, 100)
}

func benchmarkTableInserts(b *testing.B, rows, iterations, writers int) {
	var (
		def    = dynparquet.SampleDefinition()
		ctx    = context.Background()
		config = NewTableConfig(def)
	)

	c, err := New(
		WithLogger(log.NewNopLogger()),
		WithWAL(),
		WithStoragePath(b.TempDir()),
	)
	require.NoError(b, err)
	defer c.Close()

	db, err := c.DB(context.Background(), "test")
	require.NoError(b, err)
	ts := &atomic.Int64{}
	generateRows := func(id string, n int) arrow.Record {
		rows := make(dynparquet.Samples, 0, n)
		for i := 0; i < n; i++ {
			rows = append(rows, dynparquet.Sample{
				Labels: map[string]string{ // TODO would be nice to not have all the same column
					"label1": id,
					"label2": "value2",
				},
				Stacktrace: []uuid.UUID{
					{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
					{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
				},
				Timestamp: ts.Add(1),
				Value:     int64(i),
			})
		}

		r, err := rows.ToRecord()
		require.NoError(b, err)
		return r
	}

	// Pre-generate all rows we're inserting
	inserts := make(map[string][]arrow.Record, writers)
	for i := 0; i < writers; i++ {
		id := uuid.New().String()
		inserts[id] = make([]arrow.Record, iterations)
		for j := 0; j < iterations; j++ {
			inserts[id][j] = generateRows(id, rows)
		}
	}

	// Run GC now so it doesn't interfere with our benchmark.
	runtime.GC()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Create table for test
		table, err := db.Table(uuid.New().String(), config)
		require.NoError(b, err)
		// Spawn n workers that will insert values into the table
		wg := &sync.WaitGroup{}
		for id := range inserts {
			wg.Add(1)
			go func(id string, tbl *Table, w *sync.WaitGroup) {
				defer w.Done()
				var (
					maxTx uint64
					err   error
				)
				for i := 0; i < iterations; i++ {
					if maxTx, err = tbl.InsertRecord(ctx, inserts[id][i]); err != nil {
						fmt.Println("Received error on insert: ", err)
					}
				}
				db.Wait(maxTx)
			}(id, table, wg)
		}
		wg.Wait()

		b.StopTimer()
		pool := memory.NewGoAllocator()
		require.NoError(b, table.EnsureCompaction())

		// Calculate the number of entries in database
		totalrows := int64(0)
		err = table.View(ctx, func(ctx context.Context, tx uint64) error {
			return table.Iterator(
				ctx,
				tx,
				pool,
				[]logicalplan.Callback{func(_ context.Context, ar arrow.Record) error {
					defer ar.Release()
					totalrows += ar.NumRows()

					return nil
				}},
			)
		})
		require.NoError(b, err)
		require.Equal(b, int64(rows*iterations*writers), totalrows)

		b.StartTimer()
	}
}

func Test_Table_ReadIsolation(t *testing.T) {
	c, table := basicTable(t)
	defer c.Close()

	samples := dynparquet.GenerateTestSamples(3)
	r, err := samples.ToRecord()
	require.NoError(t, err)

	ctx := context.Background()

	_, err = table.InsertRecord(ctx, r)
	require.NoError(t, err)

	// Perform a new insert that will have a higher tx id
	samples = dynparquet.GenerateTestSamples(1)

	r, err = samples.ToRecord()
	require.NoError(t, err)

	tx, err := table.InsertRecord(ctx, r)
	require.NoError(t, err)

	table.db.Wait(tx)

	// Reset the database to the previous tx
	table.db.resetToTxn(tx-1, nil)

	pool := memory.NewGoAllocator()
	err = table.View(ctx, func(ctx context.Context, tx uint64) error {
		rows := int64(0)
		err = table.Iterator(
			ctx,
			tx,
			pool,
			[]logicalplan.Callback{func(_ context.Context, ar arrow.Record) error {
				rows += ar.NumRows()
				return nil
			}},
		)
		require.NoError(t, err)
		require.Equal(t, int64(3), rows)
		return nil
	})
	require.NoError(t, err)

	// Now set the tx back to what it was, and perform the same read, we should return all 4 rows
	table.db.tx.Store(3)
	table.db.highWatermark.Store(3)

	err = table.View(ctx, func(ctx context.Context, _ uint64) error {
		rows := int64(0)
		err = table.Iterator(
			ctx,
			table.db.highWatermark.Load(),
			pool,
			[]logicalplan.Callback{func(_ context.Context, ar arrow.Record) error {
				rows += ar.NumRows()

				return nil
			}},
		)
		require.NoError(t, err)
		require.Equal(t, int64(4), rows)
		return nil
	})
	require.NoError(t, err)
}

func Test_Table_NewTableValidIndexDegree(t *testing.T) {
	config := NewTableConfig(dynparquet.SampleDefinition())
	c, err := New(
		WithLogger(newTestLogger(t)),
		WithIndexDegree(-1),
	)
	require.NoError(t, err)
	defer c.Close()
	db, err := c.DB(context.Background(), "test")
	require.NoError(t, err)

	_, err = db.Table("test", config)
	require.Error(t, err)
	require.Equal(t, err.Error(), "failed to create table: Table's columnStore index degree must be a positive integer (received -1)")
}

func Test_Table_NewTableValidSplitSize(t *testing.T) {
	config := NewTableConfig(
		dynparquet.SampleDefinition(),
	)

	logger := newTestLogger(t)

	c, err := New(WithLogger(logger), WithSplitSize(1))
	require.NoError(t, err)
	defer c.Close()
	db, err := c.DB(context.Background(), "test")
	require.NoError(t, err)
	_, err = db.Table("test", config)
	require.Error(t, err)
	require.Equal(t, err.Error(), "failed to create table: Table's columnStore splitSize must be a positive integer > 1 (received 1)")

	c, err = New(WithLogger(logger), WithSplitSize(-1))
	require.NoError(t, err)
	defer c.Close()
	db, err = c.DB(context.Background(), "test")
	require.NoError(t, err)
	_, err = db.Table("test", NewTableConfig(dynparquet.SampleDefinition()))
	require.Error(t, err)
	require.Equal(t, err.Error(), "failed to create table: Table's columnStore splitSize must be a positive integer > 1 (received -1)")

	c, err = New(WithLogger(logger), WithSplitSize(2))
	require.NoError(t, err)
	defer c.Close()
	db, err = c.DB(context.Background(), "test")
	require.NoError(t, err)
	_, err = db.Table("test", NewTableConfig(dynparquet.SampleDefinition()))
	require.NoError(t, err)
}

func Test_Table_Bloomfilter(t *testing.T) {
	c, table := basicTable(t, WithIndexConfig(
		[]*index.LevelConfig{
			{Level: index.L0, MaxSize: 452, Type: index.CompactionTypeParquetMemory}, // NOTE: 452 is the current size of the 3 records that are inserted
			{Level: index.L1, MaxSize: 100000},
		},
	))
	defer c.Close()

	samples := dynparquet.Samples{{
		ExampleType: "test",
		Labels: map[string]string{
			"label1": "value1",
			"label2": "value2",
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 1,
		Value:     1,
	}, {
		ExampleType: "test",
		Labels: map[string]string{
			"label1": "value2",
			"label2": "value2",
			"label3": "value3",
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     2,
	}, {
		ExampleType: "test",
		Labels: map[string]string{
			"label1": "value3",
			"label2": "value2",
			"label4": "value4",
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 3,
		Value:     3,
	}}

	for i := range samples {
		r, err := samples[i : i+1].ToRecord()
		require.NoError(t, err)

		ctx := context.Background()

		_, err = table.InsertRecord(ctx, r)
		require.NoError(t, err)
	}

	require.NoError(t, table.EnsureCompaction())

	require.Eventually(t, func() bool {
		iterations := 0
		err := table.View(context.Background(), func(_ context.Context, tx uint64) error {
			pool := memory.NewGoAllocator()

			require.NoError(t, table.Iterator(
				context.Background(),
				tx,
				pool,
				[]logicalplan.Callback{func(_ context.Context, _ arrow.Record) error {
					iterations++
					return nil
				}},
				logicalplan.WithFilter(logicalplan.Col("labels.label4").Eq(logicalplan.Literal("value4"))),
			))
			return nil
		})
		require.NoError(t, err)
		return iterations == 1
	}, time.Millisecond*60, time.Millisecond*10)
}

func Test_RecordToRow(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{
			Name:     "labels.label1",
			Type:     &arrow.StringType{},
			Nullable: true,
		},
		{
			Name:     "labels.label2",
			Type:     &arrow.StringType{},
			Nullable: true,
		},
		{
			Name: "timestamp",
			Type: &arrow.Int64Type{},
		},
		{
			Name: "value",
			Type: &arrow.Int64Type{},
		},
	}, nil)

	bld := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	t.Cleanup(bld.Release)

	bld.Field(0).(*array.StringBuilder).Append("hello")
	bld.Field(1).(*array.StringBuilder).Append("world")
	bld.Field(2).(*array.Int64Builder).Append(10)
	bld.Field(3).(*array.Int64Builder).Append(20)

	record := bld.NewRecord()

	dynschema := dynparquet.NewSampleSchema()
	ps, err := dynschema.GetDynamicParquetSchema(pqarrow.RecordDynamicCols(record))
	require.NoError(t, err)
	defer dynschema.PutPooledParquetSchema(ps)

	row, err := pqarrow.RecordToRow(ps.Schema, record, 0)
	require.NoError(t, err)
	require.Equal(t, "[<null> hello world <null> 10 20]", fmt.Sprintf("%v", row))
}

func Test_L0Query(t *testing.T) {
	c, table := basicTable(t)
	t.Cleanup(func() { c.Close() })

	samples := dynparquet.Samples{{
		ExampleType: "test",
		Labels: map[string]string{
			"label1": "value1",
			"label2": "value2",
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 1,
		Value:     1,
	}, {
		ExampleType: "test",
		Labels: map[string]string{
			"label1": "value2",
			"label2": "value2",
			"label3": "value3",
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     2,
	}, {
		ExampleType: "test",
		Labels: map[string]string{
			"label1": "value3",
			"label2": "value2",
			"label4": "value4",
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 3,
		Value:     3,
	}}

	r, err := samples.ToRecord()
	require.NoError(t, err)

	ctx := context.Background()
	_, err = table.InsertRecord(ctx, r)
	require.NoError(t, err)

	pool := memory.NewGoAllocator()

	records := 0
	err = table.View(ctx, func(ctx context.Context, tx uint64) error {
		err = table.Iterator(
			ctx,
			tx,
			pool,
			[]logicalplan.Callback{func(_ context.Context, ar arrow.Record) error {
				records++
				require.Equal(t, int64(3), ar.NumRows())
				require.Equal(t, int64(8), ar.NumCols())
				return nil
			}},
		)
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, records)
}

func Test_Serialize_DisparateDynamicColumns(t *testing.T) {
	c, table := basicTable(t)
	defer c.Close()

	samples := dynparquet.Samples{{
		ExampleType: "test",
		Labels: map[string]string{
			"label1": "value1",
			"label2": "value2",
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 1,
		Value:     1,
	}, {
		ExampleType: "test",
		Labels: map[string]string{
			"label1": "value2",
			"label2": "value2",
			"label3": "value3",
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     2,
	}, {
		ExampleType: "test",
		Labels: map[string]string{
			"label1": "value3",
			"label2": "value2",
			"label4": "value4",
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 3,
		Value:     3,
	}}

	r, err := samples.ToRecord()
	require.NoError(t, err)

	ctx := context.Background()
	_, err = table.InsertRecord(ctx, r)
	require.NoError(t, err)

	samples = dynparquet.Samples{{
		ExampleType: "test",
		Labels: map[string]string{
			"label100": "a",
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     2,
	}}

	r, err = samples.ToRecord()
	require.NoError(t, err)

	_, err = table.InsertRecord(ctx, r)
	require.NoError(t, err)

	// Serialize the table
	require.NoError(t, table.active.Serialize(io.Discard))
}

func Test_RowWriter(t *testing.T) {
	config := NewTableConfig(
		dynparquet.SampleDefinition(),
		WithRowGroupSize(5),
	)

	logger := newTestLogger(t)

	c, err := New(WithLogger(logger))
	require.NoError(t, err)

	db, err := c.DB(context.Background(), "test")
	require.NoError(t, err)
	table, err := db.Table("test", config)
	require.NoError(t, err)
	defer c.Close()

	b := &bytes.Buffer{}
	pw, err := table.schema.GetWriter(b, map[string][]string{
		"labels": {"node"},
	}, false)
	defer table.schema.PutWriter(pw)
	require.NoError(t, err)
	rowWriter, err := table.ActiveBlock().rowWriter(pw)
	require.NoError(t, err)

	// Write 17(8,9) rows, expect 3 row groups of 5 rows and 1 row group of 2 rows
	samples := dynparquet.GenerateTestSamples(8)
	buf, err := dynparquet.ToBuffer(samples, table.Schema())
	require.NoError(t, err)
	rows := buf.Rows()
	_, err = rowWriter.writeRows(rows)
	require.NoError(t, err)
	require.NoError(t, rows.Close())

	samples = dynparquet.GenerateTestSamples(9)
	buf, err = dynparquet.ToBuffer(samples, table.Schema())
	require.NoError(t, err)
	rows = buf.Rows()
	_, err = rowWriter.writeRows(rows)
	require.NoError(t, err)
	require.NoError(t, rows.Close())

	require.NoError(t, rowWriter.close())

	f, err := parquet.OpenFile(bytes.NewReader(b.Bytes()), int64(b.Len()))
	require.NoError(t, err)

	require.Equal(t, 4, len(f.Metadata().RowGroups))
	for i, rg := range f.Metadata().RowGroups {
		switch i {
		case 3:
			require.Equal(t, int64(2), rg.NumRows)
		default:
			require.Equal(t, int64(5), rg.NumRows)
		}
	}
}

// Test_Table_Size ensures the size of the table increases by the size of the inserted data.
func Test_Table_Size(t *testing.T) {
	c, table := basicTable(t)
	defer c.Close()

	before := table.ActiveBlock().Size()

	samples := dynparquet.NewTestSamples()
	rec, err := samples.ToRecord()
	require.NoError(t, err)

	ctx := context.Background()
	_, err = table.InsertRecord(ctx, rec)
	require.NoError(t, err)

	after := table.ActiveBlock().Size()
	require.Equal(t, util.TotalRecordSize(rec), after-before)
}

func Test_Insert_Repeated(t *testing.T) {
	schema := &schemapb.Schema{
		Name: "repeated",
		Columns: []*schemapb.Column{{
			Name: "name",
			StorageLayout: &schemapb.StorageLayout{
				Type:     schemapb.StorageLayout_TYPE_STRING,
				Encoding: schemapb.StorageLayout_ENCODING_RLE_DICTIONARY,
			},
		}, {
			Name: "values",
			StorageLayout: &schemapb.StorageLayout{
				Type:     schemapb.StorageLayout_TYPE_STRING,
				Encoding: schemapb.StorageLayout_ENCODING_RLE_DICTIONARY,
				Repeated: true,
			},
		}, {
			Name: "value",
			StorageLayout: &schemapb.StorageLayout{
				Type: schemapb.StorageLayout_TYPE_INT64,
			},
		}},
		SortingColumns: []*schemapb.SortingColumn{{
			Name:      "name",
			Direction: schemapb.SortingColumn_DIRECTION_ASCENDING,
		}},
	}
	config := NewTableConfig(schema)
	logger := newTestLogger(t)

	tests := map[string]struct {
		nilBeg    bool
		nilMiddle bool
		nilEnd    bool
	}{
		"beginning": {true, false, false},
		"middle":    {false, true, false},
		"end":       {false, false, true},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			c, err := New(WithLogger(logger))
			require.NoError(t, err)
			t.Cleanup(func() {
				c.Close()
			})

			db, err := c.DB(context.Background(), "test")
			require.NoError(t, err)
			table, err := db.Table("test", config)
			require.NoError(t, err)

			buffer, err := table.Schema().GetBuffer(nil)
			require.NoError(t, err)

			var row parquet.Row
			if test.nilBeg {
				row = nil
				row = append(row, parquet.ValueOf("foo").Level(0, 0, 0))
				row = append(row, parquet.ValueOf(4).Level(0, 0, 1))
				row = append(row, parquet.ValueOf(nil).Level(0, 0, 2))
				_, err = buffer.WriteRows([]parquet.Row{row})
				require.NoError(t, err)
			}

			row = nil
			row = append(row, parquet.ValueOf("foo2").Level(0, 0, 0))
			row = append(row, parquet.ValueOf(3).Level(0, 0, 1))
			row = append(row, parquet.ValueOf("bar").Level(0, 1, 2))
			row = append(row, parquet.ValueOf("baz").Level(1, 1, 2))
			_, err = buffer.WriteRows([]parquet.Row{row})
			require.NoError(t, err)

			if test.nilMiddle {
				row = nil
				row = append(row, parquet.ValueOf("foo").Level(0, 0, 0))
				row = append(row, parquet.ValueOf(4).Level(0, 0, 1))
				row = append(row, parquet.ValueOf(nil).Level(0, 0, 2))
				_, err = buffer.WriteRows([]parquet.Row{row})
				require.NoError(t, err)
			}

			row = nil
			row = append(row, parquet.ValueOf("foo3").Level(0, 0, 0))
			row = append(row, parquet.ValueOf(6).Level(0, 0, 1))
			row = append(row, parquet.ValueOf("bar").Level(0, 1, 2))
			row = append(row, parquet.ValueOf("baz").Level(1, 1, 2))
			_, err = buffer.WriteRows([]parquet.Row{row})
			require.NoError(t, err)

			if test.nilEnd {
				row = nil
				row = append(row, parquet.ValueOf("foo").Level(0, 0, 0))
				row = append(row, parquet.ValueOf(4).Level(0, 0, 1))
				row = append(row, parquet.ValueOf(nil).Level(0, 0, 2))
				_, err = buffer.WriteRows([]parquet.Row{row})
				require.NoError(t, err)
			}

			ctx := context.Background()

			// Test insertion as record
			converter := pqarrow.NewParquetConverter(memory.NewGoAllocator(), logicalplan.IterOptions{})
			defer converter.Close()

			require.NoError(t, converter.Convert(ctx, buffer, table.Schema()))
			record := converter.NewRecord()
			defer record.Release()

			_, err = table.InsertRecord(ctx, record)
			require.NoError(t, err)

			err = table.View(ctx, func(ctx context.Context, tx uint64) error {
				err = table.Iterator(
					ctx,
					tx,
					memory.NewGoAllocator(),
					[]logicalplan.Callback{func(_ context.Context, ar arrow.Record) error {
						require.Equal(t, int64(3), ar.NumRows())
						require.Equal(t, int64(3), ar.NumCols())
						return nil
					}},
				)
				require.NoError(t, err)
				return nil
			})

			engine := query.NewEngine(memory.NewGoAllocator(), db.TableProvider())
			err = engine.ScanTable("test").
				Aggregate(
					[]*logicalplan.AggregationFunction{logicalplan.Sum(logicalplan.Col("value"))},
					[]logicalplan.Expr{logicalplan.Col("values")},
				).
				Execute(context.Background(), func(_ context.Context, r arrow.Record) error {
					require.Equal(t, int64(2), r.NumRows())
					require.Equal(t, int64(2), r.NumCols())
					return nil
				})
			require.NoError(t, err)
		})
	}
}

func Test_Compact_Repeated(t *testing.T) {
	schema := &schemapb.Schema{
		Name: "repeated",
		Columns: []*schemapb.Column{{
			Name: "name",
			StorageLayout: &schemapb.StorageLayout{
				Type:     schemapb.StorageLayout_TYPE_STRING,
				Encoding: schemapb.StorageLayout_ENCODING_RLE_DICTIONARY,
			},
		}, {
			Name: "values",
			StorageLayout: &schemapb.StorageLayout{
				Type:     schemapb.StorageLayout_TYPE_STRING,
				Encoding: schemapb.StorageLayout_ENCODING_RLE_DICTIONARY,
				Repeated: true,
			},
		}, {
			Name: "value",
			StorageLayout: &schemapb.StorageLayout{
				Type: schemapb.StorageLayout_TYPE_INT64,
			},
		}},
		SortingColumns: []*schemapb.SortingColumn{{
			Name:      "name",
			Direction: schemapb.SortingColumn_DIRECTION_ASCENDING,
		}},
	}
	config := NewTableConfig(schema)
	logger := newTestLogger(t)

	c, err := New(
		WithLogger(logger),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		c.Close()
	})

	db, err := c.DB(context.Background(), "test")
	require.NoError(t, err)
	table, err := db.Table("test", config)
	require.NoError(t, err)

	buffer, err := table.Schema().GetBuffer(nil)
	require.NoError(t, err)

	var row parquet.Row
	row = nil
	row = append(row, parquet.ValueOf("foo").Level(0, 0, 0))
	row = append(row, parquet.ValueOf(4).Level(0, 0, 1))
	row = append(row, parquet.ValueOf(nil).Level(0, 0, 2))
	_, err = buffer.WriteRows([]parquet.Row{row})
	require.NoError(t, err)

	row = nil
	row = append(row, parquet.ValueOf("foo2").Level(0, 0, 0))
	row = append(row, parquet.ValueOf(3).Level(0, 0, 1))
	row = append(row, parquet.ValueOf("bar").Level(0, 1, 2))
	row = append(row, parquet.ValueOf("baz").Level(1, 1, 2))
	_, err = buffer.WriteRows([]parquet.Row{row})
	require.NoError(t, err)

	row = nil
	row = append(row, parquet.ValueOf("foo3").Level(0, 0, 0))
	row = append(row, parquet.ValueOf(6).Level(0, 0, 1))
	row = append(row, parquet.ValueOf("bar").Level(0, 1, 2))
	row = append(row, parquet.ValueOf("baz").Level(1, 1, 2))
	_, err = buffer.WriteRows([]parquet.Row{row})
	require.NoError(t, err)

	ctx := context.Background()

	converter := pqarrow.NewParquetConverter(memory.NewGoAllocator(), logicalplan.IterOptions{})
	defer converter.Close()

	require.NoError(t, converter.Convert(ctx, buffer, table.Schema()))
	before := converter.NewRecord()
	defer before.Release()

	_, err = table.InsertRecord(ctx, before)
	require.NoError(t, err)

	// Compact the record
	require.NoError(t, table.EnsureCompaction())

	// Retrieve the compacted data
	err = table.View(ctx, func(ctx context.Context, tx uint64) error {
		err = table.Iterator(
			ctx,
			tx,
			memory.NewGoAllocator(),
			[]logicalplan.Callback{func(_ context.Context, after arrow.Record) error {
				require.True(t, array.RecordEqual(before, after))
				return nil
			}},
		)
		require.NoError(t, err)
		return nil
	})
}

func Test_Table_DynamicColumnMap(t *testing.T) {
	c, err := New()
	require.NoError(t, err)
	t.Cleanup(func() {
		c.Close()
	})

	db, err := c.DB(context.Background(), "test")
	require.NoError(t, err)

	type ColMap struct {
		Name       string
		Attributes map[string]string
	}
	table, err := NewGenericTable[ColMap](
		db, "test", memory.NewGoAllocator(),
	)
	require.NoError(t, err)
	defer table.Release()

	_, err = table.Write(context.Background(), ColMap{
		Name: "albert",
		Attributes: map[string]string{
			"age": "9999",
		},
	})
	require.NoError(t, err)
}

func Test_Table_DynamicColumnNotDefined(t *testing.T) {
	c, err := New()
	require.NoError(t, err)
	t.Cleanup(func() {
		c.Close()
	})

	db, err := c.DB(context.Background(), "test")
	require.NoError(t, err)

	type ColMap struct {
		Name       string
		Attributes map[string]string
	}

	table, err := NewGenericTable[ColMap](
		db, "test", memory.NewGoAllocator(),
	)
	require.NoError(t, err)
	defer table.Release()

	_, err = table.Write(context.Background(), ColMap{
		Name: "albert",
	})
	require.NoError(t, err)
}

func TestTableUniquePrimaryIndex(t *testing.T) {
	c, err := New(WithIndexConfig([]*index.LevelConfig{
		{Level: index.L0, MaxSize: 180, Type: index.CompactionTypeParquetMemory},
		{Level: index.L1, MaxSize: 1 * TiB},
	}))
	require.NoError(t, err)
	defer c.Close()

	db, err := c.DB(context.Background(), "test")
	require.NoError(t, err)

	type Record struct {
		Name string `frostdb:",asc"`
	}
	const tableName = "test"
	table, err := NewGenericTable[Record](
		db, tableName, memory.NewGoAllocator(), WithUniquePrimaryIndex(true),
	)
	require.NoError(t, err)
	defer table.Release()

	const numRecords = 9
	for i := 0; i < numRecords; i++ {
		_, err = table.Write(context.Background(), Record{
			Name: "duplicate",
		})
		require.NoError(t, err)
	}

	rowsRead := 0
	require.NoError(t, query.NewEngine(
		memory.DefaultAllocator,
		db.TableProvider()).ScanTable(tableName).Execute(
		context.Background(), func(_ context.Context, r arrow.Record) error {
			rowsRead += int(r.NumRows())
			return nil
		}))
	// Duplicates are only dropped after compaction.
	require.Equal(t, numRecords, rowsRead)

	// Trigger compaction with a new record.
	_, err = table.Write(context.Background(), Record{
		Name: "duplicate",
	})
	require.NoError(t, err)

	require.NoError(t, err)
	require.NoError(t, table.ActiveBlock().EnsureCompaction())

	rowsRead = 0
	require.NoError(t, query.NewEngine(
		memory.DefaultAllocator,
		db.TableProvider()).ScanTable(tableName).Execute(
		context.Background(), func(_ context.Context, r arrow.Record) error {
			rowsRead += int(r.NumRows())
			return nil
		}))
	require.Equal(t, 1, rowsRead)
}

func TestTable_write_ptr_struct(t *testing.T) {
	columnstore, err := New()
	require.Nil(t, err)
	defer columnstore.Close()

	database, err := columnstore.DB(context.Background(), "simple_db")
	require.Nil(t, err)

	table, err := NewGenericTable[*dynparquet.Sample](
		database, "simple_table", memory.NewGoAllocator(),
	)
	require.Nil(t, err)
	defer table.Release()

	_, err = table.Write(context.Background(), &dynparquet.Sample{
		ExampleType: "ptr",
	})
	require.Nil(t, err)
}

func Test_Issue685(t *testing.T) {
	c, err := New()
	require.NoError(t, err)
	t.Cleanup(func() {
		c.Close()
	})

	db, err := c.DB(context.Background(), "test")
	require.NoError(t, err)

	type ColMap struct {
		Value      map[string]int64
		Attributes map[string]string
	}
	table, err := NewGenericTable[ColMap](
		db, "test", memory.NewGoAllocator(),
	)
	require.NoError(t, err)
	defer table.Release()

	_, err = table.Write(context.Background(), ColMap{
		Value: map[string]int64{
			"age": 9999,
		},
		Attributes: map[string]string{
			"age": "9999",
		},
	})
	require.NoError(t, err)

	_, err = table.Write(context.Background(), ColMap{
		Value: map[string]int64{
			"other": 1234,
			"age":   3,
		},
		Attributes: map[string]string{
			"age": "9999",
		},
	})
	require.NoError(t, err)

	engine := query.NewEngine(memory.NewGoAllocator(), db.TableProvider())
	err = engine.ScanTable("test").
		Aggregate(
			[]*logicalplan.AggregationFunction{
				logicalplan.Sum(logicalplan.Col("value.age")),
			},
			nil,
		).
		Execute(context.Background(), func(_ context.Context, r arrow.Record) error {
			require.Equal(t, int64(1), r.NumRows())
			require.Equal(t, int64(10002), r.Column(0).(*array.Int64).Value(0))
			return nil
		})
	require.NoError(t, err)
}

func Test_Issue741_Deadlock(t *testing.T) {
	logger := newTestLogger(t)
	dir := t.TempDir()
	c, err := New(
		WithIndexConfig([]*index.LevelConfig{
			{Level: index.L0, MaxSize: 1 * TiB, Type: index.CompactionTypeParquetDisk},
			{Level: index.L1, MaxSize: 1 * TiB},
		}),
		WithLogger(logger),
		WithStoragePath(dir),
		WithWAL(),
		WithManualBlockRotation(),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, c.Close())
	})

	db, err := c.DB(context.Background(), "test")
	require.NoError(t, err)

	table, err := db.Table("test", NewTableConfig(dynparquet.SampleDefinition()))
	require.NoError(t, err)

	// Insert a record
	r, err := dynparquet.GenerateTestSamples(1).ToRecord()
	require.NoError(t, err)
	ctx := context.Background()
	_, err = table.InsertRecord(ctx, r)
	require.NoError(t, err)

	// Compact the table to create a Parquet file backed record
	require.NoError(t, table.EnsureCompaction())

	// Simulate a query that is canceled
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.Error(t, table.Iterator(ctx, math.MaxUint64, memory.NewGoAllocator(), []logicalplan.Callback{func(_ context.Context, _ arrow.Record) error {
		return nil
	}}))

	// This releases all the parts and waits for all reads to finish accessing the parts. This was causing a deadlock.
	table.active.index.Close()
}
