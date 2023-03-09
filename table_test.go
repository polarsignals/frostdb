package frostdb

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/segmentio/parquet-go"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/polarsignals/frostdb/dynparquet"
	schemapb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/schema/v1alpha1"
	"github.com/polarsignals/frostdb/pqarrow"
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

func TestTable(t *testing.T) {
	c, table := basicTable(t)
	defer c.Close()

	samples := dynparquet.Samples{{
		ExampleType: "test",
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value1"},
			{Name: "label2", Value: "value2"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 1,
		Value:     1,
	}, {
		ExampleType: "test",
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value2"},
			{Name: "label2", Value: "value2"},
			{Name: "label3", Value: "value3"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     2,
	}, {
		ExampleType: "test",
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value3"},
			{Name: "label2", Value: "value2"},
			{Name: "label4", Value: "value4"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 3,
		Value:     3,
	}}

	buf, err := samples.ToBuffer(table.Schema())
	require.NoError(t, err)

	ctx := context.Background()
	_, err = table.InsertBuffer(ctx, buf)
	require.NoError(t, err)

	samples = dynparquet.Samples{{
		ExampleType: "test",
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value1"},
			{Name: "label2", Value: "value2"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     2,
	}}

	buf, err = samples.ToBuffer(table.Schema())
	require.NoError(t, err)

	_, err = table.InsertBuffer(ctx, buf)
	require.NoError(t, err)

	samples = dynparquet.Samples{{
		ExampleType: "test",
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value1"},
			{Name: "label2", Value: "value2"},
			{Name: "label3", Value: "value3"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 3,
		Value:     3,
	}}

	buf, err = samples.ToBuffer(table.Schema())
	require.NoError(t, err)

	_, err = table.InsertBuffer(ctx, buf)
	require.NoError(t, err)

	pool := memory.NewGoAllocator()

	err = table.View(ctx, func(ctx context.Context, tx uint64) error {
		return table.Iterator(
			ctx,
			tx,
			pool,
			[]logicalplan.Callback{func(ctx context.Context, ar arrow.Record) error {
				t.Log(ar)
				defer ar.Release()
				return nil
			}},
		)
	})
	require.NoError(t, err)

	uuid1 := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	uuid2 := uuid.MustParse("00000000-0000-0000-0000-000000000002")

	// One granule with 3 parts
	require.Equal(t, 1, table.active.Index().Len())
	require.Equal(t, 3, table.active.Index().Min().(*Granule).parts.Total())
	require.Equal(t, parquet.Row{
		parquet.ValueOf("test").Level(0, 0, 0),
		parquet.ValueOf("value1").Level(0, 1, 1),
		parquet.ValueOf("value2").Level(0, 1, 2),
		parquet.ValueOf(nil).Level(0, 0, 3),
		parquet.ValueOf(nil).Level(0, 0, 4),
		parquet.ValueOf(append(uuid1[:], uuid2[:]...)).Level(0, 0, 5),
		parquet.ValueOf(1).Level(0, 0, 6),
		parquet.ValueOf(1).Level(0, 0, 7),
	}, (*dynparquet.DynamicRow)(table.active.Index().Min().(*Granule).metadata.least).Row)
	require.Equal(t, 1, table.active.Index().Len())
}

// This test issues concurrent writes to the database, and expects all of them to be recorded successfully.
func Test_Table_Concurrency(t *testing.T) {
	tests := map[string]struct {
		granuleSize int64
	}{
		"25MB": {25 * 1024 * 1024},
		"15MB": {15 * 1024 * 1024},
		"8MB":  {8 * 1024 * 1024},
		"1MB":  {1024 * 1024},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			c, table := basicTable(t, WithGranuleSizeBytes(test.granuleSize))
			defer c.Close()

			generateRows := func(n int) *dynparquet.Buffer {
				rows := make(dynparquet.Samples, 0, n)
				for i := 0; i < n; i++ {
					rows = append(rows, dynparquet.Sample{
						Labels: []dynparquet.Label{ // TODO would be nice to not have all the same column
							{Name: "label1", Value: "value1"},
							{Name: "label2", Value: "value2"},
						},
						Stacktrace: []uuid.UUID{
							{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
							{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
						},
						Timestamp: rand.Int63(),
						Value:     rand.Int63(),
					})
				}
				buf, err := rows.ToBuffer(table.Schema())
				require.NoError(t, err)

				buf.Sort()

				// This is necessary because sorting a buffer makes concurrent reading not
				// safe as the internal pages are cyclically sorted at read time. Cloning
				// executes the cyclic sort once and makes the resulting buffer safe for
				// concurrent reading as it no longer has to perform the cyclic sorting at
				// read time. This should probably be improved in the parquet library.
				buf, err = buf.Clone()
				require.NoError(t, err)

				return buf
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
						tx, err := table.InsertBuffer(ctx, generateRows(rows))
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
					[]logicalplan.Callback{func(ctx context.Context, ar arrow.Record) error {
						totalrows += ar.NumRows()
						defer ar.Release()

						return nil
					}},
				)

				require.NoError(t, err)
				require.Equal(t, int64(n*inserts*rows), totalrows)
				return nil
			})
			require.NoError(t, err)
		})
	}
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

// BenchmarkInsertSimple is a benchmark used to measure the performance of the
// core insert path without the additional complexity of Benchmark_Table_Insert.
func BenchmarkInsertSimple(b *testing.B) {
	var (
		ctx    = context.Background()
		def    = dynparquet.SampleDefinition()
		config = NewTableConfig(def)
	)

	schema, err := dynparquet.SchemaFromDefinition(def)
	require.NoError(b, err)

	c, err := New()
	require.NoError(b, err)
	defer c.Close()

	db, err := c.DB(ctx, "test")
	require.NoError(b, err)

	const (
		numInserts = 100
		// In production, we see anything from a couple of rows to 60 per
		// insert at the time of writing this benchmark.
		samplesPerInsert = 30
	)
	inserts := make([][]byte, 0, numInserts)
	for i := 0; i < numInserts; i++ {
		samples := make([]dynparquet.Sample, 0, samplesPerInsert)
		for len(samples) < samplesPerInsert {
			samples = append(samples, dynparquet.NewTestSamples()...)
		}
		for _, s := range samples {
			s.Timestamp += int64(i)
		}
		buf, err := dynparquet.Samples(samples).ToBuffer(schema)
		require.NoError(b, err)
		var bytesBuf bytes.Buffer
		require.NoError(b, schema.SerializeBuffer(&bytesBuf, buf))
		inserts = append(inserts, bytesBuf.Bytes())
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		table, err := db.Table(fmt.Sprintf("test%d", i), config)
		if err != nil {
			b.Fatal(err)
		}

		for j := 0; j < numInserts; j++ {
			if _, err := table.Insert(ctx, inserts[j]); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func benchmarkTableInserts(b *testing.B, rows, iterations, writers int) {
	var (
		def    = dynparquet.SampleDefinition()
		ctx    = context.Background()
		config = NewTableConfig(def)
	)

	schema, err := dynparquet.SchemaFromDefinition(def)
	require.NoError(b, err)

	logger := log.NewNopLogger()

	c, err := New(
		WithLogger(logger),
		WithWAL(),
		WithStoragePath(b.TempDir()),
	)
	require.NoError(b, err)
	defer c.Close()

	db, err := c.DB(context.Background(), "test")
	require.NoError(b, err)
	ts := &atomic.Int64{}
	generateRows := func(id string, n int) []byte {
		rows := make(dynparquet.Samples, 0, n)
		for i := 0; i < n; i++ {
			rows = append(rows, dynparquet.Sample{
				Labels: []dynparquet.Label{ // TODO would be nice to not have all the same column
					{Name: "label1", Value: id},
					{Name: "label2", Value: "value2"},
				},
				Stacktrace: []uuid.UUID{
					{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
					{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
				},
				Timestamp: ts.Add(1),
				Value:     int64(i),
			})
		}

		buf, err := rows.ToBuffer(schema)
		require.NoError(b, err)

		buf.Sort()
		bytes := bytes.NewBuffer(nil)
		require.NoError(b, schema.SerializeBuffer(bytes, buf))
		return bytes.Bytes()
	}

	// Pre-generate all rows we're inserting
	inserts := make(map[string][][]byte, writers)
	for i := 0; i < writers; i++ {
		id := uuid.New().String()
		inserts[id] = make([][]byte, iterations)
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
					if maxTx, err = tbl.Insert(ctx, inserts[id][i]); err != nil {
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
				[]logicalplan.Callback{func(ctx context.Context, ar arrow.Record) error {
					defer ar.Release()
					totalrows += ar.NumRows()

					return nil
				}},
			)
		})
		require.Equal(b, 0., testutil.ToFloat64(table.metrics.granulesCompactionAborted))
		require.NoError(b, err)
		require.Equal(b, int64(rows*iterations*writers), totalrows)

		b.StartTimer()
	}
}

func Test_Table_ReadIsolation(t *testing.T) {
	c, table := basicTable(t)
	defer c.Close()

	samples := dynparquet.Samples{{
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value1"},
			{Name: "label2", Value: "value2"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 1,
		Value:     1,
	}, {
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value2"},
			{Name: "label2", Value: "value2"},
			{Name: "label3", Value: "value3"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     2,
	}, {
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value3"},
			{Name: "label2", Value: "value2"},
			{Name: "label4", Value: "value4"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 3,
		Value:     3,
	}}

	buf, err := samples.ToBuffer(table.Schema())
	require.NoError(t, err)

	ctx := context.Background()

	_, err = table.InsertBuffer(ctx, buf)
	require.NoError(t, err)

	// Perform a new insert that will have a higher tx id
	samples = dynparquet.Samples{{
		Labels: []dynparquet.Label{
			{Name: "blarg", Value: "blarg"},
			{Name: "blah", Value: "blah"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 1,
		Value:     1,
	}}

	buf, err = samples.ToBuffer(table.Schema())
	require.NoError(t, err)

	tx, err := table.InsertBuffer(ctx, buf)
	require.NoError(t, err)

	table.db.Wait(tx)

	// Now we cheat and reset our tx and watermark
	table.db.tx.Store(2)
	table.db.highWatermark.Store(2)

	pool := memory.NewGoAllocator()

	err = table.View(ctx, func(ctx context.Context, tx uint64) error {
		rows := int64(0)
		err = table.Iterator(
			ctx,
			tx,
			pool,
			[]logicalplan.Callback{func(ctx context.Context, ar arrow.Record) error {
				rows += ar.NumRows()
				defer ar.Release()

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

	err = table.View(ctx, func(ctx context.Context, tx uint64) error {
		rows := int64(0)
		err = table.Iterator(
			ctx,
			table.db.highWatermark.Load(),
			pool,
			[]logicalplan.Callback{func(ctx context.Context, ar arrow.Record) error {
				rows += ar.NumRows()
				defer ar.Release()

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

func Test_Table_Filter(t *testing.T) {
	c, table := basicTable(t)
	defer c.Close()

	samples := dynparquet.Samples{{
		ExampleType: "test",
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value1"},
			{Name: "label2", Value: "value2"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 1,
		Value:     1,
	}, {
		ExampleType: "test",
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value2"},
			{Name: "label2", Value: "value2"},
			{Name: "label3", Value: "value3"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     2,
	}, {
		ExampleType: "test",
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value3"},
			{Name: "label2", Value: "value2"},
			{Name: "label4", Value: "value4"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 3,
		Value:     3,
	}}

	buf, err := samples.ToBuffer(table.Schema())
	require.NoError(t, err)

	ctx := context.Background()

	_, err = table.InsertBuffer(ctx, buf)
	require.NoError(t, err)

	samples = dynparquet.Samples{{
		ExampleType: "test",
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value1"},
			{Name: "label2", Value: "value2"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     2,
	}}

	buf, err = samples.ToBuffer(table.Schema())
	require.NoError(t, err)

	_, err = table.InsertBuffer(ctx, buf)
	require.NoError(t, err)

	samples = dynparquet.Samples{{
		ExampleType: "test",
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value1"},
			{Name: "label2", Value: "value2"},
			{Name: "label3", Value: "value3"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 3,
		Value:     3,
	}}

	buf, err = samples.ToBuffer(table.Schema())
	require.NoError(t, err)

	_, err = table.InsertBuffer(ctx, buf)
	require.NoError(t, err)

	filterExpr := logicalplan.And( // Filter that excludes the granule
		logicalplan.Col("timestamp").Gt(logicalplan.Literal(-10)),
		logicalplan.Col("timestamp").Lt(logicalplan.Literal(1)),
	)

	pool := memory.NewGoAllocator()

	err = table.View(ctx, func(ctx context.Context, tx uint64) error {
		iterated := false

		err = table.Iterator(
			ctx,
			tx,
			pool,
			[]logicalplan.Callback{func(ctx context.Context, ar arrow.Record) error {
				defer ar.Release()

				iterated = true

				return nil
			}},
			logicalplan.WithFilter(filterExpr),
		)
		require.NoError(t, err)
		require.False(t, iterated)
		return nil
	})
	require.NoError(t, err)
}

func Test_Table_Bloomfilter(t *testing.T) {
	c, table := basicTable(t)
	defer c.Close()

	samples := dynparquet.Samples{{
		ExampleType: "test",
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value1"},
			{Name: "label2", Value: "value2"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 1,
		Value:     1,
	}, {
		ExampleType: "test",
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value2"},
			{Name: "label2", Value: "value2"},
			{Name: "label3", Value: "value3"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     2,
	}, {
		ExampleType: "test",
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value3"},
			{Name: "label2", Value: "value2"},
			{Name: "label4", Value: "value4"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 3,
		Value:     3,
	}}

	for i := range samples {
		buf, err := samples[i : i+1].ToBuffer(table.Schema())
		require.NoError(t, err)

		ctx := context.Background()

		_, err = table.InsertBuffer(ctx, buf)
		require.NoError(t, err)
	}

	iterations := 0
	err := table.View(context.Background(), func(ctx context.Context, tx uint64) error {
		pool := memory.NewGoAllocator()

		require.NoError(t, table.Iterator(
			context.Background(),
			tx,
			pool,
			[]logicalplan.Callback{func(ctx context.Context, ar arrow.Record) error {
				defer ar.Release()
				iterations++
				return nil
			}},
			logicalplan.WithFilter(logicalplan.Col("labels.label4").Eq(logicalplan.Literal("value4"))),
		))
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, iterations)
}

func Test_DoubleTable(t *testing.T) {
	def := &schemapb.Schema{
		Name: "test",
		Columns: []*schemapb.Column{{
			Name:          "id",
			StorageLayout: &schemapb.StorageLayout{Type: schemapb.StorageLayout_TYPE_STRING},
			Dynamic:       false,
		}, {
			Name:          "value",
			StorageLayout: &schemapb.StorageLayout{Type: schemapb.StorageLayout_TYPE_DOUBLE},
			Dynamic:       false,
		}},
		SortingColumns: []*schemapb.SortingColumn{{
			Name:      "id",
			Direction: schemapb.SortingColumn_DIRECTION_ASCENDING,
		}},
	}
	config := NewTableConfig(def)

	bucket, err := filesystem.NewBucket(t.TempDir())
	require.NoError(t, err)

	logger := newTestLogger(t)
	c, err := New(
		WithLogger(logger),
		WithBucketStorage(bucket),
	)
	require.NoError(t, err)
	defer c.Close()

	db, err := c.DB(context.Background(), "test")
	require.NoError(t, err)
	table, err := db.Table("test", config)
	require.NoError(t, err)

	b, err := table.schema.NewBuffer(nil)
	require.NoError(t, err)

	value := rand.Float64()

	_, err = b.WriteRows([]parquet.Row{{
		parquet.ValueOf("a").Level(0, 0, 0),
		parquet.ValueOf(value).Level(0, 0, 1),
	}})
	require.NoError(t, err)

	ctx := context.Background()

	n, err := table.InsertBuffer(ctx, b)
	require.NoError(t, err)

	// Read the schema from a previous transaction. Reading transaction 2 here
	// because transaction 1 is just the new block creation, therefore there
	// would be no schema to read (schemas only materialize when data is
	// inserted).
	require.Equal(t, uint64(2), n)

	err = table.View(ctx, func(ctx context.Context, tx uint64) error {
		pool := memory.NewGoAllocator()

		return table.Iterator(
			ctx,
			tx,
			pool,
			[]logicalplan.Callback{func(ctx context.Context, ar arrow.Record) error {
				defer ar.Release()
				require.Equal(t, value, ar.Column(1).(*array.Float64).Value(0))
				return nil
			}},
		)
	})
	require.NoError(t, err)
}

func Test_Table_EmptyRowGroup(t *testing.T) {
	c, table := basicTable(t)
	defer c.Close()

	ctx := context.Background()

	samples := dynparquet.Samples{{
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value1"},
			{Name: "label2", Value: "value2"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 1,
		Value:     1,
	}, {
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value2"},
			{Name: "label2", Value: "value2"},
			{Name: "label3", Value: "value3"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     2,
	}, {
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value3"},
			{Name: "label2", Value: "value2"},
			{Name: "label4", Value: "value4"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 3,
		Value:     3,
	}}

	buf, err := samples.ToBuffer(table.Schema())
	require.NoError(t, err)

	_, err = table.InsertBuffer(ctx, buf)
	require.NoError(t, err)

	// Insert new samples / buffer / rowGroup that doesn't have label1

	samples = dynparquet.Samples{{
		Labels: []dynparquet.Label{
			{Name: "foo", Value: "bar"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 1,
		Value:     1,
	}}

	buf, err = samples.ToBuffer(table.Schema())
	require.NoError(t, err)

	tx, err := table.InsertBuffer(ctx, buf)
	require.NoError(t, err)

	// Wait until data has been written.
	table.db.Wait(tx)

	pool := memory.NewGoAllocator()

	err = table.View(ctx, func(ctx context.Context, tx uint64) error {
		rows := int64(0)
		err = table.Iterator(
			ctx,
			tx,
			pool,
			// Select all distinct values for the label1 column.
			[]logicalplan.Callback{func(ctx context.Context, ar arrow.Record) error {
				rows += ar.NumRows()
				defer ar.Release()

				return nil
			}},
			logicalplan.WithProjection(&logicalplan.DynamicColumn{ColumnName: "label1"}),
			logicalplan.WithDistinctColumns(&logicalplan.DynamicColumn{ColumnName: "label1"}),
		)
		require.NoError(t, err)
		require.Equal(t, int64(0), rows)
		return nil
	})
	require.NoError(t, err)
}

func Test_Table_NestedSchema(t *testing.T) {
	schema := dynparquet.NewNestedSampleSchema(t)

	ctx := context.Background()
	config := NewTableConfig(schema)
	c, err := New(WithLogger(newTestLogger(t)))
	t.Cleanup(func() { c.Close() })
	require.NoError(t, err)
	db, err := c.DB(ctx, "nested")
	require.NoError(t, err)

	tbl, err := db.Table("nested", config)
	require.NoError(t, err)

	pb, err := tbl.schema.NewBufferV2(
		dynparquet.LabelColumn("label1"),
		dynparquet.LabelColumn("label2"),
	)

	require.NoError(t, err)

	_, err = pb.WriteRows([]parquet.Row{
		{
			parquet.ValueOf("value1").Level(0, 1, 0), // labels.label1
			parquet.ValueOf("value1").Level(0, 1, 1), // labels.label2
			parquet.ValueOf(1).Level(0, 2, 2),        // timestamps: [1]
			parquet.ValueOf(2).Level(1, 2, 2),        // timestamps: [1,2]
			parquet.ValueOf(2).Level(0, 2, 3),        // values: [2]
			parquet.ValueOf(3).Level(1, 2, 3),        // values: [2,3]
		},
	})
	require.NoError(t, err)

	_, err = tbl.InsertBuffer(ctx, pb)
	require.NoError(t, err)

	pool := memory.NewGoAllocator()

	var r arrow.Record
	records := 0
	err = tbl.View(ctx, func(ctx context.Context, tx uint64) error {
		err = tbl.Iterator(
			ctx,
			tx,
			pool,
			// Select all distinct values for the label1 column.
			[]logicalplan.Callback{func(ctx context.Context, ar arrow.Record) error {
				records++
				require.Equal(t, int64(1), ar.NumRows())
				require.Equal(t, int64(3), ar.NumCols())
				fmt.Println(ar)
				ar.Retain()
				r = ar
				return nil
			}},
		)
		require.NoError(t, err)
		return nil
	})
	t.Cleanup(r.Release)
	require.NoError(t, err)
	require.Equal(t, 1, records)

	require.Equal(t, `{{ dictionary: ["value1"]
  indices: [0] } { dictionary: ["value1"]
  indices: [0] }}`, fmt.Sprintf("%v", r.Column(0)))
	require.Equal(t, `[[1 2]]`, fmt.Sprintf("%v", r.Column(1)))
	require.Equal(t, `[[2 3]]`, fmt.Sprintf("%v", r.Column(2)))
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
	ps, err := dynschema.DynamicParquetSchema(pqarrow.RecordDynamicCols(record))
	require.NoError(t, err)

	row, err := pqarrow.RecordToRow(dynschema, ps, record, 0)
	require.NoError(t, err)
	require.Equal(t, "[<null> hello world <null> 10 20]", fmt.Sprintf("%v", row))
}

func Test_L0Query(t *testing.T) {
	c, table := basicTable(t)
	t.Cleanup(func() { c.Close() })

	samples := dynparquet.Samples{{
		ExampleType: "test",
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value1"},
			{Name: "label2", Value: "value2"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 1,
		Value:     1,
	}, {
		ExampleType: "test",
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value2"},
			{Name: "label2", Value: "value2"},
			{Name: "label3", Value: "value3"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     2,
	}, {
		ExampleType: "test",
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value3"},
			{Name: "label2", Value: "value2"},
			{Name: "label4", Value: "value4"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 3,
		Value:     3,
	}}

	ps, err := table.Schema().DynamicParquetSchema(map[string][]string{
		"labels": {"label1", "label2", "label3", "label4"},
	})
	require.NoError(t, err)

	ctx := context.Background()
	sc, err := pqarrow.ParquetSchemaToArrowSchema(ctx, ps, logicalplan.IterOptions{})
	require.NoError(t, err)

	r, err := samples.ToRecord(sc)
	require.NoError(t, err)

	_, err = table.InsertRecord(ctx, r)
	require.NoError(t, err)

	pool := memory.NewGoAllocator()

	records := 0
	err = table.View(ctx, func(ctx context.Context, tx uint64) error {
		err = table.Iterator(
			ctx,
			tx,
			pool,
			[]logicalplan.Callback{func(ctx context.Context, ar arrow.Record) error {
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

// This test checks to make sure that if a new row is added that is globally the least it shall be added as a new granule.
func Test_Table_InsertLeast(t *testing.T) {
	c, table := basicTable(t)
	defer c.Close()

	samples := dynparquet.Samples{{
		ExampleType: "test",
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value1"},
			{Name: "label2", Value: "value2"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 1,
		Value:     1,
	}, {
		ExampleType: "test",
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value2"},
			{Name: "label2", Value: "value2"},
			{Name: "label3", Value: "value3"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     2,
	}, {
		ExampleType: "test",
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value3"},
			{Name: "label2", Value: "value2"},
			{Name: "label4", Value: "value4"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 3,
		Value:     3,
	}}

	buf, err := samples.ToBuffer(table.Schema())
	require.NoError(t, err)

	ctx := context.Background()
	_, err = table.InsertBuffer(ctx, buf)
	require.NoError(t, err)

	before := table.active.Index().Len()

	samples = dynparquet.Samples{{
		ExampleType: "test",
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "a"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     2,
	}}

	buf, err = samples.ToBuffer(table.Schema())
	require.NoError(t, err)

	_, err = table.InsertBuffer(ctx, buf)
	require.NoError(t, err)

	require.Equal(t, before+1, table.active.Index().Len())
}

func Test_Serialize_DisparateDynamicColumns(t *testing.T) {
	c, table := basicTable(t)
	defer c.Close()

	samples := dynparquet.Samples{{
		ExampleType: "test",
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value1"},
			{Name: "label2", Value: "value2"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 1,
		Value:     1,
	}, {
		ExampleType: "test",
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value2"},
			{Name: "label2", Value: "value2"},
			{Name: "label3", Value: "value3"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     2,
	}, {
		ExampleType: "test",
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value3"},
			{Name: "label2", Value: "value2"},
			{Name: "label4", Value: "value4"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 3,
		Value:     3,
	}}

	buf, err := samples.ToBuffer(table.Schema())
	require.NoError(t, err)

	ctx := context.Background()
	_, err = table.InsertBuffer(ctx, buf)
	require.NoError(t, err)

	samples = dynparquet.Samples{{
		ExampleType: "test",
		Labels: []dynparquet.Label{
			{Name: "label100", Value: "a"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     2,
	}}

	buf, err = samples.ToBuffer(table.Schema())
	require.NoError(t, err)

	_, err = table.InsertBuffer(ctx, buf)
	require.NoError(t, err)

	// Serialize the table
	require.NoError(t, table.active.Serialize(io.Discard))
}

func Test_RowWriter(t *testing.T) {
	config := NewTableConfig(
		dynparquet.NewSampleSchema(),
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
	rowWriter, err := table.ActiveBlock().rowWriter(b, map[string][]string{
		"labels": {"node"},
	})
	require.NoError(t, err)

	// Write 17(8,9) rows, expect 3 row groups of 5 rows and 1 row group of 2 rows
	samples := dynparquet.GenerateTestSamples(8)
	buf, err := samples.ToBuffer(table.Schema())
	require.NoError(t, err)
	rows := buf.Rows()
	_, err = rowWriter.writeRows(rows)
	require.NoError(t, err)
	require.NoError(t, rows.Close())

	samples = dynparquet.GenerateTestSamples(9)
	buf, err = samples.ToBuffer(table.Schema())
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
