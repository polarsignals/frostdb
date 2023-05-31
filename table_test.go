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

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/segmentio/parquet-go"
	"github.com/stretchr/testify/require"

	"github.com/polarsignals/frostdb/dynparquet"
	schemapb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/schema/v1alpha1"
	"github.com/polarsignals/frostdb/pqarrow"
	"github.com/polarsignals/frostdb/pqarrow/arrowutils"
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

	ps, err := table.Schema().GetDynamicParquetSchema(map[string][]string{
		"labels": {"label1", "label2", "label3", "label4"},
	})
	require.NoError(t, err)
	defer table.Schema().PutPooledParquetSchema(ps)

	ctx := context.Background()
	sc, err := pqarrow.ParquetSchemaToArrowSchema(ctx, ps.Schema, logicalplan.IterOptions{})
	require.NoError(t, err)

	buf, err := samples.ToRecord(sc)
	require.NoError(t, err)

	_, err = table.InsertRecord(ctx, buf)
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

	ps, err = table.Schema().GetDynamicParquetSchema(map[string][]string{
		"labels": {"label1", "label2"},
	})
	require.NoError(t, err)
	defer table.Schema().PutPooledParquetSchema(ps)

	sc, err = pqarrow.ParquetSchemaToArrowSchema(ctx, ps.Schema, logicalplan.IterOptions{})
	require.NoError(t, err)

	buf, err = samples.ToRecord(sc)
	require.NoError(t, err)

	_, err = table.InsertRecord(ctx, buf)
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

	ps, err = table.Schema().GetDynamicParquetSchema(map[string][]string{
		"labels": {"label1", "label2", "label3"},
	})
	require.NoError(t, err)
	defer table.Schema().PutPooledParquetSchema(ps)

	sc, err = pqarrow.ParquetSchemaToArrowSchema(ctx, ps.Schema, logicalplan.IterOptions{})
	require.NoError(t, err)

	buf, err = samples.ToRecord(sc)
	require.NoError(t, err)

	_, err = table.InsertRecord(ctx, buf)
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

			generateRows := func(n int) arrow.Record {
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

				ps, err := table.Schema().GetDynamicParquetSchema(map[string][]string{
					"labels": {"label1", "label2"},
				})
				require.NoError(t, err)
				defer table.Schema().PutPooledParquetSchema(ps)

				ctx := context.Background()
				sc, err := pqarrow.ParquetSchemaToArrowSchema(ctx, ps.Schema, logicalplan.IterOptions{})
				require.NoError(t, err)
				buf, err := rows.ToRecord(sc)
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
	inserts := make([]arrow.Record, 0, numInserts)
	require.NoError(b, err)
	ps, err := schema.GetDynamicParquetSchema(map[string][]string{
		"labels": {"node", "namespace", "container", "pod"},
	})
	require.NoError(b, err)

	sc, err := pqarrow.ParquetSchemaToArrowSchema(ctx, ps.Schema, logicalplan.IterOptions{})
	require.NoError(b, err)

	for i := 0; i < numInserts; i++ {
		samples := make([]dynparquet.Sample, 0, samplesPerInsert)
		for len(samples) < samplesPerInsert {
			samples = append(samples, dynparquet.NewTestSamples()...)
		}
		for _, s := range samples {
			s.Timestamp += int64(i)
		}
		buf, err := dynparquet.Samples(samples).ToRecord(sc)
		require.NoError(b, err)
		inserts = append(inserts, buf)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		table, err := db.Table(fmt.Sprintf("test%d", i), config)
		if err != nil {
			b.Fatal(err)
		}

		for j := 0; j < numInserts; j++ {
			if _, err := table.InsertRecord(ctx, inserts[j]); err != nil {
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
	generateRows := func(id string, n int) arrow.Record {
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

		ps, err := schema.GetDynamicParquetSchema(map[string][]string{
			"labels": {"label1", "label2"},
		})
		require.NoError(b, err)

		ctx := context.Background()
		sc, err := pqarrow.ParquetSchemaToArrowSchema(ctx, ps.Schema, logicalplan.IterOptions{})
		require.NoError(b, err)

		buf, err := rows.ToRecord(sc)
		require.NoError(b, err)

		return buf
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

	ps, err := table.Schema().GetDynamicParquetSchema(map[string][]string{
		"labels": {"label1", "label2", "label3", "label4"},
	})
	require.NoError(t, err)
	defer table.Schema().PutPooledParquetSchema(ps)

	ctx := context.Background()
	sc, err := pqarrow.ParquetSchemaToArrowSchema(ctx, ps.Schema, logicalplan.IterOptions{})
	require.NoError(t, err)

	buf, err := samples.ToRecord(sc)
	require.NoError(t, err)

	_, err = table.InsertRecord(ctx, buf)
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

	ps, err = table.Schema().GetDynamicParquetSchema(map[string][]string{
		"labels": {"blarg", "blah"},
	})
	require.NoError(t, err)

	sc, err = pqarrow.ParquetSchemaToArrowSchema(ctx, ps.Schema, logicalplan.IterOptions{})
	require.NoError(t, err)

	buf, err = samples.ToRecord(sc)
	require.NoError(t, err)

	tx, err := table.InsertRecord(ctx, buf)
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

	row, err := pqarrow.RecordToRow(dynschema, ps.Schema, record, 0)
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

	ps, err := table.Schema().GetDynamicParquetSchema(map[string][]string{
		"labels": {"label1", "label2", "label3", "label4"},
	})
	require.NoError(t, err)
	defer table.Schema().PutPooledParquetSchema(ps)

	ctx := context.Background()
	sc, err := pqarrow.ParquetSchemaToArrowSchema(ctx, ps.Schema, logicalplan.IterOptions{})
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

	ps, err := table.Schema().GetDynamicParquetSchema(map[string][]string{
		"labels": {"label1", "label2", "label3", "label4"},
	})
	require.NoError(t, err)
	defer table.Schema().PutPooledParquetSchema(ps)

	ctx := context.Background()
	sc, err := pqarrow.ParquetSchemaToArrowSchema(ctx, ps.Schema, logicalplan.IterOptions{})
	require.NoError(t, err)

	buf, err := samples.ToRecord(sc)
	require.NoError(t, err)

	_, err = table.InsertRecord(ctx, buf)
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

	ps, err = table.Schema().GetDynamicParquetSchema(map[string][]string{
		"labels": {"label100"},
	})
	require.NoError(t, err)
	defer table.Schema().PutPooledParquetSchema(ps)

	sc, err = pqarrow.ParquetSchemaToArrowSchema(ctx, ps.Schema, logicalplan.IterOptions{})
	require.NoError(t, err)

	buf, err = samples.ToRecord(sc)
	require.NoError(t, err)

	_, err = table.InsertRecord(ctx, buf)
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

// Test_Table_Size ensures the size of the table increases by the size of the inserted data.
func Test_Table_Size(t *testing.T) {
	t.Skip("unimplemented")
	test := func(isArrow bool) {
		c, table := basicTable(t)
		defer c.Close()

		before := table.ActiveBlock().Size()

		samples := dynparquet.NewTestSamples()
		switch isArrow {
		case true:
			ps, err := table.Schema().GetDynamicParquetSchema(map[string][]string{
				"labels": {"node", "namespace", "container"},
			})
			require.NoError(t, err)
			defer table.Schema().PutPooledParquetSchema(ps)

			ctx := context.Background()
			sc, err := pqarrow.ParquetSchemaToArrowSchema(ctx, ps.Schema, logicalplan.IterOptions{})
			require.NoError(t, err)

			rec, err := samples.ToRecord(sc)
			require.NoError(t, err)

			_, err = table.InsertRecord(ctx, rec)
			require.NoError(t, err)

			after := table.ActiveBlock().Size()
			require.Equal(t, arrowutils.RecordSize(rec), after-before)
		}
	}

	t.Run("arrow", func(t *testing.T) {
		test(true)
	})

	t.Run("parquet", func(t *testing.T) {
		test(false)
	})
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

			require.NoError(t, converter.Convert(ctx, buffer))
			record := converter.NewRecord()
			defer record.Release()

			_, err = table.InsertRecord(ctx, record)
			require.NoError(t, err)

			err = table.View(ctx, func(ctx context.Context, tx uint64) error {
				err = table.Iterator(
					ctx,
					tx,
					memory.NewGoAllocator(),
					[]logicalplan.Callback{func(ctx context.Context, ar arrow.Record) error {
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
					[]logicalplan.Expr{logicalplan.Sum(logicalplan.Col("value"))},
					[]logicalplan.Expr{logicalplan.Col("values")},
				).
				Execute(context.Background(), func(ctx context.Context, r arrow.Record) error {
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
		WithGranuleSizeBytes(10), // NOTE: set small granule size to force compaction
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

	require.NoError(t, converter.Convert(ctx, buffer))
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
			[]logicalplan.Callback{func(ctx context.Context, after arrow.Record) error {
				require.True(t, array.RecordEqual(before, after))
				return nil
			}},
		)
		require.NoError(t, err)
		return nil
	})
}
