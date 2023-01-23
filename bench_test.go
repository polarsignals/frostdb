package frostdb

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/polarsignals/frostdb/query"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

const (
	dbName      = "parca"
	tableName   = "stacktraces"
	storagePath = "testdata/data"
	skipReason  = "requires data directory"
)

// This file runs end-to-end benchmarks on a single FrostDB instance using
// Polar Signals-specific data and schema. These benchmarks are skipped by
// default since they require a manually-generated data directory to run. To run
// these benchmarks, unskip the benchmarks and create a data directory in
// testdata/data.

func newDBForBenchmarks(ctx context.Context, b testing.TB) (*ColumnStore, *DB, error) {
	b.Helper()

	col, err := New(
		WithWAL(),
		WithStoragePath(storagePath),
	)
	if err != nil {
		return nil, nil, err
	}

	b.Logf("replaying WAL")
	start := time.Now()
	if err := col.ReplayWALs(ctx); err != nil {
		return nil, nil, err
	}
	b.Logf("replayed WAL in %s", time.Since(start))

	colDB, err := col.DB(ctx, dbName)
	if err != nil {
		return nil, nil, err
	}

	table, err := colDB.GetTable(tableName)
	if err != nil {
		return nil, nil, err
	}
	b.Logf("ensuring compaction")
	start = time.Now()
	if err := table.EnsureCompaction(); err != nil {
		return nil, nil, err
	}
	b.Logf("ensured compaction in %s", time.Since(start))

	b.Logf("db initialized and WAL replayed, starting benchmark %s", b.Name())
	return col, colDB, nil
}

func getLatest15MinInterval(ctx context.Context, b testing.TB, engine *query.LocalEngine) (start, end int64) {
	b.Helper()

	var result arrow.Record
	require.NoError(b, engine.ScanTable(tableName).
		Aggregate(
			[]logicalplan.Expr{
				logicalplan.Max(logicalplan.Col("timestamp")),
			},
			nil,
		).Execute(ctx,
		func(ctx context.Context, r arrow.Record) error {
			r.Retain()
			result = r
			return nil
		}))
	defer result.Release()

	require.Equal(b, int64(1), result.NumCols())
	require.Equal(b, int64(1), result.NumRows())

	end = result.Column(0).(*array.Int64).Int64Values()[0]
	// start will be 15 minutes before end.
	start = time.UnixMilli(end).Add(-(15 * time.Minute)).UnixMilli()
	require.True(b, start < end)
	return start, end
}

// filterExpr returns a hardcoded filter expression to filter for cpu profiles
// within a given timestamp range.
func filterExprs(start, end int64) []logicalplan.Expr {
	return []logicalplan.Expr{
		logicalplan.Col("name").Eq(logicalplan.Literal("process_cpu")),
		logicalplan.Col("sample_type").Eq(logicalplan.Literal("cpu")),
		logicalplan.Col("sample_unit").Eq(logicalplan.Literal("nanoseconds")),
		logicalplan.Col("period_type").Eq(logicalplan.Literal("cpu")),
		logicalplan.Col("period_unit").Eq(logicalplan.Literal("nanoseconds")),
		logicalplan.Col("labels.job").Eq(logicalplan.Literal("default")),
		logicalplan.Col("timestamp").Gt(logicalplan.Literal(start)),
		logicalplan.Col("timestamp").Lt(logicalplan.Literal(end)),
	}
}

// Remove unused warning.
var (
	_ = newDBForBenchmarks
	_ = getLatest15MinInterval
	_ = filterExprs
)

func BenchmarkQuery(b *testing.B) {
	b.Skip(skipReason)

	ctx := context.Background()
	c, db, err := newDBForBenchmarks(ctx, b)
	require.NoError(b, err)
	defer c.Close()

	engine := query.NewEngine(
		memory.NewGoAllocator(),
		db.TableProvider(),
	)
	start, end := getLatest15MinInterval(ctx, b, engine)

	b.Run("Types", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := engine.ScanTable(tableName).
				Distinct(
					logicalplan.Col("name"),
					logicalplan.Col("sample_type"),
					logicalplan.Col("sample_unit"),
					logicalplan.Col("period_type"),
					logicalplan.Col("period_unit"),
					logicalplan.Col("duration").Gt(logicalplan.Literal(0)),
				).
				Execute(ctx, func(ctx context.Context, r arrow.Record) error {
					if r.NumRows() == 0 {
						b.Fatal("expected at least one row")
					}
					return nil
				}); err != nil {
				b.Fatalf("query returned error: %v", err)
			}
		}
	})

	// Merge executes a merge of profiles over a 15-minute time window.
	b.Run("Merge", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := engine.ScanTable(tableName).
				Filter(
					logicalplan.And(filterExprs(start, end)...),
				).
				Aggregate(
					[]logicalplan.Expr{
						logicalplan.Sum(logicalplan.Col("value")),
					},
					[]logicalplan.Expr{
						logicalplan.Col("stacktrace"),
					},
				).
				Execute(ctx, func(ctx context.Context, r arrow.Record) error {
					if r.NumRows() == 0 {
						b.Fatal("expected at least one row")
					}
					return nil
				}); err != nil {
				b.Fatalf("query returned error: %v", err)
			}
		}
	})

	// Range gets a series of profiles over a 15-minute time window.
	b.Run("Range", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := engine.ScanTable(tableName).
				Filter(
					logicalplan.And(filterExprs(start, end)...),
				).
				Aggregate(
					[]logicalplan.Expr{
						logicalplan.Sum(logicalplan.Col("value")),
					},
					[]logicalplan.Expr{
						logicalplan.DynCol("labels"),
						logicalplan.Col("timestamp"),
					},
				).
				Execute(ctx, func(ctx context.Context, r arrow.Record) error {
					if r.NumRows() == 0 {
						b.Fatal("expected at least one row")
					}
					return nil
				}); err != nil {
				b.Fatalf("query returned error: %v", err)
			}
		}
	})
}
