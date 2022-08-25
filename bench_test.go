package frostdb

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

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

func newDBForBenchmarks(ctx context.Context, b testing.TB) (*DB, error) {
	b.Helper()

	b.Logf("initializing %s", b.Name())

	col, err := New(
		log.NewNopLogger(),
		prometheus.NewRegistry(),
		trace.NewNoopTracerProvider().Tracer(""),
		WithWAL(),
		WithStoragePath(storagePath),
	)
	if err != nil {
		return nil, err
	}

	if err := col.ReplayWALs(ctx); err != nil {
		return nil, err
	}

	colDB, err := col.DB(ctx, dbName)
	if err != nil {
		return nil, err
	}

	table, err := colDB.GetTable(tableName)
	if err != nil {
		return nil, err
	}
	table.Sync()

	b.Logf("db initialized and WAL replayed, starting benchmark %s", b.Name())
	return colDB, nil
}

func getLatest15MinInterval(ctx context.Context, b testing.TB, engine *query.LocalEngine) (start, end int64) {
	b.Helper()

	var result arrow.Record
	require.NoError(b, engine.ScanTable(tableName).
		Aggregate(
			logicalplan.Max(logicalplan.Col("timestamp")),
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

func BenchmarkQueryTypes(b *testing.B) {
	b.Skip(skipReason)

	ctx := context.Background()
	db, err := newDBForBenchmarks(ctx, b)
	require.NoError(b, err)

	engine := query.NewEngine(
		memory.NewGoAllocator(),
		trace.NewNoopTracerProvider().Tracer(""),
		db.TableProvider(),
	)

	b.ResetTimer()

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
}

// BenchmarkMerge executes a merge of profiles over a 15-minute time window.
func BenchmarkQueryMerge(b *testing.B) {
	b.Skip(skipReason)

	ctx := context.Background()
	db, err := newDBForBenchmarks(ctx, b)
	require.NoError(b, err)

	engine := query.NewEngine(
		memory.NewGoAllocator(),
		trace.NewNoopTracerProvider().Tracer(""),
		db.TableProvider(),
	)
	start, end := getLatest15MinInterval(ctx, b, engine)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := engine.ScanTable(tableName).
			Filter(
				logicalplan.And(filterExprs(start, end)...),
			).
			Aggregate(
				logicalplan.Sum(logicalplan.Col("value")),
				logicalplan.Col("stacktrace"),
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
}

// BenchmarkRange gets a series of profiles over a 15-minute time window.
func BenchmarkQueryRange(b *testing.B) {
	b.Skip(skipReason)

	ctx := context.Background()
	db, err := newDBForBenchmarks(ctx, b)
	require.NoError(b, err)

	engine := query.NewEngine(
		memory.NewGoAllocator(),
		trace.NewNoopTracerProvider().Tracer(""),
		db.TableProvider(),
	)
	start, end := getLatest15MinInterval(ctx, b, engine)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := engine.ScanTable(tableName).
			Filter(
				logicalplan.And(filterExprs(start, end)...),
			).
			Aggregate(
				logicalplan.Sum(logicalplan.Col("value")),
				logicalplan.DynCol("labels"),
				logicalplan.Col("timestamp"),
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
}
