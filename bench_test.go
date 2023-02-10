package frostdb

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/polarsignals/frostdb/pqarrow/arrowutils"
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

var typeColumns = []logicalplan.Expr{
	logicalplan.Col("name"),
	logicalplan.Col("sample_type"),
	logicalplan.Col("sample_unit"),
	logicalplan.Col("period_type"),
	logicalplan.Col("period_unit"),
}

func getTypesQuery(engine *query.LocalEngine) query.Builder {
	return engine.ScanTable(tableName).
		Distinct(
			append(
				typeColumns,
				logicalplan.Col("duration").Gt(logicalplan.Literal(0)),
			)...,
		)
}

func getLabelsQuery(engine *query.LocalEngine) query.Builder {
	return engine.ScanSchema(tableName).
		Distinct(logicalplan.Col("name")).
		Filter(logicalplan.Col("name").RegexMatch("^labels\\..+$"))
}

func getValuesForLabelQuery(engine *query.LocalEngine, labelName string) query.Builder {
	return engine.ScanTable(tableName).
		Distinct(logicalplan.Col(labelName))
}

type typesResult [][]string

func (t typesResult) Len() int {
	return len(t)
}

func (t typesResult) Less(i, j int) bool {
	for col := 0; col < len(t[0]); col++ {
		if t[i][col] == t[j][col] {
			continue
		}
		return t[i][col] < t[j][col]
	}
	return false
}

func (t *typesResult) Swap(i, j int) {
	(*t)[i], (*t)[j] = (*t)[j], (*t)[i]
}

// getDeterministicTypeFilterExpr will always return a deterministic profile
// type across benchmark runs, as well as a pretty string to print this type.
func getDeterministicTypeFilterExpr(
	ctx context.Context, engine *query.LocalEngine,
) ([]logicalplan.Expr, string, error) {
	results := make(typesResult, 0)
	if err := getTypesQuery(engine).Execute(ctx, func(_ context.Context, r arrow.Record) error {
		for i := 0; i < int(r.NumRows()); i++ {
			row := make([]string, 0, len(typeColumns))
			for j := range typeColumns {
				v, err := arrowutils.GetValue(r.Column(j), i)
				if err != nil {
					return err
				}
				row = append(row, string(v.([]byte)))
			}
			results = append(results, row)
		}
		return nil
	}); err != nil {
		return nil, "", err
	}

	if len(results) == 0 {
		return nil, "", errors.New("no types found")
	}
	sort.Sort(&results)

	filterExpr := make([]logicalplan.Expr, 0, len(typeColumns))
	for i, toMatch := range results[0] {
		filterExpr = append(
			filterExpr,
			typeColumns[i].(*logicalplan.Column).Eq(logicalplan.Literal(toMatch)),
		)
	}
	return filterExpr, strings.Join(results[0], ":"), nil
}

// getDeterministicLabel will always return a deterministic label/value pair
// across benchmarks.
func getDeterministicLabelValuePair(ctx context.Context, engine *query.LocalEngine) (string, string, error) {
	labels := make([]string, 0)
	if err := getLabelsQuery(engine).Execute(ctx, func(_ context.Context, r arrow.Record) error {
		arr := r.Column(0)
		for i := 0; i < arr.Len(); i++ {
			labels = append(labels, arr.(*array.String).Value(i))
		}
		return nil
	}); err != nil {
		return "", "", err
	}
	sort.Strings(labels)
	for _, label := range labels {
		values := make([]string, 0)
		if err := getValuesForLabelQuery(engine, label).Execute(ctx, func(ctx context.Context, r arrow.Record) error {
			arr := r.Column(0)
			for i := 0; i < arr.Len(); i++ {
				if arr.IsNull(i) {
					continue
				}
				v, err := arrowutils.GetValue(arr, i)
				if err != nil {
					return err
				}
				values = append(values, string(v.([]byte)))
			}
			return nil
		}); err != nil {
			return "", "", err
		}
		if len(values) == 0 {
			continue
		}
		sort.Strings(values)
		return label, values[0], nil
	}
	return "", "", errors.New("no non-null values found")
}

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
	label, value, err := getDeterministicLabelValuePair(ctx, engine)
	require.NoError(b, err)
	typeFilter, filterPretty, err := getDeterministicTypeFilterExpr(ctx, engine)
	require.NoError(b, err)

	b.Logf("using label/value pair: (label=%s,value=%s)", label, value)
	b.Logf("using types filter: %s", filterPretty)

	fullFilter := append(
		typeFilter,
		logicalplan.Col(label).Eq(logicalplan.Literal(value)),
		logicalplan.Col("timestamp").Gt(logicalplan.Literal(start)),
		logicalplan.Col("timestamp").Lt(logicalplan.Literal(end)),
	)

	b.Run("Types", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := getTypesQuery(engine).
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

	b.Run("Labels", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := getLabelsQuery(engine).Execute(ctx, func(ctx context.Context, r arrow.Record) error {
				if r.NumRows() == 0 {
					b.Fatal("expected at least one row")
				}
				return nil
			}); err != nil {
				b.Fatalf("query returned error: %v", err)
			}
		}
	})

	b.Run("Values", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := getValuesForLabelQuery(engine, label).
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
					logicalplan.And(fullFilter...),
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
					logicalplan.And(fullFilter...),
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
