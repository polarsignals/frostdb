package frostdb

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/polarsignals/frostdb/dynparquet"
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

	b.Logf("recovering DB")
	start := time.Now()
	col, err := New(
		WithWAL(),
		WithStoragePath(storagePath),
	)
	if err != nil {
		return nil, nil, err
	}
	b.Logf("recovered DB in %s", time.Since(start))

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

	b.Logf("db initialized and recovered, starting benchmark %s", b.Name())
	return col, colDB, nil
}

func getLatest15MinInterval(ctx context.Context, b testing.TB, engine *query.LocalEngine) (start, end int64) {
	b.Helper()

	var result arrow.Record
	require.NoError(b, engine.ScanTable(tableName).
		Aggregate(
			[]*logicalplan.AggregationFunction{
				logicalplan.Max(logicalplan.Col("timestamp")),
			},
			nil,
		).Execute(ctx,
		func(_ context.Context, r arrow.Record) error {
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

// getCPUTypeFilter will always return a deterministic profile
// type across benchmark runs, as well as a pretty string to print this type.
func getCPUTypeFilter(
	ctx context.Context, engine *query.LocalEngine,
) ([]logicalplan.Expr, string, error) {
	results := make(typesResult, 0)
	if err := getTypesQuery(engine).Execute(ctx, func(_ context.Context, r arrow.Record) error {
		nameIdx := r.Schema().FieldIndices("name")[0]
		for i := 0; i < int(r.NumRows()); i++ {
			if !strings.Contains(string(r.Column(nameIdx).(*array.Dictionary).GetOneForMarshal(i).([]byte)), "cpu") {
				// Not a CPU profile type, ignore.
				continue
			}
			row := make([]string, 0, len(typeColumns))
			for j := range typeColumns {
				v := r.Column(j).GetOneForMarshal(i)
				row = append(row, string(v.([]byte)))
			}
			results = append(results, row)
		}
		return nil
	}); err != nil {
		return nil, "", err
	}

	if len(results) == 0 {
		return nil, "", errors.New("no cpu types found")
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
func getDeterministicLabelValuePairForType(ctx context.Context, engine *query.LocalEngine, typeFilter []logicalplan.Expr) (string, string, error) {
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
		if err := engine.ScanTable(tableName).
			Filter(logicalplan.And(typeFilter...)).
			Distinct(logicalplan.Col(label)).
			Execute(ctx, func(_ context.Context, r arrow.Record) error {
				arr := r.Column(0)
				for i := 0; i < arr.Len(); i++ {
					if arr.IsNull(i) {
						continue
					}
					v := arr.GetOneForMarshal(i)
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
	typeFilter, filterPretty, err := getCPUTypeFilter(ctx, engine)
	require.NoError(b, err)

	start, end := getLatest15MinInterval(ctx, b, engine)
	label, value, err := getDeterministicLabelValuePairForType(ctx, engine, typeFilter)
	require.NoError(b, err)

	b.Logf("using types filter: %s", filterPretty)
	b.Logf("using label/value pair: (label=%s,value=%s)", label, value)

	fullFilter := append(
		typeFilter,
		logicalplan.Col(label).Eq(logicalplan.Literal(value)),
		logicalplan.Col("timestamp").Gt(logicalplan.Literal(start)),
		logicalplan.Col("timestamp").Lt(logicalplan.Literal(end)),
	)

	b.Run("Types", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := getTypesQuery(engine).
				Execute(ctx, func(_ context.Context, r arrow.Record) error {
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
			if err := getLabelsQuery(engine).Execute(ctx, func(_ context.Context, r arrow.Record) error {
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
				Execute(ctx, func(_ context.Context, r arrow.Record) error {
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
					[]*logicalplan.AggregationFunction{
						logicalplan.Sum(logicalplan.Col("value")),
					},
					[]logicalplan.Expr{
						logicalplan.Col("stacktrace"),
					},
				).
				Execute(ctx, func(_ context.Context, r arrow.Record) error {
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
					[]*logicalplan.AggregationFunction{
						logicalplan.Sum(logicalplan.Col("value")),
					},
					[]logicalplan.Expr{
						logicalplan.DynCol("labels"),
						logicalplan.Col("timestamp"),
					},
				).
				Execute(ctx, func(_ context.Context, r arrow.Record) error {
					if r.NumRows() == 0 {
						b.Fatal("expected at least one row")
					}
					return nil
				}); err != nil {
				b.Fatalf("query returned error: %v", err)
			}
		}
	})

	// BenchmarkFilter benchmarks performing a simple filter. This is useful to get an idea of our scan speed (minus the
	// execution engine).
	b.Run("Filter", func(b *testing.B) {
		schema := dynparquet.SampleDefinition()
		table, err := db.Table(tableName, NewTableConfig(schema))
		require.NoError(b, err)
		size := table.ActiveBlock().Size()
		for i := 0; i < b.N; i++ {
			if err := engine.ScanTable(tableName).
				Filter(
					logicalplan.And(fullFilter...),
				).
				Execute(ctx, func(_ context.Context, r arrow.Record) error {
					if r.NumRows() == 0 {
						b.Fatal("expected at least one row")
					}
					return nil
				}); err != nil {
				b.Fatalf("query returned error: %v", err)
			}
		}
		b.ReportMetric(float64(size)/(float64(b.Elapsed().Milliseconds())/float64(b.N)), "B/msec")
	})
}

func BenchmarkReplay(b *testing.B) {
	b.Skip(skipReason)

	for i := 0; i < b.N; i++ {
		func() {
			col, err := New(
				WithWAL(),
				WithStoragePath(storagePath),
			)
			require.NoError(b, err)
			defer col.Close()
		}()
	}
}

type writeCounter struct {
	io.Writer
	count int
}

func (wc *writeCounter) Write(p []byte) (int, error) {
	count, err := wc.Writer.Write(p)
	wc.count += count
	return count, err
}

func BenchmarkSnapshot(b *testing.B) {
	b.Skip(skipReason)

	ctx := context.Background()
	col, err := New(
		WithWAL(),
		WithStoragePath(storagePath),
	)
	require.NoError(b, err)
	defer col.Close()

	db, err := col.DB(ctx, dbName)
	require.NoError(b, err)

	b.Log("recovered DB, starting benchmark")

	b.ResetTimer()
	bytesWritten := 0
	for i := 0; i < b.N; i++ {
		wc := &writeCounter{Writer: io.Discard}
		require.NoError(b, WriteSnapshot(ctx, db.HighWatermark(), db, wc))
		bytesWritten += wc.count
	}
	b.ReportMetric(float64(bytesWritten)/float64(b.N), "size/op")
}

func NewTestSamples(num int) dynparquet.Samples {
	samples := make(dynparquet.Samples, 0, num)
	for i := 0; i < num; i++ {
		samples = append(samples,
			dynparquet.Sample{
				ExampleType: "cpu",
				Labels: map[string]string{
					"node": "test3",
				},
				Stacktrace: []uuid.UUID{
					{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
					{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
				},
				Timestamp: rand.Int63n(100000),
				Value:     rand.Int63(),
			},
		)
	}
	return samples
}

func Benchmark_Serialize(b *testing.B) {
	ctx := context.Background()
	schema := dynparquet.SampleDefinition()

	col, err := New()
	require.NoError(b, err)
	defer col.Close()

	db, err := col.DB(ctx, "test")
	require.NoError(b, err)

	tbl, err := db.Table("test", NewTableConfig(schema))
	require.NoError(b, err)

	// Insert 10k rows
	samples := NewTestSamples(10000)
	r, err := samples.ToRecord()
	require.NoError(b, err)

	_, err = tbl.InsertRecord(ctx, r)
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Serialize the table
		require.NoError(b, tbl.active.Serialize(io.Discard))
	}
}
