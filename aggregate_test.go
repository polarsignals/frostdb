package frostdb

import (
	"context"
	"math/rand"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/polarsignals/frostdb/dynparquet"
	schemapb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/schema/v1alpha1"
	"github.com/polarsignals/frostdb/query"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

func TestAggregateInconsistentSchema(t *testing.T) {
	config := NewTableConfig(
		dynparquet.SampleDefinition(),
	)

	logger := newTestLogger(t)

	c, err := New(
		WithLogger(logger),
	)
	require.NoError(t, err)
	defer c.Close()
	db, err := c.DB(context.Background(), "test")
	require.NoError(t, err)
	table, err := db.Table("test", config)
	require.NoError(t, err)

	samples := dynparquet.Samples{{
		Labels: map[string]string{
			"label1": "value1",
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 1,
		Value:     1,
	}, {
		Labels: map[string]string{
			"label2": "value2",
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     2,
	}, {
		Labels: map[string]string{
			"label2": "value2",
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

		_, err = table.InsertRecord(context.Background(), r)
		require.NoError(t, err)
	}

	engine := query.NewEngine(
		memory.NewGoAllocator(),
		db.TableProvider(),
	)

	for _, testCase := range []struct {
		fn      func(logicalplan.Expr) *logicalplan.AggregationFunction
		alias   string
		expVals []int64
	}{
		{
			fn:      logicalplan.Sum,
			alias:   "value_sum",
			expVals: []int64{5, 1},
		},
		{
			fn:      logicalplan.Min,
			alias:   "value_min",
			expVals: []int64{2, 1},
		},
		{
			fn:      logicalplan.Max,
			alias:   "value_max",
			expVals: []int64{3, 1},
		},
		{
			fn:      logicalplan.Count,
			alias:   "value_count",
			expVals: []int64{2, 1},
		},
		{
			fn:      logicalplan.Avg,
			alias:   "value_avg",
			expVals: []int64{2, 1},
		},
	} {
		t.Run(testCase.alias, func(t *testing.T) {
			var res arrow.Record
			err = engine.ScanTable("test").
				Aggregate(
					[]*logicalplan.AggregationFunction{
						testCase.fn(logicalplan.Col("value")),
					},
					[]logicalplan.Expr{logicalplan.Col("labels.label2")},
				).
				Project(testCase.fn(logicalplan.Col("value")).Alias(testCase.alias)).
				Execute(context.Background(), func(_ context.Context, r arrow.Record) error {
					r.Retain()
					res = r
					return nil
				})
			require.NoError(t, err)
			require.NotNil(t, res)
			defer res.Release()

			cols := res.Columns()
			require.Equal(t, 1, len(cols))
			for i, col := range cols {
				require.Equal(t, 2, col.Len(), "unexpected number of values in column %s", res.Schema().Field(i).Name)
			}
			actual := cols[len(cols)-1].(*array.Int64).Int64Values()
			// sort actual returned values to not have flaky tests with concurrency.
			sort.Slice(actual, func(i, j int) bool {
				return actual[i] > actual[j]
			})
			require.Equal(t, testCase.expVals, actual)
		})
	}
}

func TestAggregationProjection(t *testing.T) {
	config := NewTableConfig(
		dynparquet.SampleDefinition(),
	)

	logger := newTestLogger(t)

	c, err := New(
		WithLogger(logger),
	)
	require.NoError(t, err)
	defer c.Close()
	db, err := c.DB(context.Background(), "test")
	require.NoError(t, err)
	table, err := db.Table("test", config)
	require.NoError(t, err)

	samples := dynparquet.Samples{{
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

	for i := 0; i < len(samples); i++ {
		r, err := samples[i : i+1].ToRecord()
		require.NoError(t, err)

		_, err = table.InsertRecord(context.Background(), r)
		require.NoError(t, err)
	}

	engine := query.NewEngine(
		memory.NewGoAllocator(),
		db.TableProvider(),
	)

	records := []arrow.Record{}
	err = engine.ScanTable("test").
		Project(
			logicalplan.DynCol("labels"),
			logicalplan.Col("timestamp"),
			logicalplan.Col("value"),
		).
		Aggregate(
			[]*logicalplan.AggregationFunction{
				logicalplan.Sum(logicalplan.Col("value")),
				logicalplan.Max(logicalplan.Col("value")),
			},
			[]logicalplan.Expr{
				logicalplan.DynCol("labels"),
				logicalplan.Col("timestamp"),
			},
		).
		Project(
			logicalplan.Col(logicalplan.Sum(logicalplan.Col("value")).Name()),
			logicalplan.Col(logicalplan.Max(logicalplan.Col("value")).Name()),
			logicalplan.DynCol("labels"),
			logicalplan.Col("timestamp").Gt(logicalplan.Literal(1)).Alias("timestamp"),
		).
		Execute(context.Background(), func(_ context.Context, ar arrow.Record) error {
			records = append(records, ar)
			ar.Retain()
			return nil
		})
	require.NoError(t, err)
	require.Equal(t, 1, len(records))
	record := records[0]
	require.Equal(t, int64(7), record.NumCols())
	require.Equal(t, int64(3), record.NumRows())

	require.True(t, record.Schema().HasField("timestamp"))
	require.True(t, record.Schema().HasField("labels.label1"))
	require.True(t, record.Schema().HasField("labels.label2"))
	require.True(t, record.Schema().HasField("labels.label3"))
	require.True(t, record.Schema().HasField("labels.label4"))
	require.True(t, record.Schema().HasField("sum(value)"))
	require.True(t, record.Schema().HasField("max(value)"))
}

func TestDurationAggregation(t *testing.T) {
	c, err := New()
	require.NoError(t, err)
	defer c.Close()

	db, err := c.DB(context.Background(), "test")
	require.NoError(t, err)

	type Record struct {
		Timestamp  int64 `frostdb:",asc"`
		Stacktrace string
		Value      int64
	}

	table, err := NewGenericTable[Record](db, "test", memory.NewGoAllocator())
	require.NoError(t, err)
	defer table.Release()

	records := []Record{
		{
			Timestamp:  1 * int64(time.Second),
			Stacktrace: "stack1",
			Value:      3,
		},
		{
			Timestamp:  1 * int64(time.Second),
			Stacktrace: "stack2",
			Value:      5,
		},
		{
			Timestamp:  1 * int64(time.Second),
			Stacktrace: "stack3",
			Value:      8,
		},
		{
			Timestamp:  2 * int64(time.Second),
			Stacktrace: "stack1",
			Value:      2,
		},
		{
			Timestamp:  2 * int64(time.Second),
			Stacktrace: "stack2",
			Value:      3,
		},
	}
	_, err = table.Write(context.Background(), records...)
	require.NoError(t, err)

	engine := query.NewEngine(memory.DefaultAllocator, db.TableProvider())

	results := []arrow.Record{}
	defer func() {
		for _, r := range results {
			r.Release()
		}
	}()

	_ = engine.ScanTable("test").
		Aggregate(
			[]*logicalplan.AggregationFunction{
				logicalplan.Sum(logicalplan.Col("value")),
			},
			[]logicalplan.Expr{
				logicalplan.Duration(time.Second),
			},
		).
		Execute(context.Background(), func(_ context.Context, r arrow.Record) error {
			r.Retain()
			results = append(results, r)
			return nil
		})

	require.Equal(t, 1, len(results))
	require.Equal(t, int64(2), results[0].NumRows())

	for i := 0; int64(i) < results[0].NumRows(); i++ {
		switch results[0].Column(0).(*array.Int64).Value(i) {
		case 1 * int64(time.Second):
			require.Equal(t, int64(16), results[0].Column(1).(*array.Int64).Value(i))
		case 2 * int64(time.Second):
			require.Equal(t, int64(5), results[0].Column(1).(*array.Int64).Value(i))
		}
	}
}

// go test -bench=BenchmarkAggregation -benchmem -count=10 . | tee BenchmarkAggregation

func BenchmarkAggregation(b *testing.B) {
	ctx := context.Background()

	columnStore, err := New()
	require.NoError(b, err)
	defer columnStore.Close()

	db, err := columnStore.DB(ctx, "test")
	require.NoError(b, err)

	// Insert sample data
	{
		config := NewTableConfig(dynparquet.SampleDefinition())
		table, err := db.Table("test", config)
		require.NoError(b, err)

		samples := make(dynparquet.Samples, 0, 10_000)
		for i := 0; i < cap(samples); i++ {
			samples = append(samples, dynparquet.Sample{
				Labels: map[string]string{
					"label1": "value1",
					"label2": "value" + strconv.Itoa(i%3),
				},
				Stacktrace: []uuid.UUID{
					{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
				},
				Timestamp: int64(i),
				Value:     int64(i),
			})
		}

		r, err := samples.ToRecord()
		require.NoError(b, err)
		_, err = table.InsertRecord(ctx, r)
		require.NoError(b, err)
	}

	engine := query.NewEngine(
		memory.NewGoAllocator(),
		db.TableProvider(),
	)

	for _, bc := range []struct {
		name    string
		builder query.Builder
	}{{
		name: "sum",
		builder: engine.ScanTable("test").
			Aggregate(
				[]*logicalplan.AggregationFunction{
					logicalplan.Sum(logicalplan.Col("value")),
				},
				[]logicalplan.Expr{
					logicalplan.Col("labels.label2"),
				},
			),
	}, {
		name: "count",
		builder: engine.ScanTable("test").
			Aggregate(
				[]*logicalplan.AggregationFunction{
					logicalplan.Count(logicalplan.Col("value")),
				},
				[]logicalplan.Expr{
					logicalplan.Col("labels.label2"),
				},
			),
	}, {
		name: "max",
		builder: engine.ScanTable("test").
			Aggregate(
				[]*logicalplan.AggregationFunction{
					logicalplan.Max(logicalplan.Col("value")),
				},
				[]logicalplan.Expr{
					logicalplan.Col("labels.label2"),
				},
			),
	}} {
		b.Run(bc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = bc.builder.Execute(ctx, func(_ context.Context, _ arrow.Record) error {
					return nil
				})
			}
		})
	}
}

func Test_Aggregation_DynCol(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	config := NewTableConfig(
		&schemapb.Schema{
			Name: "test",
			Columns: []*schemapb.Column{{
				Name: "foo",
				StorageLayout: &schemapb.StorageLayout{
					Type:     schemapb.StorageLayout_TYPE_INT64,
					Nullable: true,
				},
				Dynamic: true,
			}},
			SortingColumns: []*schemapb.SortingColumn{{
				Name:      "foo",
				Direction: schemapb.SortingColumn_DIRECTION_ASCENDING,
			}},
		},
	)

	logger := newTestLogger(t)

	c, err := New(
		WithLogger(logger),
	)
	require.NoError(t, err)
	defer c.Close()
	db, err := c.DB(context.Background(), "test")
	require.NoError(t, err)
	table, err := db.Table("test", config)
	require.NoError(t, err)

	fields := []arrow.Field{
		{Name: "foo.bar", Type: arrow.PrimitiveTypes.Int64},
		{Name: "foo.baz", Type: arrow.PrimitiveTypes.Int64},
		{Name: "foo.bah", Type: arrow.PrimitiveTypes.Int64},
	}

	records := make([]arrow.Record, 0)
	// For each field, create one record with only that field.
	for i := 0; i < len(fields); i++ {
		func() {
			bldr := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema(fields[i:i+1], nil))
			defer bldr.Release()
			bldr.Field(0).(*array.Int64Builder).Append(int64(rand.Intn(100)))
			records = append(records, bldr.NewRecord())
		}()
	}
	// One more record with all concrete columns.
	bldr := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema(fields, nil))
	defer bldr.Release()
	for i := 0; i < len(fields); i++ {
		bldr.Field(i).(*array.Int64Builder).Append(int64(rand.Intn(100)))
	}

	records = append(records, bldr.NewRecord())
	defer func() {
		for _, r := range records {
			r.Release()
		}
	}()

	ctx := context.Background()
	for _, r := range records {
		_, err = table.InsertRecord(ctx, r)
		require.NoError(t, err)
	}

	engine := query.NewEngine(mem, db.TableProvider())

	err = engine.ScanTable("test").
		Aggregate(
			[]*logicalplan.AggregationFunction{logicalplan.Max(logicalplan.DynCol("foo"))},
			nil,
		).
		Execute(context.Background(), func(_ context.Context, ar arrow.Record) error {
			require.Equal(t, 3, int(ar.NumCols()))
			require.Equal(t, 1, int(ar.NumRows()))
			return nil
		})
	require.NoError(t, err)
}
