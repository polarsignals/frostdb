package frostdb

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"testing"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
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
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value1"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 1,
		Value:     1,
	}, {
		Labels: []dynparquet.Label{
			{Name: "label2", Value: "value2"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     2,
	}, {
		Labels: []dynparquet.Label{
			{Name: "label2", Value: "value2"},
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

		_, err = table.InsertBuffer(context.Background(), buf)
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
					[]logicalplan.Expr{testCase.fn(logicalplan.Col("value")).Alias(testCase.alias)},
					[]logicalplan.Expr{logicalplan.Col("labels.label2")},
				).Execute(context.Background(), func(ctx context.Context, r arrow.Record) error {
				r.Retain()
				res = r
				return nil
			})
			require.NoError(t, err)
			require.NotNil(t, res)
			defer res.Release()

			cols := res.Columns()
			require.Equal(t, 2, len(cols))
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

	for i := 0; i < len(samples); i++ {
		buf, err := samples[i : i+1].ToBuffer(table.Schema())
		require.NoError(t, err)

		_, err = table.InsertBuffer(context.Background(), buf)
		require.NoError(t, err)
	}

	engine := query.NewEngine(
		memory.NewGoAllocator(),
		db.TableProvider(),
	)

	records := []arrow.Record{}
	err = engine.ScanTable("test").
		Aggregate(
			[]logicalplan.Expr{
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
		Execute(context.Background(), func(ctx context.Context, ar arrow.Record) error {
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

func TestAggregationTakeLimit(t *testing.T) {
	config := NewTableConfig(dynparquet.SampleDefinition())
	logger := newTestLogger(t)
	c, err := New(WithLogger(logger))
	require.NoError(t, err)
	defer c.Close()
	db, err := c.DB(context.Background(), "test")
	require.NoError(t, err)
	table, err := db.Table("test", config)
	require.NoError(t, err)

	samples := dynparquet.Samples{{
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value1"},
		},
		Value:     1,
		Timestamp: 1,
	}, {
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value2"},
		},
		Value:     2,
		Timestamp: 1,
	}, {
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value3"},
		},
		Value:     1,
		Timestamp: 1,
	}, {
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value1"},
		},
		Value:     1,
		Timestamp: 2,
	}, {
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value2"},
		},
		Value:     2,
		Timestamp: 2,
	}, {
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value3"},
		},
		Value:     1,
		Timestamp: 3,
	}}

	for i := 0; i < len(samples); i++ {
		buf, err := samples[i : i+1].ToBuffer(table.Schema())
		require.NoError(t, err)

		_, err = table.InsertBuffer(context.Background(), buf)
		require.NoError(t, err)
	}

	engine := query.NewEngine(
		memory.NewGoAllocator(),
		db.TableProvider(),
	)

	records := []arrow.Record{}
	err = engine.ScanTable("test").
		Aggregate(
			[]logicalplan.Expr{
				logicalplan.Take(logicalplan.Col("value"), 2),
			},
			[]logicalplan.Expr{
				logicalplan.Col("timestamp"),
			},
		).
		Execute(context.Background(), func(ctx context.Context, ar arrow.Record) error {
			records = append(records, ar)
			ar.Retain()
			fmt.Printf("%s", ar)
			return nil
		})

	require.NoError(t, err)

	require.Equal(t, 1, len(records))
	record := records[0]

	require.True(t, record.Schema().HasField("timestamp"))
	require.True(t, record.Schema().HasField("take(value, 2)"))

	require.Equal(t, int64(2), record.NumCols())
	require.Equal(t, int64(3), record.NumRows())

	// the first column should be the timestamps and there should be 3 of them
	require.Equal(t, record.Column(0).(*array.Int64).Len(), 3)

	// the second column should be the lists of values
	list, ok := record.Column(1).(*array.List)
	require.True(t, ok)
	require.Equal(t, list.Len(), 3)
	require.Equal(t, len(list.Offsets()), 4)
	require.Equal(t, list.ListValues().Len(), 5)

	for i := 0; i < list.Len(); i++ {
		start, end := list.ValueOffsets(i)
		values := array.NewSlice(list.ListValues(), start, end)
		valuesRaw := []int64{}
		for i := 0; i < values.Len(); i++ {
			valuesRaw = append(valuesRaw, values.(*array.Int64).Value(i))
		}

		defer values.Release()
		switch record.Column(0).(*array.Int64).Value(i) {
		case 1:
			require.Equal(t, values.(*array.Int64).Len(), 2)
			require.Contains(t, valuesRaw, int64(1))
			// the 2nd value in this could be 1 or 2 so we don't check it
			// to avoid the test flaking ..
		case 2:
			require.Equal(t, values.(*array.Int64).Len(), 2)
			require.Contains(t, valuesRaw, int64(1))
			require.Contains(t, valuesRaw, int64(2))
		case 3:
			require.Equal(t, values.(*array.Int64).Len(), 1)
			require.Contains(t, valuesRaw, int64(1))
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
				Labels: []dynparquet.Label{
					{Name: "label1", Value: "value1"},
					{Name: "label2", Value: "value" + strconv.Itoa(i%3)},
				},
				Stacktrace: []uuid.UUID{
					{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
				},
				Timestamp: int64(i),
				Value:     int64(i),
			})
		}

		buf, err := samples.ToBuffer(table.Schema())
		require.NoError(b, err)
		_, err = table.InsertBuffer(ctx, buf)
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
				[]logicalplan.Expr{
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
				[]logicalplan.Expr{
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
				[]logicalplan.Expr{
					logicalplan.Max(logicalplan.Col("value")),
				},
				[]logicalplan.Expr{
					logicalplan.Col("labels.label2"),
				},
			),
	}} {
		b.Run(bc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = bc.builder.Execute(ctx, func(ctx context.Context, r arrow.Record) error {
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
				Dynamic: false,
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

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "foo.bar", Type: arrow.PrimitiveTypes.Int64},
		{Name: "foo.baz", Type: arrow.PrimitiveTypes.Int64},
		{Name: "foo.bah", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer bldr.Release()

	n := 3
	for i := 0; i < schema.NumFields(); i++ {
		for j := 0; j < n; j++ {
			bldr.Field(i).(*array.Int64Builder).Append(int64(rand.Intn(100)))
		}
	}

	r := bldr.NewRecord()
	defer r.Release()

	ctx := context.Background()
	_, err = table.InsertRecord(ctx, r)
	require.NoError(t, err)

	engine := query.NewEngine(mem, db.TableProvider())

	err = engine.ScanTable("test").
		Aggregate(
			[]logicalplan.Expr{logicalplan.Max(logicalplan.DynCol("foo"))},
			nil,
		).
		Execute(context.Background(), func(ctx context.Context, ar arrow.Record) error {
			require.Equal(t, 3, int(ar.NumCols()))
			require.Equal(t, 1, int(ar.NumRows()))
			return nil
		})
	require.NoError(t, err)
}
