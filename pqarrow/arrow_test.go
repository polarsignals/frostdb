package pqarrow

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/google/uuid"
	"github.com/segmentio/parquet-go"
	"github.com/stretchr/testify/require"

	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

func TestMergeToArrow(t *testing.T) {
	dynSchema := dynparquet.NewSampleSchema()

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

	buf1, err := samples.ToBuffer(dynSchema)
	require.NoError(t, err)

	samples = dynparquet.Samples{{
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

	buf2, err := samples.ToBuffer(dynSchema)
	require.NoError(t, err)

	samples = dynparquet.Samples{{
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

	buf3, err := samples.ToBuffer(dynSchema)
	require.NoError(t, err)

	merge, err := dynSchema.MergeDynamicRowGroups([]dynparquet.DynamicRowGroup{buf1, buf2, buf3})
	require.NoError(t, err)

	ctx := context.Background()
	pool := memory.DefaultAllocator

	as, err := ParquetRowGroupToArrowSchema(ctx, dynSchema, merge, nil, nil, nil, nil)
	require.NoError(t, err)
	require.Len(t, as.Fields(), 8)
	require.Equal(t, as.Field(0), arrow.Field{Name: "example_type", Type: &arrow.BinaryType{}})
	require.Equal(t, as.Field(1), arrow.Field{Name: "labels.label1", Type: &arrow.BinaryType{}, Nullable: true})
	require.Equal(t, as.Field(2), arrow.Field{Name: "labels.label2", Type: &arrow.BinaryType{}, Nullable: true})
	require.Equal(t, as.Field(3), arrow.Field{Name: "labels.label3", Type: &arrow.BinaryType{}, Nullable: true})
	require.Equal(t, as.Field(4), arrow.Field{Name: "labels.label4", Type: &arrow.BinaryType{}, Nullable: true})
	require.Equal(t, as.Field(5), arrow.Field{Name: "stacktrace", Type: &arrow.BinaryType{}})
	require.Equal(t, as.Field(6), arrow.Field{Name: "timestamp", Type: &arrow.Int64Type{}})
	require.Equal(t, as.Field(7), arrow.Field{Name: "value", Type: &arrow.Int64Type{}})

	ar, err := ParquetRowGroupToArrowRecord(ctx, pool, merge, as, nil, nil)
	require.NoError(t, err)
	require.Equal(t, int64(5), ar.NumRows())
	require.Equal(t, int64(8), ar.NumCols())
	require.Len(t, ar.Schema().Fields(), 8)
}

func BenchmarkParquetToArrow(b *testing.B) {
	dynSchema := dynparquet.NewSampleSchema()

	samples := make(dynparquet.Samples, 0, 1000)
	for i := 0; i < 1000; i++ {
		samples = append(samples, dynparquet.Sample{
			Labels: []dynparquet.Label{
				{Name: "label1", Value: "value1"},
				{Name: "label2", Value: "value2"},
			},
			Stacktrace: []uuid.UUID{
				{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
				{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
			},
			Timestamp: int64(i + 1),
			Value:     1,
		})
	}

	buf, err := samples.ToBuffer(dynSchema)
	require.NoError(b, err)

	ctx := context.Background()
	pool := memory.NewGoAllocator()

	schema, err := ParquetRowGroupToArrowSchema(ctx, dynSchema, buf, nil, nil, nil, nil)
	require.NoError(b, err)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err = ParquetRowGroupToArrowRecord(
			ctx,
			pool,
			buf,
			schema,
			nil,
			nil,
		)
		require.NoError(b, err)
	}
}

type minMax struct {
	min parquet.Value
	max parquet.Value
}

type fakeIndex struct {
	minMax []minMax
}

func (i *fakeIndex) NumPages() int              { return len(i.minMax) }
func (i *fakeIndex) NullCount(int) int64        { return 0 }
func (i *fakeIndex) NullPage(int) bool          { return false }
func (i *fakeIndex) MinValue(int) parquet.Value { return i.minMax[0].min }
func (i *fakeIndex) MaxValue(int) parquet.Value { return i.minMax[0].max }
func (i *fakeIndex) IsAscending() bool          { return false }
func (i *fakeIndex) IsDescending() bool         { return false }

func TestAllOrNoneGreaterThan(t *testing.T) {
	typ := parquet.Int(64).Type()
	cases := []struct {
		name            string
		value           parquet.Value
		minMax          []minMax
		allGreaterThan  bool
		noneGreaterThan bool
	}{{
		name: "all_greater",
		minMax: []minMax{
			{parquet.ValueOf(int64(1)), parquet.ValueOf(int64(2))},
			{parquet.ValueOf(int64(3)), parquet.ValueOf(int64(4))},
		},
		value:           parquet.ValueOf(int64(0)),
		allGreaterThan:  true,
		noneGreaterThan: false,
	}, {
		name: "none_greater",
		minMax: []minMax{
			{parquet.ValueOf(int64(1)), parquet.ValueOf(int64(2))},
			{parquet.ValueOf(int64(3)), parquet.ValueOf(int64(4))},
		},
		value:           parquet.ValueOf(int64(5)),
		allGreaterThan:  false,
		noneGreaterThan: true,
	}, {
		name: "equal",
		minMax: []minMax{
			{parquet.ValueOf(int64(0)), parquet.ValueOf(int64(0))},
			{parquet.ValueOf(int64(0)), parquet.ValueOf(int64(0))},
		},
		value:           parquet.ValueOf(int64(0)),
		allGreaterThan:  false,
		noneGreaterThan: true,
	}, {
		name: "middle",
		minMax: []minMax{
			{parquet.ValueOf(int64(1)), parquet.ValueOf(int64(2))},
			{parquet.ValueOf(int64(3)), parquet.ValueOf(int64(4))},
		},
		value:           parquet.ValueOf(int64(3)),
		allGreaterThan:  false,
		noneGreaterThan: true,
	}}

	for _, c := range cases {
		t.Run(fmt.Sprintf("%v", c.value), func(t *testing.T) {
			index := &fakeIndex{minMax: c.minMax}
			allGreaterThan, noneGreaterThan := allOrNoneGreaterThan(typ, index, c.value)
			require.Equal(t, c.allGreaterThan, allGreaterThan)
			require.Equal(t, c.noneGreaterThan, noneGreaterThan)
		})
	}
}

func TestDistinctBinaryExprOptimization(t *testing.T) {
	dynSchema := dynparquet.NewSampleSchema()

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

	buf, err := samples.ToBuffer(dynSchema)
	require.NoError(t, err)

	ctx := context.Background()

	distinctColumns := []logicalplan.Expr{
		logicalplan.Col("example_type"),
		logicalplan.Col("timestamp").GT(logicalplan.Literal(int64(0))),
	}
	as, err := ParquetRowGroupToArrowSchema(
		ctx,
		dynSchema,
		buf,
		[]logicalplan.ColumnMatcher{
			logicalplan.Col("example_type"),
			logicalplan.Col("timestamp"),
		},
		nil,
		nil,
		distinctColumns,
	)
	require.NoError(t, err)
	require.Len(t, as.Fields(), 3)
	require.Equal(t, as.Field(0), arrow.Field{Name: "example_type", Type: &arrow.BinaryType{}})
	require.Equal(t, as.Field(1), arrow.Field{Name: "timestamp", Type: &arrow.Int64Type{}})
	require.Equal(t, as.Field(2), arrow.Field{Name: "timestamp > 0", Type: &arrow.BooleanType{}, Nullable: true})

	pool := memory.DefaultAllocator
	ar, err := ParquetRowGroupToArrowRecord(ctx, pool, buf, as, nil, distinctColumns)
	require.NoError(t, err)
	require.Equal(t, int64(1), ar.NumRows())
	require.Equal(t, int64(3), ar.NumCols())
	require.Len(t, ar.Schema().Fields(), 3)
}

func TestDistinctBinaryExprOptimizationMixed(t *testing.T) {
	dynSchema := dynparquet.NewSampleSchema()

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
			{Name: "label1", Value: "value1"},
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
			{Name: "label1", Value: "value1"},
			{Name: "label2", Value: "value2"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 3,
		Value:     0,
	}, {
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value1"},
			{Name: "label2", Value: "value2"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 4,
		Value:     0,
	}}

	buf, err := samples.ToBuffer(dynSchema)
	require.NoError(t, err)

	ctx := context.Background()

	distinctColumns := []logicalplan.Expr{
		logicalplan.Col("example_type"),
		logicalplan.Col("value").GT(logicalplan.Literal(int64(0))),
	}
	as, err := ParquetRowGroupToArrowSchema(
		ctx,
		dynSchema,
		buf,
		[]logicalplan.ColumnMatcher{
			logicalplan.Col("example_type"),
			logicalplan.Col("value"),
		},
		nil,
		nil,
		distinctColumns,
	)
	require.NoError(t, err)
	require.Len(t, as.Fields(), 3)
	require.Equal(t, as.Field(0), arrow.Field{Name: "example_type", Type: &arrow.BinaryType{}})
	require.Equal(t, as.Field(1), arrow.Field{Name: "value", Type: &arrow.Int64Type{}})
	require.Equal(t, as.Field(2), arrow.Field{Name: "value > 0", Type: &arrow.BooleanType{}, Nullable: true})

	pool := memory.DefaultAllocator
	ar, err := ParquetRowGroupToArrowRecord(ctx, pool, buf, as, nil, distinctColumns)
	require.NoError(t, err)
	t.Log(ar)
	require.Equal(t, int64(2), ar.NumRows())
	require.Equal(t, int64(3), ar.NumCols())
	require.Len(t, ar.Schema().Fields(), 3)
}
