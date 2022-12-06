package pqarrow

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/memory"
	pqarrowv10 "github.com/apache/arrow/go/v10/parquet/pqarrow"
	"github.com/google/uuid"
	"github.com/segmentio/parquet-go"
	"github.com/stretchr/testify/require"

	"github.com/polarsignals/frostdb/dynparquet"
	schemapb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/schema/v1alpha1"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

func TestDifferentSchemasToArrow(t *testing.T) {
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
	}}

	buf0, err := samples.ToBuffer(dynSchema)
	require.NoError(t, err)

	samples = dynparquet.Samples{{
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
	}}

	buf1, err := samples.ToBuffer(dynSchema)
	require.NoError(t, err)

	samples = dynparquet.Samples{{
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

	buf2, err := samples.ToBuffer(dynSchema)
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

	buf3, err := samples.ToBuffer(dynSchema)
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

	buf4, err := samples.ToBuffer(dynSchema)
	require.NoError(t, err)

	ctx := context.Background()

	c := NewParquetConverter(memory.DefaultAllocator, logicalplan.IterOptions{})
	defer c.Close()

	require.NoError(t, c.Convert(ctx, buf0))
	require.NoError(t, c.Convert(ctx, buf1))
	require.NoError(t, c.Convert(ctx, buf2))
	require.NoError(t, c.Convert(ctx, buf3))
	require.NoError(t, c.Convert(ctx, buf4))

	ar := c.NewRecord()
	require.Equal(t, int64(8), ar.NumCols())
	require.Equal(t, int64(5), ar.NumRows())
	for j := 0; j < int(ar.NumCols()); j++ {
		switch j {
		case 0:
			require.Equal(t, `["" "" "" "" ""]`, fmt.Sprintf("%v", ar.Column(j)))
		case 1:
			require.Equal(t, `["value1" "value2" "value3" "value1" "value1"]`, fmt.Sprintf("%v", ar.Column(j)))
		case 2:
			require.Equal(t, `["value2" "value2" "value2" "value2" "value2"]`, fmt.Sprintf("%v", ar.Column(j)))
		case 3:
			require.Equal(t, `[(null) "value3" (null) (null) "value3"]`, fmt.Sprintf("%v", ar.Column(j)))
		case 4:
			require.Equal(t, `[(null) (null) "value4" (null) (null)]`, fmt.Sprintf("%v", ar.Column(j)))
		case 6:
			require.Equal(t, `[1 2 3 2 3]`, fmt.Sprintf("%v", ar.Column(j)))
		case 7:
			require.Equal(t, `[1 2 3 2 3]`, fmt.Sprintf("%v", ar.Column(j)))
		}
	}
}

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

	as, err := ParquetRowGroupToArrowSchema(ctx, merge, nil, nil, nil, nil)
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

	c := NewParquetConverter(memory.DefaultAllocator, logicalplan.IterOptions{})
	defer c.Close()
	require.NoError(t, c.Convert(ctx, merge))
	ar := c.NewRecord()
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

	c := NewParquetConverter(memory.DefaultAllocator, logicalplan.IterOptions{})
	defer c.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		require.NoError(b, c.Convert(ctx, buf))
		// Reset converter.
		_ = c.NewRecord()
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
		logicalplan.Col("timestamp").Gt(logicalplan.Literal(int64(0))),
	}
	as, err := ParquetRowGroupToArrowSchema(
		ctx,
		buf,
		[]logicalplan.Expr{
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

	c := NewParquetConverter(
		memory.DefaultAllocator,
		logicalplan.IterOptions{
			PhysicalProjection: []logicalplan.Expr{
				logicalplan.Col("example_type"),
				logicalplan.Col("timestamp"),
			},
			DistinctColumns: distinctColumns,
		})
	defer c.Close()
	require.NoError(t, c.Convert(ctx, buf))
	ar := c.NewRecord()
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
		logicalplan.Col("value").Gt(logicalplan.Literal(int64(0))),
	}
	as, err := ParquetRowGroupToArrowSchema(
		ctx,
		buf,
		[]logicalplan.Expr{
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

	c := NewParquetConverter(memory.DefaultAllocator, logicalplan.IterOptions{
		PhysicalProjection: []logicalplan.Expr{
			logicalplan.Col("example_type"),
			logicalplan.Col("value"),
		},
		DistinctColumns: distinctColumns,
	})
	defer c.Close()
	require.NoError(t, c.Convert(ctx, buf))
	ar := c.NewRecord()
	require.Equal(t, int64(2), ar.NumRows())
	require.Equal(t, int64(3), ar.NumCols())
	require.Len(t, ar.Schema().Fields(), 3)
}

func TestList(t *testing.T) {
	type model struct {
		Data []int
	}
	data := []int{3, 9, 2}
	buf := parquet.NewGenericBuffer[model]()
	_, err := buf.Write([]model{{Data: data}})
	require.NoError(t, err)

	ctx := context.Background()

	c := NewParquetConverter(memory.DefaultAllocator, logicalplan.IterOptions{})
	defer c.Close()
	require.NoError(t, c.Convert(ctx, buf))

	record := c.NewRecord()
	t.Log(record)
	rows := record.NumRows()
	require.Equal(t, int64(1), rows)
	require.Equal(t, int64(1), record.NumCols())

	column := record.Column(0)
	colType := column.DataType().(*arrow.ListType)
	require.True(t, colType.ElemField().Nullable)

	listArray := column.(*array.List)
	vals := listArray.ListValues().(*array.Int64).Int64Values()
	for i := range vals {
		require.Equal(
			t,
			data[i],
			int(vals[i]),
			"data mismatch at index %d, expected: %v, actual: %v",
			i,
			data,
			vals,
		)
	}
}

func Test_Arrow_ListSchema(t *testing.T) {
	def := &schemapb.Schema{
		Name: "test",
		Columns: []*schemapb.Column{{
			Name: "labels",
			StorageLayout: &schemapb.StorageLayout{
				Type:     schemapb.StorageLayout_TYPE_STRING,
				Nullable: true,
				Encoding: schemapb.StorageLayout_ENCODING_RLE_DICTIONARY,
			},
			Dynamic: true,
		}, {
			Name: "timestamp",
			StorageLayout: &schemapb.StorageLayout{
				Type:     schemapb.StorageLayout_TYPE_INT64,
				Repeated: true,
			},
			Dynamic: false,
		}, {
			Name: "value",
			StorageLayout: &schemapb.StorageLayout{
				Type:     schemapb.StorageLayout_TYPE_INT64,
				Repeated: true,
			},
			Dynamic: false,
		}},
		SortingColumns: []*schemapb.SortingColumn{{
			Name:       "labels",
			Direction:  schemapb.SortingColumn_DIRECTION_ASCENDING,
			NullsFirst: true,
		}, {
			Name:       "stacktrace",
			Direction:  schemapb.SortingColumn_DIRECTION_ASCENDING,
			NullsFirst: true,
		}, {
			Name:      "timestamp",
			Direction: schemapb.SortingColumn_DIRECTION_ASCENDING,
		}},
	}
	schema, err := dynparquet.SchemaFromDefinition(def)
	require.NoError(t, err)

	sc := schema.ParquetV10Schema()

	fmt.Println("Converted schema")
	fmt.Println(sc)

	arschema, err := pqarrowv10.FromParquet(sc, nil, nil)
	require.NoError(t, err)

	fmt.Println("Arrow schema")
	fmt.Println(arschema)

	/*
		b, err := schema.NewBuffer(map[string][]string{
			"labels": {"lables1", "labels2"},
		})
		require.NoError(t, err)

		_, err = b.WriteRows([]parquet.Row{
			{
				parquet.ValueOf("value1").Level(0, 1, 0),
				parquet.ValueOf("value2").Level(0, 1, 1),
				parquet.ValueOf(int64(1)).Level(1, 0, 2),
				parquet.ValueOf(int64(2)).Level(1, 0, 2),
				parquet.ValueOf(int64(1)).Level(1, 0, 3),
				parquet.ValueOf(int64(2)).Level(1, 0, 3),
			},
		})
		require.NoError(t, err)

		_, err = b.WriteRows([]parquet.Row{
			{
				parquet.ValueOf("value3").Level(0, 1, 0),
				parquet.ValueOf(int64(3)).Level(1, 0, 2),
				parquet.ValueOf(int64(2)).Level(1, 0, 2),
				parquet.ValueOf(int64(3)).Level(1, 0, 2),
				parquet.ValueOf(int64(3)).Level(1, 0, 3),
				parquet.ValueOf(int64(2)).Level(1, 0, 3),
				parquet.ValueOf(int64(3)).Level(1, 0, 3),
			},
		})
		require.NoError(t, err)

		ctx := context.Background()

		c := NewParquetConverter(memory.DefaultAllocator, logicalplan.IterOptions{})
		defer c.Close()

		require.NoError(t, c.Convert(ctx, b))

		ar := c.NewRecord()
		fmt.Println(ar) // TODO: REMOVE ME
		require.Equal(t, int64(4), ar.NumCols())
		require.Equal(t, int64(2), ar.NumRows())
		for j := 0; j < int(ar.NumCols()); j++ {
			switch j {
			case 0:
				require.Equal(t, `["value1" "value3"]`, fmt.Sprintf("%v", ar.Column(j)))
			case 1:
				require.Equal(t, `["value2" (null)]`, fmt.Sprintf("%v", ar.Column(j)))
			case 2:
				require.Equal(t, `[[1 2] [3 2 3]]`, fmt.Sprintf("%v", ar.Column(j)))
			case 3:
				require.Equal(t, `[[1 2] [3 2 3]]`, fmt.Sprintf("%v", ar.Column(j)))
			}
		}
	*/
}
