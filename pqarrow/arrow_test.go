package pqarrow

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/google/uuid"
	"github.com/segmentio/parquet-go"
	"github.com/stretchr/testify/require"

	"github.com/polarsignals/frostdb/dynparquet"
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
			require.Equal(t, `{ dictionary: [""]
  indices: [0 0 0 0 0] }`, fmt.Sprintf("%v", ar.Column(j)))
		case 1:
			require.Equal(t, `{ dictionary: ["value1" "value2" "value3"]
  indices: [0 1 2 0 0] }`, fmt.Sprintf("%v", ar.Column(j)))
		case 2:
			require.Equal(t, `{ dictionary: ["value2"]
  indices: [0 0 0 0 0] }`, fmt.Sprintf("%v", ar.Column(j)))
		case 3:
			require.Equal(t, `{ dictionary: ["value3"]
  indices: [(null) 0 (null) (null) 0] }`, fmt.Sprintf("%v", ar.Column(j)))
		case 4:
			require.Equal(t, `{ dictionary: ["value4"]
  indices: [(null) (null) 0 (null) (null)] }`, fmt.Sprintf("%v", ar.Column(j)))
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

	as, err := ParquetRowGroupToArrowSchema(ctx, merge, logicalplan.IterOptions{})
	require.NoError(t, err)
	require.Len(t, as.Fields(), 8)
	require.Equal(t, as.Field(0), arrow.Field{Name: "example_type", Type: &arrow.DictionaryType{
		IndexType: &arrow.Uint32Type{},
		ValueType: &arrow.BinaryType{},
	}})
	require.Equal(t, as.Field(1), arrow.Field{Name: "labels.label1", Type: &arrow.DictionaryType{
		IndexType: &arrow.Uint32Type{},
		ValueType: &arrow.BinaryType{},
	}, Nullable: true})
	require.Equal(t, as.Field(2), arrow.Field{Name: "labels.label2", Type: &arrow.DictionaryType{
		IndexType: &arrow.Uint32Type{},
		ValueType: &arrow.BinaryType{},
	}, Nullable: true})
	require.Equal(t, as.Field(3), arrow.Field{Name: "labels.label3", Type: &arrow.DictionaryType{
		IndexType: &arrow.Uint32Type{},
		ValueType: &arrow.BinaryType{},
	}, Nullable: true})
	require.Equal(t, as.Field(4), arrow.Field{Name: "labels.label4", Type: &arrow.DictionaryType{
		IndexType: &arrow.Uint32Type{},
		ValueType: &arrow.BinaryType{},
	}, Nullable: true})
	require.Equal(t, as.Field(5), arrow.Field{Name: "stacktrace", Type: &arrow.DictionaryType{
		IndexType: &arrow.Uint32Type{},
		ValueType: &arrow.BinaryType{},
	}})
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

func BenchmarkNestedParquetToArrow(b *testing.B) {
	dynSchema := dynparquet.NewNestedSampleSchema(b)
	schema, err := dynparquet.SchemaFromDefinition(dynSchema)
	require.NoError(b, err)

	pb, err := schema.NewBuffer(map[string][]string{})
	require.NoError(b, err)

	for i := 0; i < 1000; i++ {
		_, err = pb.WriteRows([]parquet.Row{
			{
				parquet.ValueOf("value1").Level(0, 1, 0), // labels.label1
				parquet.ValueOf("value1").Level(0, 1, 1), // labels.label2
				parquet.ValueOf(i+1).Level(0, 2, 2),      // timestamps: [1]
				parquet.ValueOf(2).Level(0, 2, 3),        // values: [2]
			},
		})
		require.NoError(b, err)
	}

	ctx := context.Background()

	c := NewParquetConverter(memory.DefaultAllocator, logicalplan.IterOptions{})
	defer c.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		require.NoError(b, c.Convert(ctx, pb))
		// Reset converter.
		_ = c.NewRecord()
	}
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
		logicalplan.IterOptions{
			PhysicalProjection: []logicalplan.Expr{
				logicalplan.Col("example_type"),
				logicalplan.Col("timestamp"),
			},
			DistinctColumns: distinctColumns,
		},
	)

	require.NoError(t, err)
	require.Len(t, as.Fields(), 3)
	require.Equal(t, as.Field(0), arrow.Field{Name: "example_type", Type: &arrow.DictionaryType{
		IndexType: &arrow.Uint32Type{},
		ValueType: &arrow.BinaryType{},
	}})
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
		logicalplan.IterOptions{
			PhysicalProjection: []logicalplan.Expr{
				logicalplan.Col("example_type"),
				logicalplan.Col("value"),
			},
			DistinctColumns: distinctColumns,
		},
	)
	require.NoError(t, err)
	require.Len(t, as.Fields(), 3)
	require.Equal(t, as.Field(0), arrow.Field{Name: "example_type", Type: &arrow.DictionaryType{
		IndexType: &arrow.Uint32Type{},
		ValueType: &arrow.BinaryType{},
	}})
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
	for i := range data {
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

func Test_ParquetRowGroupToArrowSchema_Groups(t *testing.T) {
	dynSchema := dynparquet.NewNestedSampleSchema(t)
	schema, err := dynparquet.SchemaFromDefinition(dynSchema)
	require.NoError(t, err)
	buf, err := schema.NewBufferV2(
		dynparquet.LabelColumn("label1"),
		dynparquet.LabelColumn("label2"),
	)
	require.NoError(t, err)

	_, err = buf.WriteRows([]parquet.Row{
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

	ctx := context.Background()

	tests := map[string]struct {
		physicalProjections []logicalplan.Expr
		expectedSchema      *arrow.Schema
	}{
		"none": {
			expectedSchema: arrow.NewSchema([]arrow.Field{
				{
					Name: "labels",
					Type: arrow.StructOf([]arrow.Field{
						{
							Name: "label1",
							Type: &arrow.DictionaryType{
								IndexType: &arrow.Uint32Type{},
								ValueType: &arrow.BinaryType{},
							},
							Nullable: true,
						},
						{
							Name: "label2",
							Type: &arrow.DictionaryType{
								IndexType: &arrow.Uint32Type{},
								ValueType: &arrow.BinaryType{},
							},
							Nullable: true,
						},
					}...),
				},
				{
					Name: "timestamps",
					Type: arrow.ListOf(&arrow.Int64Type{}),
				},
				{
					Name: "values",
					Type: arrow.ListOf(&arrow.Int64Type{}),
				},
			}, nil),
		},
		"label 1 select": {
			physicalProjections: []logicalplan.Expr{
				logicalplan.Col("labels.label1"),
				logicalplan.Col("timestamps"),
			},
			expectedSchema: arrow.NewSchema([]arrow.Field{
				{
					Name: "labels",
					Type: arrow.StructOf([]arrow.Field{
						{
							Name: "label1",
							Type: &arrow.DictionaryType{
								IndexType: &arrow.Uint32Type{},
								ValueType: &arrow.BinaryType{},
							},
							Nullable: true,
						},
					}...),
				},
				{
					Name: "timestamps",
					Type: arrow.ListOf(&arrow.Int64Type{}),
				},
			}, nil),
		},
		"label 2 select": {
			physicalProjections: []logicalplan.Expr{
				logicalplan.Col("labels.label2"),
				logicalplan.Col("timestamps"),
			},
			expectedSchema: arrow.NewSchema([]arrow.Field{
				{
					Name: "labels",
					Type: arrow.StructOf([]arrow.Field{
						{
							Name: "label2",
							Type: &arrow.DictionaryType{
								IndexType: &arrow.Uint32Type{},
								ValueType: &arrow.BinaryType{},
							},
							Nullable: true,
						},
					}...),
				},
				{
					Name: "timestamps",
					Type: arrow.ListOf(&arrow.Int64Type{}),
				},
			}, nil),
		},
		"select all labels": {
			physicalProjections: []logicalplan.Expr{
				logicalplan.Col("labels"),
				logicalplan.Col("timestamps"),
			},
			expectedSchema: arrow.NewSchema([]arrow.Field{
				{
					Name: "labels",
					Type: arrow.StructOf([]arrow.Field{
						{
							Name: "label1",
							Type: &arrow.DictionaryType{
								IndexType: &arrow.Uint32Type{},
								ValueType: &arrow.BinaryType{},
							},
							Nullable: true,
						},
						{
							Name: "label2",
							Type: &arrow.DictionaryType{
								IndexType: &arrow.Uint32Type{},
								ValueType: &arrow.BinaryType{},
							},
							Nullable: true,
						},
					}...),
				},
				{
					Name: "timestamps",
					Type: arrow.ListOf(&arrow.Int64Type{}),
				},
			}, nil),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			as, err := ParquetRowGroupToArrowSchema(
				ctx,
				buf,
				logicalplan.IterOptions{
					PhysicalProjection: test.physicalProjections,
				},
			)
			require.NoError(t, err)
			require.True(t, as.Equal(test.expectedSchema))
		})
	}
}

func Test_ParquetToArrowV2(t *testing.T) {
	dynSchema := dynparquet.NewNestedSampleSchema(t)
	schema, err := dynparquet.SchemaFromDefinition(dynSchema)
	require.NoError(t, err)

	ctx := context.Background()
	c := NewParquetConverter(memory.DefaultAllocator, logicalplan.IterOptions{})
	defer c.Close()

	n := 10
	for i := 0; i < n; i++ {
		pb, err := schema.NewBufferV2(
			dynparquet.LabelColumn("label1"),
			dynparquet.LabelColumn("label2"),
		)
		require.NoError(t, err)
		_, err = pb.WriteRows([]parquet.Row{
			{
				parquet.ValueOf("value1").Level(0, 1, 0), // labels.label1
				parquet.ValueOf("value1").Level(0, 1, 1), // labels.label2
				parquet.ValueOf(i).Level(0, 2, 2),        // timestamps: [i]
				parquet.ValueOf(i+1).Level(1, 2, 2),      // timestamps: [i,i+1]
				parquet.ValueOf(2).Level(0, 2, 3),        // values: [2]
				parquet.ValueOf(3).Level(1, 2, 3),        // values: [2,3]
			},
		})
		require.NoError(t, err)
		require.NoError(t, c.Convert(ctx, pb))
	}
	r := c.NewRecord()
	fmt.Println(r)
	require.Equal(t, int64(n), r.NumRows())
}

func Test_ParquetToArrow(t *testing.T) {
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
	require.NoError(t, err)

	ctx := context.Background()

	c := NewParquetConverter(memory.DefaultAllocator, logicalplan.IterOptions{})
	defer c.Close()

	require.NoError(t, c.Convert(ctx, buf))
	r := c.NewRecord()
	require.Equal(t, int64(1000), r.NumRows())
}
