package query

import (
	"context"
	"testing"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/polarsignals/frostdb/dynparquet"
	schemapb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/schema/v1alpha1"
	"github.com/polarsignals/frostdb/query/logicalplan"
	"github.com/polarsignals/frostdb/query/physicalplan"
)

func TestUniqueAggregation(t *testing.T) {
	tests := map[string]struct {
		execOptions []physicalplan.Option
	}{
		"no concurrency": {
			execOptions: []physicalplan.Option{
				physicalplan.WithConcurrency(1),
			},
		},
		"default": {
			execOptions: []physicalplan.Option{},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)

			schema, err := dynparquet.SchemaFromDefinition(&schemapb.Schema{
				Name: "test",
				Columns: []*schemapb.Column{{
					Name: "example",
					StorageLayout: &schemapb.StorageLayout{
						Type: schemapb.StorageLayout_TYPE_INT64,
					},
				}, {
					Name: "timestamp",
					StorageLayout: &schemapb.StorageLayout{
						Type: schemapb.StorageLayout_TYPE_INT64,
					},
				}},
			})
			require.NoError(t, err)

			rb := array.NewRecordBuilder(mem, arrow.NewSchema([]arrow.Field{{
				Name: "example",
				Type: arrow.PrimitiveTypes.Int64,
			}, {
				Name: "timestamp",
				Type: arrow.PrimitiveTypes.Int64,
			}}, nil))
			defer rb.Release()

			rb.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
			rb.Field(1).(*array.Int64Builder).AppendValues([]int64{1, 1, 3}, nil)

			r := rb.NewRecord()
			defer r.Release()

			ran := false
			err = NewEngine(mem, &FakeTableProvider{
				Tables: map[string]logicalplan.TableReader{
					"test": &FakeTableReader{
						FrostdbSchema: schema,
						Records:       []arrow.Record{r},
					},
				},
			}, WithPhysicalplanOptions(test.execOptions...)).ScanTable("test").
				Aggregate(
					[]*logicalplan.AggregationFunction{logicalplan.Unique(logicalplan.Col("example"))},
					[]logicalplan.Expr{logicalplan.Col("timestamp")},
				).
				Execute(context.Background(), func(ctx context.Context, r arrow.Record) error {
					require.Equal(t, []int64{1, 3}, r.Column(0).(*array.Int64).Int64Values())
					require.True(t, r.Column(1).(*array.Int64).IsNull(0))
					require.True(t, r.Column(1).(*array.Int64).IsValid(1))
					require.Equal(t, int64(3), r.Column(1).(*array.Int64).Value(1))
					ran = true
					return nil
				})
			require.NoError(t, err)
			require.True(t, ran)
		})
	}
}

func TestAndAggregation(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	schema, err := dynparquet.SchemaFromDefinition(&schemapb.Schema{
		Name: "test",
		Columns: []*schemapb.Column{{
			Name: "example",
			StorageLayout: &schemapb.StorageLayout{
				Type: schemapb.StorageLayout_TYPE_BOOL,
			},
		}, {
			Name: "timestamp",
			StorageLayout: &schemapb.StorageLayout{
				Type: schemapb.StorageLayout_TYPE_INT64,
			},
		}},
	})
	require.NoError(t, err)

	rb := array.NewRecordBuilder(mem, arrow.NewSchema([]arrow.Field{{
		Name: "example",
		Type: arrow.FixedWidthTypes.Boolean,
	}, {
		Name: "timestamp",
		Type: arrow.PrimitiveTypes.Int64,
	}}, nil))
	defer rb.Release()

	rb.Field(0).(*array.BooleanBuilder).AppendValues([]bool{true, false, true, true}, nil)
	rb.Field(1).(*array.Int64Builder).AppendValues([]int64{1, 1, 3, 3}, nil)

	r := rb.NewRecord()
	defer r.Release()

	ran := false
	err = NewEngine(mem, &FakeTableProvider{
		Tables: map[string]logicalplan.TableReader{
			"test": &FakeTableReader{
				FrostdbSchema: schema,
				Records:       []arrow.Record{r},
			},
		},
	}).ScanTable("test").
		Aggregate(
			[]*logicalplan.AggregationFunction{logicalplan.AndAgg(logicalplan.Col("example"))},
			[]logicalplan.Expr{logicalplan.Col("timestamp")},
		).
		Execute(context.Background(), func(ctx context.Context, r arrow.Record) error {
			require.Equal(t, []int64{1, 3}, r.Column(0).(*array.Int64).Int64Values())
			require.False(t, r.Column(1).(*array.Boolean).Value(0))
			require.True(t, r.Column(1).(*array.Boolean).Value(1))
			ran = true
			return nil
		})
	require.NoError(t, err)
	require.True(t, ran)
}

func TestIfProjection(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	schema, err := dynparquet.SchemaFromDefinition(&schemapb.Schema{
		Name: "test",
		Columns: []*schemapb.Column{{
			Name: "example",
			StorageLayout: &schemapb.StorageLayout{
				Type: schemapb.StorageLayout_TYPE_INT64,
			},
		}, {
			Name: "timestamp",
			StorageLayout: &schemapb.StorageLayout{
				Type: schemapb.StorageLayout_TYPE_INT64,
			},
		}},
	})
	require.NoError(t, err)

	rb := array.NewRecordBuilder(mem, arrow.NewSchema([]arrow.Field{{
		Name: "example",
		Type: arrow.PrimitiveTypes.Int64,
	}, {
		Name: "timestamp",
		Type: arrow.PrimitiveTypes.Int64,
	}}, nil))
	defer rb.Release()

	rb.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	rb.Field(1).(*array.Int64Builder).AppendValues([]int64{1, 1, 3}, nil)

	r := rb.NewRecord()
	defer r.Release()

	ran := false
	err = NewEngine(mem, &FakeTableProvider{
		Tables: map[string]logicalplan.TableReader{
			"test": &FakeTableReader{
				FrostdbSchema: schema,
				Records:       []arrow.Record{r},
			},
		},
	}).ScanTable("test").
		Project(
			logicalplan.If(logicalplan.Col("example").Eq(logicalplan.Literal(int64(1))), logicalplan.Literal(int64(1)), logicalplan.Literal(int64(0))),
		).
		Execute(context.Background(), func(ctx context.Context, r arrow.Record) error {
			require.Equal(t, []int64{1, 0, 0}, r.Column(0).(*array.Int64).Int64Values())
			ran = true
			return nil
		})
	require.NoError(t, err)
	require.True(t, ran)
}

func TestIsNullProjection(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	schema, err := dynparquet.SchemaFromDefinition(&schemapb.Schema{
		Name: "test",
		Columns: []*schemapb.Column{{
			Name: "example",
			StorageLayout: &schemapb.StorageLayout{
				Type: schemapb.StorageLayout_TYPE_INT64,
			},
		}},
	})
	require.NoError(t, err)

	rb := array.NewRecordBuilder(mem, arrow.NewSchema([]arrow.Field{{
		Name: "example",
		Type: arrow.PrimitiveTypes.Int64,
	}}, nil))
	defer rb.Release()

	rb.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, []bool{true, true, false})

	r := rb.NewRecord()
	defer r.Release()

	ran := false
	err = NewEngine(mem, &FakeTableProvider{
		Tables: map[string]logicalplan.TableReader{
			"test": &FakeTableReader{
				FrostdbSchema: schema,
				Records:       []arrow.Record{r},
			},
		},
	}).ScanTable("test").
		Project(
			logicalplan.IsNull(logicalplan.Col("example")),
		).
		Execute(context.Background(), func(ctx context.Context, r arrow.Record) error {
			require.False(t, r.Column(0).(*array.Boolean).Value(0))
			require.False(t, r.Column(0).(*array.Boolean).Value(1))
			require.True(t, r.Column(0).(*array.Boolean).Value(2))
			ran = true
			return nil
		})
	require.NoError(t, err)
	require.True(t, ran)
}
