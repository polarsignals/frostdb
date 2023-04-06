package physicalplan

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	"github.com/polarsignals/frostdb/pqarrow/builder"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

// TestOrderedAggregate unit tests aggregation logic specific to
// OrderedAggregate internals using arrow records.
func TestOrderedAggregate(t *testing.T) {
	ctx := context.Background()

	type record struct {
		// NOTE: "" and 0 are considered NULL in this test to introduce a bit
		// of excitement.
		groups [][]string
		vals   []int64
	}
	testCases := []struct {
		name          string
		numGroupCols  int
		inputRecords  []record
		resultRecords []record
	}{
		{
			name:         "SingleGroupCol",
			numGroupCols: 1,
			inputRecords: []record{
				{
					groups: [][]string{
						{"a", "a", "b", "c", "c"},
					},
					vals: []int64{1, 1, 1, 1, 1},
				},
			},
			resultRecords: []record{
				{
					groups: [][]string{
						{"a", "b", "c"},
					},
					vals: []int64{2, 1, 2},
				},
			},
		},
		{
			name:         "MultipleRecords",
			numGroupCols: 1,
			inputRecords: []record{
				{
					groups: [][]string{
						{"a", "a", "a"},
					},
					vals: []int64{1, 1, 1},
				},
				{
					groups: [][]string{
						{"b", "b"},
					},
					vals: []int64{1, 1},
				},
			},
			resultRecords: []record{
				{
					groups: [][]string{
						{"a", "b"},
					},
					vals: []int64{3, 2},
				},
			},
		},
		{
			name:         "MultiGroupCol",
			numGroupCols: 2,
			inputRecords: []record{
				{
					groups: [][]string{
						{"a", "a", "a", "c", "d"},
						{"b", "b", "c", "c", "d"},
					},
					vals: []int64{1, 1, 1, 1, 1},
				},
			},
			resultRecords: []record{
				{
					groups: [][]string{
						{"a", "a", "c", "d"},
						{"b", "c", "c", "d"},
					},
					vals: []int64{2, 1, 1, 1},
				},
			},
		},
		{
			name:         "PartialOrdering",
			numGroupCols: 1,
			inputRecords: []record{
				{
					groups: [][]string{
						{"a", "a", "b", "c", "a", "b", "c"},
					},
					vals: []int64{1, 1, 2, 3, 1, 2, 3},
				},
			},
			resultRecords: []record{
				{
					groups: [][]string{
						{"a", "b", "c"},
					},
					vals: []int64{3, 4, 6},
				},
			},
		},
		{
			name:         "PartialOrderingMultiRecord",
			numGroupCols: 1,
			inputRecords: []record{
				{
					groups: [][]string{
						{"a", "a", "b", "c"},
					},
					vals: []int64{1, 1, 2, 3},
				},
				{
					groups: [][]string{
						{"a", "b", "c"},
					},
					vals: []int64{1, 2, 3},
				},
			},
			resultRecords: []record{
				{
					groups: [][]string{
						{"a", "b", "c"},
					},
					vals: []int64{3, 4, 6},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			groupColNameForIdx := func(i int) string {
				return fmt.Sprintf("group%d", i)
			}
			const valColName = "vals"
			groupCols := make([]logicalplan.Expr, 0, tc.numGroupCols)
			for i := 0; i < tc.numGroupCols; i++ {
				groupCols = append(groupCols, logicalplan.Col(groupColNameForIdx(i)))
			}
			groupBuilders := make([]*builder.OptBinaryBuilder, 0, tc.numGroupCols)
			for i := 0; i < tc.numGroupCols; i++ {
				groupBuilders = append(groupBuilders, builder.NewOptBinaryBuilder(arrow.BinaryTypes.Binary))
			}
			valBuilder := builder.NewOptInt64Builder(arrow.PrimitiveTypes.Int64)
			o := NewOrderedAggregate(
				memory.DefaultAllocator,
				trace.NewNoopTracerProvider().Tracer(""),
				Aggregation{
					expr:       logicalplan.Col(valColName),
					resultName: "result",
					function:   logicalplan.AggFuncSum,
				},
				groupCols,
				true,
			)
			resultCursor := 0
			o.SetNext(&OutputPlan{
				callback: func(ctx context.Context, r arrow.Record) error {
					if r.NumRows() == 0 {
						require.True(t, resultCursor < len(tc.resultRecords))
						return nil
					}
					expected := tc.resultRecords[resultCursor]
					resultCursor++

					for i, groupCol := range expected.groups {
						a := r.Column(i).(*array.Binary)
						require.Equal(t, len(groupCol), a.Len())
						for j, v := range groupCol {
							require.Equal(t, v, string(a.Value(j)),
								"unexpected group at row %d column %d, record: %v", j, i, r,
							)
						}
					}
					a := r.Column(len(expected.groups)).(*array.Int64)
					require.Equal(t, len(expected.vals), a.Len())
					for i, v := range expected.vals {
						require.Equal(t, v, a.Value(i))
					}
					return nil
				},
			})

			for _, record := range tc.inputRecords {
				recordFields := make([]arrow.Field, 0)
				arrays := make([]arrow.Array, 0)
				nrows := -1
				for i, groupCol := range record.groups {
					if len(groupCol) == 0 {
						// Test omitted this group column on purpose.
						continue
					}
					if nrows == -1 {
						nrows = len(groupCol)
					}
					require.Equal(t, nrows, len(groupCol), "group %d has wrong number of values", i)

					for _, v := range groupCol {
						if v == "" {
							groupBuilders[i].AppendNull()
							continue
						}
						require.NoError(t, groupBuilders[i].Append([]byte(v)))
					}
					a := groupBuilders[i].NewArray()
					recordFields = append(
						recordFields,
						arrow.Field{
							Name: groupColNameForIdx(i), Type: a.DataType(),
						},
					)
					arrays = append(arrays, a)
				}
				require.Equal(t, nrows, len(record.vals), "val col has wrong number of values")
				for _, v := range record.vals {
					if v == 0 {
						valBuilder.AppendNull()
						continue
					}
					valBuilder.Append(v)
				}
				a := valBuilder.NewArray()
				recordFields = append(
					recordFields,
					arrow.Field{
						Name: valColName, Type: a.DataType(),
					},
				)
				arrays = append(arrays, a)
				require.NoError(t, o.Callback(ctx, array.NewRecord(
					arrow.NewSchema(
						recordFields,
						nil,
					),
					arrays,
					int64(nrows),
				)))
			}
			require.NoError(t, o.Finish(ctx))
		})
	}
}

// TestOrderedAggregateDynCols verifies that the OrderedAggregate handles
// dynamic group by columns appearing/disappearing in records correctly.
func TestOrderedAggregateDynCols(t *testing.T) {
	const (
		dynColName = "labels"
		valColName = "value"
	)
	ctx := context.Background()
	o := NewOrderedAggregate(
		memory.DefaultAllocator,
		trace.NewNoopTracerProvider().Tracer(""),
		Aggregation{
			expr:     logicalplan.Col(valColName),
			function: logicalplan.AggFuncSum,
		},
		[]logicalplan.Expr{
			logicalplan.DynCol(dynColName),
		},
		true,
	)
	const (
		dynCols = 4
		numVals = 10
	)
	// The loop below will call Callback dynCols times with different dynamic
	// column names. The group by column values will not change, so we expect
	// to see four different groups.
	o.SetNext(&OutputPlan{
		callback: func(_ context.Context, r arrow.Record) error {
			require.Equal(t, int64(dynCols), r.NumRows())
			require.Equal(t, int64(dynCols+1), r.NumCols())
			arr := r.Column(dynCols).(*array.Int64)
			for i := 0; i < arr.Len(); i++ {
				require.Equal(t, int64(numVals), arr.Value(i))
			}
			return nil
		},
	})
	for i := 0; i < dynCols; i++ {
		groupBuilder := builder.NewOptBinaryBuilder(arrow.BinaryTypes.Binary)
		valBuilder := builder.NewOptInt64Builder(arrow.PrimitiveTypes.Int64)
		for j := 0; j < numVals; j++ {
			require.NoError(t, groupBuilder.Append([]byte("group")))
			valBuilder.Append(1)
		}

		// Keep the first dynamic column as a constant in the schema.
		schema := []arrow.Field{
			{Name: dynColName + ".0", Type: arrow.BinaryTypes.Binary},
		}
		groupArr := groupBuilder.NewArray()
		arrs := []arrow.Array{groupArr}
		if i != 0 {
			schema = append(schema, arrow.Field{
				Name: dynColName + fmt.Sprintf(".%d", i), Type: arrow.BinaryTypes.Binary,
			})
			arrs = append(arrs, groupArr)
		}
		schema = append(schema, arrow.Field{Name: valColName, Type: arrow.PrimitiveTypes.Int64})
		arrs = append(arrs, valBuilder.NewArray())

		require.NoError(
			t,
			o.Callback(
				ctx,
				array.NewRecord(
					arrow.NewSchema(
						schema,
						nil,
					),
					arrs,
					int64(numVals),
				),
			),
		)
	}
	require.NoError(t, o.Finish(ctx))
}
