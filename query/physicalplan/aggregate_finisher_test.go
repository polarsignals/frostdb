package physicalplan

import (
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

func TestCombineAggregateResults(t *testing.T) {
	schema := dynparquet.NewSampleSchema()
	aggLogPlan := logicalplan.Aggregation{
		AggExpr: logicalplan.Sum(logicalplan.Col("value")),
		GroupExprs: []logicalplan.Expr{
			logicalplan.Col("example_type"),
			logicalplan.Col("stacktrace"),
		},
	}

	phyPlanAgg1, err := Aggregate(memory.DefaultAllocator, schema, &aggLogPlan)
	require.Nil(t, err)
	phyPlanAgg2, err := Aggregate(memory.DefaultAllocator, schema, &aggLogPlan)
	require.Nil(t, err)

	ebuilder := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	sbuilder := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	vbuilder := array.NewInt64Builder(memory.DefaultAllocator)

	// 0
	ebuilder.AppendString("a")
	sbuilder.AppendString("1")
	vbuilder.Append(3)
	// 1
	ebuilder.AppendString("a")
	sbuilder.AppendString("2")
	vbuilder.Append(5)
	// 2
	ebuilder.AppendString("b")
	sbuilder.AppendString("1")
	vbuilder.Append(7)

	arrays := []arrow.Array{
		ebuilder.NewArray(),
		sbuilder.NewArray(),
		vbuilder.NewArray(),
	}

	arrowSchema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "example_type", Type: arrays[0].DataType()},
			{Name: "stacktrace", Type: arrays[1].DataType()},
			{Name: "value", Type: arrays[2].DataType()},
		},
		nil,
	)

	phyPlanAgg1.Callback(array.NewRecord(arrowSchema, arrays, int64(3)))
	phyPlanAgg2.Callback(array.NewRecord(arrowSchema, arrays, int64(3)))

	results := make([]arrow.Record, 0)
	callback := func(r arrow.Record) error {
		results = append(results, r)
		return nil
	}
	phyPlanAgg1.SetNextCallback(callback)
	phyPlanAgg2.SetNextCallback(callback)

	aggFinisher := HashAggregateFinisher{
		pool: memory.DefaultAllocator,
		aggregations: []*HashAggregate{
			phyPlanAgg1,
			phyPlanAgg2,
		},
	}

	aggFinisher.Finish()
	require.Len(t, results, 1)
	require.Equal(t, int64(3), results[0].NumRows())

	exampleTypes, ok := results[0].Column(0).(*array.Binary)
	require.True(t, ok)

	stacktraces, ok := results[0].Column(1).(*array.Binary)
	require.True(t, ok)

	values, ok := results[0].Column(2).(*array.Int64)
	require.True(t, ok)

	// each value will be doubled, but the order isn't guaranteed in the results
	for i := 0; i < 3; i++ {
		val := values.Value(i)
		switch val {
		case 6:
			require.Equal(t, "a", exampleTypes.ValueString(i))
			require.Equal(t, "1", stacktraces.ValueString(i))
		case 10:
			require.Equal(t, "a", exampleTypes.ValueString(i))
			require.Equal(t, "2", stacktraces.ValueString(i))
		case 14:
			require.Equal(t, "b", exampleTypes.ValueString(i))
			require.Equal(t, "1", stacktraces.ValueString(i))
		default:
			require.Fail(t, fmt.Sprintf("invalid sum value %d", val))
		}
	}
}
