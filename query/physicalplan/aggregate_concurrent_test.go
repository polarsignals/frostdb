package physicalplan

import (
	"fmt"
	"sync"
	"testing"
	"time"

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

	merge := Merge()
	merge.wg.Add(1)
	merge.wg.Add(1)

	phyPlanAgg1.SetNextPlan(merge)
	phyPlanAgg2.SetNextPlan(merge)

	concurrentAgg := ConcurrentAggregate(
		memory.DefaultAllocator,
		phyPlanAgg1.aggregationFunction,
	)
	merge.SetNextPlan(concurrentAgg)

	results := make([]arrow.Record, 0)
	resultMtx := sync.Mutex{}
	finishCalled := false
	concurrentAgg.SetNextPlan(&mockPhyPlan{
		callback: func(r arrow.Record) error {
			resultMtx.Lock()
			defer resultMtx.Unlock()
			results = append(results, r)
			return nil
		},
		finish: func() error {
			require.False(t, finishCalled) // should only be called once
			finishCalled = true
			return nil
		},
	})

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

	record1 := array.NewRecord(arrowSchema, arrays, int64(3))
	record1.Retain()
	defer record1.Release()
	phyPlanAgg1.Callback(record1)

	record2 := array.NewRecord(arrowSchema, arrays, int64(3))
	record2.Retain()
	defer record2.Release()
	phyPlanAgg2.Callback(record2)
	go func() {
		err = phyPlanAgg1.Finish()
		require.Nil(t, err)
	}()
	go func() {
		err = phyPlanAgg2.Finish()
		require.Nil(t, err)
	}()
	merge.wg.Wait()
	// give it time to call the finisher
	time.Sleep(100 * time.Millisecond)
	resultMtx.Lock()
	defer resultMtx.Unlock()

	require.Len(t, results, 1)
	require.Equal(t, int64(3), results[0].NumRows())
	require.True(t, finishCalled)

	resultSchema := results[0].Schema()

	eIdx := resultSchema.FieldIndices("example_type")
	require.Len(t, eIdx, 1)
	exampleTypes, ok := results[0].Column(eIdx[0]).(*array.Binary)
	require.True(t, ok)

	sIdx := resultSchema.FieldIndices("stacktrace")
	require.Len(t, sIdx, 1)
	stacktraces, ok := results[0].Column(sIdx[0]).(*array.Binary)
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
