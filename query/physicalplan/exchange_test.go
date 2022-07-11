package physicalplan

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestExchangeOperator(t *testing.T) {
	makeNextPlan := func() (PhysicalPlan, *atomic.Int32, *atomic.Int32) {
		numCbCalls := atomic.NewInt32(0)
		numFinCalls := atomic.NewInt32(0)

		nextPlan := mockPhyPlan{
			callback: func(record arrow.Record) error {
				numCbCalls.Add(1)
				return nil
			},
			finish: func() error {
				numFinCalls.Add(1)
				return nil
			},
		}

		return &nextPlan, numCbCalls, numFinCalls
	}

	nextPlan0, numCbCalls0, numFinCalls0 := makeNextPlan()
	nextPlan1, numCbCalls1, numFinCalls1 := makeNextPlan()

	exchange := Exchange(context.Background())
	exchange.SetNextPlan(nextPlan0)
	exchange.SetNextPlan(nextPlan1)

	for i := 0; i < 10_000; i++ {
		recBuilder := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
		recBuilder.AppendString("a")
		arr := recBuilder.NewArray()
		schema := arrow.NewSchema([]arrow.Field{{Name: "f1", Type: arr.DataType()}}, nil)
		record := array.NewRecord(schema, []arrow.Array{arr}, int64(1))
		record.Retain()
		err := exchange.Callback(record)
		record.Release()
		require.Nil(t, err)
	}
	time.Sleep(50 * time.Millisecond)
	require.Greater(t, numCbCalls0.Load(), int32(0))
	require.Greater(t, numCbCalls1.Load(), int32(0))
	require.Equal(t, int32(10_000), numCbCalls0.Load()+numCbCalls1.Load())

	// expect it doesnt' call the finisher until it's done
	require.Equal(t, int32(0), numFinCalls0.Load())
	require.Equal(t, int32(0), numFinCalls1.Load())

	// finish and expect it calls it
	err := exchange.Finish()
	require.Nil(t, err)

	// give it some time to call and scheduler the finishers
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, int32(1), numFinCalls0.Load())
	require.Equal(t, int32(1), numFinCalls1.Load())
}
