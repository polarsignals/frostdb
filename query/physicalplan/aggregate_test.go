package physicalplan

import (
	"context"
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	"github.com/polarsignals/frostdb/query/logicalplan"
)

func randByteSlice(n int) []byte {
	b := make([]byte, n)
	binary.LittleEndian.PutUint64(b, rand.Uint64())
	return b
}

// Test_Aggregate_ArrayOverflow ensures that if more data is sent to the aggreagte function than can fit in a single array
// that the Aggregate function correctly splits the data into multiple records.
func Test_Aggregate_ArrayOverflow(t *testing.T) {
	if testing.Short() {
		t.Skip("short test; skipping")
	}
	ctx := context.Background()
	allocator := memory.NewGoAllocator()

	agg := NewHashAggregate(
		allocator,
		trace.NewNoopTracerProvider().Tracer(""),
		[]Aggregation{
			{
				expr:       logicalplan.Col("value"),
				resultName: "result",
				function:   logicalplan.AggFuncSum,
			},
		},
		[]logicalplan.Expr{
			logicalplan.Col("stacktrace"),
			logicalplan.Col("id"),
		},
		false,
	)

	totalRows := int64(0)
	agg.SetNext(&OutputPlan{
		callback: func(ctx context.Context, r arrow.Record) error {
			require.Equal(t, 3, len(r.Schema().Fields()))
			for i := 0; i < int(r.NumCols()); i++ {
				require.Equal(t, r.NumRows(), int64(r.Column(i).Len()))
			}
			totalRows += r.NumRows()
			return nil
		},
	})

	fields := []arrow.Field{
		{
			Name: "value",
			Type: &arrow.Int64Type{},
		},
		{
			Name: "id",
			Type: &arrow.Int64Type{},
		},
		{
			Name: "stacktrace",
			Type: &arrow.BinaryType{},
		},
	}

	n := 3
	rows := 1000
	for i := 0; i < n; i++ {
		arrays := make([]arrow.Array, len(fields))

		// Build the value array
		valBuilder := array.NewInt64Builder(allocator)
		for j := 0; j < rows; j++ {
			valBuilder.Append(rand.Int63())
		}
		arrays[0] = valBuilder.NewArray()

		// Build the id array
		idBuilder := array.NewInt64Builder(allocator)
		for j := 0; j < rows; j++ {
			idBuilder.Append(int64(j + (i * rows)))
		}
		arrays[1] = idBuilder.NewArray()

		// Build the stacktrace array
		stacktraceBuilder := array.NewBinaryBuilder(allocator, &arrow.BinaryType{})
		for j := 0; j < rows; j++ {
			// Generate a new stack trace each time, this will cause the group by array to grow quite large
			stacktraceBuilder.Append(randByteSlice(1024 * 1024)) // Use 1MB stack traces to quickly exceed array size
		}
		arrays[2] = stacktraceBuilder.NewArray()

		require.NoError(t, agg.Callback(ctx,
			array.NewRecord(
				arrow.NewSchema(fields, nil),
				arrays,
				int64(rows),
			),
		))
	}

	require.NoError(t, agg.Finish(ctx))
	require.Equal(t, int64(n*rows), totalRows)
}
