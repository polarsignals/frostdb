package physicalplan

import (
	"testing"

	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/arrow/scalar"
	"github.com/stretchr/testify/require"

	"github.com/polarsignals/frostdb/query/logicalplan"
)

func BenchmarkBinaryScalarOperation(b *testing.B) {
	ab := array.NewInt64Builder(memory.DefaultAllocator)
	for i := int64(0); i < 1_000_000; i++ {
		ab.Append(i % 10)
	}

	arr := ab.NewInt64Array()
	ab.Release()

	s := scalar.NewInt64Scalar(4) // chosen by fair dice roll. guaranteed to be random.

	operators := []logicalplan.Op{
		logicalplan.OpAnd,
		logicalplan.OpEq,
		logicalplan.OpNotEq,
		logicalplan.OpLt,
		logicalplan.OpLtEq,
		logicalplan.OpGt,
		logicalplan.OpGtEq,
	}

	for _, op := range operators {
		b.Run(op.String(), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = BinaryScalarOperation(arr, s, op)
			}
		})
	}
}

func TestBinaryScalarOperationNotImplemented(t *testing.T) {
	ab := array.NewInt64Builder(memory.DefaultAllocator)
	arr := ab.NewInt64Array()
	ab.Release()

	s := scalar.NewInt64Scalar(4) // chosen by fair dice roll. guaranteed to be random.

	_, err := BinaryScalarOperation(arr, s, logicalplan.OpAnd)
	require.Equal(t, err, ErrUnsupportedBinaryOperation)
}

func Test_ArrayScalarCompute_Leak(t *testing.T) {
	allocator := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer allocator.AssertSize(t, 0)

	ab := array.NewInt64Builder(allocator)
	defer ab.Release()

	ab.AppendValues([]int64{1, 2, 3}, nil)
	arr := ab.NewInt64Array()
	defer arr.Release()

	s := scalar.NewInt64Scalar(4)
	_, err := ArrayScalarCompute("equal", arr, s)
	require.NoError(t, err)
}
