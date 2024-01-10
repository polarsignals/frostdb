package physicalplan

import (
	"testing"

	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/arrow/scalar"

	"github.com/polarsignals/frostdb/query/logicalplan"
)

func BenchmarkBinaryScalarOperation(b *testing.B) {
	ab := array.NewInt64Builder(memory.DefaultAllocator)
	for i := int64(0); i < 10_000_000; i++ {
		ab.Append(i % 10)
	}

	arr := ab.NewInt64Array()
	ab.Release()

	s := scalar.NewInt64Scalar(4) // chosen by fair dice roll. guaranteed to be random.

	for i := 0; i < b.N; i++ {
		_, _ = BinaryScalarOperation(arr, s, logicalplan.OpEq)
	}
}
