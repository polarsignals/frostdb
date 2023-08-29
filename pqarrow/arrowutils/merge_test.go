package arrowutils_test

import (
	"testing"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/polarsignals/frostdb/pqarrow/arrowutils"
)

func TestMerge(t *testing.T) {
	schema := arrow.NewSchema(
		[]arrow.Field{{Name: "test", Type: arrow.PrimitiveTypes.Int64}},
		nil,
	)

	b := array.NewInt64Builder(memory.DefaultAllocator)
	b.Append(0)
	b.Append(1)
	b.Append(3)
	b.Append(5)
	a := b.NewArray()
	record1 := array.NewRecord(schema, []arrow.Array{a}, int64(a.Len()))
	b.Append(2)
	b.Append(4)
	b.Append(5)
	b.Append(6)
	a = b.NewArray()
	record2 := array.NewRecord(schema, []arrow.Array{a}, int64(a.Len()))
	b.AppendNull()
	a = b.NewArray()
	record3 := array.NewRecord(schema, []arrow.Array{a}, int64(a.Len()))

	res, err := arrowutils.MergeRecords(
		memory.DefaultAllocator, []arrow.Record{record1, record2, record3}, []int{0},
	)
	require.NoError(t, err)
	require.Equal(t, int64(1), res.NumCols())
	col := res.Column(0).(*array.Int64)
	require.Equal(t, 9, col.Len())
	// Nulls sort first.
	require.True(t, col.IsNull(0))
	expected := []int64{0, 1, 2, 3, 4, 5, 5, 6}
	for i := 1; i < col.Len(); i++ {
		require.Equal(t, expected[i-1], col.Value(i))
	}
}
