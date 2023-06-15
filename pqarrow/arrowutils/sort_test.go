package arrowutils

import (
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestSortRecord(t *testing.T) {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "int", Type: arrow.PrimitiveTypes.Int64},
			{Name: "string", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	mem := memory.DefaultAllocator
	ib := array.NewInt64Builder(mem)
	ib.Append(0)
	ib.Append(3)
	ib.Append(5)
	ib.Append(1)

	sb := array.NewStringBuilder(mem)
	sb.Append("d")
	sb.Append("c")
	sb.Append("b")
	sb.Append("a")

	record := array.NewRecord(schema, []arrow.Array{ib.NewArray(), sb.NewArray()}, int64(4))

	// Sort the record by the first column - int64
	{
		sortedByInts, err := SortRecord(mem, record, 0)
		require.NoError(t, err)

		// check that the column got sortedByInts
		intCol := sortedByInts.Column(0).(*array.Int64)
		require.Equal(t, []int64{0, 1, 3, 5}, intCol.Int64Values())
		// make sure the other column got updated too
		strings := make([]string, 4)
		stringCol := sortedByInts.Column(1).(*array.String)
		for i := 0; i < 4; i++ {
			strings[i] = stringCol.Value(i)
		}
		require.Equal(t, []string{"d", "a", "c", "b"}, strings)
	}

	// Sort the record by the second column - string
	{
		sortedByStrings, err := SortRecord(mem, record, 1)
		require.NoError(t, err)

		// check that the column got sortedByInts
		intCol := sortedByStrings.Column(0).(*array.Int64)
		require.Equal(t, []int64{1, 5, 3, 0}, intCol.Int64Values())
		// make sure the other column got updated too
		strings := make([]string, 4)
		stringCol := sortedByStrings.Column(1).(*array.String)
		for i := 0; i < 4; i++ {
			strings[i] = stringCol.Value(i)
		}
		require.Equal(t, []string{"a", "b", "c", "d"}, strings)
	}
}
