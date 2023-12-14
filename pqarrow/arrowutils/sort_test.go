package arrowutils

import (
	"testing"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
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
	ib.AppendNull()
	ib.Append(3)
	ib.Append(5)
	ib.Append(1)

	sb := array.NewStringBuilder(mem)
	sb.Append("d")
	sb.Append("c")
	sb.Append("b")
	sb.AppendNull()
	sb.Append("a")

	record := array.NewRecord(schema, []arrow.Array{ib.NewArray(), sb.NewArray()}, int64(5))

	// Sort the record by the first column - int64
	{
		sortedIndices, err := SortRecord(mem, record, []int{record.Schema().FieldIndices("int")[0]})
		require.NoError(t, err)
		require.Equal(t, []int64{0, 4, 2, 3, 1}, sortedIndices.Int64Values())

		sortedByInts, err := ReorderRecord(record, sortedIndices)
		require.NoError(t, err)

		// check that the column got sortedIndices
		intCol := sortedByInts.Column(0).(*array.Int64)
		require.Equal(t, []int64{0, 1, 3, 5, 0}, intCol.Int64Values())
		require.True(t, intCol.IsNull(intCol.Len()-1)) // last is NULL
		// make sure the other column got updated too
		strings := make([]string, sortedByInts.NumRows())
		stringCol := sortedByInts.Column(1).(*array.String)
		for i := 0; i < int(sortedByInts.NumRows()); i++ {
			strings[i] = stringCol.Value(i)
		}
		require.Equal(t, []string{"d", "a", "b", "", "c"}, strings)
	}

	// Sort the record by the second column - string
	{
		sortedIndices, err := SortRecord(mem, record, []int{record.Schema().FieldIndices("string")[0]})
		require.NoError(t, err)
		require.Equal(t, []int64{4, 2, 1, 0, 3}, sortedIndices.Int64Values())

		sortedByStrings, err := ReorderRecord(record, sortedIndices)
		require.NoError(t, err)

		// check that the column got sortedByInts
		intCol := sortedByStrings.Column(0).(*array.Int64)
		require.Equal(t, []int64{1, 3, 0, 0, 5}, intCol.Int64Values())
		// make sure the other column got updated too
		strings := make([]string, sortedByStrings.NumRows())
		stringCol := sortedByStrings.Column(1).(*array.String)
		for i := 0; i < int(sortedByStrings.NumRows()); i++ {
			strings[i] = stringCol.Value(i)
		}
		require.Equal(t, []string{"a", "b", "c", "d", ""}, strings)
		require.True(t, stringCol.IsNull(stringCol.Len()-1)) // last is NULL
	}
}
