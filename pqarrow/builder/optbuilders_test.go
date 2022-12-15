package builder_test

import (
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/polarsignals/frostdb/pqarrow/builder"
	"github.com/stretchr/testify/require"
)

// https://github.com/polarsignals/frostdb/issues/270
func TestIssue270(t *testing.T) {
	b := builder.NewOptBinaryBuilder(arrow.BinaryTypes.Binary)
	b.AppendNull()
	const expString = "hello"
	b.Append([]byte(expString))
	require.Equal(t, b.Len(), 2)

	a := b.NewArray().(*array.Binary)
	require.Equal(t, a.Len(), 2)
	require.True(t, a.IsNull(0))
	require.Equal(t, string(a.Value(1)), expString)
}

func TestRepeatLastValue(t *testing.T) {
	testCases := []struct {
		b builder.OptimizedBuilder
		v any
	}{
		{
			b: builder.NewOptBinaryBuilder(arrow.BinaryTypes.Binary),
			v: []byte("hello"),
		},
		{
			b: builder.NewOptInt64Builder(arrow.PrimitiveTypes.Int64),
			v: int64(123),
		},
		{
			b: builder.NewOptBooleanBuilder(arrow.FixedWidthTypes.Boolean),
			v: true,
		},
	}
	for _, tc := range testCases {
		require.NoError(t, builder.AppendGoValue(tc.b, tc.v))
		require.Equal(t, tc.b.Len(), 1)
		tc.b.RepeatLastValue(9)
		require.Equal(t, tc.b.Len(), 10)
		a := tc.b.NewArray()
		for i := 0; i < a.Len(); i++ {
			v, err := builder.GetValue(a, i)
			require.NoError(t, err)
			require.Equal(t, tc.v, v)
		}
	}
}

func Test_ListBuilder(t *testing.T) {
	lb := builder.NewListBuilder(memory.NewGoAllocator(), &arrow.Int64Type{})

	lb.Append(true)
	lb.ValueBuilder().(*builder.OptInt64Builder).Append(1)
	lb.ValueBuilder().(*builder.OptInt64Builder).Append(2)
	lb.ValueBuilder().(*builder.OptInt64Builder).Append(3)
	lb.Append(true)
	lb.ValueBuilder().(*builder.OptInt64Builder).Append(4)
	lb.ValueBuilder().(*builder.OptInt64Builder).Append(5)
	lb.ValueBuilder().(*builder.OptInt64Builder).Append(6)

	ar := lb.NewArray()
	require.Equal(t, "[[1 2 3] [4 5 6]]", fmt.Sprintf("%v", ar))
}
