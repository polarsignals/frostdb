package builder_test

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/polarsignals/frostdb/pqarrow/arrowutils"
	"github.com/polarsignals/frostdb/pqarrow/builder"
)

// https://github.com/polarsignals/frostdb/issues/270
func TestIssue270(t *testing.T) {
	b := builder.NewOptBinaryBuilder(arrow.BinaryTypes.Binary)
	b.AppendNull()
	const expString = "hello"
	require.NoError(t, b.Append([]byte(expString)))
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
		require.NoError(t, tc.b.RepeatLastValue(9))
		require.Equal(t, tc.b.Len(), 10)
		a := tc.b.NewArray()
		for i := 0; i < a.Len(); i++ {
			v, err := arrowutils.GetValue(a, i)
			require.NoError(t, err)
			require.Equal(t, tc.v, v)
		}
	}
}

func Test_ListBuilder(t *testing.T) {
	lb := builder.NewListBuilder(memory.NewGoAllocator(), &arrow.Int64Type{}, false)

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

// Test_BuildLargeArray is a test that build a large array ( > MaxInt32)
// The reason for this test we've hit cases where the binary array builder had so many values appended onto
// it that it caused the uint32 that was being used to track value offsets to overflow.
func Test_BuildLargeArray(t *testing.T) {
	if testing.Short() {
		t.Skip("in short mode; skipping long test")
	}
	alloc := memory.NewGoAllocator()
	bldr := builder.NewBuilder(alloc, &arrow.BinaryType{}, true)

	size := rand.Intn(1024) * 1024 // [1k,1MB) values
	buf := make([]byte, size)
	binbldr := array.NewBinaryBuilder(alloc, &arrow.BinaryType{})
	binbldr.Append(buf)
	arr := binbldr.NewArray()

	n := (math.MaxInt32 / size) + 1
	for i := 0; i < n; i++ {
		switch i {
		case n - 1:
			require.Error(t, builder.AppendValue(bldr, arr, 0))
		default:
			require.NoError(t, builder.AppendValue(bldr, arr, 0))
		}
	}

	newarr := bldr.NewArray()

	// Validate we can read all rows
	for i := 0; i < n-1; i++ {
		newarr.(*array.Binary).Value(i)
	}

	// We expect fewer rows in the array
	require.Equal(t, n-1, newarr.Data().Len())
}
