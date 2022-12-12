package builder_test

import (
	"testing"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/stretchr/testify/require"

	"github.com/polarsignals/frostdb/pqarrow/builder"
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
