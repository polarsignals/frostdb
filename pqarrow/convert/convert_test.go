package convert

import (
	"testing"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/segmentio/parquet-go"
	"github.com/stretchr/testify/require"
)

func TestParquetNodeToType(t *testing.T) {
	cases := []struct {
		parquetNode parquet.Node
		arrowType   arrow.DataType
	}{
		{
			parquetNode: parquet.String(),
			arrowType:   &arrow.BinaryType{},
		},
		{
			parquetNode: parquet.Int(64),
			arrowType:   &arrow.Int64Type{},
		},
		{
			parquetNode: parquet.Uint(64),
			arrowType:   &arrow.Uint64Type{},
		},
	}

	for _, c := range cases {
		typ, err := ParquetNodeToType(c.parquetNode)
		require.NoError(t, err)
		require.Equal(t, c.arrowType, typ)
	}

	errCases := []struct {
		parquetNode parquet.Node
		msg         string
	}{
		{
			parquetNode: parquet.Leaf(parquet.DoubleType),
			msg:         "unsupported type: DOUBLE",
		},
	}
	for _, c := range errCases {
		_, err := ParquetNodeToType(c.parquetNode)
		require.EqualError(t, err, c.msg)
	}
}
