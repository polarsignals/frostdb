package convert

import (
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/segmentio/parquet-go"
	"github.com/stretchr/testify/require"
)

func TestParquetNodeToType(t *testing.T) {
	t.Parallel()
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
		{
			parquetNode: parquet.Leaf(parquet.BooleanType),
			arrowType:   &arrow.BooleanType{},
		},
		{
			parquetNode: parquet.Group{},
			arrowType:   &arrow.StructType{},
		},
		{
			parquetNode: parquet.List(parquet.String()), // NOTE: can't determine string vs binary in conversion
			arrowType:   arrow.ListOfNonNullable(&arrow.BinaryType{}),
		},
		{
			parquetNode: parquet.Map( // NOTE: can't determine string vs binary in conversion
				parquet.String(),
				parquet.String(),
			),
			arrowType: arrow.MapOf(&arrow.BinaryType{}, &arrow.BinaryType{}),
		},
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("%v", c.arrowType.Name()), func(t *testing.T) {
			typ, err := ParquetNodeToType(c.parquetNode)
			require.NoError(t, err)
			require.Equal(t, c.arrowType, typ)
		})
	}

	errCases := []struct {
		parquetNode parquet.Node
		msg         string
	}{
		{
			parquetNode: parquet.Int(32),
			msg:         "unsupported int bit width",
		},
		{
			parquetNode: parquet.Leaf(parquet.Int96Type),
			msg:         "unsupported type: INT96",
		},
		{
			parquetNode: parquet.Leaf(parquet.FloatType),
			msg:         "unsupported type: FLOAT",
		},
		{
			parquetNode: parquet.Leaf(parquet.ByteArrayType),
			msg:         "unsupported type: BYTE_ARRAY",
		},
		{
			parquetNode: parquet.Leaf(parquet.FixedLenByteArrayType(8)),
			msg:         "unsupported type: FIXED_LEN_BYTE_ARRAY(8)",
		},
		{
			parquetNode: parquet.Decimal(0, 9, parquet.Int32Type),
			msg:         "unsupported logical type: DECIMAL(9,0)",
		},
		{
			parquetNode: parquet.UUID(),
			msg:         "unsupported logical type: UUID",
		},
		{
			parquetNode: parquet.Enum(),
			msg:         "unsupported logical type: ENUM",
		},
		// This causes stack overflow.
		// Fix PR: https://github.com/segmentio/parquet-go/pull/244
		//{
		//	parquetNode: parquet.JSON(),
		//	msg:         "unsupported type: JSON",
		//},
		{
			parquetNode: parquet.BSON(),
			msg:         "unsupported logical type: BSON",
		},
		{
			parquetNode: parquet.Date(),
			msg:         "unsupported logical type: DATE",
		},
		{
			parquetNode: parquet.Time(parquet.Millisecond),
			msg:         "unsupported logical type: TIME(isAdjustedToUTC=true,unit=MILLIS)",
		},
		{
			parquetNode: parquet.Timestamp(parquet.Millisecond),
			msg:         "unsupported logical type: TIMESTAMP(isAdjustedToUTC=true,unit=MILLIS)",
		},
		// nullType is unexported by segmentio/parquet-go.
	}
	for _, c := range errCases {
		t.Run(c.msg, func(t *testing.T) {
			typ, err := ParquetNodeToType(c.parquetNode)
			require.EqualError(t, err, c.msg, "type returned: %v", typ)
		})
	}
}
