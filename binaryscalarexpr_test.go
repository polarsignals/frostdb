package frostdb

import (
	"testing"

	"github.com/google/uuid"
	"github.com/segmentio/parquet-go"
	"github.com/stretchr/testify/require"

	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

func TestBinaryScalarOperation(t *testing.T) {
	samples := dynparquet.Samples{{
		Labels: []dynparquet.Label{
			{Name: "foo", Value: "bar"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 1,
		Value:     1,
	}, {
		Labels: []dynparquet.Label{
			{Name: "bar", Value: "baz"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 1,
		Value:     2,
	}}
	buf, err := samples.ToBuffer(dynparquet.NewSampleSchema())
	require.NoError(t, err)

	labelsFoo := buf.ColumnChunks()[1]

	compares, err := BinaryScalarOperation(labelsFoo, parquet.ValueOf("bar"), logicalplan.OpEq)
	require.NoError(t, err)
	require.True(t, compares)

	compares, err = BinaryScalarOperation(labelsFoo, parquet.ValueOf([]byte("bar")), logicalplan.OpEq)
	require.NoError(t, err)
	require.True(t, compares)

	compares, err = BinaryScalarOperation(labelsFoo, parquet.ValueOf(123), logicalplan.OpEq)
	require.NoError(t, err)
	require.True(t, compares)
}
