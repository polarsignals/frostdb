package frostdb

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/google/uuid"
	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/pqarrow"
	"github.com/polarsignals/frostdb/query/logicalplan"
	"github.com/stretchr/testify/require"
)

func Test_LSM(t *testing.T) {
	c, table := basicTable(t)
	defer c.Close()

	lsm := NewLSM("test", 2)

	samples := dynparquet.Samples{{
		ExampleType: "test",
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value1"},
			{Name: "label2", Value: "value2"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 1,
		Value:     1,
	}, {
		ExampleType: "test",
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value2"},
			{Name: "label2", Value: "value2"},
			{Name: "label3", Value: "value3"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     2,
	}, {
		ExampleType: "test",
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value3"},
			{Name: "label2", Value: "value2"},
			{Name: "label4", Value: "value4"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 3,
		Value:     3,
	}}

	ps, err := table.Schema().GetDynamicParquetSchema(map[string][]string{
		"labels": {"label1", "label2", "label3", "label4"},
	})
	require.NoError(t, err)
	defer table.Schema().PutPooledParquetSchema(ps)

	ctx := context.Background()
	sc, err := pqarrow.ParquetSchemaToArrowSchema(ctx, ps.Schema, logicalplan.IterOptions{})
	require.NoError(t, err)
	r, err := samples.ToRecord(sc)
	require.NoError(t, err)

	check := func(records, buffers int) {
		fmt.Println(lsm)
		rec := 0
		buf := 0
		require.NoError(t, lsm.Scan(ctx, "", table.Schema(), nil, 0, func(ctx context.Context, v any) error {
			switch v.(type) {
			case arrow.Record:
				rec++
			case *dynparquet.SerializedBuffer:
				buf++
			}
			return nil
		}))
		require.Equal(t, records, rec)
		require.Equal(t, buf, buffers)
	}

	fmt.Println(lsm)
	lsm.Add(1, r)
	lsm.Add(1, r)
	lsm.Add(1, r)
	check(3, 0)
	require.NoError(t, lsm.merge(L0, table.Schema(), nil))
	check(0, 1)
	lsm.Add(1, r)
	check(1, 1)
	lsm.Add(1, r)
	check(2, 1)
	require.NoError(t, lsm.merge(L0, table.Schema(), nil))
	check(0, 2)
	lsm.Add(1, r)
	check(1, 2)
	require.NoError(t, lsm.merge(L1, table.Schema(), nil))
	check(1, 1)
	require.NoError(t, lsm.merge(L0, table.Schema(), nil))
	check(0, 2)
}
