package frostdb

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/pqarrow"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

// insertSamples is a helper function to insert a deterministic sample with a
// given timestamp. Note that rows inserted should be sorted by timestamp since
// it is a sorting column.
func insertSamples(ctx context.Context, t *testing.T, table *Table, timestamps ...int64) uint64 {
	t.Helper()
	samples := make(dynparquet.Samples, 0, len(timestamps))
	for _, ts := range timestamps {
		samples = append(samples, dynparquet.Sample{
			Labels: []dynparquet.Label{
				{Name: "label1", Value: "value1"},
			},
			Timestamp: ts,
		})
	}

	ps, err := table.Schema().GetDynamicParquetSchema(map[string][]string{
		"labels": {"label1"},
	})
	require.NoError(t, err)
	defer table.Schema().PutPooledParquetSchema(ps)

	sc, err := pqarrow.ParquetSchemaToArrowSchema(ctx, ps.Schema, logicalplan.IterOptions{})
	require.NoError(t, err)
	buf, err := samples.ToRecord(sc)
	require.NoError(t, err)

	tx, err := table.InsertRecord(ctx, buf)
	require.NoError(t, err)
	return tx
}

// insertSampleRecords is the same helper function as insertSamples but it inserts arrow records instead.
func insertSampleRecords(ctx context.Context, t *testing.T, table *Table, timestamps ...int64) uint64 {
	t.Helper()
	var samples dynparquet.Samples
	samples = make([]dynparquet.Sample, 0, len(timestamps))
	for _, ts := range timestamps {
		samples = append(samples, dynparquet.Sample{
			ExampleType: "ex",
			Labels: []dynparquet.Label{
				{Name: "label1", Value: "value1"},
			},
			Stacktrace: []uuid.UUID{
				{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			},
			Timestamp: ts,
		})
	}

	ps, err := table.Schema().GetDynamicParquetSchema(map[string][]string{
		"labels": {"label1"},
	})
	require.NoError(t, err)
	defer table.Schema().PutPooledParquetSchema(ps)

	sc, err := pqarrow.ParquetSchemaToArrowSchema(ctx, ps.Schema, logicalplan.IterOptions{})
	require.NoError(t, err)

	ar, err := samples.ToRecord(sc)
	require.NoError(t, err)

	tx, err := table.InsertRecord(ctx, ar)
	require.NoError(t, err)
	return tx
}
