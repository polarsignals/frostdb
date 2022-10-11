package frostdb

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/polarsignals/frostdb/dynparquet"
)

func TestCompaction(t *testing.T) {
	c, table := basicTable(t)
	defer c.Close()
	table.config.rowGroupSize = 2 // override row group size setting

	samples := dynparquet.Samples{{
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
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value1"},
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
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value1"},
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

	ctx := context.Background()
	tx, err := table.Write(ctx, samples[0], samples[1], samples[2])
	require.NoError(t, err)

	// Wait for the writes to have been marked as completed
	table.db.Wait(tx)

	// Expect only a single granule to have been created
	require.Equal(t, 1, table.active.Index().Len())

	// Force compaction on the granule
	g := (table.active.Index().Min()).(*Granule)
	table.active.compactGranule(g)

	// Expect the granule to have been compacted into a single granule
	require.Equal(t, 1, table.active.Index().Len())

	// Expect the granule to contain a single part with multiple row groups
	g = (table.active.Index().Min()).(*Granule)
	parts := 0
	g.parts.Iterate(func(p *Part) bool {
		parts++
		require.Equal(t, 2, p.Buf.NumRowGroups())
		return true
	})

	// There should only be a single part that contains multiple row groups
	require.Equal(t, 1, parts)
}
