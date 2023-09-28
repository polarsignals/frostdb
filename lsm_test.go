package frostdb

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/stretchr/testify/require"
)

func check(t *testing.T, lsm *LSM, records, buffers int) {
	t.Helper()
	seen := map[SentinelType]bool{}
	lsm.levels.Iterate(func(node *Node) bool {
		if node.part == nil {
			if seen[node.sentinel] {
				t.Fatal("duplicate sentinel")
			}
			seen[node.sentinel] = true
		}
		return true
	})
	rec := 0
	buf := 0
	require.NoError(t, lsm.Scan(context.Background(), "", nil, nil, 2, func(ctx context.Context, v any) error {
		switch v.(type) {
		case arrow.Record:
			rec++
		case dynparquet.DynamicRowGroup:
			buf++
		}
		return nil
	}))
	require.Equal(t, records, rec)
	require.Equal(t, buf, buffers)
}

func Test_LSM_Basic(t *testing.T) {
	t.Parallel()
	c, table := basicTable(t)
	defer c.Close()

	lsm, err := NewLSM("test", []*LevelConfig{
		{Level: L0, MaxSize: 1024 * 1024 * 1024, Compact: table.parquetCompaction},
		{Level: L1, MaxSize: 1024 * 1024 * 1024, Compact: table.parquetCompaction},
		{Level: L2, MaxSize: 1024 * 1024 * 1024},
	})
	require.NoError(t, err)

	samples := dynparquet.NewTestSamples()
	r, err := samples.ToRecord()
	require.NoError(t, err)

	lsm.Add(1, r)
	lsm.Add(1, r)
	lsm.Add(1, r)
	check(t, lsm, 3, 0)
	require.NoError(t, lsm.merge(L0, nil))
	check(t, lsm, 0, 1)
	lsm.Add(1, r)
	check(t, lsm, 1, 1)
	lsm.Add(1, r)
	check(t, lsm, 2, 1)
	require.NoError(t, lsm.merge(L0, nil))
	check(t, lsm, 0, 2)
	lsm.Add(1, r)
	check(t, lsm, 1, 2)
	require.NoError(t, lsm.merge(L1, nil))
	check(t, lsm, 1, 1)
	require.NoError(t, lsm.merge(L0, nil))
	check(t, lsm, 0, 2)
}

func Test_LSM_DuplicateSentinel(t *testing.T) {
	t.Parallel()

	c, table := basicTable(t)
	defer c.Close()

	lsm, err := NewLSM("test", []*LevelConfig{
		{Level: L0, MaxSize: 1024 * 1024 * 1024, Compact: table.parquetCompaction},
		{Level: L1, MaxSize: 1024 * 1024 * 1024, Compact: table.parquetCompaction},
		{Level: L2, MaxSize: 1024 * 1024 * 1024},
	})
	require.NoError(t, err)

	samples := dynparquet.NewTestSamples()
	r, err := samples.ToRecord()
	require.NoError(t, err)

	lsm.Add(1, r)
	lsm.Add(1, r)
	lsm.Add(1, r)
	check(t, lsm, 3, 0)
	require.NoError(t, lsm.merge(L0, nil))
	check(t, lsm, 0, 1)
	require.NoError(t, lsm.merge(L0, nil))
	check(t, lsm, 0, 1)
}

func Test_LSM_Compaction(t *testing.T) {
	t.Parallel()
	c, table := basicTable(t)
	defer c.Close()

	lsm, err := NewLSM("test", []*LevelConfig{
		{Level: L0, MaxSize: 1, Compact: table.parquetCompaction},
		{Level: L1, MaxSize: 1024 * 1024 * 1024},
	})
	require.NoError(t, err)

	samples := dynparquet.NewTestSamples()
	r, err := samples.ToRecord()
	require.NoError(t, err)

	lsm.Add(1, r)
	require.Eventually(t, func() bool {
		return lsm.sizes[L0].Load() == 0 && lsm.sizes[L1].Load() != 0
	}, time.Second, 5*time.Millisecond)
}

func Test_LSM_CascadeCompaction(t *testing.T) {
	t.Parallel()
	c, table := basicTable(t)
	defer c.Close()

	lsm, err := NewLSM("test", []*LevelConfig{
		{Level: L0, MaxSize: 257, Compact: table.parquetCompaction},
		{Level: L1, MaxSize: 2281, Compact: table.parquetCompaction},
		{Level: L2, MaxSize: 2281, Compact: table.parquetCompaction},
		{Level: 3, MaxSize: 2281, Compact: table.parquetCompaction},
		{Level: 4, MaxSize: 2281},
	})
	require.NoError(t, err)

	samples := dynparquet.NewTestSamples()
	r, err := samples.ToRecord()
	require.NoError(t, err)

	lsm.Add(1, r)
	require.Eventually(t, func() bool {
		return lsm.sizes[L0].Load() == 0 &&
			lsm.sizes[L1].Load() != 0 &&
			lsm.sizes[L2].Load() == 0 &&
			lsm.sizes[3].Load() == 0 &&
			lsm.sizes[4].Load() == 0
	}, time.Second, 5*time.Millisecond)
	lsm.Add(1, r)
	require.Eventually(t, func() bool {
		return lsm.sizes[L0].Load() == 0 &&
			lsm.sizes[L1].Load() == 0 &&
			lsm.sizes[L2].Load() == 0 &&
			lsm.sizes[3].Load() == 0 &&
			lsm.sizes[4].Load() != 0
	}, time.Second, 5*time.Millisecond)
}
