package index

import (
	"context"
	"errors"
	"io"
	"math"
	"math/rand"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"

	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/parts"
)

func compactParts(w io.Writer, compact []parts.Part, _ ...parquet.WriterOption) (int64, error) {
	schema := dynparquet.NewSampleSchema()
	bufs := []dynparquet.DynamicRowGroup{}
	var size int64
	for _, part := range compact {
		size += part.Size()
		buf, err := part.AsSerializedBuffer(schema)
		if err != nil {
			return 0, err
		}
		bufs = append(bufs, buf.MultiDynamicRowGroup())
	}
	merged, err := schema.MergeDynamicRowGroups(bufs)
	if err != nil {
		return 0, err
	}
	err = func() error {
		writer, err := schema.GetWriter(w, merged.DynamicColumns(), false)
		if err != nil {
			return err
		}
		defer writer.Close()

		rows := merged.Rows()
		defer rows.Close()

		buf := make([]parquet.Row, merged.NumRows())
		if _, err := rows.ReadRows(buf); err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		if _, err := writer.WriteRows(buf); err != nil && !errors.Is(err, io.EOF) {
			return err
		}

		return nil
	}()
	if err != nil {
		return 0, err
	}

	return size, nil
}

func check(t *testing.T, lsm *LSM, records, buffers int) {
	t.Helper()
	seen := map[SentinelType]bool{}
	lsm.partList.Iterate(func(node *Node) bool {
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
	require.NoError(t, lsm.Scan(context.Background(), "", nil, nil, math.MaxUint64, func(_ context.Context, v any) error {
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
	lsm, err := NewLSM("test", nil, []*LevelConfig{
		{Level: L0, MaxSize: 1024 * 1024 * 1024, Type: CompactionTypeParquetMemory, Compact: compactParts},
		{Level: L1, MaxSize: 1024 * 1024 * 1024, Type: CompactionTypeParquetMemory, Compact: compactParts},
		{Level: L2, MaxSize: 1024 * 1024 * 1024},
	},
		func() uint64 { return math.MaxUint64 },
	)
	require.NoError(t, err)

	samples := dynparquet.NewTestSamples()
	r, err := samples.ToRecord()
	require.NoError(t, err)

	lsm.Add(1, r)
	lsm.Add(2, r)
	lsm.Add(3, r)
	check(t, lsm, 3, 0)
	require.NoError(t, lsm.merge(L0))
	check(t, lsm, 0, 1)
	lsm.Add(4, r)
	check(t, lsm, 1, 1)
	lsm.Add(5, r)
	check(t, lsm, 2, 1)
	require.NoError(t, lsm.merge(L0))
	check(t, lsm, 0, 2)
	lsm.Add(6, r)
	check(t, lsm, 1, 2)
	require.NoError(t, lsm.merge(L1))
	check(t, lsm, 1, 1)
	require.NoError(t, lsm.merge(L0))
	check(t, lsm, 0, 2)
}

func Test_LSM_DuplicateSentinel(t *testing.T) {
	t.Parallel()
	lsm, err := NewLSM("test", nil, []*LevelConfig{
		{Level: L0, MaxSize: 1024 * 1024 * 1024, Type: CompactionTypeParquetMemory, Compact: compactParts},
		{Level: L1, MaxSize: 1024 * 1024 * 1024, Type: CompactionTypeParquetMemory, Compact: compactParts},
		{Level: L2, MaxSize: 1024 * 1024 * 1024},
	},
		func() uint64 { return math.MaxUint64 },
	)
	require.NoError(t, err)

	samples := dynparquet.NewTestSamples()
	r, err := samples.ToRecord()
	require.NoError(t, err)

	lsm.Add(1, r)
	lsm.Add(2, r)
	lsm.Add(3, r)
	check(t, lsm, 3, 0)
	require.NoError(t, lsm.merge(L0))
	check(t, lsm, 0, 1)
	require.NoError(t, lsm.merge(L0))
	check(t, lsm, 0, 1)
}

func Test_LSM_Compaction(t *testing.T) {
	t.Parallel()
	lsm, err := NewLSM("test", nil, []*LevelConfig{
		{Level: L0, MaxSize: 1, Type: CompactionTypeParquetMemory, Compact: compactParts},
		{Level: L1, MaxSize: 1024 * 1024 * 1024},
	},
		func() uint64 { return math.MaxUint64 },
	)
	require.NoError(t, err)

	samples := dynparquet.NewTestSamples()
	r, err := samples.ToRecord()
	require.NoError(t, err)

	lsm.Add(1, r)
	require.Eventually(t, func() bool {
		return lsm.sizes[L0].Load() == 0 && lsm.sizes[L1].Load() != 0
	}, 30*time.Second, 10*time.Millisecond)
}

func Test_LSM_CascadeCompaction(t *testing.T) {
	t.Parallel()
	lsm, err := NewLSM("test", nil, []*LevelConfig{
		{Level: L0, MaxSize: 257, Type: CompactionTypeParquetMemory, Compact: compactParts},
		{Level: L1, MaxSize: 2281, Type: CompactionTypeParquetMemory, Compact: compactParts},
		{Level: L2, MaxSize: 2281, Type: CompactionTypeParquetMemory, Compact: compactParts},
		{Level: 3, MaxSize: 2281, Type: CompactionTypeParquetMemory, Compact: compactParts},
		{Level: 4, MaxSize: 2281},
	},
		func() uint64 { return math.MaxUint64 },
	)
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
	}, 3*time.Second, 10*time.Millisecond)
	lsm.Add(2, r)
	require.Eventually(t, func() bool {
		return lsm.sizes[L0].Load() == 0 &&
			lsm.sizes[L1].Load() == 0 &&
			lsm.sizes[L2].Load() == 0 &&
			lsm.sizes[3].Load() == 0 &&
			lsm.sizes[4].Load() != 0
	}, 30*time.Second, 10*time.Millisecond)
}

func Test_LSM_InOrderInsert(t *testing.T) {
	t.Parallel()
	lsm, err := NewLSM("test", nil, []*LevelConfig{
		{Level: L0, MaxSize: 1024 * 1024 * 1024, Type: CompactionTypeParquetMemory, Compact: compactParts},
		{Level: L1, MaxSize: 1024 * 1024 * 1024, Type: CompactionTypeParquetMemory, Compact: compactParts},
		{Level: L2, MaxSize: 1024 * 1024 * 1024},
	},
		func() uint64 { return math.MaxUint64 },
	)
	require.NoError(t, err)

	samples := dynparquet.NewTestSamples()
	r, err := samples.ToRecord()
	require.NoError(t, err)

	wg := &sync.WaitGroup{}
	workers := 100
	inserts := 100
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < inserts; j++ {
				lsm.Add(rand.Uint64(), r)
			}
		}()
	}
	wg.Wait()

	tx := make([]uint64, 0, workers*inserts)
	lsm.Iterate(func(node *Node) bool {
		if node.part != nil {
			tx = append(tx, node.part.TX())
		}
		return true
	})

	// check that the transactions are sorted in descending order
	require.True(t, slices.IsSortedFunc[[]uint64, uint64](tx, func(i, j uint64) int {
		if i < j {
			return 1
		} else if i > j {
			return -1
		}

		return 0
	}))
}
