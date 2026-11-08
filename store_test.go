package frostdb

import (
	"bytes"
	"context"
	"errors"
	"path"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/polarsignals/frostdb/dynparquet"
)

type storeTestRow struct {
	Value int64 `parquet:"value"`
}

func writeStoreTestParquet(t *testing.T, opts ...parquet.WriterOption) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[storeTestRow](&buf, opts...)
	_, err := w.Write([]storeTestRow{{Value: 1}, {Value: 2}})
	require.NoError(t, err)
	require.NoError(t, w.Close())
	return buf.Bytes()
}

func writeStoreTestBlock(t *testing.T, bkt objstore.Bucket, name string, files map[string][]byte) {
	t.Helper()
	for f, data := range files {
		require.NoError(t, bkt.Upload(context.Background(), path.Join("db", "table", name, f), bytes.NewReader(data)))
	}
}

func TestScanSkipUnreadableBlocks(t *testing.T) {
	bkt := objstore.NewInMemBucket()
	good := writeStoreTestParquet(t, parquet.KeyValueMetadata(dynparquet.DynamicColumnsKey, ""))
	writeStoreTestBlock(t, bkt, "01HZZZZZZZZZZZZZZZZZZZZZZ0", map[string][]byte{"data.parquet": good})
	writeStoreTestBlock(t, bkt, "01HZZZZZZZZZZZZZZZZZZZZZZ1", map[string][]byte{"data.parquet": good})
	writeStoreTestBlock(t, bkt, "01HZZZZZZZZZZZZZZZZZZZZZZ2", map[string][]byte{"data.parquet": nil})
	bad := []string{"01HZZZZZZZZZZZZZZZZZZZZZZ3", "01HZZZZZZZZZZZZZZZZZZZZZZ4", "01HZZZZZZZZZZZZZZZZZZZZZZ5", "not-a-ulid"}
	writeStoreTestBlock(t, bkt, bad[0], map[string][]byte{"data.parquet": good[:len(good)/2]})
	writeStoreTestBlock(t, bkt, bad[1], map[string][]byte{"data.parquet.swap": good})
	writeStoreTestBlock(t, bkt, bad[2], map[string][]byte{"data.parquet": writeStoreTestParquet(t)})
	writeStoreTestBlock(t, bkt, bad[3], map[string][]byte{"x": nil})

	var rowGroups atomic.Int64
	count := func(context.Context, any) error {
		rowGroups.Add(1)
		return nil
	}
	require.Error(t, NewDefaultObjstoreBucket(bkt).Scan(context.Background(), "db/table", nil, nil, 0, count))

	var mu sync.Mutex
	var skipped []string
	b := NewDefaultObjstoreBucket(bkt, StorageWithSkipUnreadableBlocks(func(blockDir string, err error) error {
		require.Error(t, err)
		mu.Lock()
		defer mu.Unlock()
		skipped = append(skipped, path.Base(blockDir))
		return nil
	}))
	rowGroups.Store(0)
	require.NoError(t, b.Scan(context.Background(), "db/table", nil, nil, 0, count))
	require.Equal(t, int64(2), rowGroups.Load())
	require.ElementsMatch(t, bad, skipped)
}

func TestScanSkipUnreadableBlocksReturnsHandlerError(t *testing.T) {
	bkt := objstore.NewInMemBucket()
	good := writeStoreTestParquet(t, parquet.KeyValueMetadata(dynparquet.DynamicColumnsKey, ""))
	writeStoreTestBlock(t, bkt, "01HZZZZZZZZZZZZZZZZZZZZZZ0", map[string][]byte{"data.parquet": good[:len(good)/2]})
	errStop := errors.New("stop")
	b := NewDefaultObjstoreBucket(bkt, StorageWithSkipUnreadableBlocks(func(string, error) error { return errStop }))

	err := b.Scan(context.Background(), "db/table", nil, nil, 0, func(context.Context, any) error { return nil })
	require.ErrorIs(t, err, errStop)
}

func TestScanSkipUnreadableBlocksReturnsCallbackError(t *testing.T) {
	bkt := objstore.NewInMemBucket()
	good := writeStoreTestParquet(t, parquet.KeyValueMetadata(dynparquet.DynamicColumnsKey, ""))
	writeStoreTestBlock(t, bkt, "01HZZZZZZZZZZZZZZZZZZZZZZ0", map[string][]byte{"data.parquet": good})
	b := NewDefaultObjstoreBucket(bkt, StorageWithSkipUnreadableBlocks(func(blockDir string, err error) error {
		t.Errorf("skipped %s: %v", blockDir, err)
		return nil
	}))
	errStop := errors.New("stop")

	err := b.Scan(context.Background(), "db/table", nil, nil, 0, func(context.Context, any) error { return errStop })
	require.ErrorIs(t, err, errStop)
}
