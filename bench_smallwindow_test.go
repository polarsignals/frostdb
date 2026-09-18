package frostdb

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/google/uuid"
	"github.com/polarsignals/iceberg-go"
	"github.com/polarsignals/iceberg-go/catalog"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/query"
	"github.com/polarsignals/frostdb/query/logicalplan"
	"github.com/polarsignals/frostdb/samples"
	"github.com/polarsignals/frostdb/storage"
)

// BenchmarkQuerySmallWindowManyBlocks measures a query over a small, fixed time
// window as the number of persisted blocks grows, using the default objstore
// backend. Each block covers a distinct range of timestamps, and the query
// always asks for only the newest block's range -- so every run matches the
// same amount of data, and only the number of older, non-matching blocks
// changes.
//
// If blocks were eliminated by their time range before being opened, the cost
// would be roughly constant across block counts. To serve a query the store
// opens each persisted block and decodes its Parquet metadata (the footer, and
// the column/page index), then filters row groups by the predicate only after
// the block is already open -- there is no step that skips a block whose time
// range is outside the query. So a small-window query's cost grows with the
// total number of blocks retained, not with the size of the window.
//
// The growth is clearest in allocs/op, which is close to linear in the block
// count; wall time grows more slowly because blocks are opened concurrently.
//
// Run with:
//
//	go test -run '^$' -bench BenchmarkQuerySmallWindowManyBlocks -benchmem -benchtime=20x
func BenchmarkQuerySmallWindowManyBlocks(b *testing.B) {
	benchmarkSmallWindowManyBlocks(b, func(bucket objstore.Bucket) DataSinkSource {
		return NewDefaultObjstoreBucket(bucket)
	})
}

// BenchmarkQuerySmallWindowManyBlocksIceberg runs the exact same workload as
// BenchmarkQuerySmallWindowManyBlocks against the Iceberg backend, whose
// manifests carry per-data-file upper/lower bounds for the partition columns.
// Here the table is partitioned by timestamp, so the query planner can skip a
// data file whose timestamp bounds fall entirely outside the query window
// without opening it. The matched data is again always just the newest block.
//
// Pruning does not make the cost flat -- the manifest still has one entry per
// data file, so reading it is O(total files) -- but it is far cheaper than
// opening and decoding each block's Parquet metadata, so the cost grows much
// more slowly with the block count than the default backend does. The manifest
// and catalog machinery also add a fixed overhead, so for a small number of
// blocks Iceberg is actually more expensive than the default backend; it wins
// once enough non-matching blocks accumulate.
//
// Measured alongside BenchmarkQuerySmallWindowManyBlocks
// (-benchmem -benchtime=20x), allocs/op -- default vs Iceberg:
//
//	blocks=1      4,624  vs   14,436
//	blocks=25    12,271  vs   18,284
//	blocks=100   36,126  vs   30,112
//	blocks=400  131,535  vs   77,539
//
// i.e. ~28x growth (default) vs ~5x growth (Iceberg) from 1 to 400 blocks.
//
// (WithIcebergPartitionSpec does not physically repartition the data files; it
// records each file's column bounds in the manifest, which is what enables the
// skip. See storage.WithIcebergPartitionSpec.)
//
// Run with:
//
//	go test -run '^$' -bench BenchmarkQuerySmallWindowManyBlocksIceberg -benchmem -benchtime=20x
func BenchmarkQuerySmallWindowManyBlocksIceberg(b *testing.B) {
	benchmarkSmallWindowManyBlocks(b, func(bucket objstore.Bucket) DataSinkSource {
		berg, err := storage.NewIceberg("/", catalog.NewHDFS("/", bucket), bucket,
			storage.WithIcebergPartitionSpec(
				iceberg.NewPartitionSpec( // Partition the table by timestamp.
					iceberg.PartitionField{
						Name:      "timestamp",
						Transform: iceberg.IdentityTransform{},
					},
				),
			))
		require.NoError(b, err)
		return berg
	})
}

// benchmarkSmallWindowManyBlocks is the shared harness for the two benchmarks
// above. makeStore builds the DataSinkSource under test from the bucket that
// holds the persisted blocks.
func benchmarkSmallWindowManyBlocks(b *testing.B, makeStore func(objstore.Bucket) DataSinkSource) {
	const rowsPerBlock = 200

	for _, numBlocks := range []int{1, 25, 100, 400} {
		b.Run(fmt.Sprintf("blocks=%d", numBlocks), func(b *testing.B) {
			ctx := context.Background()
			// The bucket holds the persisted blocks and survives the store
			// being closed and reopened below.
			bucket := objstore.NewInMemBucket()
			sinksource := makeStore(bucket)

			writeBlocks(b, sinksource, numBlocks, rowsPerBlock)

			// Reopen against the same bucket. The blocks now live only in the
			// bucket, so a query reads them back through the bucket-scan path --
			// the path that opens and decodes each block's Parquet metadata.
			// This is the state a server is in after a restart, and the state a
			// long-running server reaches once old blocks leave active memory.
			c, err := New(
				WithStoragePath(b.TempDir()),
				WithWAL(),
				WithReadWriteStorage(sinksource),
				WithManualBlockRotation(),
				WithActiveMemorySize(64*KiB),
			)
			require.NoError(b, err)
			b.Cleanup(func() { require.NoError(b, c.Close()) })
			db, err := c.DB(ctx, "bench")
			require.NoError(b, err)
			_, err = db.Table("bench", NewTableConfig(dynparquet.SampleDefinition()))
			require.NoError(b, err)

			engine := query.NewEngine(memory.NewGoAllocator(), db.TableProvider())

			// The window covers ONLY the newest block, regardless of how many
			// older blocks exist.
			from := int64(numBlocks-1) * 1000
			to := from + rowsPerBlock

			var rows int64
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				rows = 0
				err := engine.ScanTable("bench").
					Filter(logicalplan.And(
						logicalplan.Col("timestamp").Gt(logicalplan.Literal(from-1)),
						logicalplan.Col("timestamp").Lt(logicalplan.Literal(to)),
					)).
					Aggregate(
						[]*logicalplan.AggregationFunction{logicalplan.Sum(logicalplan.Col("value"))},
						[]logicalplan.Expr{logicalplan.Col("stacktrace")},
					).
					Execute(ctx, func(_ context.Context, r arrow.Record) error {
						rows += r.NumRows()
						return nil
					})
				require.NoError(b, err)
			}
			b.StopTimer()
			// Guard against silently benchmarking an empty query: the window
			// must match the newest block's rows. Without this a filter or
			// schema change that matched nothing would still "pass" and just
			// report fast, flat timings.
			require.Positive(b, rows, "query matched no rows")
		})
	}
}

// writeBlocks writes numBlocks persisted blocks into the bucket behind
// sinksource, one per time bucket (block i covers timestamps
// [i*1000, i*1000+rowsPerBlock)), then closes the store so the blocks live only
// in the bucket.
func writeBlocks(b *testing.B, sinksource DataSinkSource, numBlocks, rowsPerBlock int) {
	b.Helper()
	ctx := context.Background()
	c, err := New(
		WithStoragePath(b.TempDir()),
		WithWAL(),
		WithReadWriteStorage(sinksource),
		WithManualBlockRotation(),
		WithActiveMemorySize(64*KiB),
	)
	require.NoError(b, err)
	db, err := c.DB(ctx, "bench")
	require.NoError(b, err)
	table, err := db.Table("bench", NewTableConfig(dynparquet.SampleDefinition()))
	require.NoError(b, err)

	for i := 0; i < numBlocks; i++ {
		base := int64(i) * 1000
		ss := make(samples.Samples, 0, rowsPerBlock)
		for r := 0; r < rowsPerBlock; r++ {
			ss = append(ss, samples.Sample{
				ExampleType: "cpu",
				Labels:      map[string]string{"instance": fmt.Sprintf("i-%d", r%8)},
				Stacktrace:  []uuid.UUID{{byte(r), byte(r >> 8)}},
				Timestamp:   base + int64(r),
				Value:       int64(r),
			})
		}
		rec, err := ss.ToRecord()
		require.NoError(b, err)
		writeTx, err := table.InsertRecord(ctx, rec)
		require.NoError(b, err)
		require.NoError(b, table.RotateBlock(ctx, table.ActiveBlock()))
		db.Wait(writeTx + 2) // wait for the async block-persist txn
	}
	require.NoError(b, c.Close())
}
