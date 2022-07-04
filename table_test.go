package frostdb

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/google/btree"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/segmentio/parquet-go"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore/filesystem"
	"go.uber.org/atomic"

	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

type testOutput struct {
	t *testing.T
}

func (l *testOutput) Write(p []byte) (n int, err error) {
	l.t.Log(string(p))
	return len(p), nil
}

func newTestLogger(t *testing.T) log.Logger {
	logger := log.NewLogfmtLogger(log.NewSyncWriter(&testOutput{t: t}))
	logger = level.NewFilter(logger, level.AllowDebug())
	return logger
}

func basicTable(t *testing.T, granuleSize int) *Table {
	config := NewTableConfig(
		dynparquet.NewSampleSchema(),
	)

	bucket, err := filesystem.NewBucket(".")
	require.NoError(t, err)

	c := New(
		nil,
		granuleSize,
		512*1024*1024,
	).WithStorageBucket(bucket)

	db, err := c.DB("test")
	require.NoError(t, err)
	table, err := db.Table("test", config, newTestLogger(t))
	require.NoError(t, err)

	return table
}

func TestTable(t *testing.T) {
	table := basicTable(t, 2^12)

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

	buf, err := samples.ToBuffer(table.Schema())
	require.NoError(t, err)

	ctx := context.Background()
	_, err = table.InsertBuffer(ctx, buf)
	require.NoError(t, err)

	samples = dynparquet.Samples{{
		ExampleType: "test",
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value1"},
			{Name: "label2", Value: "value2"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     2,
	}}

	buf, err = samples.ToBuffer(table.Schema())
	require.NoError(t, err)

	_, err = table.InsertBuffer(ctx, buf)
	require.NoError(t, err)

	samples = dynparquet.Samples{{
		ExampleType: "test",
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value1"},
			{Name: "label2", Value: "value2"},
			{Name: "label3", Value: "value3"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 3,
		Value:     3,
	}}

	buf, err = samples.ToBuffer(table.Schema())
	require.NoError(t, err)

	_, err = table.InsertBuffer(ctx, buf)
	require.NoError(t, err)

	pool := memory.NewGoAllocator()

	err = table.View(func(tx uint64) error {
		as, err := table.ArrowSchema(ctx, tx, pool, nil, nil, nil)
		if err != nil {
			return err
		}

		return table.Iterator(
			ctx,
			tx,
			pool,
			as,
			nil,
			nil,
			nil,
			func(ar arrow.Record) error {
				t.Log(ar)
				defer ar.Release()
				return nil
			},
		)
	})
	require.NoError(t, err)

	uuid1 := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	uuid2 := uuid.MustParse("00000000-0000-0000-0000-000000000002")

	// One granule with 3 parts
	require.Equal(t, 1, table.active.Index().Len())
	require.Equal(t, uint64(3), table.active.Index().Min().(*Granule).parts.total.Load())
	require.Equal(t, uint64(5), table.active.Index().Min().(*Granule).metadata.card.Load())
	require.Equal(t, parquet.Row{
		parquet.ValueOf("test").Level(0, 0, 0),
		parquet.ValueOf("value1").Level(0, 1, 1),
		parquet.ValueOf("value2").Level(0, 1, 2),
		parquet.ValueOf(nil).Level(0, 0, 3),
		parquet.ValueOf(nil).Level(0, 0, 4),
		parquet.ValueOf(append(uuid1[:], uuid2[:]...)).Level(0, 0, 5),
		parquet.ValueOf(1).Level(0, 0, 6),
		parquet.ValueOf(1).Level(0, 0, 7),
	}, (*dynparquet.DynamicRow)(table.active.Index().Min().(*Granule).metadata.least.Load()).Row)
	require.Equal(t, 1, table.active.Index().Len())
}

func Test_Table_GranuleSplit(t *testing.T) {
	table := basicTable(t, 4)

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

	buf, err := samples.ToBuffer(table.Schema())
	require.NoError(t, err)

	ctx := context.Background()
	_, err = table.InsertBuffer(ctx, buf)
	require.NoError(t, err)

	samples = dynparquet.Samples{{
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value1"},
			{Name: "label2", Value: "value2"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     2,
	}}

	buf, err = samples.ToBuffer(table.Schema())
	require.NoError(t, err)

	_, err = table.InsertBuffer(ctx, buf)
	require.NoError(t, err)

	samples = dynparquet.Samples{{
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value1"},
			{Name: "label2", Value: "value2"},
			{Name: "label3", Value: "value3"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 3,
		Value:     3,
	}}

	buf, err = samples.ToBuffer(table.Schema())
	require.NoError(t, err)

	// Wait for the index to be updated by the asynchronous granule split.
	table.Sync()

	tx, err := table.InsertBuffer(ctx, buf)
	require.NoError(t, err)

	// Wait for the last tx to be marked as completed
	table.db.Wait(tx)

	// Because inserts happen in parallel to compaction both of the triggered compactions may have aborted because the writes weren't completed.
	// Manually perform the compaction if we run into this corner case.
	for table.active.Index().Len() == 1 {
		table.active.Index().Ascend(func(i btree.Item) bool {
			g := i.(*Granule)
			table.active.wg.Add(1)
			table.active.compact(g)
			return false
		})
	}

	err = table.View(func(tx uint64) error {
		pool := memory.NewGoAllocator()
		as, err := table.ArrowSchema(ctx, tx, pool, nil, nil, nil)
		if err != nil {
			return err
		}

		return table.Iterator(ctx, tx, pool, as, nil, nil, nil, func(r arrow.Record) error {
			defer r.Release()
			t.Log(r)
			return nil
		})
	})
	require.NoError(t, err)

	require.Equal(t, 2, table.active.Index().Len())
	require.Equal(t, uint64(2), table.active.Index().Min().(*Granule).metadata.card.Load())
	require.Equal(t, uint64(3), table.active.Index().Max().(*Granule).metadata.card.Load())
}

/*

	This test is meant for the following case
	If the table index is as follows

	[10,11]
		\
		[12,13,14]


	And we try and insert [8,9], we expect them to be inserted into the top granule

	[8,9,10,11]
		\
		[12,13]

*/
func Test_Table_InsertLowest(t *testing.T) {
	table := basicTable(t, 4)

	samples := dynparquet.Samples{{
		Labels: []dynparquet.Label{
			{Name: "label13", Value: "value13"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     2,
	}, {
		Labels: []dynparquet.Label{
			{Name: "label12", Value: "value12"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     2,
	}, {
		Labels: []dynparquet.Label{
			{Name: "label11", Value: "value11"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     2,
	}, {
		Labels: []dynparquet.Label{
			{Name: "label10", Value: "value10"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     2,
	}}

	buf, err := samples.ToBuffer(table.Schema())
	require.NoError(t, err)

	ctx := context.Background()

	// Since we are inserting 4 elements and the granule size is 4, the granule
	// will immediately split.
	_, err = table.InsertBuffer(ctx, buf)
	require.NoError(t, err)

	// Since a compaction happens async, it may abort if it runs before the transactions are completed. In that case; we'll manually compact the granule
	table.Sync()
	if table.active.Index().Len() == 1 {
		table.active.wg.Add(1)
		table.active.compact(table.active.Index().Min().(*Granule))
		table.Sync()
	}

	require.Equal(t, 2, table.active.Index().Len())
	require.Equal(t, uint64(2), table.active.Index().Min().(*Granule).metadata.card.Load()) // [13, 12]
	require.Equal(t, uint64(2), table.active.Index().Max().(*Granule).metadata.card.Load()) // [11, 10]

	samples = dynparquet.Samples{{
		Labels: []dynparquet.Label{
			{Name: "label14", Value: "value14"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     2,
	}}

	buf, err = samples.ToBuffer(table.Schema())
	require.NoError(t, err)

	_, err = table.InsertBuffer(ctx, buf)
	require.NoError(t, err)

	// Wait for the index to be updated by the asynchronous granule split.
	table.Sync()

	require.Equal(t, 2, table.active.Index().Len())
	require.Equal(t, uint64(3), table.active.Index().Min().(*Granule).metadata.card.Load()) // [14, 13, 12]
	require.Equal(t, uint64(2), table.active.Index().Max().(*Granule).metadata.card.Load()) // [11, 10]

	// Insert a new column that is the lowest column yet; expect it to be added to the minimum column
	samples = dynparquet.Samples{{
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value1"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 10,
		Value:     10,
	}}

	buf, err = samples.ToBuffer(table.Schema())
	require.NoError(t, err)

	_, err = table.InsertBuffer(ctx, buf)
	require.NoError(t, err)

	// Wait for the index to be updated by the asynchronous granule split.
	table.Sync()

	pool := memory.NewGoAllocator()

	err = table.View(func(tx uint64) error {
		as, err := table.ArrowSchema(ctx, tx, pool, nil, nil, nil)
		if err != nil {
			return err
		}

		return table.Iterator(ctx, tx, pool, as, nil, nil, nil, func(r arrow.Record) error {
			defer r.Release()
			t.Log(r)
			return nil
		})
	})
	require.NoError(t, err)

	require.Equal(t, 2, table.active.Index().Len())
	require.Equal(t, uint64(3), table.active.Index().Min().(*Granule).metadata.card.Load()) // [14,13,12]
	require.Equal(t, uint64(3), table.active.Index().Max().(*Granule).metadata.card.Load()) // [11,10,1]
}

// This test issues concurrent writes to the database, and expects all of them to be recorded successfully.
func Test_Table_Concurrency(t *testing.T) {
	tests := map[string]struct {
		granuleSize int
	}{
		"8192": {8192},
		"4096": {4096},
		"2048": {2048},
		"1024": {1024},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			table := basicTable(t, test.granuleSize)
			defer os.RemoveAll("test")

			generateRows := func(n int) *dynparquet.Buffer {
				rows := make(dynparquet.Samples, 0, n)
				for i := 0; i < n; i++ {
					rows = append(rows, dynparquet.Sample{
						Labels: []dynparquet.Label{ // TODO would be nice to not have all the same column
							{Name: "label1", Value: "value1"},
							{Name: "label2", Value: "value2"},
						},
						Stacktrace: []uuid.UUID{
							{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
							{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
						},
						Timestamp: rand.Int63(),
						Value:     rand.Int63(),
					})
				}
				buf, err := rows.ToBuffer(table.Schema())
				require.NoError(t, err)

				buf.Sort()

				// This is necessary because sorting a buffer makes concurrent reading not
				// safe as the internal pages are cyclically sorted at read time. Cloning
				// executes the cyclic sort once and makes the resulting buffer safe for
				// concurrent reading as it no longer has to perform the cyclic sorting at
				// read time. This should probably be improved in the parquet library.
				buf, err = buf.Clone()
				require.NoError(t, err)

				return buf
			}

			// Spawn n workers that will insert values into the table
			maxTxID := atomic.NewUint64(0)
			n := 8
			inserts := 100
			rows := 10
			wg := &sync.WaitGroup{}
			ctx := context.Background()
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < inserts; i++ {
						tx, err := table.InsertBuffer(ctx, generateRows(rows))
						if err != nil {
							fmt.Println("Received error on insert: ", err)
						}

						//	 Set the max tx id that we've seen
						if maxTX := maxTxID.Load(); tx > maxTX {
							maxTxID.CAS(maxTX, tx)
						}
					}
				}()
			}

			// Wait for all our writes to exit
			wg.Wait()

			// Wait for our last tx to be marked as complete
			table.db.Wait(maxTxID.Load())

			pool := memory.NewGoAllocator()

			err := table.View(func(tx uint64) error {
				totalrows := int64(0)

				as, err := table.ArrowSchema(ctx, tx, pool, nil, nil, nil)
				if err != nil {
					return err
				}

				err = table.Iterator(ctx, tx, pool, as, nil, nil, nil, func(ar arrow.Record) error {
					totalrows += ar.NumRows()
					defer ar.Release()

					return nil
				})

				require.NoError(t, err)
				require.Equal(t, int64(n*inserts*rows), totalrows)
				return nil
			})
			require.NoError(t, err)
		})
	}
}

func Benchmark_Table_Insert_10Rows_10Iter_10Writers(b *testing.B) {
	benchmarkTableInserts(b, 10, 10, 10)
}

func Benchmark_Table_Insert_100Row_100Iter_100Writers(b *testing.B) {
	benchmarkTableInserts(b, 100, 100, 100)
}

func benchmarkTableInserts(b *testing.B, rows, iterations, writers int) {
	var (
		ctx    = context.Background()
		config = NewTableConfig(
			dynparquet.NewSampleSchema(),
		)
		c = New(prometheus.NewRegistry(), 512, 512*1024*1024)
	)
	db, err := c.DB("test")
	require.NoError(b, err)
	generateRows := func(id string, n int) *dynparquet.Buffer {
		rows := make(dynparquet.Samples, 0, n)
		for i := 0; i < n; i++ {
			rows = append(rows, dynparquet.Sample{
				Labels: []dynparquet.Label{ // TODO would be nice to not have all the same column
					{Name: "label1", Value: id},
					{Name: "label2", Value: "value2"},
				},
				Stacktrace: []uuid.UUID{
					{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
					{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
				},
				Timestamp: rand.Int63(),
				Value:     int64(i),
			})
		}

		buf, err := rows.ToBuffer(config.schema)
		require.NoError(b, err)

		buf.Sort()
		return buf
	}

	// Pre-generate all rows we're inserting
	inserts := make(map[string][]*dynparquet.Buffer, writers)
	for i := 0; i < writers; i++ {
		id := uuid.New().String()
		inserts[id] = make([]*dynparquet.Buffer, iterations)
		for j := 0; j < iterations; j++ {
			inserts[id][j] = generateRows(id, rows)
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Create table for test
		table, err := db.Table(uuid.New().String(), config, log.NewNopLogger())
		require.NoError(b, err)
		// Spawn n workers that will insert values into the table
		wg := &sync.WaitGroup{}
		for id := range inserts {
			wg.Add(1)
			go func(id string, tbl *Table, w *sync.WaitGroup) {
				defer w.Done()
				var (
					maxTx uint64
					err   error
				)
				for i := 0; i < iterations; i++ {
					if maxTx, err = tbl.InsertBuffer(ctx, inserts[id][i]); err != nil {
						fmt.Println("Received error on insert: ", err)
					}
				}
				db.Wait(maxTx)
			}(id, table, wg)
		}
		wg.Wait()

		b.StopTimer()
		pool := memory.NewGoAllocator()
		// Wait for all compaction routines to complete
		table.Sync()

		// Calculate the number of entries in database
		totalrows := int64(0)
		err = table.View(func(tx uint64) error {
			as, err := table.ArrowSchema(ctx, tx, pool, nil, nil, nil)
			if err != nil {
				return err
			}
			return table.Iterator(ctx, tx, pool, as, nil, nil, nil, func(ar arrow.Record) error {
				defer ar.Release()
				totalrows += ar.NumRows()

				return nil
			})
		})
		require.Equal(b, 0., testutil.ToFloat64(table.metrics.granulesCompactionAborted))
		require.NoError(b, err)
		require.Equal(b, int64(rows*iterations*writers), totalrows)

		b.StartTimer()
	}
}

func Test_Table_ReadIsolation(t *testing.T) {
	table := basicTable(t, 2<<12)

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

	buf, err := samples.ToBuffer(table.Schema())
	require.NoError(t, err)

	ctx := context.Background()

	_, err = table.InsertBuffer(ctx, buf)
	require.NoError(t, err)

	// Perform a new insert that will have a higher tx id
	samples = dynparquet.Samples{{
		Labels: []dynparquet.Label{
			{Name: "blarg", Value: "blarg"},
			{Name: "blah", Value: "blah"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 1,
		Value:     1,
	}}

	buf, err = samples.ToBuffer(table.Schema())
	require.NoError(t, err)

	tx, err := table.InsertBuffer(ctx, buf)
	require.NoError(t, err)

	table.db.Wait(tx)

	// Now we cheat and reset our tx and watermark
	table.db.tx.Store(1)
	table.db.highWatermark.Store(1)

	pool := memory.NewGoAllocator()

	err = table.View(func(tx uint64) error {
		as, err := table.ArrowSchema(ctx, tx, pool, nil, nil, nil)
		require.NoError(t, err)

		rows := int64(0)
		err = table.Iterator(ctx, tx, pool, as, nil, nil, nil, func(ar arrow.Record) error {
			rows += ar.NumRows()
			defer ar.Release()

			return nil
		})
		require.NoError(t, err)
		require.Equal(t, int64(3), rows)
		return nil
	})
	require.NoError(t, err)

	// Now set the tx back to what it was, and perform the same read, we should return all 4 rows
	table.db.tx.Store(2)
	table.db.highWatermark.Store(2)

	err = table.View(func(tx uint64) error {
		as, err := table.ArrowSchema(ctx, tx, pool, nil, nil, nil)
		require.NoError(t, err)

		rows := int64(0)
		err = table.Iterator(ctx, table.db.highWatermark.Load(), pool, as, nil, nil, nil, func(ar arrow.Record) error {
			rows += ar.NumRows()
			defer ar.Release()

			return nil
		})
		require.NoError(t, err)
		require.Equal(t, int64(4), rows)
		return nil
	})
	require.NoError(t, err)
}

//func Test_Table_Sorting(t *testing.T) {
//	granuleSize := 2 << 12
//	schema1 := NewSchema(
//		[]ColumnDefinition{{
//			Name:     "labels",
//			Type:     StringType,
//			Encoding: PlainEncoding,
//			Dynamic:  true,
//		}, {
//			Name:     "stacktrace",
//			Type:     List(UUIDType),
//			Encoding: PlainEncoding,
//		}, {
//			Name:     "timestamp",
//			Type:     Int64Type,
//			Encoding: PlainEncoding,
//		}, {
//			Name:     "value",
//			Type:     Int64Type,
//			Encoding: PlainEncoding,
//		}},
//		granuleSize,
//		"labels",
//		"timestamp",
//	)
//
//	schema2 := NewSchema(
//		[]ColumnDefinition{{
//			Name:     "labels",
//			Type:     StringType,
//			Encoding: PlainEncoding,
//			Dynamic:  true,
//		}, {
//			Name:     "stacktrace",
//			Type:     List(UUIDType),
//			Encoding: PlainEncoding,
//		}, {
//			Name:     "timestamp",
//			Type:     Int64Type,
//			Encoding: PlainEncoding,
//		}, {
//			Name:     "value",
//			Type:     Int64Type,
//			Encoding: PlainEncoding,
//		}},
//		granuleSize,
//		"timestamp",
//		"labels",
//	)
//
//	c := New(nil)
//	db := c.DB("test")
//	table1 := db.Table("test1", schema1, log.NewNopLogger())
//	table2 := db.Table("test2", schema2, log.NewNopLogger())
//
//	tables := []*Table{
//		table1,
//		table2,
//	}
//
//	for _, table := range tables {
//
//		err := table.Insert(
//			[]Row{{
//				Values: []interface{}{
//					[]DynamicColumnValue{
//						{Name: "label1", Value: "value1"},
//						{Name: "label2", Value: "value2"},
//					},
//					[]UUID{
//						{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
//						{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
//					},
//					int64(3),
//					int64(3),
//				},
//			}},
//		)
//		require.NoError(t, err)
//
//		err = table.Insert(
//			[]Row{{
//				Values: []interface{}{
//					[]DynamicColumnValue{
//						{Name: "label1", Value: "value1"},
//						{Name: "label2", Value: "value2"},
//						{Name: "label3", Value: "value3"},
//					},
//					[]UUID{
//						{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
//						{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
//					},
//					int64(2),
//					int64(2),
//				},
//			}},
//		)
//		require.NoError(t, err)
//
//		err = table.Insert(
//			[]Row{{
//				Values: []interface{}{
//					[]DynamicColumnValue{
//						{Name: "label1", Value: "value1"},
//						{Name: "label2", Value: "value2"},
//						{Name: "label4", Value: "value4"},
//					},
//					[]UUID{
//						{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
//						{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
//					},
//					int64(1),
//					int64(1),
//				},
//			}},
//		)
//		require.NoError(t, err)
//	}
//
//	for i, table := range tables {
//		err := table.Iterator(memory.NewGoAllocator(), func(ar arrow.Record) error {
//			switch i {
//			case 0:
//				require.Equal(t, "[3 2 1]", fmt.Sprintf("%v", ar.Column(6)))
//			case 1:
//				require.Equal(t, "[1 2 3]", fmt.Sprintf("%v", ar.Column(6)))
//			}
//			defer ar.Release()
//
//			return nil
//		})
//		require.NoError(t, err)
//	}
//}

//func Test_Granule_Less(t *testing.T) {
//	config := NewTableConfig(
//		dynparquet.NewSampleSchema(),
//		2<<13,
//	)
//
//	g := &Granule{
//		tableConfig: config,
//		least: &Row{
//			Values: []interface{}{
//				[]DynamicColumnValue{
//					{Name: "label1", Value: "06e32507-cda3-49db-8093-53a8a4c8da76"},
//					{Name: "label2", Value: "value2"},
//				},
//				int64(6375179957311426905),
//				int64(0),
//			},
//		},
//	}
//	g1 := &Granule{
//		tableConfig: config,
//		least: &Row{
//			Values: []interface{}{
//				[]DynamicColumnValue{
//					{Name: "label1", Value: "06e32507-cda3-49db-8093-53a8a4c8da76"},
//					{Name: "label2", Value: "value2"},
//				},
//				int64(8825936717838690748),
//				int64(0),
//			},
//		},
//	}
//
//	require.NotEqual(t, g.Less(g1), g1.Less(g))
//}

func Test_Table_NewTableValidIndexDegree(t *testing.T) {
	config := NewTableConfig(dynparquet.NewSampleSchema())
	c := New(nil, 512, 512*1024*1024).WithIndexDegree(-1)
	db, err := c.DB("test")
	require.NoError(t, err)

	_, err = db.Table("test", config, newTestLogger(t))
	require.Error(t, err)
	require.Equal(t, err.Error(), "failed to create table: Table's columnStore index degree must be a positive integer (received -1)")
}

func Test_Table_NewTableValidSplitSize(t *testing.T) {
	config := NewTableConfig(
		dynparquet.NewSampleSchema(),
	)

	c := New(nil, 512, 512*1024*1024).WithSplitSize(1)
	db, err := c.DB("test")
	require.NoError(t, err)
	_, err = db.Table("test", config, newTestLogger(t))
	require.Error(t, err)
	require.Equal(t, err.Error(), "failed to create table: Table's columnStore splitSize must be a positive integer > 1 (received 1)")

	c = New(nil, 512, 512*1024*1024).WithSplitSize(-1)
	db, err = c.DB("test")
	require.NoError(t, err)
	_, err = db.Table("test", NewTableConfig(dynparquet.NewSampleSchema()), newTestLogger(t))
	require.Error(t, err)
	require.Equal(t, err.Error(), "failed to create table: Table's columnStore splitSize must be a positive integer > 1 (received -1)")

	c = New(nil, 512, 512*1024*1024).WithSplitSize(2)
	db, err = c.DB("test")
	_, err = db.Table("test", NewTableConfig(dynparquet.NewSampleSchema()), newTestLogger(t))
	require.NoError(t, err)
}

func Test_Table_Filter(t *testing.T) {
	table := basicTable(t, 2^12)

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

	buf, err := samples.ToBuffer(table.Schema())
	require.NoError(t, err)

	ctx := context.Background()

	_, err = table.InsertBuffer(ctx, buf)
	require.NoError(t, err)

	samples = dynparquet.Samples{{
		ExampleType: "test",
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value1"},
			{Name: "label2", Value: "value2"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 2,
		Value:     2,
	}}

	buf, err = samples.ToBuffer(table.Schema())
	require.NoError(t, err)

	_, err = table.InsertBuffer(ctx, buf)
	require.NoError(t, err)

	samples = dynparquet.Samples{{
		ExampleType: "test",
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value1"},
			{Name: "label2", Value: "value2"},
			{Name: "label3", Value: "value3"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
		Timestamp: 3,
		Value:     3,
	}}

	buf, err = samples.ToBuffer(table.Schema())
	require.NoError(t, err)

	_, err = table.InsertBuffer(ctx, buf)
	require.NoError(t, err)

	filterExpr := logicalplan.And( // Filter that excludes the granule
		logicalplan.Col("timestamp").GT(logicalplan.Literal(-10)),
		logicalplan.Col("timestamp").LT(logicalplan.Literal(1)),
	)

	pool := memory.NewGoAllocator()

	err = table.View(func(tx uint64) error {
		iterated := false

		as, err := table.ArrowSchema(ctx, tx, pool, nil, nil, nil)
		if err != nil {
			return err
		}

		err = table.Iterator(ctx, tx, pool, as, nil, filterExpr, nil, func(ar arrow.Record) error {
			defer ar.Release()

			iterated = true

			return nil
		})
		require.NoError(t, err)
		require.False(t, iterated)
		return nil
	})
	require.NoError(t, err)
}

func Test_Table_InsertCancellation(t *testing.T) {
	tests := map[string]struct {
		granuleSize int
	}{
		"1024": {1024},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			table := basicTable(t, test.granuleSize)
			ctx := context.Background()
			ctx, cancel := context.WithTimeout(ctx, 5*time.Millisecond)
			defer cancel()

			generateRows := func(n int) *dynparquet.Buffer {
				rows := make(dynparquet.Samples, 0, n)
				for i := 0; i < n; i++ {
					rows = append(rows, dynparquet.Sample{
						Labels: []dynparquet.Label{ // TODO would be nice to not have all the same column
							{Name: "label1", Value: "value1"},
							{Name: "label2", Value: "value2"},
						},
						Stacktrace: []uuid.UUID{
							{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
							{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
						},
						Timestamp: rand.Int63(),
						Value:     rand.Int63(),
					})
				}
				buf, err := rows.ToBuffer(table.Schema())
				require.NoError(t, err)

				buf.Sort()

				// This is necessary because sorting a buffer makes concurrent reading not
				// safe as the internal pages are cyclically sorted at read time. Cloning
				// executes the cyclic sort once and makes the resulting buffer safe for
				// concurrent reading as it no longer has to perform the cyclic sorting at
				// read time. This should probably be improved in the parquet library.
				buf, err = buf.Clone()
				require.NoError(t, err)

				return buf
			}

			// Spawn n workers that will insert values into the table
			n := 4
			inserts := 10
			rows := 10
			wg := &sync.WaitGroup{}
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < inserts; i++ {
						if _, err := table.InsertBuffer(ctx, generateRows(rows)); err != nil {
							if !errors.Is(err, context.DeadlineExceeded) {
								fmt.Println("Received error on insert: ", err)
							}
						}
					}
				}()
			}

			wg.Wait()
			table.Sync()

			// Wait for the transaction pool to drain before iterating
			pending := 0
			table.db.txPool.Iterate(func(tx uint64) bool {
				pending++
				return false
			})
			for pending > 1 {
				time.Sleep(30 * time.Millisecond) // sleep wait for pool to drain
				pending = 0
				table.db.txPool.Iterate(func(tx uint64) bool {
					pending++
					return false
				})
			}

			pool := memory.NewGoAllocator()

			err := table.View(func(tx uint64) error {
				totalrows := int64(0)

				as, err := table.ArrowSchema(ctx, tx, pool, nil, nil, nil)
				if err != nil {
					return err
				}

				err = table.Iterator(ctx, tx, pool, as, nil, nil, nil, func(ar arrow.Record) error {
					totalrows += ar.NumRows()
					defer ar.Release()

					return nil
				})
				require.NoError(t, err)
				require.Less(t, totalrows, int64(n*inserts*rows)) // We expect to cancel some subset of our writes
				return nil
			})
			require.NoError(t, err)
		})
	}
}

func Test_Table_CancelBasic(t *testing.T) {
	table := basicTable(t, 8192)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

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

	buf, err := samples.ToBuffer(table.Schema())
	require.NoError(t, err)

	_, err = table.InsertBuffer(ctx, buf)
	require.True(t, errors.Is(err, context.Canceled))

	pool := memory.NewGoAllocator()

	err = table.View(func(tx uint64) error {
		totalrows := int64(0)

		as, err := table.ArrowSchema(ctx, tx, pool, nil, nil, nil)
		if err != nil {
			return err
		}

		err = table.Iterator(ctx, tx, pool, as, nil, nil, nil, func(ar arrow.Record) error {
			totalrows += ar.NumRows()
			defer ar.Release()

			return nil
		})
		require.NoError(t, err)
		require.Equal(t, int64(0), totalrows)
		return nil
	})
	require.NoError(t, err)
}

func Test_Table_ArrowSchema(t *testing.T) {
	table := basicTable(t, 8192)

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
	}}

	buf, err := samples.ToBuffer(table.Schema())
	require.NoError(t, err)

	ctx := context.Background()

	_, err = table.InsertBuffer(ctx, buf)
	require.NoError(t, err)

	samples = dynparquet.Samples{{
		ExampleType: "test",
		Labels: []dynparquet.Label{
			{Name: "label1", Value: "value1"},
			{Name: "label2", Value: "value2"},
			{Name: "label3", Value: "value3"},
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3},
		},
		Timestamp: 1,
		Value:     2,
	}}

	buf, err = samples.ToBuffer(table.Schema())
	require.NoError(t, err)

	_, err = table.InsertBuffer(ctx, buf)
	require.NoError(t, err)

	pool := memory.NewGoAllocator()

	// Read the schema from a previous transaction.

	schema, err := table.ArrowSchema(ctx, 1, pool, nil, nil, nil)
	require.NoError(t, err)

	require.Len(t, schema.Fields(), 6)
	require.Equal(t,
		arrow.Field{Name: "example_type", Type: &arrow.BinaryType{}, Nullable: false, Metadata: arrow.Metadata{}},
		schema.Field(0),
	)
	require.Equal(t,
		arrow.Field{Name: "labels.label1", Type: &arrow.BinaryType{}, Nullable: true, Metadata: arrow.Metadata{}},
		schema.Field(1),
	)
	require.Equal(t,
		arrow.Field{Name: "labels.label2", Type: &arrow.BinaryType{}, Nullable: true, Metadata: arrow.Metadata{}},
		schema.Field(2),
	)
	require.Equal(t,
		arrow.Field{Name: "stacktrace", Type: &arrow.BinaryType{}, Nullable: false, Metadata: arrow.Metadata{}},
		schema.Field(3),
	)
	require.Equal(t,
		arrow.Field{Name: "timestamp", Type: &arrow.Int64Type{}, Nullable: false, Metadata: arrow.Metadata{}},
		schema.Field(4),
	)
	require.Equal(t,
		arrow.Field{Name: "value", Type: &arrow.Int64Type{}, Nullable: false, Metadata: arrow.Metadata{}},
		schema.Field(5),
	)

	// Read two schemas for two different queries within the same transaction.

	err = table.View(func(tx uint64) error {
		schema, err := table.ArrowSchema(
			ctx,
			tx,
			pool,
			nil, nil, nil,
		)
		require.NoError(t, err)

		require.Len(t, schema.Fields(), 7)
		require.Equal(t,
			arrow.Field{Name: "example_type", Type: &arrow.BinaryType{}, Nullable: false, Metadata: arrow.Metadata{}},
			schema.Field(0),
		)
		require.Equal(t,
			arrow.Field{Name: "labels.label1", Type: &arrow.BinaryType{}, Nullable: true, Metadata: arrow.Metadata{}},
			schema.Field(1),
		)
		require.Equal(t,
			arrow.Field{Name: "labels.label2", Type: &arrow.BinaryType{}, Nullable: true, Metadata: arrow.Metadata{}},
			schema.Field(2),
		)
		require.Equal(t,
			arrow.Field{Name: "labels.label3", Type: &arrow.BinaryType{}, Nullable: true, Metadata: arrow.Metadata{}},
			schema.Field(3),
		)
		require.Equal(t,
			arrow.Field{Name: "stacktrace", Type: &arrow.BinaryType{}, Nullable: false, Metadata: arrow.Metadata{}},
			schema.Field(4),
		)
		require.Equal(t,
			arrow.Field{Name: "timestamp", Type: &arrow.Int64Type{}, Nullable: false, Metadata: arrow.Metadata{}},
			schema.Field(5),
		)
		require.Equal(t,
			arrow.Field{Name: "value", Type: &arrow.Int64Type{}, Nullable: false, Metadata: arrow.Metadata{}},
			schema.Field(6),
		)

		schema, err = table.ArrowSchema(
			ctx,
			tx,
			pool,
			[]logicalplan.ColumnMatcher{
				logicalplan.DynamicColumnMatcher{ColumnName: "labels"},
				logicalplan.StaticColumnMatcher{ColumnName: "value"},
			},
			nil, nil,
		)
		require.NoError(t, err)

		require.Len(t, schema.Fields(), 4)
		require.Equal(t,
			arrow.Field{Name: "labels.label1", Type: &arrow.BinaryType{}, Nullable: true, Metadata: arrow.Metadata{}},
			schema.Field(0),
		)
		require.Equal(t,
			arrow.Field{Name: "labels.label2", Type: &arrow.BinaryType{}, Nullable: true, Metadata: arrow.Metadata{}},
			schema.Field(1),
		)
		require.Equal(t,
			arrow.Field{Name: "labels.label3", Type: &arrow.BinaryType{}, Nullable: true, Metadata: arrow.Metadata{}},
			schema.Field(2),
		)
		require.Equal(t,
			arrow.Field{Name: "value", Type: &arrow.Int64Type{}, Nullable: false, Metadata: arrow.Metadata{}},
			schema.Field(3),
		)

		return nil
	})
	require.NoError(t, err)
}

func Test_DoubleTable(t *testing.T) {
	schema := dynparquet.NewSchema(
		"test",
		[]dynparquet.ColumnDefinition{{
			Name:          "id",
			StorageLayout: parquet.String(),
			Dynamic:       false,
		}, {
			Name:          "value",
			StorageLayout: parquet.Leaf(parquet.DoubleType),
			Dynamic:       false,
		}},
		[]dynparquet.SortingColumn{
			dynparquet.Ascending("id"),
		},
	)
	config := NewTableConfig(schema)

	bucket, err := filesystem.NewBucket(".")
	require.NoError(t, err)

	c := New(
		nil,
		4096,
		512*1024*1024,
	).WithStorageBucket(bucket)

	db, err := c.DB("test")
	require.NoError(t, err)
	table, err := db.Table("test", config, newTestLogger(t))
	require.NoError(t, err)

	b, err := schema.NewBuffer(nil)
	require.NoError(t, err)

	value := rand.Float64()

	_, err = b.WriteRows([]parquet.Row{{
		parquet.ValueOf("a").Level(0, 0, 0),
		parquet.ValueOf(value).Level(0, 0, 1),
	}})
	require.NoError(t, err)

	ctx := context.Background()

	n, err := table.InsertBuffer(ctx, b)
	require.NoError(t, err)
	require.Equal(t, uint64(1), n)

	err = table.View(func(tx uint64) error {
		pool := memory.NewGoAllocator()

		as, err := table.ArrowSchema(ctx, tx, pool, nil, nil, nil)
		if err != nil {
			return err
		}

		return table.Iterator(ctx, tx, pool, as, nil, nil, nil, func(ar arrow.Record) error {
			defer ar.Release()
			require.Equal(t, value, ar.Column(1).(*array.Float64).Value(0))
			return nil
		})
	})
	require.NoError(t, err)
}
