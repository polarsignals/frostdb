package arcticdb

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/google/btree"
	"github.com/google/uuid"
	"github.com/segmentio/parquet-go"
	"github.com/stretchr/testify/require"

	"github.com/polarsignals/arcticdb/dynparquet"
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
		granuleSize,
		512*1024*1024,
	)

	c := New(nil)
	db := c.DB("test")
	table := db.Table("test", config, newTestLogger(t))

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

	err = table.Insert(buf)
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

	err = table.Insert(buf)
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

	err = table.Insert(buf)
	require.NoError(t, err)

	err = table.Iterator(memory.NewGoAllocator(), nil, nil, nil, func(ar arrow.Record) error {
		t.Log(ar)
		defer ar.Release()

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	uuid1 := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	uuid2 := uuid.MustParse("00000000-0000-0000-0000-000000000002")

	// One granule with 3 parts
	require.Equal(t, 1, (*btree.BTree)(table.active.index.Load()).Len())
	require.Equal(t, uint64(3), (*btree.BTree)(table.active.index.Load()).Min().(*Granule).parts.total.Load())
	require.Equal(t, uint64(5), (*btree.BTree)(table.active.index.Load()).Min().(*Granule).card.Load())
	require.Equal(t, parquet.Row{
		parquet.ValueOf("test").Level(0, 0, 0),
		parquet.ValueOf("value1").Level(0, 1, 1),
		parquet.ValueOf("value2").Level(0, 1, 2),
		parquet.ValueOf(nil).Level(0, 0, 3),
		parquet.ValueOf(nil).Level(0, 0, 4),
		parquet.ValueOf(append(uuid1[:], uuid2[:]...)).Level(0, 0, 5),
		parquet.ValueOf(1).Level(0, 0, 6),
		parquet.ValueOf(1).Level(0, 0, 7),
	}, (*dynparquet.DynamicRow)((*btree.BTree)(table.active.index.Load()).Min().(*Granule).least.Load()).Row)
	require.Equal(t, 1, (*btree.BTree)(table.active.index.Load()).Len())
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

	err = table.Insert(buf)
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

	err = table.Insert(buf)
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

	err = table.Insert(buf)
	require.NoError(t, err)

	// Wait for the index to be updated by the asynchronous granule split.
	table.Sync()

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
	table.Iterator(memory.NewGoAllocator(), nil, nil, nil, func(r arrow.Record) error {
		defer r.Release()
		t.Log(r)
		return nil
	})

	require.Equal(t, 2, (*btree.BTree)(table.active.index.Load()).Len())
	require.Equal(t, uint64(2), (*btree.BTree)(table.active.index.Load()).Min().(*Granule).card.Load())
	require.Equal(t, uint64(3), (*btree.BTree)(table.active.index.Load()).Max().(*Granule).card.Load())
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

	// Since we are inserting 4 elements and the granule size is 4, the granule
	// will immediately split.
	err = table.Insert(buf)
	require.NoError(t, err)

	// Since a compaction happens async, it may abort if it runs before the transactions are completed. In that case; we'll manually compact the granule
	table.Sync()
	if table.index.Len() == 1 {
		table.Add(1)
		table.compact(table.index.Min().(*Granule))
		table.Sync()
	}

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

	err = table.Insert(buf)
	require.NoError(t, err)

	// Wait for the index to be updated by the asynchronous granule split.
	table.Sync()

	require.Equal(t, 2, (*btree.BTree)(table.active.index.Load()).Len())
	require.Equal(t, uint64(3), (*btree.BTree)(table.active.index.Load()).Min().(*Granule).card.Load()) // [10,11]
	require.Equal(t, uint64(2), (*btree.BTree)(table.active.index.Load()).Max().(*Granule).card.Load()) // [12,13,14]

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

	err = table.Insert(buf)
	require.NoError(t, err)

	table.Iterator(memory.NewGoAllocator(), nil, nil, nil, func(r arrow.Record) error {
		defer r.Release()
		t.Log(r)
		return nil
	})

	require.Equal(t, 2, (*btree.BTree)(table.active.index.Load()).Len())
	require.Equal(t, uint64(3), (*btree.BTree)(table.active.index.Load()).Min().(*Granule).card.Load()) // [1,10,11]
	require.Equal(t, uint64(3), (*btree.BTree)(table.active.index.Load()).Max().(*Granule).card.Load()) // [12,13,14]
}

// This test issues concurrent writes to the database, and expects all of them to be recorded successfully.
func Test_Table_Concurrency(t *testing.T) {
	table := basicTable(t, 1<<13)

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
	n := 8
	inserts := 100
	rows := 10
	wg := &sync.WaitGroup{}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < inserts; i++ {
				if err := table.Insert(generateRows(rows)); err != nil {
					fmt.Println("Received error on insert: ", err)
				}
			}
		}()
	}

	// TODO probably have them generate until a stop event
	wg.Wait()

	totalrows := int64(0)
	err := table.Iterator(memory.NewGoAllocator(), nil, nil, nil, func(ar arrow.Record) error {
		totalrows += ar.NumRows()
		defer ar.Release()

		return nil
	})
	require.NoError(t, err)
	require.Equal(t, int64(n*inserts*rows), totalrows)
}

//func Benchmark_Table_Insert_10Rows_10Iter_10Writers(b *testing.B) {
//	benchmarkTableInserts(b, 10, 10, 10)
//}
//
//func Benchmark_Table_Insert_100Row_100Iter_100Writers(b *testing.B) {
//	benchmarkTableInserts(b, 100, 100, 100)
//}
//
//func benchmarkTableInserts(b *testing.B, rows, iterations, writers int) {
//	config := NewTableConfig(
//		dynparquet.NewSampleSchema(),
//		2<<13,
//	)
//
//	c := New(nil)
//	db := c.DB("test")
//	generateRows := func(id string, n int) *dynparquet.Buffer {
//		rows := make(dynparquet.Samples, 0, n)
//		for i := 0; i < n; i++ {
//			rows = append(rows, dynparquet.Sample{
//				Labels: []dynparquet.Label{ // TODO would be nice to not have all the same column
//					{Name: "label1", Value: id},
//					{Name: "label2", Value: "value2"},
//				},
//				Stacktrace: []uuid.UUID{
//					{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
//					{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
//				},
//				Timestamp: rand.Int63(),
//				Value:     int64(i),
//			})
//		}
//
//		buf, err := rows.ToBuffer(config.schema)
//		require.NoError(b, err)
//
//		buf.Sort()
//		return buf
//	}
//
//	// Pre-generate all rows we're inserting
//	inserts := make(map[string]*dynparquet.Buffer, writers)
//	for i := 0; i < writers; i++ {
//		id := uuid.New().String()
//		inserts[id] = generateRows(id, rows*iterations)
//	}
//
//	b.ResetTimer()
//
//	for i := 0; i < b.N; i++ {
//
//		// Create table for test
//		table := db.Table(uuid.New().String(), config, log.NewNopLogger())
//
//		// Spawn n workers that will insert values into the table
//		wg := &sync.WaitGroup{}
//		for id := range inserts {
//			wg.Add(1)
//			go func(id string, tbl *Table, w *sync.WaitGroup) {
//				defer w.Done()
//				for i := 0; i < iterations; i++ {
//					if err := tbl.Insert(inserts[id][i*rows : i*rows+rows]); err != nil {
//						fmt.Println("Received error on insert: ", err)
//					}
//				}
//			}(id, table, wg)
//		}
//		wg.Wait()
//
//		b.StopTimer()
//
//		// Wait for all compaction routines to complete
//		table.Sync()
//
//		// Calculate the number of entries in database
//		totalrows := int64(0)
//		err := table.Iterator(memory.NewGoAllocator(), func(ar arrow.Record) error {
//			totalrows += ar.NumRows()
//			defer ar.Release()
//
//			return nil
//		})
//		require.NoError(b, err)
//		require.Equal(b, int64(rows*iterations*writers), totalrows)
//
//		b.StartTimer()
//	}
//}

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

	err = table.Insert(buf)
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

	err = table.Insert(buf)
	require.NoError(t, err)

	// Now we cheat and reset our tx so that we can perform a read in the past.
	prev := table.db.tx.Load()
	table.db.tx.Store(1)

	rows := int64(0)
	err = table.Iterator(memory.NewGoAllocator(), nil, nil, nil, func(ar arrow.Record) error {
		rows += ar.NumRows()
		defer ar.Release()

		return nil
	})
	require.NoError(t, err)
	require.Equal(t, int64(3), rows)

	// Now set the tx back to what it was, and perform the same read, we should return all 4 rows
	table.db.tx.Store(prev)

	rows = int64(0)
	err = table.Iterator(memory.NewGoAllocator(), nil, nil, nil, func(ar arrow.Record) error {
		rows += ar.NumRows()
		defer ar.Release()

		return nil
	})
	require.NoError(t, err)
	require.Equal(t, int64(4), rows)
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
