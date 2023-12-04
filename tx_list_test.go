package frostdb

import (
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

type Uint64Slice []uint64

func (x Uint64Slice) Len() int           { return len(x) }
func (x Uint64Slice) Less(i, j int) bool { return x[i] < x[j] }
func (x Uint64Slice) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

func Test_TXList_Mark(t *testing.T) {
	node := &TxNode{
		next:     &atomic.Pointer[TxNode]{},
		original: &atomic.Pointer[TxNode]{},
	}
	next := &TxNode{
		next:     &atomic.Pointer[TxNode]{},
		original: &atomic.Pointer[TxNode]{},
	}
	node.next.Store(next)

	node.next.Store(getMarked(node))
	node.original.Store(next)
	require.NotNil(t, isMarked(node))
	node.next.Store(getUnmarked(node))
	require.Nil(t, isMarked(node))
}

func Test_TXList_Basic(t *testing.T) {
	wm := atomic.Uint64{}
	wm.Store(1) // set the watermark so that the sweeper won't remove any of our txs
	p := NewTxPool(&wm)
	txs := []uint64{9, 8, 7, 6, 4, 5, 3, 10}
	for _, tx := range txs {
		p.Insert(tx)
	}

	found := make(Uint64Slice, 0, len(txs))
	p.Iterate(func(tx uint64) bool {
		found = append(found, tx)
		return true
	})

	p.Stop() // stop the sweeper
	require.True(t, sort.IsSorted(found))
	require.Equal(t, 8, len(found))
}

func Test_TXList_Async(t *testing.T) {
	wm := atomic.Uint64{}
	p := NewTxPool(&wm)

	tx := &atomic.Uint64{}
	tx.Add(1) // adjust the tx id to ensure the sweeper doesn't drain the pool

	// Swap writers that will each complete n transactions
	n := 10
	writers := 100
	wg := &sync.WaitGroup{}
	wg.Add(writers)
	for i := 0; i < writers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < n; j++ {
				p.Insert(tx.Add(1))
			}
		}()
	}

	wg.Wait()

	found := make(Uint64Slice, 0, writers*n)
	p.Iterate(func(tx uint64) bool {
		found = append(found, tx)
		return true
	})
	require.True(t, sort.IsSorted(found))
	require.Equal(t, n*writers, len(found))

	p.Insert(1) // insert the missing tx to drain the pool

	for v := wm.Load(); v < tx.Load(); v = wm.Load() {
		// Satisfy linter with statement.
		continue
	}
	require.Equal(t, tx.Load(), wm.Load())

	// Verify the pool is empty
	foundtx := false
	p.Iterate(func(tx uint64) bool {
		foundtx = true
		return true
	})
	require.False(t, foundtx)

	p.Stop() // stop the sweeper
}

func Benchmark_TXList_InsertAndDrain(b *testing.B) {
	benchmarks := map[string]struct {
		writers int
		values  int
	}{
		"10:100": {
			writers: 10,
			values:  100,
		},
		"10:1000": {
			writers: 10,
			values:  1000,
		},
		"100:100": {
			writers: 100,
			values:  100,
		},
		"1000:100": {
			writers: 1000,
			values:  100,
		},
	}

	for name, benchmark := range benchmarks {
		b.Run(name, func(b *testing.B) {
			wm := atomic.Uint64{}
			p := NewTxPool(&wm)
			tx := &atomic.Uint64{}
			wg := &sync.WaitGroup{}
			for i := 0; i < b.N; i++ {
				wg.Add(benchmark.writers)
				for i := 0; i < benchmark.writers; i++ {
					go func() {
						defer wg.Done()
						for j := 0; j < benchmark.values; j++ {
							p.Insert(tx.Add(1))
						}
					}()
				}

				wg.Wait()

				// Wait for the sweeper to drain
				for v := wm.Load(); v < tx.Load(); v = wm.Load() {
					// Satisfy linter with statement.
					continue
				}
				require.Equal(b, tx.Load(), wm.Load())
			}
			p.Stop()
		})
	}
}
