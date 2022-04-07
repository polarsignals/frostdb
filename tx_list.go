package arcticdb

import (
	"sync/atomic"
	"time"
	"unsafe"
)

type TxNode struct {
	next unsafe.Pointer
	tx   uint64
}

type TxPool struct {
	next unsafe.Pointer
}

// NewTxPool returns a new TxPool and starts the pool cleaner routine
func NewTxPool(watermark *uint64) *TxPool {
	txpool := &TxPool{}
	go txpool.cleaner(watermark)
	return txpool
}

// Prepend a node onto the front of the list
func (l *TxPool) Prepend(tx uint64) *TxNode {
	node := &TxNode{
		tx: tx,
	}
	for { // continue until a successful compare and swap occurs
		next := atomic.LoadPointer(&l.next)
		node.next = next
		if atomic.CompareAndSwapPointer(&l.next, next, (unsafe.Pointer)(node)) {
			return node
		}
	}
}

// Iterate accesses every node in the list
func (l *TxPool) Iterate(iterate func(tx uint64) bool) {
	next := atomic.LoadPointer(&l.next)
	for {
		node := (*TxNode)(next)
		if node == nil {
			return
		}
		if iterate(node.tx) {
			// TODO remove this node
		}
		next = atomic.LoadPointer(&node.next)
	}
}

// cleaner sweeps the pool periodically, and bubbles up the given watermark.
// this function does not return
func (l *TxPool) cleaner(watermark *uint64) {
	for {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		select {
		case <-ticker.C:
			l.Iterate(func(tx uint64) bool {
				mark := atomic.LoadUint64(watermark)
				if mark+1 == tx {
					atomic.AddUint64(watermark, 1)
					return true // return true to indicate that this node should be removed from the tx list
				}
				return false
			})
		}
	}
}
