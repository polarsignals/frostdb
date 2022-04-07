package arcticdb

import (
	"time"
	"unsafe"

	"go.uber.org/atomic"
)

type TxNode struct {
	next *atomic.UnsafePointer
	tx   uint64
}

type TxPool struct {
	next *atomic.UnsafePointer
}

// NewTxPool returns a new TxPool and starts the pool cleaner routine
func NewTxPool(watermark *atomic.Uint64) *TxPool {
	txpool := &TxPool{
		next: atomic.NewUnsafePointer(unsafe.Pointer(nil)),
	}
	go txpool.cleaner(watermark)
	return txpool
}

// Prepend a node onto the front of the list
func (l *TxPool) Prepend(tx uint64) *TxNode {
	node := &TxNode{
		tx: tx,
	}
	for { // continue until a successful compare and swap occurs
		next := l.next.Load()
		node.next = atomic.NewUnsafePointer(next)
		if l.next.CAS(next, unsafe.Pointer(node)) {
			return node
		}
	}
}

// Iterate accesses every node in the list
func (l *TxPool) Iterate(iterate func(tx uint64) bool) {
	next := l.next.Load()
	for {
		node := (*TxNode)(next)
		if node == nil {
			return
		}
		if iterate(node.tx) {
			// TODO remove this node
		}
		next = node.next.Load()
	}
}

// cleaner sweeps the pool periodically, and bubbles up the given watermark.
// this function does not return
func (l *TxPool) cleaner(watermark *atomic.Uint64) {
	for {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		select {
		case <-ticker.C:
			l.Iterate(func(tx uint64) bool {
				if watermark.Load()+1 == tx {
					watermark.Inc()
					return true // return true to indicate that this node should be removed from the tx list
				}
				return false
			})
		}
	}
}
