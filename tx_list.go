package frostdb

import (
	"sync/atomic"
	"time"
)

type TxNode struct {
	next atomic.Pointer[TxNode]
	tx   uint64
}

type TxPool struct {
	next  atomic.Pointer[TxNode]
	drain chan interface{}
}

// NewTxPool returns a new TxPool and starts the pool cleaner routine.
func NewTxPool(watermark *atomic.Uint64) *TxPool {
	txpool := &TxPool{
		next:  atomic.Pointer[TxNode]{},
		drain: make(chan interface{}, 1),
	}
	go txpool.cleaner(watermark)
	return txpool
}

// Prepend a node onto the front of the list.
func (l *TxPool) Prepend(tx uint64) *TxNode {
	node := &TxNode{
		tx: tx,
	}
	for { // continue until a successful compare and swap occurs.
		next := l.next.Load()
		node.next.Store(next)
		if l.next.CompareAndSwap(next, node) {
			select {
			case l.drain <- true:
				return node
			default:
				return node
			}
		}
	}
}

// Iterate accesses every node in the list.
func (l *TxPool) Iterate(iterate func(tx uint64) bool) {
	next := l.next.Load()
	prev := atomic.Pointer[TxNode]{}
	for {
		node := (*TxNode)(next)
		if node == nil {
			return
		}
		if iterate(node.tx) {
			if prev.Load() == nil { // we're removing the first node
				l.next.CompareAndSwap(nil, node.next.Load())
			} else {
				// set the previous nodes next to this nodes nex
				prevnode := prev.Load()
				prevnode.next.CompareAndSwap(prevnode.next.Load(), node.next.Load())
			}
		}
		prev.Store(next)
		next = node.next.Load()
	}
}

// cleaner sweeps the pool periodically, and bubbles up the given watermark.
// this function does not return.
func (l *TxPool) cleaner(watermark *atomic.Uint64) {
	ticker := time.NewTicker(time.Millisecond * 10)
	defer ticker.Stop()

	for {
		select { // sweep whenever notified or when ticker
		case _, ok := <-l.drain:
			if !ok {
				// Channel closed.
				return
			}
			l.sweep(watermark)
		case <-ticker.C:
			l.sweep(watermark)
		}
	}
}

func (l *TxPool) sweep(watermark *atomic.Uint64) {
	l.Iterate(func(tx uint64) bool {
		mark := watermark.Load()
		switch {
		case mark+1 == tx:
			watermark.Add(1)
			return true // return true to indicate that this node should be removed from the tx list.
		case mark >= tx:
			return true
		default:
			return false
		}
	})
}

// Stop stops the TxPool's cleaner goroutine.
func (l *TxPool) Stop() {
	close(l.drain)
}
