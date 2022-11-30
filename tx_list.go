package frostdb

import (
	"sync/atomic"
	"unsafe"
)

type TxNode struct {
	next atomic.Pointer[TxNode]
	tx   uint64
}

type TxPool struct {
	head  atomic.Pointer[TxNode]
	tail  atomic.Pointer[TxNode]
	drain chan interface{}
}

// NewTxPool returns a new TxPool and starts the pool cleaner routine.
// TxPool is a sorted lockless linked-list described in https://timharris.uk/papers/2001-disc.pdf
func NewTxPool(watermark *atomic.Uint64) *TxPool {
	tail := &TxNode{
		next: atomic.Pointer[TxNode]{},
		tx:   0,
	}
	head := &TxNode{
		next: atomic.Pointer[TxNode]{},
		tx:   0,
	}
	txpool := &TxPool{
		head:  atomic.Pointer[TxNode]{},
		tail:  atomic.Pointer[TxNode]{},
		drain: make(chan interface{}, 1),
	}

	// [head] -> [tail]
	head.next.Store(tail)
	txpool.head.Store(head)
	txpool.tail.Store(tail)

	go txpool.cleaner(watermark)
	return txpool
}

// Insert performs an insertion sort of the given tx
func (l *TxPool) Insert(tx uint64) {
	n := &TxNode{
		tx: tx,
	}
	assertAlignment(n)

	tryInsert := func() bool {
		prev := l.head.Load()
		for node := l.head.Load().next.Load(); node != nil; node = getUnmarked(node) {
			if node.tx == 0 { // end of list
				return l.insert(n, prev, node)
			}

			// remove deleted nodes encountered
			if isMarked(node) {
				prev.next.CompareAndSwap(node, getUnmarked(node))
				return false
			}

			if node.tx > tx {
				return l.insert(n, prev, node)
			}
			prev = node
		}
		return false
	}
	for !tryInsert() {
	}
}

// insert will insert the node after previous and before next
func (l *TxPool) insert(node, prev, next *TxNode) bool {
	node.next.Store(next)
	success := prev.next.CompareAndSwap(next, node)
	if success {
		select {
		case l.drain <- struct{}{}: // notify the cleaner
		default:
		}
	}
	return success
}

func (l *TxPool) Iterate(iterate func(tx uint64) bool) {
	for node := l.head.Load().next.Load(); node.tx != 0; node = getUnmarked(node) {
		if !isMarked(node) && !iterate(node.tx) {
			return
		}
	}
}

// delete iterates over the list and deletes until the delete function returns false
func (l *TxPool) delete(deleteFunc func(tx uint64) bool) {
	for node := l.head.Load().next.Load(); node.tx != 0; node = getUnmarked(node) {
		if !deleteFunc(node.tx) {
			return
		}
		for {
			next := node.next.Load()
			if node.next.CompareAndSwap(next, getMarked(node)) {
				break
			}
		}
	}
}

func assertAlignment(node *TxNode) {
	if (uintptr(unsafe.Pointer(node)) & uintptr(1)) == 1 {
		panic("node pointers are required to be aligned")
	}
}

func isMarked(node *TxNode) bool {
	return (uintptr(unsafe.Pointer(node.next.Load())) & uintptr(1)) == 1
}

func getMarked(node *TxNode) *TxNode {
	next := node.next.Load()
	if (uintptr(unsafe.Pointer(next)) & uintptr(1)) == 1 {
		return next
	}
	return (*TxNode)(unsafe.Add(unsafe.Pointer(next), 1))
}

func getUnmarked(node *TxNode) *TxNode {
	next := node.next.Load()
	if (uintptr(unsafe.Pointer(next)) & uintptr(1)) == 0 {
		return next
	}

	return (*TxNode)(unsafe.Add(unsafe.Pointer(next), -1))
}

// cleaner sweeps the pool periodically, and bubbles up the given watermark.
// this function does not return.
func (l *TxPool) cleaner(watermark *atomic.Uint64) {
	for {
		select { // sweep whenever notified or when ticker
		case _, ok := <-l.drain:
			if !ok {
				// Channel closed.
				return
			}
			l.delete(func(tx uint64) bool {
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
	}
}

// Stop stops the TxPool's cleaner goroutine.
func (l *TxPool) Stop() {
	close(l.drain)
}
