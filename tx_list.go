package frostdb

import (
	"context"
	"sync/atomic"
)

type TxNode struct {
	next     *atomic.Pointer[TxNode]
	original *atomic.Pointer[TxNode]
	tx       uint64
}

type TxPool struct {
	head   *atomic.Pointer[TxNode]
	tail   *atomic.Pointer[TxNode]
	cancel context.CancelFunc
	drain  chan interface{}
}

// NewTxPool returns a new TxPool and starts the pool cleaner routine.
// The transaction pool is used to keep track of completed transactions. It does
// this by inserting completed transactions into an ordered linked list.
//
// Ex:
// insert: 12
// [9]->[10]->[13] => [9]->[10]->[12]->[13]
//
// Inserting a new node triggers the pool cleaner routine to run. The pool
// cleaner's job is to increment a high-watermark counter when it encounters
// contiguous transactions in the list, and then remove those elements in the
// pool.
//
// Ex:
// watermark: 7 insert: 8
// [9]->[10]->[13] => [8]->[9]->[10]->[13] (cleaner notified)
//
// [8]->[9]->[10]->[13]
//
//	^ watermark++; delete 8
//
// [9]->[10]->[13]
//
//	^ watermark++; delete 9
//
// [10]->[13]
//
//	^ watermark++; delete 9
//
// [13]
// watermark: 10
//
// TxPool is a sorted lockless linked-list described in
// https://timharris.uk/papers/2001-disc.pdf
func NewTxPool(watermark *atomic.Uint64) *TxPool {
	tail := &TxNode{
		next:     &atomic.Pointer[TxNode]{},
		original: &atomic.Pointer[TxNode]{},
	}
	head := &TxNode{
		next:     &atomic.Pointer[TxNode]{},
		original: &atomic.Pointer[TxNode]{},
	}
	txpool := &TxPool{
		head:  &atomic.Pointer[TxNode]{},
		tail:  &atomic.Pointer[TxNode]{},
		drain: make(chan interface{}, 1),
	}

	// [head] -> [tail]
	head.next.Store(tail)
	txpool.head.Store(head)
	txpool.tail.Store(tail)

	ctx, cancel := context.WithCancel(context.Background())
	txpool.cancel = cancel
	go txpool.cleaner(ctx, watermark)
	return txpool
}

// Insert performs an insertion sort of the given tx.
func (l *TxPool) Insert(tx uint64) {
	n := &TxNode{
		tx:       tx,
		next:     &atomic.Pointer[TxNode]{},
		original: &atomic.Pointer[TxNode]{},
	}

	tryInsert := func() bool {
		prev := l.head.Load()
		for node := l.head.Load().next.Load(); node != nil; node = getUnmarked(node) {
			if node.tx == 0 { // end of list
				return l.insert(n, prev, node)
			}

			// remove deleted nodes encountered
			if next := isMarked(node); next != nil {
				prev.next.CompareAndSwap(node, next)
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
		// Satisfy linter with statement.
		continue
	}
}

// insert will insert the node after previous and before next.
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

// notifyWatermark notifies the TxPool that the watermark has been updated. This
// triggers a sweep of the pool.
func (l *TxPool) notifyWatermark() {
	select {
	case l.drain <- struct{}{}:
	default:
	}
}

func (l *TxPool) Iterate(iterate func(txn uint64) bool) {
	for node := l.head.Load().next.Load(); node.tx != 0; node = getUnmarked(node) {
		if isMarked(node) == nil && !iterate(node.tx) {
			return
		}
	}
}

// delete iterates over the list and deletes until the delete function returns false.
func (l *TxPool) delete(deleteFunc func(txn uint64) bool) {
	for node := l.head.Load().next.Load(); node.tx != 0; node = getUnmarked(node) {
		if !deleteFunc(node.tx) {
			return
		}
		for next := node.next.Load(); next != nil; next = node.next.Load() { // only attempt to mark nodes as deleted that haven't already been marked
			if node.next.CompareAndSwap(next, getMarked(node)) {
				node.original.Store(next) // NOTE: deletes are not concurrent; so we don't need to CAS the original pointer
				break
			}
		}
	}
}

// isMarked returns the next node if and only if the node is marked for deletion.
func isMarked(node *TxNode) *TxNode {
	next := node.next.Load()
	if next != nil {
		return nil
	}

	// this node has been marked for deletion, get the original next pointer
	og := node.original.Load()
	for og == nil {
		og = node.original.Load()
	}
	return og
}

func getMarked(_ *TxNode) *TxNode {
	// using nil as the marker
	return nil
}

func getUnmarked(node *TxNode) *TxNode {
	next := node.next.Load()
	if next != nil {
		return next
	}

	// get the original pointer
	og := node.original.Load()
	for og == nil {
		og = node.original.Load()
	}
	return og
}

// cleaner sweeps the pool periodically, and bubbles up the given watermark.
// this function does not return.
func (l *TxPool) cleaner(ctx context.Context, watermark *atomic.Uint64) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-l.drain:
			l.delete(func(txn uint64) bool {
				mark := watermark.Load()
				switch {
				case mark+1 == txn:
					watermark.Store(txn)
					return true // return true to indicate that this node should be removed from the tx list.
				case mark >= txn:
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
	l.cancel()
}
