package index

import (
	"fmt"
	"sync/atomic"

	"github.com/polarsignals/frostdb/parts"
)

type SentinelType int

const (
	L0 SentinelType = iota
	L1
	L2
)

func (s SentinelType) String() string {
	return fmt.Sprintf("L%v", int(s))
}

// Node is a Part that is a part of a linked-list.
type Node struct {
	next atomic.Pointer[Node]
	part parts.Part

	sentinel SentinelType // sentinel nodes contain no parts, and are to indicate the start of a new sub list
}

func (n *Node) Part() parts.Part {
	return n.part
}

func (n *Node) String() string {
	if n.part == nil {
		if n.next.Load() == nil {
			return fmt.Sprintf("[%v]", n.sentinel)
		}
		return fmt.Sprintf("[%v]->%v", n.sentinel, n.next.Load().String())
	}

	if n.part.Record() != nil {
		if n.next.Load() == nil {
			return fmt.Sprintf("[%v]", n.part.Record().NumRows())
		}
		return fmt.Sprintf("[%v]->%v", n.part.Record().NumRows(), n.next.Load().String())
	}

	b, _ := n.part.AsSerializedBuffer(nil)
	if n.next.Load() == nil {
		return fmt.Sprintf("[%v]", b.NumRows())
	}
	return fmt.Sprintf("[%v]->%v", b.NumRows(), n.next.Load().String())
}

// NewList creates a new part list using atomic constructs.
func NewList(sentinel SentinelType) *Node {
	p := &Node{
		sentinel: sentinel,
	}
	return p
}

// Sentinel adds a new sentinel node to the list, and returns the sub list starting from that sentinel.
func (n *Node) Sentinel(s SentinelType) *Node {
	return n.prepend(&Node{
		sentinel: s,
	})
}

// Prepend a node onto the front of the list.
func (n *Node) Prepend(part parts.Part) *Node {
	return n.prepend(&Node{
		part: part,
	})
}

// Insert a Node into the list, in order by Tx.
func (n *Node) Insert(part parts.Part) {
	node := &Node{
		part: part,
	}
	tx := node.part.TX()
	tryInsert := func() bool {
		prev := n
		next := n.next.Load()
		for {
			if next == nil {
				return prev.next.CompareAndSwap(next, node)
			}
			if next.part == nil || next.part.TX() < tx {
				node.next.Store(next)
				return prev.next.CompareAndSwap(next, node)
			}
			prev = next
			next = next.next.Load()
		}
	}
	for !tryInsert() {
		continue // make the linter happy
	}
}

func (n *Node) prepend(node *Node) *Node {
	for { // continue until a successful compare and swap occurs
		next := n.next.Load()
		node.next.Store(next)
		if n.next.CompareAndSwap(next, node) {
			return node
		}
	}
}

// Iterate accesses every node in the list.
func (n *Node) Iterate(iterate func(*Node) bool) {
	if !iterate(n) {
		return
	}

	node := n.next.Load()
	for {
		if node == nil {
			return
		}
		if !iterate(node) {
			return
		}
		node = node.next.Load()
	}
}
