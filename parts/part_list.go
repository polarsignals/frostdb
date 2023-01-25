package parts

import (
	"sync/atomic"
)

type SentinelType uint8

const (
	None SentinelType = iota
	Compacting
	Compacted
)

// Node is a Part that is a part of a linked-list.
type Node struct {
	next *atomic.Pointer[Node]
	part *Part

	sentinel SentinelType // sentinel nodes contain no parts, and are to indicate the start of a new sub list
}

func (n *Node) Compacted() bool {
	return n.sentinel == Compacted
}

type List struct {
	next *atomic.Pointer[Node]

	// listType indicates the type of list this list is
	listType SentinelType
}

// NewList creates a new part list using atomic constructs.
func NewList(next *atomic.Pointer[Node], s SentinelType) *List {
	p := &List{
		next:     next,
		listType: s,
	}
	return p
}

// Sentinel adds a new sentinel node to the list, and returns the sub list starting from that sentinel.
func (l *List) Sentinel(s SentinelType) *List {
	node := &Node{
		next:     &atomic.Pointer[Node]{},
		sentinel: s,
	}
	for { // continue until a successful compare and swap occurs
		next := l.next.Load()
		node.next.Store(next)
		if l.next.CompareAndSwap(next, node) {
			return NewList(l.next, s)
		}
	}
}

// Prepend a node onto the front of the list.
func (l *List) Prepend(part *Part) *Node {
	node := &Node{
		next: &atomic.Pointer[Node]{},
		part: part,
	}
	for { // continue until a successful compare and swap occurs
		next := l.next.Load()
		node.next.Store(next)
		if next != nil && next.sentinel == Compacted { // This list is apart of a compacted granule, propogate the compacted value so each subsequent Prepend can return the correct value
			node.sentinel = Compacted
		}
		if l.next.CompareAndSwap(next, node) {
			return node
		}
	}
}

// Iterate accesses every node in the list.
func (l *List) Iterate(iterate func(*Part) bool) {
	next := l.next.Load()
	for {
		node := (*Node)(next)
		if node == nil {
			return
		}
		switch node.part {
		case nil: // sentinel node
			if l.listType != None && node.sentinel != l.listType { // if we've encountererd a sentinel node from a different type of list we must exit
				return
			}
		default: // normal node
			if !iterate(node.part) { // if the part == nil then this is a sentinel node, and we can skip it
				return
			}
		}
		next = node.next.Load()
	}
}

func (l *List) Total() int {
	count := 0
	l.Iterate(func(_ *Part) bool {
		count++
		return true
	})

	return count
}
