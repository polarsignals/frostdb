package frostdb

import (
	"fmt"
	"strings"
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
	next *atomic.Pointer[Node]
	part *parts.Part

	sentinel SentinelType // sentinel nodes contain no parts, and are to indicate the start of a new sub list
}

func (n *Node) String() string {
	if n.part == nil {
		return fmt.Sprintf("[%v]->", n.sentinel)
	} else {
		if n.part.Record() != nil {
			return fmt.Sprintf("[%v]->", n.part.Record().NumRows())
		} else {
			b, _ := n.part.AsSerializedBuffer(nil)
			return fmt.Sprintf("[%v]->", b.NumRows())
		}
	}
}

func (l *List) String() string {
	s := ""
	l.Iterate(func(node *Node) bool {
		s += node.String()
		return true
	})

	return strings.TrimSuffix(s, "->")
}

type List struct {
	head *atomic.Pointer[Node]
}

// NewList creates a new part list using atomic constructs.
func NewList(head *atomic.Pointer[Node]) *List {
	p := &List{
		head: head,
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
		next := l.head.Load()
		node.next.Store(next)
		if l.head.CompareAndSwap(next, node) {
			return NewList(l.head)
		}
	}
}

// Prepend a node onto the front of the list.
func (l *List) Prepend(part *parts.Part) *Node {
	node := &Node{
		next: &atomic.Pointer[Node]{},
		part: part,
	}
	for { // continue until a successful compare and swap occurs
		next := l.head.Load()
		node.next.Store(next)
		if l.head.CompareAndSwap(next, node) {
			return node
		}
	}
}

// Iterate accesses every node in the list.
func (l *List) Iterate(iterate func(*Node) bool) {
	node := l.head.Load()
	for {
		if node == nil {
			return
		}
		if !iterate(node) { // if the part == nil then this is a sentinel node, and we can skip it
			return
		}
		node = node.next.Load()
	}
}
