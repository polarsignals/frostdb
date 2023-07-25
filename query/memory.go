package query

import (
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow/go/v13/arrow/memory"
)

const PanicMemoryLimit = "memory limit exceeded"

var _ memory.Allocator = (*LimitAllocator)(nil)

// LimitAllocator is a wrapper around a memory.Allocator that panics if the memory usage exceeds the defined limit.
type LimitAllocator struct {
	limit     int64
	allocated *atomic.Int64
	allocator memory.Allocator
}

func NewLimitAllocator(limit int64, allocator memory.Allocator) *LimitAllocator {
	return &LimitAllocator{
		limit:     limit,
		allocated: &atomic.Int64{},
		allocator: allocator,
	}
}

func (a *LimitAllocator) Allocate(size int) []byte {
	for {
		allocated := a.allocated.Load()
		if allocated+int64(size) > a.limit {
			panic(PanicMemoryLimit)
		}

		if a.allocated.CompareAndSwap(allocated, allocated+int64(size)) {
			return a.allocator.Allocate(size)
		}
	}
}

func (a *LimitAllocator) Reallocate(size int, b []byte) []byte {
	if len(b) == size {
		return b
	}

	diff := int64(size - len(b))
	for {
		allocated := a.allocated.Load()
		if allocated+diff > a.limit {
			debug.PrintStack()
			panic(PanicMemoryLimit)
		}

		if a.allocated.CompareAndSwap(allocated, allocated+diff) {
			return a.allocator.Reallocate(size, b)
		}
	}
}

func (a *LimitAllocator) Free(b []byte) {
	a.allocated.Add(-int64(len(b)))
	a.allocator.Free(b)
}

func (a *LimitAllocator) Allocated() int {
	return int(a.allocated.Load())
}

type allocation struct {
	stack string
	size  int
}

type DebugAllocator struct {
	sync.Mutex

	allocations map[*byte]allocation
	allocator   memory.Allocator
}

func NewDebugAllocator(allocator memory.Allocator) *DebugAllocator {
	return &DebugAllocator{
		allocations: make(map[*byte]allocation),
		allocator:   allocator,
	}
}

func (a *DebugAllocator) Allocate(size int) []byte {
	a.Lock()
	defer a.Unlock()

	b := a.allocator.Allocate(size)
	a.allocations[&b[0]] = allocation{
		stack: string(debug.Stack()),
		size:  size,
	}
	return b
}

func (a *DebugAllocator) Reallocate(size int, b []byte) []byte {
	a.Lock()
	defer a.Unlock()

	delete(a.allocations, &b[0])
	b = a.allocator.Reallocate(size, b)
	a.allocations[&b[0]] = allocation{
		stack: string(debug.Stack()),
		size:  size,
	}
	return b
}

func (a *DebugAllocator) Free(b []byte) {
	a.Lock()
	defer a.Unlock()

	if len(b) == 0 {
		return
	}

	delete(a.allocations, &b[0])
	a.allocator.Free(b)
}

func (a *DebugAllocator) String() string {
	a.Lock()
	defer a.Unlock()

	s := fmt.Sprintf("Allocations remaining: %v\n", len(a.allocations))
	for _, stack := range a.allocations {
		s += fmt.Sprintf("Size: %v\n", stack.size)
		s += stack.stack
		s += "\n================================================================\n"
	}

	return s
}
