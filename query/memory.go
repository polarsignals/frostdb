package query

import (
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
	allocated := a.allocated.Add(int64(size))
	if allocated > a.limit {
		panic(PanicMemoryLimit)
	}

	return a.allocator.Allocate(size)
}

func (a *LimitAllocator) Reallocate(size int, b []byte) []byte {
	if len(b) == size {
		return b
	}

	allocated := a.allocated.Add(int64(size - len(b)))
	if allocated > a.limit {
		panic(PanicMemoryLimit)
	}
	buf := a.allocator.Reallocate(size, b)
	return buf
}

func (a *LimitAllocator) Free(b []byte) {
	a.allocated.Add(-int64(len(b)))
	a.allocator.Free(b)
}

func (a *LimitAllocator) Allocated() int {
	return int(a.allocated.Load())
}
