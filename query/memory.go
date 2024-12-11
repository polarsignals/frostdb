package query

import (
	"runtime/debug"
	"sync/atomic"

	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const PanicMemoryLimit = "memory limit exceeded"

var _ memory.Allocator = (*LimitAllocator)(nil)

// LimitAllocator is a wrapper around a memory.Allocator that panics if the memory usage exceeds the defined limit.
type LimitAllocator struct {
	limit     int64
	allocated *atomic.Int64
	allocator memory.Allocator
	reg       prometheus.Registerer
}

type AllocatorOption func(*LimitAllocator)

func WithRegistry(reg prometheus.Registerer) AllocatorOption {
	return func(a *LimitAllocator) {
		a.reg = reg
	}
}

func NewLimitAllocator(limit int64, allocator memory.Allocator, options ...AllocatorOption) *LimitAllocator {
	l := &LimitAllocator{
		limit:     limit,
		allocated: &atomic.Int64{},
		allocator: allocator,
		reg:       prometheus.NewRegistry(),
	}

	for _, option := range options {
		option(l)
	}

	promauto.With(l.reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "memory_allocated_bytes",
		Help: "The total number of bytes allocated by the allocator.",
	}, func() float64 {
		return float64(l.allocated.Load())
	})

	return l
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
