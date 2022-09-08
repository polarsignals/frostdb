package physicalplan

import (
	"context"
	"sync"

	"github.com/apache/arrow/go/v8/arrow"
	"go.uber.org/atomic"
)

// Synchronizer is used to combine the results of multiple parallel streams
// into a single stream concurrent stream. It also forms a barrier on the
// finishers, by waiting to call next plan's finish until all previous parallel
// stages have finished.
type Synchronizer struct {
	next    PhysicalPlan
	nextMtx sync.Mutex
	running *atomic.Uint64
}

func Synchronize(concurrency int) *Synchronizer {
	return &Synchronizer{running: atomic.NewUint64(uint64(concurrency))}
}

func (m *Synchronizer) Callback(ctx context.Context, r arrow.Record) error {
	// multiple threads can emit the results to the next step, but they will do
	// it synchronously
	m.nextMtx.Lock()
	defer m.nextMtx.Unlock()

	err := m.next.Callback(ctx, r)
	if err != nil {
		return err
	}
	return nil
}

func (m *Synchronizer) Finish(ctx context.Context) error {
	if m.running.Dec() > 0 {
		return nil
	}
	return m.next.Finish(ctx)
}

func (m *Synchronizer) SetNext(next PhysicalPlan) {
	m.next = next
}

func (m *Synchronizer) SetNextPlan(nextPlan PhysicalPlan) {
	m.next = nextPlan
}

func (m *Synchronizer) Draw() *Diagram {
	return &Diagram{}
}
