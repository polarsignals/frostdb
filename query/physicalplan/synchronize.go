package physicalplan

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow/go/v12/arrow"
)

// Synchronizer is used to combine the results of multiple parallel streams
// into a single stream concurrent stream. It also forms a barrier on the
// finishers, by waiting to call next plan's finish until all previous parallel
// stages have finished.
type Synchronizer struct {
	next    PhysicalPlan
	nextMtx sync.Mutex
	running *atomic.Int64
}

func Synchronize(concurrency int) *Synchronizer {
	running := &atomic.Int64{}
	running.Add(int64(concurrency))
	return &Synchronizer{running: running}
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
	running := m.running.Add(-1)
	if running < 0 {
		return errors.New("too many Synchronizer Finish calls")
	}
	if running > 0 {
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
	return &Diagram{Details: "Synchronizer", Child: m.next.Draw()}
}
