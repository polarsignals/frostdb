package physicalplan

import (
	"context"
	"sync"

	"github.com/apache/arrow/go/v8/arrow"
)

// Synchronizer is used to combine the results of multiple parallel streams
// into a single stream concurrent stream. It also forms a barrier on the
// finishers, by waiting to call next plan's finish until all previous parallel
// stages have finished.
type Synchronizer struct {
	next      PhysicalPlan
	wg        *sync.WaitGroup
	finishMtx sync.Mutex
	nextMtx   sync.Mutex
	finished  bool
}

func Synchronize() *Synchronizer {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	return &Synchronizer{wg: wg}
}

func (m *Synchronizer) SetNext(next PhysicalPlan) {
	m.next = next
}

func (m *Synchronizer) Start(ctx context.Context) error {
	return nil
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
	// all results from the previous step in this thread have been added to buffer
	m.wg.Done()

	// only one thread will emit the results and the thread that holds the finish
	// mutex is the one that will do it
	if m.finishMtx.TryLock() && !m.finished {
		// wait for all threads to finish adding their results to buffer
		m.wg.Wait()
		defer m.finishMtx.Unlock()
		m.finished = true

		err := m.next.Finish(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Synchronizer) Draw() *Diagram {
	var child *Diagram
	if s.next != nil {
		child = s.next.Draw()
	}
	return &Diagram{
		Details: "Synchronizer (1x)",
		Child:   child,
	}
}
