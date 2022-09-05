package physicalplan

import (
	"context"
	"sync"

	"github.com/apache/arrow/go/v8/arrow"

	"github.com/polarsignals/frostdb/query/logicalplan"
)

// Synchronizer is used to combine the results of multiple parallel streams
// into a single stream concurrent stream. It also forms a barrier on the
// finishers, by waiting to call next plan's finish until all previous parallel
// stages have finished.
type Synchronizer struct {
	next      PhysicalPlan
	callback  logicalplan.Callback   // synchronous
	callbacks []logicalplan.Callback // concurrent

	wg        *sync.WaitGroup
	finishMtx sync.Mutex
	nextMtx   sync.Mutex
	finished  bool
}

func (s *Synchronizer) Callbacks() []logicalplan.Callback {
	return s.callbacks
}

func Synchronize(concurrency int, callback logicalplan.Callback) *Synchronizer {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	s := &Synchronizer{wg: wg, callback: callback}

	s.callbacks = make([]logicalplan.Callback, 0, concurrency)
	for i := 0; i < concurrency; i++ {
		s.callbacks = append(s.callbacks, func(ctx context.Context, r arrow.Record) error {
			s.nextMtx.Lock()
			defer s.nextMtx.Unlock()

			return s.callback(ctx, r)
		})
	}

	return s
}

func (s *Synchronizer) SetNext(next PhysicalPlan) {
	s.next = next
}

func (s *Synchronizer) Finish(ctx context.Context) error {
	if s.finished {
		return nil
	}

	// all results from the previous step in this thread have been added to buffer
	s.wg.Done()

	// only one thread will emit the results and the thread that holds the finish
	// mutex is the one that will do it
	if s.finishMtx.TryLock() && !s.finished {
		// wait for all threads to finish adding their results to buffer
		s.wg.Wait()
		defer s.finishMtx.Unlock()
		s.finished = true

		err := s.next.Finish(ctx)
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
