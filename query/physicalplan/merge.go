package physicalplan

import (
	"sync"

	"github.com/apache/arrow/go/v8/arrow"
)

// MergeOperator is used to combined the results of multiple parallel streams
// into a single stream concurrent stream. It also forms a barrier on the
// finishers, by waiting to call next plan's finish until all previous parallel
// stages have finished.
type MergeOperator struct {
	wg        sync.WaitGroup
	finishMtx sync.Mutex
	nextPlan  PhysicalPlan
	nextMtx   sync.Mutex
	finished  bool
}

func Merge() *MergeOperator {
	return &MergeOperator{}
}

func (m *MergeOperator) Callback(record arrow.Record) error {
	// multiple threads can emit the results to the next step, but they will do
	// it synchronously
	m.nextMtx.Lock()
	defer m.nextMtx.Unlock()

	err := m.nextPlan.Callback(record)
	if err != nil {
		return err
	}
	return nil
}

func (m *MergeOperator) SetNextPlan(nextPlan PhysicalPlan) {
	m.nextPlan = nextPlan
}

func (m *MergeOperator) Finish() error {
	// all results from the previous step in this thread have been added to buffer
	m.wg.Done()

	// only one thread will emit the results and the thread that holds the finish
	// mutex is the one that will do it
	if m.finishMtx.TryLock() && !m.finished {
		// wait for all threads to finish adding their results to buffer
		m.wg.Wait()
		defer m.finishMtx.Unlock()
		m.finished = true

		err := m.nextPlan.Finish()
		if err != nil {
			return err
		}
	}
	return nil
}
