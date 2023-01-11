package physicalplan

import (
	"context"
	"errors"
	"sync"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/memory"

	"github.com/polarsignals/frostdb/pqarrow/arrowutils"
)

// OrderedSynchronizer implements synchronizing ordered input from multiple
// goroutines. The strategy used is that any input that calls Callback must wait
// for all the other inputs to call Callback, since an ordered result cannot
// be produced until all inputs have pushed data. Another strategy would be to
// store the pushed records, but that requires fully copying all the data for
// safety.
type OrderedSynchronizer struct {
	pool        memory.Allocator
	orderByCols []int

	sync struct {
		mtx  sync.Mutex
		data []arrow.Record
		// inputsWaiting is an integer that keeps track of the number of inputs
		// waiting on the wait channel. It cannot be an atomic because it
		// sometimes needs to be compared to inputsRunning.
		inputsWaiting int
		// inputsRunning is an integer that keeps track of the number of inputs
		// that have not called Finish yet.
		inputsRunning int
	}
	wait chan struct{}
	next PhysicalPlan
}

func NewOrderedSynchronizer(pool memory.Allocator, inputs int, orderByCols []int) *OrderedSynchronizer {
	o := &OrderedSynchronizer{
		pool:        pool,
		orderByCols: orderByCols,
		wait:        make(chan struct{}),
	}
	o.sync.inputsRunning = inputs
	return o
}

func (o *OrderedSynchronizer) Callback(ctx context.Context, r arrow.Record) error {
	o.sync.mtx.Lock()
	o.sync.data = append(o.sync.data, r)
	o.sync.inputsWaiting++
	inputsWaiting := o.sync.inputsWaiting
	inputsRunning := o.sync.inputsRunning
	o.sync.mtx.Unlock()

	if inputsWaiting != inputsRunning {
		select {
		case <-o.wait:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	o.sync.mtx.Lock()
	defer o.sync.mtx.Unlock()
	o.sync.inputsWaiting--
	// This is the last input to call Callback, merge the records.
	mergedRecord, err := o.mergeRecordsLocked()
	if err != nil {
		return err
	}

	// Note that we hold the mutex while calling Callback because we want to
	// ensure that Callback is called in an ordered fashion since we could race
	// with a call to Callback in Finish.
	return o.next.Callback(ctx, mergedRecord)
}

func (o *OrderedSynchronizer) Finish(ctx context.Context) error {
	o.sync.mtx.Lock()
	defer o.sync.mtx.Unlock()
	o.sync.inputsRunning--
	running := o.sync.inputsRunning
	if running > 0 && running == o.sync.inputsWaiting {
		// All other goroutines are currently waiting to be woken up. We need to
		// merge the records.
		mergedRecord, err := o.mergeRecordsLocked()
		if err != nil {
			return err
		}
		return o.next.Callback(ctx, mergedRecord)
	}
	if running < 0 {
		return errors.New("too many OrderedSynchronizer Finish calls")
	}
	if running > 0 {
		return nil
	}
	return o.next.Finish(ctx)
}

// mergeRecordsLocked must be called while holding o.sync.mtx. It merges the
// records found in o.sync.data and unblocks all the inputs waiting on o.wait.
func (o *OrderedSynchronizer) mergeRecordsLocked() (arrow.Record, error) {
	mergedRecord, err := arrowutils.MergeRecords(o.pool, o.sync.data, o.orderByCols)
	if err != nil {
		return nil, err
	}
	// Now that the records have been merged, we can wake up the other input
	// goroutines. Since we have exactly o.sync.inputsWaiting waiting on the
	// channel, send the corresponding number of messages. Since we are also
	// holding the records mutex during this broadcast, fast inputs won't be
	// able to re-enter Callback until the mutex is released, so won't
	// mistakenly read another input's signal.
	for i := 0; i < o.sync.inputsWaiting; i++ {
		o.wait <- struct{}{}
	}
	// Reset inputsWaiting.
	o.sync.inputsWaiting = 0
	o.sync.data = o.sync.data[:0]
	return mergedRecord, nil
}

func (o *OrderedSynchronizer) SetNext(next PhysicalPlan) {
	o.next = next
}

func (o *OrderedSynchronizer) Draw() *Diagram {
	return &Diagram{}
}
