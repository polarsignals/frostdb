package physicalplan

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

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
	inputs      int
	orderByCols []int
	running     atomic.Int64

	records struct {
		mtx  sync.Mutex
		data []arrow.Record
	}
	wait chan struct{}
	// pendingInputs is the number of inputs that are yet to call Callback.
	pendingInputs atomic.Int64
	next          PhysicalPlan
}

func NewOrderedSynchronizer(pool memory.Allocator, inputs int, orderByCols []int) *OrderedSynchronizer {
	o := &OrderedSynchronizer{
		pool:        pool,
		inputs:      inputs,
		orderByCols: orderByCols,
		wait:        make(chan struct{}),
	}
	o.running.Add(int64(inputs))
	o.pendingInputs.Add(int64(inputs))
	return o
}

func (o *OrderedSynchronizer) Callback(ctx context.Context, r arrow.Record) error {
	o.records.mtx.Lock()
	o.records.data = append(o.records.data, r)
	o.records.mtx.Unlock()

	if o.pendingInputs.Add(-1) != 0 {
		select {
		case <-o.wait:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// This is the last input to call Callback, merge the records.
	o.records.mtx.Lock()
	defer o.records.mtx.Unlock()
	mergedRecord, err := arrowutils.MergeRecords(o.pool, o.records.data, o.orderByCols)
	if err != nil {
		return err
	}
	// Now that the records have been merged, we can wake up the other input
	// goroutines. Since we have exactly o.inputs-1 waiting on the
	// channel, send the corresponding number of messages. Since we are also
	// holding the records mutex during this broadcast, fast inputs won't be
	// able to re-enter Callback until the mutex is released, so won't
	// mistakenly read another input's signal.
	for i := 0; i < o.inputs-1; i++ {
		o.wait <- struct{}{}
	}
	// Reset pendingInputs.
	o.pendingInputs.Store(int64(o.inputs))
	o.records.data = o.records.data[:0]

	return o.next.Callback(ctx, mergedRecord)
}

func (o *OrderedSynchronizer) Finish(ctx context.Context) error {
	running := o.running.Add(-1)
	if running < 0 {
		return errors.New("too many OrderedSynchronizer Finish calls")
	}
	if running > 0 {
		return nil
	}
	return o.next.Finish(ctx)
}

func (o *OrderedSynchronizer) SetNext(next PhysicalPlan) {
	o.next = next
}

func (o *OrderedSynchronizer) Draw() *Diagram {
	return &Diagram{}
}
