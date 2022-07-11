package physicalplan

import (
	"context"

	"github.com/apache/arrow/go/v8/arrow"
	"golang.org/x/sync/errgroup"
)

// ExchangeOperator is used to horizontally scale query execution. It take a
// single stream of execution and invokes the next step's callback in parallel.
// The next steps finisher will also be invoked in parallel.
type ExchangeOperator struct {
	recordChan chan arrow.Record
	eg         *errgroup.Group
	context    context.Context
}

func Exchange(ctx context.Context) *ExchangeOperator {
	eg, egCtx := errgroup.WithContext(ctx)
	return &ExchangeOperator{
		recordChan: make(chan arrow.Record),
		eg:         eg,
		context:    egCtx,
	}
}

func (e *ExchangeOperator) Callback(record arrow.Record) error {
	record.Retain()
	e.recordChan <- record
	return nil
}

func (e *ExchangeOperator) SetNextPlan(nextPlan PhysicalPlan) {
	// each time a next plan is added invoke a new goroutine to handle the records
	e.eg.Go(func() error {
		for record := range e.recordChan {
			err := nextPlan.Callback(record)
			record.Release()
			if err != nil {
				return err
			}
		}
		return nextPlan.Finish()
	})
}

func (e *ExchangeOperator) Finish() error {
	close(e.recordChan)
	return e.eg.Wait()
}
