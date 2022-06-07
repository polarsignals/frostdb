package physicalplan

import (
	"sync"

	"github.com/apache/arrow/go/v8/arrow/memory"
)

type Finisher struct {
	pool        memory.Allocator
	mutex       sync.Mutex
	aggregation *HashAggregateFinisher
}

// TODO somehow need to extend this to work for multiple callbacks

func (f *Finisher) Finish() error {
	if f.aggregation != nil {
		err := f.aggregation.Finish()
		if err != nil {
			return err
		}
	}
	return nil
}

func (f *Finisher) AddHashAgg(agg *HashAggregate) {
	f.mutex.Lock()
	if f.aggregation == nil {
		f.aggregation = &HashAggregateFinisher{
			pool:         f.pool,
			aggregations: make([]*HashAggregate, 0),
		}
	}
	f.aggregation.aggregations = append(f.aggregation.aggregations, agg)
	f.mutex.Unlock()
}
