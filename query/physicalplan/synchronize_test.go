package physicalplan

import (
	"context"
	"sync"
	"testing"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestSynchronize(t *testing.T) {
	concurrency := 4
	plans := make([]PhysicalPlan, 0, concurrency)
	for i := 0; i < concurrency; i++ {
		plans = append(plans, &mockPhysicalPlan{})
	}
	op := &OutputPlan{
		scan: &mockTableScan{plans: plans},
	}

	synchronizer := Synchronize(len(plans))
	synchronizer.SetNext(op)

	for _, p := range plans {
		p.SetNext(synchronizer)
	}

	calls := 0
	err := op.Execute(
		context.Background(),
		memory.NewGoAllocator(),
		func(ctx context.Context, r arrow.Record) error {
			calls++
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, calls, concurrency*10)
}

type mockTableScan struct {
	plans []PhysicalPlan
}

func (m *mockTableScan) Execute(ctx context.Context, pool memory.Allocator) error {
	records := make(chan arrow.Record, len(m.plans))

	var wg sync.WaitGroup

	for _, plan := range m.plans {
		wg.Add(1)
		go func(plan PhysicalPlan) {
			for r := range records {
				_ = plan.Callback(ctx, r)
			}
			wg.Done()
		}(plan)
	}

	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: &arrow.Int64Type{}}}, nil)
	for i := int64(0); i < int64(len(m.plans)*10); i++ {
		b := array.NewRecordBuilder(pool, schema)
		b.Field(0).(*array.Int64Builder).AppendValues([]int64{i}, nil)
		records <- b.NewRecord()
	}

	close(records)
	wg.Wait()

	for _, plan := range m.plans {
		if err := plan.Finish(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (m *mockTableScan) Draw() *Diagram {
	return &Diagram{}
}
