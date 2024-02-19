package physicalplan

import (
	"context"
	"testing"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

type mockTableReader struct {
	schema *dynparquet.Schema
}

func (m *mockTableReader) Schema() *dynparquet.Schema {
	return m.schema
}

func (m *mockTableReader) View(_ context.Context, _ func(ctx context.Context, tx uint64) error) error {
	return nil
}

func (m *mockTableReader) Iterator(
	_ context.Context,
	_ uint64,
	_ memory.Allocator,
	_ []logicalplan.Callback,
	_ ...logicalplan.Option,
) error {
	return nil
}

func (m *mockTableReader) SchemaIterator(
	_ context.Context,
	_ uint64,
	_ memory.Allocator,
	_ []logicalplan.Callback,
	_ ...logicalplan.Option,
) error {
	return nil
}

type mockTableProvider struct {
	schema *dynparquet.Schema
}

func (m *mockTableProvider) GetTable(_ string) (logicalplan.TableReader, error) {
	return &mockTableReader{
		schema: m.schema,
	}, nil
}

func TestBuildPhysicalPlan(t *testing.T) {
	p, _ := (&logicalplan.Builder{}).
		Scan(&mockTableProvider{schema: dynparquet.NewSampleSchema()}, "table1").
		Filter(logicalplan.Col("labels.test").Eq(logicalplan.Literal("abc"))).
		Aggregate(
			[]*logicalplan.AggregationFunction{logicalplan.Sum(logicalplan.Col("value"))},
			[]logicalplan.Expr{logicalplan.Col("stacktrace")},
		).
		Project(logicalplan.Col("stacktrace"), logicalplan.Sum(logicalplan.Col("value")).Alias("value_sum")).
		Build()

	optimizers := []logicalplan.Optimizer{
		&logicalplan.PhysicalProjectionPushDown{},
		&logicalplan.FilterPushDown{},
	}

	for _, optimizer := range optimizers {
		optimizer.Optimize(p)
	}

	_, err := Build(
		context.Background(),
		memory.DefaultAllocator,
		trace.NewNoopTracerProvider().Tracer(""),
		dynparquet.NewSampleSchema(),
		p,
	)
	require.NoError(t, err)
}

type mockPhysicalPlan struct {
	next PhysicalPlan
}

func (m *mockPhysicalPlan) Callback(ctx context.Context, r arrow.Record) error {
	return m.next.Callback(ctx, r)
}

func (m *mockPhysicalPlan) Finish(ctx context.Context) error {
	return m.next.Finish(ctx)
}

func (m *mockPhysicalPlan) SetNext(next PhysicalPlan) {
	m.next = next
}

func (m *mockPhysicalPlan) Draw() *Diagram {
	return &Diagram{}
}

func (m *mockPhysicalPlan) Close() {
	m.next.Close()
}
