package physicalplan

import (
	"context"
	"testing"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/memory"
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

func (m *mockTableReader) View(ctx context.Context, fn func(ctx context.Context, tx uint64) error) error {
	return nil
}

func (m *mockTableReader) Iterator(
	ctx context.Context,
	tx uint64,
	pool memory.Allocator,
	callbacks []logicalplan.Callback,
	iterOpts ...logicalplan.Option,
) error {
	return nil
}

func (m *mockTableReader) SchemaIterator(
	ctx context.Context,
	tx uint64,
	pool memory.Allocator,
	callbacks []logicalplan.Callback,
	iterOpts ...logicalplan.Option,
) error {
	return nil
}

type mockTableProvider struct {
	schema *dynparquet.Schema
}

func (m *mockTableProvider) GetTable(name string) (logicalplan.TableReader, error) {
	return &mockTableReader{
		schema: m.schema,
	}, nil
}

func TestBuildPhysicalPlan(t *testing.T) {
	p, _ := (&logicalplan.Builder{}).
		Scan(&mockTableProvider{schema: dynparquet.NewSampleSchema()}, "table1").
		Filter(logicalplan.Col("labels.test").Eq(logicalplan.Literal("abc"))).
		Aggregate(
			[]logicalplan.Expr{logicalplan.Sum(logicalplan.Col("value")).Alias("value_sum")},
			[]logicalplan.Expr{logicalplan.Col("stacktrace")},
		).
		Project(logicalplan.Col("stacktrace"), logicalplan.Col("value_sum")).
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
