package physicalplan

import (
	"context"
	"testing"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/memory"
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
	schema *arrow.Schema,
	iterOpts logicalplan.IterOptions,
	callbacks []logicalplan.Callback,
) error {
	return nil
}

func (m *mockTableReader) SchemaIterator(
	ctx context.Context,
	tx uint64,
	pool memory.Allocator,
	iterOpts logicalplan.IterOptions,
	callback []logicalplan.Callback,
) error {
	return nil
}

func (m *mockTableReader) ArrowSchema(
	ctx context.Context,
	tx uint64,
	pool memory.Allocator,
	iterOpts logicalplan.IterOptions,
) (*arrow.Schema, error) {
	return nil, nil
}

type mockTableProvider struct {
	schema *dynparquet.Schema
}

func (m *mockTableProvider) GetTable(name string) logicalplan.TableReader {
	return &mockTableReader{
		schema: m.schema,
	}
}

func TestBuildPhysicalPlan(t *testing.T) {
	p, _ := (&logicalplan.Builder{}).
		Scan(&mockTableProvider{schema: dynparquet.NewSampleSchema()}, "table1").
		Filter(logicalplan.Col("labels.test").Eq(logicalplan.Literal("abc"))).
		Aggregate(
			logicalplan.Sum(logicalplan.Col("value")).Alias("value_sum"),
			logicalplan.Col("stacktrace"),
		).
		Project(logicalplan.Col("stacktrace"), logicalplan.Col("value_sum")).
		Build()

	for _, optimizer := range logicalplan.DefaultOptimizers {
		optimizer.Optimize(p)
	}

	_, err := Build(
		context.Background(),
		memory.DefaultAllocator,
		trace.NewNoopTracerProvider().Tracer(""),
		dynparquet.NewSampleSchema(),
		p,
		func(ctx context.Context, record arrow.Record) error { return nil },
	)
	require.NoError(t, err)
}

type mockPhysicalPlan struct {
	callback func(ctx context.Context, r arrow.Record) error
	finish   func(ctx context.Context) error
}

func (m *mockPhysicalPlan) Start(ctx context.Context) error {
	// noop
	return nil
}

func (m *mockPhysicalPlan) Callback(ctx context.Context, r arrow.Record) error {
	return m.callback(ctx, r)
}

func (m *mockPhysicalPlan) Finish(ctx context.Context) error {
	return m.finish(ctx)
}

func (m *mockPhysicalPlan) SetNext(next PhysicalPlan) {
	// noop
}

func (m *mockPhysicalPlan) Draw() *Diagram {
	// noop
	return nil
}
