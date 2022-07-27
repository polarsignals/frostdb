package physicalplan

import (
	"context"
	"testing"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

type mockTableReader struct {
	schema *dynparquet.Schema
}

func (m *mockTableReader) Schema() *dynparquet.Schema {
	return m.schema
}

func (m *mockTableReader) View(fn func(tx uint64) error) error {
	return nil
}

func (m *mockTableReader) Iterator(
	ctx context.Context,
	tx uint64,
	pool memory.Allocator,
	schema *arrow.Schema,
	physicalProjection []logicalplan.Expr,
	projection []logicalplan.Expr,
	filter logicalplan.Expr,
	distinctColumns []logicalplan.Expr,
	callback func(r arrow.Record) error,
) error {
	return nil
}

func (m *mockTableReader) SchemaIterator(
	ctx context.Context,
	tx uint64,
	pool memory.Allocator,
	physicalProjection []logicalplan.Expr,
	projection []logicalplan.Expr,
	filter logicalplan.Expr,
	distinctColumns []logicalplan.Expr,
	callback func(r arrow.Record) error,
) error {
	return nil
}

func (m *mockTableReader) ArrowSchema(
	ctx context.Context,
	tx uint64,
	pool memory.Allocator,
	physicalProjection []logicalplan.Expr,
	projection []logicalplan.Expr,
	filter logicalplan.Expr,
	distinctColumns []logicalplan.Expr,
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

	optimizers := []logicalplan.Optimizer{
		&logicalplan.PhysicalProjectionPushDown{},
		&logicalplan.FilterPushDown{},
	}

	for _, optimizer := range optimizers {
		optimizer.Optimize(p)
	}

	_, err := Build(memory.DefaultAllocator, dynparquet.NewSampleSchema(), p)
	require.NoError(t, err)
}
