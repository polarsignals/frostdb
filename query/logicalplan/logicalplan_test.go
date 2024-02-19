package logicalplan

import (
	"context"
	"testing"

	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/polarsignals/frostdb/dynparquet"
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
	_ []Callback,
	_ ...Option,
) error {
	return nil
}

func (m *mockTableReader) SchemaIterator(
	_ context.Context,
	_ uint64,
	_ memory.Allocator,
	_ []Callback,
	_ ...Option,
) error {
	return nil
}

type mockTableProvider struct {
	schema *dynparquet.Schema
}

func (m *mockTableProvider) GetTable(_ string) (TableReader, error) {
	return &mockTableReader{
		schema: m.schema,
	}, nil
}

func TestInputSchemaGetter(t *testing.T) {
	schema := dynparquet.NewSampleSchema()

	// test we can get the table by traversing to find the TableScan
	plan, err := (&Builder{}).
		Scan(&mockTableProvider{schema}, "table1").
		Filter(Col("labels.test").Eq(Literal("abc"))).
		Aggregate(
			[]*AggregationFunction{Sum(Col("value"))},
			[]Expr{Col("stacktrace")},
		).
		Project(Col("stacktrace"), Sum(Col("value")).Alias("value_sum")).
		Build()
	require.NoError(t, err)
	require.Equal(t, schema, plan.InputSchema())
}

func Test_ExprClone(t *testing.T) {
	expr := Col("labels.test").Eq(Literal("abc"))
	expr2 := expr.Clone()
	require.Equal(t, expr, expr2)

	// Modify the original expr and make sure the clone is not affected
	expr.Op = OpGt
	require.NotEqual(t, expr, expr2)
}
