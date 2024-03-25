package exprpb

import (
	"context"
	"testing"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/polarsignals/frostdb/dynparquet"
	pb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/storage/v1alpha1"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

func TestPlan(t *testing.T) {
	protoPlan := &pb.PlanNode{
		Spec: &pb.PlanNodeSpec{
			Spec: &pb.PlanNodeSpec_SchemaScan{
				SchemaScan: &pb.SchemaScan{
					Base: &pb.ScanBase{
						Database: "foo",
						Table:    "bar",
					},
				},
			},
		},
	}

	engine := NewEngine(memory.NewGoAllocator(), &mockTableProvider{
		schema: dynparquet.NewSampleSchema(),
	})

	builder, err := engine.FromProto(protoPlan)
	require.NoError(t, err)

	require.Equal(t, "bar", builder.LogicalPlan.SchemaScan.TableName)

	err = builder.Execute(context.Background(), func(ctx context.Context, r arrow.Record) error {
		return nil
	})
	require.NoError(t, err)

	explain, err := builder.Explain(context.Background())
	require.NoError(t, err)
	require.Equal(t, `SchemaScan [concurrent] - Synchronizer`, explain)

	// next plan

	protoPlan = &pb.PlanNode{
		Spec: &pb.PlanNodeSpec{
			Spec: &pb.PlanNodeSpec_Projection{
				Projection: &pb.Projection{
					Exprs: []*pb.Expr{{
						Def: &pb.ExprDef{
							Content: &pb.ExprDef_Column{
								Column: &pb.Column{
									Name: "foo",
								},
							},
						},
					}},
				},
			},
		},
		Next: &pb.PlanNode{
			Spec: &pb.PlanNodeSpec{
				Spec: &pb.PlanNodeSpec_TableScan{
					TableScan: &pb.TableScan{
						Base: &pb.ScanBase{
							Table: "bar",
						},
					},
				},
			},
		},
	}

	builder, err = engine.FromProto(protoPlan)
	require.NoError(t, err)

	require.Equal(t, &logicalplan.Column{ColumnName: "foo"}, builder.LogicalPlan.Projection.Exprs[0])
	require.Equal(t, "bar", builder.LogicalPlan.Input.TableScan.TableName)

	err = builder.Execute(context.Background(), func(ctx context.Context, r arrow.Record) error {
		return nil
	})
	require.NoError(t, err)

	explain, err = builder.Explain(context.Background())
	require.NoError(t, err)
	require.Equal(t, `TableScan [concurrent] - Projection (foo) - Synchronizer`, explain)
}

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
