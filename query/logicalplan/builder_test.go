package logicalplan

import (
	"testing"

	"github.com/apache/arrow/go/v12/arrow/scalar"
	"github.com/stretchr/testify/require"

	"github.com/polarsignals/frostdb/dynparquet"
)

func TestLogicalPlanBuilder(t *testing.T) {
	tableProvider := &mockTableProvider{schema: dynparquet.NewSampleSchema()}
	p, err := (&Builder{}).
		Scan(tableProvider, "table1").
		Filter(Col("labels.test").Eq(Literal("abc"))).
		Aggregate(
			[]Expr{Sum(Col("value")).Alias("value_sum")},
			[]Expr{Col("stacktrace")},
		).
		Project(Col("stacktrace")).
		Build()

	require.Nil(t, err)

	require.Equal(t, &LogicalPlan{
		Projection: &Projection{
			Exprs: []Expr{
				&Column{ColumnName: "stacktrace"},
			},
		},
		Input: &LogicalPlan{
			Aggregation: &Aggregation{
				GroupExprs: []Expr{&Column{ColumnName: "stacktrace"}},
				AggExprs: []Expr{&AliasExpr{
					Expr:  &AggregationFunction{Func: AggFuncSum, Expr: &Column{ColumnName: "value"}},
					Alias: "value_sum",
				}},
			},
			Input: &LogicalPlan{
				Filter: &Filter{
					Expr: &BinaryExpr{
						Left:  &Column{ColumnName: "labels.test"},
						Op:    OpEq,
						Right: &LiteralExpr{Value: scalar.MakeScalar("abc")},
					},
				},
				Input: &LogicalPlan{
					TableScan: &TableScan{
						TableProvider: tableProvider,
						TableName:     "table1",
					},
				},
			},
		},
	}, p)
}

func TestLogicalPlanBuilderWithoutProjection(t *testing.T) {
	tableProvider := &mockTableProvider{schema: dynparquet.NewSampleSchema()}
	p, _ := (&Builder{}).
		Scan(tableProvider, "table1").
		Distinct(Col("labels.test")).
		Build()

	require.Equal(t, &LogicalPlan{
		Distinct: &Distinct{
			Exprs: []Expr{&Column{ColumnName: "labels.test"}},
		},
		Input: &LogicalPlan{
			TableScan: &TableScan{
				TableProvider: tableProvider,
				TableName:     "table1",
			},
		},
	}, p)
}
