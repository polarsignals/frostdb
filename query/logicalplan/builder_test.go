package logicalplan

import (
	"testing"

	"github.com/apache/arrow/go/v14/arrow/scalar"
	"github.com/stretchr/testify/require"

	"github.com/polarsignals/frostdb/dynparquet"
)

func TestLogicalPlanBuilder(t *testing.T) {
	tableProvider := &mockTableProvider{schema: dynparquet.NewSampleSchema()}
	p, err := (&Builder{}).
		Scan(tableProvider, "table1").
		Filter(Col("labels.test").Eq(Literal("abc"))).
		Aggregate(
			[]*AggregationFunction{Sum(Col("value"))},
			[]Expr{Col("stacktrace")},
		).
		Project(Col("stacktrace"), Sum(Col("value")).Alias("value_sum")).
		Build()

	require.Nil(t, err)

	require.Equal(t, &LogicalPlan{
		Projection: &Projection{
			Exprs: []Expr{
				&Column{ColumnName: "stacktrace"},
				&AliasExpr{
					Expr:  &AggregationFunction{Func: AggFuncSum, Expr: &Column{ColumnName: "value"}},
					Alias: "value_sum",
				},
			},
		},
		Input: &LogicalPlan{
			Aggregation: &Aggregation{
				GroupExprs: []Expr{&Column{ColumnName: "stacktrace"}},
				AggExprs:   []*AggregationFunction{{Func: AggFuncSum, Expr: &Column{ColumnName: "value"}}},
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
			Projection: &Projection{
				Exprs: []Expr{&Column{ColumnName: "labels.test"}},
			},
			Input: &LogicalPlan{
				TableScan: &TableScan{
					TableProvider: tableProvider,
					TableName:     "table1",
				},
			},
		},
	}, p)
}

func TestRenamedColumn(t *testing.T) {
	tableProvider := &mockTableProvider{schema: dynparquet.NewSampleSchema()}
	_, err := (&Builder{}).
		Scan(tableProvider, "table1").
		Project(
			Col("value").Alias("other_value"),
			Col("stacktrace"),
		).
		Aggregate(
			[]*AggregationFunction{Sum(Col("other_value"))},
			[]Expr{Col("stacktrace")},
		).
		Project(Col("stacktrace"), Sum(Col("other_value")).Alias("value_sum")).
		Build()
	require.NoError(t, err)
}
