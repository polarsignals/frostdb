package logicalplan

import (
	"encoding/json"
	"testing"

	"github.com/polarsignals/arcticdb/dynparquet"

	"github.com/apache/arrow/go/v8/arrow/scalar"
	"github.com/stretchr/testify/require"
)

func TestLogicalPlanBuilder(t *testing.T) {
	tableProvider := &mockTableProvider{schema: dynparquet.NewSampleSchema()}
	p, err := (&Builder{}).
		Scan(tableProvider, "table1").
		Filter(Col("labels.test").Eq(Literal("abc"))).
		Aggregate(
			Sum(Col("value")).Alias("value_sum"),
			Col("stacktrace"),
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
				AggExpr: &AliasExpr{
					Expr:  &AggregationFunction{Func: SumAggFunc, Expr: &Column{ColumnName: "value"}},
					Alias: "value_sum",
				},
			},
			Input: &LogicalPlan{
				Filter: &Filter{
					Expr: &BinaryExpr{
						Left:  &Column{ColumnName: "labels.test"},
						Op:    EqOp,
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
			Columns: []Expr{&Column{ColumnName: "labels.test"}},
		},
		Input: &LogicalPlan{
			TableScan: &TableScan{
				TableProvider: tableProvider,
				TableName:     "table1",
			},
		},
	}, p)
}

func TestLogicalPlanBuilderFilterJSON(t *testing.T) {
	p := (&Builder{}).
		Filter(Col("labels.test").Eq(Literal("abc"))).
		Filter(Col("name").RegexMatch("^labels$"))

	expected := `{"ExprType":"*logicalplan.BinaryExpr","Expr":{"LeftType":"*logicalplan.Column","Left":{"Expr":"string","ColumnName":"name"},"RightType":"*logicalplan.LiteralExpr","Right":{"ValueType":"*scalar.String","Value":"^labels$"},"Op":6}}`

	output, err := json.Marshal(p.plan.Filter)
	require.NoError(t, err)
	require.JSONEq(t, expected, string(output))

	var f *Filter
	err = json.Unmarshal(output, &f)
	require.NoError(t, err)
	require.Equal(t, p.plan.Filter, f)
}
