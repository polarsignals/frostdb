package logicalplan

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/polarsignals/frostdb/dynparquet"
)

func TestOnlyOneFieldCanBeSet(t *testing.T) {
	plan := LogicalPlan{
		Filter: &Filter{
			Expr: &BinaryExpr{
				Left:  Col("example_type"),
				Op:    OpEq,
				Right: Literal(4),
			},
		},
		TableScan: &TableScan{
			TableProvider: &mockTableProvider{dynparquet.NewSampleSchema()},
			TableName:     "table1",
		},
	}

	err := Validate(&plan)
	require.NotNil(t, err)

	planErr, ok := err.(*PlanValidationError)
	require.True(t, ok)
	require.True(t, strings.HasPrefix(planErr.message, "invalid number of fields"))
}

func TestCanTraverseInputThatIsInvalid(t *testing.T) {
	_, err := (&Builder{}).
		Scan(&mockTableProvider{dynparquet.NewSampleSchema()}, "table1").
		Filter(&BinaryExpr{
			Left:  Col("example_type"),
			Op:    OpEq,
			Right: Literal(4),
		}).
		Filter(&BinaryExpr{
			Left:  Col("stacktrace"),
			Op:    OpEq,
			Right: Literal(4),
		}).
		Build()

	require.NotNil(t, err)
	planErr, ok := err.(*PlanValidationError)
	require.True(t, ok)
	require.True(t, strings.HasPrefix(planErr.message, "invalid filter"))

	inputErr := planErr.input
	require.NotNil(t, inputErr)
	require.True(t, strings.HasPrefix(inputErr.message, "invalid filter"))
}

func TestAggregationMustHaveExpr(t *testing.T) {
	_, err := (&Builder{}).
		Aggregate(nil, nil).
		Build()

	require.NotNil(t, err)
	require.NotNil(t, err)
	planErr, ok := err.(*PlanValidationError)
	require.True(t, ok)
	require.True(t, strings.HasPrefix(planErr.message, "invalid aggregation: expression cannot be nil"))
}

func TestAggregationExprCannotHaveInvalidType(t *testing.T) {
	invalidExprs := [][]Expr{
		{Literal(4)},
		{Col("Test")},
	}

	for _, expr := range invalidExprs {
		_, err := (&Builder{}).
			Aggregate(expr, nil).
			Build()

		require.NotNil(t, err)
		require.NotNil(t, err)
		planErr, ok := err.(*PlanValidationError)
		require.True(t, ok)
		require.True(t, strings.HasPrefix(planErr.message, "invalid aggregation"))
		require.Len(t, planErr.children, 1)
		exprErr := planErr.children[0]
		require.True(t, strings.HasPrefix(exprErr.message, "aggregation expression is invalid"))
	}
}

func TestAggregationExprColumnMustExistInSchema(t *testing.T) {
	_, err := (&Builder{}).
		Scan(&mockTableProvider{dynparquet.NewSampleSchema()}, "table1").
		Aggregate([]Expr{Sum(Col("bad_column"))}, nil).
		Build()

	require.NotNil(t, err)
	require.NotNil(t, err)
	planErr, ok := err.(*PlanValidationError)
	require.True(t, ok)
	require.True(t, strings.HasPrefix(planErr.message, "invalid aggregation"))
	require.Len(t, planErr.children, 1)
	exprErr := planErr.children[0]
	require.True(t, strings.HasPrefix(exprErr.message, "column not found"))
}

func TestAggregationCannotSumOrMaxTextColumn(t *testing.T) {
	for _, testCase := range []struct {
		fn     func(Expr) *AggregationFunction
		errMsg string
	}{
		{
			fn:     Sum,
			errMsg: "cannot sum text column",
		},
		{
			fn:     Max,
			errMsg: "cannot max text column",
		},
	} {
		_, err := (&Builder{}).
			Scan(&mockTableProvider{dynparquet.NewSampleSchema()}, "table1").
			Aggregate([]Expr{testCase.fn(Col("example_type"))}, nil).
			Build()

		require.NotNil(t, err)
		require.NotNil(t, err)
		planErr, ok := err.(*PlanValidationError)
		require.True(t, ok)
		require.True(t, strings.HasPrefix(planErr.message, "invalid aggregation"))
		require.Len(t, planErr.children, 1)
		exprErr := planErr.children[0]
		require.True(t, strings.HasPrefix(exprErr.message, testCase.errMsg))
	}
}

func TestAggregationCannotUseAliasTwice(t *testing.T) {
	_, err := (&Builder{}).
		Scan(&mockTableProvider{dynparquet.NewSampleSchema()}, "table1").
		Aggregate([]Expr{
			Sum(Col("value")).Alias("value"), // should use e.g. sum_foo
			Max(Col("value")).Alias("value"), // should use e.g. max_foo
		}, nil).
		Build()

	require.NotNil(t, err)
	planErr, ok := err.(*PlanValidationError)
	require.True(t, ok)
	require.True(t, strings.HasPrefix(planErr.message, "invalid aggregation"))
	require.Len(t, planErr.children, 1)
	exprErr := planErr.children[0]
	require.True(t, strings.HasPrefix(exprErr.message, "alias used twice: value"))
}

func TestFilterBinaryExprLeftSideMustBeColumn(t *testing.T) {
	_, err := (&Builder{}).
		Scan(&mockTableProvider{dynparquet.NewSampleSchema()}, "table1").
		Filter(&BinaryExpr{
			Left:  Literal(5),
			Op:    OpEq,
			Right: Literal(4),
		}).
		Build()

	planErr, ok := err.(*PlanValidationError)
	require.True(t, ok)
	require.True(t, strings.HasPrefix(planErr.message, "invalid filter"))
	require.Len(t, planErr.children, 1)
	exprErr := planErr.children[0]
	require.True(t, strings.HasPrefix(exprErr.message, "left side of binary expression must be a column"))
}

func TestFilterBinaryExprColMustMatchLiteralType(t *testing.T) {
	_, err := (&Builder{}).
		Scan(&mockTableProvider{dynparquet.NewSampleSchema()}, "table1").
		Filter(&BinaryExpr{
			Left:  Col("example_type"),
			Op:    OpEq,
			Right: Literal(4.6),
		}).
		Build()

	require.NotNil(t, err)
	planErr, ok := err.(*PlanValidationError)
	require.True(t, ok)
	require.True(t, strings.HasPrefix(planErr.message, "invalid filter"))
	require.Len(t, planErr.children, 1)
	exprErr := planErr.children[0]
	require.True(t, strings.HasPrefix(exprErr.message, "incompatible types"))

	// check that it also works the other way around, can't compare number w/ string
	_, err = (&Builder{}).
		Scan(&mockTableProvider{dynparquet.NewSampleSchema()}, "table1").
		Filter(&BinaryExpr{
			Left:  Col("timestamp"),
			Op:    OpEq,
			Right: Literal("albert"),
		}).
		Build()
	require.NotNil(t, err)
	planErr, ok = err.(*PlanValidationError)
	require.True(t, ok)
	require.True(t, strings.HasPrefix(planErr.message, "invalid filter"))
	require.Len(t, planErr.children, 1)
	exprErr = planErr.children[0]
	require.True(t, strings.HasPrefix(exprErr.message, "incompatible types"))
}

func TestFilterAndExprEvaluatesEachAndedRule(t *testing.T) {
	_, err := (&Builder{}).
		Scan(&mockTableProvider{dynparquet.NewSampleSchema()}, "table1").
		Filter(And(
			&BinaryExpr{
				Left:  Col("example_type"),
				Op:    OpEq,
				Right: Literal(4),
			},
			&BinaryExpr{
				Left:  Literal("a"),
				Op:    OpEq,
				Right: Literal("b"),
			},
		)).
		Build()

	require.NotNil(t, err)
	require.NotNil(t, err)
	planErr, ok := err.(*PlanValidationError)
	require.True(t, ok)
	require.True(t, strings.HasPrefix(planErr.message, "invalid filter"))
	require.Len(t, planErr.children, 1)
	exprErr := planErr.children[0]

	require.True(t, strings.HasPrefix(exprErr.message, "invalid children:"))
	require.True(t, strings.Contains(exprErr.message, "left"))
	require.True(t, strings.Contains(exprErr.message, "right"))
	require.Len(t, exprErr.children, 2)

	leftErr := exprErr.children[0]
	require.True(t, strings.HasPrefix(leftErr.message, "incompatible types"))

	rightErr := exprErr.children[1]
	require.True(t, strings.HasPrefix(rightErr.message, "left side of binary expression must be a column"))
}
