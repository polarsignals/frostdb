package logicalplan

import (
	"testing"

	"github.com/apache/arrow/go/v8/arrow/scalar"
	"github.com/stretchr/testify/require"
)

func TestOptimizeProjectionPushDown(t *testing.T) {
	p := (&Builder{}).
		Scan(nil, "table1").
		Filter(Col("labels.test").Eq(Literal("abc"))).
		Aggregate(
			Sum(Col("value")).Alias("value_sum"),
			Col("stacktrace"),
		).
		Project("stacktrace").
		Build()

	optimizer := &ProjectionPushDown{}
	optimizer.Optimize(p)

	// Projection -> Aggregate -> Filter -> TableScan
	require.Equal(t, &TableScan{
		TableName: "table1",
		// Only these columns are needed to compute the result. There can be
		// duplicates because the statements just add the matchers for the
		// columns they access. The optimizer could potentially deduplicate or
		// use a more efficient datastructure in the future.
		Projection: []ColumnMatcher{
			StaticColumnMatcher{ColumnName: "stacktrace"},
			StaticColumnMatcher{ColumnName: "stacktrace"},
			StaticColumnMatcher{ColumnName: "value"},
			StaticColumnMatcher{ColumnName: "labels.test"},
		},
	}, p.Input.Input.Input.TableScan)
}

func TestOptimizeDistinctPushDown(t *testing.T) {
	p := (&Builder{}).
		Scan(nil, "table1").
		Distinct(Col("labels.test")).
		Build()

	optimizer := &DistinctPushDown{}
	optimizer.Optimize(p)

	// Projection -> Aggregate -> Filter -> TableScan
	require.Equal(t, &TableScan{
		TableName: "table1",
		Distinct: []ColumnMatcher{
			StaticColumnMatcher{ColumnName: "labels.test"},
		},
	}, p.Input.TableScan)
}

func TestOptimizeFilterPushDown(t *testing.T) {
	p := (&Builder{}).
		Scan(nil, "table1").
		Filter(Col("labels.test").Eq(Literal("abc"))).
		Aggregate(
			Sum(Col("value")).Alias("value_sum"),
			Col("stacktrace"),
		).
		Project("stacktrace").
		Build()

	optimizer := &FilterPushDown{}
	optimizer.Optimize(p)

	// Projection -> Aggregate -> Filter -> TableScan
	require.Equal(t, &TableScan{
		TableName: "table1",
		// Only these columns are needed to compute the result.
		Filter: BinaryExpr{
			Left: Column{ColumnName: "labels.test"},
			Op:   EqOp,
			Right: LiteralExpr{
				Value: scalar.MakeScalar("abc"),
			},
		},
	}, p.Input.Input.Input.TableScan)
}

func TestAllOptimizers(t *testing.T) {
	p := (&Builder{}).
		Scan(nil, "table1").
		Filter(Col("labels.test").Eq(Literal("abc"))).
		Aggregate(
			Sum(Col("value")).Alias("value_sum"),
			Col("stacktrace"),
		).
		Project("stacktrace").
		Build()

	optimizers := []Optimizer{
		&ProjectionPushDown{},
		&FilterPushDown{},
		&DistinctPushDown{},
	}

	for _, optimizer := range optimizers {
		optimizer.Optimize(p)
	}

	// Projection -> Aggregate -> Filter -> TableScan
	require.Equal(t, &TableScan{
		TableName: "table1",
		// Only these columns are needed to compute the result. There can be
		// duplicates because the statements just add the matchers for the
		// columns they access. The optimizer could potentially deduplicate or
		// use a more efficient datastructure in the future.
		Projection: []ColumnMatcher{
			StaticColumnMatcher{ColumnName: "stacktrace"},
			StaticColumnMatcher{ColumnName: "stacktrace"},
			StaticColumnMatcher{ColumnName: "value"},
			StaticColumnMatcher{ColumnName: "labels.test"},
		},
		Filter: BinaryExpr{
			Left: Column{ColumnName: "labels.test"},
			Op:   EqOp,
			Right: LiteralExpr{
				Value: scalar.MakeScalar("abc"),
			},
		},
	}, p.Input.Input.Input.TableScan)
}
