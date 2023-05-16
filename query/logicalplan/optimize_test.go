package logicalplan

import (
	"testing"

	"github.com/polarsignals/frostdb/dynparquet"

	"github.com/apache/arrow/go/v12/arrow/scalar"
	"github.com/stretchr/testify/require"
)

func TestOptimizePhysicalProjectionPushDown(t *testing.T) {
	tableProvider := &mockTableProvider{schema: dynparquet.NewSampleSchema()}
	p, _ := (&Builder{}).
		Scan(tableProvider, "table1").
		Filter(Col("labels.test").Eq(Literal("abc"))).
		Aggregate(
			[]Expr{Sum(Col("value")).Alias("value_sum")},
			[]Expr{Col("stacktrace")},
		).
		Project(Col("stacktrace")).
		Build()

	optimizer := &PhysicalProjectionPushDown{}
	optimizer.Optimize(p)

	require.Equal(t, &TableScan{
		TableName:     "table1",
		TableProvider: tableProvider,
		// Only these columns are needed to compute the result. There can be
		// duplicates because the statements just add the matchers for the
		// columns they access. The optimizer could potentially deduplicate or
		// use a more efficient datastructure in the future.
		PhysicalProjection: []Expr{
			&Column{ColumnName: "stacktrace"},
			&Column{ColumnName: "stacktrace"},
			&Column{ColumnName: "value"},
			&Column{ColumnName: "labels.test"},
		},
	},
		// Projection -> Aggregate -> Filter -> TableScan
		p.Input.Input.Input.TableScan,
	)
}

func TestOptimizeDistinctPushDown(t *testing.T) {
	p, _ := (&Builder{}).
		Scan(nil, "table1").
		Distinct(Col("labels.test")).
		Build()

	optimizer := &DistinctPushDown{}
	p = optimizer.Optimize(p)

	require.Equal(t, &TableScan{
		TableName: "table1",
		Distinct: []Expr{
			&Column{ColumnName: "labels.test"},
		},
	},
		// Distinct -> TableScan
		p.Input.TableScan,
	)
}

func TestOptimizeFilterPushDown(t *testing.T) {
	tableProvider := &mockTableProvider{schema: dynparquet.NewSampleSchema()}
	p, _ := (&Builder{}).
		Scan(tableProvider, "table1").
		Filter(Col("labels.test").Eq(Literal("abc"))).
		Aggregate(
			[]Expr{Sum(Col("value")).Alias("value_sum")},
			[]Expr{Col("stacktrace")},
		).
		Project(Col("stacktrace")).
		Build()

	optimizer := &FilterPushDown{}
	optimizer.Optimize(p)

	require.Equal(t, &TableScan{
		TableName:     "table1",
		TableProvider: tableProvider,
		// Only these columns are needed to compute the result.
		Filter: &BinaryExpr{
			Left: &Column{ColumnName: "labels.test"},
			Op:   OpEq,
			Right: &LiteralExpr{
				Value: scalar.MakeScalar("abc"),
			},
		},
	},
		// Projection -> Aggregate -> Filter -> TableScan
		p.Input.Input.Input.TableScan,
	)
}

func TestRemoveProjectionAtRoot(t *testing.T) {
	p, _ := (&Builder{}).
		Scan(&mockTableProvider{schema: dynparquet.NewSampleSchema()}, "table1").
		Filter(Col("labels.test").Eq(Literal("abc"))).
		Aggregate(
			[]Expr{Sum(Col("value")).Alias("value_sum")},
			[]Expr{Col("stacktrace")},
		).
		Project(Col("stacktrace")).
		Build()

	p = removeProjection(p)

	require.True(t, p.Projection == nil)
}

func TestRemoveMiddleProjection(t *testing.T) {
	p, _ := (&Builder{}).
		Scan(&mockTableProvider{schema: dynparquet.NewSampleSchema()}, "table1").
		Filter(Col("labels.test").Eq(Literal("abc"))).
		Project(Col("stacktrace")).
		Aggregate(
			[]Expr{Sum(Col("value")).Alias("value_sum")},
			[]Expr{Col("stacktrace")},
		).
		Build()

	p = removeProjection(p)

	require.True(t, p.Input.Projection == nil)
}

func TestRemoveLowestProjection(t *testing.T) {
	p, _ := (&Builder{}).
		Scan(&mockTableProvider{schema: dynparquet.NewSampleSchema()}, "table1").
		Project(Col("stacktrace")).
		Filter(Col("labels.test").Eq(Literal("abc"))).
		Aggregate(
			[]Expr{Sum(Col("value")).Alias("value_sum")},
			[]Expr{Col("stacktrace")},
		).
		Build()

	p = removeProjection(p)

	require.True(t, p.Input.Input.Projection == nil)
}

func TestProjectionPushDown(t *testing.T) {
	p, _ := (&Builder{}).
		Scan(&mockTableProvider{schema: dynparquet.NewSampleSchema()}, "table1").
		Filter(Col("labels.test").Eq(Literal("abc"))).
		Aggregate(
			[]Expr{Sum(Col("value")).Alias("value_sum")},
			[]Expr{Col("stacktrace")},
		).
		Project(Col("labels")).
		Build()

	p = (&ProjectionPushDown{}).Optimize(p)

	require.True(t, p.Input.Input.Projection == nil)
}

func TestProjectionPushDownOfDistinct(t *testing.T) {
	p, _ := (&Builder{}).
		Scan(&mockTableProvider{schema: dynparquet.NewSampleSchema()}, "table1").
		Distinct(DynCol("labels")).
		Build()

	p = (&ProjectionPushDown{}).Optimize(p)

	require.True(t, p.Input.Projection != nil)
}

func TestAllOptimizers(t *testing.T) {
	tableProvider := &mockTableProvider{schema: dynparquet.NewSampleSchema()}
	p, _ := (&Builder{}).
		Scan(tableProvider, "table1").
		Filter(Col("labels.test").Eq(Literal("abc"))).
		Aggregate(
			[]Expr{Sum(Col("value")).Alias("value_sum")},
			[]Expr{Col("stacktrace")},
		).
		Project(Col("stacktrace")).
		Build()

	optimizers := []Optimizer{
		&PhysicalProjectionPushDown{},
		&FilterPushDown{},
		&DistinctPushDown{},
		&ProjectionPushDown{},
	}

	for _, optimizer := range optimizers {
		p = optimizer.Optimize(p)
	}

	require.Equal(t, &TableScan{
		TableName:     "table1",
		TableProvider: tableProvider,
		// Only these columns are needed to compute the result. There can be
		// duplicates because the statements just add the matchers for the
		// columns they access. The optimizer could potentially deduplicate or
		// use a more efficient datastructure in the future.
		PhysicalProjection: []Expr{
			&Column{ColumnName: "stacktrace"},
			&Column{ColumnName: "stacktrace"},
			&Column{ColumnName: "value"},
			&Column{ColumnName: "labels.test"},
		},
		Filter: &BinaryExpr{
			Left: &Column{ColumnName: "labels.test"},
			Op:   OpEq,
			Right: &LiteralExpr{
				Value: scalar.MakeScalar("abc"),
			},
		},
	},
		// Aggregate -> Filter -> Projection -> TableScan
		p.Input.Input.Input.TableScan,
	)
}
