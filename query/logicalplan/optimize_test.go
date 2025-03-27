package logicalplan

import (
	"testing"

	"github.com/polarsignals/frostdb/dynparquet"

	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/stretchr/testify/require"
)

func TestOptimizePhysicalProjectionPushDown(t *testing.T) {
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
	require.NoError(t, err)

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
			&Column{ColumnName: "value"},
			DynCol(hashedMatch),
			&Column{ColumnName: "labels.test"},
		},
	},
		// Projection -> Aggregate -> Filter -> TableScan
		p.Input.Input.Input.TableScan,
	)
}

func TestOptimizeDistinctPushDown(t *testing.T) {
	tableProvider := &mockTableProvider{schema: dynparquet.NewSampleSchema()}
	p, err := (&Builder{}).
		Scan(tableProvider, "table1").
		Distinct(Col("labels.test")).
		Build()
	require.NoError(t, err)

	optimizer := &DistinctPushDown{}
	p = optimizer.Optimize(p)

	require.Equal(t,
		[]Expr{&Column{ColumnName: "labels.test"}},
		// Distinct -> TableScan
		p.Input.Input.TableScan.Distinct,
	)
}

func TestOptimizeFilterPushDown(t *testing.T) {
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
	require.NoError(t, err)

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

func TestProjectionPushDown(t *testing.T) {
	p, err := (&Builder{}).
		Scan(&mockTableProvider{schema: dynparquet.NewSampleSchema()}, "table1").
		Filter(Col("labels.test").Eq(Literal("abc"))).
		Aggregate(
			[]*AggregationFunction{Sum(Col("value"))},
			[]Expr{Col("stacktrace")},
		).
		Build()
	require.NoError(t, err)

	p = (&PhysicalProjectionPushDown{}).Optimize(p)

	require.Equal(t, []Expr{
		Col("stacktrace"),
		Col("value"),
		DynCol("hashed"),
		Col("labels.test"),
	}, p.Input.Input.TableScan.PhysicalProjection)
}

func TestProjectionPushDownReset(t *testing.T) {
	p, err := (&Builder{}).
		Scan(&mockTableProvider{schema: dynparquet.NewSampleSchema()}, "table1").
		Filter(Col("labels.test").Eq(Literal("abc"))).
		Aggregate(
			[]*AggregationFunction{Sum(Col("value"))},
			[]Expr{Col("stacktrace")},
		).
		Project(Col("test")).
		Build()
	require.NoError(t, err)

	p = (&PhysicalProjectionPushDown{}).Optimize(p)

	require.Equal(t, []Expr{
		Col("stacktrace"),
		Col("value"),
		DynCol("hashed"),
		Col("labels.test"),
	}, p.Input.Input.Input.TableScan.PhysicalProjection)
}

func TestProjectionPushDownOfDistinct(t *testing.T) {
	p, err := (&Builder{}).
		Scan(&mockTableProvider{schema: dynparquet.NewSampleSchema()}, "table1").
		Distinct(DynCol("labels")).
		Build()
	require.NoError(t, err)

	p = (&PhysicalProjectionPushDown{}).Optimize(p)

	require.Equal(t, []Expr{DynCol("labels")}, p.Input.Input.TableScan.PhysicalProjection)
}

func TestAllOptimizers(t *testing.T) {
	tableProvider := &mockTableProvider{schema: dynparquet.NewSampleSchema()}
	p, err := (&Builder{}).
		Scan(tableProvider, "table1").
		Filter(Col("labels.test").Eq(Literal("abc"))).
		Aggregate(
			[]*AggregationFunction{Sum(Col("value"))},
			[]Expr{Col("stacktrace")},
		).
		Project(Col("stacktrace")).
		Build()
	require.NoError(t, err)

	optimizers := DefaultOptimizers()

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
			&Column{ColumnName: "value"},
			DynCol(hashedMatch),
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

func TestAggFuncPushDown(t *testing.T) {
	t.Run("GlobalAgg", func(t *testing.T) {
		tableProvider := &mockTableProvider{schema: dynparquet.NewSampleSchema()}
		p, err := (&Builder{}).
			Scan(tableProvider, "table1").
			Aggregate(
				[]*AggregationFunction{Max(Col("value"))},
				nil,
			).
			Build()
		require.NoError(t, err)

		p = (&AggFuncPushDown{}).Optimize(p)
		// Aggregation should still happen.
		require.NotNil(t, p.Aggregation)
		require.Equal(t,
			&TableScan{
				TableName:     "table1",
				TableProvider: tableProvider,
				Filter: &AggregationFunction{
					Func: AggFuncMax,
					Expr: &Column{ColumnName: "value"},
				},
			},
			p.Input.TableScan,
		)
	})
	t.Run("DontPushWithGroupExprs", func(t *testing.T) {
		tableProvider := &mockTableProvider{schema: dynparquet.NewSampleSchema()}
		p, err := (&Builder{}).
			Scan(tableProvider, "table1").
			Aggregate(
				[]*AggregationFunction{Max(Col("value"))},
				[]Expr{Col("stacktrace")},
			).
			Build()
		require.NoError(t, err)

		p = (&AggFuncPushDown{}).Optimize(p)
		// Aggregation should still happen.
		require.NotNil(t, p.Aggregation)
		require.Equal(t,
			&TableScan{
				TableName:     "table1",
				TableProvider: tableProvider,
			},
			p.Input.TableScan,
		)
	})
	t.Run("DontPushWithFilter", func(t *testing.T) {
		tableProvider := &mockTableProvider{schema: dynparquet.NewSampleSchema()}
		p, err := (&Builder{}).
			Scan(tableProvider, "table1").
			Filter(Col("labels.test").Eq(Literal("abc"))).
			Aggregate(
				[]*AggregationFunction{Max(Col("value"))},
				nil,
			).
			Build()
		require.NoError(t, err)

		p = (&AggFuncPushDown{}).Optimize(p)
		// Aggregation should still happen.
		require.NotNil(t, p.Aggregation)
		require.Equal(t,
			&TableScan{
				TableName:     "table1",
				TableProvider: tableProvider,
			},
			p.Input.Input.TableScan,
		)
	})
}
