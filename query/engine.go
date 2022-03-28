package query

import (
	"github.com/apache/arrow/go/v7/arrow"
	"github.com/apache/arrow/go/v7/arrow/memory"

	"github.com/polarsignals/arcticdb/dynparquet"
	"github.com/polarsignals/arcticdb/query/logicalplan"
	"github.com/polarsignals/arcticdb/query/physicalplan"
)

type Engine struct {
	pool          memory.Allocator
	tableProvider logicalplan.TableProvider
}

func NewEngine(
	pool memory.Allocator,
	tableProvider logicalplan.TableProvider,
) *Engine {
	return &Engine{
		pool:          pool,
		tableProvider: tableProvider,
	}
}

type Builder struct {
	pool        memory.Allocator
	planBuilder logicalplan.Builder
}

func (e *Engine) ScanTable(name string) Builder {
	return Builder{
		pool:        e.pool,
		planBuilder: (&logicalplan.Builder{}).Scan(e.tableProvider, name),
	}
}

func (b Builder) Aggregate(
	aggExpr logicalplan.Expr,
	groupExprs ...logicalplan.ColumnExpr,
) Builder {
	return Builder{
		pool:        b.pool,
		planBuilder: b.planBuilder.Aggregate(aggExpr, groupExprs...),
	}
}

func (b Builder) Filter(
	expr logicalplan.Expr,
) Builder {
	return Builder{
		pool:        b.pool,
		planBuilder: b.planBuilder.Filter(expr),
	}
}

func (b Builder) Distinct(
	expr ...logicalplan.ColumnExpr,
) Builder {
	return Builder{
		pool:        b.pool,
		planBuilder: b.planBuilder.Distinct(expr...),
	}
}

func (b Builder) Project(
	projections ...string,
) Builder {
	return Builder{
		pool:        b.pool,
		planBuilder: b.planBuilder.Project(projections...),
	}
}

func (b Builder) Execute(callback func(r arrow.Record) error) error {
	logicalPlan := b.planBuilder.Build()

	optimizers := []logicalplan.Optimizer{
		&logicalplan.ProjectionPushDown{},
		&logicalplan.FilterPushDown{},
	}

	for _, optimizer := range optimizers {
		optimizer.Optimize(logicalPlan)
	}

	phyPlan, err := physicalplan.Build(
		b.pool,
		dynparquet.NewSampleSchema(),
		logicalPlan,
	)
	if err != nil {
		return err
	}

	return phyPlan.Execute(b.pool, callback)
}
