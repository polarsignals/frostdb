package query

import (
	"context"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"go.opentelemetry.io/otel/trace"

	"github.com/polarsignals/frostdb/query/logicalplan"
	"github.com/polarsignals/frostdb/query/physicalplan"
)

type Builder interface {
	Aggregate(aggExpr logicalplan.Expr, groupExprs ...logicalplan.Expr) Builder
	Filter(expr logicalplan.Expr) Builder
	Distinct(expr ...logicalplan.Expr) Builder
	Project(projections ...logicalplan.Expr) Builder
	Execute(ctx context.Context, callback func(ctx context.Context, r arrow.Record) error) error
}

type LocalEngine struct {
	pool          memory.Allocator
	tracer        trace.Tracer
	tableProvider logicalplan.TableProvider
}

type Option func(*LocalEngine)

func WithTracer(tracer trace.Tracer) Option {
	return func(e *LocalEngine) {
		e.tracer = tracer
	}
}

func NewEngine(
	pool memory.Allocator,
	tableProvider logicalplan.TableProvider,
	options ...Option,
) *LocalEngine {
	e := &LocalEngine{
		pool:          pool,
		tracer:        trace.NewNoopTracerProvider().Tracer(""),
		tableProvider: tableProvider,
	}

	for _, option := range options {
		option(e)
	}

	return e
}

type LocalQueryBuilder struct {
	pool        memory.Allocator
	tracer      trace.Tracer
	planBuilder logicalplan.Builder
}

func (e *LocalEngine) ScanTable(name string) Builder {
	return LocalQueryBuilder{
		pool:        e.pool,
		tracer:      e.tracer,
		planBuilder: (&logicalplan.Builder{}).Scan(e.tableProvider, name),
	}
}

func (e *LocalEngine) ScanSchema(name string) Builder {
	return LocalQueryBuilder{
		pool:        e.pool,
		tracer:      e.tracer,
		planBuilder: (&logicalplan.Builder{}).ScanSchema(e.tableProvider, name),
	}
}

func (b LocalQueryBuilder) Aggregate(
	aggExpr logicalplan.Expr,
	groupExprs ...logicalplan.Expr,
) Builder {
	return LocalQueryBuilder{
		pool:        b.pool,
		tracer:      b.tracer,
		planBuilder: b.planBuilder.Aggregate(aggExpr, groupExprs...),
	}
}

func (b LocalQueryBuilder) Filter(
	expr logicalplan.Expr,
) Builder {
	return LocalQueryBuilder{
		pool:        b.pool,
		tracer:      b.tracer,
		planBuilder: b.planBuilder.Filter(expr),
	}
}

func (b LocalQueryBuilder) Distinct(
	expr ...logicalplan.Expr,
) Builder {
	return LocalQueryBuilder{
		pool:        b.pool,
		tracer:      b.tracer,
		planBuilder: b.planBuilder.Distinct(expr...),
	}
}

func (b LocalQueryBuilder) Project(
	projections ...logicalplan.Expr,
) Builder {
	return LocalQueryBuilder{
		pool:        b.pool,
		tracer:      b.tracer,
		planBuilder: b.planBuilder.Project(projections...),
	}
}

func (b LocalQueryBuilder) Execute(ctx context.Context, callback func(ctx context.Context, r arrow.Record) error) error {
	ctx, span := b.tracer.Start(ctx, "LocalQueryBuilder/Execute")
	defer span.End()

	logicalPlan, err := b.planBuilder.Build()
	if err != nil {
		return err
	}

	for _, optimizer := range logicalplan.DefaultOptimizers {
		logicalPlan = optimizer.Optimize(logicalPlan)
	}

	phyPlan, err := physicalplan.Build(
		ctx,
		b.pool,
		b.tracer,
		logicalPlan.InputSchema(),
		logicalPlan,
		callback,
	)
	if err != nil {
		return err
	}

	return phyPlan.Execute(ctx, b.pool, callback)
}
