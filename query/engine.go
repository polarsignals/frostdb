package query

import (
	"context"

	"go.opentelemetry.io/otel/trace/noop"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"go.opentelemetry.io/otel/trace"

	"github.com/polarsignals/frostdb/query/logicalplan"
	"github.com/polarsignals/frostdb/query/physicalplan"
)

type Builder interface {
	Aggregate(aggExpr []*logicalplan.AggregationFunction, groupExprs []logicalplan.Expr) Builder
	Filter(expr logicalplan.Expr) Builder
	Distinct(expr ...logicalplan.Expr) Builder
	Project(projections ...logicalplan.Expr) Builder
	Execute(ctx context.Context, callback func(ctx context.Context, r arrow.Record) error) error
	Explain(ctx context.Context) (string, error)
}

type LocalEngine struct {
	pool          memory.Allocator
	tracer        trace.Tracer
	tableProvider logicalplan.TableProvider
	execOpts      []physicalplan.Option
}

type Option func(*LocalEngine)

func WithTracer(tracer trace.Tracer) Option {
	return func(e *LocalEngine) {
		e.tracer = tracer
	}
}

func WithPhysicalplanOptions(opts ...physicalplan.Option) Option {
	return func(e *LocalEngine) {
		e.execOpts = opts
	}
}

func NewEngine(
	pool memory.Allocator,
	tableProvider logicalplan.TableProvider,
	options ...Option,
) *LocalEngine {
	e := &LocalEngine{
		pool:          pool,
		tracer:        noop.NewTracerProvider().Tracer(""),
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
	execOpts    []physicalplan.Option
}

func (e *LocalEngine) ScanTable(name string) Builder {
	return LocalQueryBuilder{
		pool:        e.pool,
		tracer:      e.tracer,
		planBuilder: (&logicalplan.Builder{}).Scan(e.tableProvider, name),
		execOpts:    e.execOpts,
	}
}

func (e *LocalEngine) ScanSchema(name string) Builder {
	return LocalQueryBuilder{
		pool:        e.pool,
		tracer:      e.tracer,
		planBuilder: (&logicalplan.Builder{}).ScanSchema(e.tableProvider, name),
		execOpts:    e.execOpts,
	}
}

func (b LocalQueryBuilder) Aggregate(
	aggExpr []*logicalplan.AggregationFunction,
	groupExprs []logicalplan.Expr,
) Builder {
	return LocalQueryBuilder{
		pool:        b.pool,
		tracer:      b.tracer,
		planBuilder: b.planBuilder.Aggregate(aggExpr, groupExprs),
		execOpts:    b.execOpts,
	}
}

func (b LocalQueryBuilder) Filter(
	expr logicalplan.Expr,
) Builder {
	return LocalQueryBuilder{
		pool:        b.pool,
		tracer:      b.tracer,
		planBuilder: b.planBuilder.Filter(expr),
		execOpts:    b.execOpts,
	}
}

func (b LocalQueryBuilder) Distinct(
	expr ...logicalplan.Expr,
) Builder {
	return LocalQueryBuilder{
		pool:        b.pool,
		tracer:      b.tracer,
		planBuilder: b.planBuilder.Distinct(expr...),
		execOpts:    b.execOpts,
	}
}

func (b LocalQueryBuilder) Project(
	projections ...logicalplan.Expr,
) Builder {
	return LocalQueryBuilder{
		pool:        b.pool,
		tracer:      b.tracer,
		planBuilder: b.planBuilder.Project(projections...),
		execOpts:    b.execOpts,
	}
}

func (b LocalQueryBuilder) Execute(ctx context.Context, callback func(ctx context.Context, r arrow.Record) error) error {
	ctx, span := b.tracer.Start(ctx, "LocalQueryBuilder/Execute")
	defer span.End()

	phyPlan, err := b.buildPhysical(ctx)
	if err != nil {
		return err
	}

	return phyPlan.Execute(ctx, b.pool, callback)
}

func (b LocalQueryBuilder) Explain(ctx context.Context) (string, error) {
	phyPlan, err := b.buildPhysical(ctx)
	if err != nil {
		return "", err
	}
	return phyPlan.DrawString(), nil
}

func (b LocalQueryBuilder) buildPhysical(ctx context.Context) (*physicalplan.OutputPlan, error) {
	logicalPlan, err := b.planBuilder.Build()
	if err != nil {
		return nil, err
	}

	for _, optimizer := range logicalplan.DefaultOptimizers() {
		logicalPlan = optimizer.Optimize(logicalPlan)
	}

	return physicalplan.Build(
		ctx,
		b.pool,
		b.tracer,
		logicalPlan.InputSchema(),
		logicalPlan,
		b.execOpts...,
	)
}
