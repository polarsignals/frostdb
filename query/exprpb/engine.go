package exprpb

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	pb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/storage/v1alpha1"
	"github.com/polarsignals/frostdb/query/logicalplan"
	"github.com/polarsignals/frostdb/query/physicalplan"
)

type ProtoEngine struct {
	pool          memory.Allocator
	tracer        trace.Tracer
	tableProvider logicalplan.TableProvider
	execOpts      []physicalplan.Option
}

type Option func(*ProtoEngine)

func WithTracer(tracer trace.Tracer) Option {
	return func(e *ProtoEngine) {
		e.tracer = tracer
	}
}

func WithPhysicalplanOptions(opts ...physicalplan.Option) Option {
	return func(e *ProtoEngine) {
		e.execOpts = opts
	}
}

func NewEngine(
	pool memory.Allocator,
	tableProvider logicalplan.TableProvider,
	options ...Option,
) *ProtoEngine {
	e := &ProtoEngine{
		pool:          pool,
		tracer:        noop.NewTracerProvider().Tracer(""),
		tableProvider: tableProvider,
	}

	for _, option := range options {
		option(e)
	}

	return e
}

type ProtoQueryBuilder struct {
	pool          memory.Allocator
	tracer        trace.Tracer
	LogicalPlan   *logicalplan.LogicalPlan
	execOpts      []physicalplan.Option
	tableProvider logicalplan.TableProvider
}

func (e *ProtoEngine) FromProto(root *pb.PlanNode) (ProtoQueryBuilder, error) {
	pqb := ProtoQueryBuilder{
		pool:          e.pool,
		tracer:        e.tracer,
		execOpts:      e.execOpts,
		tableProvider: e.tableProvider,
	}
	builder, err := pqb.planFromProto(root)
	if err != nil {
		return pqb, err
	}
	pqb.LogicalPlan, err = builder.Build()
	if err != nil {
		return pqb, err
	}

	return pqb, nil
}

func (qb ProtoQueryBuilder) planFromProto(plan *pb.PlanNode) (logicalplan.Builder, error) {
	var b logicalplan.Builder
	// First convert the next plan node.
	if plan.GetNext() != nil {
		var err error
		b, err = qb.planFromProto(plan.GetNext())
		if err != nil {
			return b, err
		}
	} else {
		b = logicalplan.Builder{}
	}

	switch {
	case plan.GetSpec().GetSchemaScan() != nil:
		b = b.ScanSchema(qb.tableProvider, plan.GetSpec().GetSchemaScan().GetBase().GetTable())
	case plan.GetSpec().GetTableScan() != nil:
		b = b.Scan(qb.tableProvider, plan.GetSpec().GetTableScan().GetBase().GetTable())
	case plan.GetSpec().GetFilter() != nil:
		expr, err := ExprFromProto(plan.GetSpec().GetFilter().GetExpr())
		if err != nil {
			return b, fmt.Errorf("failed to convert expr from proto: %v", err)
		}
		b = b.Filter(expr)
	case plan.GetSpec().GetDistinct() != nil:
		exprs, err := ExprsFromProtos(plan.GetSpec().GetDistinct().GetExprs())
		if err != nil {
			return b, fmt.Errorf("failed to convert exprs from proto: %v", err)
		}
		b = b.Distinct(exprs...)
	case plan.GetSpec().GetProjection() != nil:
		exprs, err := ExprsFromProtos(plan.GetSpec().GetProjection().GetExprs())
		if err != nil {
			return b, fmt.Errorf("failed to convert exprs from proto: %v", err)
		}
		b = b.Project(exprs...)
	case plan.GetSpec().GetLimit() != nil:
		expr, err := ExprFromProto(plan.GetSpec().GetLimit().GetExpr())
		if err != nil {
			return b, fmt.Errorf("failed to convert expr from proto: %v", err)
		}
		b = b.Limit(expr)
	case plan.GetSpec().GetAggregation() != nil:
		exprs, err := ExprsFromProtos(plan.GetSpec().GetAggregation().GetAggExprs())
		if err != nil {
			return b, fmt.Errorf("failed to convert exprs from proto: %v", err)
		}
		groupExprs, err := ExprsFromProtos(plan.GetSpec().GetAggregation().GetGroupExprs())
		if err != nil {
			return b, fmt.Errorf("failed to convert exprs from proto: %v", err)
		}

		aggExprs := make([]*logicalplan.AggregationFunction, 0, len(exprs))
		for _, expr := range exprs {
			aggExprs = append(aggExprs, expr.(*logicalplan.AggregationFunction))
		}

		b.Aggregate(aggExprs, groupExprs)
	}

	return b, nil
}

func (qb ProtoQueryBuilder) Execute(ctx context.Context, callback func(ctx context.Context, r arrow.Record) error) error {
	ctx, span := qb.tracer.Start(ctx, "ProtoEngine/Execute")
	defer span.End()

	phyPlan, err := qb.buildPhysical(ctx)
	if err != nil {
		return err
	}

	return phyPlan.Execute(ctx, qb.pool, callback)
}

func (qb ProtoQueryBuilder) buildPhysical(ctx context.Context) (*physicalplan.OutputPlan, error) {
	for _, optimizer := range logicalplan.DefaultOptimizers() {
		qb.LogicalPlan = optimizer.Optimize(qb.LogicalPlan)
	}

	return physicalplan.Build(
		ctx,
		qb.pool,
		qb.tracer,
		qb.LogicalPlan.InputSchema(),
		qb.LogicalPlan,
		qb.execOpts...,
	)
}

func (qb ProtoQueryBuilder) Explain(ctx context.Context) (string, error) {
	phyPlan, err := qb.buildPhysical(ctx)
	if err != nil {
		return "", err
	}
	return phyPlan.DrawString(), nil
}
