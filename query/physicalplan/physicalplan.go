package physicalplan

import (
	"context"
	"errors"
	"fmt"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

type PhysicalPlan interface {
	Callback(ctx context.Context, r arrow.Record) error
	Finish(ctx context.Context) error
	SetNext(next PhysicalPlan)
	Draw() *Diagram
}

type ScanPhysicalPlan interface {
	Execute(ctx context.Context, pool memory.Allocator) error
	Draw() *Diagram
}

type PrePlanVisitorFunc func(plan *logicalplan.LogicalPlan) bool

func (f PrePlanVisitorFunc) PreVisit(plan *logicalplan.LogicalPlan) bool {
	return f(plan)
}

func (f PrePlanVisitorFunc) PostVisit(plan *logicalplan.LogicalPlan) bool {
	return false
}

type PostPlanVisitorFunc func(plan *logicalplan.LogicalPlan) bool

func (f PostPlanVisitorFunc) PreVisit(plan *logicalplan.LogicalPlan) bool {
	return true
}

func (f PostPlanVisitorFunc) PostVisit(plan *logicalplan.LogicalPlan) bool {
	return f(plan)
}

type OutputPlan struct {
	callback func(ctx context.Context, r arrow.Record) error
	scan     ScanPhysicalPlan
}

func (e *OutputPlan) Draw() *Diagram {
	// Doesn't change anything anymore as it's the root of the plan.
	return &Diagram{}
}

func (e *OutputPlan) DrawString() string {
	return e.scan.Draw().String()
}

func (e *OutputPlan) Callback(ctx context.Context, r arrow.Record) error {
	return e.callback(ctx, r)
}

func (e *OutputPlan) SetNextCallback(next func(ctx context.Context, r arrow.Record) error) {
	e.callback = next
}

func (e *OutputPlan) Finish(ctx context.Context) error {
	return nil
}

func (e *OutputPlan) SetNext(next PhysicalPlan) {
	// OutputPlan should be the last step.
	// If this gets called we're doing something wrong.
	panic("bug in builder! output plan should not have a next plan!")
}

func (e *OutputPlan) Execute(ctx context.Context, pool memory.Allocator, callback func(ctx context.Context, r arrow.Record) error) error {
	e.callback = callback
	return e.scan.Execute(ctx, pool)
}

type TableScan struct {
	tracer  trace.Tracer
	options *logicalplan.TableScan
	next    PhysicalPlan
}

func (s *TableScan) Draw() *Diagram {
	var child *Diagram
	if s.next != nil {
		child = s.next.Draw()
	}
	details := fmt.Sprintf("TableScan")
	return &Diagram{Details: details, Child: child}
}

func (s *TableScan) Execute(ctx context.Context, pool memory.Allocator) error {
	ctx, span := s.tracer.Start(ctx, "TableScan/Execute")
	defer span.End()

	table := s.options.TableProvider.GetTable(s.options.TableName)
	if table == nil {
		return errors.New("table not found")
	}

	err := table.View(ctx, func(ctx context.Context, tx uint64) error {
		schema, err := table.ArrowSchema(
			ctx,
			tx,
			pool,
			s.options.PhysicalProjection,
			s.options.Projection,
			s.options.Filter,
			s.options.Distinct,
		)
		if err != nil {
			return err
		}

		return table.Iterator(
			ctx,
			tx,
			pool,
			schema,
			s.options.PhysicalProjection,
			s.options.Projection,
			s.options.Filter,
			s.options.Distinct,
			s.next.Callback,
		)
	})
	if err != nil {
		return err
	}

	return s.next.Finish(ctx)
}

type SchemaScan struct {
	tracer  trace.Tracer
	options *logicalplan.SchemaScan
	next    PhysicalPlan
}

func (s *SchemaScan) Draw() *Diagram {
	var child *Diagram
	if s.next != nil {
		child = s.next.Draw()
	}
	return &Diagram{Details: "SchemaScan", Child: child}
}

func (s *SchemaScan) Execute(ctx context.Context, pool memory.Allocator) error {
	table := s.options.TableProvider.GetTable(s.options.TableName)
	if table == nil {
		return errors.New("table not found")
	}
	err := table.View(ctx, func(ctx context.Context, tx uint64) error {
		return table.SchemaIterator(
			ctx,
			tx,
			pool,
			s.options.PhysicalProjection,
			s.options.Projection,
			s.options.Filter,
			s.options.Distinct,
			s.next.Callback,
		)
	})
	if err != nil {
		return err
	}

	return s.next.Finish(ctx)
}

func Build(ctx context.Context, pool memory.Allocator, tracer trace.Tracer, s *dynparquet.Schema, plan *logicalplan.LogicalPlan) (*OutputPlan, error) {
	_, span := tracer.Start(ctx, "PhysicalPlan/Build")
	defer span.End()

	outputPlan := &OutputPlan{}
	var (
		err  error
		prev PhysicalPlan = outputPlan
	)

	plan.Accept(PrePlanVisitorFunc(func(plan *logicalplan.LogicalPlan) bool {
		var phyPlan PhysicalPlan
		switch {
		case plan.SchemaScan != nil:
			outputPlan.scan = &SchemaScan{
				tracer:  tracer,
				options: plan.SchemaScan,
				next:    prev,
			}
			return false
		case plan.TableScan != nil:
			outputPlan.scan = &TableScan{
				tracer:  tracer,
				options: plan.TableScan,
				next:    prev,
			}
			return false
		case plan.Projection != nil:
			phyPlan, err = Project(pool, tracer, plan.Projection.Exprs)
		case plan.Distinct != nil:
			phyPlan = Distinct(pool, tracer, plan.Distinct.Exprs)
		case plan.Filter != nil:
			phyPlan, err = Filter(pool, tracer, plan.Filter.Expr)
		case plan.Aggregation != nil:
			phyPlan, err = Aggregate(pool, tracer, s.ParquetSchema(), plan.Aggregation)
		default:
			panic("Unsupported plan")
		}

		if err != nil {
			return false
		}

		phyPlan.SetNext(prev)
		prev = phyPlan
		return true
	}))

	outputPlan.SetNextCallback(prev.Callback)
	span.SetAttributes(attribute.String("plan", outputPlan.scan.Draw().String()))

	return outputPlan, err
}

type Diagram struct {
	Details string
	Child   *Diagram
}

func (d *Diagram) String() string {
	if d.Child == nil {
		return d.Details
	}
	child := d.Child.String()
	if child == "" {
		return d.Details
	}
	return d.Details + " - " + child
}
