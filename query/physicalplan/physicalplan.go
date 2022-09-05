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

// TODO: Make this smarter and concurrent.
var concurrencyHardcoded = 1

type PhysicalPlan interface {
	Callbacks() []logicalplan.Callback
	SetNext(next PhysicalPlan)
	Finish(ctx context.Context) error
	Draw() *Diagram
}

type ScanPhysicalPlan interface {
	Execute(ctx context.Context, pool memory.Allocator) error
	Concurrency() uint
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
	callbacks []logicalplan.Callback
	scan      ScanPhysicalPlan
}

func (e *OutputPlan) Draw() *Diagram {
	// Doesn't change anything anymore as it's the root of the plan.
	return &Diagram{}
}

func (e *OutputPlan) DrawString() string {
	return e.scan.Draw().String()
}

func (e *OutputPlan) Callbacks() []logicalplan.Callback {
	return e.callbacks
}

func (e *OutputPlan) Finish(ctx context.Context) error {
	return nil
}

func (e *OutputPlan) SetNext(next PhysicalPlan) {
	// OutputPlan should be the last step.
	// If this gets called we're doing something wrong.
	panic("bug in builder! output plan should not have a next plan!")
}

func (e *OutputPlan) Execute(ctx context.Context, pool memory.Allocator) error {
	return e.scan.Execute(ctx, pool)
}

type TableScan struct {
	tracer      trace.Tracer
	options     *logicalplan.TableScan
	next        PhysicalPlan
	concurrency uint
}

func (s *TableScan) Concurrency() uint {
	return s.concurrency
}

func (s *TableScan) Draw() *Diagram {
	var child *Diagram
	if s.next != nil {
		child = s.next.Draw()
	}
	details := fmt.Sprintf("TableScan(%dx)", len(s.next.Callbacks()))
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
			logicalplan.IterOptions{
				PhysicalProjection: s.options.PhysicalProjection,
				Projection:         s.options.Projection,
				Filter:             s.options.Filter,
				DistinctColumns:    s.options.Distinct,
			},
		)
		if err != nil {
			return err
		}

		return table.Iterator(
			ctx,
			tx,
			pool,
			schema,
			logicalplan.IterOptions{
				PhysicalProjection: s.options.PhysicalProjection,
				Projection:         s.options.Projection,
				Filter:             s.options.Filter,
				DistinctColumns:    s.options.Distinct,
			},
			s.next.Callbacks(),
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

func (s *SchemaScan) Concurrency() uint {
	return 0
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
			logicalplan.IterOptions{
				PhysicalProjection: s.options.PhysicalProjection,
				Projection:         s.options.Projection,
				Filter:             s.options.Filter,
				DistinctColumns:    s.options.Distinct,
			},
			s.next.Callbacks(),
		)
	})
	if err != nil {
		return err
	}

	return s.next.Finish(ctx)
}

func Build(
	ctx context.Context,
	pool memory.Allocator,
	tracer trace.Tracer,
	s *dynparquet.Schema,
	plan *logicalplan.LogicalPlan,
	callback func(ctx context.Context, record arrow.Record) error,
) (*OutputPlan, error) {
	_, span := tracer.Start(ctx, "PhysicalPlan/Build")
	defer span.End()

	// TODO(metalmatze): Right now we create all these callbacks but only ever use the first one (in the Table/Iterator)
	// We also need to inject a Synchronizer here by default, to not call the passed in callback concurrently.
	callbacks := make([]logicalplan.Callback, 0, concurrencyHardcoded)
	for i := 0; i < concurrencyHardcoded; i++ {
		callbacks = append(callbacks, func(ctx context.Context, r arrow.Record) error {
			return callback(ctx, r)
		})
	}

	outputPlan := &OutputPlan{callbacks: callbacks}
	var (
		err  error
		prev PhysicalPlan = outputPlan
		tail PhysicalPlan // the last callback to get the arrow record
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
			var concurrency uint = 1
			if plan.TableScan.Concurrent {
				// TODO: Be smarter about the wanted concurrency
				concurrency = uint(concurrencyHardcoded)
			}

			outputPlan.scan = &TableScan{
				tracer:      tracer,
				options:     plan.TableScan,
				next:        prev,
				concurrency: concurrency,
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

		if tail == nil {
			tail = phyPlan
		}

		phyPlan.SetNext(prev)
		prev = phyPlan
		return true
	}))

	span.SetAttributes(attribute.String("plan", outputPlan.scan.Draw().String()))
	// fmt.Println(outputPlan.scan.Draw().String()) // Comment out for debugging without tracing

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
