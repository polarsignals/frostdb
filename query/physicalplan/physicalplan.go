package physicalplan

import (
	"context"
	"fmt"
	"runtime"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

// TODO: Make this smarter.
var concurrencyHardcoded = runtime.NumCPU()

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
	plans   []PhysicalPlan
}

func (s *TableScan) Draw() *Diagram {
	//var child *Diagram
	//if s.plans != nil {
	//	child = s.plans.Draw()
	//}
	details := fmt.Sprintf("TableScan")
	return &Diagram{Details: details}
}

func (s *TableScan) Execute(ctx context.Context, pool memory.Allocator) error {
	ctx, span := s.tracer.Start(ctx, "TableScan/Execute")
	defer span.End()

	table, err := s.options.TableProvider.GetTable(s.options.TableName)
	if table == nil || err != nil {
		return fmt.Errorf("table not found: %w", err)
	}

	callbacks := make([]logicalplan.Callback, 0, len(s.plans))
	for _, plan := range s.plans {
		callbacks = append(callbacks, plan.Callback)
	}

	err = table.View(ctx, func(ctx context.Context, tx uint64) error {
		return table.Iterator(
			ctx,
			tx,
			pool,
			logicalplan.IterOptions{
				PhysicalProjection: s.options.PhysicalProjection,
				Projection:         s.options.Projection,
				Filter:             s.options.Filter,
				DistinctColumns:    s.options.Distinct,
			},
			callbacks,
		)
	})
	if err != nil {
		return err
	}

	errg, ctx := errgroup.WithContext(ctx)

	for _, plan := range s.plans {
		plan := plan
		errg.Go(func() error {
			return plan.Finish(ctx)
		})
	}

	return errg.Wait()
}

type SchemaScan struct {
	tracer  trace.Tracer
	options *logicalplan.SchemaScan
	plans   []PhysicalPlan
}

func (s *SchemaScan) Draw() *Diagram {
	//var children []*Diagram
	//for _, plan := range s.plans {
	//	children = append(children, plan.Draw())
	//}
	return &Diagram{Details: "SchemaScan"}
}

func (s *SchemaScan) Execute(ctx context.Context, pool memory.Allocator) error {
	table, err := s.options.TableProvider.GetTable(s.options.TableName)
	if table == nil || err != nil {
		return fmt.Errorf("table not found: %w", err)
	}

	callbacks := make([]logicalplan.Callback, 0, len(s.plans))
	for _, plan := range s.plans {
		callbacks = append(callbacks, plan.Callback)
	}

	err = table.View(ctx, func(ctx context.Context, tx uint64) error {
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
			callbacks,
		)
	})
	if err != nil {
		return err
	}

	errg, ctx := errgroup.WithContext(ctx)

	for _, plan := range s.plans {
		plan := plan
		errg.Go(func() error {
			return plan.Finish(ctx)
		})
	}

	return errg.Wait()
}

func Build(ctx context.Context, pool memory.Allocator, tracer trace.Tracer, s *dynparquet.Schema, plan *logicalplan.LogicalPlan) (*OutputPlan, error) {
	_, span := tracer.Start(ctx, "PhysicalPlan/Build")
	defer span.End()

	outputPlan := &OutputPlan{}
	var (
		err  error
		prev = []PhysicalPlan{outputPlan}
	)

	plan.Accept(PrePlanVisitorFunc(func(plan *logicalplan.LogicalPlan) bool {
		var phyPlans []PhysicalPlan
		switch {
		case plan.SchemaScan != nil:
			outputPlan.scan = &SchemaScan{
				tracer:  tracer,
				options: plan.SchemaScan,
				plans:   prev,
			}
			return false
		case plan.TableScan != nil:
			outputPlan.scan = &TableScan{
				tracer:  tracer,
				options: plan.TableScan,
				plans:   prev,
			}
			return false
		case plan.Projection != nil:
			// For each previous physical plan create one Projection
			for i := 0; i < len(prev); i++ {
				p, err := Project(pool, tracer, plan.Projection.Exprs)
				if err != nil {
					return false
				}
				phyPlans = append(phyPlans, p)
			}
		case plan.Distinct != nil:
			concurrency := concurrencyHardcoded
			if len(prev) > 1 {
				// Just in case the previous plan's concurrency differs from the hardcoded concurrency
				concurrency = len(prev)
			}

			phyPlans = make([]PhysicalPlan, 0, concurrency)
			for i := 0; i < concurrency; i++ {
				phyPlans = append(phyPlans, Distinct(pool, tracer, plan.Distinct.Exprs))
			}

			// If the prev is 1 then we need a synchronizer to handle the concurrency
			// writing into a single callback of the previous PhysicalPlan.
			if len(prev) == 1 {
				// This Distinct is going to do a final distinct on the previous concurrent Distincts.
				synchronizeDistinct := Distinct(pool, tracer, plan.Distinct.Exprs)
				synchronizeDistinct.SetNext(prev[0])

				synchronizer := Synchronize(len(phyPlans))
				synchronizer.SetNext(synchronizeDistinct)

				for _, p := range phyPlans {
					p.SetNext(synchronizer)
				}
			}
		case plan.Filter != nil:
			// Create a filter for each previous plan.
			// Can be multiple filters or just a single
			// filter depending on the previous concurrency.
			for range prev {
				p, err := Filter(pool, tracer, plan.Filter.Expr)
				if err != nil {
					return false
				}
				phyPlans = append(phyPlans, p)
			}
		case plan.Aggregation != nil:
			concurrency := concurrencyHardcoded
			if len(prev) > 1 {
				// Just in case the previous plan's concurrency differs from the hardcoded concurrency
				concurrency = len(prev)
			}

			schema := s.ParquetSchema()

			phyPlans = make([]PhysicalPlan, 0, concurrency)
			for i := 0; i < concurrency; i++ {
				p, err := Aggregate(pool, tracer, schema, plan.Aggregation, false)
				if err != nil {
					return false
				}
				phyPlans = append(phyPlans, p)
			}

			// If the previous plan is not concurrent we need to add a synchronizer to
			// fan-in/synchronize the concurrent callbacks before passing the data on to single previous plan.
			if len(prev) == 1 {
				synchronizeAggregate, err := Aggregate(pool, tracer, schema, plan.Aggregation, true)
				if err != nil {
					return false
				}
				synchronizeAggregate.SetNext(prev[0])

				synchronizer := Synchronize(len(phyPlans))
				synchronizer.SetNext(synchronizeAggregate)

				for _, p := range phyPlans {
					p.SetNext(synchronizer)
				}
			}
		default:
			panic("Unsupported plan")
		}

		if err != nil {
			return false
		}

		// If these aren't the same the operator should handle how to synchronize
		if len(phyPlans) == len(prev) {
			for i := 0; i < len(phyPlans); i++ {
				phyPlans[i].SetNext(prev[i])
			}
		}

		prev = phyPlans
		return true
	}))

	if len(prev) == 1 {
		outputPlan.SetNextCallback(prev[0].Callback)
	} else {
		// This case should be handled in the concurrent operator above.
	}

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
