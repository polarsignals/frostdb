package physicalplan

import (
	"context"
	"fmt"
	"runtime"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/memory"
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

func (f PrePlanVisitorFunc) PostVisit(_ *logicalplan.LogicalPlan) bool {
	return false
}

type PostPlanVisitorFunc func(plan *logicalplan.LogicalPlan) bool

func (f PostPlanVisitorFunc) PreVisit(_ *logicalplan.LogicalPlan) bool {
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

func (e *OutputPlan) Finish(_ context.Context) error {
	return nil
}

func (e *OutputPlan) SetNext(_ PhysicalPlan) {
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
	details := "TableScan"
	var child *Diagram
	if children := len(s.plans); children > 0 {
		child = s.plans[0].Draw()
		if children > 1 {
			details += " [concurrent]"
		}
	}
	return &Diagram{Details: details, Child: child}
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
	// var children []*Diagram
	// for _, plan := range s.plans {
	//	children = append(children, plan.Draw())
	// }
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

type noopOperator struct {
	next PhysicalPlan
}

func (p *noopOperator) Callback(ctx context.Context, r arrow.Record) error {
	return p.next.Callback(ctx, r)
}

func (p *noopOperator) Finish(ctx context.Context) error {
	return p.next.Finish(ctx)
}

func (p *noopOperator) SetNext(next PhysicalPlan) {
	p.next = next
}

func (p *noopOperator) Draw() *Diagram {
	if p.next == nil {
		return nil
	}
	return p.next.Draw()
}

type execOptions struct {
	orderedAggregations bool
}

type Option func(o *execOptions)

func WithOrderedAggregations() Option {
	return func(o *execOptions) {
		o.orderedAggregations = true
	}
}

func Build(
	ctx context.Context,
	pool memory.Allocator,
	tracer trace.Tracer,
	s *dynparquet.Schema,
	plan *logicalplan.LogicalPlan,
	options ...Option,
) (*OutputPlan, error) {
	_, span := tracer.Start(ctx, "PhysicalPlan/Build")
	defer span.End()

	execOpts := execOptions{}
	for _, o := range options {
		o(&execOpts)
	}

	outputPlan := &OutputPlan{}
	var (
		visitErr error
		prev     []PhysicalPlan
	)

	oInfo := &planOrderingInfo{
		state: planOrderingInfoStateInit,
	}
	if s != nil {
		// TODO(asubiotto): There are cases in which the schema can be nil.
		// Eradicate these.
		oInfo.sortingCols = s.SortingColumns()
	}

	plan.Accept(PostPlanVisitorFunc(func(plan *logicalplan.LogicalPlan) bool {
		oInfo.newNode()
		switch {
		case plan.SchemaScan != nil:
			// Create noop operators since we don't know what to push the scan
			// results to. In a following node visit, these noops will have
			// SetNext called on them and push to the correct operator.
			plans := make([]PhysicalPlan, concurrencyHardcoded)
			for i := range plans {
				plans[i] = &noopOperator{}
			}
			outputPlan.scan = &SchemaScan{
				tracer:  tracer,
				options: plan.SchemaScan,
				plans:   plans,
			}
			prev = append(prev[:0], plans...)
		case plan.TableScan != nil:
			// Create noop operators since we don't know what to push the scan
			// results to. In a following node visit, these noops will have
			// SetNext called on them and push to the correct operator.
			plans := make([]PhysicalPlan, concurrencyHardcoded)
			for i := range plans {
				plans[i] = &noopOperator{}
			}
			outputPlan.scan = &TableScan{
				tracer:  tracer,
				options: plan.TableScan,
				plans:   plans,
			}
			prev = append(prev[:0], plans...)
			oInfo.nodeMaintainsOrdering()
		case plan.Projection != nil:
			// For each previous physical plan create one Projection
			for i := range prev {
				p, err := Project(pool, tracer, plan.Projection.Exprs)
				if err != nil {
					visitErr = err
					return false
				}
				prev[i].SetNext(p)
				prev[i] = p
			}
		case plan.Distinct != nil:
			var sync *Synchronizer
			if len(prev) > 1 {
				// These distinct operators need to be synchronized.
				sync = Synchronize(len(prev))
			}
			for i := 0; i < len(prev); i++ {
				d := Distinct(pool, tracer, plan.Distinct.Exprs)
				prev[i].SetNext(d)
				prev[i] = d
				if sync != nil {
					d.SetNext(sync)
				}
			}
			if sync != nil {
				// Plan a distinct operator to run a distinct on all the
				// synchronized distincts.
				d := Distinct(pool, tracer, plan.Distinct.Exprs)
				sync.SetNext(d)
				prev = prev[0:1]
				prev[0] = d
			}
		case plan.Filter != nil:
			// Create a filter for each previous plan.
			// Can be multiple filters or just a single
			// filter depending on the previous concurrency.
			for i := range prev {
				f, err := Filter(pool, tracer, plan.Filter.Expr)
				if err != nil {
					visitErr = err
					return false
				}
				prev[i].SetNext(f)
				prev[i] = f
			}
			oInfo.applyFilter(plan.Filter.Expr)
			oInfo.nodeMaintainsOrdering()
		case plan.Aggregation != nil:
			schema := s.ParquetSchema()
			ordered, err := shouldPlanOrderedAggregate(execOpts, oInfo, plan.Aggregation)
			if err != nil {
				// TODO(asubiotto): Log the error.
				ordered = false
			}
			var sync PhysicalPlan
			if len(prev) > 1 {
				// These aggregate operators need to be synchronized.
				if ordered && len(plan.Aggregation.GroupExprs) > 0 {
					sync = NewOrderedSynchronizer(pool, len(prev), plan.Aggregation.GroupExprs)
				} else {
					sync = Synchronize(len(prev))
				}
			}
			for i := 0; i < len(prev); i++ {
				a, err := Aggregate(pool, tracer, schema, plan.Aggregation, sync == nil, ordered)
				if err != nil {
					visitErr = err
					return false
				}
				prev[i].SetNext(a)
				prev[i] = a
				if sync != nil {
					a.SetNext(sync)
				}
			}
			if sync != nil {
				// Plan an aggregate operator to run an aggregation on all the
				// aggregations.
				a, err := Aggregate(pool, tracer, schema, plan.Aggregation, true, ordered)
				if err != nil {
					visitErr = err
					return false
				}
				sync.SetNext(a)
				prev = prev[0:1]
				prev[0] = a
			}
			if ordered {
				oInfo.nodeMaintainsOrdering()
			}
		default:
			panic("Unsupported plan")
		}
		return visitErr == nil
	}))
	if visitErr != nil {
		return nil, visitErr
	}

	span.SetAttributes(attribute.String("plan", outputPlan.scan.Draw().String()))

	// Synchronize the last stage if necessary.
	var sync *Synchronizer
	if len(prev) > 1 {
		sync = Synchronize(len(prev))
		for i := range prev {
			prev[i].SetNext(sync)
		}
		sync.SetNext(outputPlan)
	} else {
		prev[0].SetNext(outputPlan)
	}

	return outputPlan, nil
}

func shouldPlanOrderedAggregate(
	execOpts execOptions, oInfo *planOrderingInfo, agg *logicalplan.Aggregation,
) (bool, error) {
	if !execOpts.orderedAggregations {
		// Ordered aggregations disabled.
		return false, nil
	}
	if len(agg.AggExprs) > 1 {
		// More than one aggregation is not yet supported.
		return false, nil
	}
	if !oInfo.orderingMaintained() {
		return false, nil
	}
	groupExprs := agg.GroupExprs
	ordering := oInfo.getNonCoveringOrdering()
	for _, expr := range groupExprs {
		groupCols := expr.ColumnsUsedExprs()
		if len(groupCols) > 1 {
			return false, fmt.Errorf("expected only one group column but found %v", groupCols)
		}
		if len(ordering) == 0 {
			return false, nil
		}
		orderCol := ordering[0]
		ordering = ordering[1:]
		orderColName := orderCol.Name
		if orderCol.Dynamic {
			// TODO(asubiotto): Appending a "." is necessary for the MatchColumn
			// call below to work for dynamic columns. Not sure if there's a
			// better way to do this.
			orderColName += "."
		}
		if !groupCols[0].MatchColumn(orderColName) {
			return false, nil
		}
	}
	return true, nil
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
