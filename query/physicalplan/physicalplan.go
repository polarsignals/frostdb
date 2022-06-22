package physicalplan

import (
	"context"
	"errors"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/memory"

	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

type PhysicalPlan interface {
	Callback(r arrow.Record) error
	SetNextCallback(next func(r arrow.Record) error)
}

type ScanPhysicalPlan interface {
	Execute(ctx context.Context, pool memory.Allocator) error
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
	callback func(r arrow.Record) error
	scan     ScanPhysicalPlan
}

func (e *OutputPlan) Callback(r arrow.Record) error {
	return e.callback(r)
}

func (e *OutputPlan) SetNextCallback(next func(r arrow.Record) error) {
	e.callback = next
}

func (e *OutputPlan) Execute(ctx context.Context, pool memory.Allocator, callback func(r arrow.Record) error) error {
	e.callback = callback
	return e.scan.Execute(ctx, pool)
}

type TableScan struct {
	options  *logicalplan.TableScan
	next     PhysicalPlan
	finisher func() error
}

func (s *TableScan) Execute(ctx context.Context, pool memory.Allocator) error {
	table := s.options.TableProvider.GetTable(s.options.TableName)
	if table == nil {
		return errors.New("table not found")
	}

	err := table.View(func(tx uint64) error {
		schema, err := table.ArrowSchema(
			ctx,
			tx,
			pool,
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
			s.options.Projection,
			s.options.Filter,
			s.options.Distinct,
			s.next.Callback,
		)
	})
	if err != nil {
		return err
	}

	return s.finisher()
}

type SchemaScan struct {
	options  *logicalplan.SchemaScan
	next     PhysicalPlan
	finisher func() error
}

func (s *SchemaScan) Execute(ctx context.Context, pool memory.Allocator) error {
	table := s.options.TableProvider.GetTable(s.options.TableName)
	if table == nil {
		return errors.New("table not found")
	}
	err := table.View(func(tx uint64) error {
		return table.SchemaIterator(
			ctx,
			tx,
			pool,
			s.options.Projection,
			s.options.Filter,
			s.options.Distinct,
			s.next.Callback,
		)
	})
	if err != nil {
		return err
	}

	return s.finisher()
}

func Build(pool memory.Allocator, s *dynparquet.Schema, plan *logicalplan.LogicalPlan) (*OutputPlan, error) {
	outputPlan := &OutputPlan{}
	var (
		err      error
		prev     PhysicalPlan = outputPlan
		finisher              = func() error { return nil }
	)

	plan.Accept(PrePlanVisitorFunc(func(plan *logicalplan.LogicalPlan) bool {
		var phyPlan PhysicalPlan
		switch {
		case plan.SchemaScan != nil:
			outputPlan.scan = &SchemaScan{
				options:  plan.SchemaScan,
				next:     prev,
				finisher: finisher,
			}
			return false
		case plan.TableScan != nil:
			outputPlan.scan = &TableScan{
				options:  plan.TableScan,
				next:     prev,
				finisher: finisher,
			}
			return false
		case plan.Projection != nil:
			phyPlan, err = Project(pool, plan.Projection.Exprs)
		case plan.Distinct != nil:
			matchers := make([]logicalplan.ColumnMatcher, 0, len(plan.Distinct.Columns))
			for _, col := range plan.Distinct.Columns {
				matchers = append(matchers, col.Matcher())
			}

			phyPlan = Distinct(pool, matchers)
		case plan.Filter != nil:
			phyPlan, err = Filter(pool, plan.Filter.Expr)
		case plan.Aggregation != nil:
			var agg *HashAggregate
			agg, err = Aggregate(pool, s, plan.Aggregation)
			phyPlan = agg
			if agg != nil {
				finisher = agg.Finish
			}
		default:
			panic("Unsupported plan")
		}

		if err != nil {
			return false
		}

		phyPlan.SetNextCallback(prev.Callback)
		prev = phyPlan

		return true
	}))
	return outputPlan, err
}
