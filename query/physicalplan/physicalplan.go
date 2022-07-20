package physicalplan

import (
	"context"
	"errors"
	"sync"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/memory"

	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

type PhysicalPlan interface {
	Callback(r arrow.Record) error
	Finish() error
	SetNextPlan(PhysicalPlan)
}

type ScanPhysicalPlan interface {
	Execute(ctx context.Context, pool memory.Allocator) error
	PlanBuilder() func(context.Context) PhysicalPlan
}

type QueryOptions interface {
	GetSyncCallback() bool
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

func (e *OutputPlan) Finish() error {
	return nil
}

func (e *OutputPlan) SetNextPlan(nextPlan PhysicalPlan) {
	// output plan should be the last step .. if this gets called we're doing
	// something wrong
	panic("bug in builder! output plan should not have a next plan!")
}

func (e *OutputPlan) SetNextCallback(next func(r arrow.Record) error) {
	e.callback = next
}

func (e *OutputPlan) Execute(
	ctx context.Context,
	pool memory.Allocator,
	callback func(r arrow.Record) error,
) error {
	e.callback = callback
	return e.scan.Execute(ctx, pool)
}

type IteratorProvider struct {
	scan ScanPhysicalPlan
}

func (p *IteratorProvider) Iterator(ctx context.Context) logicalplan.Iterator {
	planBuilder := p.scan.PlanBuilder()
	return planBuilder(ctx)
}

type TableScan struct {
	options     *logicalplan.TableScan
	nextBuilder func(context.Context) PhysicalPlan
}

func (s *TableScan) PlanBuilder() func(context.Context) PhysicalPlan {
	return s.nextBuilder
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
			&IteratorProvider{scan: s},
		)
	})
	if err != nil {
		return err
	}

	return nil
}

type SchemaScan struct {
	options     *logicalplan.SchemaScan
	nextBuilder func(context.Context) PhysicalPlan
	finisher    func() error
}

func (s *SchemaScan) PlanBuilder() func(context.Context) PhysicalPlan {
	return s.nextBuilder
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
			&IteratorProvider{scan: s},
		)
	})
	if err != nil {
		return err
	}

	return s.finisher()
}

func Build(pool memory.Allocator, s *dynparquet.Schema, plan *logicalplan.LogicalPlan) (*OutputPlan, error) {
	outputPlan := &OutputPlan{}

	nextBuilder := nextIteratorBuilder(pool, s, plan, outputPlan)
	plan.Accept(PrePlanVisitorFunc(func(plan *logicalplan.LogicalPlan) bool {
		switch {
		case plan.SchemaScan != nil:
			outputPlan.scan = &SchemaScan{
				options:     plan.SchemaScan,
				nextBuilder: nextBuilder,
			}
			return false
		case plan.TableScan != nil:
			outputPlan.scan = &TableScan{
				options:     plan.TableScan,
				nextBuilder: nextBuilder,
			}
			return false
		case plan.Aggregation != nil:
			break
		case plan.Distinct != nil:
			break
		case plan.Filter != nil:
			break
		case plan.Projection != nil:
			break
		default:
			panic("Unsupported plan")
		}

		return true
	}))
	return outputPlan, nil
}

// nextIteratorBuilder returns a function that when called will build the part
// of the physical plan that will iterate the arrow records. The builder
// function can be called in multiple threads to parallelize query execution
// from the scan step onward.
func nextIteratorBuilder(
	pool memory.Allocator,
	s *dynparquet.Schema,
	plan *logicalplan.LogicalPlan,
	outputPlan *OutputPlan,
) func(ctx context.Context) PhysicalPlan {
	distinctExchanges := make(map[*logicalplan.Distinct]*ExchangeOperator)
	distinctsPhys := make(map[*logicalplan.Distinct]*Distinction)
	distinctMerges := make(map[*logicalplan.Distinct]*MergeOperator)
	distintsLock := sync.Mutex{}

	aggExchanges := make(map[*logicalplan.Aggregation]*ExchangeOperator)
	aggMerges := make(map[*logicalplan.Aggregation]*MergeOperator)
	aggLock := sync.Mutex{}

	return func(ctx context.Context) PhysicalPlan {
		var (
			err  error
			prev PhysicalPlan = outputPlan
		)
		plan.Accept(PrePlanVisitorFunc(func(plan *logicalplan.LogicalPlan) bool {
			var phyPlan PhysicalPlan
			switch {
			case plan.SchemaScan != nil:
				return false
			case plan.TableScan != nil:
				return false
			case plan.Projection != nil:
				phyPlan, err = Project(pool, plan.Projection.Exprs)
			case plan.Distinct != nil:
				// instances of distinct are not shared across threads so that multiple
				// instances do not have to syncrhonize which distinct values they have seen.
				// we'll only create one instance of the phy plan distinct per logical plan
				distintsLock.Lock()
				defer distintsLock.Unlock()
				// if the distinct instance for the logical plan has not been
				// insantiated, do it now
				if _, ok := distinctExchanges[plan.Distinct]; !ok {
					exchange := Exchange(ctx)
					distinctExchanges[plan.Distinct] = exchange

					matchers := make([]logicalplan.ColumnMatcher, 0, len(plan.Distinct.Columns))
					for _, col := range plan.Distinct.Columns {
						matchers = append(matchers, col.Matcher())
					}
					distinct := Distinct(pool, matchers)
					distinct.SetNextPlan(exchange)
					distinctsPhys[plan.Distinct] = distinct

					merge := Merge()
					merge.SetNextPlan(distinct)
					distinctMerges[plan.Distinct] = merge
				}
				distinctExchanges[plan.Distinct].SetNextPlan(prev)
				merge := distinctMerges[plan.Distinct]
				merge.wg.Add(1)
				phyPlan = merge
				prev = distinctsPhys[plan.Distinct]
			case plan.Filter != nil:
				phyPlan, err = Filter(pool, plan.Filter.Expr)

			case plan.Aggregation != nil:
				// aggregations results are computed in parallel and then a concurrent agg
				// is used to combine the results of the mulitple threads. Exchange/merge ops
				// are wrapping the concurrent agg
				var agg *HashAggregate
				agg, err = Aggregate(pool, s, plan.Aggregation)

				// add the exchange operator that will parallelize execution
				// after the concurrent merge
				aggLock.Lock()
				defer aggLock.Unlock()
				if _, ok := aggExchanges[plan.Aggregation]; !ok {
					aggExchanges[plan.Aggregation] = Exchange(ctx)
				}
				exchange := aggExchanges[plan.Aggregation]
				exchange.SetNextPlan(prev)

				// add concurrent agg & merge for combining the results of the
				// parallel merges
				if _, ok := aggMerges[plan.Aggregation]; !ok {
					concurrentAgg := ConcurrentAggregate(
						pool,
						agg.aggregationFunction,
					)
					concurrentAgg.SetNextPlan(exchange)

					merge := Merge()
					merge.SetNextPlan(concurrentAgg)
					aggMerges[plan.Aggregation] = merge
				}
				merge := aggMerges[plan.Aggregation]
				merge.wg.Add(1)
				prev = merge

				// add the aggregation
				phyPlan = agg

			default:
				panic("Unsupported plan")
			}
			if err != nil {
				return false
			}

			phyPlan.SetNextPlan(prev)
			prev = phyPlan
			return true
		}))
		return prev
	}
}
