package logicalplan

var hashedMatch = "hashed"

type Optimizer interface {
	Optimize(plan *LogicalPlan) *LogicalPlan
}

func DefaultOptimizers() []Optimizer {
	return []Optimizer{
		&PhysicalProjectionPushDown{
			defaultProjections: []Expr{
				Not(DynCol(hashedMatch)),
			},
		},
		&FilterPushDown{},
		&DistinctPushDown{},
	}
}

// PhysicalProjectionPushDown finds the first projecting logical plan and
// collects all columns it needs, it is concatenated with all other columns
// used until it, for example a filter layer. Because the tree has the scan
// layer as the inner most layer, the logic actually works by resetting the
// list every time a projecting layer is found.
type PhysicalProjectionPushDown struct {
	defaultProjections []Expr
}

func (p *PhysicalProjectionPushDown) Optimize(plan *LogicalPlan) *LogicalPlan {
	p.optimize(plan, nil)
	return plan
}

func (p *PhysicalProjectionPushDown) optimize(plan *LogicalPlan, columnsUsedExprs []Expr) {
	switch {
	case plan.SchemaScan != nil:
		plan.SchemaScan.PhysicalProjection = append(p.defaultProjections, columnsUsedExprs...)
	case plan.TableScan != nil:
		plan.TableScan.PhysicalProjection = append(p.defaultProjections, columnsUsedExprs...)
	case plan.Filter != nil:
		p.defaultProjections = []Expr{}
		columnsUsedExprs = append(columnsUsedExprs, plan.Filter.Expr.ColumnsUsedExprs()...)
	case plan.Distinct != nil:
		// distinct is projecting so we need to reset
		columnsUsedExprs = []Expr{}
		for _, expr := range plan.Distinct.Exprs {
			columnsUsedExprs = append(columnsUsedExprs, expr.ColumnsUsedExprs()...)
		}
	case plan.Projection != nil:
		// projections are is projecting so we need to reset
		columnsUsedExprs = []Expr{}
		for _, expr := range plan.Projection.Exprs {
			columnsUsedExprs = append(columnsUsedExprs, expr.ColumnsUsedExprs()...)
		}
	case plan.Aggregation != nil:
		// aggregations are projecting so we need to reset
		columnsUsedExprs = []Expr{}
		for _, expr := range plan.Aggregation.GroupExprs {
			columnsUsedExprs = append(columnsUsedExprs, expr.ColumnsUsedExprs()...)
		}
		for _, expr := range plan.Aggregation.AggExprs {
			columnsUsedExprs = append(columnsUsedExprs, expr.ColumnsUsedExprs()...)
		}
		p.defaultProjections = []Expr{}
		columnsUsedExprs = append(columnsUsedExprs, DynCol(hashedMatch))
	}

	if plan.Input != nil {
		p.optimize(plan.Input, columnsUsedExprs)
	}
}

// FilterPushDown optimizer tries to push down the filters of a query down
// to the actual physical table scan. This allows the table provider to make
// smarter decisions about which pieces of data to load in the first place or
// which are definitely not useful to the query at all. It does not guarantee
// that all data will be filtered accordingly, it is just a mechanism to read
// less data from disk. It modifies the plan in place.
type FilterPushDown struct{}

func (p *FilterPushDown) Optimize(plan *LogicalPlan) *LogicalPlan {
	p.optimize(plan, nil)
	return plan
}

func (p *FilterPushDown) optimize(plan *LogicalPlan, exprs []Expr) {
	switch {
	case plan.SchemaScan != nil:
		if len(exprs) > 0 {
			plan.SchemaScan.Filter = and(exprs)
		}
	case plan.TableScan != nil:
		if len(exprs) > 0 {
			plan.TableScan.Filter = and(exprs)
		}
	case plan.Filter != nil:
		exprs = append(exprs, plan.Filter.Expr)
	}

	if plan.Input != nil {
		p.optimize(plan.Input, exprs)
	}
}

// DistinctPushDown optimizer tries to push down the distinct operator to
// the table provider. There are certain cases of distinct queries where the
// storage engine can make smarter decisions than just returning all the data,
// such as with dictionary encoded columns that are not filtered they can
// return only the dictionary avoiding unnecessary decoding and deduplication
// in downstream distinct operators. It modifies the plan in place.
type DistinctPushDown struct{}

func (p *DistinctPushDown) Optimize(plan *LogicalPlan) *LogicalPlan {
	p.optimize(plan, nil)
	return plan
}

func (p *DistinctPushDown) optimize(plan *LogicalPlan, distinctColumns []Expr) {
	switch {
	case plan.TableScan != nil:
		if len(distinctColumns) > 0 {
			plan.TableScan.Distinct = distinctColumns
		}
	case plan.Distinct != nil:
		distinctColumns = append(distinctColumns, plan.Distinct.Exprs...)
	}

	if plan.Input != nil {
		p.optimize(plan.Input, distinctColumns)
	}
}
