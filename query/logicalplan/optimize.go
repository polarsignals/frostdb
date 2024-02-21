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
		&AggFuncPushDown{},
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

func exprsEqual(a, b []Expr) bool {
	if len(a) != len(b) {
		return false
	}

	for i, expr := range a {
		if !expr.Equal(b[i]) {
			return false
		}
	}

	return true
}

func (p *DistinctPushDown) optimize(plan *LogicalPlan, distinctColumns []Expr) {
	switch {
	case plan.TableScan != nil:
		if len(distinctColumns) > 0 {
			plan.TableScan.Distinct = distinctColumns
		}
	case plan.Distinct != nil:
		distinctColumns = append(distinctColumns, plan.Distinct.Exprs...)
	case plan.Projection != nil:
		if !exprsEqual(distinctColumns, plan.Projection.Exprs) {
			// if and only if the distinct columns are identical to the
			// projection columns we can perform the optimization, so we need
			// to reset it in this case.
			distinctColumns = []Expr{}
		}
	default:
		// reset distinct columns
		distinctColumns = []Expr{}
	}

	if plan.Input != nil {
		p.optimize(plan.Input, distinctColumns)
	}
}

// AggFuncPushDown optimizer tries to push down an aggregation function operator
// to the table provider. This can be done in the case of some aggregation
// functions on global aggregations (i.e. no group by) without filters.
// The storage engine can make smarter decisions than just returning all the
// data, such as in the case of max functions, memoizing the max value seen
// so far and only scanning row groups that contain a value greater than the
// memoized value. It modifies the plan in place.
type AggFuncPushDown struct{}

func (p *AggFuncPushDown) Optimize(plan *LogicalPlan) *LogicalPlan {
	p.optimize(plan, nil)
	return plan
}

func (p *AggFuncPushDown) optimize(plan *LogicalPlan, filterExpr Expr) {
	switch {
	case plan.TableScan != nil:
		if filterExpr != nil {
			plan.TableScan.Filter = filterExpr
		}
	case plan.Aggregation != nil:
		if len(plan.Aggregation.GroupExprs) == 0 && len(plan.Aggregation.AggExprs) == 1 {
			// TODO(asubiotto): Should we make this less specific?
			filterExpr = plan.Aggregation.AggExprs[0]
		}
	default:
		// If we find anything other than a table scan after a global
		// aggregation, bail out by setting the filterExpr to nil.
		filterExpr = nil
	}

	if plan.Input != nil {
		p.optimize(plan.Input, filterExpr)
	}
}
