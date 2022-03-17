package logicalplan

type Optimizer interface {
	Optimize(plan *LogicalPlan)
}

// The ProjectionPushDown optimizer tries to push down the actual physical
// columns used by the query to the table scan, so the table provider can
// decide to only read the columns that are actually going to be used by the
// query.
type ProjectionPushDown struct{}

func (p *ProjectionPushDown) Optimize(plan *LogicalPlan) {
	p.optimize(plan, nil)
}

func (p *ProjectionPushDown) optimize(plan *LogicalPlan, columnsUsed []ColumnMatcher) {
	switch {
	case plan.SchemaScan != nil:
		plan.SchemaScan.Projection = columnsUsed
	case plan.TableScan != nil:
		plan.TableScan.Projection = columnsUsed
	case plan.Filter != nil:
		columnsUsed = append(columnsUsed, plan.Filter.Expr.ColumnsUsed()...)
	case plan.Distinct != nil:
		for _, expr := range plan.Distinct.Columns {
			columnsUsed = append(columnsUsed, expr.ColumnsUsed()...)
		}
	case plan.Projection != nil:
		for _, expr := range plan.Projection.Exprs {
			columnsUsed = append(columnsUsed, expr.ColumnsUsed()...)
		}
	case plan.Aggregation != nil:
		for _, expr := range plan.Aggregation.GroupExprs {
			columnsUsed = append(columnsUsed, expr.ColumnsUsed()...)
		}
		columnsUsed = append(columnsUsed, plan.Aggregation.AggExpr.ColumnsUsed()...)
	}

	if plan.Input != nil {
		p.optimize(plan.Input, columnsUsed)
	}
}

// The FilterPushDown optimizer tries to push down the filters of a query down
// to the actual physical table scan. This allows the table provider to make
// smarter decisions about which pieces of data to load in the first place or
// which are definitely not useful to the query at all. It does not guarantee
// that all data will be filtered accordingly, it is just a mechanism to read
// less data from disk.
type FilterPushDown struct{}

func (p *FilterPushDown) Optimize(plan *LogicalPlan) {
	p.optimize(plan, nil)
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
