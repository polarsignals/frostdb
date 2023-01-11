package physicalplan

import (
	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

type planOrderingInfoState int

const (
	planOrderingInfoStateInit planOrderingInfoState = iota
	planOrderingInfoStateNewNode
	planOrderingInfoStateMaintained
	planOrderingInfoStateInvalidated
)

// planOrderingInfo is a helper struct that stores the ordering that an operator
// can expect from its input stream.
type planOrderingInfo struct {
	// state stores the ordering info state. This is a little more complex than
	// a boolean because we want a visit to a logical plan node to explicitly
	// validate the ordering to avoid programming errors in the future. This
	// requires notifying that a new node visit will occur, and invalidating the
	// ordering if a node visit does not explicitly call nodeMaintainsOrdering
	// in between node visits.
	state       planOrderingInfoState
	sortingCols []dynparquet.ColumnDefinition
	// filterCoverIdx is an index into sortingCols that specifies the first
	// column that is not "covered" by a filter. What this means is best
	// explained by an example:
	// Say that our ordering consists of columns a, b, c, d. At some point
	// during planning, we apply a filter that has binary equality expressions
	// on columns a and b. This means that a and b are covered by this filter
	// so don't really matter for ordering purposes given that all values after
	// the filter will be the same. This is helpful to operators downstream that
	// can only perform optimizations if they expect some ordering on c and d
	// only.
	filterCoverIdx int
}

// newNode should be called when visiting a new node in the logical plan.
func (i *planOrderingInfo) newNode() {
	if i.state == planOrderingInfoStateNewNode {
		// If newNode is called twice in a row, the previous node did not
		// maintain ordering, so invalidate the ordering.
		i.state = planOrderingInfoStateInvalidated
		return
	}
	i.state = planOrderingInfoStateNewNode
}

// nodeMaintainsOrdering should be called when a logical plan node maintains
// input ordering.
func (i *planOrderingInfo) nodeMaintainsOrdering() {
	if i.state == planOrderingInfoStateNewNode {
		i.state = planOrderingInfoStateMaintained
		return
	}
}

func (i *planOrderingInfo) orderingMaintained() bool {
	return i.state != planOrderingInfoStateInvalidated
}

// preExprVisitorFunc is an expr visitor that returns true on PostVisit as well.
// TODO(asubiotto): Remove this in favor of PreExprVisitorFunc, just that the
// latter aborts visitation on post-visit.
type preExprVisitorFunc func(expr logicalplan.Expr) bool

func (f preExprVisitorFunc) PreVisit(expr logicalplan.Expr) bool {
	return f(expr)
}

func (f preExprVisitorFunc) PostVisit(_ logicalplan.Expr) bool {
	return true
}

func (i *planOrderingInfo) applyFilter(expr logicalplan.Expr) {
	if i.state == planOrderingInfoStateInvalidated {
		return
	}
	equalityColumns := make(map[string]struct{})
	expr.Accept(preExprVisitorFunc(func(expr logicalplan.Expr) bool {
		switch e := expr.(type) {
		case *logicalplan.BinaryExpr:
			switch e.Op {
			case logicalplan.OpAnd:
				// Continue visiting.
				return true
			case logicalplan.OpEq:
				if c, ok := e.Left.(*logicalplan.Column); ok {
					equalityColumns[c.ColumnName] = struct{}{}
				}
			}
		}
		return true
	}))
	for _, col := range i.sortingCols {
		if col.Dynamic {
			// Equality filters on dynamic columns cannot be considered to
			// be covering.
			return
		}
		if _, ok := equalityColumns[col.Name]; !ok {
			return
		}
		i.filterCoverIdx++
	}
}

func (i *planOrderingInfo) getNonCoveringOrdering() []dynparquet.ColumnDefinition {
	return i.sortingCols[i.filterCoverIdx:]
}
