package logicalplan

import (
	"errors"
	"fmt"

	"github.com/apache/arrow/go/v15/arrow"
)

type Builder struct {
	plan *LogicalPlan
	err  error
}

func (b Builder) Scan(
	provider TableProvider,
	tableName string,
) Builder {
	return Builder{
		err: b.err,
		plan: &LogicalPlan{
			TableScan: &TableScan{
				TableProvider: provider,
				TableName:     tableName,
			},
		},
	}
}

func (b Builder) ScanSchema(
	provider TableProvider,
	tableName string,
) Builder {
	return Builder{
		err: b.err,
		plan: &LogicalPlan{
			SchemaScan: &SchemaScan{
				TableProvider: provider,
				TableName:     tableName,
			},
		},
	}
}

func (b Builder) Project(
	exprs ...Expr,
) Builder {
	return Builder{
		err: b.err,
		plan: &LogicalPlan{
			Input: b.plan,
			Projection: &Projection{
				Exprs: exprs,
			},
		},
	}
}

type Visitor interface {
	PreVisit(expr Expr) bool
	Visit(expr Expr) bool
	PostVisit(expr Expr) bool
}

type ExprTypeFinder interface {
	DataTypeForExpr(expr Expr) (arrow.DataType, error)
}

type Expr interface {
	DataType(ExprTypeFinder) (arrow.DataType, error)
	Accept(Visitor) bool
	Name() string
	Equal(Expr) bool
	fmt.Stringer

	// ColumnsUsedExprs extracts all the expressions that are used that cause
	// physical data to be read from a column.
	ColumnsUsedExprs() []Expr

	// MatchColumn returns whether it would operate on the passed column. In
	// contrast to the ColumnUsedExprs function from the Expr interface, it is not
	// useful to identify which columns are to be read physically. This is
	// necessary to distinguish between projections.
	//
	// Take the example of a column that projects `XYZ > 0`. Matcher can be
	// used to identify the column in the resulting Apache Arrow frames, while
	// ColumnsUsed will return `XYZ` to be necessary to be loaded physically.
	MatchColumn(columnName string) bool

	// MatchPath returns whether it would operate on the passed path. This is nessesary for nested schemas.
	MatchPath(path string) bool

	// Computed returns whether the expression is computed as opposed to being
	// a static value or unmodified physical column.
	Computed() bool

	// Clone returns a deep copy of the expression.
	Clone() Expr
}

func (b Builder) Filter(expr Expr) Builder {
	if expr == nil {
		return b
	}

	return Builder{
		err: b.err,
		plan: &LogicalPlan{
			Input: b.plan,
			Filter: &Filter{
				Expr: expr,
			},
		},
	}
}

func (b Builder) Distinct(
	exprs ...Expr,
) Builder {
	return Builder{
		err: b.err,
		plan: &LogicalPlan{
			Distinct: &Distinct{
				Exprs: exprs,
			},
			Input: &LogicalPlan{
				Projection: &Projection{
					Exprs: exprs,
				},
				Input: b.plan,
			},
		},
	}
}

func (b Builder) Aggregate(
	aggExpr []*AggregationFunction,
	groupExprs []Expr,
) Builder {
	resolvedAggExpr := make([]*AggregationFunction, 0, len(aggExpr))
	projectExprs := make([]Expr, 0, len(aggExpr))
	needsPostProcessing := false

	var err error
	for _, agg := range aggExpr {
		resolvedAggregations, projections, changed, rerr := resolveAggregation(b.plan, agg)
		if err != nil {
			err = errors.Join(err, rerr)
		}

		if changed {
			needsPostProcessing = true
		}

		resolvedAggExpr = append(resolvedAggExpr, resolvedAggregations...)
		projectExprs = append(projectExprs, projections...)
	}

	if !needsPostProcessing {
		return Builder{
			err: err,
			plan: &LogicalPlan{
				Aggregation: &Aggregation{
					GroupExprs: groupExprs,
					AggExprs:   aggExpr,
				},
				Input: b.plan,
			},
		}
	}

	return Builder{
		err: err,
		plan: &LogicalPlan{
			Projection: &Projection{
				Exprs: append(groupExprs, projectExprs...),
			},
			Input: &LogicalPlan{
				Aggregation: &Aggregation{
					GroupExprs: groupExprs,
					AggExprs:   resolvedAggExpr,
				},
				Input: b.plan,
			},
		},
	}
}

func resolveAggregation(plan *LogicalPlan, agg *AggregationFunction) ([]*AggregationFunction, []Expr, bool, error) {
	switch agg.Func {
	case AggFuncAvg:
		sum := &AggregationFunction{
			Func: AggFuncSum,
			Expr: agg.Expr,
		}
		count := &AggregationFunction{
			Func: AggFuncCount,
			Expr: agg.Expr,
		}

		var (
			countExpr Expr = count
			aggType   arrow.DataType
		)
		aggType, err := agg.Expr.DataType(plan)
		// intentionally not handling the error here, as it will be handled
		// in the build function.
		if !arrow.TypeEqual(aggType, arrow.PrimitiveTypes.Int64) {
			countExpr = Convert(countExpr, aggType)
		}

		div := (&BinaryExpr{
			Left:  sum,
			Op:    OpDiv,
			Right: countExpr,
		}).Alias(agg.String())

		return []*AggregationFunction{sum, count}, []Expr{div}, true, err
	default:
		return []*AggregationFunction{agg}, []Expr{agg}, false, nil
	}
}

func (b Builder) Build() (*LogicalPlan, error) {
	if b.err != nil {
		return nil, b.err
	}

	if err := Validate(b.plan); err != nil {
		return nil, err
	}
	return b.plan, nil
}
