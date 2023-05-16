package logicalplan

import (
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/segmentio/parquet-go"
)

type Builder struct {
	plan *LogicalPlan
}

func (b Builder) Scan(
	provider TableProvider,
	tableName string,
) Builder {
	return Builder{
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

type Expr interface {
	DataType(*parquet.Schema) (arrow.DataType, error)
	Accept(Visitor) bool
	Name() string

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
}

func (b Builder) Filter(expr Expr) Builder {
	if expr == nil {
		return b
	}

	return Builder{
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
		plan: &LogicalPlan{
			Input: b.plan,
			Distinct: &Distinct{
				Exprs: exprs,
			},
		},
	}
}

func (b Builder) Aggregate(
	aggExpr []Expr,
	groupExprs []Expr,
) Builder {
	return Builder{
		plan: &LogicalPlan{
			Input: b.plan,
			Aggregation: &Aggregation{
				GroupExprs: groupExprs,
				AggExprs:   aggExpr,
			},
		},
	}
}

func (b Builder) Build() (*LogicalPlan, error) {
	if err := Validate(b.plan); err != nil {
		return nil, err
	}
	return b.plan, nil
}
