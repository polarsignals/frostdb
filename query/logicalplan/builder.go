package logicalplan

import (
	"encoding/json"

	"github.com/apache/arrow/go/v8/arrow"

	"github.com/polarsignals/frostdb/dynparquet"
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
	PostVisit(expr Expr) bool
}

type StaticColumnMatcher struct {
	ColumnName string
}

func (m StaticColumnMatcher) Name() string {
	return m.ColumnName
}

func (m StaticColumnMatcher) Match(columnName string) bool {
	return m.ColumnName == columnName
}

type ColumnMatcher interface {
	MatchColumn(columnName string) bool
	Name() string
}

type Expr interface {
	DataType(*dynparquet.Schema) (arrow.DataType, error)
	Accept(Visitor) bool
	Name() string
	ColumnsUsed() []ColumnMatcher

	// MatchColumn returns whether it would operate on the passed column. In
	// contrast to the ColumnUsed function from the Expr interface, it is not
	// useful to identify which columns are to be read physically. This is
	// necessary to distinguish between projections.
	//
	// Take the example of a column that projects `XYZ > 0`. Matcher can be
	// used to identify the column in the resulting Apache Arrow frames, while
	// ColumnsUsed will return `XYZ` to be necessary to be loaded physically.
	MatchColumn(columnName string) bool

	// Computed returns whether the expression is computed as opposed to being
	// a static value or unmodified physical column.
	Computed() bool

	// Expr implements these two interfaces
	// so that queries can be transported as JSON.
	json.Marshaler
	json.Unmarshaler
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
	aggExpr Expr,
	groupExprs ...Expr,
) Builder {
	return Builder{
		plan: &LogicalPlan{
			Input: b.plan,
			Aggregation: &Aggregation{
				GroupExprs: groupExprs,
				AggExpr:    aggExpr,
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
