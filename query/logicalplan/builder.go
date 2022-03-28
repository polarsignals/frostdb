package logicalplan

import (
	"strings"

	"github.com/apache/arrow/go/v7/arrow"

	"github.com/polarsignals/arcticdb/dynparquet"
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

func (b Builder) Project(
	projections ...string,
) Builder {
	exprs := make([]Expr, len(projections))
	for i, name := range projections {
		exprs[i] = Col(name)
	}

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

func (m StaticColumnMatcher) Match(columnName string) bool {
	return m.ColumnName == columnName
}

type ColumnMatcher interface {
	Match(columnName string) bool
}

type DynamicColumnMatcher struct {
	ColumnName string
}

func (m DynamicColumnMatcher) Match(columnName string) bool {
	return strings.HasPrefix(columnName, m.ColumnName+".")
}

type Expr interface {
	DataType(*dynparquet.Schema) arrow.DataType
	Accept(Visitor) bool
	Name() string
	ColumnsUsed() []ColumnMatcher
}

type ColumnExpr interface {
	Expr
	Matcher() ColumnMatcher
}

func (b Builder) Filter(
	expr Expr,
) Builder {
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
	columns ...ColumnExpr,
) Builder {
	return Builder{
		plan: &LogicalPlan{
			Input: b.plan,
			Distinct: &Distinct{
				Columns: columns,
			},
		},
	}
}

func (b Builder) Aggregate(
	aggExpr Expr,
	groupExprs ...ColumnExpr,
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

func (b Builder) Build() *LogicalPlan {
	return b.plan
}
