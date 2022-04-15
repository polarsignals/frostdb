package logicalplan

import (
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/memory"

	"github.com/polarsignals/arcticdb/dynparquet"
)

// LogicalPlan is a logical representation of a query. Each LogicalPlan is a
// sub-tree of the query. It is built recursively.
type LogicalPlan struct {
	Input *LogicalPlan

	// Each LogicalPlan struct must only have one of the following.
	SchemaScan  *SchemaScan
	TableScan   *TableScan
	Filter      *Filter
	Distinct    *Distinct
	Projection  *Projection
	Aggregation *Aggregation
}

func (plan *LogicalPlan) String() string {
	return plan.string(0)
}

func (plan *LogicalPlan) string(indent int) string {
	res := ""
	switch {
	case plan.SchemaScan != nil:
		res = plan.SchemaScan.String()
	case plan.TableScan != nil:
		res = plan.TableScan.String()
	case plan.Filter != nil:
		res = plan.Filter.String()
	case plan.Projection != nil:
		res = plan.Projection.String()
	case plan.Aggregation != nil:
		res = plan.Aggregation.String()
	default:
		res = "Unknown LogicalPlan"
	}

	res = strings.Repeat("  ", indent) + res
	if plan.Input != nil {
		res += "\n" + plan.Input.string(indent+1)
	}
	return res
}

type PlanVisitor interface {
	PreVisit(plan *LogicalPlan) bool
	PostVisit(plan *LogicalPlan) bool
}

func (plan *LogicalPlan) Accept(visitor PlanVisitor) bool {
	continu := visitor.PreVisit(plan)
	if !continu {
		return false
	}

	if plan.Input != nil {
		continu = plan.Input.Accept(visitor)
		if !continu {
			return false
		}
	}

	return visitor.PostVisit(plan)
}

type IterateOption interface {
	Apply(*IterateOptions)
}

type IterateOptions struct {
	GreaterOrEqual *dynparquet.DynamicRow
	LessThan       *dynparquet.DynamicRow
}

type iterateOption struct {
	f func(*IterateOptions)
}

func (i *iterateOption) Apply(options *IterateOptions) {
	i.f(options)
}

func LessThan(row *dynparquet.DynamicRow) IterateOption {
	return &iterateOption{
		f: func(o *IterateOptions) {
			o.LessThan = row
		},
	}
}

func GreaterOrEqual(row *dynparquet.DynamicRow) IterateOption {
	return &iterateOption{
		f: func(o *IterateOptions) {
			o.GreaterOrEqual = row
		},
	}
}

type TableReader interface {
	Iterator(
		pool memory.Allocator,
		projection []ColumnMatcher,
		filter Expr,
		distinctColumns []ColumnMatcher,
		callback func(r arrow.Record) error,
		opts ...IterateOption,
	) error
	SchemaIterator(
		pool memory.Allocator,
		projection []ColumnMatcher,
		filter Expr,
		distinctColumns []ColumnMatcher,
		callback func(r arrow.Record) error,
	) error
}

type TableProvider interface {
	GetTable(name string) TableReader
}

type TableScan struct {
	TableProvider TableProvider
	TableName     string

	// Projection in this case means the columns that are to be read by the
	// table scan.
	Projection []ColumnMatcher

	// Filter is the predicate that is to be applied by the table scan to rule
	// out any blocks of data to be scanned at all.
	Filter Expr

	// Distinct describes the columns that are to be distinct.
	Distinct []ColumnMatcher

	// IterateOptions are options around iteration
	IterateOptions []IterateOption
}

func (scan *TableScan) String() string {
	return "TableScan" +
		" Table: " + scan.TableName +
		" Projection: " + fmt.Sprint(scan.Projection) +
		" Filter: " + fmt.Sprint(scan.Filter) +
		" Distinct: " + fmt.Sprint(scan.Distinct)
}

type SchemaScan struct {
	TableProvider TableProvider
	TableName     string

	// projection in this case means the columns that are to be read by the
	// table scan.
	Projection []ColumnMatcher

	// filter is the predicate that is to be applied by the table scan to rule
	// out any blocks of data to be scanned at all.
	Filter Expr

	// Distinct describes the columns that are to be distinct.
	Distinct []ColumnMatcher
}

func (scan *SchemaScan) String() string {
	return "SchemaScan"
}

type Filter struct {
	Expr Expr
}

func (filter *Filter) String() string {
	return "Filter" + " Expr: " + fmt.Sprint(filter.Expr)
}

type Distinct struct {
	Columns []ColumnExpr
}

func (distinct *Distinct) String() string {
	return "Distinct"
}

type Projection struct {
	Exprs []Expr
}

func (projection *Projection) String() string {
	return "Projection"
}

type Aggregation struct {
	GroupExprs []ColumnExpr
	AggExpr    Expr
}

func (aggregation *Aggregation) String() string {
	return "Aggregation " + fmt.Sprint(aggregation.AggExpr) + " Group: " + fmt.Sprint(aggregation.GroupExprs)
}
