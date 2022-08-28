package logicalplan

import (
	"errors"
	"strings"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/scalar"
	"github.com/segmentio/parquet-go"

	"github.com/polarsignals/frostdb/pqarrow/convert"
)

type Op uint32

const (
	OpUnknown Op = iota
	OpEq
	OpNotEq
	OpLt
	OpLtEq
	OpGt
	OpGtEq
	OpRegexMatch
	OpRegexNotMatch
	OpAnd
	OpOr
)

func (o Op) String() string {
	switch o {
	case OpEq:
		return "=="
	case OpNotEq:
		return "!="
	case OpLt:
		return "<"
	case OpLtEq:
		return "<="
	case OpGt:
		return ">"
	case OpGtEq:
		return ">="
	case OpRegexMatch:
		return "=~"
	case OpRegexNotMatch:
		return "!~"
	case OpAnd:
		return "&&"
	case OpOr:
		return "||"
	default:
		panic("unknown operator")
	}
}

type BinaryExpr struct {
	Left  Expr
	Op    Op
	Right Expr
}

func (e *BinaryExpr) Accept(visitor Visitor) bool {
	continu := visitor.PreVisit(e)
	if !continu {
		return false
	}

	continu = e.Left.Accept(visitor)
	if !continu {
		return false
	}

	continu = e.Right.Accept(visitor)
	if !continu {
		return false
	}

	return visitor.PostVisit(e)
}

func (e *BinaryExpr) DataType(_ *parquet.Schema) (arrow.DataType, error) {
	return &arrow.BooleanType{}, nil
}

func (e *BinaryExpr) Name() string {
	return e.Left.Name() + " " + e.Op.String() + " " + e.Right.Name()
}

func (e *BinaryExpr) ColumnsUsedExprs() []Expr {
	return append(e.Left.ColumnsUsedExprs(), e.Right.ColumnsUsedExprs()...)
}

func (e *BinaryExpr) MatchColumn(columnName string) bool {
	return e.Name() == columnName
}

func (e *BinaryExpr) Computed() bool {
	return true
}

func (e *BinaryExpr) Alias(alias string) AliasExpr {
	return AliasExpr{Expr: e, Alias: alias}
}

type Column struct {
	ColumnName string
}

func (c *Column) Computed() bool {
	return false
}

func (c *Column) Accept(visitor Visitor) bool {
	continu := visitor.PreVisit(c)
	if !continu {
		return false
	}

	return visitor.PostVisit(c)
}

func (c *Column) Name() string {
	return c.ColumnName
}

func (c *Column) DataType(s *parquet.Schema) (arrow.DataType, error) {
	for _, field := range s.Fields() {
		if field.Name() == c.ColumnName {
			return convert.ParquetNodeToType(field)
		}
	}

	return nil, errors.New("column not found")
}

func (c *Column) Alias(alias string) AliasExpr {
	return AliasExpr{Expr: c, Alias: alias}
}

func (c *Column) ColumnsUsedExprs() []Expr {
	return []Expr{c}
}

func (c *Column) MatchColumn(columnName string) bool {
	return c.ColumnName == columnName
}

func (c *Column) Eq(e Expr) *BinaryExpr {
	return &BinaryExpr{
		Left:  c,
		Op:    OpEq,
		Right: e,
	}
}

func (c *Column) NotEq(e Expr) *BinaryExpr {
	return &BinaryExpr{
		Left:  c,
		Op:    OpNotEq,
		Right: e,
	}
}

func (c *Column) Gt(e Expr) *BinaryExpr {
	return &BinaryExpr{
		Left:  c,
		Op:    OpGt,
		Right: e,
	}
}

func (c *Column) GtEq(e Expr) *BinaryExpr {
	return &BinaryExpr{
		Left:  c,
		Op:    OpGtEq,
		Right: e,
	}
}

func (c *Column) Lt(e Expr) *BinaryExpr {
	return &BinaryExpr{
		Left:  c,
		Op:    OpLt,
		Right: e,
	}
}

func (c *Column) LtEq(e Expr) *BinaryExpr {
	return &BinaryExpr{
		Left:  c,
		Op:    OpLtEq,
		Right: e,
	}
}

func (c *Column) RegexMatch(pattern string) *BinaryExpr {
	return &BinaryExpr{
		Left:  c,
		Op:    OpRegexMatch,
		Right: Literal(pattern),
	}
}

func (c *Column) RegexNotMatch(pattern string) *BinaryExpr {
	return &BinaryExpr{
		Left:  c,
		Op:    OpRegexNotMatch,
		Right: Literal(pattern),
	}
}

func Col(name string) *Column {
	return &Column{ColumnName: name}
}

func And(exprs ...Expr) Expr {
	return and(exprs)
}

func and(exprs []Expr) Expr {
	return computeBinaryExpr(exprs, OpAnd)
}

func Or(exprs ...Expr) Expr {
	return or(exprs)
}

func or(exprs []Expr) Expr {
	return computeBinaryExpr(exprs, OpOr)
}

func computeBinaryExpr(exprs []Expr, op Op) Expr {
	nonNilExprs := make([]Expr, 0, len(exprs))
	for _, expr := range exprs {
		if expr != nil {
			nonNilExprs = append(nonNilExprs, expr)
		}
	}

	if len(nonNilExprs) == 0 {
		return nil
	}
	if len(nonNilExprs) == 1 {
		return nonNilExprs[0]
	}
	if len(nonNilExprs) == 2 {
		return &BinaryExpr{
			Left:  nonNilExprs[0],
			Op:    op,
			Right: nonNilExprs[1],
		}
	}

	return &BinaryExpr{
		Left:  nonNilExprs[0],
		Op:    op,
		Right: computeBinaryExpr(nonNilExprs[1:], op),
	}
}

type DynamicColumn struct {
	ColumnName string
}

func (c *DynamicColumn) Computed() bool {
	return false
}

func DynCol(name string) *DynamicColumn {
	return &DynamicColumn{ColumnName: name}
}

func (c *DynamicColumn) DataType(s *parquet.Schema) (arrow.DataType, error) {
	for _, field := range s.Fields() {
		if names := strings.Split(field.Name(), "."); len(names) == 2 {
			if names[0] == c.ColumnName {
				return convert.ParquetNodeToType(field)
			}
		}
	}

	return nil, errors.New("column not found")
}

func (c *DynamicColumn) ColumnsUsedExprs() []Expr {
	return []Expr{c}
}

func (c *DynamicColumn) MatchColumn(columnName string) bool {
	return strings.HasPrefix(columnName, c.ColumnName+".")
}

func (c *DynamicColumn) Name() string {
	return c.ColumnName
}

func (c *DynamicColumn) Accept(visitor Visitor) bool {
	return visitor.PreVisit(c) && visitor.PostVisit(c)
}

func Cols(names ...string) []Expr {
	exprs := make([]Expr, len(names))
	for i, name := range names {
		exprs[i] = Col(name)
	}
	return exprs
}

type LiteralExpr struct {
	Value scalar.Scalar
}

func (e *LiteralExpr) Computed() bool {
	return false
}

func Literal(v interface{}) *LiteralExpr {
	return &LiteralExpr{
		Value: scalar.MakeScalar(v),
	}
}

func (e *LiteralExpr) DataType(_ *parquet.Schema) (arrow.DataType, error) {
	return e.Value.DataType(), nil
}

func (e *LiteralExpr) Name() string {
	return e.Value.String()
}

func (e *LiteralExpr) Accept(visitor Visitor) bool {
	continu := visitor.PreVisit(e)
	if !continu {
		return false
	}

	return visitor.PostVisit(e)
}

func (e *LiteralExpr) ColumnsUsedExprs() []Expr { return nil }

func (e *LiteralExpr) MatchColumn(columnName string) bool {
	return e.Name() == columnName
}

type AggregationFunction struct {
	Func AggFunc
	Expr Expr
}

func (f *AggregationFunction) DataType(s *parquet.Schema) (arrow.DataType, error) {
	return f.Expr.DataType(s)
}

func (f *AggregationFunction) Accept(visitor Visitor) bool {
	continu := visitor.PreVisit(f)
	if !continu {
		return false
	}

	continu = f.Expr.Accept(visitor)
	if !continu {
		return false
	}

	return visitor.PostVisit(f)
}

func (f *AggregationFunction) Computed() bool {
	return true
}

func (f *AggregationFunction) Name() string {
	return f.Func.String() + "(" + f.Expr.Name() + ")"
}

func (f *AggregationFunction) ColumnsUsedExprs() []Expr {
	return f.Expr.ColumnsUsedExprs()
}

func (f *AggregationFunction) MatchColumn(columnName string) bool {
	return f.Name() == columnName
}

type AggFunc uint32

const (
	AggFuncUnknown AggFunc = iota
	AggFuncSum
	AggFuncMax
	AggFuncCount
)

func (f AggFunc) String() string {
	switch f {
	case AggFuncSum:
		return "sum"
	case AggFuncMax:
		return "max"
	case AggFuncCount:
		return "count"
	default:
		panic("unknown aggregation function")
	}
}

func Sum(expr Expr) *AggregationFunction {
	return &AggregationFunction{
		Func: AggFuncSum,
		Expr: expr,
	}
}

func Max(expr Expr) *AggregationFunction {
	return &AggregationFunction{
		Func: AggFuncMax,
		Expr: expr,
	}
}

func Count(expr Expr) *AggregationFunction {
	return &AggregationFunction{
		Func: AggFuncCount,
		Expr: expr,
	}
}

type AliasExpr struct {
	Expr  Expr
	Alias string
}

func (e *AliasExpr) DataType(s *parquet.Schema) (arrow.DataType, error) {
	return e.Expr.DataType(s)
}

func (e *AliasExpr) Name() string {
	return e.Alias
}

func (e *AliasExpr) Computed() bool {
	return e.Expr.Computed()
}

func (e *AliasExpr) ColumnsUsedExprs() []Expr {
	return e.Expr.ColumnsUsedExprs()
}

func (e *AliasExpr) MatchColumn(columnName string) bool {
	return e.Name() == columnName
}

func (e *AliasExpr) Accept(visitor Visitor) bool {
	continu := visitor.PreVisit(e)
	if !continu {
		return false
	}

	continu = e.Expr.Accept(visitor)
	if !continu {
		return false
	}

	return visitor.PostVisit(e)
}

func (f *AggregationFunction) Alias(alias string) *AliasExpr {
	return &AliasExpr{
		Expr:  f,
		Alias: alias,
	}
}
