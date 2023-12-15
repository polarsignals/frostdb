package logicalplan

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/scalar"
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
	OpAdd
	OpSub
	OpMul
	OpDiv
	OpContains
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
	case OpAdd:
		return "+"
	case OpSub:
		return "-"
	case OpMul:
		return "*"
	case OpDiv:
		return "/"
	case OpContains:
		return "|="
	default:
		panic("unknown operator")
	}
}

func (o Op) ArrowString() string {
	switch o {
	case OpEq:
		return "equal"
	case OpNotEq:
		return "not_equal"
	case OpLt:
		return "less"
	case OpLtEq:
		return "less_equal"
	case OpGt:
		return "greater"
	case OpGtEq:
		return "greater_equal"
	case OpAnd:
		return "and"
	case OpOr:
		return "or"
	case OpAdd:
		return "add"
	case OpSub:
		return "subtract"
	case OpMul:
		return "multiply"
	case OpDiv:
		return "divide"
	default:
		panic("unknown operator")
	}
}

type BinaryExpr struct {
	Left  Expr
	Op    Op
	Right Expr
}

func (e *BinaryExpr) Equal(other Expr) bool {
	if other == nil {
		// if both are nil, they are equal
		return e == nil
	}

	if b, ok := other.(*BinaryExpr); ok {
		return e.Op == b.Op && e.Left.Equal(b.Left) && e.Right.Equal(b.Right)
	}

	return false
}

func (e *BinaryExpr) Clone() Expr {
	return &BinaryExpr{
		Left:  e.Left.Clone(),
		Op:    e.Op,
		Right: e.Right.Clone(),
	}
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

	continu = visitor.Visit(e)
	if !continu {
		return false
	}

	continu = e.Right.Accept(visitor)
	if !continu {
		return false
	}

	return visitor.PostVisit(e)
}

func (e *BinaryExpr) DataType(l ExprTypeFinder) (arrow.DataType, error) {
	leftType, err := e.Left.DataType(l)
	if err != nil {
		return nil, fmt.Errorf("left operand: %w", err)
	}

	rightType, err := e.Right.DataType(l)
	if err != nil {
		return nil, fmt.Errorf("right operand: %w", err)
	}

	if !arrow.TypeEqual(leftType, rightType) {
		return nil, fmt.Errorf("left and right operands must be of the same type, got %s and %s", leftType, rightType)
	}

	switch e.Op {
	case OpEq, OpNotEq, OpLt, OpLtEq, OpGt, OpGtEq, OpAnd, OpOr:
		return arrow.FixedWidthTypes.Boolean, nil
	case OpAdd, OpSub, OpMul, OpDiv:
		return leftType, nil
	default:
		return nil, errors.New("unknown operator")
	}
}

func (e *BinaryExpr) Name() string {
	return e.Left.Name() + " " + e.Op.String() + " " + e.Right.Name()
}

func (e *BinaryExpr) String() string { return e.Name() }

func (e *BinaryExpr) ColumnsUsedExprs() []Expr {
	return append(e.Left.ColumnsUsedExprs(), e.Right.ColumnsUsedExprs()...)
}

func (e *BinaryExpr) MatchPath(path string) bool {
	return strings.HasPrefix(e.Name(), path)
}

func (e *BinaryExpr) MatchColumn(columnName string) bool {
	return e.Name() == columnName
}

func (e *BinaryExpr) Computed() bool {
	return true
}

func (e *BinaryExpr) Alias(alias string) *AliasExpr {
	return &AliasExpr{Expr: e, Alias: alias}
}

func Convert(e Expr, t arrow.DataType) *ConvertExpr {
	return &ConvertExpr{Expr: e, Type: t}
}

type ConvertExpr struct {
	Expr Expr
	Type arrow.DataType
}

func (e *ConvertExpr) Equal(other Expr) bool {
	if other == nil {
		// if both are nil, they are equal
		return e == nil
	}

	if c, ok := other.(*ConvertExpr); ok {
		return arrow.TypeEqual(e.Type, c.Type) && e.Expr.Equal(c.Expr)
	}

	return false
}

func (e *ConvertExpr) Clone() Expr {
	return &ConvertExpr{
		Expr: e.Expr.Clone(),
		Type: e.Type,
	}
}

func (e *ConvertExpr) Accept(visitor Visitor) bool {
	continu := visitor.PreVisit(e)
	if !continu {
		return false
	}

	continu = e.Expr.Accept(visitor)
	if !continu {
		return false
	}

	continu = visitor.Visit(e)
	if !continu {
		return false
	}

	return visitor.PostVisit(e)
}

func (e *ConvertExpr) DataType(l ExprTypeFinder) (arrow.DataType, error) {
	// We don't care about the result, but we want to make sure the expression
	// tree typing is correct.
	_, err := e.Expr.DataType(l)
	if err != nil {
		return nil, fmt.Errorf("convert type: %w", err)
	}

	return e.Type, nil
}

func (e *ConvertExpr) Name() string {
	return "convert(" + e.Expr.Name() + ", " + e.Type.String() + ")"
}

func (e *ConvertExpr) String() string { return e.Name() }

func (e *ConvertExpr) ColumnsUsedExprs() []Expr {
	return e.Expr.ColumnsUsedExprs()
}

func (e *ConvertExpr) MatchPath(path string) bool {
	return strings.HasPrefix(e.Name(), path)
}

func (e *ConvertExpr) MatchColumn(columnName string) bool {
	return e.Name() == columnName
}

func (e *ConvertExpr) Computed() bool {
	return true
}

func (e *ConvertExpr) Alias(alias string) *AliasExpr {
	return &AliasExpr{Expr: e, Alias: alias}
}

type Column struct {
	ColumnName string
}

func (c *Column) Equal(other Expr) bool {
	if other == nil {
		// if both are nil, they are equal
		return c == nil
	}

	if col, ok := other.(*Column); ok {
		return c.ColumnName == col.ColumnName
	}

	return false
}

func (c *Column) Clone() Expr {
	return &Column{ColumnName: c.ColumnName}
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

func (c *Column) String() string { return c.Name() }

func (c *Column) DataType(l ExprTypeFinder) (arrow.DataType, error) {
	t, err := l.DataTypeForExpr(c)
	if err != nil {
		return nil, fmt.Errorf("column %q type: %w", c.ColumnName, err)
	}

	return t, nil
}

func (c *Column) Alias(alias string) *AliasExpr {
	return &AliasExpr{Expr: c, Alias: alias}
}

func (c *Column) ColumnsUsedExprs() []Expr {
	return []Expr{c}
}

func (c *Column) MatchPath(path string) bool {
	return strings.HasPrefix(c.Name(), path)
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

func (c *Column) Contains(pattern string) *BinaryExpr {
	return &BinaryExpr{
		Left:  c,
		Op:    OpContains,
		Right: Literal(pattern),
	}
}

func Col(name string) *Column {
	return &Column{ColumnName: name}
}

func And(exprs ...Expr) Expr {
	return and(exprs)
}

func Add(left, right Expr) *BinaryExpr {
	return &BinaryExpr{
		Left:  left,
		Op:    OpAdd,
		Right: right,
	}
}

func Sub(left, right Expr) *BinaryExpr {
	return &BinaryExpr{
		Left:  left,
		Op:    OpSub,
		Right: right,
	}
}

func Mul(left, right Expr) *BinaryExpr {
	return &BinaryExpr{
		Left:  left,
		Op:    OpMul,
		Right: right,
	}
}

func Div(left, right Expr) *BinaryExpr {
	return &BinaryExpr{
		Left:  left,
		Op:    OpDiv,
		Right: right,
	}
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

func (c *DynamicColumn) Equal(other Expr) bool {
	if other == nil {
		// if both are nil, they are equal
		return c == nil
	}

	if col, ok := other.(*DynamicColumn); ok {
		return c.ColumnName == col.ColumnName
	}

	return false
}

func (c *DynamicColumn) Clone() Expr {
	return &DynamicColumn{ColumnName: c.ColumnName}
}

func (c *DynamicColumn) Computed() bool {
	return false
}

func DynCol(name string) *DynamicColumn {
	return &DynamicColumn{ColumnName: name}
}

func (c *DynamicColumn) DataType(l ExprTypeFinder) (arrow.DataType, error) {
	t, err := l.DataTypeForExpr(c)
	if err != nil {
		return nil, fmt.Errorf("dynamic column %q type: %w", c.ColumnName, err)
	}

	return t, nil
}

func (c *DynamicColumn) ColumnsUsedExprs() []Expr {
	return []Expr{c}
}

func (c *DynamicColumn) MatchPath(path string) bool {
	return strings.HasPrefix(c.Name(), path)
}

func (c *DynamicColumn) MatchColumn(columnName string) bool {
	return strings.HasPrefix(columnName, c.ColumnName+".")
}

func (c *DynamicColumn) Name() string {
	return c.ColumnName
}

func (c *DynamicColumn) String() string { return c.Name() }

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

func (e *LiteralExpr) Equal(other Expr) bool {
	if other == nil {
		// if both are nil, they are equal
		return e == nil
	}

	if lit, ok := other.(*LiteralExpr); ok {
		return scalar.Equals(e.Value, lit.Value)
	}

	return false
}

func (e *LiteralExpr) Clone() Expr {
	return &LiteralExpr{
		Value: e.Value,
	}
}

func (e *LiteralExpr) Computed() bool {
	return false
}

func Literal(v interface{}) *LiteralExpr {
	return &LiteralExpr{
		Value: scalar.MakeScalar(v),
	}
}

func (e *LiteralExpr) DataType(_ ExprTypeFinder) (arrow.DataType, error) {
	return e.Value.DataType(), nil
}

func (e *LiteralExpr) Name() string {
	return e.Value.String()
}

func (e *LiteralExpr) String() string { return e.Name() }

func (e *LiteralExpr) Accept(visitor Visitor) bool {
	continu := visitor.PreVisit(e)
	if !continu {
		return false
	}

	return visitor.PostVisit(e)
}

func (e *LiteralExpr) ColumnsUsedExprs() []Expr { return nil }

func (e *LiteralExpr) MatchPath(path string) bool {
	return strings.HasPrefix(e.Name(), path)
}

func (e *LiteralExpr) MatchColumn(columnName string) bool {
	return e.Name() == columnName
}

type AggregationFunction struct {
	Func AggFunc
	Expr Expr
}

func (f *AggregationFunction) Equal(other Expr) bool {
	if other == nil {
		// if both are nil, they are equal
		return f == nil
	}

	if agg, ok := other.(*AggregationFunction); ok {
		return f.Func == agg.Func && f.Expr.Equal(agg.Expr)
	}

	return false
}

func (f *AggregationFunction) Clone() Expr {
	return &AggregationFunction{
		Func: f.Func,
		Expr: f.Expr.Clone(),
	}
}

func (f *AggregationFunction) DataType(l ExprTypeFinder) (arrow.DataType, error) {
	return f.Expr.DataType(l)
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

	continu = visitor.Visit(f)
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

func (f *AggregationFunction) String() string { return f.Name() }

func (f *AggregationFunction) ColumnsUsedExprs() []Expr {
	return f.Expr.ColumnsUsedExprs()
}

func (f *AggregationFunction) MatchColumn(columnName string) bool {
	return f.Name() == columnName
}

func (f *AggregationFunction) MatchPath(path string) bool {
	return strings.HasPrefix(f.Name(), path)
}

type AggFunc uint32

const (
	AggFuncUnknown AggFunc = iota
	AggFuncSum
	AggFuncMin
	AggFuncMax
	AggFuncCount
	AggFuncAvg
)

func (f AggFunc) String() string {
	switch f {
	case AggFuncSum:
		return "sum"
	case AggFuncMin:
		return "min"
	case AggFuncMax:
		return "max"
	case AggFuncCount:
		return "count"
	case AggFuncAvg:
		return "avg"
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

func Min(expr Expr) *AggregationFunction {
	return &AggregationFunction{
		Func: AggFuncMin,
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

func Avg(expr Expr) *AggregationFunction {
	return &AggregationFunction{
		Func: AggFuncAvg,
		Expr: expr,
	}
}

type AliasExpr struct {
	Expr  Expr
	Alias string
}

func (e *AliasExpr) Equal(other Expr) bool {
	if other == nil {
		// if both are nil, they are equal
		return e == nil
	}

	if alias, ok := other.(*AliasExpr); ok {
		return e.Alias == alias.Alias && e.Expr.Equal(alias.Expr)
	}

	return false
}

func (e *AliasExpr) Clone() Expr {
	return &AliasExpr{
		Expr:  e.Expr.Clone(),
		Alias: e.Alias,
	}
}

func (e *AliasExpr) DataType(l ExprTypeFinder) (arrow.DataType, error) {
	return e.Expr.DataType(l)
}

func (e *AliasExpr) Name() string {
	return e.Alias
}

func (e *AliasExpr) String() string { return fmt.Sprintf("%s as %s", e.Expr.String(), e.Alias) }

func (e *AliasExpr) Computed() bool {
	return e.Expr.Computed()
}

func (e *AliasExpr) ColumnsUsedExprs() []Expr {
	return e.Expr.ColumnsUsedExprs()
}

func (e *AliasExpr) MatchPath(path string) bool {
	return strings.HasPrefix(e.Name(), path)
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

func Duration(d time.Duration) *DurationExpr {
	return &DurationExpr{duration: d}
}

type DurationExpr struct {
	duration time.Duration
}

func (d *DurationExpr) Equal(other Expr) bool {
	if other == nil {
		// if both are nil, they are equal
		return d == nil
	}

	if dur, ok := other.(*DurationExpr); ok {
		return d.duration == dur.duration
	}

	return false
}

func (d *DurationExpr) Clone() Expr {
	return &DurationExpr{
		duration: d.duration,
	}
}

func (d *DurationExpr) DataType(_ ExprTypeFinder) (arrow.DataType, error) {
	return &arrow.DurationType{}, nil
}

func (d *DurationExpr) MatchPath(_ string) bool {
	return false
}

func (d *DurationExpr) Accept(visitor Visitor) bool {
	continu := visitor.PreVisit(d)
	if !continu {
		return false
	}

	return visitor.PostVisit(d)
}

func (d *DurationExpr) Name() string {
	return fmt.Sprintf("second(%d)", int(d.duration.Seconds()))
}

func (d *DurationExpr) String() string { return d.Name() }

func (d *DurationExpr) ColumnsUsedExprs() []Expr {
	// DurationExpr expect to work on a timestamp column
	return []Expr{Col("timestamp")}
}

func (d *DurationExpr) MatchColumn(columnName string) bool {
	return columnName == "timestamp"
}

func (d *DurationExpr) Computed() bool {
	return false
}

func (d *DurationExpr) Value() time.Duration {
	return d.duration
}

type AllExpr struct{}

func All() *AllExpr {
	return &AllExpr{}
}

func (a *AllExpr) Equal(other Expr) bool {
	if other == nil {
		// if both are nil, they are equal
		return a == nil
	}

	_, ok := other.(*AllExpr)
	return ok
}

func (a *AllExpr) DataType(ExprTypeFinder) (arrow.DataType, error) { return nil, nil }
func (a *AllExpr) Accept(visitor Visitor) bool {
	continu := visitor.PreVisit(a)
	if !continu {
		return false
	}

	return visitor.PostVisit(a)
}
func (a *AllExpr) Name() string   { return "all" }
func (a *AllExpr) String() string { return a.Name() }
func (a *AllExpr) ColumnsUsedExprs() []Expr {
	return []Expr{&AllExpr{}}
}
func (a *AllExpr) MatchColumn(_ string) bool { return true }
func (a *AllExpr) MatchPath(_ string) bool   { return true }
func (a *AllExpr) Computed() bool            { return false }
func (a *AllExpr) Clone() Expr               { return &AllExpr{} }

type NotExpr struct {
	Expr Expr
}

func Not(expr Expr) *NotExpr {
	return &NotExpr{
		Expr: expr,
	}
}

func (n *NotExpr) Equal(other Expr) bool {
	if other == nil {
		// if both are nil, they are equal
		return n == nil
	}

	if not, ok := other.(*NotExpr); ok {
		return n.Expr.Equal(not.Expr)
	}

	return false
}

func (n *NotExpr) DataType(ExprTypeFinder) (arrow.DataType, error) { return nil, nil }
func (n *NotExpr) Accept(visitor Visitor) bool {
	continu := visitor.PreVisit(n)
	if !continu {
		return false
	}

	return visitor.PostVisit(n)
}
func (n *NotExpr) Name() string   { return "not " + n.Expr.Name() }
func (n *NotExpr) String() string { return n.Name() }
func (n *NotExpr) ColumnsUsedExprs() []Expr {
	return []Expr{&NotExpr{Expr: n.Expr}}
}
func (n *NotExpr) MatchColumn(columnName string) bool { return !n.Expr.MatchColumn(columnName) }
func (n *NotExpr) MatchPath(path string) bool         { return !n.Expr.MatchPath(path) }
func (n *NotExpr) Computed() bool                     { return false }
func (n *NotExpr) Clone() Expr                        { return &NotExpr{Expr: n.Expr} }
