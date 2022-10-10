package sqlparse

import (
	"fmt"
	"strings"

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/parser/test_driver"

	"github.com/polarsignals/frostdb/query"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

type astVisitor struct {
	builder     query.Builder
	dynColNames map[string]struct{}
	err         error

	exprStack []logicalplan.Expr
}

var _ ast.Visitor = &astVisitor{}

func newASTVisitor(builder query.Builder, dynColNames []string) *astVisitor {
	dynMap := make(map[string]struct{})
	for _, n := range dynColNames {
		dynMap[n] = struct{}{}
	}
	return &astVisitor{
		builder:     builder,
		dynColNames: dynMap,
	}
}

func (v *astVisitor) Enter(n ast.Node) (nRes ast.Node, skipChildren bool) {
	return n, false
}

func (v *astVisitor) Leave(n ast.Node) (nRes ast.Node, ok bool) {
	if err := v.leaveImpl(n); err != nil {
		v.err = err
		return n, false
	}
	return n, true
}

func (v *astVisitor) leaveImpl(n ast.Node) error {
	switch expr := n.(type) {
	case *ast.SelectStmt:
		if expr.Where != nil {
			lastExpr, newExprs := pop(v.exprStack)
			v.exprStack = newExprs
			v.builder = v.builder.Filter(lastExpr)
		}
		switch {
		case expr.Distinct:
			v.builder = v.builder.Distinct(v.exprStack...)
		case expr.GroupBy != nil:
			v.builder = v.builder.Aggregate(
				// Aggregation function is evaluated first.
				v.exprStack[0],
				v.exprStack[1:]...,
			)
		}
		// Reset for safety.
		v.exprStack = v.exprStack[:0]
		return nil
	case *ast.AggregateFuncExpr:
		// At this point, the child node is the column name, so it has just been
		// added to exprs.
		lastExpr := len(v.exprStack) - 1
		switch strings.ToLower(expr.F) {
		case "count":
			v.exprStack[lastExpr] = logicalplan.Count(v.exprStack[lastExpr])
		case "sum":
			v.exprStack[lastExpr] = logicalplan.Sum(v.exprStack[lastExpr])
		case "max":
			v.exprStack[lastExpr] = logicalplan.Max(v.exprStack[lastExpr])
		default:
			return fmt.Errorf("unhandled aggregate function %s", expr.F)
		}
	case *ast.BinaryOperationExpr:
		// Note that we're resolving exprs as a stack, so the last two
		// expressions are the leaf expressions.
		rightExpr, newExprs := pop(v.exprStack)
		leftExpr, newExprs := pop(newExprs)
		v.exprStack = newExprs

		var frostDBOp logicalplan.Op
		switch expr.Op {
		case opcode.GT:
			frostDBOp = logicalplan.OpGt
		case opcode.EQ:
			frostDBOp = logicalplan.OpEq
		case opcode.LogicAnd:
			v.exprStack = append(v.exprStack, logicalplan.And(leftExpr, rightExpr))
			return nil
		}
		v.exprStack = append(v.exprStack, &logicalplan.BinaryExpr{
			Left:  leftExpr,
			Op:    frostDBOp,
			Right: rightExpr,
		})
	case *ast.ColumnName:
		colName := columnNameToString(expr)
		var col logicalplan.Expr
		if _, ok := v.dynColNames[colName]; ok {
			col = logicalplan.DynCol(colName)
		} else {
			col = logicalplan.Col(colName)
		}
		v.exprStack = append(v.exprStack, col)
	case *test_driver.ValueExpr:
		v.exprStack = append(v.exprStack, logicalplan.Literal(expr.GetValue()))
	case *ast.SelectField:
		if as := expr.AsName.String(); as != "" {
			lastExpr := len(v.exprStack) - 1
			v.exprStack[lastExpr] = v.exprStack[lastExpr].(*logicalplan.AggregationFunction).Alias(as)
		}
	case *ast.FieldList, *ast.ColumnNameExpr, *ast.GroupByClause, *ast.ByItem, *ast.RowExpr,
		*ast.ParenthesesExpr:
		// Deliberate pass-through nodes.
	default:
		return fmt.Errorf("unhandled ast node %T", expr)
	}
	return nil
}

func columnNameToString(c *ast.ColumnName) string {
	// Note that in SQL labels.label2 is interpreted as referencing
	// the label2 column of a table called labels. In our case,
	// these are dynamic columns, which is why the table name is
	// accessed here.
	colName := ""
	if c.Table.String() != "" {
		colName = c.Table.String() + "."
	}
	colName += c.Name.String()
	return colName
}

func pop[T any](s []T) (T, []T) {
	lastIdx := len(s) - 1
	return s[lastIdx], s[:lastIdx]
}
