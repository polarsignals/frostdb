package sqlparse

import (
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow/go/v14/arrow/scalar"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/parser/test_driver"

	"github.com/polarsignals/frostdb/query"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

type astVisitor struct {
	explain     bool
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
	switch expr := n.(type) {
	case *ast.SelectStmt:
		// The SelectStmt is handled in during pre-visit given that it has many
		// clauses we need to handle independently (e.g. a group by with a
		// filter).
		if expr.Where != nil {
			expr.Where.Accept(v)
			lastExpr, newExprs := pop(v.exprStack)
			v.exprStack = newExprs
			v.builder = v.builder.Filter(lastExpr)
		}
		expr.Fields.Accept(v)
		switch {
		case expr.GroupBy != nil:
			expr.GroupBy.Accept(v)
			var agg []logicalplan.Expr
			var groups []logicalplan.Expr

			for _, expr := range v.exprStack {
				switch expr.(type) {
				case *logicalplan.AliasExpr, *logicalplan.AggregationFunction:
					agg = append(agg, expr)
				default:
					groups = append(groups, expr)
				}
			}
			v.builder = v.builder.Aggregate(agg, groups)
		case expr.Distinct:
			v.builder = v.builder.Distinct(v.exprStack...)
		default:
			v.builder = v.builder.Project(v.exprStack...)
		}
		return n, true
	}
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
		// Handled in Enter.
		return nil
	case *ast.ExplainStmt:
		v.explain = true
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
		case "min":
			v.exprStack[lastExpr] = logicalplan.Min(v.exprStack[lastExpr])
		case "max":
			v.exprStack[lastExpr] = logicalplan.Max(v.exprStack[lastExpr])
		case "avg":
			v.exprStack[lastExpr] = logicalplan.Avg(v.exprStack[lastExpr])
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
		case opcode.LT:
			frostDBOp = logicalplan.OpLt
		case opcode.GE:
			frostDBOp = logicalplan.OpGtEq
		case opcode.LE:
			frostDBOp = logicalplan.OpLtEq
		case opcode.EQ:
			frostDBOp = logicalplan.OpEq
		case opcode.NE:
			frostDBOp = logicalplan.OpNotEq
		case opcode.LogicAnd:
			v.exprStack = append(v.exprStack, logicalplan.And(leftExpr, rightExpr))
			return nil
		case opcode.LogicOr:
			v.exprStack = append(v.exprStack, logicalplan.Or(leftExpr, rightExpr))
			return nil
		}
		v.exprStack = append(v.exprStack, &logicalplan.BinaryExpr{
			Left:  logicalplan.Col(leftExpr.Name()),
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
		switch logicalplan.Literal(expr.GetValue()).Name() { // NOTE: special case for boolean fields since the mysql parser doesn't support booleans as a type
		case "true":
			v.exprStack = append(v.exprStack, logicalplan.Literal(true))
		case "false":
			v.exprStack = append(v.exprStack, logicalplan.Literal(false))
		default:
			v.exprStack = append(v.exprStack, logicalplan.Literal(expr.GetValue()))
		}
	case *ast.SelectField:
		if as := expr.AsName.String(); as != "" {
			lastExpr := len(v.exprStack) - 1
			v.exprStack[lastExpr] = v.exprStack[lastExpr].(*logicalplan.AggregationFunction).Alias(as) // TODO should probably just be an alias expr and not from an aggregate function
		}
	case *ast.PatternRegexpExpr:
		rightExpr, newExprs := pop(v.exprStack)
		leftExpr, newExprs := pop(newExprs)
		v.exprStack = newExprs

		e := &logicalplan.BinaryExpr{
			Left:  logicalplan.Col(leftExpr.Name()),
			Op:    logicalplan.OpRegexMatch,
			Right: rightExpr,
		}
		if expr.Not {
			e.Op = logicalplan.OpRegexNotMatch
		}
		v.exprStack = append(v.exprStack, e)
	case *ast.FieldList, *ast.ColumnNameExpr, *ast.GroupByClause, *ast.ByItem, *ast.RowExpr,
		*ast.ParenthesesExpr:
		// Deliberate pass-through nodes.
	case *ast.FuncCallExpr:
		switch expr.FnName.String() {
		case "take":
			left, right := pop(v.exprStack)
			var exprStack []logicalplan.Expr
			// exprStack = append(exprStack, right...)
			switch l := left.(type) {
			case *logicalplan.LiteralExpr:
				val := l.Value.(*scalar.Int64)
				limit := uint64(val.Value)
				exprStack = append(exprStack, logicalplan.Take(right[0], limit))
				v.exprStack = exprStack
			}

		case ast.Second:
			// This is pretty hacky and only fine because it's in the test only.
			left, right := pop(v.exprStack)
			var exprStack []logicalplan.Expr
			exprStack = append(exprStack, right...)
			switch l := left.(type) {
			case *logicalplan.LiteralExpr:
				val := l.Value.(*scalar.Int64)
				duration := time.Duration(val.Value) * time.Second
				exprStack = append(exprStack, logicalplan.Duration(duration))
				v.exprStack = exprStack
			}
		default:
			return fmt.Errorf("unhandled func call: %s", expr.FnName.String())
		}
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
