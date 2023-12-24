package expr

import (
	"errors"
	"fmt"

	"github.com/parquet-go/parquet-go"

	"github.com/polarsignals/frostdb/pqarrow"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

type PreExprVisitorFunc func(expr logicalplan.Expr) bool

func (f PreExprVisitorFunc) PreVisit(expr logicalplan.Expr) bool {
	return f(expr)
}

func (f PreExprVisitorFunc) Visit(_ logicalplan.Expr) bool {
	return false
}

func (f PreExprVisitorFunc) PostVisit(_ logicalplan.Expr) bool {
	return false
}

// Particulate is an abstraction of something that can be filtered.
// A parquet.RowGroup is a particulate that is able to be filtered, and wrapping a parquet.File with
// ParquetFileParticulate allows a file to be filtered.
type Particulate interface {
	Schema() *parquet.Schema
	ColumnChunks() []parquet.ColumnChunk
}

type TrueNegativeFilter interface {
	Eval(Particulate) (bool, error)
}

type AlwaysTrueFilter struct{}

func (f *AlwaysTrueFilter) Eval(_ Particulate) (bool, error) {
	return true, nil
}

func binaryBooleanExpr(expr *logicalplan.BinaryExpr) (TrueNegativeFilter, error) {
	switch expr.Op {
	case logicalplan.OpNotEq:
		fallthrough
	case logicalplan.OpLt:
		fallthrough
	case logicalplan.OpLtEq:
		fallthrough
	case logicalplan.OpGt:
		fallthrough
	case logicalplan.OpGtEq:
		fallthrough
	case logicalplan.OpEq: //, logicalplan.OpNotEq, logicalplan.OpLt, logicalplan.OpLtEq, logicalplan.OpGt, logicalplan.OpGtEq, logicalplan.OpRegexMatch, logicalplan.RegexNotMatch:
		var leftColumnRef *ColumnRef
		expr.Left.Accept(PreExprVisitorFunc(func(expr logicalplan.Expr) bool {
			switch e := expr.(type) {
			case *logicalplan.Column:
				leftColumnRef = &ColumnRef{
					ColumnName: e.ColumnName,
				}
				return false
			}
			return true
		}))
		if leftColumnRef == nil {
			return nil, errors.New("left side of binary expression must be a column")
		}

		var (
			rightValue parquet.Value
			err        error
		)
		expr.Right.Accept(PreExprVisitorFunc(func(expr logicalplan.Expr) bool {
			switch e := expr.(type) {
			case *logicalplan.LiteralExpr:
				rightValue, err = pqarrow.ArrowScalarToParquetValue(e.Value)
				return false
			}
			return true
		}))

		if err != nil {
			return nil, err
		}

		return &BinaryScalarExpr{
			Left:  leftColumnRef,
			Op:    expr.Op,
			Right: rightValue,
		}, nil
	case logicalplan.OpAnd:
		left, err := BooleanExpr(expr.Left)
		if err != nil {
			return nil, err
		}

		right, err := BooleanExpr(expr.Right)
		if err != nil {
			return nil, err
		}

		return &AndExpr{
			Left:  left,
			Right: right,
		}, nil
	case logicalplan.OpOr:
		left, err := BooleanExpr(expr.Left)
		if err != nil {
			return nil, err
		}

		right, err := BooleanExpr(expr.Right)
		if err != nil {
			return nil, err
		}

		return &OrExpr{
			Left:  left,
			Right: right,
		}, nil
	default:
		return &AlwaysTrueFilter{}, nil
	}
}

type AndExpr struct {
	Left  TrueNegativeFilter
	Right TrueNegativeFilter
}

func (a *AndExpr) Eval(p Particulate) (bool, error) {
	left, err := a.Left.Eval(p)
	if err != nil {
		return false, err
	}
	if !left {
		return false, nil
	}

	right, err := a.Right.Eval(p)
	if err != nil {
		return false, err
	}
	return right, nil
}

type OrExpr struct {
	Left  TrueNegativeFilter
	Right TrueNegativeFilter
}

func (a *OrExpr) Eval(p Particulate) (bool, error) {
	left, err := a.Left.Eval(p)
	if err != nil {
		return false, err
	}
	if left {
		return true, nil
	}

	right, err := a.Right.Eval(p)
	if err != nil {
		return false, err
	}

	return right, nil
}

func BooleanExpr(expr logicalplan.Expr) (TrueNegativeFilter, error) {
	if expr == nil {
		return &AlwaysTrueFilter{}, nil
	}

	switch e := expr.(type) {
	case *logicalplan.BinaryExpr:
		return binaryBooleanExpr(e)
	default:
		return nil, fmt.Errorf("unsupported boolean expression %T", e)
	}
}
