package expr

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow/go/v15/arrow/scalar"
	"github.com/parquet-go/parquet-go"

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
	// Eval should be safe to call concurrently.
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
	case logicalplan.OpEq: // , logicalplan.OpNotEq, logicalplan.OpLt, logicalplan.OpLtEq, logicalplan.OpGt, logicalplan.OpGtEq, logicalplan.OpRegexMatch, logicalplan.RegexNotMatch:
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
			rightValue scalar.Scalar
			err        error
		)
		expr.Right.Accept(PreExprVisitorFunc(func(expr logicalplan.Expr) bool {
			switch e := expr.(type) {
			case *logicalplan.LiteralExpr:
				rightValue = e.Value
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

func aggregationExpr(expr *logicalplan.AggregationFunction) (TrueNegativeFilter, error) {
	switch expr.Func {
	case logicalplan.AggFuncMax:
		a := &MaxAgg{}
		expr.Expr.Accept(PreExprVisitorFunc(func(expr logicalplan.Expr) bool {
			switch e := expr.(type) {
			case *logicalplan.Column:
				a.columnName = e.ColumnName
			case *logicalplan.DynamicColumn:
				a.columnName = e.ColumnName
				a.dynamic = true
			default:
				return true
			}
			return false
		}))
		return a, nil
	default:
		return &AlwaysTrueFilter{}, nil
	}
}

type MaxAgg struct {
	maxMap sync.Map

	// It would be nicer to use a dynamic-aware ColumnRef here, but that would
	// introduce allocations (slices for indexes and concrete names), so for
	// performance reasons we execute the column lookup manually.
	columnName string
	dynamic    bool
}

func (a *MaxAgg) Eval(p Particulate) (bool, error) {
	processFurther := false
	for i, f := range p.Schema().Fields() {
		if (a.dynamic && !strings.HasPrefix(f.Name(), a.columnName+".")) || (!a.dynamic && f.Name() != a.columnName) {
			continue
		}

		chunk := p.ColumnChunks()[i]
		index, err := chunk.ColumnIndex()
		if err != nil {
			return false, fmt.Errorf("error retrieving column index in MaxAgg.Eval")
		}
		if NullCount(index) == chunk.NumValues() {
			// This page is full of nulls. Nothing to do since we can't trust
			// min/max index values. This chunk should not be processed since
			// it can't contribute to min/max unless another column can.
			continue
		}

		columnPointer, _ := a.maxMap.LoadOrStore(f.Name(), &atomic.Pointer[parquet.Value]{})
		atomicMax := columnPointer.(*atomic.Pointer[parquet.Value])

		v := Max(index)
		for globalMax := atomicMax.Load(); globalMax == nil || compareParquetValues(v, *globalMax) > 0; globalMax = atomicMax.Load() {
			if atomicMax.CompareAndSwap(globalMax, &v) {
				// At least one column exceeded the current max so this chunk
				// satisfies the filter. Note that we do not break out of
				// scanning the rest of the columns since we do want to memoize
				// the max for other columns as well.
				processFurther = true
				break
			}
		}
		if !a.dynamic && processFurther {
			// No need to look at the remaining columns if we're only looking
			// for a single concrete column.
			break
		}
	}
	return processFurther, nil
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
	case *logicalplan.AggregationFunction:
		// NOTE: Aggregations are optimized in the case of no grouping columns
		// or other filters.
		return aggregationExpr(e)
	default:
		return nil, fmt.Errorf("unsupported boolean expression %T", e)
	}
}
