package physicalplan

import (
	"context"
	"errors"
	"fmt"
	"regexp"

	"github.com/RoaringBitmap/roaring"
	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/apache/arrow/go/v10/arrow/scalar"
	"go.opentelemetry.io/otel/trace"

	"github.com/polarsignals/frostdb/query/logicalplan"
)

type PredicateFilter struct {
	pool       memory.Allocator
	tracer     trace.Tracer
	filterExpr BooleanExpression
	next       PhysicalPlan
}

func (f *PredicateFilter) Draw() *Diagram {
	var child *Diagram
	if f.next != nil {
		child = f.next.Draw()
	}
	details := fmt.Sprintf("PredicateFilter (%s)", f.filterExpr.String())
	return &Diagram{Details: details, Child: child}
}

type Bitmap = roaring.Bitmap

func NewBitmap() *Bitmap {
	return roaring.New()
}

type BooleanExpression interface {
	Eval(r arrow.Record) (*Bitmap, error)
	String() string
}

var ErrUnsupportedBooleanExpression = errors.New("unsupported boolean expression")

type ArrayReference struct{}

type PreExprVisitorFunc func(expr logicalplan.Expr) bool

func (f PreExprVisitorFunc) PreVisit(expr logicalplan.Expr) bool {
	return f(expr)
}

func (f PreExprVisitorFunc) PostVisit(expr logicalplan.Expr) bool {
	return false
}

func binaryBooleanExpr(expr *logicalplan.BinaryExpr) (BooleanExpression, error) {
	switch expr.Op {
	case logicalplan.OpEq, logicalplan.OpNotEq, logicalplan.OpLt, logicalplan.OpLtEq, logicalplan.OpGt, logicalplan.OpGtEq, logicalplan.OpRegexMatch, logicalplan.OpRegexNotMatch:
		var leftColumnRef *ArrayRef
		expr.Left.Accept(PreExprVisitorFunc(func(expr logicalplan.Expr) bool {
			switch e := expr.(type) {
			case *logicalplan.Column:
				leftColumnRef = &ArrayRef{
					ColumnName: e.ColumnName,
				}
				return false
			}
			return true
		}))
		if leftColumnRef == nil {
			return nil, errors.New("left side of binary expression must be a column")
		}

		var rightScalar scalar.Scalar
		expr.Right.Accept(PreExprVisitorFunc(func(expr logicalplan.Expr) bool {
			switch e := expr.(type) {
			case *logicalplan.LiteralExpr:
				rightScalar = e.Value
				return false
			}
			return true
		}))

		switch expr.Op {
		case logicalplan.OpRegexMatch:
			regexp, err := regexp.Compile(string(rightScalar.(*scalar.String).Data()))
			if err != nil {
				return nil, err
			}
			return &RegExpFilter{
				left:  leftColumnRef,
				right: regexp,
			}, nil
		case logicalplan.OpRegexNotMatch:
			regexp, err := regexp.Compile(string(rightScalar.(*scalar.String).Data()))
			if err != nil {
				return nil, err
			}
			return &RegExpFilter{
				left:     leftColumnRef,
				right:    regexp,
				notMatch: true,
			}, nil
		}

		return &BinaryScalarExpr{
			Left:  leftColumnRef,
			Op:    expr.Op,
			Right: rightScalar,
		}, nil
	case logicalplan.OpAnd:
		left, err := booleanExpr(expr.Left)
		if err != nil {
			return nil, err
		}

		right, err := booleanExpr(expr.Right)
		if err != nil {
			return nil, err
		}

		return &AndExpr{
			Left:  left,
			Right: right,
		}, nil
	case logicalplan.OpOr:
		left, err := booleanExpr(expr.Left)
		if err != nil {
			return nil, err
		}

		right, err := booleanExpr(expr.Right)
		if err != nil {
			return nil, err
		}

		return &OrExpr{
			Left:  left,
			Right: right,
		}, nil
	case logicalplan.OpAdd, logicalplan.OpSub, logicalplan.OpMul, logicalplan.OpDiv:
		var leftColumnRef *ArrayRef
		var leftScalar scalar.Scalar
		expr.Left.Accept(PreExprVisitorFunc(func(expr logicalplan.Expr) bool {
			switch e := expr.(type) {
			case *logicalplan.LiteralExpr:
				leftScalar = e.Value
				return false
			case *logicalplan.Column:
				leftColumnRef = &ArrayRef{
					ColumnName: e.ColumnName,
				}
				return false
			}
			return true
		}))
		if leftColumnRef == nil && leftScalar == nil {
			return nil, errors.New("left side of arithmetic expression must be a column or a scalar")
		}

		var rightColumnRef *ArrayRef
		var rightScalar scalar.Scalar
		expr.Right.Accept(PreExprVisitorFunc(func(expr logicalplan.Expr) bool {
			switch e := expr.(type) {
			case *logicalplan.LiteralExpr:
				rightScalar = e.Value
				return false
			case *logicalplan.Column:
				rightColumnRef = &ArrayRef{
					ColumnName: e.ColumnName,
				}
				return false
			}
			return true
		}))
		if rightColumnRef == nil && rightScalar == nil {
			return nil, errors.New("right side of arithmetic expression must be a column or a scalar")
		}

		return &ArithmeticExpr{
			left:        leftColumnRef,
			leftScalar:  leftScalar,
			right:       rightColumnRef,
			rightScalar: rightScalar,
			operation:   expr.Op.String(),
		}, nil
	default:
		panic("unsupported binary boolean expression")
	}
}

type AndExpr struct {
	Left  BooleanExpression
	Right BooleanExpression
}

func (a *AndExpr) Eval(r arrow.Record) (*Bitmap, error) {
	left, err := a.Left.Eval(r)
	if err != nil {
		return nil, err
	}

	right, err := a.Right.Eval(r)
	if err != nil {
		return nil, err
	}

	// This stores the result in place to avoid allocations.
	left.And(right)
	return left, nil
}

func (a *AndExpr) String() string {
	return "(" + a.Left.String() + " AND " + a.Right.String() + ")"
}

type OrExpr struct {
	Left  BooleanExpression
	Right BooleanExpression
}

func (a *OrExpr) Eval(r arrow.Record) (*Bitmap, error) {
	left, err := a.Left.Eval(r)
	if err != nil {
		return nil, err
	}

	right, err := a.Right.Eval(r)
	if err != nil {
		return nil, err
	}

	// This stores the result in place to avoid allocations.
	left.Or(right)
	return left, nil
}

func (a *OrExpr) String() string {
	return "(" + a.Left.String() + " OR " + a.Right.String() + ")"
}

type ArithmeticExpr struct {
	operation   string
	left        *ArrayRef
	leftScalar  scalar.Scalar
	right       *ArrayRef
	rightScalar scalar.Scalar
}

func (m *ArithmeticExpr) Eval(r arrow.Record) (*Bitmap, error) {
	return nil, fmt.Errorf("not used")
}

func (m *ArithmeticExpr) Project(mem memory.Allocator, r arrow.Record) ([]arrow.Field, []arrow.Array, error) {
	schema := r.Schema()

	fields := make([]arrow.Field, 0, len(schema.Fields())-1)
	columns := make([]arrow.Array, 0, len(schema.Fields())-1)

	if m.leftScalar != nil && m.rightScalar != nil {
		left := m.leftScalar.(*scalar.Int64).Value
		right := m.rightScalar.(*scalar.Int64).Value

		res := array.NewInt64Builder(mem)
		switch m.operation {
		case "+":
			// Add the two scalars and return a single value as often as the number of rows.
			for i := int64(0); i < r.NumRows(); i++ {
				res.Append(left + right)
			}
		case "-":
			// Subtract the two scalars and return a single value as often as the number of rows.
			for i := int64(0); i < r.NumRows(); i++ {
				res.Append(left - right)
			}
		case "*":
			// Multiply the two scalars and return a single value as often as the number of rows.
			for i := int64(0); i < r.NumRows(); i++ {
				res.Append(left * right)
			}
		case "/":
			// Divide the two scalars and return a single value as often as the number of rows.
			for i := int64(0); i < r.NumRows(); i++ {
				res.Append(left / right)
			}
		default:
			return nil, nil, fmt.Errorf("unsupported operation %s", m.operation)
		}

		fields = append(fields, arrow.Field{Name: m.String(), Type: arrow.PrimitiveTypes.Int64})
		columns = append(columns, res.NewArray())

		return fields, columns, nil
	}

	if (m.left != nil && m.rightScalar != nil) || (m.right != nil && m.leftScalar != nil) {
		columnName := ""
		if m.left != nil {
			columnName = m.left.ColumnName
		}
		if m.right != nil {
			columnName = m.right.ColumnName
		}
		arrIndex := schema.FieldIndices(columnName)
		if len(arrIndex) != 1 {
			return nil, nil, fmt.Errorf("%s column for %s expression not found", columnName, m.operation)
		}

		arr := r.Column(arrIndex[0])

		// Exclude the arr column from the projection.
		for i, field := range schema.Fields() {
			if i != arrIndex[0] {
				fields = append(fields, field)
				columns = append(columns, r.Column(i))
			}
		}

		var s int64
		var side scalarSide
		if m.leftScalar != nil {
			side = left
			s = m.leftScalar.(*scalar.Int64).Value
		}
		if m.rightScalar != nil {
			side = right
			s = m.rightScalar.(*scalar.Int64).Value
		}

		if m.operation == "/" && side == right && s == 0 {
			return nil, nil, fmt.Errorf("division by zero")
		}

		fields = append(fields, arrow.Field{Name: m.String(), Type: arr.DataType()})
		columns = append(columns, arithmeticInt64ArraysScalar(mem, arr, s, m.operation, side))

		return fields, columns, nil
	}

	if m.left != nil && m.right != nil {
		leftIndex := schema.FieldIndices(m.left.ColumnName)
		rightIndex := schema.FieldIndices(m.right.ColumnName)
		if len(leftIndex) != 1 {
			return nil, nil, fmt.Errorf("%s column for %s expression not found", m.left.ColumnName, m.operation)
		}
		if len(rightIndex) != 1 {
			return nil, nil, fmt.Errorf("%s column for %s expression not found", m.right.ColumnName, m.operation)
		}

		left := r.Column(leftIndex[0])
		right := r.Column(rightIndex[0])

		// Only add the fields and columns that aren't the average's underlying sum and count columns.
		for i, field := range schema.Fields() {
			if i != leftIndex[0] && i != rightIndex[0] {
				fields = append(fields, field)
				columns = append(columns, r.Column(i))
			}
		}

		// Add the field and column for the projected average aggregation.
		fields = append(fields, arrow.Field{
			Name: m.String(),
			Type: &arrow.Int64Type{},
		})
		columns = append(columns, arithmeticInt64arrays(mem, left, right, m.operation))

		return fields, columns, nil
	}

	return nil, nil, fmt.Errorf("unsupported arithmetic expression: %s", m)
}

func (m *ArithmeticExpr) String() string {
	leftString := ""
	if m.left != nil {
		leftString = m.left.ColumnName
	}
	if m.leftScalar != nil {
		leftString = m.leftScalar.String()
	}
	rightString := ""
	if m.right != nil {
		rightString = m.right.ColumnName
	}
	if m.rightScalar != nil {
		rightString = m.rightScalar.String()
	}

	return leftString + m.operation + rightString
}

func arithmeticInt64arrays(pool memory.Allocator, left, right arrow.Array, operation string) arrow.Array {
	leftInts := left.(*array.Int64)
	rightInts := right.(*array.Int64)

	res := array.NewInt64Builder(pool)

	switch operation {
	case "+":
		for i := 0; i < leftInts.Len(); i++ {
			res.Append(leftInts.Value(i) + rightInts.Value(i))
		}
	case "-":
		for i := 0; i < leftInts.Len(); i++ {
			res.Append(leftInts.Value(i) - rightInts.Value(i))
		}
	case "*":
		for i := 0; i < leftInts.Len(); i++ {
			res.Append(leftInts.Value(i) * rightInts.Value(i))
		}
	case "/":
		for i := 0; i < leftInts.Len(); i++ {
			res.Append(leftInts.Value(i) / rightInts.Value(i))
		}
	default:
		panic(fmt.Sprintf("unsupported operation %s", operation))
	}

	return res.NewArray()
}

type scalarSide bool

const (
	left  scalarSide = false
	right scalarSide = true
)

func arithmeticInt64ArraysScalar(mem memory.Allocator, arr arrow.Array, scalar int64, operation string, side scalarSide) arrow.Array {
	ints := arr.(*array.Int64)
	res := array.NewInt64Builder(mem)

	switch operation {
	case "+":
		for i := 0; i < ints.Len(); i++ {
			res.Append(ints.Value(i) + scalar)
		}
	case "-":
		// For subtraction, we need to know which side of the expression is the scalar.
		if side == left {
			for i := 0; i < ints.Len(); i++ {
				res.Append(scalar - ints.Value(i))
			}
		} else {
			for i := 0; i < ints.Len(); i++ {
				res.Append(ints.Value(i) - scalar)
			}
		}
	case "*":
		for i := 0; i < ints.Len(); i++ {
			res.Append(ints.Value(i) * scalar)
		}
	case "/":
		// For division, we need to know which side of the expression is the scalar.
		if side == left {
			for i := 0; i < ints.Len(); i++ {
				res.Append(scalar / ints.Value(i))
			}
		} else {
			for i := 0; i < ints.Len(); i++ {
				res.Append(ints.Value(i) / scalar)
			}
		}
	default:
		panic(fmt.Sprintf("unsupported operation %s", operation))

	}

	return res.NewArray()
}

func booleanExpr(expr logicalplan.Expr) (BooleanExpression, error) {
	switch e := expr.(type) {
	case *logicalplan.BinaryExpr:
		return binaryBooleanExpr(e)
	default:
		return nil, ErrUnsupportedBooleanExpression
	}
}

func Filter(pool memory.Allocator, tracer trace.Tracer, filterExpr logicalplan.Expr) (*PredicateFilter, error) {
	expr, err := booleanExpr(filterExpr)
	if err != nil {
		return nil, err
	}

	return newFilter(pool, tracer, expr), nil
}

func newFilter(pool memory.Allocator, tracer trace.Tracer, filterExpr BooleanExpression) *PredicateFilter {
	return &PredicateFilter{
		pool:       pool,
		tracer:     tracer,
		filterExpr: filterExpr,
	}
}

func (f *PredicateFilter) SetNext(next PhysicalPlan) {
	f.next = next
}

func (f *PredicateFilter) Callback(ctx context.Context, r arrow.Record) error {
	// Generates high volume of spans. Comment out if needed during development.
	// ctx, span := f.tracer.Start(ctx, "PredicateFilter/Callback")
	// defer span.End()

	filtered, empty, err := filter(f.pool, f.filterExpr, r)
	if err != nil {
		return err
	}
	if empty {
		return nil
	}

	defer filtered.Release()
	return f.next.Callback(ctx, filtered)
}

func (f *PredicateFilter) Finish(ctx context.Context) error {
	return f.next.Finish(ctx)
}

func filter(pool memory.Allocator, filterExpr BooleanExpression, ar arrow.Record) (arrow.Record, bool, error) {
	bitmap, err := filterExpr.Eval(ar)
	if err != nil {
		return nil, true, err
	}

	if bitmap.IsEmpty() {
		return nil, true, nil
	}

	indicesToKeep := bitmap.ToArray()
	ranges := buildIndexRanges(indicesToKeep)

	totalRows := int64(0)
	recordRanges := make([]arrow.Record, len(ranges))
	for j, r := range ranges {
		recordRanges[j] = ar.NewSlice(int64(r.Start), int64(r.End))
		totalRows += int64(r.End - r.Start)
	}

	cols := make([]arrow.Array, ar.NumCols())
	numRanges := len(recordRanges)
	for i := range cols {
		colRanges := make([]arrow.Array, 0, numRanges)
		for _, rr := range recordRanges {
			colRanges = append(colRanges, rr.Column(i))
		}

		cols[i], err = array.Concatenate(colRanges, pool)
		if err != nil {
			return nil, true, err
		}
	}

	return array.NewRecord(ar.Schema(), cols, totalRows), false, nil
}

type IndexRange struct {
	Start uint32
	End   uint32
}

// buildIndexRanges returns a set of continguous index ranges from the given indicies
// ex: [1,2,7,8,9] would return [{Start:1, End:3},{Start:7,End:10}]
func buildIndexRanges(indices []uint32) []IndexRange {
	ranges := []IndexRange{}

	cur := IndexRange{
		Start: indices[0],
		End:   indices[0] + 1,
	}

	for _, i := range indices[1:] {
		if i == cur.End {
			cur.End++
		} else {
			ranges = append(ranges, cur)
			cur = IndexRange{
				Start: i,
				End:   i + 1,
			}
		}
	}

	ranges = append(ranges, cur)
	return ranges
}
