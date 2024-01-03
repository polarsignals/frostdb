package physicalplan

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"go.opentelemetry.io/otel/trace"

	"github.com/polarsignals/frostdb/query/logicalplan"
)

type columnProjection interface {
	// Name returns the name of the column that this projection will return.
	Name() string
	// Projects one or more columns. Each element in the field list corresponds to an element in the list of arrays.
	Project(mem memory.Allocator, ar arrow.Record) ([]arrow.Field, []arrow.Array, error)
}

type aliasProjection struct {
	expr *logicalplan.AliasExpr
	name string
}

func (a aliasProjection) Name() string {
	return a.name
}

func (a aliasProjection) Project(mem memory.Allocator, ar arrow.Record) ([]arrow.Field, []arrow.Array, error) {
	switch e := a.expr.Expr.(type) {
	case *logicalplan.BinaryExpr:
		boolExpr, err := binaryBooleanExpr(e)
		if err != nil {
			return nil, nil, err
		}
		fields, array, err := boolExprProjection{boolExpr: boolExpr}.Project(mem, ar)
		if err != nil {
			return nil, nil, err
		}
		for i, field := range fields {
			if a.expr.Expr.MatchColumn(field.Name) {
				fields[i].Name = a.name
			}
		}
		return fields, array, nil
	case *logicalplan.Column:
		for i := 0; i < ar.Schema().NumFields(); i++ {
			field := ar.Schema().Field(i)
			if a.expr.MatchColumn(field.Name) {
				field.Name = a.name
				ar.Column(i).Retain() // Retain the column since we're keeping it.
				return []arrow.Field{field}, []arrow.Array{ar.Column(i)}, nil
			}
		}
	default:
		return nil, nil, fmt.Errorf("unsupported alias expression type: %T", e)
	}

	return nil, nil, nil
}

type binaryExprProjection struct {
	expr *logicalplan.BinaryExpr

	left  columnProjection
	right columnProjection
}

func (b binaryExprProjection) Name() string {
	return b.expr.String()
}

func (b binaryExprProjection) Project(mem memory.Allocator, ar arrow.Record) ([]arrow.Field, []arrow.Array, error) {
	leftFields, leftArrays, err := b.left.Project(mem, ar)
	if err != nil {
		return nil, nil, fmt.Errorf("project left side of binary expression: %w", err)
	}

	rightFields, rightArrays, err := b.right.Project(mem, ar)
	if err != nil {
		return nil, nil, fmt.Errorf("project right side of binary expression: %w", err)
	}

	if len(leftFields) != 1 || len(rightFields) != 1 || len(leftArrays) != 1 || len(rightArrays) != 1 {
		return nil, nil, fmt.Errorf("binary expression projection expected one field and one array for each side")
	}

	leftArray := leftArrays[0]
	rightArray := rightArrays[0]

	switch leftArray := leftArray.(type) {
	case *array.Int64:
		switch b.expr.Op {
		case logicalplan.OpAdd:
			return []arrow.Field{leftFields[0]}, []arrow.Array{AddInt64s(mem, leftArray, rightArray.(*array.Int64))}, nil
		case logicalplan.OpSub:
			return []arrow.Field{leftFields[0]}, []arrow.Array{SubInt64s(mem, leftArray, rightArray.(*array.Int64))}, nil
		case logicalplan.OpMul:
			return []arrow.Field{leftFields[0]}, []arrow.Array{MulInt64s(mem, leftArray, rightArray.(*array.Int64))}, nil
		case logicalplan.OpDiv:
			return []arrow.Field{leftFields[0]}, []arrow.Array{DivInt64s(mem, leftArray, rightArray.(*array.Int64))}, nil
		default:
			return nil, nil, fmt.Errorf("unsupported binary expression: %s", b.expr.String())
		}
	case *array.Float64:
		switch b.expr.Op {
		case logicalplan.OpAdd:
			return []arrow.Field{leftFields[0]}, []arrow.Array{AddFloat64s(mem, leftArray, rightArray.(*array.Float64))}, nil
		case logicalplan.OpSub:
			return []arrow.Field{leftFields[0]}, []arrow.Array{SubFloat64s(mem, leftArray, rightArray.(*array.Float64))}, nil
		case logicalplan.OpMul:
			return []arrow.Field{leftFields[0]}, []arrow.Array{MulFloat64s(mem, leftArray, rightArray.(*array.Float64))}, nil
		case logicalplan.OpDiv:
			return []arrow.Field{leftFields[0]}, []arrow.Array{DivFloat64s(mem, leftArray, rightArray.(*array.Float64))}, nil
		default:
			return nil, nil, fmt.Errorf("unsupported binary expression: %s", b.expr.String())
		}
	case *array.Int32:
		switch b.expr.Op {
		case logicalplan.OpAdd:
			return []arrow.Field{leftFields[0]}, []arrow.Array{AddInt32s(mem, leftArray, rightArray.(*array.Int32))}, nil
		case logicalplan.OpSub:
			return []arrow.Field{leftFields[0]}, []arrow.Array{SubInt32s(mem, leftArray, rightArray.(*array.Int32))}, nil
		case logicalplan.OpMul:
			return []arrow.Field{leftFields[0]}, []arrow.Array{MulInt32s(mem, leftArray, rightArray.(*array.Int32))}, nil
		case logicalplan.OpDiv:
			return []arrow.Field{leftFields[0]}, []arrow.Array{DivInt32s(mem, leftArray, rightArray.(*array.Int32))}, nil
		default:
			return nil, nil, fmt.Errorf("unsupported binary expression: %s", b.expr.String())
		}
	default:
		return nil, nil, fmt.Errorf("unsupported type: %T", leftArray)
	}
}

func AddInt64s(mem memory.Allocator, left, right *array.Int64) *array.Int64 {
	res := array.NewInt64Builder(mem)
	defer res.Release()

	res.Resize(left.Len())

	for i := 0; i < left.Len(); i++ {
		res.Append(left.Value(i) + right.Value(i))
	}

	return res.NewInt64Array()
}

func SubInt64s(mem memory.Allocator, left, right *array.Int64) *array.Int64 {
	res := array.NewInt64Builder(mem)
	defer res.Release()

	res.Resize(left.Len())

	for i := 0; i < left.Len(); i++ {
		res.Append(left.Value(i) - right.Value(i))
	}

	return res.NewInt64Array()
}

func MulInt64s(mem memory.Allocator, left, right *array.Int64) *array.Int64 {
	res := array.NewInt64Builder(mem)
	defer res.Release()

	res.Resize(left.Len())

	for i := 0; i < left.Len(); i++ {
		res.Append(left.Value(i) * right.Value(i))
	}

	return res.NewInt64Array()
}

func DivInt64s(mem memory.Allocator, left, right *array.Int64) *array.Int64 {
	res := array.NewInt64Builder(mem)
	defer res.Release()

	res.Resize(left.Len())

	for i := 0; i < left.Len(); i++ {
		res.Append(left.Value(i) / right.Value(i))
	}

	return res.NewInt64Array()
}

func AddFloat64s(mem memory.Allocator, left, right *array.Float64) *array.Float64 {
	res := array.NewFloat64Builder(mem)
	defer res.Release()

	res.Resize(left.Len())

	for i := 0; i < left.Len(); i++ {
		res.Append(left.Value(i) + right.Value(i))
	}

	return res.NewFloat64Array()
}

func SubFloat64s(mem memory.Allocator, left, right *array.Float64) *array.Float64 {
	res := array.NewFloat64Builder(mem)
	defer res.Release()

	res.Resize(left.Len())

	for i := 0; i < left.Len(); i++ {
		res.Append(left.Value(i) - right.Value(i))
	}

	return res.NewFloat64Array()
}

func MulFloat64s(mem memory.Allocator, left, right *array.Float64) *array.Float64 {
	res := array.NewFloat64Builder(mem)
	defer res.Release()

	res.Resize(left.Len())

	for i := 0; i < left.Len(); i++ {
		res.Append(left.Value(i) * right.Value(i))
	}

	return res.NewFloat64Array()
}

func DivFloat64s(mem memory.Allocator, left, right *array.Float64) *array.Float64 {
	res := array.NewFloat64Builder(mem)
	defer res.Release()

	res.Resize(left.Len())

	for i := 0; i < left.Len(); i++ {
		res.Append(left.Value(i) / right.Value(i))
	}

	return res.NewFloat64Array()
}

func AddInt32s(mem memory.Allocator, left, right *array.Int32) *array.Int32 {
	res := array.NewInt32Builder(mem)
	defer res.Release()

	res.Resize(left.Len())

	for i := 0; i < left.Len(); i++ {
		res.Append(left.Value(i) + right.Value(i))
	}

	return res.NewInt32Array()
}

func SubInt32s(mem memory.Allocator, left, right *array.Int32) *array.Int32 {
	res := array.NewInt32Builder(mem)
	defer res.Release()

	res.Resize(left.Len())

	for i := 0; i < left.Len(); i++ {
		res.Append(left.Value(i) - right.Value(i))
	}

	return res.NewInt32Array()
}

func MulInt32s(mem memory.Allocator, left, right *array.Int32) *array.Int32 {
	res := array.NewInt32Builder(mem)
	defer res.Release()

	res.Resize(left.Len())

	for i := 0; i < left.Len(); i++ {
		res.Append(left.Value(i) * right.Value(i))
	}

	return res.NewInt32Array()
}

func DivInt32s(mem memory.Allocator, left, right *array.Int32) *array.Int32 {
	res := array.NewInt32Builder(mem)
	defer res.Release()

	res.Resize(left.Len())

	for i := 0; i < left.Len(); i++ {
		res.Append(left.Value(i) / right.Value(i))
	}

	return res.NewInt32Array()
}

type boolExprProjection struct {
	boolExpr BooleanExpression
}

func (b boolExprProjection) Name() string {
	return b.boolExpr.String()
}

func (b boolExprProjection) Project(mem memory.Allocator, ar arrow.Record) ([]arrow.Field, []arrow.Array, error) {
	var partiallyComputedExprRes arrow.Array
	if ar.Schema().HasField(b.boolExpr.String()) {
		arr := ar.Column(ar.Schema().FieldIndices(b.boolExpr.String())[0])
		if arr.NullN() == 0 {
			// This means we have fully pre-computed the result of the
			// expression in the table scan already.
			arr.Retain()
			return []arrow.Field{
				{
					Name: b.boolExpr.String(),
					Type: &arrow.BooleanType{},
				},
			}, []arrow.Array{arr}, nil
		} else if arr.NullN() < arr.Len() {
			// If we got here, expression results were partially computed at
			// the table scan layer. We fall back to evaluating the expression
			// and overwriting with any results found in this array (i.e.
			// non-null entries). Note that if zero results were pre-computed
			// (arr.NullN == arr.Len()), we'll just fully fall back to
			// expression evaluation.
			partiallyComputedExprRes = arr
		}
	}

	bitmap, err := b.boolExpr.Eval(ar)
	if err != nil {
		return nil, nil, err
	}

	vals := make([]bool, ar.NumRows())
	builder := array.NewBooleanBuilder(mem)
	defer builder.Release()

	// We can do this because we now the values in the array are between 0 and
	// NumRows()-1
	for _, pos := range bitmap.ToArray() {
		vals[int(pos)] = true
	}

	if partiallyComputedExprRes != nil {
		boolArr := partiallyComputedExprRes.(*array.Boolean)
		for i := 0; i < boolArr.Len(); i++ {
			if !boolArr.IsNull(i) {
				// Non-null result, this is the precomputed expression result.
				vals[i] = boolArr.Value(i)
			}
		}
	}

	builder.AppendValues(vals, nil)

	return []arrow.Field{
		{
			Name: b.boolExpr.String(),
			Type: &arrow.BooleanType{},
		},
	}, []arrow.Array{builder.NewArray()}, nil
}

type plainProjection struct {
	expr *logicalplan.Column
}

func (p plainProjection) Name() string {
	return p.expr.ColumnName
}

func (p plainProjection) Project(_ memory.Allocator, ar arrow.Record) ([]arrow.Field, []arrow.Array, error) {
	for i := 0; i < ar.Schema().NumFields(); i++ {
		field := ar.Schema().Field(i)
		if p.expr.MatchColumn(field.Name) {
			ar.Column(i).Retain() // Retain the column since we're keeping it.
			return []arrow.Field{field}, []arrow.Array{ar.Column(i)}, nil
		}
	}

	return nil, nil, nil
}

type dynamicProjection struct {
	expr *logicalplan.DynamicColumn
}

func (p dynamicProjection) Name() string {
	return p.expr.ColumnName
}

func (p dynamicProjection) Project(_ memory.Allocator, ar arrow.Record) ([]arrow.Field, []arrow.Array, error) {
	fields := []arrow.Field{}
	arrays := []arrow.Array{}
	for i := 0; i < ar.Schema().NumFields(); i++ {
		field := ar.Schema().Field(i)
		if p.expr.MatchColumn(field.Name) {
			fields = append(fields, field)
			arrays = append(arrays, ar.Column(i))
			ar.Column(i).Retain() // Retain the column since we're keeping it.
		}
	}

	return fields, arrays, nil
}

func projectionFromExpr(expr logicalplan.Expr) (columnProjection, error) {
	switch e := expr.(type) {
	case *logicalplan.AllExpr:
		return allProjection{}, nil
	case *logicalplan.Column:
		return plainProjection{
			expr: e,
		}, nil
	case *logicalplan.DynamicColumn:
		return dynamicProjection{
			expr: e,
		}, nil
	case *logicalplan.AliasExpr:
		return aliasProjection{
			expr: e,
			name: e.Name(),
		}, nil
	case *logicalplan.BinaryExpr:
		switch e.Op {
		case logicalplan.OpEq, logicalplan.OpNotEq, logicalplan.OpGt, logicalplan.OpGtEq, logicalplan.OpLt, logicalplan.OpLtEq, logicalplan.OpRegexMatch, logicalplan.OpRegexNotMatch, logicalplan.OpAnd, logicalplan.OpOr:
			boolExpr, err := binaryBooleanExpr(e)
			if err != nil {
				return nil, err
			}
			return boolExprProjection{
				boolExpr: boolExpr,
			}, nil
		case logicalplan.OpAdd, logicalplan.OpSub, logicalplan.OpMul, logicalplan.OpDiv:
			left, err := projectionFromExpr(e.Left)
			if err != nil {
				return nil, err
			}

			right, err := projectionFromExpr(e.Right)
			if err != nil {
				return nil, err
			}

			return binaryExprProjection{
				expr: e,

				left:  left,
				right: right,
			}, nil
		default:
			return nil, fmt.Errorf("unknown binary expression: %s", e.String())
		}
	case *logicalplan.AverageExpr:
		return &averageProjection{expr: e}, nil
	default:
		return nil, fmt.Errorf("unsupported expression type for projection: %T", expr)
	}
}

type Projection struct {
	pool   memory.Allocator
	tracer trace.Tracer

	colProjections []columnProjection

	next PhysicalPlan
}

func Project(mem memory.Allocator, tracer trace.Tracer, exprs []logicalplan.Expr) (*Projection, error) {
	p := &Projection{
		pool:           mem,
		tracer:         tracer,
		colProjections: make([]columnProjection, 0, len(exprs)),
	}

	for _, e := range exprs {
		proj, err := projectionFromExpr(e)
		if err != nil {
			return nil, err
		}
		p.colProjections = append(p.colProjections, proj)
	}

	return p, nil
}

func (p *Projection) Close() {
	p.next.Close()
}

func (p *Projection) Callback(ctx context.Context, r arrow.Record) error {
	// Generates high volume of spans. Comment out if needed during development.
	// ctx, span := p.tracer.Start(ctx, "Projection/Callback")
	// defer span.End()

	resFields := make([]arrow.Field, 0, len(p.colProjections))
	resArrays := make([]arrow.Array, 0, len(p.colProjections))

	for _, proj := range p.colProjections {
		f, a, err := proj.Project(p.pool, r)
		if err != nil {
			return err
		}
		if a == nil {
			continue
		}

		resFields = append(resFields, f...)
		resArrays = append(resArrays, a...)
	}

	rows := int64(0)
	if len(resArrays) > 0 {
		rows = int64(resArrays[0].Len())
	}

	ar := array.NewRecord(
		arrow.NewSchema(resFields, nil),
		resArrays,
		rows,
	)
	defer ar.Release()
	for _, arr := range resArrays {
		arr.Release()
	}
	return p.next.Callback(ctx, ar)
}

func (p *Projection) Finish(ctx context.Context) error {
	return p.next.Finish(ctx)
}

func (p *Projection) SetNext(next PhysicalPlan) {
	p.next = next
}

func (p *Projection) Draw() *Diagram {
	var child *Diagram
	if p.next != nil {
		child = p.next.Draw()
	}

	var columns []string
	for _, p := range p.colProjections {
		columns = append(columns, p.Name())
	}
	details := fmt.Sprintf("Projection (%s)", strings.Join(columns, ","))
	return &Diagram{Details: details, Child: child}
}

type averageProjection struct {
	expr logicalplan.Expr
}

func (a *averageProjection) Name() string {
	return a.expr.Name()
}

func (a *averageProjection) Project(mem memory.Allocator, r arrow.Record) ([]arrow.Field, []arrow.Array, error) {
	columnName := a.expr.Name()
	resultName := "avg(" + columnName + ")"
	if avgExpr, ok := a.expr.(*logicalplan.AverageExpr); ok {
		if ae, ok := avgExpr.Expr.(*logicalplan.AliasExpr); ok {
			columnName = ae.Expr.Name()
			resultName = ae.Alias
		}
	}

	columnSum := "sum(" + columnName + ")"
	columnCount := "count(" + columnName + ")"

	schema := r.Schema()

	sumIndex := schema.FieldIndices(columnSum)
	if len(sumIndex) != 1 {
		return nil, nil, fmt.Errorf("sum column for average projection for column %s not found", columnName)
	}
	countIndex := schema.FieldIndices(columnCount)
	if len(countIndex) != 1 {
		return nil, nil, fmt.Errorf("count column for average projection for column %s not found", columnName)
	}

	sums := r.Column(sumIndex[0])
	counts := r.Column(countIndex[0])

	fields := make([]arrow.Field, 0, schema.NumFields()-1)
	columns := make([]arrow.Array, 0, schema.NumFields()-1)

	// Only add the fields and columns that aren't the average's underlying sum and count columns.
	for i := 0; i < schema.NumFields(); i++ {
		field := schema.Field(i)
		if i != sumIndex[0] && i != countIndex[0] {
			fields = append(fields, field)
			columns = append(columns, r.Column(i))
			r.Column(i).Retain() // Retain the column since we're keeping it.
		}
	}

	// Add the field and column for the projected average aggregation.
	switch sums.DataType().ID() {
	case arrow.INT64:
		fields = append(fields, arrow.Field{
			Name: resultName,
			Type: &arrow.Int64Type{},
		})
		columns = append(columns, avgInt64arrays(mem, sums, counts))
	case arrow.FLOAT64:
		fields = append(fields, arrow.Field{
			Name: resultName,
			Type: &arrow.Float64Type{},
		})
		columns = append(columns, avgFloat64arrays(mem, sums, counts))
	default:
		return nil, nil, fmt.Errorf("Datatype %s is not supported for average projection", sums.DataType().ID())
	}

	return fields, columns, nil
}

func avgInt64arrays(pool memory.Allocator, sums, counts arrow.Array) arrow.Array {
	sumsInts := sums.(*array.Int64)
	countsInts := counts.(*array.Int64)

	res := array.NewInt64Builder(pool)
	defer res.Release()
	for i := 0; i < sumsInts.Len(); i++ {
		res.Append(sumsInts.Value(i) / countsInts.Value(i))
	}

	return res.NewArray()
}

func avgFloat64arrays(pool memory.Allocator, sums, counts arrow.Array) arrow.Array {
	sumsFloats := sums.(*array.Float64)
	countsInts := counts.(*array.Int64)

	res := array.NewFloat64Builder(pool)
	defer res.Release()
	for i := 0; i < sumsFloats.Len(); i++ {
		res.Append(sumsFloats.Value(i) / float64(countsInts.Value(i)))
	}

	return res.NewArray()
}

type allProjection struct{}

func (a allProjection) Name() string { return "all" }

func (a allProjection) Project(_ memory.Allocator, ar arrow.Record) ([]arrow.Field, []arrow.Array, error) {
	return ar.Schema().Fields(), ar.Columns(), nil
}
