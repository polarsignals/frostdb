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
	Name() string
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
		fields, array, err := binaryExprProjection{boolExpr: boolExpr}.Project(mem, ar)
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
	}

	return nil, nil, nil
}

type binaryExprProjection struct {
	boolExpr BooleanExpression
}

func (b binaryExprProjection) Name() string {
	return b.boolExpr.String()
}

func (b binaryExprProjection) Project(mem memory.Allocator, ar arrow.Record) ([]arrow.Field, []arrow.Array, error) {
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
		boolExpr, err := binaryBooleanExpr(e)
		if err != nil {
			return nil, err
		}
		return binaryExprProjection{
			boolExpr: boolExpr,
		}, nil
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
