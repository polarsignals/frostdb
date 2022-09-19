package physicalplan

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
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
	for i, field := range ar.Schema().Fields() {
		if a.expr.MatchColumn(field.Name) {
			field.Name = a.name
			return []arrow.Field{field}, []arrow.Array{ar.Column(i)}, nil
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

func (p plainProjection) Project(mem memory.Allocator, ar arrow.Record) ([]arrow.Field, []arrow.Array, error) {
	for i, field := range ar.Schema().Fields() {
		if p.expr.MatchColumn(field.Name) {
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

func (p dynamicProjection) Project(mem memory.Allocator, ar arrow.Record) ([]arrow.Field, []arrow.Array, error) {
	fields := []arrow.Field{}
	arrays := []arrow.Array{}
	for i, field := range ar.Schema().Fields() {
		if p.expr.MatchColumn(field.Name) {
			fields = append(fields, field)
			arrays = append(arrays, ar.Column(i))
		}
	}

	return fields, arrays, nil
}

func projectionFromExpr(expr logicalplan.Expr) (columnProjection, error) {
	switch e := expr.(type) {
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
