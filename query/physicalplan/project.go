package physicalplan

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"go.opentelemetry.io/otel/trace"

	"github.com/polarsignals/frostdb/query/logicalplan"
)

type columnProjection interface {
	Project(mem memory.Allocator, ar arrow.Record) ([]arrow.Field, []arrow.Array, error)
}

type aliasProjection struct {
	expr *logicalplan.AliasExpr
	name string
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

func (b binaryExprProjection) Project(mem memory.Allocator, ar arrow.Record) ([]arrow.Field, []arrow.Array, error) {
	if ar.Schema().HasField(b.boolExpr.String()) {
		arr := ar.Column(ar.Schema().FieldIndices(b.boolExpr.String())[0])
		if arr.Len() != arr.NullN() {
			// This means we have pre-computed the result of the expression in
			// the table scan already.
			return []arrow.Field{
				{
					Name: b.boolExpr.String(),
					Type: &arrow.BooleanType{},
				},
			}, []arrow.Array{arr}, nil
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

	next func(ctx context.Context, r arrow.Record) error
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
	return p.next(ctx, ar)
}

func (p *Projection) SetNextCallback(next func(ctx context.Context, r arrow.Record) error) {
	p.next = next
}
