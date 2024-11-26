package physicalplan

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/apache/arrow/go/v16/arrow/scalar"
	"go.opentelemetry.io/otel/trace"

	"github.com/polarsignals/frostdb/pqarrow/arrowutils"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

type Limiter struct {
	pool   memory.Allocator
	tracer trace.Tracer
	next   PhysicalPlan

	count uint64
}

func Limit(pool memory.Allocator, tracer trace.Tracer, expr logicalplan.Expr) (*Limiter, error) {
	literal, ok := expr.(*logicalplan.LiteralExpr)
	if !ok {
		return nil, fmt.Errorf("expected literal expression, got %T", expr)
	}

	var count uint64
	switch v := literal.Value.(type) {
	case *scalar.Uint64:
		count = v.Value
	case *scalar.Int64:
		count = uint64(v.Value)
	default:
		return nil, fmt.Errorf("expected limit count type, got %T", v)
	}

	return &Limiter{
		pool:   pool,
		tracer: tracer,
		count:  count,
	}, nil
}

func (l *Limiter) SetNext(next PhysicalPlan) { l.next = next }

func (l *Limiter) Finish(ctx context.Context) error { return l.next.Finish(ctx) }

func (l *Limiter) Close() { l.next.Close() }

func (l *Limiter) Draw() *Diagram {
	var child *Diagram
	if l.next != nil {
		child = l.next.Draw()
	}
	details := fmt.Sprintf("Limit(%d)", l.count)
	return &Diagram{Details: details, Child: child}
}

func (l *Limiter) Callback(ctx context.Context, r arrow.Record) error {
	if r.NumRows() == 0 {
		return l.next.Callback(ctx, r)
	}
	if l.count == 0 {
		newRecord := array.NewRecord(r.Schema(), nil, 0)
		return l.next.Callback(ctx, newRecord)
	}

	if uint64(r.NumRows()) <= l.count {
		return l.next.Callback(ctx, r)
	}

	// TODO: We should figure out a way to avoid copying the record here.
	// Maybe we can use a different approach to limit the record.

	indicesBuilder := array.NewInt32Builder(l.pool)
	defer indicesBuilder.Release()

	for i := int32(0); i < int32(l.count); i++ {
		indicesBuilder.Append(i)
	}
	indices := indicesBuilder.NewInt32Array()
	defer indices.Release()

	limitedRecord, err := arrowutils.Take(ctx, r, indices)
	if err != nil {
		return err
	}

	if err := l.next.Callback(ctx, limitedRecord); err != nil {
		return err
	}

	return nil
}
