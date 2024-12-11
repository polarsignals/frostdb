package physicalplan

import (
	"context"
	"fmt"
	"hash/maphash"
	"strings"
	"sync"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/arrow/scalar"
	"go.opentelemetry.io/otel/trace"

	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/pqarrow/builder"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

type Distinction struct {
	pool     memory.Allocator
	tracer   trace.Tracer
	next     PhysicalPlan
	columns  []logicalplan.Expr
	hashSeed maphash.Seed

	mtx  *sync.RWMutex
	seen map[uint64]struct{}
}

func (d *Distinction) Draw() *Diagram {
	var child *Diagram
	if d.next != nil {
		child = d.next.Draw()
	}

	var columns []string
	for _, c := range d.columns {
		columns = append(columns, c.Name())
	}

	return &Diagram{Details: fmt.Sprintf("Distinction (%s)", strings.Join(columns, ",")), Child: child}
}

func Distinct(pool memory.Allocator, tracer trace.Tracer, columns []logicalplan.Expr) *Distinction {
	return &Distinction{
		pool:     pool,
		tracer:   tracer,
		columns:  columns,
		hashSeed: maphash.MakeSeed(),

		mtx:  &sync.RWMutex{},
		seen: make(map[uint64]struct{}),
	}
}

func (d *Distinction) SetNext(plan PhysicalPlan) {
	d.next = plan
}

func (d *Distinction) Finish(ctx context.Context) error {
	return d.next.Finish(ctx)
}

func (d *Distinction) Close() {
	d.next.Close()
}

func (d *Distinction) Callback(ctx context.Context, r arrow.Record) error {
	// Generates high volume of spans. Comment out if needed during development.
	// ctx, span := d.tracer.Start(ctx, "Distinction/Callback")
	// defer span.End()

	distinctFields := make([]arrow.Field, 0, 10)
	distinctFieldHashes := make([]uint64, 0, 10)
	distinctArrays := make([]arrow.Array, 0, 10)

	for i := 0; i < r.Schema().NumFields(); i++ {
		field := r.Schema().Field(i)
		for _, col := range d.columns {
			if col.MatchColumn(field.Name) {
				distinctFields = append(distinctFields, field)
				distinctFieldHashes = append(distinctFieldHashes, scalar.Hash(d.hashSeed, scalar.NewStringScalar(field.Name)))
				distinctArrays = append(distinctArrays, r.Column(i))
			}
		}
	}

	resBuilders := make([]builder.ColumnBuilder, 0, len(distinctArrays))
	defer func() {
		for _, builder := range resBuilders {
			builder.Release()
		}
	}()
	for _, arr := range distinctArrays {
		resBuilders = append(resBuilders, builder.NewBuilder(d.pool, arr.DataType()))
	}
	rows := int64(0)

	numRows := int(r.NumRows())

	colHashes := make([][]uint64, len(distinctFields))
	for i, arr := range distinctArrays {
		colHashes[i] = dynparquet.HashArray(arr)
	}

	for i := 0; i < numRows; i++ {
		hash := uint64(0)
		for j := range colHashes {
			if colHashes[j][i] == 0 {
				continue
			}

			hash = hashCombine(
				hash,
				hashCombine(
					distinctFieldHashes[j],
					colHashes[j][i],
				),
			)
		}

		d.mtx.RLock()
		if _, ok := d.seen[hash]; ok {
			d.mtx.RUnlock()
			continue
		}
		d.mtx.RUnlock()

		for j, arr := range distinctArrays {
			err := builder.AppendValue(resBuilders[j], arr, i)
			if err != nil {
				return err
			}
		}

		rows++
		d.mtx.Lock()
		d.seen[hash] = struct{}{}
		d.mtx.Unlock()
	}

	if rows == 0 {
		// No need to call anything further down the chain, no new values were
		// seen so we can skip.
		return nil
	}

	resArrays := make([]arrow.Array, 0, len(resBuilders))
	defer func() {
		for _, arr := range resArrays {
			arr.Release()
		}
	}()
	for _, builder := range resBuilders {
		resArrays = append(resArrays, builder.NewArray())
	}

	schema := arrow.NewSchema(distinctFields, nil)

	distinctRecord := array.NewRecord(
		schema,
		resArrays,
		rows,
	)

	defer distinctRecord.Release()
	return d.next.Callback(ctx, distinctRecord)
}
