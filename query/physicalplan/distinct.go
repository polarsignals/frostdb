package physicalplan

import (
	"context"
	"fmt"
	"hash/maphash"
	"sort"
	"strings"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/apache/arrow/go/v8/arrow/scalar"
	"go.opentelemetry.io/otel/trace"

	"github.com/polarsignals/frostdb/query/logicalplan"
)

type Distinction struct {
	pool     memory.Allocator
	tracer   trace.Tracer
	next     PhysicalPlan
	columns  []logicalplan.Expr
	hashSeed maphash.Seed
	seen     map[uint64]struct{}
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
		seen:     make(map[uint64]struct{}),
	}
}

func (d *Distinction) SetNext(plan PhysicalPlan) {
	d.next = plan
}

func (d *Distinction) Finish(ctx context.Context) error {
	return d.next.Finish(ctx)
}

func (d *Distinction) Callback(ctx context.Context, r arrow.Record) error {
	// Generates high volume of spans. Comment out if needed during development.
	// ctx, span := d.tracer.Start(ctx, "Distinction/Callback")
	// defer span.End()

	distinctFields := make([]arrow.Field, 0, 10)
	distinctFieldHashes := make([]uint64, 0, 10)
	distinctArrays := make([]arrow.Array, 0, 10)

	for i, field := range r.Schema().Fields() {
		for _, col := range d.columns {
			if col.MatchColumn(field.Name) {
				distinctFields = append(distinctFields, field)
				distinctFieldHashes = append(distinctFieldHashes, scalar.Hash(d.hashSeed, scalar.NewStringScalar(field.Name)))
				distinctArrays = append(distinctArrays, r.Column(i))
			}
		}
	}

	resBuilders := make([]array.Builder, 0, len(distinctArrays))
	for _, arr := range distinctArrays {
		resBuilders = append(resBuilders, array.NewBuilder(d.pool, arr.DataType()))
	}
	rows := int64(0)

	numRows := int(r.NumRows())

	colHashes := make([][]uint64, len(distinctFields))
	for i, arr := range distinctArrays {
		colHashes[i] = hashArray(arr)
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

		if _, ok := d.seen[hash]; ok {
			continue
		}

		for j, arr := range distinctArrays {
			err := appendValue(resBuilders[j], arr, i)
			if err != nil {
				return err
			}
		}

		rows++
		d.seen[hash] = struct{}{}
	}

	if rows == 0 {
		// No need to call anything further down the chain, no new values were
		// seen so we can skip.
		return nil
	}

	resArrays := make([]arrow.Array, 0, len(resBuilders))
	for _, builder := range resBuilders {
		resArrays = append(resArrays, builder.NewArray())
	}

	schema := arrow.NewSchema(distinctFields, nil)

	distinctRecord := array.NewRecord(
		schema,
		resArrays,
		rows,
	)

	err := d.next.Callback(ctx, distinctRecord)
	distinctRecord.Release()
	return err
}

// FinalDistinct is called by the Synchronizer, so it's not concurrency safe.
// It batches all the callbacks and only with the Finish call releases the final record.
func FinalDistinct(pool memory.Allocator, tracer trace.Tracer, columns []logicalplan.Expr) *FinalDistinction {
	return &FinalDistinction{
		pool:     pool,
		tracer:   tracer,
		columns:  columns,
		hashSeed: maphash.MakeSeed(),

		distinctFields:      make(map[string]arrow.Field),
		distinctRows:        make(map[string][]int64),
		distinctArrays:      make(map[string][]arrow.Array),
		distinctFieldHashes: make(map[string]uint64),
	}
}

type FinalDistinction struct {
	next     PhysicalPlan
	pool     memory.Allocator
	tracer   trace.Tracer
	columns  []logicalplan.Expr
	hashSeed maphash.Seed

	distinctFields      map[string]arrow.Field
	distinctArrays      map[string][]arrow.Array
	distinctRows        map[string][]int64
	distinctFieldHashes map[string]uint64
}

func (fd *FinalDistinction) Callback(ctx context.Context, r arrow.Record) error {
	for i, field := range r.Schema().Fields() {
		fd.distinctFields[field.Name] = field
		fd.distinctRows[field.Name] = append(fd.distinctRows[field.Name], r.NumRows())
		fd.distinctArrays[field.Name] = append(fd.distinctArrays[field.Name], r.Column(i))
		fd.distinctFieldHashes[field.Name] = scalar.Hash(fd.hashSeed, scalar.NewStringScalar(field.Name))
	}

	return nil
}

func (fd *FinalDistinction) Finish(ctx context.Context) error {
	if len(fd.distinctFields) == 0 {
		// We have nothing to do here, we can call the next Finish right away.
		return fd.next.Finish(ctx)
	}

	distinctFields := make([]arrow.Field, 0, len(fd.distinctFields))
	for _, f := range fd.distinctFields {
		distinctFields = append(distinctFields, f)
	}
	// sort the field names as the map is random and unsorted
	sort.Slice(distinctFields, func(i, j int) bool {
		return distinctFields[i].Name < distinctFields[j].Name
	})

	colHashes := make(map[string][][]uint64, len(distinctFields))
	for name, f := range fd.distinctFields {
		if colHashes[name] == nil {
			colHashes[name] = make([][]uint64, len(fd.distinctArrays[name]))
		}
		for i, arr := range fd.distinctArrays[f.Name] {
			colHashes[name][i] = hashArray(arr)
		}
	}

	seen := map[uint64]struct{}{}

	// Get a random field to iterate over the inner rows.
	var randomField string
	for fn := range colHashes {
		randomField = fn
		break
	}

	distinctBuilder := make(map[string]array.Builder, len(distinctFields))

	// We iterate over all rows. For each row, we combine the hashes of all the values to thus find duplicate rows.
	// If distinct column values in a row weren't seen before we append the row's values to each individual column.

	for i := range colHashes[randomField] {
		for j := range colHashes[randomField][i] {
			hash := uint64(0)
			for _, f := range distinctFields {
				if colHashes[f.Name][i][j] == 0 {
					continue
				}

				hash = hashCombine(
					hash,
					hashCombine(
						fd.distinctFieldHashes[f.Name],
						colHashes[f.Name][i][j],
					),
				)
			}

			if _, ok := seen[hash]; ok {
				continue
			}

			for fn, arrs := range fd.distinctArrays {
				if distinctBuilder[fn] == nil {
					distinctBuilder[fn] = array.NewBuilder(fd.pool, arrs[i].DataType())
				}
				err := appendValue(distinctBuilder[fn], arrs[i], j)
				if err != nil {
					return err
				}
			}
			seen[hash] = struct{}{}
		}
	}

	schema := arrow.NewSchema(distinctFields, nil)

	resArrays := make([]arrow.Array, 0, len(distinctBuilder))
	for _, f := range distinctFields {
		resArrays = append(resArrays, distinctBuilder[f.Name].NewArray())
	}

	distinctRecord := array.NewRecord(
		schema,
		resArrays,
		int64(len(seen)),
	)

	err := fd.next.Callback(ctx, distinctRecord)
	distinctRecord.Release()
	if err != nil {
		return err
	}

	return fd.next.Finish(ctx)
}

func (fd *FinalDistinction) SetNext(next PhysicalPlan) {
	fd.next = next
}

func (fd *FinalDistinction) Draw() *Diagram {
	return &Diagram{}
}
