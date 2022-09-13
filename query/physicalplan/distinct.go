package physicalplan

import (
	"context"
	"hash/maphash"
	"sort"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/apache/arrow/go/v8/arrow/scalar"
	"go.opentelemetry.io/otel/trace"

	"github.com/polarsignals/frostdb/query/logicalplan"
)

type Distinction struct {
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

// Distinct batches all the callbacks and only with the Finish call releases the final record.
func Distinct(pool memory.Allocator, tracer trace.Tracer, columns []logicalplan.Expr) *Distinction {
	return &Distinction{
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

func (d *Distinction) Callback(ctx context.Context, r arrow.Record) error {
	for i, field := range r.Schema().Fields() {
		arr := r.Column(i)
		arr.Retain()

		d.distinctFields[field.Name] = field
		d.distinctRows[field.Name] = append(d.distinctRows[field.Name], r.NumRows())
		d.distinctArrays[field.Name] = append(d.distinctArrays[field.Name], arr)
		d.distinctFieldHashes[field.Name] = scalar.Hash(d.hashSeed, scalar.NewStringScalar(field.Name))
	}

	return nil
}

func (d *Distinction) Finish(ctx context.Context) error {
	ctx, span := d.tracer.Start(ctx, "Distinction/Finish")
	defer span.End()

	if len(d.distinctFields) == 0 {
		// We have nothing to do here, we can call the next Finish right away.
		return d.next.Finish(ctx)
	}

	distinctFields := make([]arrow.Field, 0, len(d.distinctFields))
	for _, f := range d.distinctFields {
		distinctFields = append(distinctFields, f)
	}
	// sort the field names as the map is random and unsorted
	sort.Slice(distinctFields, func(i, j int) bool {
		return distinctFields[i].Name < distinctFields[j].Name
	})

	colHashes := make(map[string][][]uint64, len(distinctFields))
	for name, f := range d.distinctFields {
		if colHashes[name] == nil {
			colHashes[name] = make([][]uint64, len(d.distinctArrays[name]))
		}
		for i, arr := range d.distinctArrays[f.Name] {
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
						d.distinctFieldHashes[f.Name],
						colHashes[f.Name][i][j],
					),
				)
			}

			if _, ok := seen[hash]; ok {
				continue
			}

			for fn, arrs := range d.distinctArrays {
				if distinctBuilder[fn] == nil {
					distinctBuilder[fn] = array.NewBuilder(d.pool, arrs[i].DataType())
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

	err := d.next.Callback(ctx, distinctRecord)
	distinctRecord.Release()
	if err != nil {
		return err
	}

	return d.next.Finish(ctx)
}

func (d *Distinction) SetNext(next PhysicalPlan) {
	d.next = next
}

func (d *Distinction) Draw() *Diagram {
	return &Diagram{}
}
