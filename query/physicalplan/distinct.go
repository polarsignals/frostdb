package physicalplan

import (
	"hash/maphash"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/apache/arrow/go/v8/arrow/scalar"
	"github.com/dgryski/go-metro"

	"github.com/polarsignals/arcticdb/query/logicalplan"
)

type Distinction struct {
	pool     memory.Allocator
	seen     map[uint64]struct{}
	next     func(r arrow.Record) error
	columns  []logicalplan.ColumnMatcher
	hashSeed maphash.Seed
}

func Distinct(pool memory.Allocator, columns []logicalplan.ColumnMatcher) *Distinction {
	return &Distinction{
		pool:     pool,
		columns:  columns,
		seen:     make(map[uint64]struct{}),
		hashSeed: maphash.MakeSeed(),
	}
}

func (d *Distinction) SetNextCallback(callback func(r arrow.Record) error) {
	d.next = callback
}

func (d *Distinction) Callback(r arrow.Record) error {
	distinctFields := make([]arrow.Field, 0, 10)
	distinctArrays := make([]arrow.Array, 0, 10)

	for i, field := range r.Schema().Fields() {
		for _, col := range d.columns {
			if col.Match(field.Name) {
				distinctFields = append(distinctFields, field)
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
	colScalars := make([]scalar.Scalar, len(distinctFields))
	for i := 0; i < numRows; i++ {
		colScalars = colScalars[:0]

		for _, arr := range distinctArrays {
			colScalar, err := scalar.GetScalar(arr, i)
			if err != nil {
				return err
			}

			colScalars = append(colScalars, colScalar)
		}

		hash := uint64(0)
		for j, colScalar := range colScalars {
			if colScalar == nil || !colScalar.IsValid() {
				continue
			}

			// TODO: This is extremely naive and will probably cause a ton of collisions.
			hash ^= metro.Hash64Str(distinctFields[j].Name, 0)
			hash ^= scalar.Hash(d.hashSeed, colScalar)
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

	err := d.next(distinctRecord)
	distinctRecord.Release()
	return err
}
