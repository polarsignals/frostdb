package dynparquet

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/cespare/xxhash/v2"
	"github.com/dgryski/go-metro"
)

const prehashedPrefix = "hashed"

func HashedColumnName(col string) string {
	return prehashedPrefix + "." + col
}

func IsHashedColumn(col string) bool {
	return strings.HasPrefix(col, prehashedPrefix)
}

// findHashedColumn finds the index of the column in the given fields that have been prehashed.
func FindHashedColumn(col string, fields []arrow.Field) int {
	for j, f := range fields {
		if HashedColumnName(col) == f.Name {
			return j
		}
	}

	return -1
}

// prehashColumns prehashes the columns in the given record that have been marked as prehashed in the given schema.
func PrehashColumns(schema *Schema, r arrow.Record) arrow.Record {
	bldr := array.NewInt64Builder(memory.DefaultAllocator) // TODO pass in allocator
	defer bldr.Release()

	fields := r.Schema().Fields()
	additionalFields := make([]arrow.Field, 0, len(fields))
	additionalColumns := make([]arrow.Array, 0, len(fields))
	defer func() {
		for _, col := range additionalColumns {
			col.Release()
		}
	}()

	for _, col := range schema.Columns() {
		if !col.PreHash {
			continue
		}

		for i, f := range fields {
			if col.Name == f.Name || (col.Dynamic && strings.HasPrefix(f.Name, col.Name)) {
				additionalFields = append(additionalFields, arrow.Field{
					Name: HashedColumnName(f.Name),
					Type: arrow.PrimitiveTypes.Int64,
				})

				// Hash the column
				hashed := HashArray(r.Column(i))

				// Build the new column
				bldr.Reserve(len(hashed))
				for _, v := range hashed {
					bldr.UnsafeAppend(int64(v))
				}

				additionalColumns = append(additionalColumns, bldr.NewArray())
			}
		}
	}

	if len(additionalColumns) == 0 {
		r.Retain() // NOTE: we retain here because we expect the caller to release the record that we're returning
		return r
	}

	sch := arrow.NewSchema(append(fields, additionalFields...), nil)
	return array.NewRecord(sch, append(r.Columns(), additionalColumns...), r.NumRows())
}

func HashArray(arr arrow.Array) []uint64 {
	switch ar := arr.(type) {
	case *array.String:
		return hashStringArray(ar)
	case *array.Binary:
		return hashBinaryArray(ar)
	case *array.Int64:
		return hashInt64Array(ar)
	case *array.Boolean:
		return hashBooleanArray(ar)
	case *array.Dictionary:
		return hashDictionaryArray(ar)
	case *array.List:
		return hashListArray(ar)
	default:
		panic("unsupported array type " + fmt.Sprintf("%T", arr))
	}
}

func hashListArray(arr *array.List) []uint64 {
	res := make([]uint64, arr.Len())
	digest := xxhash.New()
	var hv func(int, *xxhash.Digest)
	switch e := arr.ListValues().(type) {
	case *array.Int64:
		var buf [8]byte
		hv = func(i int, d *xxhash.Digest) {
			binary.BigEndian.PutUint64(buf[:], uint64(e.Value(i)))
			_, _ = d.Write(buf[:])
		}
	case *array.Float64:
		var buf [8]byte
		hv = func(i int, d *xxhash.Digest) {
			binary.BigEndian.PutUint64(buf[:], math.Float64bits(e.Value(i)))
			_, _ = d.Write(buf[:])
		}
	case *array.Boolean:
		var buf [1]byte
		hv = func(i int, d *xxhash.Digest) {
			if e.Value(i) {
				buf[0] = 2
			} else {
				buf[0] = 1
			}
			_, _ = d.Write(buf[:])
		}
	case *array.Binary:
		hv = func(i int, d *xxhash.Digest) {
			_, _ = d.Write(e.Value(i))
		}
	case *array.String:
		hv = func(i int, d *xxhash.Digest) {
			_, _ = d.WriteString(e.Value(i))
		}
	case *array.Dictionary:
		switch dict := e.Dictionary().(type) {
		case *array.Binary:
			hv = func(i int, d *xxhash.Digest) {
				_, _ = d.Write(dict.Value(e.GetValueIndex(i)))
			}
		case *array.String:
			hv = func(i int, d *xxhash.Digest) {
				_, _ = d.WriteString(dict.Value(e.GetValueIndex(i)))
			}
		default:
			panic(fmt.Sprintf("list dictionary not of expected type: %T", dict))
		}
	default:
		panic(fmt.Sprintf("list not of expected type: %T", e))
	}
	for i := 0; i < arr.Len(); i++ {
		start, end := arr.ValueOffsets(i)
		for j := start; j < end; j++ {
			hv(int(j), digest)
		}
		res[i] = digest.Sum64()
		digest.Reset()
	}
	return res
}

func hashDictionaryArray(arr *array.Dictionary) []uint64 {
	res := make([]uint64, arr.Len())
	for i := 0; i < arr.Len(); i++ {
		if !arr.IsNull(i) {
			switch dict := arr.Dictionary().(type) {
			case *array.Binary:
				res[i] = metro.Hash64(dict.Value(arr.GetValueIndex(i)), 0)
			case *array.String:
				res[i] = metro.Hash64([]byte(dict.Value(arr.GetValueIndex(i))), 0)
			default:
				panic("unsupported dictionary type " + fmt.Sprintf("%T", dict))
			}
		}
	}
	return res
}

func hashBinaryArray(arr *array.Binary) []uint64 {
	res := make([]uint64, arr.Len())
	for i := 0; i < arr.Len(); i++ {
		if !arr.IsNull(i) {
			res[i] = metro.Hash64(arr.Value(i), 0)
		}
	}
	return res
}

func hashBooleanArray(arr *array.Boolean) []uint64 {
	res := make([]uint64, arr.Len())
	for i := 0; i < arr.Len(); i++ {
		if arr.IsNull(i) {
			res[i] = 0
			continue
		}
		if arr.Value(i) {
			res[i] = 2
		} else {
			res[i] = 1
		}
	}
	return res
}

func hashStringArray(arr *array.String) []uint64 {
	res := make([]uint64, arr.Len())
	for i := 0; i < arr.Len(); i++ {
		if !arr.IsNull(i) {
			res[i] = metro.Hash64([]byte(arr.Value(i)), 0)
		}
	}
	return res
}

func hashInt64Array(arr *array.Int64) []uint64 {
	res := make([]uint64, arr.Len())
	for i := 0; i < arr.Len(); i++ {
		if !arr.IsNull(i) {
			res[i] = uint64(arr.Value(i))
		}
	}
	return res
}

// RemoveHashedColumns removes the hashed columns from the record.
func RemoveHashedColumns(r arrow.Record) arrow.Record {
	cols := make([]arrow.Array, 0, r.Schema().NumFields())
	fields := make([]arrow.Field, 0, r.Schema().NumFields())
	for i := 0; i < r.Schema().NumFields(); i++ {
		if !IsHashedColumn(r.Schema().Field(i).Name) {
			cols = append(cols, r.Column(i))
			fields = append(fields, r.Schema().Field(i))
		}
	}

	return array.NewRecord(arrow.NewSchema(fields, nil), cols, r.NumRows())
}
