package records

import (
	"reflect"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

type Reader[T any] struct {
	records []arrow.Record
}

func NewReader[T any](records ...arrow.Record) *Reader[T] {
	var a T
	r := reflect.TypeOf(a)
	for r.Kind() == reflect.Ptr {
		r = r.Elem()
	}
	if r.Kind() != reflect.Struct {
		panic("frostdb/dynschema: " + r.String() + " is not supported")
	}

	return &Reader[T]{records: records}
}

func (r *Reader[T]) NumRows() int64 {
	var rows int64
	for _, record := range r.records {
		rows += record.NumRows()
	}
	return rows
}

func (r *Reader[T]) Value(i int) T {
	row := *new(T)
	rowType := reflect.TypeOf(row)

	// find the record with the value
	var record arrow.Record
	var previousRows int64
	for _, rec := range r.records {
		if i < int(previousRows+rec.NumRows()) {
			record = rec
			i = i - int(previousRows)
			break
		}
		previousRows += rec.NumRows()
	}

	for j := 0; j < rowType.NumField(); j++ {
		f := rowType.Field(j)
		name, _ := fieldName(f)

		indices := record.Schema().FieldIndices(name)
		if len(indices) != 1 {
			panic("field " + name + " not found or ambiguous")
		}

		switch f.Type.Kind() {
		case reflect.Bool:
			arr, ok := record.Column(indices[0]).(*array.Boolean)
			if !ok || arr.IsNull(i) {
				continue
			}
			reflect.ValueOf(&row).Elem().Field(j).Set(
				reflect.ValueOf(
					arr.Value(i),
				),
			)
		case reflect.Float32:
			arr, ok := record.Column(indices[0]).(*array.Float32)
			if !ok || arr.IsNull(i) {
				continue
			}
			reflect.ValueOf(&row).Elem().Field(j).Set(
				reflect.ValueOf(
					arr.Value(i),
				),
			)
		case reflect.Float64:
			arr, ok := record.Column(indices[0]).(*array.Float64)
			if !ok || arr.IsNull(i) {
				continue
			}
			reflect.ValueOf(&row).Elem().Field(j).Set(
				reflect.ValueOf(
					arr.Value(i),
				),
			)
		case reflect.Int8:
			arr, ok := record.Column(indices[0]).(*array.Int8)
			if !ok || arr.IsNull(i) {
				continue
			}
			reflect.ValueOf(&row).Elem().Field(j).Set(
				reflect.ValueOf(
					arr.Value(i),
				),
			)
		case reflect.Int16:
			arr, ok := record.Column(indices[0]).(*array.Int16)
			if !ok || arr.IsNull(i) {
				continue
			}
			reflect.ValueOf(&row).Elem().Field(j).Set(
				reflect.ValueOf(
					arr.Value(i),
				),
			)
		case reflect.Int32:
			arr, ok := record.Column(indices[0]).(*array.Int32)
			if !ok || arr.IsNull(i) {
				continue
			}
			reflect.ValueOf(&row).Elem().Field(j).Set(
				reflect.ValueOf(
					arr.Value(i),
				),
			)
		case reflect.Int64:
			arr, ok := record.Column(indices[0]).(*array.Int64)
			if !ok || arr.IsNull(i) {
				continue
			}
			reflect.ValueOf(&row).Elem().Field(j).Set(
				reflect.ValueOf(
					arr.Value(i),
				),
			)
		case reflect.Uint8:
			arr, ok := record.Column(indices[0]).(*array.Uint8)
			if !ok || arr.IsNull(i) {
				continue
			}
			reflect.ValueOf(&row).Elem().Field(j).Set(
				reflect.ValueOf(
					arr.Value(i),
				),
			)
		case reflect.Uint16:
			arr, ok := record.Column(indices[0]).(*array.Uint16)
			if !ok || arr.IsNull(i) {
				continue
			}
			reflect.ValueOf(&row).Elem().Field(j).Set(
				reflect.ValueOf(
					arr.Value(i),
				),
			)
		case reflect.Uint32:
			arr, ok := record.Column(indices[0]).(*array.Uint32)
			if !ok || arr.IsNull(i) {
				continue
			}
			reflect.ValueOf(&row).Elem().Field(j).Set(
				reflect.ValueOf(
					arr.Value(i),
				),
			)
		case reflect.Uint64:
			arr, ok := record.Column(indices[0]).(*array.Uint64)
			if !ok || arr.IsNull(i) {
				continue
			}
			reflect.ValueOf(&row).Elem().Field(j).Set(
				reflect.ValueOf(
					arr.Value(i),
				),
			)
		case reflect.String:
			// We probably need to support array.Binary too
			arr, ok := record.Column(indices[0]).(*array.String)
			if !ok || arr.IsNull(i) {
				continue
			}
			reflect.ValueOf(&row).Elem().Field(j).Set(
				reflect.ValueOf(
					arr.Value(i),
				),
			)
		default:
			panic("unsupported type " + f.Type.String())
		}
	}

	return row
}
