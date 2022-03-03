package pqarrow

import (
	"fmt"
	"io"

	"github.com/apache/arrow/go/v7/arrow"
	"github.com/apache/arrow/go/v7/arrow/array"
	"github.com/apache/arrow/go/v7/arrow/memory"
	"github.com/segmentio/parquet-go"
)

// ParquetRowGroupToArrowRecord converts a parquet row group to an arrow record.
func ParquetRowGroupToArrowRecord(pool memory.Allocator, rg parquet.RowGroup) (arrow.Record, error) {
	s := rg.Schema()

	children := s.ChildNames()
	fields := make([]arrow.Field, 0, len(children))
	cols := make([]array.Interface, 0, len(children))
	for i, child := range children {
		typ, nullable, array, err := parquetColumnToArrowArray(pool, s.ChildByName(child), rg.Column(i))
		if err != nil {
			return nil, fmt.Errorf("convert parquet column to arrow array: %w", err)
		}
		fields = append(fields, arrow.Field{
			Name:     child,
			Type:     typ,
			Nullable: nullable,
		})
		cols = append(cols, array)
	}

	schema := arrow.NewSchema(fields, nil)
	return array.NewRecord(schema, cols, rg.NumRows()), nil
}

// parquetColumnToArrowArray converts a single parquet column to an arrow array
// and returns the type, nullability, and the actual resulting arrow array. If
// a column is a repeated type, it automatically boxes it into the appropriate
// arrow equivalent.
func parquetColumnToArrowArray(
	pool memory.Allocator,
	n parquet.Node,
	c parquet.ColumnChunk,
) (
	arrow.DataType,
	bool,
	array.Interface,
	error,
) {
	at, newValueWriter := parquetNodeToType(n)

	var (
		writeValues func([]parquet.Value)
		b           array.Builder
		lb          *array.ListBuilder

		repeated = false
		nullable = false
	)

	optional := n.Optional()
	if optional {
		nullable = true
	}

	// Using the retrieved arrow type and whether the type is repeated we can
	// build a type-safe `writeValues` function that only casts the resulting
	// builder once and can perform optimized transfers of the page values to
	// the target array.
	if n.Repeated() {
		// If the column is repeated, we need to box it into a list.
		lt := arrow.ListOf(at)
		lt.SetElemNullable(optional)
		at = lt

		nullable = true
		repeated = true

		lb = array.NewBuilder(pool, at).(*array.ListBuilder)
		// A list builder actually expects all values of all sublists to be
		// written contiguously, the offsets where litsts start are recorded in
		// the offsets array below.
		writeValues = newValueWriter(lb.ValueBuilder())
		b = lb
	} else {
		b = array.NewBuilder(pool, at)
		writeValues = newValueWriter(b)
	}
	defer b.Release()

	err := writePagesToArray(
		c.Pages(),
		repeated,
		lb,
		writeValues,
	)
	if err != nil {
		return nil, false, nil, err
	}

	arr := b.NewArray()

	// Is this a bug in arrow? We already set the nullability above, but it
	// doesn't appear to transfer into the resulting array's type. Needs to be
	// investigated.
	switch t := arr.DataType().(type) {
	case *arrow.ListType:
		t.SetElemNullable(optional)
	}

	return at, nullable, arr, nil
}

// writePagesToArray reads all pages of a page iterator and writes the values
// to an array builder. If the type is a repeated type it will also write the
// starting offsets of lists to the list builder.
func writePagesToArray(
	pages parquet.Pages,
	repeated bool,
	lb *array.ListBuilder,
	writeValues func([]parquet.Value),
) error {
	for {
		p, err := pages.ReadPage()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("read page: %w", err)
		}
		values := make([]parquet.Value, p.NumValues())
		reader := p.Values()
		_, err = reader.ReadValues(values)
		// We're reading all values in the page so we always expect an io.EOF.
		if err != io.EOF {
			return fmt.Errorf("expected io.EOF since all values of the page are being read: %w", err)
		}

		writeValues(values)

		if repeated {
			offsets := []int32{}
			validity := []bool{}
			for i, v := range values {
				rep := v.RepetitionLevel()
				def := v.DefinitionLevel()
				if rep == 0 && def == 1 {
					offsets = append(offsets, int32(i))
					validity = append(validity, true)
				}
				if rep == 0 && def == 0 {
					offsets = append(offsets, int32(i))
					validity = append(validity, false)
				}
				// rep == 1 && def == 1 means the item in the list is not null which is handled by the value writer
				// rep == 1 && def == 0 means the item in the list is null which is handled by the value writer
			}

			lb.AppendValues(offsets, validity)
		}
	}

	return nil
}

// parquetNodeToType converts a parquet node to an arrow type and a function to
// create a value writer.
func parquetNodeToType(n parquet.Node) (arrow.DataType, func(b array.Builder) func([]parquet.Value)) {
	t := n.Type()
	lt := t.LogicalType()
	switch {
	case lt.UUID != nil:
		return &arrow.FixedSizeBinaryType{
			ByteWidth: 16,
		}, uuidValueWriter
	case lt.UTF8 != nil:
		return &arrow.StringType{}, stringValueWriter
	case lt.Integer != nil:
		switch lt.Integer.BitWidth {
		case 64:
			if lt.Integer.IsSigned {
				return &arrow.Int64Type{}, int64ValueWriter
			}
			return &arrow.Uint64Type{}, uint64ValueWriter
		default:
			panic("unsupported int bit width")
		}
	default:
		panic("unsupported type")
	}
}

// uuidValueWriter writes parquet UUID values to an arrow array builder.
func uuidValueWriter(b array.Builder) func([]parquet.Value) {
	uuidBuilder := b.(*array.FixedSizeBinaryBuilder)
	return func(values []parquet.Value) {
		// Depending on the nullability of the column this could be optimized
		// further by reading UUIDs directly and adding all of them at once to
		// the array builder.
		for _, v := range values {
			if v.IsNull() {
				uuidBuilder.AppendNull()
			} else {
				uuidBuilder.Append(v.ByteArray())
			}
		}
	}
}

// stringValueWriter writes parquet string values to an arrow array builder.
func stringValueWriter(b array.Builder) func([]parquet.Value) {
	stringBuilder := b.(*array.StringBuilder)
	return func(values []parquet.Value) {
		// Depending on the nullability of the column this could be optimized
		// further by reading strings directly and adding all of them at once
		// to the array builder.
		for _, v := range values {
			if v.IsNull() {
				stringBuilder.AppendNull()
			} else {
				stringBuilder.Append(string(v.ByteArray()))
			}
		}
	}
}

// int64ValueWriter writes parquet int64 values to an arrow array builder.
func int64ValueWriter(b array.Builder) func([]parquet.Value) {
	int64Builder := b.(*array.Int64Builder)
	return func(values []parquet.Value) {
		// Depending on the nullability of the column this could be optimized
		// further by reading int64s directly and adding all of them at once to
		// the array builder.
		for _, v := range values {
			if v.IsNull() {
				int64Builder.AppendNull()
			} else {
				int64Builder.Append(v.Int64())
			}
		}
	}
}

// uint64ValueWriter writes parquet uint64 values to an arrow array builder.
func uint64ValueWriter(b array.Builder) func([]parquet.Value) {
	uint64Builder := b.(*array.Uint64Builder)
	return func(values []parquet.Value) {
		// Depending on the nullability of the column this could be optimized
		// further by reading uint64s directly and adding all of them at once
		// to the array builder.
		for _, v := range values {
			if v.IsNull() {
				uint64Builder.AppendNull()
			} else {
				uint64Builder.Append(uint64(v.Int64()))
			}
		}
	}
}
