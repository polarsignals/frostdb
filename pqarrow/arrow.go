package pqarrow

import (
	"fmt"
	"io"

	"github.com/apache/arrow/go/v7/arrow"
	"github.com/apache/arrow/go/v7/arrow/array"
	"github.com/apache/arrow/go/v7/arrow/memory"
	"github.com/segmentio/parquet-go"

	"github.com/polarsignals/arcticdb/dynparquet"
	"github.com/polarsignals/arcticdb/query/logicalplan"
)

// ParquetRowGroupToArrowRecord converts a parquet row group to an arrow record.
func ParquetRowGroupToArrowRecord(
	pool memory.Allocator,
	rg parquet.RowGroup,
	projections []logicalplan.ColumnMatcher,
) (arrow.Record, error) {
	switch rg.(type) {
	case *dynparquet.MergedRowGroup:
		return rowBasedParquetRowGroupToArrowRecord(
			pool,
			rg,
		)
	default:
		return contiguousParquetRowGroupToArrowRecord(
			pool,
			rg,
			projections,
		)
	}
}

// rowBasedParquetRowGroupToArrowRecord converts a parquet row group to an arrow record row by row.
func rowBasedParquetRowGroupToArrowRecord(pool memory.Allocator, rg parquet.RowGroup) (arrow.Record, error) {
	s := rg.Schema()

	children := s.ChildNames()
	fields := make([]arrow.Field, 0, len(children))
	newWriterFuncs := make([]func(array.Builder) valueWriter, 0, len(children))
	for _, child := range children {
		node := s.ChildByName(child)
		typ, newValueWriter := parquetNodeToType(node)
		nullable := false
		if node.Optional() {
			nullable = true
		}

		if node.Repeated() {
			typ = arrow.ListOf(typ)
			newValueWriter = listValueWriter(newValueWriter)
		}
		newWriterFuncs = append(newWriterFuncs, newValueWriter)

		fields = append(fields, arrow.Field{
			Name:     child,
			Type:     typ,
			Nullable: nullable,
		})
	}

	writers := make([]valueWriter, len(children))
	b := array.NewRecordBuilder(pool, arrow.NewSchema(fields, nil))
	for i, column := range b.Fields() {
		writers[i] = newWriterFuncs[i](column)
	}

	rows := rg.Rows()
	row := make(parquet.Row, 0, 50) // Random guess.
	var err error
	for {
		row, err = rows.ReadRow(row[:0])
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read row: %w", err)
		}

		for i, writer := range writers {
			values := dynparquet.ValuesForIndex(row, i)
			writer.Write(values)
		}
	}

	return b.NewRecord(), nil
}

// contiguousParquetRowGroupToArrowRecord converts a parquet row group to an arrow record.
func contiguousParquetRowGroupToArrowRecord(
	pool memory.Allocator,
	rg parquet.RowGroup,
	projections []logicalplan.ColumnMatcher,
) (arrow.Record, error) {
	s := rg.Schema()

	children := s.ChildNames()
	fields := make([]arrow.Field, 0, len(children))
	cols := make([]array.Interface, 0, len(children))
	for i, child := range children {
		if includedProjection(projections, child) {
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
	}

	schema := arrow.NewSchema(fields, nil)
	return array.NewRecord(schema, cols, rg.NumRows()), nil
}

func includedProjection(projections []logicalplan.ColumnMatcher, name string) bool {
	if len(projections) == 0 {
		return true
	}

	for _, p := range projections {
		if p.Match(name) {
			return true
		}
	}
	return false
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
		w  valueWriter
		b  array.Builder
		lb *array.ListBuilder

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
		w = newValueWriter(lb.ValueBuilder())
		b = lb
	} else {
		b = array.NewBuilder(pool, at)
		w = newValueWriter(b)
	}
	defer b.Release()

	err := writePagesToArray(
		c.Pages(),
		repeated,
		lb,
		w,
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
	w valueWriter,
) error {
	// We are potentially writing multiple pages to the same array, so we need
	// to keep track of the index of the offsets in case this is a List-type.
	i := 0
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
		if err != nil && err != io.EOF {
			return fmt.Errorf("read values: %w", err)
		}

		w.Write(values)

		if repeated {
			offsets := []int32{}
			validity := []bool{}
			for _, v := range values {
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
				i++
			}

			lb.AppendValues(offsets, validity)
		}
	}

	return nil
}

// ParquetNodeToType converts a parquet node to an arrow type.
func ParquetNodeToType(n parquet.Node) arrow.DataType {
	typ, _ := parquetNodeToType(n)
	return typ
}

// parquetNodeToType converts a parquet node to an arrow type and a function to
// create a value writer.
func parquetNodeToType(n parquet.Node) (arrow.DataType, func(b array.Builder) valueWriter) {
	t := n.Type()
	lt := t.LogicalType()
	switch {
	case lt.UUID != nil:
		return &arrow.FixedSizeBinaryType{
			ByteWidth: 16,
		}, newUUIDValueWriter
	case lt.UTF8 != nil:
		return &arrow.StringType{}, newStringValueWriter
	case lt.Integer != nil:
		switch lt.Integer.BitWidth {
		case 64:
			if lt.Integer.IsSigned {
				return &arrow.Int64Type{}, newInt64ValueWriter
			}
			return &arrow.Uint64Type{}, newUint64ValueWriter
		default:
			panic("unsupported int bit width")
		}
	default:
		panic("unsupported type")
	}
}

type valueWriter interface {
	Write([]parquet.Value)
}

type uuidValueWriter struct {
	b *array.FixedSizeBinaryBuilder
}

func newUUIDValueWriter(b array.Builder) valueWriter {
	return &uuidValueWriter{
		b: b.(*array.FixedSizeBinaryBuilder),
	}
}

func (w *uuidValueWriter) Write(values []parquet.Value) {
	// Depending on the nullability of the column this could be optimized
	// further by reading UUIDs directly and adding all of them at once to
	// the array builder.
	for _, v := range values {
		if v.IsNull() {
			w.b.AppendNull()
		} else {
			w.b.Append(v.ByteArray())
		}
	}
}

type stringValueWriter struct {
	b *array.StringBuilder
}

func newStringValueWriter(b array.Builder) valueWriter {
	return &stringValueWriter{
		b: b.(*array.StringBuilder),
	}
}

func (w *stringValueWriter) Write(values []parquet.Value) {
	// Depending on the nullability of the column this could be optimized
	// further by reading strings directly and adding all of them at once
	// to the array builder.
	for _, v := range values {
		if v.IsNull() {
			w.b.AppendNull()
		} else {
			w.b.Append(string(v.ByteArray()))
		}
	}
}

type int64ValueWriter struct {
	b *array.Int64Builder
}

func newInt64ValueWriter(b array.Builder) valueWriter {
	return &int64ValueWriter{
		b: b.(*array.Int64Builder),
	}
}

func (w *int64ValueWriter) Write(values []parquet.Value) {
	// Depending on the nullability of the column this could be optimized
	// further by reading int64s directly and adding all of them at once to
	// the array builder.
	for _, v := range values {
		if v.IsNull() {
			w.b.AppendNull()
		} else {
			w.b.Append(v.Int64())
		}
	}
}

type uint64ValueWriter struct {
	b *array.Uint64Builder
}

func newUint64ValueWriter(b array.Builder) valueWriter {
	return &uint64ValueWriter{
		b: b.(*array.Uint64Builder),
	}
}

func (w *uint64ValueWriter) Write(values []parquet.Value) {
	// Depending on the nullability of the column this could be optimized
	// further by reading uint64s directly and adding all of them at once
	// to the array builder.
	for _, v := range values {
		if v.IsNull() {
			w.b.AppendNull()
		} else {
			w.b.Append(uint64(v.Int64()))
		}
	}
}

type repeatedValueWriter struct {
	b      *array.ListBuilder
	values valueWriter
}

// listValueWriter writes repeated parquet values to an arrow list array builder.
func listValueWriter(newValueWriterFunc func(array.Builder) valueWriter) func(array.Builder) valueWriter {
	return func(b array.Builder) valueWriter {
		builder := b.(*array.ListBuilder)

		return &repeatedValueWriter{
			b:      builder,
			values: newValueWriterFunc(builder.ValueBuilder()),
		}
	}
}

func (w *repeatedValueWriter) Write(values []parquet.Value) {
	v0 := values[0]
	rep := v0.RepetitionLevel()
	def := v0.DefinitionLevel()
	if rep == 0 && def == 0 {
		w.b.AppendNull()
	}

	w.b.Append(true)
	w.values.Write(values)
}
