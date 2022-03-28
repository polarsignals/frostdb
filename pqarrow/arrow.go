package pqarrow

import (
	"errors"
	"fmt"
	"io"

	"github.com/apache/arrow/go/v7/arrow"
	"github.com/apache/arrow/go/v7/arrow/array"
	"github.com/apache/arrow/go/v7/arrow/memory"
	"github.com/segmentio/parquet-go"
	"github.com/segmentio/parquet-go/deprecated"
	"github.com/segmentio/parquet-go/encoding"

	"github.com/polarsignals/arcticdb/dynparquet"
	"github.com/polarsignals/arcticdb/query/logicalplan"
)

// ParquetRowGroupToArrowRecord converts a parquet row group to an arrow record.
func ParquetRowGroupToArrowRecord(
	pool memory.Allocator,
	rg parquet.RowGroup,
	projections []logicalplan.ColumnMatcher,
	filterExpr logicalplan.Expr,
	distinctColumns []logicalplan.ColumnMatcher,
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
			filterExpr,
			distinctColumns,
		)
	}
}

// rowBasedParquetRowGroupToArrowRecord converts a parquet row group to an arrow record row by row.
func rowBasedParquetRowGroupToArrowRecord(
	pool memory.Allocator,
	rg parquet.RowGroup,
) (arrow.Record, error) {
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
	filterExpr logicalplan.Expr,
	distinctColumns []logicalplan.ColumnMatcher,
) (arrow.Record, error) {
	s := rg.Schema()
	children := s.ChildNames()

	if len(distinctColumns) == 1 && filterExpr == nil {
		// We can use the faster path for a single distinct column by just
		// returning its dictionary.
		fields := make([]arrow.Field, 0, 1)
		cols := make([]arrow.Array, 0, 1)
		rows := int64(0)
		for i, child := range children {
			if distinctColumns[0].Match(child) {
				typ, nullable, array, err := parquetColumnToArrowArray(
					pool,
					s.ChildByName(child),
					rg.Column(i),
					true,
				)
				if err != nil {
					return nil, fmt.Errorf("convert parquet column to arrow array: %w", err)
				}
				fields = append(fields, arrow.Field{
					Name:     child,
					Type:     typ,
					Nullable: nullable,
				})
				cols = append(cols, array)
				rows = int64(array.Len())
			}
		}

		schema := arrow.NewSchema(fields, nil)
		return array.NewRecord(schema, cols, rows), nil
	}

	fields := make([]arrow.Field, 0, len(children))
	cols := make([]array.Interface, 0, len(children))

	for i, child := range children {
		if includedProjection(projections, child) {
			typ, nullable, array, err := parquetColumnToArrowArray(
				pool,
				s.ChildByName(child),
				rg.Column(i),
				false,
			)
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
	dictionaryOnly bool,
) (
	arrow.DataType,
	bool,
	arrow.Array,
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
		optional,
		repeated,
		lb,
		w,
		dictionaryOnly,
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
	optional bool,
	repeated bool,
	lb *array.ListBuilder,
	w valueWriter,
	dictionaryOnly bool,
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
		var values []parquet.Value
		dict := p.Dictionary()

		switch {
		case !repeated && dictionaryOnly && dict != nil:
			// If we are only writing the dictionary, we don't need to read
			// the values.
			dict.WriteTo(w)
		case !repeated && !optional && dict == nil:
			// If the column is not optional, we can read all values at once
			// consecutively without worrying about null values.
			p.Buffer().WriteTo(w)
		default:
			values = make([]parquet.Value, p.NumValues())
			reader := p.Values()
			_, err = reader.ReadValues(values)
			// We're reading all values in the page so we always expect an io.EOF.
			if err != nil && err != io.EOF {
				return fmt.Errorf("read values: %w", err)
			}

			w.Write(values)
		}

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

var (
	ErrPageTypeMismatch = errors.New("page type mismatch")
)

type valueWriter interface {
	encoding.Encoder
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

func (w *uuidValueWriter) Reset(io.Writer)                           {}
func (w *uuidValueWriter) EncodeBoolean(data []bool) error           { return ErrPageTypeMismatch }
func (w *uuidValueWriter) EncodeInt8(data []int8) error              { return ErrPageTypeMismatch }
func (w *uuidValueWriter) EncodeInt16(data []int16) error            { return ErrPageTypeMismatch }
func (w *uuidValueWriter) EncodeInt32(data []int32) error            { return ErrPageTypeMismatch }
func (w *uuidValueWriter) EncodeInt64(data []int64) error            { return ErrPageTypeMismatch }
func (w *uuidValueWriter) EncodeInt96(data []deprecated.Int96) error { return ErrPageTypeMismatch }
func (w *uuidValueWriter) EncodeFloat(data []float32) error          { return ErrPageTypeMismatch }
func (w *uuidValueWriter) EncodeDouble(data []float64) error         { return ErrPageTypeMismatch }
func (w *uuidValueWriter) EncodeByteArray(data encoding.ByteArrayList) error {
	return ErrPageTypeMismatch
}
func (w *uuidValueWriter) SetBitWidth(bitWidth int) {}

func (w *uuidValueWriter) EncodeFixedLenByteArray(size int, data []byte) error {
	values := make([][]byte, size)
	for i := 0; i < size; i++ {
		values[i] = data[i*16 : (i+1)*16]
	}

	w.b.AppendValues(values, nil)

	return nil
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

func (w *stringValueWriter) Reset(io.Writer)                           {}
func (w *stringValueWriter) EncodeBoolean(data []bool) error           { return ErrPageTypeMismatch }
func (w *stringValueWriter) EncodeInt8(data []int8) error              { return ErrPageTypeMismatch }
func (w *stringValueWriter) EncodeInt16(data []int16) error            { return ErrPageTypeMismatch }
func (w *stringValueWriter) EncodeInt32(data []int32) error            { return ErrPageTypeMismatch }
func (w *stringValueWriter) EncodeInt64(data []int64) error            { return ErrPageTypeMismatch }
func (w *stringValueWriter) EncodeInt96(data []deprecated.Int96) error { return ErrPageTypeMismatch }
func (w *stringValueWriter) EncodeFloat(data []float32) error          { return ErrPageTypeMismatch }
func (w *stringValueWriter) EncodeDouble(data []float64) error         { return ErrPageTypeMismatch }
func (w *stringValueWriter) EncodeFixedLenByteArray(size int, data []byte) error {
	return ErrPageTypeMismatch
}
func (w *stringValueWriter) SetBitWidth(bitWidth int) {}

func (w *stringValueWriter) EncodeByteArray(data encoding.ByteArrayList) error {
	values := make([]string, data.Len())
	for i := 0; i < data.Len(); i++ {
		values[i] = string(data.Index(i))
	}

	w.b.AppendValues(values, nil)
	return nil
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

func (w *int64ValueWriter) Reset(io.Writer)                           {}
func (w *int64ValueWriter) EncodeBoolean(data []bool) error           { return ErrPageTypeMismatch }
func (w *int64ValueWriter) EncodeInt8(data []int8) error              { return ErrPageTypeMismatch }
func (w *int64ValueWriter) EncodeInt16(data []int16) error            { return ErrPageTypeMismatch }
func (w *int64ValueWriter) EncodeInt32(data []int32) error            { return ErrPageTypeMismatch }
func (w *int64ValueWriter) EncodeInt96(data []deprecated.Int96) error { return ErrPageTypeMismatch }
func (w *int64ValueWriter) EncodeFloat(data []float32) error          { return ErrPageTypeMismatch }
func (w *int64ValueWriter) EncodeDouble(data []float64) error         { return ErrPageTypeMismatch }
func (w *int64ValueWriter) EncodeByteArray(data encoding.ByteArrayList) error {
	return ErrPageTypeMismatch
}
func (w *int64ValueWriter) EncodeFixedLenByteArray(size int, data []byte) error {
	return ErrPageTypeMismatch
}
func (w *int64ValueWriter) SetBitWidth(bitWidth int) {}

func (w *int64ValueWriter) EncodeInt64(data []int64) error {
	w.b.AppendValues(data, nil)
	return nil
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

func (w *uint64ValueWriter) Reset(io.Writer)                           {}
func (w *uint64ValueWriter) EncodeBoolean(data []bool) error           { return ErrPageTypeMismatch }
func (w *uint64ValueWriter) EncodeInt8(data []int8) error              { return ErrPageTypeMismatch }
func (w *uint64ValueWriter) EncodeInt16(data []int16) error            { return ErrPageTypeMismatch }
func (w *uint64ValueWriter) EncodeInt32(data []int32) error            { return ErrPageTypeMismatch }
func (w *uint64ValueWriter) EncodeInt96(data []deprecated.Int96) error { return ErrPageTypeMismatch }
func (w *uint64ValueWriter) EncodeFloat(data []float32) error          { return ErrPageTypeMismatch }
func (w *uint64ValueWriter) EncodeDouble(data []float64) error         { return ErrPageTypeMismatch }
func (w *uint64ValueWriter) EncodeByteArray(data encoding.ByteArrayList) error {
	return ErrPageTypeMismatch
}
func (w *uint64ValueWriter) EncodeFixedLenByteArray(size int, data []byte) error {
	return ErrPageTypeMismatch
}
func (w *uint64ValueWriter) SetBitWidth(bitWidth int) {}

func (w *uint64ValueWriter) EncodeInt64(data []int64) error {
	values := make([]uint64, len(data))
	for i, v := range data {
		values[i] = uint64(v)
	}
	w.b.AppendValues(values, nil)
	return nil
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

var (
	ErrBatchWriteNotSupported = errors.New("batch write not supported")
)

func (w *repeatedValueWriter) Reset(io.Writer)                 {}
func (w *repeatedValueWriter) EncodeBoolean(data []bool) error { return ErrBatchWriteNotSupported }
func (w *repeatedValueWriter) EncodeInt8(data []int8) error    { return ErrBatchWriteNotSupported }
func (w *repeatedValueWriter) EncodeInt16(data []int16) error  { return ErrBatchWriteNotSupported }
func (w *repeatedValueWriter) EncodeInt32(data []int32) error  { return ErrBatchWriteNotSupported }
func (w *repeatedValueWriter) EncodeInt96(data []deprecated.Int96) error {
	return ErrBatchWriteNotSupported
}
func (w *repeatedValueWriter) EncodeFloat(data []float32) error  { return ErrBatchWriteNotSupported }
func (w *repeatedValueWriter) EncodeDouble(data []float64) error { return ErrBatchWriteNotSupported }
func (w *repeatedValueWriter) EncodeByteArray(data encoding.ByteArrayList) error {
	return ErrBatchWriteNotSupported
}
func (w *repeatedValueWriter) EncodeFixedLenByteArray(size int, data []byte) error {
	return ErrBatchWriteNotSupported
}
func (w *repeatedValueWriter) SetBitWidth(bitWidth int) {}

func (w *repeatedValueWriter) EncodeInt64(data []int64) error {
	return ErrBatchWriteNotSupported
}
