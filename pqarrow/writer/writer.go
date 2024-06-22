package writer

import (
	"errors"
	"fmt"
	"io"
	"unsafe"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/parquet-go/parquet-go"

	"github.com/polarsignals/frostdb/pqarrow/builder"
)

type ValueWriter interface {
	// Write writes a slice of values to the underlying builder (slow path).
	Write([]parquet.Value)
}

type PageWriter interface {
	ValueWriter
	// WritePage is the optimized path for writing a page of values to the
	// underlying builder. There are cases in which the given page cannot be
	// written directly, in which case ErrCannotWritePageDirectly is returned.
	// The caller should fall back to writing values.
	WritePage(parquet.Page) error
}

var ErrCannotWritePageDirectly = errors.New("cannot write page directly")

type writerBase struct{}

func (w writerBase) canWritePageDirectly(p parquet.Page) bool {
	// Currently, for most writers, only pages with zero nulls and no dictionary
	// can be written.
	return p.NumNulls() == 0 && p.Dictionary() == nil
}

type binaryValueWriter struct {
	writerBase
	b *builder.OptBinaryBuilder
}

type NewWriterFunc func(b builder.ColumnBuilder, numValues int) ValueWriter

func NewBinaryValueWriter(b builder.ColumnBuilder, _ int) ValueWriter {
	return &binaryValueWriter{
		b: b.(*builder.OptBinaryBuilder),
	}
}

func (w *binaryValueWriter) Write(values []parquet.Value) {
	if err := w.b.AppendParquetValues(values); err != nil {
		panic("unable to write value") // TODO: handle this error gracefully
	}
}

func (w *binaryValueWriter) WritePage(p parquet.Page) error {
	if !w.canWritePageDirectly(p) {
		return ErrCannotWritePageDirectly
	}
	values := p.Data()
	return w.b.AppendData(values.ByteArray())
}

type int64ValueWriter struct {
	writerBase
	b *builder.OptInt64Builder
}

func NewInt64ValueWriter(b builder.ColumnBuilder, _ int) ValueWriter {
	res := &int64ValueWriter{
		b: b.(*builder.OptInt64Builder),
	}
	return res
}

func (w *int64ValueWriter) Write(values []parquet.Value) {
	w.b.AppendParquetValues(values)
}

func (w *int64ValueWriter) WritePage(p parquet.Page) error {
	if !w.canWritePageDirectly(p) {
		return ErrCannotWritePageDirectly
	}
	// No nulls in page.
	values := p.Data()
	w.b.AppendData(values.Int64())
	return nil
}

type uint64ValueWriter struct {
	b *array.Uint64Builder
}

func NewUint64ValueWriter(b builder.ColumnBuilder, numValues int) ValueWriter {
	res := &uint64ValueWriter{
		b: b.(*array.Uint64Builder),
	}
	res.b.Reserve(numValues)
	return res
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
	b      *builder.ListBuilder
	values ValueWriter
}

func NewListValueWriter(newValueWriter func(b builder.ColumnBuilder, numValues int) ValueWriter) func(b builder.ColumnBuilder, numValues int) ValueWriter {
	return func(b builder.ColumnBuilder, numValues int) ValueWriter {
		builder := b.(*builder.ListBuilder)

		return &repeatedValueWriter{
			b:      builder,
			values: newValueWriter(builder.ValueBuilder(), numValues),
		}
	}
}

func (w *repeatedValueWriter) Write(values []parquet.Value) {
	listStart := false
	start := 0
	for i, v := range values {
		if v.RepetitionLevel() == 0 {
			if listStart {
				w.b.Append(true)
				w.values.Write(values[start:i])
			}

			if v.DefinitionLevel() == 0 {
				w.b.AppendNull()
				listStart = false
				start = i + 1
			} else {
				listStart = true
				start = i
			}
		}
	}

	// write final list
	if len(values[start:]) > 0 {
		w.b.Append(true)
		w.values.Write(values[start:])
	}
}

type float64ValueWriter struct {
	b   *array.Float64Builder
	buf []float64
}

func NewFloat64ValueWriter(b builder.ColumnBuilder, numValues int) ValueWriter {
	res := &float64ValueWriter{
		b: b.(*array.Float64Builder),
	}
	res.b.Reserve(numValues)
	return res
}

func (w *float64ValueWriter) Write(values []parquet.Value) {
	for _, v := range values {
		if v.IsNull() {
			w.b.AppendNull()
		} else {
			w.b.Append(v.Double())
		}
	}
}

func (w *float64ValueWriter) WritePage(p parquet.Page) error {
	ireader, ok := p.Values().(parquet.DoubleReader)
	if !ok {
		return ErrCannotWritePageDirectly
	}

	if w.buf == nil {
		w.buf = make([]float64, p.NumValues())
	}
	values := w.buf
	for {
		n, err := ireader.ReadDoubles(values)
		if err != nil && err != io.EOF {
			return fmt.Errorf("read values: %w", err)
		}

		w.b.AppendValues(values[:n], nil)
		if err == io.EOF {
			break
		}
	}
	return nil
}

type booleanValueWriter struct {
	writerBase
	b *builder.OptBooleanBuilder
}

func NewBooleanValueWriter(b builder.ColumnBuilder, numValues int) ValueWriter {
	res := &booleanValueWriter{
		b: b.(*builder.OptBooleanBuilder),
	}
	res.b.Reserve(numValues)
	return res
}

func (w *booleanValueWriter) Write(values []parquet.Value) {
	w.b.AppendParquetValues(values)
}

func (w *booleanValueWriter) WritePage(p parquet.Page) error {
	if !w.canWritePageDirectly(p) {
		return ErrCannotWritePageDirectly
	}
	values := p.Data()
	w.b.Append(values.Boolean(), int(p.NumValues()))
	return nil
}

type structWriter struct {
	// offset is the column index offset that this node has in the overall schema
	offset int
	b      *array.StructBuilder
}

func NewStructWriterFromOffset(offset int) NewWriterFunc {
	return func(b builder.ColumnBuilder, _ int) ValueWriter {
		return &structWriter{
			offset: offset,
			b:      b.(*array.StructBuilder),
		}
	}
}

func (s *structWriter) Write(values []parquet.Value) {
	total := 0
	for _, v := range values {
		if v.RepetitionLevel() == 0 {
			total++
			if total > s.b.Len() {
				s.b.Append(true)
			}
		}
	}
	// recursively search the struct builder for the leaf that matches the values column index
	_, ok := s.findLeafBuilder(values[0].Column(), s.offset, s.b, values)
	if !ok {
		panic("unable to write values to builder")
	}
}

// findLeafBuilder is a recursive function to find the leaf builder whose column index matches the search index.
// It returns the number of leaves found for the given builder and if one of the leaves in the builder was appended to.
func (s *structWriter) findLeafBuilder(searchIndex, currentIndex int, builder array.Builder, values []parquet.Value) (int, bool) {
	switch b := builder.(type) {
	case *array.StructBuilder:
		totalLeaves := 0
		for i := 0; i < b.NumField(); i++ {
			// recurse
			leaves, appended := s.findLeafBuilder(searchIndex, currentIndex+totalLeaves, b.FieldBuilder(i), values)
			if appended {
				if b.FieldBuilder(i).Len() != b.Len() { // NOTE: I'm unsure if this is correct for multi-nested structs
					b.Append(true)
				}
				return b.NumField(), true
			}

			totalLeaves += leaves
		}

		return totalLeaves, false

	case *array.Int64Builder:
		if searchIndex == currentIndex {
			for _, v := range values {
				switch v.IsNull() {
				case true:
					b.AppendNull()
				default:
					b.Append(v.Int64())
				}
			}
			return 1, true
		}
	case *array.Uint64Builder:
		if searchIndex == currentIndex {
			for _, v := range values {
				switch v.IsNull() {
				case true:
					b.AppendNull()
				default:
					b.Append(v.Uint64())
				}
			}
			return 1, true
		}
	case *array.Float64Builder:
		if searchIndex == currentIndex {
			for _, v := range values {
				switch v.IsNull() {
				case true:
					b.AppendNull()
				default:
					b.Append(v.Double())
				}
			}
			return 1, true
		}
	case *array.StringBuilder:
		if searchIndex == currentIndex {
			for _, v := range values {
				switch v.IsNull() {
				case true:
					b.AppendNull()
				default:
					b.Append(string(v.ByteArray()))
				}
			}
			return 1, true
		}
	case *array.BinaryBuilder:
		if searchIndex == currentIndex {
			for _, v := range values {
				switch v.IsNull() {
				case true:
					b.AppendNull()
				default:
					b.Append(v.ByteArray())
				}
			}
			return 1, true
		}
	case *array.BinaryDictionaryBuilder:
		if searchIndex == currentIndex {
			for _, v := range values {
				switch v.IsNull() {
				case true:
					b.AppendNull()
				default:
					if err := b.Append(v.ByteArray()); err != nil {
						panic("failed to append to dictionary")
					}
				}
			}
			return 1, true
		}
	default:
		panic(fmt.Sprintf("unsuported value type: %v", b))
	}

	return 1, false
}

type mapWriter struct {
	b *array.MapBuilder
}

func NewMapWriter(b builder.ColumnBuilder, _ int) ValueWriter {
	return &mapWriter{
		b: b.(*array.MapBuilder),
	}
}

func (m *mapWriter) Write(_ []parquet.Value) {
	panic("not implemented")
}

type dictionaryValueWriter struct {
	b       builder.ColumnBuilder
	scratch struct {
		values []parquet.Value
	}
}

func NewDictionaryValueWriter(b builder.ColumnBuilder, _ int) ValueWriter {
	res := &dictionaryValueWriter{
		b: b,
	}
	return res
}

func (w *dictionaryValueWriter) Write(values []parquet.Value) {
	switch db := w.b.(type) {
	case *array.BinaryDictionaryBuilder:
		for _, v := range values {
			if v.IsNull() {
				db.AppendNull()
			} else {
				if err := db.Append(v.Bytes()); err != nil {
					panic("failed to append to dictionary")
				}
			}
		}
	default:
		panic(fmt.Sprintf("unsupported dictionary type: %T", db))
	}
}

// WritePage writes a page directly to the dictionaryValueWriter. It is valid
// for callers to write a page with dictionary values directly (i.e.
// p.Dictionary() == nil).
func (w *dictionaryValueWriter) WritePage(p parquet.Page) error {
	if p.NumNulls() > 0 {
		return ErrCannotWritePageDirectly
	}

	dict := p.Dictionary()
	if dict == nil {
		// This page represents the dictionary values.
		v := p.Data()
		switch b := w.b.(type) {
		case *array.BinaryDictionaryBuilder:
			data, offsets := v.ByteArray()
			for i := 0; i < len(offsets)-1; i++ {
				if err := b.Append(data[offsets[i]:offsets[i+1]]); err != nil {
					return fmt.Errorf("appending dictionary value: %w", err)
				}
			}
		default:
			panic(fmt.Sprintf("unsupported dictionary type: %T", b))
		}
		return nil
	}

	indexData := p.Data()
	dictData := dict.Page().Data()

	indexes := indexData.Int32()
	flatDictValues, dictValueOffsets := dictData.ByteArray()

	dictValues := array.NewBinaryData(
		array.NewData(
			arrow.BinaryTypes.Binary,
			p.Dictionary().Len(),
			[]*memory.Buffer{
				nil,
				memory.NewBufferBytes(unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(dictValueOffsets))), len(dictValueOffsets)*arrow.Uint32SizeBytes)),
				memory.NewBufferBytes(flatDictValues),
			},
			nil, 0, 0),
	)

	switch b := w.b.(type) {
	case *array.BinaryDictionaryBuilder:
		// TODO(asubiotto): Improve this to be able to reuse the dictionary.
		if err := b.InsertDictValues(dictValues); err != nil {
			return fmt.Errorf("inserting dict values: %w", err)
		}
		b.IndexBuilder().Builder.(*array.Uint32Builder).AppendValues(unsafe.Slice((*uint32)(unsafe.Pointer(unsafe.SliceData(indexes))), len(indexes)), nil)
		b.Resize(b.IndexBuilder().Len())
	default:
		panic(fmt.Sprintf("unsupported dictionary type: %T", b))
	}

	return nil
}
