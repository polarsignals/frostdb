package writer

import (
	"fmt"
	"io"

	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/segmentio/parquet-go"

	"github.com/polarsignals/frostdb/pqarrow/builder"
)

type ValueWriter interface {
	WritePage(p parquet.Page) error
	Write([]parquet.Value)
}

type binaryValueWriter struct {
	b       *builder.OptBinaryBuilder
	scratch struct {
		values []parquet.Value
	}
}

type NewWriterFunc func(b builder.ColumnBuilder, numValues int) ValueWriter

func NewBinaryValueWriter(b builder.ColumnBuilder, numValues int) ValueWriter {
	return &binaryValueWriter{
		b: b.(*builder.OptBinaryBuilder),
	}
}

func (w *binaryValueWriter) Write(values []parquet.Value) {
	w.b.AppendParquetValues(values)
}

func (w *binaryValueWriter) WritePage(p parquet.Page) error {
	if p.NumNulls() != 0 {
		reader := p.Values()
		if cap(w.scratch.values) < int(p.NumValues()) {
			w.scratch.values = make([]parquet.Value, p.NumValues())
		}
		w.scratch.values = w.scratch.values[:p.NumValues()]
		_, err := reader.ReadValues(w.scratch.values)
		// We're reading all values in the page so we always expect an io.EOF.
		if err != nil && err != io.EOF {
			return fmt.Errorf("read values: %w", err)
		}
		w.Write(w.scratch.values)
		return nil
	}

	// No nulls in page.
	values := p.Data()
	w.b.AppendData(values.ByteArray())
	return nil
}

type int64ValueWriter struct {
	b       *builder.OptInt64Builder
	scratch struct {
		values []parquet.Value
	}
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
	if p.NumNulls() != 0 {
		reader := p.Values()
		if cap(w.scratch.values) < int(p.NumValues()) {
			w.scratch.values = make([]parquet.Value, p.NumValues())
		}
		w.scratch.values = w.scratch.values[:p.NumValues()]
		_, err := reader.ReadValues(w.scratch.values)
		// We're reading all values in the page so we always expect an io.EOF.
		if err != nil && err != io.EOF {
			return fmt.Errorf("read values: %w", err)
		}
		w.Write(w.scratch.values)
		return nil
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

// TODO: implement fast path of writing the whole page directly.
func (w *uint64ValueWriter) WritePage(p parquet.Page) error {
	reader := p.Values()

	values := make([]parquet.Value, p.NumValues())
	_, err := reader.ReadValues(values)
	// We're reading all values in the page so we always expect an io.EOF.
	if err != nil && err != io.EOF {
		return fmt.Errorf("read values: %w", err)
	}

	w.Write(values)

	return nil
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
	v0 := values[0]
	rep := v0.RepetitionLevel()
	def := v0.DefinitionLevel()
	if rep == 0 && def == 0 {
		w.b.AppendNull()
	}

	listStart := false
	start := 0
	for i, v := range values {
		if v.RepetitionLevel() == 0 {
			if listStart {
				w.b.Append(true)
				w.values.Write(values[start:i])
			}
			listStart = true
			start = i
		}
	}

	// write final list
	w.b.Append(true)
	w.values.Write(values[start:])
}

// TODO: implement fast path of writing the whole page directly.
func (w *repeatedValueWriter) WritePage(p parquet.Page) error {
	reader := p.Values()

	values := make([]parquet.Value, p.NumValues())
	_, err := reader.ReadValues(values)
	// We're reading all values in the page so we always expect an io.EOF.
	if err != nil && err != io.EOF {
		return fmt.Errorf("read values: %w", err)
	}

	w.Write(values)

	return nil
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
	reader := p.Values()

	ireader, ok := reader.(parquet.DoubleReader)
	if ok {
		// fast path
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

	values := make([]parquet.Value, p.NumValues())
	_, err := reader.ReadValues(values)
	// We're reading all values in the page so we always expect an io.EOF.
	if err != nil && err != io.EOF {
		return fmt.Errorf("read values: %w", err)
	}

	w.Write(values)

	return nil
}

type booleanValueWriter struct {
	b       *builder.OptBooleanBuilder
	scratch struct {
		values []parquet.Value
	}
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
	if p.NumNulls() != 0 {
		reader := p.Values()
		if cap(w.scratch.values) < int(p.NumValues()) {
			w.scratch.values = make([]parquet.Value, p.NumValues())
		}
		w.scratch.values = w.scratch.values[:p.NumValues()]
		_, err := reader.ReadValues(w.scratch.values)
		// We're reading all values in the page so we always expect an io.EOF.
		if err != nil && err != io.EOF {
			return fmt.Errorf("read values: %w", err)
		}
		w.Write(w.scratch.values)
		return nil
	}

	// No nulls in page.
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

func (s *structWriter) WritePage(p parquet.Page) error {
	// TODO: there's probably a more optimized way to handle a page of values here; but doing this for simplicity of implementation right meow.
	values := make([]parquet.Value, p.NumValues())
	_, err := p.Values().ReadValues(values)
	// We're reading all values in the page so we always expect an io.EOF.
	if err != nil && err != io.EOF {
		return fmt.Errorf("read values: %w", err)
	}

	s.Write(values)
	return nil
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

func (m *mapWriter) WritePage(p parquet.Page) error {
	panic("not implemented")
}

func (m *mapWriter) Write(values []parquet.Value) {
	panic("not implemented")
}

type dictionaryValueWriter struct {
	b builder.ColumnBuilder
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
	}
}

func (w *dictionaryValueWriter) WritePage(p parquet.Page) error {
	values := make([]parquet.Value, p.NumValues())
	_, err := p.Values().ReadValues(values)
	// We're reading all values in the page so we always expect an io.EOF.
	if err != nil && err != io.EOF {
		return fmt.Errorf("read values: %w", err)
	}

	w.Write(values)
	return nil
}
