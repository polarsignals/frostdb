package writer

import (
	"fmt"
	"io"

	"github.com/apache/arrow/go/v8/arrow/array"
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

	w.b.Append(true)
	w.values.Write(values)
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
