package dynparquet

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/parquet-go/parquet-go"
)

const (
	DynamicColumnsKey = "dynamic_columns"
)

var ErrNoDynamicColumns = errors.New("no dynamic columns metadata found, it must be present")

type SerializedBuffer struct {
	f       *parquet.File
	dynCols map[string][]string
	fields  []parquet.Field
}

func ReaderFromBytes(buf []byte) (*SerializedBuffer, error) {
	f, err := parquet.OpenFile(bytes.NewReader(buf), int64(len(buf)))
	if err != nil {
		return nil, fmt.Errorf("error opening file from buffer: %w", err)
	}

	return NewSerializedBuffer(f)
}

func NewSerializedBuffer(f *parquet.File) (*SerializedBuffer, error) {
	dynColString, found := f.Lookup(DynamicColumnsKey)
	if !found {
		return nil, ErrNoDynamicColumns
	}

	dynCols, err := deserializeDynamicColumns(dynColString)
	if err != nil {
		return nil, fmt.Errorf("deserialize dynamic columns metadata %q: %w", dynColString, err)
	}

	return &SerializedBuffer{
		f:       f,
		dynCols: dynCols,
		fields:  f.Schema().Fields(),
	}, nil
}

func (b *SerializedBuffer) Reader() *parquet.GenericReader[any] {
	return parquet.NewGenericReader[any](b.ParquetFile())
}

func (b *SerializedBuffer) ParquetFile() *parquet.File {
	return b.f
}

func (b *SerializedBuffer) NumRows() int64 {
	return b.ParquetFile().NumRows()
}

func (b *SerializedBuffer) NumRowGroups() int {
	return len(b.f.RowGroups())
}

func (b *SerializedBuffer) DynamicRows() DynamicRowReader {
	rowGroups := b.f.RowGroups()
	drg := make([]DynamicRowGroup, len(rowGroups))
	for i, rowGroup := range rowGroups {
		drg[i] = b.newDynamicRowGroup(rowGroup)
	}
	return Concat(b.fields, drg...).DynamicRows()
}

func (b *SerializedBuffer) String() string {
	numRowGroups := b.NumRowGroups()
	numRows := b.NumRows()
	w := newPrettyWriter()
	_, _ = fmt.Fprintf(w, "num row groups: %d\tnum rows: %d\n", numRowGroups, numRows)
	for i := 0; i < numRowGroups; i++ {
		_, _ = fmt.Fprint(w, "---\n")
		w.writePrettyRowGroup(b.DynamicRowGroup(i))
	}
	_ = w.Flush()

	return w.String()
}

type serializedRowGroup struct {
	parquet.RowGroup
	dynCols map[string][]string
	fields  []parquet.Field
}

func (b *SerializedBuffer) DynamicRowGroup(i int) DynamicRowGroup {
	return b.newDynamicRowGroup(b.f.RowGroups()[i])
}

// MultiDynamicRowGroup returns all the row groups wrapped in a single multi
// row group.
func (b *SerializedBuffer) MultiDynamicRowGroup() DynamicRowGroup {
	return b.newDynamicRowGroup(parquet.MultiRowGroup(b.f.RowGroups()...))
}

func (b *SerializedBuffer) newDynamicRowGroup(rowGroup parquet.RowGroup) DynamicRowGroup {
	return &serializedRowGroup{
		RowGroup: rowGroup,
		dynCols:  b.dynCols,
		fields:   b.fields,
	}
}

func (g *serializedRowGroup) String() string {
	return prettyRowGroup(g)
}

func (g *serializedRowGroup) DynamicColumns() map[string][]string {
	return g.dynCols
}

func (g *serializedRowGroup) DynamicRows() DynamicRowReader {
	return newDynamicRowGroupReader(g, g.fields)
}

func (b *SerializedBuffer) DynamicColumns() map[string][]string {
	return b.dynCols
}
