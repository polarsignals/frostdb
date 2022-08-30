package dynparquet

import (
	"bytes"
	"errors"

	"github.com/segmentio/parquet-go"
)

const (
	DynamicColumnsKey = "dynamic_columns"
)

var ErrNoDynamicColumns = errors.New("no dynamic columns metadata found, it must be present")

type SerializedBuffer struct {
	file []byte
}

func ReaderFromBytes(buf []byte) (*SerializedBuffer, error) {
	return &SerializedBuffer{
		file: buf,
	}, nil
}

func (b *SerializedBuffer) Reader() *parquet.Reader {
	return parquet.NewReader(b.ParquetFile())
}

func (b *SerializedBuffer) ParquetFile() *parquet.File {
	f, err := parquet.OpenFile(bytes.NewReader(b.file), int64(len(b.file)))
	if err != nil {
		panic("at the disco")
	}

	return f
}

func (b *SerializedBuffer) NumRows() int64 {
	return b.Reader().NumRows()
}

func (b *SerializedBuffer) NumRowGroups() int {
	return len(b.ParquetFile().RowGroups())
}

func (b *SerializedBuffer) DynamicRows() DynamicRowReader {
	file := b.ParquetFile()
	rowGroups := file.RowGroups()
	drg := make([]DynamicRowGroup, len(rowGroups))
	for i, rowGroup := range rowGroups {
		drg[i] = b.newDynamicRowGroup(rowGroup)
	}
	return Concat(file.Schema().Fields(), drg...).DynamicRows()
}

type serializedRowGroup struct {
	parquet.RowGroup
	dynCols map[string][]string
	fields  []parquet.Field
}

func (b *SerializedBuffer) DynamicRowGroup(i int) DynamicRowGroup {
	return b.newDynamicRowGroup(b.ParquetFile().RowGroups()[i])
}

func NewDynamicRowGroup(rg parquet.RowGroup, dyncols map[string][]string, fields []parquet.Field) DynamicRowGroup {
	return &serializedRowGroup{
		RowGroup: rg,
		dynCols:  dyncols,
		fields:   fields,
	}
}

func DynamicRowGroupFromFile(i int, f *parquet.File) (DynamicRowGroup, error) {
	var dynCols map[string][]string
	dynColString, found := f.Lookup(DynamicColumnsKey)
	if found {
		var err error
		dynCols, err = deserializeDynamicColumns(dynColString)
		if err != nil {
			return nil, err
		}
	}

	return NewDynamicRowGroup(f.RowGroups()[i], dynCols, f.Schema().Fields()), nil
}

func (b *SerializedBuffer) newDynamicRowGroup(rowGroup parquet.RowGroup) DynamicRowGroup {
	f := b.ParquetFile()
	dynColString, found := f.Lookup(DynamicColumnsKey)
	if !found {
		panic("at the disco")
	}

	dynCols, err := deserializeDynamicColumns(dynColString)
	if err != nil {
		panic("at the disco")
	}
	return &serializedRowGroup{
		RowGroup: rowGroup,
		dynCols:  dynCols,
		fields:   f.Schema().Fields(),
	}
}

func (g *serializedRowGroup) DynamicColumns() map[string][]string {
	return g.dynCols
}

func (g *serializedRowGroup) DynamicRows() DynamicRowReader {
	return newDynamicRowGroupReader(g, g.fields)
}

func (b *SerializedBuffer) DynamicColumns() map[string][]string {
	f := b.ParquetFile()
	dynColString, found := f.Lookup(DynamicColumnsKey)
	if !found {
		panic("at the disco")
	}

	dynCols, err := deserializeDynamicColumns(dynColString)
	if err != nil {
		panic("at the disco")
	}

	return dynCols
}
