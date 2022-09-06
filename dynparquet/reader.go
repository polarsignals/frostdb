package dynparquet

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/segmentio/parquet-go"
)

const (
	DynamicColumnsKey = "dynamic_columns"
)

var ErrNoDynamicColumns = errors.New("no dynamic columns metadata found, it must be present")

type SerializedBuffer struct {
	file []byte
}

func ReaderFromBytes(buf []byte) *SerializedBuffer {
	return &SerializedBuffer{
		file: buf,
	}
}

func (b *SerializedBuffer) Reader() (*parquet.Reader, error) {
	f, err := b.ParquetFile()
	if err != nil {
		return nil, err
	}
	return parquet.NewReader(f), nil
}

func (b *SerializedBuffer) ParquetFile() (*parquet.File, error) {
	f, err := parquet.OpenFile(bytes.NewReader(b.file), int64(len(b.file)))
	if err != nil {
		return nil, err
	}

	return f, nil
}

func (b *SerializedBuffer) NumRows() (int64, error) {
	r, err := b.Reader()
	if err != nil {
		return 0, err
	}
	return r.NumRows(), nil
}

func (b *SerializedBuffer) NumRowGroups() (int, error) {
	f, err := b.ParquetFile()
	if err != nil {
		return 0, err
	}
	return len(f.RowGroups()), nil
}

func (b *SerializedBuffer) DynamicRows() (DynamicRowReader, error) {
	file, err := b.ParquetFile()
	if err != nil {
		return nil, err
	}
	rowGroups := file.RowGroups()
	drg := make([]DynamicRowGroup, len(rowGroups))
	for i, rowGroup := range rowGroups {
		var err error
		drg[i], err = b.newDynamicRowGroup(rowGroup)
		if err != nil {
			return nil, err
		}
	}
	return Concat(file.Schema().Fields(), drg...).DynamicRows(), nil
}

type serializedRowGroup struct {
	parquet.RowGroup
	dynCols map[string][]string
	fields  []parquet.Field
}

func (b *SerializedBuffer) DynamicRowGroup(i int) (DynamicRowGroup, error) {
	f, err := b.ParquetFile()
	if err != nil {
		return nil, err
	}
	return b.newDynamicRowGroup(f.RowGroups()[i])
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

func (b *SerializedBuffer) newDynamicRowGroup(rowGroup parquet.RowGroup) (DynamicRowGroup, error) {
	f, err := b.ParquetFile()
	if err != nil {
		return nil, err
	}
	dynColString, found := f.Lookup(DynamicColumnsKey)
	if !found {
		return nil, fmt.Errorf("dynamic columns key not found")
	}

	dynCols, err := deserializeDynamicColumns(dynColString)
	if err != nil {
		return nil, err
	}
	return &serializedRowGroup{
		RowGroup: rowGroup,
		dynCols:  dynCols,
		fields:   f.Schema().Fields(),
	}, nil
}

func (g *serializedRowGroup) DynamicColumns() map[string][]string {
	return g.dynCols
}

func (g *serializedRowGroup) DynamicRows() DynamicRowReader {
	return newDynamicRowGroupReader(g, g.fields)
}

func (b *SerializedBuffer) DynamicColumns() (map[string][]string, error) {
	f, err := b.ParquetFile()
	if err != nil {
		return nil, err
	}
	dynColString, found := f.Lookup(DynamicColumnsKey)
	if !found {
		return nil, fmt.Errorf("dynamic columns key not found")
	}

	dynCols, err := deserializeDynamicColumns(dynColString)
	if err != nil {
		return nil, err
	}

	return dynCols, nil
}
