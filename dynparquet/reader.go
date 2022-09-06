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

type File struct {
	*parquet.File
}

func ReaderFromBytes(buf []byte) *SerializedBuffer {
	return &SerializedBuffer{
		file: buf,
	}
}

func (s *SerializedBuffer) Size() int {
	return len(s.file)
}

func (s *SerializedBuffer) Open() (*File, error) {
	f, err := parquet.OpenFile(bytes.NewReader(s.file), int64(len(s.file)))
	if err != nil {
		return nil, err
	}
	return &File{f}, nil
}

func (f *File) Reader() *parquet.Reader {
	return parquet.NewReader(f)
}

func (f *File) ParquetFile() *parquet.File {
	return f.File
}

func (f *File) NumRows() int64 {
	return f.Reader().NumRows()
}

func (f *File) NumRowGroups() int {
	return len(f.RowGroups())
}

func (f *File) DynamicRows() (DynamicRowReader, error) {
	rowGroups := f.RowGroups()
	drg := make([]DynamicRowGroup, len(rowGroups))
	for i, rowGroup := range rowGroups {
		var err error
		drg[i], err = f.newDynamicRowGroup(rowGroup)
		if err != nil {
			return nil, err
		}
	}
	return Concat(f.Schema().Fields(), drg...).DynamicRows(), nil
}

type serializedRowGroup struct {
	parquet.RowGroup
	dynCols map[string][]string
	fields  []parquet.Field
}

func (f *File) DynamicRowGroup(i int) (DynamicRowGroup, error) {
	return f.newDynamicRowGroup(f.RowGroups()[i])
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

func (f *File) newDynamicRowGroup(rowGroup parquet.RowGroup) (DynamicRowGroup, error) {
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

func (f *File) DynamicColumns() (map[string][]string, error) {
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
