package dynparquet

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

const (
	DynamicColumnsKey = "dynamic_columns"
)

var ErrNoDynamicColumns = errors.New("no dynamic columns metadata found, it must be present")

type SerializedBuffer struct {
	f       *parquet.File
	dynCols map[string][]string
	fields  []parquet.Field

	indexes []format.ColumnIndex
	offsets []format.OffsetIndex
}

type SerializedBufferOption func(*SerializedBuffer)

func WithIndexes(indexes []format.ColumnIndex) SerializedBufferOption {
	return func(b *SerializedBuffer) {
		b.indexes = indexes
	}
}

func WithOffsets(offsets []format.OffsetIndex) SerializedBufferOption {
	return func(b *SerializedBuffer) {
		b.offsets = offsets
	}
}

func ReaderFromBytes(buf []byte) (*SerializedBuffer, error) {
	f, err := parquet.OpenFile(bytes.NewReader(buf), int64(len(buf)))
	if err != nil {
		return nil, err
	}

	return NewSerializedBuffer(f)
}

func NewSerializedBuffer(f *parquet.File, options ...SerializedBufferOption) (*SerializedBuffer, error) {
	dynColString, found := f.Lookup(DynamicColumnsKey)
	if !found {
		return nil, ErrNoDynamicColumns
	}

	dynCols, err := deserializeDynamicColumns(dynColString)
	if err != nil {
		return nil, fmt.Errorf("deserialize dynamic columns metadata %q: %w", dynColString, err)
	}

	sb := &SerializedBuffer{
		f:       f,
		dynCols: dynCols,
		fields:  f.Schema().Fields(),
	}

	for _, option := range options {
		option(sb)
	}

	return sb, nil
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
	_, _ = w.Write([]byte(fmt.Sprintf("row groups: %d\ttotal rows: %d\n", numRowGroups, numRows)))
	for i := 0; i < numRowGroups; i++ {
		_, _ = w.Write([]byte("---\n"))
		w.writePrettyRowGroup(b.DynamicRowGroup(i))
	}
	_ = w.Flush()

	return w.String()
}

type serializedRowGroup struct {
	indexes  []format.ColumnIndex
	offsets  []format.OffsetIndex
	rowGroup parquet.RowGroup
	dynCols  map[string][]string
	fields   []parquet.Field
}

func (b *SerializedBuffer) DynamicRowGroup(i int) DynamicRowGroup {
	srg := &serializedRowGroup{
		rowGroup: b.f.RowGroups()[i],
		dynCols:  b.dynCols,
		fields:   b.fields,
	}

	if b.indexes != nil {
		cols := len(b.f.Schema().Fields())
		srg.indexes = b.indexes[i*cols : (i*cols)+cols]
	}

	if b.offsets != nil {
		cols := len(b.f.Schema().Fields())
		srg.offsets = b.offsets[i*cols : (i*cols)+cols]
	}

	return srg
}

// MultiDynamicRowGroup returns all the row groups wrapped in a single multi
// row group.
func (b *SerializedBuffer) MultiDynamicRowGroup() DynamicRowGroup {
	return b.newDynamicRowGroup(parquet.MultiRowGroup(b.f.RowGroups()...))
}

func (b *SerializedBuffer) newDynamicRowGroup(rowGroup parquet.RowGroup) DynamicRowGroup {
	return &serializedRowGroup{
		rowGroup: rowGroup,
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

func (b *serializedRowGroup) NumRows() int64 { return b.rowGroup.NumRows() }
func (b *serializedRowGroup) ColumnChunks() []parquet.ColumnChunk {
	chunks := b.rowGroup.ColumnChunks()
	if b.indexes != nil && b.offsets != nil {
		cachedColumnChunks := make([]parquet.ColumnChunk, len(chunks))
		for i, chunk := range chunks {
			chunks[i] = &cachedColumnChunk{
				index:  b.indexes[i],
				offset: b.offsets[i],
				chunk:  chunk,
			}
		}

		return cachedColumnChunks
	}

	return chunks
}

func (b *serializedRowGroup) Schema() *parquet.Schema { return b.rowGroup.Schema() }
func (b *serializedRowGroup) SortingColumns() []parquet.SortingColumn {
	return b.rowGroup.SortingColumns()
}
func (b *serializedRowGroup) Rows() parquet.Rows { return b.rowGroup.Rows() }

type cachedColumnChunk struct {
	index  format.ColumnIndex
	offset format.OffsetIndex
	chunk  parquet.ColumnChunk
}

func (c *cachedColumnChunk) Type() parquet.Type   { return c.chunk.Type() }
func (c *cachedColumnChunk) Column() int          { return c.chunk.Column() }
func (c *cachedColumnChunk) Pages() parquet.Pages { return c.chunk.Pages() }
func (c *cachedColumnChunk) ColumnIndex() parquet.ColumnIndex {
	return parquet.NewColumnIndex(c.chunk.Type().Kind(), &c.index)
}
func (c *cachedColumnChunk) OffsetIndex() parquet.OffsetIndex {
	return (*parquet.FileOffsetIndex)(&c.offset)
}
func (c *cachedColumnChunk) BloomFilter() parquet.BloomFilter { return c.chunk.BloomFilter() }
func (c *cachedColumnChunk) NumValues() int64                 { return c.chunk.NumValues() }
