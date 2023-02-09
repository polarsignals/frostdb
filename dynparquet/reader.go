package dynparquet

import (
	"bytes"
	"errors"
	"fmt"
	"text/tabwriter"

	"github.com/segmentio/parquet-go"
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
		return nil, err
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

func (b *SerializedBuffer) Reader() *parquet.Reader {
	return parquet.NewReader(b.ParquetFile())
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
	var buf bytes.Buffer
	const (
		minWidth = 15
		tabWidth = 15
		padding  = 2
		padChar  = ' '
		noFlags  = 0
	)
	w := tabwriter.NewWriter(&buf, minWidth, tabWidth, padding, padChar, noFlags)

	truncateString := func(s string) string {
		const ellipses = "..."
		if len(s) > tabWidth {
			return s[:tabWidth-len(ellipses)] + ellipses
		}
		return s
	}

	numRowGroups := b.NumRowGroups()
	numRows := b.NumRows()
	_, _ = w.Write([]byte(fmt.Sprintf("row groups: %d\ttotal rows: %d\n", numRowGroups, numRows)))
	for i := 0; i < numRowGroups; i++ {
		_, _ = w.Write([]byte("---\n"))
		func() {
			rg := b.DynamicRowGroup(i)
			rows := rg.Rows()
			defer rows.Close()

			// Print sorting schema.
			for _, col := range rg.SortingColumns() {
				_, _ = w.Write([]byte(truncateString(fmt.Sprintf("%v", col.Path())) + "\t"))
			}
			_, _ = w.Write([]byte("\n"))

			rBuf := make([]parquet.Row, rg.NumRows())
			_, _ = rows.ReadRows(rBuf)

			for i := 0; i < len(rBuf); i++ {
				// Print only sorting columns.
				for _, col := range rg.SortingColumns() {
					leaf, ok := rg.Schema().Lookup(col.Path()...)
					if !ok {
						panic(fmt.Sprintf("sorting column not found: %v", col.Path()))
					}

					_, _ = w.Write([]byte(truncateString(rBuf[i][leaf.ColumnIndex].String()) + "\t"))
				}
				_, _ = w.Write([]byte("\n"))
			}
		}()
	}
	_ = w.Flush()

	return buf.String()
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

func (g *serializedRowGroup) DynamicColumns() map[string][]string {
	return g.dynCols
}

func (g *serializedRowGroup) DynamicRows() DynamicRowReader {
	return newDynamicRowGroupReader(g, g.fields)
}

func (b *SerializedBuffer) DynamicColumns() map[string][]string {
	return b.dynCols
}
