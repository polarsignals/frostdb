package dynparquet

import (
	"fmt"
	"sort"

	"github.com/segmentio/parquet-go"
)

// ColumnDefinition describes a column in a dynamic parquet schema.
type ColumnDefinition struct {
	Name          string
	StorageLayout parquet.Node
	Dynamic       bool
}

// SortingColumn describes a column to sort by in a dynamic parquet schema.
type SortingColumn interface {
	parquet.SortingColumn
	ColumnName() string
}

// Ascending constructs a SortingColumn value which dictates to sort by the column in ascending order.
func Ascending(column string) SortingColumn { return ascending(column) }

// Descending constructs a SortingColumn value which dictates to sort by the column in descending order.
func Descending(column string) SortingColumn { return descending(column) }

// NullsFirst wraps the SortingColumn passed as argument so that it instructs
// the row group to place null values first in the column.
func NullsFirst(sortingColumn SortingColumn) SortingColumn { return nullsFirst{sortingColumn} }

type ascending string

func (asc ascending) String() string     { return fmt.Sprintf("ascending(%s)", string(asc)) }
func (asc ascending) ColumnName() string { return string(asc) }
func (asc ascending) Path() []string     { return []string{string(asc)} }
func (asc ascending) Descending() bool   { return false }
func (asc ascending) NullsFirst() bool   { return false }

type descending string

func (desc descending) String() string     { return fmt.Sprintf("descending(%s)", string(desc)) }
func (desc descending) ColumnName() string { return string(desc) }
func (desc descending) Path() []string     { return []string{string(desc)} }
func (desc descending) Descending() bool   { return true }
func (desc descending) NullsFirst() bool   { return false }

type nullsFirst struct{ SortingColumn }

func (nf nullsFirst) String() string   { return fmt.Sprintf("nulls_first+%s", nf.SortingColumn) }
func (nf nullsFirst) NullsFirst() bool { return true }

func makeDynamicSortingColumn(dynamicColumnName string, sortingColumn SortingColumn) SortingColumn {
	return dynamicSortingColumn{
		dynamicColumnName: dynamicColumnName,
		SortingColumn:     sortingColumn,
	}
}

// dynamicSortingColumn is a SortingColumn which is a dynamic column.
type dynamicSortingColumn struct {
	SortingColumn
	dynamicColumnName string
}

func (dyn dynamicSortingColumn) String() string {
	return fmt.Sprintf("dynamic(%s, %v)", dyn.dynamicColumnName, dyn.SortingColumn)
}
func (dyn dynamicSortingColumn) ColumnName() string {
	return dyn.SortingColumn.ColumnName() + "." + dyn.dynamicColumnName
}
func (dyn dynamicSortingColumn) Path() []string { return []string{dyn.ColumnName()} }

// Schema is a dynamic parquet schema. It extends a parquet schema with the
// ability that any column definition that is dynamic will have columns
// dynamically created as their column name is seen for the first time.
type Schema struct {
	name           string
	columns        []ColumnDefinition
	columnIndexes  map[string]int
	sortingColumns []SortingColumn
	dynamicColumns []int
}

// NewSchema creates a new dynamic parquet schema with the given name, column
// definitions and sorting columns. The order of the sorting columns is
// important as it determines the order in which data is written to a file or
// laid out in memory.
func NewSchema(
	name string,
	columns []ColumnDefinition,
	sortingColumns []SortingColumn,
) *Schema {
	sort.Slice(columns, func(i, j int) bool {
		return columns[i].Name < columns[j].Name
	})

	columnIndexes := make(map[string]int, len(columns))
	for i, col := range columns {
		columnIndexes[col.Name] = i
	}

	s := &Schema{
		name:           name,
		columns:        columns,
		sortingColumns: sortingColumns,
		columnIndexes:  columnIndexes,
	}

	for i, col := range columns {
		if col.Dynamic {
			s.dynamicColumns = append(s.dynamicColumns, i)
		}
	}

	return s
}

// DynamicColumns is a set of concrete column names for a dynamic column.
type DynamicColumns struct {
	Column string
	Names  []string
}

// parquetSchema returns the parquet schema for the dynamic schema with the
// concrete dynamic column names given in the argument.
func (s Schema) parquetSchema(
	dynamicColumns map[string]DynamicColumns,
) (
	*parquet.Schema,
	error,
) {
	if len(dynamicColumns) != len(s.dynamicColumns) {
		return nil, fmt.Errorf("expected %d dynamic column names, got %d", len(s.dynamicColumns), len(dynamicColumns))
	}

	g := parquet.Group{}
	for _, col := range s.columns {
		if col.Dynamic {
			dyn := dynamicColumnsFor(col.Name, dynamicColumns)
			for _, name := range dyn.Names {
				g[col.Name+"."+name] = col.StorageLayout
			}
			continue
		}
		g[col.Name] = col.StorageLayout
	}

	return parquet.NewSchema(s.name, g), nil
}

// parquetSchema returns the parquet sorting columns for the dynamic sorting
// columns with the concrete dynamic column names given in the argument.
func (s Schema) parquetSortingColumns(
	dynamicColumns map[string]DynamicColumns,
) []parquet.SortingColumn {
	cols := make([]parquet.SortingColumn, 0, len(s.sortingColumns))
	for _, col := range s.sortingColumns {
		colName := col.ColumnName()
		if !s.columns[s.columnIndexes[colName]].Dynamic {
			cols = append(cols, col)
			continue
		}
		dyn := dynamicColumnsFor(colName, dynamicColumns)
		for _, name := range dyn.Names {
			cols = append(cols, makeDynamicSortingColumn(name, col))
		}
	}
	return cols
}

// dynamicColumnsFor returns the concrete dynamic column names for the given dynamic column name.
func dynamicColumnsFor(column string, dynamicColumns map[string]DynamicColumns) DynamicColumns {
	return dynamicColumns[column]
}

// Buffer represents an batch of rows with a concrete set of dynamic column
// names representing how its parquet schema was created off of a dynamic
// parquet schema.
type Buffer struct {
	buffer         *parquet.Buffer
	dynamicColumns map[string]DynamicColumns
}

// DynamicRowGroup is a parquet.RowGroup that can describe the concrete dynamic
// columns.
type DynamicRowGroup interface {
	parquet.RowGroup
	// DynamicColumns returns the concrete dynamic column names that were used
	// create its concrete parquet schema with a dynamic parquet schema.
	DynamicColumns() map[string]DynamicColumns
	// DynamicRows return an iterator over the rows in the row group.
	DynamicRows() DynamicRows
}

// DynamicRows is an iterator over the rows in a DynamicRowGroup.
type DynamicRows interface {
	ReadRow(*DynamicRow) (*DynamicRow, error)
}

type dynamicRowGroupReader struct {
	rg             DynamicRowGroup
	schema         *parquet.Schema
	dynamicColumns map[string]DynamicColumns
	rows           parquet.Rows
}

func newDynamicRowGroupReader(rg DynamicRowGroup) *dynamicRowGroupReader {
	return &dynamicRowGroupReader{
		rg:             rg,
		schema:         rg.Schema(),
		dynamicColumns: rg.DynamicColumns(),
		rows:           rg.Rows(),
	}
}

// Implements the DynamicRows interface.
func (r *dynamicRowGroupReader) ReadRow(row *DynamicRow) (*DynamicRow, error) {
	if row == nil {
		row = &DynamicRow{
			DynamicColumns: r.dynamicColumns,
			Schema:         r.schema,
			Row:            make(parquet.Row, 0, 50), // randomly chosen number
		}
	}

	row.Row = row.Row[:0]

	var err error
	row.Row, err = r.rows.ReadRow(row.Row)
	return row, err
}

// Column returns the parquet.ColumnChunk for the given index. It contains all
// the pages associated with this row group's column. Implements the
// parquet.RowGroup interface.
func (b *Buffer) Column(i int) parquet.ColumnChunk {
	return b.buffer.Column(i)
}

// NumRows returns the number of rows in the buffer. Implements the
// parquet.RowGroup interface.
func (b *Buffer) NumRows() int64 {
	return b.buffer.NumRows()
}

func (b *Buffer) Sort() {
	sort.Stable(b.buffer)
}

// NumColumns returns the number of columns in the buffer. Implements the
// parquet.RowGroup interface.
func (b *Buffer) NumColumns() int {
	return b.buffer.NumColumns()
}

// Schema returns the concrete parquet.Schema of the buffer. Implements the
// parquet.RowGroup interface.
func (b *Buffer) Schema() *parquet.Schema {
	return b.buffer.Schema()
}

// SortingColumns returns the concrete slice of parquet.SortingColumns of the
// buffer. Implements the parquet.RowGroup interface.
func (b *Buffer) SortingColumns() []parquet.SortingColumn {
	return b.buffer.SortingColumns()
}

// DynamicColumns returns the concrete dynamic column names of the buffer. It
// implements the DynamicRowGroup interface.
func (b *Buffer) DynamicColumns() map[string]DynamicColumns {
	return b.dynamicColumns
}

// WriteRow writes a single row to the buffer.
func (b *Buffer) WriteRow(row parquet.Row) error {
	return b.buffer.WriteRow(row)
}

// WriteRowGroup writes a single row to the buffer.
func (b *Buffer) WriteRowGroup(rg parquet.RowGroup) (int64, error) {
	return b.buffer.WriteRowGroup(rg)
}

// Rows returns an iterator for the rows in the buffer. It implements the
// parquet.RowGroup interface.
func (b *Buffer) Rows() parquet.Rows {
	return b.buffer.Rows()
}

// DynamicRows returns an iterator for the rows in the buffer. It implements the
// DynamicRowGroup interface.
func (b *Buffer) DynamicRows() DynamicRows {
	return newDynamicRowGroupReader(b)
}

// NewBuffer returns a new buffer with a concrete parquet schema generated
// using the given concrete dynamic column names.
func (s *Schema) NewBuffer(dynamicColumns map[string]DynamicColumns) (*Buffer, error) {
	ps, err := s.parquetSchema(dynamicColumns)
	if err != nil {
		return nil, err
	}

	cols := s.parquetSortingColumns(dynamicColumns)
	return &Buffer{
		dynamicColumns: dynamicColumns,
		buffer: parquet.NewBuffer(
			ps,
			parquet.SortingColumns(cols...),
		),
	}, nil
}

// MergedRowGroup allows wrapping any parquet.RowGroup to implement the
// DynamicRowGroup interface by specifying the concrete dynamic column names
// the RowGroup's schema contains.
type MergedRowGroup struct {
	parquet.RowGroup
	DynCols map[string]DynamicColumns
}

// DynamicColumns returns the concrete dynamic column names that were used
// create its concrete parquet schema with a dynamic parquet schema. Implements
// the DynamicRowGroup interface.
func (r *MergedRowGroup) DynamicColumns() map[string]DynamicColumns {
	return r.DynCols
}

// DynamicRows returns an iterator over the rows in the row group. Implements
// the DynamicRowGroup interface.
func (r *MergedRowGroup) DynamicRows() DynamicRows {
	return newDynamicRowGroupReader(r)
}

// MergeDynamicRowGroups merges the given dynamic row groups into a single
// dynamic row group. It merges the parquet schema in a non-conflicting way by
// merging all the concrete dynamic column names and generating a superset
// parquet schema that all given dynamic row groups are compatible with.
func (s *Schema) MergeDynamicRowGroups(rowGroups []DynamicRowGroup) (DynamicRowGroup, error) {
	if len(rowGroups) == 1 {
		return rowGroups[0], nil
	}

	dynamicColumns := mergeDynamicRowGroupDynamicColumns(rowGroups)
	ps, err := s.parquetSchema(dynamicColumns)
	if err != nil {
		return nil, err
	}

	cols := s.parquetSortingColumns(dynamicColumns)

	adapters := make([]parquet.RowGroup, 0, len(rowGroups))
	for _, rowGroup := range rowGroups {
		adapters = append(adapters, newDynamicRowGroupMergeAdapter(
			ps,
			cols,
			dynamicColumns,
			rowGroup,
		))
	}

	merge, err := parquet.MergeRowGroups(adapters, parquet.SortingColumns(cols...))
	if err != nil {
		return nil, err
	}

	return &MergedRowGroup{
		RowGroup: merge,
		DynCols:  dynamicColumns,
	}, nil
}

// mergeDynamicRowGroupDynamicColumns merges the concrete dynamic column names
// of multiple DynamicRowGroups into a single, merged, superset of dynamic
// column names.
func mergeDynamicRowGroupDynamicColumns(rowGroups []DynamicRowGroup) map[string]DynamicColumns {
	sets := []map[string]DynamicColumns{}
	for _, batch := range rowGroups {
		sets = append(sets, batch.DynamicColumns())
	}

	return mergeDynamicColumnSets(sets)
}

func mergeDynamicColumnSets(sets []map[string]DynamicColumns) map[string]DynamicColumns {
	dynamicColumns := map[string][]DynamicColumns{}
	for _, set := range sets {
		for k, v := range set {
			_, seen := dynamicColumns[k]
			if !seen {
				dynamicColumns[k] = []DynamicColumns{}
			}
			dynamicColumns[k] = append(dynamicColumns[k], v)
		}
	}

	resultDynamicColumns := map[string]DynamicColumns{}
	for name, dynCols := range dynamicColumns {
		resultDynamicColumns[name] = mergeDynamicColumns(dynCols)
	}

	return resultDynamicColumns
}

// mergeDynamicColumns merges the given concrete dynamic column names into a
// single superset. It assumes that the given DynamicColumns are all for the
// same dynamic column name.
func mergeDynamicColumns(dyn []DynamicColumns) DynamicColumns {
	names := [][]string{}
	for _, d := range dyn {
		names = append(names, d.Names)
	}
	return DynamicColumns{
		Column: dyn[0].Column,
		Names:  mergeStrings(names),
	}
}

// mergeStrings merges the given sorted string slices into a single sorted and
// deduplicated slice of strings.
func mergeStrings(str [][]string) []string {
	result := []string{}
	seen := map[string]bool{}
	for _, s := range str {
		for _, n := range s {
			if !seen[n] {
				result = append(result, n)
				seen[n] = true
			}
		}
	}
	sort.Strings(result)
	return result
}

// newDynamicRowGroupMergeAdapter returns a *dynamicRowGroupMergeAdapter, which
// maps the columns of the original row group to the columns in the super-set
// schema provided. This allows row groups that have non-conflicting dynamic
// schemas to be merged into a single row group with a superset parquet schema.
// The provided schema must not conflict with the original row group's schema
// it must be strictly a superset, this property is not checked, it is assumed
// to be true for performance reasons.
func newDynamicRowGroupMergeAdapter(
	schema *parquet.Schema,
	sortingColumns []parquet.SortingColumn,
	mergedDynamicColumns map[string]DynamicColumns,
	originalRowGroup DynamicRowGroup,
) *dynamicRowGroupMergeAdapter {
	return &dynamicRowGroupMergeAdapter{
		schema:               schema,
		sortingColumns:       sortingColumns,
		mergedDynamicColumns: mergedDynamicColumns,
		originalRowGroup:     originalRowGroup,
		indexMapping: mapMergedColumnNameIndexes(
			schema.ChildNames(),
			originalRowGroup.Schema().ChildNames(),
		),
	}
}

// mapMergedColumnNameIndexes maps the column indexes of the original row group
// to the indexes of the merged schema.
func mapMergedColumnNameIndexes(merged, original []string) []int {
	origColsLen := len(original)
	indexMapping := make([]int, len(merged))
	j := 0
	for i, col := range merged {
		if j < origColsLen && original[j] == col {
			indexMapping[i] = j
			j++
			continue
		}
		indexMapping[i] = -1
	}
	return indexMapping
}

// dynamicRowGroupMergeAdapter maps a RowBatch with a Schema with a subset of dynamic
// columns to a Schema with a superset of dynamic columns. It implements the
// parquet.RowGroup interface.
type dynamicRowGroupMergeAdapter struct {
	schema               *parquet.Schema
	sortingColumns       []parquet.SortingColumn
	mergedDynamicColumns map[string]DynamicColumns
	originalRowGroup     DynamicRowGroup
	indexMapping         []int
}

// Returns the number of rows in the group.
func (a *dynamicRowGroupMergeAdapter) NumRows() int64 {
	return a.originalRowGroup.NumRows()
}

// Returns the number of leaf columns in the group.
func (a *dynamicRowGroupMergeAdapter) NumColumns() int {
	// This only works because we currently only support flat schemas.
	return a.schema.NumChildren()
}

// Returns the leaf column at the given index in the group. Searches for the
// same column in the original batch. If not found returns a column chunk
// filled with nulls.
func (a *dynamicRowGroupMergeAdapter) Column(i int) parquet.ColumnChunk {
	colName := a.schema.ChildNames()[i]
	colIndex := a.indexMapping[i]
	if colIndex == -1 {
		return NewNilColumnChunk(a.schema.ChildByName(colName).Type(), i, int(a.NumRows()))
	}

	return &remappedColumnChunk{
		ColumnChunk:   a.originalRowGroup.Column(colIndex),
		remappedIndex: i,
	}
}

// Returns the schema of rows in the group. The schema is the configured
// merged, superset schema.
func (a *dynamicRowGroupMergeAdapter) Schema() *parquet.Schema {
	return a.schema
}

// Returns the list of sorting columns describing how rows are sorted in the
// group.
//
// The method will return an empty slice if the rows are not sorted.
func (a *dynamicRowGroupMergeAdapter) SortingColumns() []parquet.SortingColumn {
	return a.sortingColumns
}

// Returns a reader exposing the rows of the row group.
func (a *dynamicRowGroupMergeAdapter) Rows() parquet.Rows {
	return parquet.NewRowGroupRowReader(a)
}

// remappedColumnChunk is a ColumnChunk that wraps a ColumnChunk and makes it
// appear to the called as if its index in the schema was always the index it
// was remapped to. Implements the parquet.ColumnChunk interface.
type remappedColumnChunk struct {
	parquet.ColumnChunk
	remappedIndex int
}

// Column returns the column chunk's index in the schema. It returns the
// configured remapped index. Implements the parquet.ColumnChunk interface.
func (c *remappedColumnChunk) Column() int {
	return c.remappedIndex
}

// Pages returns the column chunk's pages ensuring that all pages read will be
// remapped to the configured remapped index. Implements the
// parquet.ColumnChunk interface.
func (a *remappedColumnChunk) Pages() parquet.Pages {
	return &remappedPages{
		Pages:         a.ColumnChunk.Pages(),
		remappedIndex: a.remappedIndex,
	}
}

// remappedPages is an iterator of the column chunk's pages. It ensures that
// all pages returned will appear to belong to the configured remapped column
// index. Implements the parquet.Pages interface.
type remappedPages struct {
	parquet.Pages
	remappedIndex int
}

// ReadPage reads the next page from the page iterator. It ensures that any
// page read from the underlying iterator will appear to belong to the
// configured remapped column index. Implements the parquet.Pages interface.
func (p *remappedPages) ReadPage() (parquet.Page, error) {
	page, err := p.Pages.ReadPage()
	if err != nil {
		return nil, err
	}

	return &remappedPage{
		Page:          page,
		remappedIndex: p.remappedIndex,
	}, nil
}

// remappedPage is a Page that wraps a Page and makes it appear as if its
// column index is the remapped index. Implements the parquet.Page interface.
type remappedPage struct {
	parquet.Page
	remappedIndex int
}

// Column returns the page's column index in the schema. It returns the
// configured remapped index. Implements the parquet.Page interface.
func (p *remappedPage) Column() int {
	return p.remappedIndex
}

// Values returns a parquet.ValueReader that ensures that all values read will
// be remapped to have the configured remapped index. Implements the
// parquet.Page interface.
func (p *remappedPage) Values() parquet.ValueReader {
	return &remappedValueReader{
		ValueReader:   p.Page.Values(),
		remappedIndex: p.remappedIndex,
	}
}

// Values returns the page's values. It ensures that all values read will be
// remapped to have the configured remapped index. Implements the
// parquet.ValueReader interface.
type remappedValueReader struct {
	parquet.ValueReader
	remappedIndex int
}

// ReadValues reads the next batch of values from the value reader. It ensures
// that any value read will be remapped to have the configured remapped index.
// Implements the parquet.ValueReader interface.
func (r *remappedValueReader) ReadValues(v []parquet.Value) (int, error) {
	n, err := r.ValueReader.ReadValues(v)
	for i := 0; i < len(v[:n]); i++ {
		v[i] = v[i].Level(v[i].RepetitionLevel(), v[i].DefinitionLevel(), r.remappedIndex)
	}

	return n, err
}
