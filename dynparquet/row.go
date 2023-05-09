package dynparquet

import (
	"fmt"

	"github.com/segmentio/parquet-go"
)

type DynamicRows struct {
	Rows           []parquet.Row
	Schema         *parquet.Schema
	DynamicColumns map[string][]string
	fields         []parquet.Field
}

func NewDynamicRows(
	rows []parquet.Row, schema *parquet.Schema, dynamicColumns map[string][]string, fields []parquet.Field,
) *DynamicRows {
	return &DynamicRows{
		Schema:         schema,
		DynamicColumns: dynamicColumns,
		Rows:           rows,
		fields:         fields,
	}
}

func (r *DynamicRows) Get(i int) *DynamicRow {
	return &DynamicRow{
		Schema:         r.Schema,
		DynamicColumns: r.DynamicColumns,
		Row:            r.Rows[i],
		fields:         r.fields,
	}
}

func (r *DynamicRows) GetCopy(i int) *DynamicRow {
	rowCopy := make(parquet.Row, len(r.Rows[i]))
	for i, v := range r.Rows[i] {
		rowCopy[i] = v.Clone()
	}

	return &DynamicRow{
		Schema:         r.Schema,
		DynamicColumns: r.DynamicColumns,
		Row:            rowCopy,
		fields:         r.fields,
	}
}

func (r *DynamicRows) IsSorted(schema *Schema) bool {
	for i := 1; i < len(r.Rows); i++ {
		if schema.RowLessThan(r.Get(i), r.Get(i-1)) {
			return false
		}
	}
	return true
}

func NewDynamicRow(row parquet.Row, schema *parquet.Schema, dyncols map[string][]string, fields []parquet.Field) *DynamicRow {
	return &DynamicRow{
		Row:            row,
		Schema:         schema,
		DynamicColumns: dyncols,
		fields:         fields,
	}
}

type DynamicRow struct {
	Row            parquet.Row
	Schema         *parquet.Schema
	DynamicColumns map[string][]string
	fields         []parquet.Field
}

func (s *Schema) RowLessThan(a, b *DynamicRow) bool {
	return s.Cmp(a, b) < 0
}

func (s *Schema) Cmp(a, b *DynamicRow) int {
	dynamicColumns := mergeDynamicColumnSets([]map[string][]string{a.DynamicColumns, b.DynamicColumns})
	cols := s.ParquetSortingColumns(dynamicColumns)
	pooledSchema, err := s.GetParquetSortingSchema(dynamicColumns)
	if err != nil {
		panic(fmt.Sprintf("unexpected schema state: %v", err))
	}
	sortingSchema := pooledSchema.Schema
	defer s.PutPooledParquetSchema(pooledSchema)

	// Iterate over all the schema columns to prepare the rows for comparison.
	// The main reason we can't directly pass in {a,b}.Row is that they might
	// not have explicit values for dynamic columns we want to compare. These
	// columns need to be populated with a NULL value.
	// Additionally, parquet imposes its column indexes when creating the
	// sorting schema, so these need to be respected.
	schemaCols := sortingSchema.Columns()
	rowA := make(parquet.Row, 0, len(schemaCols))
	rowB := make(parquet.Row, 0, len(schemaCols))
	for _, path := range schemaCols {
		name := path[0] // Currently we only support flat schemas.

		aIndex := FindChildIndex(a.fields, name)
		bIndex := FindChildIndex(b.fields, name)

		if aIndex == -1 && bIndex == -1 {
			continue
		}

		av, bv := extractValues(a, b, aIndex, bIndex)
		rowA = append(rowA, av...)
		rowB = append(rowB, bv...)
	}

	// Set the column indexes according to the merged schema.
	for i := range rowA {
		rowA[i] = rowA[i].Level(rowA[i].RepetitionLevel(), rowA[i].DefinitionLevel(), i)
		rowB[i] = rowB[i].Level(rowB[i].RepetitionLevel(), rowB[i].DefinitionLevel(), i)
	}

	return sortingSchema.Comparator(cols...)(rowA, rowB)
}

type DynamicRowSorter struct {
	schema *Schema
	rows   *DynamicRows
}

func NewDynamicRowSorter(schema *Schema, rows *DynamicRows) *DynamicRowSorter {
	return &DynamicRowSorter{
		schema: schema,
		rows:   rows,
	}
}

func (d *DynamicRowSorter) Len() int {
	return len(d.rows.Rows)
}

func (d *DynamicRowSorter) Less(i, j int) bool {
	return d.schema.RowLessThan(d.rows.Get(i), d.rows.Get(j))
}

func (d *DynamicRowSorter) Swap(i, j int) {
	d.rows.Rows[i], d.rows.Rows[j] = d.rows.Rows[j], d.rows.Rows[i]
}

func extractValues(a, b *DynamicRow, aIndex, bIndex int) ([]parquet.Value, []parquet.Value) {
	if aIndex != -1 && bIndex == -1 {
		return ValuesForIndex(a.Row, aIndex), []parquet.Value{parquet.ValueOf(nil).Level(0, 0, aIndex)}
	}

	if aIndex == -1 && bIndex != -1 {
		return []parquet.Value{parquet.ValueOf(nil).Level(0, 0, bIndex)}, ValuesForIndex(b.Row, bIndex)
	}

	return ValuesForIndex(a.Row, aIndex), ValuesForIndex(b.Row, bIndex)
}

func FindChildIndex(fields []parquet.Field, name string) int {
	for i, field := range fields {
		if field.Name() == name {
			return i
		}
	}
	return -1
}

func ValuesForIndex(row parquet.Row, index int) []parquet.Value {
	start := -1
	end := -1
	for i, v := range row {
		idx := v.Column()
		if end != -1 && end == idx-1 {
			return row[start:end]
		}
		if idx == index {
			if start == -1 {
				start = i
			}
			end = i + 1
		}
	}

	return row[start:end]
}
