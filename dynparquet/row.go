package dynparquet

import (
	"github.com/segmentio/parquet-go"
)

type DynamicRow struct {
	Row            parquet.Row
	Schema         *parquet.Schema
	DynamicColumns map[string][]string
}

func (s *Schema) Compare(a, b *DynamicRow) int {
	dynamicColumns := mergeDynamicColumnSets([]map[string][]string{a.DynamicColumns, b.DynamicColumns})
	cols := s.parquetSortingColumns(dynamicColumns)
	for _, col := range cols {
		name := col.Path()[0] // Currently we only support flat schemas.

		aIndex := findChildIndex(a.Schema, name)
		bIndex := findChildIndex(b.Schema, name)

		if aIndex == -1 && bIndex == -1 {
			continue
		}

		var node parquet.Node
		if aIndex != -1 {
			node = a.Schema.ChildByName(name)
		} else {
			node = b.Schema.ChildByName(name)
		}

		av, bv := extractValues(a, b, aIndex, bIndex)
		cmp := compare(col, node, av, bv)
		if cmp != 0 {
			return cmp
		}
		// neither of those case are true so a and b are equal for this column
		// and we need to continue with the next column.
	}

	return 0
}

func (s *Schema) RowGreaterThan(a, b *DynamicRow) bool {
	return s.Compare(a, b) > 0
}

func (s *Schema) RowLessThan(a, b *DynamicRow) bool {
	return s.Compare(a, b) < 0
}

func (s *Schema) RowLessThanOrEqualTo(a, b *DynamicRow) bool {
	return s.Compare(a, b) <= 0
}

func compare(col parquet.SortingColumn, node parquet.Node, av, bv []parquet.Value) int {
	sortOptions := []parquet.SortOption{
		parquet.SortDescending(col.Descending()),
		parquet.SortNullsFirst(col.NullsFirst()),
	}
	if node.Optional() || node.Repeated() {
		sortOptions = append(sortOptions, parquet.SortMaxDefinitionLevel(1))
	}

	if node.Repeated() {
		sortOptions = append(sortOptions, parquet.SortMaxRepetitionLevel(1))
	}

	return parquet.SortFuncOf(
		node.Type(),
		sortOptions...,
	)(av, bv)
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

func findChildIndex(schema *parquet.Schema, name string) int {
	for i, child := range schema.ChildNames() {
		if child == name {
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
