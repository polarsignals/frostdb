package dynparquet

import (
	"github.com/segmentio/parquet-go"
)

type DynamicRow struct {
	Row            parquet.Row
	Schema         *parquet.Schema
	DynamicColumns map[string]DynamicColumns
}

func (s *Schema) RowLessThan(a, b *DynamicRow) bool {
	dynamicColumns := mergeDynamicColumnSets([]map[string]DynamicColumns{a.DynamicColumns, b.DynamicColumns})
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

		definitionLevel := int8(0)
		if node.Optional() {
			definitionLevel = 1
		}

		repetitionLevel := int8(0)
		if node.Repeated() {
			definitionLevel = 1
			repetitionLevel = 1
		}

		cmp := parquet.SortFuncOf(
			node.Type(),
			repetitionLevel,
			definitionLevel,
			col.Descending(),
			col.NullsFirst(),
		)(av, bv)
		if cmp < 0 {
			return true
		}
		if cmp > 0 {
			return false
		}
		// neither of those case are true so a and b are equal for this column
		// and we need to continue with the next column.
	}

	return false
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
