package dynparquet

import (
	"errors"
	"sort"
	"strings"
)

var ErrMalformedDynamicColumns = errors.New("malformed dynamic columns string")

func serializeDynamicColumns(dynamicColumns map[string][]string) string {
	names := make([]string, 0, len(dynamicColumns))
	var size int
	for name, cols := range dynamicColumns {
		names = append(names, name)
		size += len(name) +
			2 // separators
		for i := range cols {
			size += len(cols[i]) + 1
		}
	}
	sort.Strings(names)
	var str strings.Builder
	str.Grow(size)
	for i, name := range names {
		if i != 0 {
			str.WriteByte(';')
		}
		str.WriteString(name)
		str.WriteByte(':')
		for j := range dynamicColumns[name] {
			if j != 0 {
				str.WriteByte(',')
			}
			str.WriteString(dynamicColumns[name][j])
		}
	}
	return str.String()
}

func deserializeDynamicColumns(columns string) (map[string][]string, error) {
	dynCols := map[string][]string{}

	// handle case where the schema has no dynamic columnns
	if len(columns) == 0 {
		return dynCols, nil
	}
	var column string
	for {
		if columns == "" {
			return dynCols, nil
		}
		column, columns, _ = strings.Cut(columns, ";")
		name, labels, ok := strings.Cut(column, ":")
		if !ok {
			return nil, ErrMalformedDynamicColumns
		}
		values := make([]string, 0, strings.Count(labels, ","))

		var label string
		for {
			if labels == "" {
				break
			}
			label, labels, _ = strings.Cut(labels, ",")
			values = append(values, label)
		}
		dynCols[name] = values
	}
}
