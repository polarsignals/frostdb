package dynparquet

import (
	"errors"
	"sort"
	"strings"
)

var ErrMalformedDynamicColumns = errors.New("malformed dynamic columns string")

func serializeDynamicColumns(dynamicColumns map[string][]string) string {
	names := make([]string, 0, len(dynamicColumns))
	for name := range dynamicColumns {
		names = append(names, name)
	}
	sort.Strings(names)

	str := ""
	for i, name := range names {
		if i != 0 {
			str += ";"
		}
		str += name + ":" + strings.Join(dynamicColumns[name], ",")
	}

	return str
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
