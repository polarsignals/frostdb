package dynparquet

import (
	"errors"
	"sort"
	"strings"
)

var (
	ErrMalformedDynamicColumns = errors.New("malformed dynamic columns string")
)

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
		i++
	}

	return str
}

func deserializeDynamicColumns(dynColString string) (map[string][]string, error) {
	dynCols := map[string][]string{}

	for _, dynString := range strings.Split(dynColString, ";") {
		split := strings.Split(dynString, ":")
		if len(split) != 2 {
			return nil, ErrMalformedDynamicColumns
		}
		dynCols[split[0]] = strings.Split(split[1], ",")
	}

	return dynCols, nil
}
