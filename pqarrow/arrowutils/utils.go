package arrowutils

import (
	"fmt"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
)

// GetValue returns the value at index i in arr. If the value is null, nil is
// returned.
func GetValue(arr arrow.Array, i int) (any, error) {
	if arr.IsNull(i) {
		return nil, nil
	}

	switch a := arr.(type) {
	case *array.Binary:
		return a.Value(i), nil
	case *array.FixedSizeBinary:
		return a.Value(i), nil
	case *array.String:
		return a.Value(i), nil
	case *array.Int64:
		return a.Value(i), nil
	case *array.Boolean:
		return a.Value(i), nil
	default:
		return nil, fmt.Errorf("unsupported type for GetValue %T", a)
	}
}
