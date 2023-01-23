package arrowutils

import (
	"fmt"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/memory"
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
	case *array.Dictionary:
		switch dict := a.Dictionary().(type) {
		case *array.Binary:
			return dict.Value(a.GetValueIndex(i)), nil
		default:
			return nil, fmt.Errorf("unsupported dictionary type for GetValue %T", dict)
		}
	default:
		return nil, fmt.Errorf("unsupported type for GetValue %T", a)
	}
}

// ArrayConcatenator is an object that helps callers keep track of a slice of
// arrays and concatenate them into a single one when needed. This is more
// efficient and memory safe than using a builder.
type ArrayConcatenator struct {
	arrs []arrow.Array
}

func (c *ArrayConcatenator) Add(arr arrow.Array) {
	c.arrs = append(c.arrs, arr)
}

func (c *ArrayConcatenator) NewArray(mem memory.Allocator) (arrow.Array, error) {
	arr, err := array.Concatenate(c.arrs, mem)
	if err != nil {
		return nil, err
	}
	c.arrs = c.arrs[:0]
	return arr, err
}

func (c *ArrayConcatenator) Len() int {
	return len(c.arrs)
}
