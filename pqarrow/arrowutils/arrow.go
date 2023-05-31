package arrowutils

import (
	"fmt"

	"github.com/apache/arrow/go/v12/arrow/array"
)

func ForEachValueInList(index int, arr *array.List, iterator func(int, any)) error {
	start, end := arr.ValueOffsets(index)
	list := array.NewSlice(arr.ListValues(), start, end)
	defer list.Release()
	switch l := list.(type) {
	case *array.Dictionary:
		switch dict := l.Dictionary().(type) {
		case *array.Binary:
			for i := 0; i < l.Len(); i++ {
				iterator(i, dict.Value(l.GetValueIndex(i)))
			}
		default:
			return fmt.Errorf("list dictionary not of expected type: %T", list)
		}
	default:
		return fmt.Errorf("list not of expected type: %T", list)
	}

	return nil
}
