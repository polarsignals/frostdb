package arrowutils

import (
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
)

// RecordSize returns the arrow record size in bytes.
func RecordSize(r arrow.Record) int64 {
	size := int64(0)
	for _, col := range r.Columns() {
		bufs := col.Data().Buffers()
		for _, buf := range bufs {
			if buf != nil {
				size += int64(buf.Len())
			}
		}
	}
	return size
}

func ForEachValueInList(index int, arr *array.List, iterator func(int, any)) error {
	start, end := arr.ValueOffsets(index)
	list := array.NewSlice(arr.ListValues(), start, end)
	defer list.Release()
	for i := 0; i < list.Len(); i++ {
		iterator(i, list.GetOneForMarshal(i))
	}
	return nil
}
