package arrowutils

import (
	"github.com/apache/arrow/go/v10/arrow"
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
