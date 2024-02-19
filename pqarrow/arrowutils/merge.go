package arrowutils

import (
	"bytes"
	"container/heap"
	"fmt"
	"math"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/memory"

	"github.com/polarsignals/frostdb/pqarrow/builder"
)

// MergeRecords merges the given records. The records must all have the same
// schema. orderByCols is a slice of indexes into the columns that the records
// and resulting records are ordered by. While ordering the limit is checked before appending more rows.
// If limit is  0, no limit is applied.
// Note that the given records should already be ordered by the given columns.
// WARNING: Only ascending ordering is currently supported.
func MergeRecords(
	mem memory.Allocator,
	records []arrow.Record,
	orderByCols []int,
	limit uint64,
) (arrow.Record, error) {
	h := cursorHeap{
		cursors:     make([]cursor, len(records)),
		orderByCols: orderByCols,
	}
	for i := range h.cursors {
		h.cursors[i].r = records[i]
	}

	schema := records[0].Schema()
	recordBuilder := builder.NewRecordBuilder(mem, schema)
	defer recordBuilder.Release()

	if limit == 0 {
		limit = math.MaxInt64
	}
	count := uint64(0)

	heap.Init(&h)
	for h.Len() > 0 && count < limit {
		// Minimum cursor is always at index 0.
		r := h.cursors[0].r
		i := h.cursors[0].curIdx
		for colIdx, b := range recordBuilder.Fields() {
			if err := builder.AppendValue(b, r.Column(colIdx), i); err != nil {
				return nil, err
			}
		}
		if int64(i+1) >= r.NumRows() {
			// Pop the cursor since it has no more data.
			_ = heap.Pop(&h)
			count++
			continue
		}
		h.cursors[0].curIdx++
		heap.Fix(&h, 0)
		count++
	}

	return recordBuilder.NewRecord(), nil
}

type cursor struct {
	r      arrow.Record
	curIdx int
}

type cursorHeap struct {
	cursors     []cursor
	orderByCols []int
}

func (h cursorHeap) Len() int {
	return len(h.cursors)
}

func (h cursorHeap) Less(i, j int) bool {
	c1 := h.cursors[i]
	c2 := h.cursors[j]
	for _, i := range h.orderByCols {
		col1 := c1.r.Column(i)
		col2 := c2.r.Column(i)
		if cmp, ok := nullComparison(col1.IsNull(c1.curIdx), col2.IsNull(c2.curIdx)); ok {
			if cmp == 0 {
				continue
			}
			return cmp < 0
		}
		switch arr1 := c1.r.Column(i).(type) {
		case *array.Binary:
			arr2 := c2.r.Column(i).(*array.Binary)
			cmp := bytes.Compare(arr1.Value(c1.curIdx), arr2.Value(c2.curIdx))
			if cmp == 0 {
				continue
			}
			return cmp < 0
		case *array.Int64:
			arr2 := c2.r.Column(i).(*array.Int64)
			v1 := arr1.Value(c1.curIdx)
			v2 := arr2.Value(c2.curIdx)
			if v1 == v2 {
				continue
			}
			return v1 < v2
		case *array.Int32:
			arr2 := c2.r.Column(i).(*array.Int32)
			v1 := arr1.Value(c1.curIdx)
			v2 := arr2.Value(c2.curIdx)
			if v1 == v2 {
				continue
			}
			return v1 < v2
		case *array.Uint64:
			arr2 := c2.r.Column(i).(*array.Uint64)
			v1 := arr1.Value(c1.curIdx)
			v2 := arr2.Value(c2.curIdx)
			if v1 == v2 {
				continue
			}
			return v1 < v2
		case *array.Dictionary:
			switch dict := arr1.Dictionary().(type) {
			case *array.Binary:
				arr2 := c2.r.Column(i).(*array.Dictionary)
				dict2 := arr2.Dictionary().(*array.Binary)
				cmp := bytes.Compare(dict.Value(arr1.GetValueIndex(c1.curIdx)), dict2.Value(arr2.GetValueIndex(c2.curIdx)))
				if cmp == 0 {
					continue
				}
				return cmp < 0
			default:
				panic(fmt.Sprintf("unsupported dictionary type for record merging %T", dict))
			}
		default:
			panic(fmt.Sprintf("unsupported type for record merging %T", arr1))
		}
	}
	return false
}

func (h cursorHeap) Swap(i, j int) {
	h.cursors[i], h.cursors[j] = h.cursors[j], h.cursors[i]
}

func (h cursorHeap) Push(_ any) {
	panic(
		"number of cursors are known at Init time, none should ever be pushed",
	)
}

func (h *cursorHeap) Pop() any {
	n := len(h.cursors) - 1
	c := h.cursors[n]
	h.cursors = h.cursors[:n]
	return c
}
