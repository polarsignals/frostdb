package arrowutils

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"golang.org/x/exp/constraints"

	"github.com/polarsignals/frostdb/pqarrow/builder"
)

// SortRecord sorts the given record's rows by the given column. Currently only supports int64, string and binary columns.
// TODO: We might consider supporting other types in the future, especially dynamic columns and sorting by multiple columns.
func SortRecord(mem memory.Allocator, r arrow.Record, col int) ([]int, error) {
	if r.NumRows() == 0 {
		return nil, nil
	}
	if r.NumRows() == 1 {
		return []int{0}, nil
	}

	indices := make([]int, r.NumRows())
	// populate indices
	for i := range indices {
		indices[i] = i
	}

	switch c := r.Column(col).(type) {
	case *array.Int64:
		sort.Sort(orderedSorter[int64]{array: c, indices: indices})
	case *array.String:
		sort.Sort(orderedSorter[string]{array: c, indices: indices})
	case *array.Binary:
		sort.Sort(binarySort{array: c, indices: indices})
	default:
		return nil, fmt.Errorf("unsupported column type for sorting %T", c)
	}

	return indices, nil
}

// ReorderRecord reorders the given record's rows by the given indices.
func ReorderRecord(mem memory.Allocator, r arrow.Record, indices []int) (arrow.Record, error) {
	// if the indices are already sorted, we can return the original record to save memory allocations
	if sort.SliceIsSorted(indices, func(i, j int) bool { return indices[i] < indices[j] }) {
		return r, nil
	}

	recordBuilder := builder.NewRecordBuilder(mem, r.Schema())
	recordBuilder.Reserve(int(r.NumRows()))
	for i := 0; i < int(r.NumRows()); i++ {
		for colIdx, b := range recordBuilder.Fields() {
			// here we read the value from the original record,
			// but we the correct index and then write it to the new record
			if err := builder.AppendValue(b, r.Column(colIdx), indices[i]); err != nil {
				return nil, err
			}
		}
	}

	return recordBuilder.NewRecord(), nil
}

type orderedArray[T constraints.Ordered] interface {
	Value(int) T
	Len() int
}

type orderedSorter[T constraints.Ordered] struct {
	array   orderedArray[T]
	indices []int
}

func (s orderedSorter[T]) Len() int {
	return s.array.Len()
}

func (s orderedSorter[T]) Less(i, j int) bool {
	return s.array.Value(s.indices[i]) < s.array.Value(s.indices[j])
}

func (s orderedSorter[T]) Swap(i, j int) {
	s.indices[i], s.indices[j] = s.indices[j], s.indices[i]
}

type binarySort struct {
	array   *array.Binary
	indices []int
}

func (s binarySort) Len() int {
	return s.array.Len()
}

func (s binarySort) Less(i, j int) bool {
	// we need to read the indices from the indices slice, as they might have already been swapped.
	return bytes.Compare(s.array.Value(s.indices[i]), s.array.Value(s.indices[j])) == -1
}

func (s binarySort) Swap(i, j int) {
	s.indices[i], s.indices[j] = s.indices[j], s.indices[i]
}
