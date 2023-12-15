package arrowutils

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/compute"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"golang.org/x/exp/constraints"
)

// SortRecord sorts the given record's rows by the given column. Currently only supports int64, string and binary columns.
func SortRecord(mem memory.Allocator, r arrow.Record, cols []int) (*array.Int64, error) {
	if len(cols) > 1 {
		return nil, fmt.Errorf("sorting by multiple columns isn't implemented yet")
	}
	indicesBuilder := array.NewInt64Builder(mem)

	if r.NumRows() == 0 {
		return indicesBuilder.NewInt64Array(), nil
	}
	if r.NumRows() == 1 {
		indicesBuilder.Append(0)
		return indicesBuilder.NewInt64Array(), nil
	}

	indices := make([]int64, r.NumRows())
	// populate indices
	for i := range indices {
		indices[i] = int64(i)
	}

	switch c := r.Column(cols[0]).(type) {
	case *array.Int64:
		sort.Sort(orderedSorter[int64]{array: c, indices: indices})
	case *array.String:
		sort.Sort(orderedSorter[string]{array: c, indices: indices})
	case *array.Binary:
		sort.Sort(binarySort{array: c, indices: indices})
	default:
		return nil, fmt.Errorf("unsupported column type for sorting %T", c)
	}

	indicesBuilder.AppendValues(indices, nil)
	return indicesBuilder.NewInt64Array(), nil
}

// ReorderRecord reorders the given record's rows by the given indices.
// This is a wrapper around compute.Take which handles the type castings.
func ReorderRecord(r arrow.Record, indices arrow.Array) (arrow.Record, error) {
	res, err := compute.Take(
		context.Background(),
		*compute.DefaultTakeOptions(),
		compute.NewDatum(r),
		compute.NewDatum(indices),
	)
	if err != nil {
		return nil, err
	}
	return res.(*compute.RecordDatum).Value, nil
}

type orderedArray[T constraints.Ordered] interface {
	Value(int) T
	IsNull(int) bool
	Len() int
}

type orderedSorter[T constraints.Ordered] struct {
	array   orderedArray[T]
	indices []int64
}

func (s orderedSorter[T]) Len() int {
	return s.array.Len()
}

func (s orderedSorter[T]) Less(i, j int) bool {
	if s.array.IsNull(int(s.indices[i])) {
		return false
	}
	if s.array.IsNull(int(s.indices[j])) {
		return true
	}
	return s.array.Value(int(s.indices[i])) < s.array.Value(int(s.indices[j]))
}

func (s orderedSorter[T]) Swap(i, j int) {
	s.indices[i], s.indices[j] = s.indices[j], s.indices[i]
}

type binarySort struct {
	array   *array.Binary
	indices []int64
}

func (s binarySort) Len() int {
	return s.array.Len()
}

func (s binarySort) Less(i, j int) bool {
	if s.array.IsNull(int(s.indices[i])) && !s.array.IsNull(int(s.indices[j])) {
		return false
	}
	if !s.array.IsNull(int(s.indices[i])) && s.array.IsNull(int(s.indices[j])) {
		return true
	}
	if s.array.IsNull(int(s.indices[i])) && s.array.IsNull(int(s.indices[j])) {
		return false
	}
	// we need to read the indices from the indices slice, as they might have already been swapped.
	return bytes.Compare(s.array.Value(int(s.indices[i])), s.array.Value(int(s.indices[j]))) == -1
}

func (s binarySort) Swap(i, j int) {
	s.indices[i], s.indices[j] = s.indices[j], s.indices[i]
}
