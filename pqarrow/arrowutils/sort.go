package arrowutils

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/compute"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"golang.org/x/sync/errgroup"
)

// SortRecord sorts given arrow.Record by columns.
//
// direction defines  the sorting  order of the columns
//
//	-1 ascending order
//	 0 no order
//	 1 descending order
//
// By default sorting is in ascending order.
//
// nullFirst defines how to handle nulls for sorting columns.WHen
// nullFirst[sorting_column_index] == true nulls will take precedent for the
// column.
// By default nullFirst is false for all sorting columns
//
// Both direction and nullsFirst are optional, nil is allowed as value. For non
// nil values they must be the same length as columns where each index maps to
// the column index.
//
// Comparison is made sequentially by each column. When rows are equal in the
// first column we compare the rows om the second column and so on and so forth
// until rows that are not equal are found.
func SortRecord(
	mem memory.Allocator,
	r arrow.Record,
	columns []int,
	direction []int,
	nullsFirst []bool,
) (*array.Int32, error) {
	if len(columns) == 0 {
		return nil, errors.New("pqarrow/arrowutils: at least one column is needed for sorting")
	}
	if direction == nil {
		direction = make([]int, len(columns))
		for i := range direction {
			// By default we sort in ascending order.
			direction[i] = -1
		}
	}
	if nullsFirst == nil {
		nullsFirst = make([]bool, len(columns))
	}
	if len(direction) != len(columns) {
		return nil, fmt.Errorf("pqarrow/arrowutils: expected %d directions got %d", len(columns), len(direction))
	}
	if len(nullsFirst) != len(columns) {
		return nil, fmt.Errorf("pqarrow/arrowutils: expected %d nullsFirst got %d", len(columns), len(nullsFirst))
	}

	build := array.NewInt32Builder(mem)
	defer build.Release()

	if r.NumRows() == 0 {
		return build.NewInt32Array(), nil
	}
	if r.NumRows() == 1 {
		build.Append(0)
		return build.NewInt32Array(), nil
	}

	ms := &multiColSorter{
		indices:     make([]int32, r.NumRows()),
		directions:  direction,
		nullsFirst:  nullsFirst,
		comparisons: make([]comparator, len(columns)),
	}
	for i := range ms.indices {
		ms.indices[i] = int32(i)
	}

	for i, colIdx := range columns {
		switch e := r.Column(colIdx).(type) {
		case *array.Int64:
			ms.comparisons[i] = &orderedSorter[int64]{array: e}
		case *array.Float64:
			ms.comparisons[i] = &orderedSorter[float64]{array: e}
		case *array.String:
			ms.comparisons[i] = &orderedSorter[string]{array: e}
		case *array.Dictionary:
			switch elem := e.Dictionary().(type) {
			case *array.String:
				ms.comparisons[i] = &orderedSorter[string]{
					array: &stringDictionary{
						dict: e,
						elem: elem,
					},
				}
			default:
				return nil, fmt.Errorf("unsupported dictionary column type for sorting %T", e)
			}
		default:
			return nil, fmt.Errorf("unsupported column type for sorting %T", e)
		}
	}
	sort.Sort(ms)
	build.AppendValues(ms.indices, nil)
	return build.NewInt32Array(), nil
}

// ReorderRecord uses indices to create a new record that is sorted according to
// the indices array.
func ReorderRecord(ctx context.Context, mem memory.Allocator, r arrow.Record, indices *array.Int32) (arrow.Record, error) {
	ctx = compute.WithAllocator(ctx, mem)
	// compute.Take doesn't support dictionaries. Use take on r when r does not have
	// dictionary column.
	var hasDictionary bool
	for i := 0; i < int(r.NumCols()); i++ {
		_, hasDictionary = r.Column(i).(*array.Dictionary)
		if hasDictionary {
			break
		}
	}
	if !hasDictionary {
		res, err := compute.Take(
			ctx,
			compute.TakeOptions{BoundsCheck: true},
			compute.NewDatum(r),
			compute.NewDatum(indices),
		)
		if err != nil {
			return nil, err
		}
		return res.(*compute.RecordDatum).Value, nil
	}
	resArr := make([]arrow.Array, r.NumCols())

	defer func() {
		for _, a := range resArr {
			if a != nil {
				a.Release()
			}
		}
	}()
	var g errgroup.Group
	for i := 0; i < int(r.NumCols()); i++ {
		i := i
		col := r.Column(i)
		if d, ok := col.(*array.Dictionary); ok {
			g.Go(takeDictColumn(ctx, d, i, resArr, indices))
		} else {
			g.Go(takeColumn(ctx, col, i, resArr, indices))
		}
	}
	err := g.Wait()
	if err != nil {
		return nil, err
	}
	return array.NewRecord(r.Schema(), resArr, r.NumRows()), nil
}

func takeColumn(ctx context.Context, a arrow.Array, idx int, arr []arrow.Array, indices *array.Int32) func() error {
	return func() error {
		r, err := compute.TakeArray(ctx, a, indices)
		if err != nil {
			return err
		}
		arr[idx] = r
		return nil
	}
}

func takeDictColumn(ctx context.Context, a *array.Dictionary, idx int, arr []arrow.Array, indices *array.Int32) func() error {
	return func() error {
		base := a.Dictionary().(*array.String)
		// Only string dictionaries are supported.
		r := array.NewBuilder(compute.GetAllocator(ctx), a.DataType()).(*array.BinaryDictionaryBuilder)
		defer r.Release()
		r.Reserve(indices.Len())
		for _, i := range indices.Int32Values() {
			if a.IsNull(int(i)) {
				r.AppendNull()
				continue
			}
			err := r.AppendString(base.Value(a.GetValueIndex(int(i))))
			if err != nil {
				return err
			}
		}
		arr[idx] = r.NewArray()
		return nil
	}
}

type multiColSorter struct {
	indices     []int32
	comparisons []comparator
	directions  []int
	nullsFirst  []bool
}

var _ sort.Interface = (*multiColSorter)(nil)

func (m *multiColSorter) Len() int { return len(m.indices) }

func (m *multiColSorter) Less(i, j int) bool {
	for idx := range m.comparisons {
		cmp := m.compare(idx, int(m.indices[i]), int(m.indices[j]))
		if cmp != 0 {
			// Use direction to reorder the comparison. Direction determines if the list
			// is in ascending or descending.
			//
			// For instance if comparison between i,j value is -1 and direction is -1
			// this will resolve to true hence the list will be in ascending order. Same
			// principle applies for descending.
			return cmp == m.directions[idx]
		}
		// Try comparing the next column
	}
	return false
}

func (m *multiColSorter) compare(idx, i, j int) int {
	x := m.comparisons[idx]
	if x.IsNull(i) {
		if x.IsNull(j) {
			return 0
		}
		if m.directions[idx] == 1 {
			if m.nullsFirst[idx] {
				return 1
			}
			return -1
		}
		if m.nullsFirst[idx] {
			return -1
		}
		return 1
	}
	if x.IsNull(j) {
		if m.directions[idx] == 1 {
			if m.nullsFirst[idx] {
				return -1
			}
			return 1
		}
		if m.nullsFirst[idx] {
			return 1
		}
		return -1
	}
	return x.Compare(i, j)
}

func (m *multiColSorter) Swap(i, j int) {
	m.indices[i], m.indices[j] = m.indices[j], m.indices[i]
}

type comparator interface {
	Compare(i, j int) int
	IsNull(int) bool
}

type orderedArray[T int64 | float64 | string] interface {
	Value(int) T
	IsNull(int) bool
}

type orderedSorter[T int64 | float64 | string] struct {
	array orderedArray[T]
}

func (s *orderedSorter[T]) IsNull(i int) bool {
	return s.array.IsNull(i)
}

func (s *orderedSorter[T]) Compare(i, j int) int {
	x := s.array.Value(i)
	y := s.array.Value(j)
	if x < y {
		return -1
	}
	if x > y {
		return 1
	}
	return 0
}

type stringDictionary struct {
	dict *array.Dictionary
	elem *array.String
}

func (s *stringDictionary) IsNull(i int) bool {
	return s.dict.IsNull(i)
}

func (s *stringDictionary) Value(i int) string {
	return s.elem.Value(s.dict.GetValueIndex(i))
}
