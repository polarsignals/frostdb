package arrowutils

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/compute"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"golang.org/x/sync/errgroup"

	"github.com/polarsignals/frostdb/pqarrow/builder"
)

type Direction uint

const (
	Ascending Direction = iota
	Descending
)

func (d Direction) comparison() int {
	switch d {
	case Ascending:
		return -1
	case Descending:
		return 1
	default:
		panic("unexpected direction value " + strconv.Itoa(int(d)) + " only -1 and 1 are allowed")
	}
}

// SortingColumn describes a sorting column on a arrow.Record.
type SortingColumn struct {
	Index      int
	Direction  Direction
	NullsFirst bool
}

// SortRecord sorts given arrow.Record by columns. Returns *array.Int32 of
// indices to sorted rows or record r.
//
// Comparison is made sequentially by each column. When rows are equal in the
// first column we compare the rows om the second column and so on and so forth
// until rows that are not equal are found.
func SortRecord(r arrow.Record, columns []SortingColumn) (*array.Int32, error) {
	if len(columns) == 0 {
		return nil, errors.New("pqarrow/arrowutils: at least one column is needed for sorting")
	}
	indicesBuilder := builder.NewOptInt32Builder(arrow.PrimitiveTypes.Int32)
	defer indicesBuilder.Release()

	if r.NumRows() == 0 {
		return indicesBuilder.NewArray().(*array.Int32), nil
	}
	if r.NumRows() == 1 {
		indicesBuilder.Append(0)
		return indicesBuilder.NewArray().(*array.Int32), nil
	}
	indicesBuilder.Reserve(int(r.NumRows()))
	for i := 0; i < int(r.NumRows()); i++ {
		indicesBuilder.UnsafeSet(i, int32(i))
	}
	ms, err := newMultiColSorter(r, indicesBuilder, columns)
	if err != nil {
		return nil, err
	}
	sort.Sort(ms)
	return indicesBuilder.NewArray().(*array.Int32), nil
}

// ReorderRecord uses indices to create a new record that is sorted according to
// the indices array.
func ReorderRecord(ctx context.Context, mem memory.Allocator, r arrow.Record, indices *array.Int32) (arrow.Record, error) {
	ctx = compute.WithAllocator(ctx, mem)
	// compute.Take doesn't support dictionaries. Use take on r when r does not have
	// dictionary column.
	var hasDictionary bool
	for i := 0; i < int(r.NumCols()); i++ {
		if r.Column(i).DataType().ID() == arrow.DICTIONARY {
			hasDictionary = true
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
			g.Go(func() error {
				return takeDictColumn(ctx, d, i, resArr, indices)
			})
		} else {
			g.Go(func() error {
				return takeColumn(ctx, col, i, resArr, indices)
			})
		}
	}
	err := g.Wait()
	if err != nil {
		return nil, err
	}
	return array.NewRecord(r.Schema(), resArr, r.NumRows()), nil
}

func takeColumn(ctx context.Context, a arrow.Array, idx int, arr []arrow.Array, indices *array.Int32) error {
	r, err := compute.TakeArray(ctx, a, indices)
	if err != nil {
		return err
	}
	arr[idx] = r
	return nil
}

func takeDictColumn(ctx context.Context, a *array.Dictionary, idx int, arr []arrow.Array, indices *array.Int32) error {
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

type multiColSorter struct {
	indices     *builder.OptInt32Builder
	comparisons []comparator
	directions  []int
	nullsFirst  []bool
}

func newMultiColSorter(
	r arrow.Record,
	indices *builder.OptInt32Builder,
	columns []SortingColumn,
) (*multiColSorter, error) {
	ms := &multiColSorter{
		indices:     indices,
		directions:  make([]int, len(columns)),
		nullsFirst:  make([]bool, len(columns)),
		comparisons: make([]comparator, len(columns)),
	}
	for i := range columns {
		ms.directions[i] = int(columns[i].Direction.comparison())
		ms.nullsFirst[i] = columns[i].NullsFirst
	}
	for i, col := range columns {
		switch e := r.Column(col.Index).(type) {
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
	return ms, nil
}

var _ sort.Interface = (*multiColSorter)(nil)

func (m *multiColSorter) Len() int { return m.indices.Len() }

func (m *multiColSorter) Less(i, j int) bool {
	for idx := range m.comparisons {
		cmp := m.compare(idx, int(m.indices.UnsafeValue(i)), int(m.indices.UnsafeValue(j)))
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
	m.indices.UnsafeSwap(i, j)
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
