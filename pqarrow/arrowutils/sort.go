package arrowutils

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"sync"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/compute"
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
	ms, err := newMultiColSorter(r, columns)
	if err != nil {
		return nil, err
	}
	defer ms.Release()
	sort.Sort(ms)
	return ms.indices.NewArray().(*array.Int32), nil
}

// Take uses indices which is an array of row index and returns a new record
// that only contains rows specified in indices.
//
// Use compute.WithAllocator to pass a custom memory.Allocator.
func Take(ctx context.Context, r arrow.Record, indices *array.Int32) (arrow.Record, error) {
	// compute.Take doesn't support dictionaries or lists. Use take on r when r
	// does not have these columns.
	var customTake bool
	for i := 0; i < int(r.NumCols()); i++ {
		if r.Column(i).DataType().ID() == arrow.DICTIONARY || r.Column(i).DataType().ID() == arrow.LIST {
			customTake = true
			break
		}
	}
	if !customTake {
		res, err := compute.Take(
			ctx,
			compute.TakeOptions{BoundsCheck: true},
			compute.NewDatumWithoutOwning(r),
			compute.NewDatumWithoutOwning(indices),
		)
		if err != nil {
			return nil, err
		}
		return res.(*compute.RecordDatum).Value, nil
	}
	if r.NumCols() == 0 {
		return r, nil
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
		switch arr := r.Column(i).(type) {
		case *array.Dictionary:
			g.Go(func() error { return TakeDictColumn(ctx, arr, i, resArr, indices) })
		case *array.List:
			g.Go(func() error { return TakeListColumn(ctx, arr, i, resArr, indices) })
		default:
			g.Go(func() error { return TakeColumn(ctx, col, i, resArr, indices) })
		}
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	// We checked for at least one column at the beginning of the function.
	expectedLen := resArr[0].Len()
	for _, a := range resArr {
		if a.Len() != expectedLen {
			return nil, fmt.Errorf(
				"pqarrow/arrowutils: expected same length %d for all columns got %d for %s", expectedLen, a.Len(), a.DataType().Name(),
			)
		}
	}
	return array.NewRecord(r.Schema(), resArr, int64(indices.Len())), nil
}

func TakeColumn(ctx context.Context, a arrow.Array, idx int, arr []arrow.Array, indices *array.Int32) error {
	r, err := compute.TakeArray(ctx, a, indices)
	if err != nil {
		return err
	}
	arr[idx] = r
	return nil
}

func TakeDictColumn(ctx context.Context, a *array.Dictionary, idx int, arr []arrow.Array, indices *array.Int32) error {
	r := array.NewDictionaryBuilderWithDict(
		compute.GetAllocator(ctx), a.DataType().(*arrow.DictionaryType), a.Dictionary(),
	).(*array.BinaryDictionaryBuilder)
	defer r.Release()

	r.Reserve(indices.Len())
	idxBuilder := r.IndexBuilder()
	for _, i := range indices.Int32Values() {
		if a.IsNull(int(i)) {
			r.AppendNull()
			continue
		}
		idxBuilder.Append(a.GetValueIndex(int(i)))
	}

	arr[idx] = r.NewArray()
	return nil
}

func TakeListColumn(ctx context.Context, a *array.List, idx int, arr []arrow.Array, indices *array.Int32) error {
	r := array.NewBuilder(compute.GetAllocator(ctx), a.DataType()).(*array.ListBuilder)
	valueBuilder, ok := r.ValueBuilder().(*array.BinaryDictionaryBuilder)
	if !ok {
		return fmt.Errorf("unexpected value builder type %T for list column", r.ValueBuilder())
	}

	listValues := a.ListValues().(*array.Dictionary)
	switch dictV := listValues.Dictionary().(type) {
	case *array.String:
		if err := valueBuilder.InsertStringDictValues(dictV); err != nil {
			return err
		}
	case *array.Binary:
		if err := valueBuilder.InsertDictValues(dictV); err != nil {
			return err
		}
	}
	idxBuilder := valueBuilder.IndexBuilder()

	r.Reserve(indices.Len())
	for _, i := range indices.Int32Values() {
		if a.IsNull(int(i)) {
			r.AppendNull()
			continue
		}

		r.Append(true)
		start, end := a.ValueOffsets(int(i))
		for j := start; j < end; j++ {
			idxBuilder.Append(listValues.GetValueIndex(int(j)))
		}
		// Resize is necessary here for the correct offsets to be appended to
		// the list builder. Otherwise length will remain at 0.
		valueBuilder.Resize(idxBuilder.Len())
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
	columns []SortingColumn,
) (*multiColSorter, error) {
	ms := multiColSorterPool.Get().(*multiColSorter)
	if r.NumRows() <= 1 {
		if r.NumRows() == 1 {
			ms.indices.Append(0)
		}
		return ms, nil
	}
	ms.Reserve(int(r.NumRows()), len(columns))
	for i := range columns {
		ms.directions[i] = int(columns[i].Direction.comparison())
		ms.nullsFirst[i] = columns[i].NullsFirst
	}
	for i, col := range columns {
		switch e := r.Column(col.Index).(type) {
		case *array.Int16:
			ms.comparisons[i] = newOrderedSorter[int16](e, cmp.Compare)
		case *array.Int32:
			ms.comparisons[i] = newOrderedSorter[int32](e, cmp.Compare)
		case *array.Int64:
			ms.comparisons[i] = newOrderedSorter[int64](e, cmp.Compare)
		case *array.Uint16:
			ms.comparisons[i] = newOrderedSorter[uint16](e, cmp.Compare)
		case *array.Uint32:
			ms.comparisons[i] = newOrderedSorter[uint32](e, cmp.Compare)
		case *array.Uint64:
			ms.comparisons[i] = newOrderedSorter[uint64](e, cmp.Compare)
		case *array.Float64:
			ms.comparisons[i] = newOrderedSorter[float64](e, cmp.Compare)
		case *array.String:
			ms.comparisons[i] = newOrderedSorter[string](e, cmp.Compare)
		case *array.Binary:
			ms.comparisons[i] = newOrderedSorter[[]byte](e, bytes.Compare)
		case *array.Dictionary:
			switch elem := e.Dictionary().(type) {
			case *array.String:
				ms.comparisons[i] = newOrderedSorter[string](
					&stringDictionary{
						dict: e,
						elem: elem,
					},
					cmp.Compare,
				)
			case *array.Binary:
				ms.comparisons[i] = newOrderedSorter[[]byte](
					&binaryDictionary{
						dict: e,
						elem: elem,
					},
					bytes.Compare,
				)
			default:
				ms.Release()
				return nil, fmt.Errorf("unsupported dictionary column type for sorting %T", e)
			}
		default:
			ms.Release()
			return nil, fmt.Errorf("unsupported column type for sorting %T", e)
		}
	}
	return ms, nil
}

func (m *multiColSorter) Reserve(rows, columns int) {
	m.indices.Reserve(rows)
	for i := 0; i < rows; i++ {
		m.indices.Set(i, int32(i))
	}
	m.comparisons = slices.Grow(m.comparisons, columns)[:columns]
	m.directions = slices.Grow(m.directions, columns)[:columns]
	m.nullsFirst = slices.Grow(m.nullsFirst, columns)[:columns]
}

func (m *multiColSorter) Reset() {
	m.indices.Reserve(0)
	m.comparisons = m.comparisons[:0]
	m.directions = m.directions[:0]
	m.nullsFirst = m.nullsFirst[:0]
}

func (m *multiColSorter) Release() {
	m.Reset()
	multiColSorterPool.Put(m)
}

var multiColSorterPool = &sync.Pool{
	New: func() any {
		return &multiColSorter{
			indices: builder.NewOptInt32Builder(arrow.PrimitiveTypes.Int32),
		}
	},
}

var _ sort.Interface = (*multiColSorter)(nil)

func (m *multiColSorter) Len() int { return m.indices.Len() }

func (m *multiColSorter) Less(i, j int) bool {
	for idx := range m.comparisons {
		cmp := m.compare(idx, int(m.indices.Value(i)), int(m.indices.Value(j)))
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
	m.indices.Swap(i, j)
}

type comparator interface {
	Compare(i, j int) int
	IsNull(int) bool
}

type orderedArray[T any] interface {
	Value(int) T
	IsNull(int) bool
}

type orderedSorter[T any] struct {
	array   orderedArray[T]
	compare func(T, T) int
}

func newOrderedSorter[T any](a orderedArray[T], compare func(T, T) int) *orderedSorter[T] {
	return &orderedSorter[T]{
		array:   a,
		compare: compare,
	}
}

func (s *orderedSorter[T]) IsNull(i int) bool {
	return s.array.IsNull(i)
}

func (s *orderedSorter[T]) Compare(i, j int) int {
	return s.compare(s.array.Value(i), s.array.Value(j))
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

type binaryDictionary struct {
	dict *array.Dictionary
	elem *array.Binary
}

func (s *binaryDictionary) IsNull(i int) bool {
	return s.dict.IsNull(i)
}

func (s *binaryDictionary) Value(i int) []byte {
	return s.elem.Value(s.dict.GetValueIndex(i))
}
