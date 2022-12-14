package physicalplan

import (
	"bytes"
	"container/heap"
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"go.opentelemetry.io/otel/trace"

	"github.com/polarsignals/frostdb/pqarrow/arrowutils"
	"github.com/polarsignals/frostdb/pqarrow/builder"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

// OrderedAggregate is an aggregation operator that supports aggregations on
// streams of data ordered by the group by columns. This is a more efficient
// aggregation than aggregating by hash since a group can be determined as
// completed once a different aggregation key is found in the ordered stream.
// OrderedAggregate also supports partially ordered aggregations. This means
// aggregating on keys that arrive in ordered sets of data that are not mutually
// exclusive. For example consider the group by columns: a, b, c, a, b, c. The
// OrderedAggregate will perform the aggregation on the first a, b, c and
// another one on the second a, b, c. The result of both aggregations is merged.
type OrderedAggregate struct {
	pool                  memory.Allocator
	tracer                trace.Tracer
	resultColumnName      string
	groupByCols           map[string]builder.ColumnBuilder
	groupByColumnMatchers []logicalplan.Expr
	curGroup              []any
	columnToAggregate     logicalplan.Expr
	aggregationFunction   AggregationFunction
	next                  PhysicalPlan
	// Indicate is this is the last aggregation or
	// if this is a aggregation with another aggregation to follow after synchronizing.
	finalStage bool

	// Buffers that are reused across callback calls.
	groupByFields []arrow.Field
	groupByArrays []arrow.Array

	// arrayToAggCarry is used in cases where there are no new groups found in
	// a record. In this case, since we cannot know if the group will continue
	// in the next record, we need to store the data to aggregate to push into
	// the aggregation function whenever the end of the group is found.
	arrayToAggCarry builder.ColumnBuilder
}

func NewOrderedAggregate(
	pool memory.Allocator,
	tracer trace.Tracer,
	resultColumnName string,
	aggregationFunction AggregationFunction,
	columnToAggregate logicalplan.Expr,
	groupByColumnMatchers []logicalplan.Expr,
	finalStage bool,
) *OrderedAggregate {
	if !finalStage {
		panic("non-final stage ordered aggregation is not supprted yet")
	}

	return &OrderedAggregate{
		pool:              pool,
		tracer:            tracer,
		resultColumnName:  resultColumnName,
		groupByCols:       make(map[string]builder.ColumnBuilder),
		columnToAggregate: columnToAggregate,
		// TODO: Matchers can be optimized to be something like a radix tree or
		// just a fast-lookup datastructure for exact matches or prefix matches.
		groupByColumnMatchers: groupByColumnMatchers,
		aggregationFunction:   aggregationFunction,
		finalStage:            finalStage,

		groupByFields: make([]arrow.Field, 0, 10),
		groupByArrays: make([]arrow.Array, 0, 10),
	}
}

func (a *OrderedAggregate) SetNext(next PhysicalPlan) {
	a.next = next
}

func (a *OrderedAggregate) Draw() *Diagram {
	var child *Diagram
	if a.next != nil {
		child = a.next.Draw()
	}

	var groupings []string
	for _, grouping := range a.groupByColumnMatchers {
		groupings = append(groupings, grouping.Name())
	}

	details := fmt.Sprintf("OrderedAggregate (%s by %s)", a.columnToAggregate.Name(), strings.Join(groupings, ","))
	return &Diagram{Details: details, Child: child}
}

func (a *OrderedAggregate) Callback(ctx context.Context, r arrow.Record) error {
	// Generates high volume of spans. Comment out if needed during development.
	// ctx, span := a.tracer.Start(ctx, "OrderedAggregate/Callback")
	// defer span.End()

	a.groupByFields = a.groupByFields[:0]
	a.groupByArrays = a.groupByArrays[:0]

	var columnToAggregate arrow.Array
	aggregateFieldFound := false
	for i, field := range r.Schema().Fields() {
		for _, matcher := range a.groupByColumnMatchers {
			if matcher.MatchColumn(field.Name) {
				a.groupByFields = append(a.groupByFields, field)
				a.groupByArrays = append(a.groupByArrays, r.Column(i))
				a.curGroup = append(a.curGroup, nil)
			}
			// TODO(asubiotto): To have a 1:1 groupByArrays to fields match,
			// we might be able to flip the search around and have a virtual
			// null column for group by's that aren't found.
		}

		if a.columnToAggregate.MatchColumn(field.Name) {
			columnToAggregate = r.Column(i)
			if a.arrayToAggCarry == nil {
				a.arrayToAggCarry = builder.NewBuilder(a.pool, columnToAggregate.DataType())
			}
			aggregateFieldFound = true
		}
	}

	if !aggregateFieldFound {
		return errors.New("aggregate field not found, aggregations are not possible without it")
	}

	// TODO(asubiotto): Explore a static schema in the execution engine, all
	// this should be initialization code.
	for i, field := range a.groupByFields {
		if _, ok := a.groupByCols[field.Name]; !ok {
			b := builder.NewBuilder(a.pool, field.Type)
			a.groupByCols[field.Name] = b
			// Append the first group value to use below.
			v, err := arrowutils.GetValue(a.groupByArrays[i], 0)
			if err != nil {
				return err
			}
			a.curGroup[i] = v
		}
	}

	groupRanges, setRanges, err := a.getGroupsAndOrderedSetRanges()
	if err != nil {
		return err
	}

	if setRanges.Len() > 0 {
		panic("ordered sets not yet implemented")
	}

	// Aggregate the values for all groups found.
	arraysToAggregate := make([]arrow.Array, 0, groupRanges.Len())
	groupStart := int64(0)
	for {
		groupEnd, groupOk := popNextNotEqual(groupRanges, groupStart)
		if !groupOk {
			// All groups have been processed.
			break
		}

		// Append the values to aggregate.
		toAgg := array.NewSlice(columnToAggregate, groupStart, groupEnd)
		if a.arrayToAggCarry.Len() > 0 {
			if err := builder.AppendArray(a.arrayToAggCarry, toAgg); err != nil {
				return err
			}
			toAgg = a.arrayToAggCarry.NewArray()
		}
		arraysToAggregate = append(arraysToAggregate, toAgg)

		// Append the groups.
		for i, field := range a.groupByFields {
			var (
				v   any
				err error
			)
			if v, err = arrowutils.GetValue(a.groupByArrays[i], int(groupStart)); err != nil {
				return err
			}
			if err := builder.AppendGoValue(
				a.groupByCols[field.Name],
				v,
			); err != nil {
				return err
			}
		}

		groupStart = groupEnd
	}

	// The values corresponding to the last group need to be carried over to the
	// next aggregation since we can't determine that the last group is closed
	// until we know the first value of the next record passed to Callback.
	// Note that the current group values should already be set in a.curGroup.
	// TODO(asubiotto): We don't handle NULL values in aggregation columns
	// in aggregation functions so disregard them here as well for now. We
	// should eventually care about this.
	if err := builder.AppendArray(
		a.arrayToAggCarry,
		array.NewSlice(columnToAggregate, groupStart, int64(columnToAggregate.Len())),
	); err != nil {
		return err
	}

	if len(arraysToAggregate) == 0 {
		// No new groups were found, carry on.
		return nil
	}

	results, err := a.aggregationFunction.Aggregate(a.pool, arraysToAggregate)
	if err != nil {
		return err
	}

	return a.flushToNext(ctx, results)
}

func (a *OrderedAggregate) Finish(ctx context.Context) error {
	ctx, span := a.tracer.Start(ctx, "OrderedAggregate/Finish")
	defer span.End()

	if a.arrayToAggCarry.Len() > 0 {
		// Aggregate the last group.
		for i, field := range a.groupByFields {
			b := a.groupByCols[field.Name]
			if err := builder.AppendGoValue(
				b, a.curGroup[i],
			); err != nil {
				return err
			}
		}

		results, err := a.aggregationFunction.Aggregate(
			a.pool,
			[]arrow.Array{a.arrayToAggCarry.NewArray()},
		)
		if err != nil {
			return err
		}

		if err := a.flushToNext(ctx, results); err != nil {
			return err
		}
	}

	return a.next.Finish(ctx)
}

func (a *OrderedAggregate) flushToNext(ctx context.Context, results arrow.Array) error {
	groups := make([]arrow.Array, 0, len(a.groupByCols))
	for _, v := range a.groupByCols {
		groups = append(groups, v.NewArray())
	}

	fieldName := a.columnToAggregate.Name()
	if a.finalStage {
		fieldName = a.resultColumnName
	}

	nrows := int64(results.Len())
	return a.next.Callback(ctx, array.NewRecord(
		arrow.NewSchema(
			append(
				a.groupByFields,
				arrow.Field{Name: fieldName, Type: results.DataType()},
			),
			nil,
		),
		append(groups, results),
		nrows,
	))
}

// getGroupsAndOrderedSetRanges returns a min-heap of group ranges and ordered
// set ranges in that order. The ranges are determined by iterating over the
// group by arrays and comparing to the current group value for each column.
func (a *OrderedAggregate) getGroupsAndOrderedSetRanges() (*int64Heap, *int64Heap, error) {
	// groupRanges keeps track of the bounds of the group by columns.
	groupRanges := &int64Heap{}
	heap.Init(groupRanges)
	// setRanges keeps track of the bounds of ordered sets. i.e. in the
	// following slice, (a, a, b, c) is an ordered set of three groups. The
	// second ordered set is (a, e): [a, a, b, c, a, e]
	setRanges := &int64Heap{}
	heap.Init(setRanges)

	// handleCmpResult is a closure that encapsulates the handling of the result
	// of comparing a current grouping column with a value in a group array.
	handleCmpResult := func(cmp, column int, t arrow.Array, j int) error {
		switch cmp {
		case -1:
			// New group, append range index.
			heap.Push(groupRanges, int64(j))

			// And update the current group.
			v, err := arrowutils.GetValue(t, j)
			if err != nil {
				return err
			}
			a.curGroup[column] = v
		case 0:
			// Equal to group, do nothing.
		case 1:
			// New ordered set encountered.
			heap.Push(setRanges, int64(j))
			heap.Push(groupRanges, int64(j))
		}
		return nil
	}
	for i, arr := range a.groupByArrays {
		switch t := arr.(type) {
		case *array.Binary:
			for j := 0; j < arr.Len(); j++ {
				var curGroup []byte
				if a.curGroup[i] != nil {
					curGroup = a.curGroup[i].([]byte)
				}
				vIsNull := t.IsNull(j)
				cmp, ok := nullGroupComparison(curGroup == nil, vIsNull)
				if !ok {
					cmp = bytes.Compare(curGroup, t.Value(j))
				}
				if err := handleCmpResult(cmp, i, t, j); err != nil {
					return nil, nil, err
				}
			}
		case *array.String:
			for j := 0; j < arr.Len(); j++ {
				var curGroup *string
				if a.curGroup[i] != nil {
					g := a.curGroup[i].(string)
					curGroup = &g
				}
				vIsNull := t.IsNull(j)
				cmp, ok := nullGroupComparison(curGroup == nil, vIsNull)
				if !ok {
					cmp = strings.Compare(*curGroup, t.Value(j))
				}
				if err := handleCmpResult(cmp, i, t, j); err != nil {
					return nil, nil, err
				}
			}
		case *array.Int64:
			for j := 0; j < arr.Len(); j++ {
				var curGroup *int64
				if a.curGroup[i] != nil {
					g := a.curGroup[i].(int64)
					curGroup = &g
				}
				vIsNull := t.IsNull(j)
				cmp, ok := nullGroupComparison(curGroup == nil, vIsNull)
				if !ok {
					cmp = compareInt64(*curGroup, t.Value(j))
				}
				if err := handleCmpResult(cmp, i, t, j); err != nil {
					return nil, nil, err
				}
			}
		case *array.Boolean:
			for j := 0; j < arr.Len(); j++ {
				var curGroup *bool
				if a.curGroup[i] != nil {
					g := a.curGroup[i].(bool)
					curGroup = &g
				}
				vIsNull := t.IsNull(j)
				cmp, ok := nullGroupComparison(curGroup == nil, vIsNull)
				if !ok {
					cmp = compareBools(*curGroup, t.Value(j))
				}
				if err := handleCmpResult(cmp, i, t, j); err != nil {
					return nil, nil, err
				}
			}
		default:
			panic("unsupported type")
		}
	}
	return groupRanges, setRanges, nil
}

// nullComparison encapsulates null comparison when scanning groups in the
// ordered aggregator. leftNull is whether the current group is null, and
// rightNull is whether the value we're comparing against is null. Note that
// this function observes default SQL semantics as well as our own, i.e. nulls
// sort first.
// The comparison integer is returned, as well as whether either value was null.
// If the returned boolean is false, the comparison should be disregarded.
func nullGroupComparison(leftNull, rightNull bool) (int, bool) {
	if !leftNull && !rightNull {
		// Both are null, this implies that the null comparison should be
		// disregarded.
		return 0, false
	}

	if leftNull {
		if !rightNull {
			return -1, true
		}
		return 0, true
	}
	return 1, true
}

func compareInt64(a, b int64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func compareBools(a, b bool) int {
	if a == b {
		return 0
	}

	if !a {
		return -1
	}
	return 1
}

type int64Heap []int64

func (h int64Heap) Len() int {
	return len(h)
}

func (h int64Heap) Less(i, j int) bool {
	return h[i] < h[j]
}

func (h int64Heap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *int64Heap) Push(x any) {
	*h = append(*h, x.(int64))
}

func (h *int64Heap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func popNextNotEqual(h *int64Heap, compare int64) (int64, bool) {
	for h.Len() > 0 {
		v := heap.Pop(h).(int64)
		if v != compare {
			return v, true
		}
	}
	return 0, false
}
