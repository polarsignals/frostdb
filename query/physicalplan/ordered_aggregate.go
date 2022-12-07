package physicalplan

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"go.opentelemetry.io/otel/trace"

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

	// TODO(asubiotto): We currently only support a single group by column.
	if len(a.groupByArrays) > 1 {
		var names []string
		for _, m := range a.groupByColumnMatchers {
			names = append(names, m.Name())
		}
		panic(
			fmt.Sprintf("multiple group by columns not yet supported %s", strings.Join(names, ", ")),
		)
	}

	// TODO(asubiotto): Explore a static schema in the execution engine, all
	// this should be initialization code.
	for i, field := range a.groupByFields {
		if _, ok := a.groupByCols[field.Name]; !ok {
			b := builder.NewBuilder(a.pool, field.Type)
			a.groupByCols[field.Name] = b
			// Append the first group value to use below.
			v, err := builder.GetValue(a.groupByArrays[i], 0)
			if err != nil {
				return err
			}
			a.curGroup[i] = v
		}
	}

	// groupRanges keeps track of the "bounds" of the group by columns.
	groupRanges := []int64{0}
	// handleCmpResult is a closure that encapsulates the handling of the result
	// of comparing a current grouping column with a value in a group array. It
	// is a closure instead of a function in order to update groupRanges
	// in-place.
	handleCmpResult := func(cmp, column int, t arrow.Array, j int) error {
		switch cmp {
		case -1:
			// New group, append range index.
			groupRanges = append(groupRanges, int64(j))
			// Append the current group (which is now closed) to the
			// builder.
			b := a.groupByCols[a.groupByFields[column].Name]
			if err := builder.AppendGoValue(b, a.curGroup[column]); err != nil {
				return err
			}
			// And update the current group.
			v, err := builder.GetValue(t, j)
			if err != nil {
				return err
			}
			a.curGroup[column] = v
		case 0:
			// Equal to group, do nothing.
		case 1:
			// New ordered set encountered. Flush aggregation and create
			// new batch to aggregate into. These will then be merged
			// before returning any rows.
			panic("new ordered sets not yet implemented")
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
					return err
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
					return err
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
					return err
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
					return err
				}
			}
		default:
			panic("unsupported type")
		}
	}

	if len(groupRanges) == 1 {
		// No new groups found. Accumulate the values to aggregate until the end
		// of the current group is found (on a future call to Callback).

		// TODO(asubiotto): We don't handle NULL values in aggregation columns
		// in aggregation functions so disregard them here as well for now. We
		// should eventually care about this.
		return builder.AppendArray(a.arrayToAggCarry, columnToAggregate)
	}

	// Aggregate the values for all groups found.
	arraysToAggregate := make([]arrow.Array, 0, len(groupRanges))
	for i := 0; i < len(groupRanges)-1; i++ {
		start, end := groupRanges[i], groupRanges[i+1]
		toAgg := array.NewSlice(columnToAggregate, start, end)

		if a.arrayToAggCarry.Len() > 0 {
			if err := builder.AppendArray(a.arrayToAggCarry, toAgg); err != nil {
				return err
			}
			toAgg = a.arrayToAggCarry.NewArray()
		}

		arraysToAggregate = append(arraysToAggregate, toAgg)
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

// nullComparison encapsulates null comparison when scanning groups in the
// ordered aggregator. leftNull is whether the current group is null, and
// rightNull is whether the value we're comparing against is null. Note that
// this function observes default SQL semantics as well as our own, i.e. nulls
// sort first.
// The comparison integer is returned, as well as whether either value was null.
// If the returned boolean is false, the comparison should be disregarded.
func nullGroupComparison(leftNull, rightNull bool) (int, bool) {
	if !leftNull && !rightNull {
		// Both are null, this implies equality.
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
