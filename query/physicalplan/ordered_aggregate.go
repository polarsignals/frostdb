package physicalplan

import (
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
// exclusive. For example consider the group by columns: a, a, b, c, a, b, c.
// The OrderedAggregate will perform the aggregation on the first ordered set
// a, a, b, c and another one on the second a, b, c. The result of both
// aggregations is merged. Specifically, if the example is pushed to Callback
// in two records (a, a, b, c) followed by (a, b, c), and assuming that the
// aggregation values for each row are 1 for simplicity and we're using a sum
// aggregation, after the first call to Callback the OrderedAggregate will store
// [a, b, c], [2, 1, 1] but not emit anything. When the second record is pushed,
// the OrderedAggregate will realize that the first value in the new record (a)
// sorts before the "current group" (c), so will store the aggregation results
// of the second record as another ordered group [a, b, c], [1, 1, 1]. Only when
// Finish is called, will the OrderedAggregate be able to emit the merged
// aggregation results. The merged results should be: [a, b, c], [3, 2, 2].
// TODO(asubiotto): The OrderedAggregate is not yet ready for production use. It
// doesn't handle dynamic columns very well (i.e. grouping columns appearing and
// disappearing in input).
type OrderedAggregate struct {
	// Fields that are constant throughout execution.
	pool                  memory.Allocator
	tracer                trace.Tracer
	resultColumnName      string
	groupByColumnMatchers []logicalplan.Expr
	aggregationFunction   AggregationFunction
	next                  PhysicalPlan
	columnToAggregate     logicalplan.Expr
	// Indicate is this is the last aggregation or if this is an aggregation
	// with another aggregation to follow after synchronizing.
	finalStage bool

	// Buffers that are reused across callback calls. These point to the fields
	// and arrays in the record passed in to Callback.
	groupByFields []arrow.Field
	groupByArrays []arrow.Array

	// curGroup is used for comparisons against the groupResults found in each
	// record. It is initialized to the first group of the first record and
	// updated as new groupResults are found. The key is the field name, as in
	// groupBuilders below (this will hopefully change once we have static
	// schemas in the execution engine).
	curGroup map[string]any

	// groupBuilders is a map from the group by field name to the group column
	// builders **for the current ordered set**. If a new ordered set is found,
	// the builders flush the array to groupResults below.
	groupBuilders map[string]builder.ColumnBuilder

	// groupResults are the group columns. groupResults[i] represents the group
	// columns of ordered set i.
	groupResults [][]arrow.Array

	// arrayToAggCarry is used to carry over the values to aggregate for the
	// last group in a record since we cannot know whether that group continues
	// in the next record.
	arrayToAggCarry builder.ColumnBuilder

	// aggResultBuilder is a builder of the aggregation results for the current
	// ordered set (i.e. each element in this builder is the aggregation result
	// for one group in the ordered set). When the end of the ordered set is
	// found, the values in this builder are appended to aggregationResults
	// below.
	aggResultBuilder arrowutils.ArrayConcatenator

	// aggregationResults are the results of aggregating the values across
	// multiple calls to Callback. aggregationResults[i] is the arrow array that
	// belongs to ordered set i.
	aggregationResults []arrow.Array

	scratch struct {
		indexes []int64
	}
}

func NewOrderedAggregate(
	pool memory.Allocator,
	tracer trace.Tracer,
	aggregation Aggregation,
	groupByColumnMatchers []logicalplan.Expr,
	finalStage bool,
) *OrderedAggregate {
	if !finalStage {
		panic("non-final stage ordered aggregation is not supprted yet")
	}

	return &OrderedAggregate{
		pool:              pool,
		tracer:            tracer,
		resultColumnName:  aggregation.resultName,
		columnToAggregate: aggregation.expr,
		// TODO: Matchers can be optimized to be something like a radix tree or
		// just a fast-lookup datastructure for exact matches or prefix matches.
		groupByColumnMatchers: groupByColumnMatchers,
		aggregationFunction:   aggregation.function,
		finalStage:            finalStage,

		groupByFields: make([]arrow.Field, 0, 10),
		groupByArrays: make([]arrow.Array, 0, 10),

		groupBuilders: make(map[string]builder.ColumnBuilder),

		aggregationResults: make([]arrow.Array, 0, 1),
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

	details := fmt.Sprintf(
		"OrderedAggregate (%s by %s)",
		a.columnToAggregate.Name(),
		strings.Join(groupings, ","),
	)
	return &Diagram{Details: details, Child: child}
}

func (a *OrderedAggregate) Callback(_ context.Context, r arrow.Record) error {
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

	firstCall := a.curGroup == nil
	// curGroup is a slice that holds the group values for an index of the
	// groupByFields it is done so that we can access group values without
	// hashing by the field name.
	curGroup := make([]any, len(a.groupByFields))
	if !firstCall {
		// This is not the first call to callback. Initialize curGroup to the
		// values stored in the curGroup map.
		for i, f := range a.groupByFields {
			curGroup[i] = a.curGroup[f.Name]
		}
	} else {
		a.curGroup = make(map[string]any, len(a.groupByFields))
	}

	if !aggregateFieldFound {
		return errors.New("aggregate field not found, aggregations are not possible without it")
	}

	// TODO(asubiotto): Explore a static schema in the execution engine, all
	// this should be initialization code.
	for i, field := range a.groupByFields {
		if _, ok := a.groupBuilders[field.Name]; !ok {
			b := builder.NewBuilder(a.pool, field.Type)
			a.groupBuilders[field.Name] = b
			if firstCall {
				// Append the first group value to use below.
				v, err := arrowutils.GetValue(a.groupByArrays[i], 0)
				if err != nil {
					return err
				}
				switch concreteV := v.(type) {
				case []byte:
					// Safe copy.
					a.curGroup[field.Name] = append([]byte(nil), concreteV...)
				default:
					a.curGroup[field.Name] = v
				}
				curGroup[i] = v
			}
		}
	}

	groupRanges, wrappedSetRanges, lastGroup, err := arrowutils.GetGroupsAndOrderedSetRanges(
		curGroup,
		a.groupByArrays,
	)
	if err != nil {
		return err
	}
	// Don't update curGroup to lastGroup yet, given that the end of the
	// curGroup from the last record might have been found at the zeroth index
	// and we need to know what values to append to the group builders.
	defer func() {
		for i, v := range lastGroup {
			a.curGroup[a.groupByFields[i].Name] = v
		}
	}()

	setRanges := wrappedSetRanges.Unwrap(a.scratch.indexes)

	// Aggregate the values for all groups found.
	arraysToAggregate := make([]arrow.Array, 0, groupRanges.Len())

	// arraysToAggregateSetIdxs keeps track of the idxs in arraysToAggregate
	// that represent new ordered sets. This is essentially a "conversion" of
	// the setRanges which refer to ranges of individual values in the input
	// record while arraysToAggregateSetIdxs refer to ranges of groups.
	var arraysToAggregateSetIdxs []int64
	for groupStart, setCursor := int64(-1), 0; ; {
		groupEnd, groupOk := groupRanges.PopNextNotEqual(groupStart)
		// groupStart is initialized to -1 to not ignore groupEnd == 0, after
		// the first pop, it should now be set to 0.
		if groupStart == -1 {
			groupStart = 0
		}
		if !groupOk {
			// All groups have been processed.
			// The values corresponding to the last group need to be carried
			// over to the next aggregation since we can't determine that the
			// last group is closed until we know the first value of the next
			// record passed to Callback.
			// Note that the current group values should already be set in
			// a.curGroup.
			// TODO(asubiotto): We don't handle NULL values in aggregation
			// columns in aggregation functions so disregard them here as well
			// for now. We should eventually care about this.
			// TODO(asubiotto): Instead of doing this copy, what would the
			// performance difference be if we just merged the aggregation?
			if err := builder.AppendArray(
				a.arrayToAggCarry,
				array.NewSlice(columnToAggregate, groupStart, int64(columnToAggregate.Len())),
			); err != nil {
				return err
			}
			break
		}

		// Append the values to aggregate.
		var toAgg arrow.Array
		if groupEnd == 0 {
			// End of the group found in the last record, the only data to
			// aggregate was carried over.
			toAgg = a.arrayToAggCarry.NewArray()
		} else {
			toAgg = array.NewSlice(columnToAggregate, groupStart, groupEnd)
			if a.arrayToAggCarry.Len() > 0 {
				if err := builder.AppendArray(a.arrayToAggCarry, toAgg); err != nil {
					return err
				}
				toAgg = a.arrayToAggCarry.NewArray()
			}
		}
		arraysToAggregate = append(arraysToAggregate, toAgg)

		// Here's a way to try out to solve th logic: Instead of consifering a new
		// ordered set the current group, we could check groupEnd == setRanges
		// This should indicate that the current group is the last one of the
		// ordered set. This means that we would basically append to builders
		// and then if newOrderedSet, we will flush to group results
		//

		// Append the groups.
		newOrderedSet := false
		if len(setRanges) > 0 && setCursor < len(setRanges) && setRanges[setCursor] == groupEnd {
			setCursor++
			newOrderedSet = true
			arraysToAggregateSetIdxs = append(arraysToAggregateSetIdxs, int64(len(arraysToAggregate)))
		}
		for i, field := range a.groupByFields {
			var (
				v   any
				err error
			)
			if groupEnd == 0 {
				// End of the current group of the last record.
				v = a.curGroup[field.Name]
			} else {
				if v, err = arrowutils.GetValue(a.groupByArrays[i], int(groupStart)); err != nil {
					return err
				}
			}
			if err := builder.AppendGoValue(
				a.groupBuilders[field.Name],
				v,
			); err != nil {
				return err
			}

			if newOrderedSet {
				// This group is the last one of the current ordered set. Flush
				// it to the results. The corresponding aggregation results are
				// flushed in a loop below.
				a.groupResults = append(a.groupResults, nil)
				n := len(a.groupResults) - 1
				for _, field := range a.groupByFields {
					a.groupResults[n] = append(a.groupResults[n], a.groupBuilders[field.Name].NewArray())
				}
			}
		}

		groupStart = groupEnd
	}

	if len(arraysToAggregate) == 0 {
		// No new groups or sets were found, carry on.
		return nil
	}

	results, err := a.aggregationFunction.Aggregate(a.pool, arraysToAggregate)
	if err != nil {
		return err
	}

	// Supporting partial ordering implies the need to accumulate all the
	// results since any group might reoccur at any point in future records.
	// If we can determine that the ordering is global at plan time, we could
	// directly flush the results.
	setStart := int64(0)
	for _, setEnd := range arraysToAggregateSetIdxs {
		set := array.NewSlice(results, setStart, setEnd)
		if a.aggResultBuilder.Len() > 0 {
			// This is the end of an ordered set that started in the last
			// record.
			a.aggResultBuilder.Add(set)
			var err error
			set, err = a.aggResultBuilder.NewArray(a.pool)
			if err != nil {
				return err
			}
		}
		a.aggregationResults = append(a.aggregationResults, set)
		setStart = setEnd
	}
	// The last ordered set cannot be determined to close within this
	// record, so carry it over.
	a.aggResultBuilder.Add(array.NewSlice(results, setStart, int64(results.Len())))
	return nil
}

func (a *OrderedAggregate) Finish(ctx context.Context) error {
	ctx, span := a.tracer.Start(ctx, "OrderedAggregate/Finish")
	defer span.End()

	if a.arrayToAggCarry.Len() > 0 {
		// Aggregate the last group.
		a.groupResults = append(a.groupResults, nil)
		n := len(a.groupResults) - 1
		for _, field := range a.groupByFields {
			b := a.groupBuilders[field.Name]
			if err := builder.AppendGoValue(
				b, a.curGroup[field.Name],
			); err != nil {
				return err
			}
			a.groupResults[n] = append(a.groupResults[n], b.NewArray())
		}

		results, err := a.aggregationFunction.Aggregate(
			a.pool,
			[]arrow.Array{a.arrayToAggCarry.NewArray()},
		)
		if err != nil {
			return err
		}

		var lastResults arrow.Array
		if a.aggResultBuilder.Len() > 0 {
			// Append the results to the last ordered set.
			a.aggResultBuilder.Add(results)
			var err error
			lastResults, err = a.aggResultBuilder.NewArray(a.pool)
			if err != nil {
				return err
			}
		} else {
			lastResults = results
		}
		a.aggregationResults = append(a.aggregationResults, lastResults)
	}

	schema := arrow.NewSchema(
		append(
			a.groupByFields,
			arrow.Field{Name: a.getResultColumnName(), Type: a.aggregationResults[0].DataType()},
		),
		nil,
	)

	records := make([]arrow.Record, 0, len(a.groupResults))
	for i := range a.groupResults {
		records = append(
			records,
			array.NewRecord(
				schema,
				append(
					a.groupResults[i],
					a.aggregationResults[i],
				),
				int64(a.aggregationResults[i].Len()),
			),
		)
	}

	if len(records) == 1 {
		// TODO(asubiotto): This can probably be simplified (i.e. have a record
		// var that is set either to records[0] or the merged records.
		if err := a.next.Callback(ctx, records[0]); err != nil {
			return err
		}
	} else {
		// The aggregation results must be merged.
		orderByCols := make([]int, len(a.groupByFields))
		for i := range orderByCols {
			orderByCols[i] = i
		}
		mergedRecord, err := arrowutils.MergeRecords(a.pool, records, orderByCols)
		if err != nil {
			return err
		}
		firstGroup := make([]any, len(a.groupByFields))
		groupArrs := mergedRecord.Columns()[:len(a.groupByFields)]
		for i, arr := range groupArrs {
			v, err := arrowutils.GetValue(arr, 0)
			if err != nil {
				return err
			}
			firstGroup[i] = v
		}
		wrappedGroupRanges, _, _, err := arrowutils.GetGroupsAndOrderedSetRanges(firstGroup, groupArrs)
		if err != nil {
			return err
		}
		groupRanges := wrappedGroupRanges.Unwrap(a.scratch.indexes)
		// Close the last range to iterate over all groupResults.
		groupRanges = append(groupRanges, mergedRecord.NumRows())

		// For better performance, the result is built a column at a time.
		for i, field := range a.groupByFields {
			start := int64(0)
			for _, end := range groupRanges {
				if err := builder.AppendValue(
					a.groupBuilders[field.Name], mergedRecord.Column(i), int(start),
				); err != nil {
					return err
				}
				start = end
			}
		}

		// The array of aggregation values is the first column index after the
		// group fields.
		aggregationVals := mergedRecord.Columns()[len(a.groupByFields)]
		start := int64(0)
		toAggregate := make([]arrow.Array, 0, len(groupRanges))
		for _, end := range groupRanges {
			toAggregate = append(toAggregate, array.NewSlice(aggregationVals, start, end))
			start = end
		}

		result, err := runAggregation(true, a.aggregationFunction, a.pool, toAggregate)
		if err != nil {
			return err
		}

		groups := make([]arrow.Array, 0, len(a.groupBuilders))
		for _, field := range a.groupByFields {
			groups = append(groups, a.groupBuilders[field.Name].NewArray())
		}
		if err := a.next.Callback(
			ctx,
			array.NewRecord(
				schema,
				append(groups, result),
				int64(result.Len()),
			),
		); err != nil {
			return err
		}
	}

	return a.next.Finish(ctx)
}

func (a *OrderedAggregate) getResultColumnName() string {
	fieldName := a.columnToAggregate.Name()
	if a.finalStage {
		fieldName = a.resultColumnName
	}
	return fieldName
}
