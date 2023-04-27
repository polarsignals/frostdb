package physicalplan

import (
	"context"
	"errors"
	"fmt"
	"hash/maphash"
	"strings"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/math"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/apache/arrow/go/v10/arrow/scalar"
	"github.com/dgryski/go-metro"
	"go.opentelemetry.io/otel/trace"

	"github.com/polarsignals/frostdb/pqarrow/arrowutils"
	"github.com/polarsignals/frostdb/pqarrow/builder"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

func Aggregate(
	pool memory.Allocator,
	tracer trace.Tracer,
	agg *logicalplan.Aggregation,
	final bool,
	ordered bool,
) (PhysicalPlan, error) {
	aggregations := make([]Aggregation, 0, len(agg.AggExprs))

	for _, expr := range agg.AggExprs {
		var (
			aggregation    Aggregation
			aggFunc        logicalplan.AggFunc
			aggFuncFound   bool
			aggColumnFound bool
		)
		expr.Accept(PreExprVisitorFunc(func(expr logicalplan.Expr) bool {
			switch e := expr.(type) {
			case *logicalplan.AggregationFunction:
				aggFunc = e.Func
				aggFuncFound = true
			case *logicalplan.Column:
				aggregation.expr = e
				aggColumnFound = true
			}

			return true
		}))

		if !aggFuncFound {
			return nil, errors.New("aggregation function not found")
		}

		if !aggColumnFound {
			return nil, errors.New("aggregation column not found")
		}

		aggregation.resultName = expr.Name()
		aggregation.function = aggFunc

		aggregations = append(aggregations, aggregation)
	}

	if ordered {
		if len(aggregations) > 1 {
			return nil, fmt.Errorf(
				"OrderedAggregate does not support multiple aggregations, found %d", len(aggregations),
			)
		}
		return NewOrderedAggregate(
			pool,
			tracer,
			// TODO(asubiotto): Multiple aggregation functions are not yet
			// supported. The planning code should already have planned a hash
			// aggregation in this case.
			aggregations[0],
			agg.GroupExprs,
			final,
		), nil
	}
	return NewHashAggregate(
		pool,
		tracer,
		aggregations,
		agg.GroupExprs,
		final,
	), nil
}

func chooseAggregationFunction(
	aggFunc logicalplan.AggFunc,
	dataType arrow.DataType,
) (AggregationFunction, error) {
	switch aggFunc {
	case logicalplan.AggFuncSum:
		switch dataType.ID() {
		case arrow.INT64:
			return &Int64SumAggregation{}, nil
		default:
			return nil, fmt.Errorf("unsupported sum of type: %s", dataType.Name())
		}
	case logicalplan.AggFuncMin:
		switch dataType.ID() {
		case arrow.INT64:
			return &Int64MinAggregation{}, nil
		default:
			return nil, fmt.Errorf("unsupported min of type: %s", dataType.Name())
		}
	case logicalplan.AggFuncMax:
		switch dataType.ID() {
		case arrow.INT64:
			return &Int64MaxAggregation{}, nil
		default:
			return nil, fmt.Errorf("unsupported max of type: %s", dataType.Name())
		}
	case logicalplan.AggFuncCount:
		return &CountAggregation{}, nil
	default:
		return nil, fmt.Errorf("unsupported aggregation function: %s", aggFunc.String())
	}
}

// Aggregation groups together some lower level primitives to for the column to be aggregated by its function.
type Aggregation struct {
	expr       logicalplan.Expr
	resultName string
	function   logicalplan.AggFunc
	arrays     []builder.ColumnBuilder // TODO: These can actually live outside this struct and be shared. Only at the very end will they be read by each column and then aggregated separately.
}

type AggregationFunction interface {
	Aggregate(pool memory.Allocator, arrs []arrow.Array) (arrow.Array, error)
}

type HashAggregate struct {
	pool                  memory.Allocator
	tracer                trace.Tracer
	groupByColumnMatchers []logicalplan.Expr
	hashSeed              maphash.Seed
	next                  PhysicalPlan
	// Indicate is this is the last aggregation or
	// if this is a aggregation with another aggregation to follow after synchronizing.
	finalStage bool

	// Buffers that are reused across callback calls.
	groupByFields      []arrow.Field
	groupByFieldHashes []hashCombiner
	groupByArrays      []arrow.Array
	hashToAggregate    map[uint64]hashtuple

	// aggregates are the collection of all the hash aggregates for this hash aggregation. This is useful when a single hash aggregate cannot fit
	// into a single record and needs to be split into multiple records.
	aggregates []*hashAggregate
}

type hashtuple struct {
	aggregate int // aggregate is the index into the aggregates slice
	array     int // array is the index into the aggregations array
}

// hashAggregate represents a single hash aggregation.
type hashAggregate struct {
	aggregations []Aggregation
	groupByCols  map[string]builder.ColumnBuilder

	colOrdering []string
	rowCount    int
}

func NewHashAggregate(
	pool memory.Allocator,
	tracer trace.Tracer,
	aggregations []Aggregation,
	groupByColumnMatchers []logicalplan.Expr,
	finalStage bool,
) *HashAggregate {
	return &HashAggregate{
		pool:   pool,
		tracer: tracer,
		// TODO: Matchers can be optimized to be something like a radix tree or just a fast-lookup datastructure for exact matches or prefix matches.
		groupByColumnMatchers: groupByColumnMatchers,
		hashSeed:              maphash.MakeSeed(),
		finalStage:            finalStage,

		groupByFields:      make([]arrow.Field, 0, 10),
		groupByFieldHashes: make([]hashCombiner, 0, 10),
		groupByArrays:      make([]arrow.Array, 0, 10),
		hashToAggregate:    map[uint64]hashtuple{},
		aggregates: []*hashAggregate{ // initialize a single hash aggregate; we expect this array to only every grow during very large aggregations.
			{
				aggregations: aggregations,
				groupByCols:  map[string]builder.ColumnBuilder{},
				colOrdering:  []string{},
			},
		},
	}
}

func (a *HashAggregate) SetNext(next PhysicalPlan) {
	a.next = next
}

func (a *HashAggregate) Draw() *Diagram {
	var child *Diagram
	if a.next != nil {
		child = a.next.Draw()
	}

	names := make([]string, 0, len(a.aggregates[0].aggregations))
	for _, agg := range a.aggregates[0].aggregations {
		names = append(names, agg.resultName)
	}

	var groupings []string
	for _, grouping := range a.groupByColumnMatchers {
		groupings = append(groupings, grouping.Name())
	}

	details := fmt.Sprintf("HashAggregate (%s by %s)", strings.Join(names, ","), strings.Join(groupings, ","))
	return &Diagram{Details: details, Child: child}
}

// Go translation of boost's hash_combine function. Read here why these values
// are used and good choices: https://stackoverflow.com/questions/35985960/c-why-is-boosthash-combine-the-best-way-to-combine-hash-values
func hashCombine(lhs, rhs uint64) uint64 {
	return lhs ^ (rhs + 0x9e3779b9 + (lhs << 6) + (lhs >> 2))
}

// hashCombiner combines a given hash with another hash that is passed.
type hashCombiner interface {
	hashCombine(rhs uint64) uint64
}

// uint64HashCombine combines a pre-defined uint64 hash with a given uint64 hash.
type uint64HashCombine struct {
	value uint64
}

func (u *uint64HashCombine) hashCombine(rhs uint64) uint64 {
	return hashCombine(u.value, rhs)
}

// durationHashCombine hashes a given timestamp by dividing it through a given duration.
// timestamp | duration | hash
// 0 		 | 2		| 0
// 1 		 | 2		| 0
// 2 		 | 2		| 1
// 3 		 | 2		| 1
// 4 		 | 2		| 2
// 5 		 | 2		| 2
// Essentially hashing timestamps into buckets of durations.
type durationHashCombine struct {
	milliseconds uint64
}

func (d *durationHashCombine) hashCombine(rhs uint64) uint64 {
	return rhs / d.milliseconds // floors by default
}

func hashArray(arr arrow.Array) []uint64 {
	switch ar := arr.(type) {
	case *array.String:
		return hashStringArray(ar)
	case *array.Binary:
		return hashBinaryArray(ar)
	case *array.Int64:
		return hashInt64Array(ar)
	case *array.Boolean:
		return hashBooleanArray(ar)
	case *array.Dictionary:
		return hashDictionaryArray(ar)
	case *array.List:
		return hashListArray(ar)
	default:
		panic("unsupported array type " + fmt.Sprintf("%T", arr))
	}
}

func hashListArray(arr *array.List) []uint64 {
	res := make([]uint64, arr.Len())
	for i := 0; i < arr.Len(); i++ {
		list := []byte{}
		if err := arrowutils.ForEachValueInList(i, arr, func(_ int, v any) {
			switch val := v.(type) {
			case []byte:
				list = append(list, val...)
			}
		}); err != nil {
			panic(err)
		}

		res[i] = metro.Hash64(list, 0)
	}
	return res
}

func hashDictionaryArray(arr *array.Dictionary) []uint64 {
	res := make([]uint64, arr.Len())
	for i := 0; i < arr.Len(); i++ {
		if !arr.IsNull(i) {
			switch dict := arr.Dictionary().(type) {
			case *array.Binary:
				res[i] = metro.Hash64(dict.Value(arr.GetValueIndex(i)), 0)
			case *array.String:
				res[i] = metro.Hash64([]byte(dict.Value(arr.GetValueIndex(i))), 0)
			default:
				panic("unsupported dictionary type " + fmt.Sprintf("%T", dict))
			}
		}
	}
	return res
}

func hashBinaryArray(arr *array.Binary) []uint64 {
	res := make([]uint64, arr.Len())
	for i := 0; i < arr.Len(); i++ {
		if !arr.IsNull(i) {
			res[i] = metro.Hash64(arr.Value(i), 0)
		}
	}
	return res
}

func hashBooleanArray(arr *array.Boolean) []uint64 {
	res := make([]uint64, arr.Len())
	for i := 0; i < arr.Len(); i++ {
		if arr.IsNull(i) {
			res[i] = 0
			continue
		}
		if arr.Value(i) {
			res[i] = 2
		} else {
			res[i] = 1
		}
	}
	return res
}

func hashStringArray(arr *array.String) []uint64 {
	res := make([]uint64, arr.Len())
	for i := 0; i < arr.Len(); i++ {
		if !arr.IsNull(i) {
			res[i] = metro.Hash64([]byte(arr.Value(i)), 0)
		}
	}
	return res
}

func hashInt64Array(arr *array.Int64) []uint64 {
	res := make([]uint64, arr.Len())
	for i := 0; i < arr.Len(); i++ {
		if !arr.IsNull(i) {
			res[i] = uint64(arr.Value(i))
		}
	}
	return res
}

func (a *HashAggregate) Callback(ctx context.Context, r arrow.Record) error {
	// Generates high volume of spans. Comment out if needed during development.
	// ctx, span := a.tracer.Start(ctx, "HashAggregate/Callback")
	// defer span.End()

	// aggregate is the current aggregation
	aggregate := a.aggregates[len(a.aggregates)-1]

	groupByFields := a.groupByFields
	groupByFieldHashes := a.groupByFieldHashes
	groupByArrays := a.groupByArrays

	defer func() {
		groupByFields = groupByFields[:0]
		groupByFieldHashes = groupByFieldHashes[:0]
		groupByArrays = groupByArrays[:0]
	}()

	columnToAggregate := make([]arrow.Array, len(aggregate.aggregations))
	aggregateFieldsFound := 0

	for i, field := range r.Schema().Fields() {
		for _, matcher := range a.groupByColumnMatchers {
			if matcher.MatchColumn(field.Name) {
				groupByFields = append(groupByFields, field)
				groupByArrays = append(groupByArrays, r.Column(i))

				switch v := matcher.(type) {
				case *logicalplan.DurationExpr:
					duration := v.Value()
					groupByFieldHashes = append(groupByFieldHashes,
						&durationHashCombine{milliseconds: uint64(duration.Milliseconds())},
					)
				default:
					groupByFieldHashes = append(groupByFieldHashes,
						&uint64HashCombine{value: scalar.Hash(a.hashSeed, scalar.NewStringScalar(field.Name))},
					)
				}
			}
		}

		for j, col := range aggregate.aggregations {
			// If we're aggregating at the final stage we have previously
			// renamed the pre-aggregated columns to their result names.
			if a.finalStage {
				if col.resultName == field.Name {
					columnToAggregate[j] = r.Column(i)
					aggregateFieldsFound++
				}
			} else {
				// If we're aggregating the raw data we need to find the columns by their actual names for now.
				if col.expr.MatchColumn(field.Name) {
					columnToAggregate[j] = r.Column(i)
					aggregateFieldsFound++
				}
			}
		}
	}

	if aggregateFieldsFound != len(aggregate.aggregations) {
		return errors.New("aggregate field not found, aggregations are not possible without it")
	}

	numRows := int(r.NumRows())

	colHashes := make([][]uint64, len(groupByArrays))
	for i, arr := range groupByArrays {
		colHashes[i] = hashArray(arr)
	}

	for i := 0; i < numRows; i++ {
		hash := uint64(0)
		for j := range colHashes {
			if colHashes[j][i] == 0 {
				continue
			}

			hash = hashCombine(
				hash,
				groupByFieldHashes[j].hashCombine(colHashes[j][i]),
			)
		}

		tuple, ok := a.hashToAggregate[hash]
		if !ok {
			aggregate = a.aggregates[len(a.aggregates)-1]
			for j, col := range columnToAggregate {
				agg := builder.NewBuilder(a.pool, col.DataType())
				aggregate.aggregations[j].arrays = append(aggregate.aggregations[j].arrays, agg)
			}
			tuple = hashtuple{
				aggregate: len(a.aggregates) - 1, // always add new aggregates to the current aggregate
				array:     len(aggregate.aggregations[0].arrays) - 1,
			}
			a.hashToAggregate[hash] = tuple
			aggregate.rowCount++

			// insert new row into columns grouped by and create new aggregate array to append to.
			if err := a.updateGroupByCols(i, groupByArrays, groupByFields); err != nil {
				if !errors.Is(err, builder.ErrMaxSizeReached) {
					return err
				}

				// Max size reached, rollback the aggregation creation and create new aggregate
				aggregate.rowCount--
				for j := range columnToAggregate {
					l := len(aggregate.aggregations[j].arrays)
					aggregate.aggregations[j].arrays = aggregate.aggregations[j].arrays[:l-1]
				}

				// Create new aggregation
				aggregations := make([]Aggregation, 0, len(a.aggregates[0].aggregations))
				for _, agg := range a.aggregates[0].aggregations {
					aggregations = append(aggregations, Aggregation{
						expr:       agg.expr,
						resultName: agg.resultName,
						function:   agg.function,
					})
				}
				a.aggregates = append(a.aggregates, &hashAggregate{
					aggregations: aggregations,
					groupByCols:  map[string]builder.ColumnBuilder{},
					colOrdering:  []string{},
				})

				aggregate = a.aggregates[len(a.aggregates)-1]
				for j, col := range columnToAggregate {
					agg := builder.NewBuilder(a.pool, col.DataType())
					aggregate.aggregations[j].arrays = append(aggregate.aggregations[j].arrays, agg)
				}
				tuple = hashtuple{
					aggregate: len(a.aggregates) - 1, // always add new aggregates to the current aggregate
					array:     len(aggregate.aggregations[0].arrays) - 1,
				}
				a.hashToAggregate[hash] = tuple
				aggregate.rowCount++

				if err := a.updateGroupByCols(i, groupByArrays, groupByFields); err != nil {
					return err
				}
			}
		}

		for j, col := range columnToAggregate {
			if err := builder.AppendValue(a.aggregates[tuple.aggregate].aggregations[j].arrays[tuple.array], col, i); err != nil {
				return err
			}
		}
	}

	return nil
}

func (a *HashAggregate) updateGroupByCols(row int, groupByArrays []arrow.Array, groupByFields []arrow.Field) error {
	// aggregate is the current aggregation
	aggregate := a.aggregates[len(a.aggregates)-1]

	for i, arr := range groupByArrays {
		fieldName := groupByFields[i].Name

		groupByCol, found := aggregate.groupByCols[fieldName]
		if !found {
			groupByCol = builder.NewBuilder(a.pool, groupByFields[i].Type)
			aggregate.groupByCols[fieldName] = groupByCol
			aggregate.colOrdering = append(aggregate.colOrdering, fieldName)
		}

		// We already appended to the arrays to aggregate, so we have
		// to account for that. We only want to back-fill null values
		// up until the index that we are about to insert into.
		for groupByCol.Len() < len(aggregate.aggregations[0].arrays)-1 {
			groupByCol.AppendNull()
		}

		if err := builder.AppendValue(groupByCol, arr, row); err != nil {
			// Rollback
			for j := 0; j < i; j++ {
				if err := builder.RollbackPrevious(aggregate.groupByCols[groupByFields[j].Name]); err != nil {
					return err
				}
			}

			return err
		}
	}
	return nil
}

func (a *HashAggregate) Finish(ctx context.Context) error {
	ctx, span := a.tracer.Start(ctx, "HashAggregate/Finish")
	defer span.End()

	for _, aggregate := range a.aggregates {
		numCols := len(aggregate.groupByCols) + len(aggregate.aggregations)
		numRows := aggregate.rowCount

		if numRows == 0 { // skip empty aggregates
			continue
		}

		groupByFields := make([]arrow.Field, 0, numCols)
		groupByArrays := make([]arrow.Array, 0, numCols)
		for _, fieldName := range aggregate.colOrdering {
			groupByCol, ok := aggregate.groupByCols[fieldName]
			if !ok {
				return fmt.Errorf("unknown field name: %s", fieldName)
			}
			for groupByCol.Len() < numRows {
				// It's possible that columns that are grouped by haven't occurred
				// in all aggregated rows which causes them to not be of equal size
				// as the total number of rows so we need to backfill. This happens
				// for example when there are different sets of dynamic columns in
				// different row-groups of the table.
				groupByCol.AppendNull()
			}
			arr := groupByCol.NewArray()
			groupByFields = append(groupByFields, arrow.Field{Name: fieldName, Type: arr.DataType()})
			groupByArrays = append(groupByArrays, arr)
		}

		// Rename to clarity upon appending aggregations later
		aggregateColumns := groupByArrays
		aggregateFields := groupByFields

		for _, aggregation := range aggregate.aggregations {
			arr := make([]arrow.Array, 0, numRows)
			for _, a := range aggregation.arrays {
				arr = append(arr, a.NewArray())
			}

			aggregateArray, err := runAggregation(a.finalStage, aggregation.function, a.pool, arr)
			if err != nil {
				return fmt.Errorf("aggregate batched arrays: %w", err)
			}
			aggregateColumns = append(aggregateColumns, aggregateArray)

			aggregateFields = append(aggregateFields, arrow.Field{
				Name: aggregation.resultName, Type: aggregateArray.DataType(),
			})
		}

		err := a.next.Callback(ctx, array.NewRecord(
			arrow.NewSchema(aggregateFields, nil),
			aggregateColumns,
			int64(numRows),
		))
		if err != nil {
			return err
		}
	}

	return a.next.Finish(ctx)
}

type Int64SumAggregation struct{}

var ErrUnsupportedSumType = errors.New("unsupported type for sum aggregation, expected int64")

func (a *Int64SumAggregation) Aggregate(pool memory.Allocator, arrs []arrow.Array) (arrow.Array, error) {
	if len(arrs) == 0 {
		return array.NewInt64Builder(pool).NewArray(), nil
	}

	typ := arrs[0].DataType().ID()
	switch typ {
	case arrow.INT64:
		return sumInt64arrays(pool, arrs), nil
	default:
		return nil, fmt.Errorf("sum array of %s: %w", typ, ErrUnsupportedSumType)
	}
}

func sumInt64arrays(pool memory.Allocator, arrs []arrow.Array) arrow.Array {
	res := array.NewInt64Builder(pool)
	for _, arr := range arrs {
		res.Append(sumInt64array(arr.(*array.Int64)))
	}

	return res.NewArray()
}

func sumInt64array(arr *array.Int64) int64 {
	return math.Int64.Sum(arr)
}

var ErrUnsupportedMinType = errors.New("unsupported type for max aggregation, expected int64")

type Int64MinAggregation struct{}

func (a *Int64MinAggregation) Aggregate(pool memory.Allocator, arrs []arrow.Array) (arrow.Array, error) {
	if len(arrs) == 0 {
		return array.NewInt64Builder(pool).NewArray(), nil
	}

	typ := arrs[0].DataType().ID()
	switch typ {
	case arrow.INT64:
		return minInt64arrays(pool, arrs), nil
	default:
		return nil, fmt.Errorf("min array of %s: %w", typ, ErrUnsupportedMinType)
	}
}

func minInt64arrays(pool memory.Allocator, arrs []arrow.Array) arrow.Array {
	res := array.NewInt64Builder(pool)
	for _, arr := range arrs {
		if arr.Len() == 0 {
			res.AppendNull()
			continue
		}
		res.Append(minInt64array(arr.(*array.Int64)))
	}

	return res.NewArray()
}

// minInt64array finds the minimum value in arr. Note that we considered using
// generics for this function, but the runtime doubled in comparison with
// processing a slice of a concrete type.
func minInt64array(arr *array.Int64) int64 {
	// Note that the zero-length check must be performed before calling this
	// function.
	vals := arr.Int64Values()
	min := vals[0]
	for _, v := range vals {
		if v < min {
			min = v
		}
	}
	return min
}

type Int64MaxAggregation struct{}

var ErrUnsupportedMaxType = errors.New("unsupported type for max aggregation, expected int64")

func (a *Int64MaxAggregation) Aggregate(pool memory.Allocator, arrs []arrow.Array) (arrow.Array, error) {
	if len(arrs) == 0 {
		return array.NewInt64Builder(pool).NewArray(), nil
	}

	typ := arrs[0].DataType().ID()
	switch typ {
	case arrow.INT64:
		return maxInt64arrays(pool, arrs), nil
	default:
		return nil, fmt.Errorf("max array of %s: %w", typ, ErrUnsupportedMaxType)
	}
}

func maxInt64arrays(pool memory.Allocator, arrs []arrow.Array) arrow.Array {
	res := array.NewInt64Builder(pool)
	for _, arr := range arrs {
		if arr.Len() == 0 {
			res.AppendNull()
			continue
		}
		res.Append(maxInt64array(arr.(*array.Int64)))
	}

	return res.NewArray()
}

// maxInt64Array finds the maximum value in arr. Note that we considered using
// generics for this function, but the runtime doubled in comparison with
// processing a slice of a concrete type.
func maxInt64array(arr *array.Int64) int64 {
	// Note that the zero-length check must be performed before calling this
	// function.
	vals := arr.Int64Values()
	max := vals[0]
	for _, v := range vals {
		if v > max {
			max = v
		}
	}
	return max
}

type CountAggregation struct{}

func (a *CountAggregation) Aggregate(pool memory.Allocator, arrs []arrow.Array) (arrow.Array, error) {
	if len(arrs) == 0 {
		return array.NewInt64Builder(pool).NewArray(), nil
	}

	res := array.NewInt64Builder(pool)
	for _, arr := range arrs {
		res.Append(int64(arr.Len()))
	}
	return res.NewArray(), nil
}

// runAggregation is a helper to run the given aggregation function given
// the set of values. It is aware of the final stage and chooses the aggregation
// function appropriately.
func runAggregation(finalStage bool, fn logicalplan.AggFunc, pool memory.Allocator, arrs []arrow.Array) (arrow.Array, error) {
	if len(arrs) == 0 {
		return array.NewInt64Builder(pool).NewArray(), nil
	}

	aggFunc, err := chooseAggregationFunction(fn, arrs[0].DataType())
	if err != nil {
		return nil, err
	}

	if _, ok := aggFunc.(*CountAggregation); ok && finalStage {
		// The final stage of aggregation needs to sum up all the counts of the
		// previous steps, instead of counting the previous counts.
		return (&Int64SumAggregation{}).Aggregate(pool, arrs)
	}
	return aggFunc.Aggregate(pool, arrs)
}
