package physicalplan

import (
	"context"
	"errors"
	"fmt"
	"hash/maphash"
	"strings"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/math"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/arrow/scalar"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/pqarrow/builder"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

func Aggregate(
	pool memory.Allocator,
	tracer trace.Tracer,
	agg *logicalplan.Aggregation,
	final bool,
	ordered bool,
	seed maphash.Seed,
) (PhysicalPlan, error) {
	aggregations := make([]Aggregation, 0, len(agg.AggExprs))

	// TODO(brancz): This is not correct, it doesn't handle aggregations
	// correctly of previously projected columns like `sum(value + timestamp)`.
	// Need to understand why we need to handle dynamic columns here
	// differently and not just use the aggregation funciton's expression.
	for _, expr := range agg.AggExprs {
		aggregation := Aggregation{}
		expr.Accept(PreExprVisitorFunc(func(expr logicalplan.Expr) bool {
			if _, ok := expr.(*logicalplan.DynamicColumn); ok {
				aggregation.dynamic = true
			}

			return true
		}))

		aggregation.resultName = expr.Name()
		aggregation.function = expr.Func
		aggregation.expr = expr.Expr

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
		seed,
		final,
	), nil
}

func chooseAggregationFunction(
	aggFunc logicalplan.AggFunc,
	_ arrow.DataType,
) (AggregationFunction, error) {
	switch aggFunc {
	case logicalplan.AggFuncSum:
		return &SumAggregation{}, nil
	case logicalplan.AggFuncMin:
		return &MinAggregation{}, nil
	case logicalplan.AggFuncMax:
		return &MaxAggregation{}, nil
	case logicalplan.AggFuncCount:
		return &CountAggregation{}, nil
	default:
		return nil, fmt.Errorf("unsupported aggregation function: %s", aggFunc.String())
	}
}

// Aggregation groups together some lower level primitives to for the column to be aggregated by its function.
type Aggregation struct {
	expr       logicalplan.Expr
	dynamic    bool // dynamic indicates that this aggregation is performed against a dynamic column.
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
	dynamicAggregations []Aggregation
	// dynamicFieldsConverted tracks the fields that match with
	// dynamicAggregations and have been converted to aggregations on a concrete
	// column.
	dynamicAggregationsConverted map[string]struct{}
	aggregations                 []Aggregation
	// concreteAggregations memoizes the number of concrete aggregations at
	// initialization this number needs to be recorded because dynamic
	// aggregations are converted to concrete aggregations at runtime.
	concreteAggregations int
	groupByCols          map[string]builder.ColumnBuilder

	colOrdering []string
	rowCount    int
}

func NewHashAggregate(
	pool memory.Allocator,
	tracer trace.Tracer,
	aggregations []Aggregation,
	groupByColumnMatchers []logicalplan.Expr,
	seed maphash.Seed,
	finalStage bool,
) *HashAggregate {
	dynamic := []Aggregation{}
	static := []Aggregation{}
	for _, agg := range aggregations {
		if agg.dynamic {
			dynamic = append(dynamic, agg)
		} else {
			static = append(static, agg)
		}
	}

	return &HashAggregate{
		pool:   pool,
		tracer: tracer,
		// TODO: Matchers can be optimized to be something like a radix tree or just a fast-lookup datastructure for exact matches or prefix matches.
		groupByColumnMatchers: groupByColumnMatchers,
		hashSeed:              seed,
		finalStage:            finalStage,

		groupByFields:      make([]arrow.Field, 0, 10),
		groupByFieldHashes: make([]hashCombiner, 0, 10),
		groupByArrays:      make([]arrow.Array, 0, 10),
		hashToAggregate:    map[uint64]hashtuple{},
		aggregates: []*hashAggregate{ // initialize a single hash aggregate; we expect this array to only every grow during very large aggregations.
			{
				dynamicAggregations:          dynamic,
				dynamicAggregationsConverted: make(map[string]struct{}),
				aggregations:                 static,
				concreteAggregations:         len(static),
				groupByCols:                  map[string]builder.ColumnBuilder{},
				colOrdering:                  []string{},
			},
		},
	}
}

func (a *HashAggregate) Close() {
	for _, arr := range a.groupByArrays {
		arr.Release()
	}
	for _, aggregate := range a.aggregates {
		for _, aggregation := range aggregate.aggregations {
			for _, bldr := range aggregation.arrays {
				bldr.Release()
			}
		}
		for _, bldr := range aggregate.groupByCols {
			bldr.Release()
		}
	}
	a.next.Close()
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
		groupings = append(groupings, grouping.String())
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

func (a *HashAggregate) Callback(_ context.Context, r arrow.Record) error {
	// Generates high volume of spans. Comment out if needed during development.
	// ctx, span := a.tracer.Start(ctx, "HashAggregate/Callback")
	// defer span.End()

	// aggregate is the current aggregation
	aggregate := a.aggregates[len(a.aggregates)-1]

	fields := r.Schema().Fields() // NOTE: call Fields() once to avoid creating a copy each time
	groupByFields := a.groupByFields
	groupByFieldHashes := a.groupByFieldHashes
	groupByArrays := a.groupByArrays

	defer func() {
		groupByFields = groupByFields[:0]
		groupByFieldHashes = groupByFieldHashes[:0]
		groupByArrays = groupByArrays[:0]
	}()

	columnToAggregate := make([]arrow.Array, len(aggregate.aggregations))
	concreteAggregateFieldsFound := 0
	dynamicAggregateFieldsFound := 0

	for i := 0; i < r.Schema().NumFields(); i++ {
		field := r.Schema().Field(i)
		for _, matcher := range a.groupByColumnMatchers {
			if matcher.MatchColumn(field.Name) {
				groupByFields = append(groupByFields, field)
				groupByArrays = append(groupByArrays, r.Column(i))

				if a.finalStage { // in the final stage expect the hashes to already exist, so only need to combine them as normal hashes
					groupByFieldHashes = append(groupByFieldHashes,
						&uint64HashCombine{value: scalar.Hash(a.hashSeed, scalar.NewStringScalar(field.Name))},
					)
					continue
				}

				groupByFieldHashes = append(groupByFieldHashes,
					&uint64HashCombine{value: scalar.Hash(a.hashSeed, scalar.NewStringScalar(field.Name))},
				)
			}
		}

		if _, ok := aggregate.dynamicAggregationsConverted[field.Name]; !ok {
			for _, col := range aggregate.dynamicAggregations {
				if a.finalStage {
					if col.expr.MatchColumn(field.Name) {
						// expand the aggregate.aggregations with a final concrete column aggregation.
						columnToAggregate = append(columnToAggregate, nil)
						aggregate.aggregations = append(aggregate.aggregations, Aggregation{
							expr:       logicalplan.Col(field.Name),
							dynamic:    true,
							resultName: resultNameWithConcreteColumn(col.function, field.Name),
							function:   col.function,
						})
						aggregate.dynamicAggregationsConverted[field.Name] = struct{}{}
					}
				} else {
					// If we're aggregating the raw data we need to find the columns by their actual names for now.
					if col.expr.MatchColumn(field.Name) {
						// expand the aggregate.aggregations with a concrete column aggregation.
						columnToAggregate = append(columnToAggregate, nil)
						aggregate.aggregations = append(aggregate.aggregations, Aggregation{
							expr:       logicalplan.Col(field.Name),
							dynamic:    true,
							resultName: field.Name, // Don't rename the column yet, we'll do that in the final stage. Dynamic aggregations can't match agains't the pre-computed name.
							function:   col.function,
						})
						aggregate.dynamicAggregationsConverted[field.Name] = struct{}{}
					}
				}
			}
		}

		for j, col := range aggregate.aggregations {
			// If we're aggregating at the final stage we have previously
			// renamed the pre-aggregated columns to their result names.
			if a.finalStage {
				if col.resultName == field.Name || (col.dynamic && col.expr.MatchColumn(field.Name)) {
					columnToAggregate[j] = r.Column(i)
					if col.dynamic {
						dynamicAggregateFieldsFound++
					} else {
						concreteAggregateFieldsFound++
					}
				}
			} else {
				// If we're aggregating the raw data we need to find the columns by their actual names for now.
				if col.expr.MatchColumn(field.Name) {
					columnToAggregate[j] = r.Column(i)
					if col.dynamic {
						dynamicAggregateFieldsFound++
					} else {
						concreteAggregateFieldsFound++
					}
				}
			}
		}
	}

	// It's ok for the same aggregation to be found multiple times, optimizers
	// should remove them but for correctness in the case where they don't we
	// need to handle it, so concrete aggregates are allowed to be different
	// from concrete aggregations.
	if ((concreteAggregateFieldsFound == 0 || aggregate.concreteAggregations == 0) && (len(aggregate.dynamicAggregations) == 0)) ||
		(len(aggregate.dynamicAggregations) > 0) && dynamicAggregateFieldsFound == 0 {
		// To perform an aggregation ALL concrete columns must have been matched
		// or at least one dynamic column if performing dynamic aggregations.
		exprs := make([]string, len(aggregate.aggregations))
		for i, col := range aggregate.aggregations {
			exprs[i] = col.expr.String()
		}

		if a.finalStage {
			return fmt.Errorf("aggregate field(s) not found %#v, final aggregations are not possible without it (%d concrete aggregation fields found; %d concrete aggregations)", exprs, concreteAggregateFieldsFound, aggregate.concreteAggregations)
		} else {
			return fmt.Errorf("aggregate field(s) not found %#v, aggregations are not possible without it (%d concrete aggregation fields found; %d concrete aggregations)", exprs, concreteAggregateFieldsFound, aggregate.concreteAggregations)
		}
	}

	numRows := int(r.NumRows())

	colHashes := make([][]uint64, len(groupByArrays))
	for i, arr := range groupByArrays {
		col := dynparquet.FindHashedColumn(groupByFields[i].Name, fields)
		if col != -1 {
			vals := make([]uint64, 0, numRows)
			for _, v := range r.Column(col).(*array.Int64).Int64Values() {
				vals = append(vals, uint64(v))
			}
			colHashes[i] = vals
		} else {
			colHashes[i] = dynparquet.HashArray(arr)
		}
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
			if col == nil {
				// This is a dynamic aggregation that had no match.
				continue
			}
			if a.aggregates[tuple.aggregate].aggregations[j].arrays == nil {
				// This can happen with dynamic column aggregations without
				// groupings. The group exists, but the array to append to does
				// not.
				agg := builder.NewBuilder(a.pool, col.DataType())
				aggregate.aggregations[j].arrays = append(aggregate.aggregations[j].arrays, agg)
			}
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
	span.SetAttributes(attribute.Bool("finalStage", a.finalStage))
	defer span.End()

	totalRows := 0
	for i, aggregate := range a.aggregates {
		if err := a.finishAggregate(ctx, i, aggregate); err != nil {
			return err
		}
		totalRows += aggregate.rowCount
	}
	span.SetAttributes(attribute.Int64("rows", int64(totalRows)))
	return a.next.Finish(ctx)
}

func (a *HashAggregate) finishAggregate(ctx context.Context, aggIdx int, aggregate *hashAggregate) error {
	numCols := len(aggregate.groupByCols) + len(aggregate.aggregations)
	numRows := aggregate.rowCount

	if numRows == 0 { // skip empty aggregates
		return nil
	}

	groupByFields := make([]arrow.Field, 0, numCols)
	groupByArrays := make([]arrow.Array, 0, numCols)
	defer func() {
		for _, arr := range groupByArrays {
			if arr != nil {
				arr.Release()
			}
		}
	}()
	for _, fieldName := range aggregate.colOrdering {
		if a.finalStage && dynparquet.IsHashedColumn(fieldName) {
			continue
		}
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
		// Pass forward the hashings of the group-by columns
		if !a.finalStage {
			groupByFields = append(groupByFields, arrow.Field{Name: dynparquet.HashedColumnName(fieldName), Type: arrow.PrimitiveTypes.Int64})
			func() {
				bldr := array.NewInt64Builder(a.pool)
				defer bldr.Release()
				sortedHashes := make([]int64, arr.Len())
				for hash, tuple := range a.hashToAggregate {
					if tuple.aggregate == aggIdx { // only append the hash for the current aggregate
						sortedHashes[tuple.array] = int64(hash)
					}
				}
				bldr.AppendValues(sortedHashes, nil)
				groupByArrays = append(groupByArrays, bldr.NewArray())
			}()
		}
	}

	// Rename to clarity upon appending aggregations later
	aggregateFields := groupByFields

	for _, aggregation := range aggregate.aggregations {
		arr := make([]arrow.Array, 0, numRows)
		for _, a := range aggregation.arrays {
			arr = append(arr, a.NewArray())
		}

		aggregateArray, err := runAggregation(a.finalStage, aggregation.function, a.pool, arr)
		for _, a := range arr {
			a.Release()
		}
		if err != nil {
			return fmt.Errorf("aggregate batched arrays: %w", err)
		}
		groupByArrays = append(groupByArrays, aggregateArray)

		aggregateFields = append(aggregateFields, arrow.Field{
			Name: aggregation.resultName, Type: aggregateArray.DataType(),
		})
	}

	r := array.NewRecord(
		arrow.NewSchema(aggregateFields, nil),
		groupByArrays,
		int64(numRows),
	)
	defer r.Release()
	err := a.next.Callback(ctx, r)
	if err != nil {
		return err
	}

	return nil
}

type SumAggregation struct{}

var ErrUnsupportedSumType = errors.New("unsupported type for sum aggregation, expected int64 or float64")

func (a *SumAggregation) Aggregate(pool memory.Allocator, arrs []arrow.Array) (arrow.Array, error) {
	if len(arrs) == 0 {
		return array.NewInt64Builder(pool).NewArray(), nil
	}

	typ := arrs[0].DataType().ID()
	switch typ {
	case arrow.INT64:
		return sumInt64arrays(pool, arrs), nil
	case arrow.FLOAT64:
		return sumFloat64arrays(pool, arrs), nil
	default:
		return nil, fmt.Errorf("sum array of %s: %w", typ, ErrUnsupportedSumType)
	}
}

func sumInt64arrays(pool memory.Allocator, arrs []arrow.Array) arrow.Array {
	res := array.NewInt64Builder(pool)
	defer res.Release()
	for _, arr := range arrs {
		res.Append(sumInt64array(arr.(*array.Int64)))
	}

	return res.NewArray()
}

func sumInt64array(arr *array.Int64) int64 {
	return math.Int64.Sum(arr)
}

func sumFloat64arrays(pool memory.Allocator, arrs []arrow.Array) arrow.Array {
	res := array.NewFloat64Builder(pool)
	defer res.Release()
	for _, arr := range arrs {
		res.Append(sumFloat64array(arr.(*array.Float64)))
	}

	return res.NewArray()
}

func sumFloat64array(arr *array.Float64) float64 {
	return math.Float64.Sum(arr)
}

var ErrUnsupportedMinType = errors.New("unsupported type for max aggregation, expected int64 or float64")

type MinAggregation struct{}

func (a *MinAggregation) Aggregate(pool memory.Allocator, arrs []arrow.Array) (arrow.Array, error) {
	if len(arrs) == 0 {
		return array.NewInt64Builder(pool).NewArray(), nil
	}

	typ := arrs[0].DataType().ID()
	switch typ {
	case arrow.INT64:
		return minInt64arrays(pool, arrs), nil
	case arrow.FLOAT64:
		return minFloat64arrays(pool, arrs), nil
	default:
		return nil, fmt.Errorf("min array of %s: %w", typ, ErrUnsupportedMinType)
	}
}

func minInt64arrays(pool memory.Allocator, arrs []arrow.Array) arrow.Array {
	res := array.NewInt64Builder(pool)
	defer res.Release()
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

func minFloat64arrays(pool memory.Allocator, arrs []arrow.Array) arrow.Array {
	res := array.NewFloat64Builder(pool)
	defer res.Release()
	for _, arr := range arrs {
		if arr.Len() == 0 {
			res.AppendNull()
			continue
		}
		res.Append(minFloat64array(arr.(*array.Float64)))
	}

	return res.NewArray()
}

// Same as minInt64array but for Float64.
func minFloat64array(arr *array.Float64) float64 {
	// Note that the zero-length check must be performed before calling this
	// function.
	vals := arr.Float64Values()
	min := vals[0]
	for _, v := range vals {
		if v < min {
			min = v
		}
	}
	return min
}

type MaxAggregation struct{}

var ErrUnsupportedMaxType = errors.New("unsupported type for max aggregation, expected int64 or float64")

func (a *MaxAggregation) Aggregate(pool memory.Allocator, arrs []arrow.Array) (arrow.Array, error) {
	if len(arrs) == 0 {
		return array.NewInt64Builder(pool).NewArray(), nil
	}

	typ := arrs[0].DataType().ID()
	switch typ {
	case arrow.INT64:
		return maxInt64arrays(pool, arrs), nil
	case arrow.FLOAT64:
		return maxFloat64arrays(pool, arrs), nil
	default:
		return nil, fmt.Errorf("max array of %s: %w", typ, ErrUnsupportedMaxType)
	}
}

func maxInt64arrays(pool memory.Allocator, arrs []arrow.Array) arrow.Array {
	res := array.NewInt64Builder(pool)
	defer res.Release()
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

func maxFloat64arrays(pool memory.Allocator, arrs []arrow.Array) arrow.Array {
	res := array.NewFloat64Builder(pool)
	defer res.Release()
	for _, arr := range arrs {
		if arr.Len() == 0 {
			res.AppendNull()
			continue
		}
		res.Append(maxFloat64array(arr.(*array.Float64)))
	}

	return res.NewArray()
}

func maxFloat64array(arr *array.Float64) float64 {
	// Note that the zero-length check must be performed before calling this
	// function.
	vals := arr.Float64Values()
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
	defer res.Release()
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
		return (&SumAggregation{}).Aggregate(pool, arrs)
	}
	return aggFunc.Aggregate(pool, arrs)
}

func resultNameWithConcreteColumn(function logicalplan.AggFunc, col string) string {
	switch function {
	case logicalplan.AggFuncSum:
		return logicalplan.Sum(logicalplan.Col(col)).Name()
	case logicalplan.AggFuncMin:
		return logicalplan.Min(logicalplan.Col(col)).Name()
	case logicalplan.AggFuncMax:
		return logicalplan.Max(logicalplan.Col(col)).Name()
	case logicalplan.AggFuncCount:
		return logicalplan.Count(logicalplan.Col(col)).Name()
	case logicalplan.AggFuncAvg:
		return logicalplan.Avg(logicalplan.Col(col)).Name()
	default:
		return ""
	}
}
