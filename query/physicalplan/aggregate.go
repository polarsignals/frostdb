package physicalplan

import (
	"context"
	"errors"
	"fmt"
	"hash/maphash"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/math"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/apache/arrow/go/v8/arrow/scalar"
	"github.com/dgryski/go-metro"
	"github.com/segmentio/parquet-go"
	"go.opentelemetry.io/otel/trace"

	"github.com/polarsignals/frostdb/pqarrow/builder"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

func Aggregate(
	pool memory.Allocator,
	tracer trace.Tracer,
	s *parquet.Schema,
	agg *logicalplan.Aggregation,
	final bool,
) (*HashAggregate, error) {
	aggregateColumns := make([]AggregateColumn, 0, len(agg.AggExprs))

	for _, expr := range agg.AggExprs {
		var (
			aggColumn      AggregateColumn
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
				aggColumn.expr = e
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

		dataType, err := expr.DataType(s)
		if err != nil {
			return nil, err
		}

		f, err := chooseAggregationFunction(aggFunc, dataType)
		if err != nil {
			return nil, err
		}

		aggColumn.resultName = expr.Name()
		aggColumn.function = f

		aggregateColumns = append(aggregateColumns, aggColumn)
	}

	return NewHashAggregate(
		pool,
		tracer,
		aggregateColumns,
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

// AggregateColumn groups together some lower level primitives to aggregate.
type AggregateColumn struct {
	expr       logicalplan.Expr
	resultName string
	function   AggregationFunction
	arrays     []builder.ColumnBuilder // TODO: These can actually live outside this struct and be shared. Only at the very end will they be read by each column and then aggregated separately.
}

type AggregationFunction interface {
	Aggregate(pool memory.Allocator, arrs []arrow.Array) (arrow.Array, error)
}

type HashAggregate struct {
	pool                  memory.Allocator
	tracer                trace.Tracer
	columns               []AggregateColumn
	groupByCols           map[string]builder.ColumnBuilder
	colOrdering           []string
	hashToAggregate       map[uint64]int
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
}

func NewHashAggregate(
	pool memory.Allocator,
	tracer trace.Tracer,
	columns []AggregateColumn,
	groupByColumnMatchers []logicalplan.Expr,
	finalStage bool,
) *HashAggregate {
	return &HashAggregate{
		pool:            pool,
		tracer:          tracer,
		columns:         columns,
		groupByCols:     map[string]builder.ColumnBuilder{},
		colOrdering:     []string{},
		hashToAggregate: map[uint64]int{},
		// TODO: Matchers can be optimized to be something like a radix tree or just a fast-lookup datastructure for exact matches or prefix matches.
		groupByColumnMatchers: groupByColumnMatchers,
		hashSeed:              maphash.MakeSeed(),
		finalStage:            finalStage,

		groupByFields:      make([]arrow.Field, 0, 10),
		groupByFieldHashes: make([]hashCombiner, 0, 10),
		groupByArrays:      make([]arrow.Array, 0, 10),
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

	var groupings []string
	for _, grouping := range a.groupByColumnMatchers {
		groupings = append(groupings, grouping.Name())
	}

	// details := fmt.Sprintf("HashAggregate (%s by %s)", a.columnsToAggregate[0].Name(), strings.Join(groupings, ","))
	details := ""
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
	switch arr.(type) {
	case *array.String:
		return hashStringArray(arr.(*array.String))
	case *array.Binary:
		return hashBinaryArray(arr.(*array.Binary))
	case *array.Int64:
		return hashInt64Array(arr.(*array.Int64))
	case *array.Boolean:
		return hashBooleanArray(arr.(*array.Boolean))
	default:
		panic("unsupported array type " + fmt.Sprintf("%T", arr))
	}
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

	groupByFields := a.groupByFields
	groupByFieldHashes := a.groupByFieldHashes
	groupByArrays := a.groupByArrays

	defer func() {
		groupByFields = groupByFields[:0]
		groupByFieldHashes = groupByFieldHashes[:0]
		groupByArrays = groupByArrays[:0]
	}()

	columnToAggregate := make([]arrow.Array, len(a.columns))
	aggregateFieldFound := make([]bool, len(a.columns))

	for i, field := range r.Schema().Fields() {
		for _, matcher := range a.groupByColumnMatchers {
			if matcher.MatchColumn(field.Name) {
				groupByFields = append(groupByFields, field)
				groupByArrays = append(groupByArrays, r.Column(i))

				switch matcher.(type) {
				case *logicalplan.DurationExpr:
					duration := matcher.(*logicalplan.DurationExpr).Value()
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

		for j, col := range a.columns {
			if a.finalStage {
				if col.resultName == field.Name {
					columnToAggregate[j] = r.Column(i)
					aggregateFieldFound[j] = true
				}
			} else {
				if col.expr.MatchColumn(field.Name) {
					columnToAggregate[j] = r.Column(i)
					aggregateFieldFound[j] = true
				}
			}
		}
	}

	for _, found := range aggregateFieldFound {
		if !found {
			return errors.New("aggregate field not found, aggregations are not possible without it")
		}
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

		k, ok := a.hashToAggregate[hash]
		if !ok {
			for j, col := range columnToAggregate {
				agg := builder.NewBuilder(a.pool, col.DataType())
				a.columns[j].arrays = append(a.columns[j].arrays, agg)
			}
			k = len(a.columns[0].arrays) - 1
			a.hashToAggregate[hash] = k

			// insert new row into columns grouped by and create new aggregate array to append to.
			for j, arr := range groupByArrays {
				fieldName := groupByFields[j].Name

				groupByCol, found := a.groupByCols[fieldName]
				if !found {
					groupByCol = builder.NewBuilder(a.pool, groupByFields[j].Type)
					a.groupByCols[fieldName] = groupByCol
					a.colOrdering = append(a.colOrdering, fieldName)
				}

				// We already appended to the arrays to aggregate, so we have
				// to account for that. We only want to back-fill null values
				// up until the index that we are about to insert into.
				for groupByCol.Len() < len(a.columns[0].arrays)-1 {
					groupByCol.AppendNull()
				}

				err := builder.AppendValue(groupByCol, arr, i)
				if err != nil {
					return err
				}
			}
		}

		for j, col := range columnToAggregate {
			if err := builder.AppendValue(a.columns[j].arrays[k], col, i); err != nil {
				return err
			}
		}
	}

	return nil
}

func (a *HashAggregate) Finish(ctx context.Context) error {
	ctx, span := a.tracer.Start(ctx, "HashAggregate/Finish")
	defer span.End()

	numCols := len(a.groupByCols) + 1
	numRows := len(a.columns[0].arrays)

	groupByFields := make([]arrow.Field, 0, numCols)
	groupByArrays := make([]arrow.Array, 0, numCols)
	for _, fieldName := range a.colOrdering {
		groupByCol, ok := a.groupByCols[fieldName]
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

	aggregateColumns := append(groupByArrays)
	aggregateFields := append(groupByFields)

	for _, column := range a.columns {
		var (
			aggregateArray arrow.Array
			err            error
		)

		arr := make([]arrow.Array, 0, numRows)
		for _, a := range column.arrays {
			arr = append(arr, a.NewArray())
		}

		switch column.function.(type) {
		case *CountAggregation:
			if a.finalStage {
				// The final stage of aggregation needs to sum up all the counts of the previous steps,
				// instead of counting the previous counts.
				aggregateArray, err = (&Int64SumAggregation{}).Aggregate(a.pool, arr)
			} else {
				// If this isn't the final stage we simply run the count aggregation.
				aggregateArray, err = column.function.Aggregate(a.pool, arr)
			}
		default:
			aggregateArray, err = column.function.Aggregate(a.pool, arr)
		}
		if err != nil {
			return fmt.Errorf("aggregate batched arrays: %w", err)
		}
		aggregateColumns = append(aggregateColumns, aggregateArray)

		aggregateFields = append(aggregateFields, arrow.Field{
			Name: column.resultName, Type: aggregateArray.DataType(),
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
		return nil, fmt.Errorf("sum array of %s: %w", typ, ErrUnsupportedSumType)
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
