package expr

import (
	"errors"
	"fmt"

	"github.com/parquet-go/parquet-go"

	"github.com/polarsignals/frostdb/query/logicalplan"
)

type ColumnRef struct {
	ColumnName string
}

func (c *ColumnRef) Column(p Particulate) (parquet.ColumnChunk, bool, error) {
	columnIndex := findColumnIndex(p.Schema(), c.ColumnName)
	var columnChunk parquet.ColumnChunk
	// columnChunk can be nil if the column is not present in the row group.
	if columnIndex != -1 {
		columnChunk = p.ColumnChunks()[columnIndex]
	}

	return columnChunk, columnIndex != -1, nil
}

func findColumnIndex(s *parquet.Schema, columnName string) int {
	for i, field := range s.Fields() {
		if field.Name() == columnName {
			return i
		}
	}
	return -1
}

type BinaryScalarExpr struct {
	Left  *ColumnRef
	Op    logicalplan.Op
	Right parquet.Value
}

func (e BinaryScalarExpr) Eval(p Particulate) (bool, error) {
	leftData, exists, err := e.Left.Column(p)
	if err != nil {
		return false, err
	}

	// TODO: This needs a bunch of test cases to validate edge cases like non
	// existant columns or null values. I'm pretty sure this is completely
	// wrong and needs per operation, per type specific behavior.
	if !exists {
		if e.Right.IsNull() {
			switch e.Op {
			case logicalplan.OpEq:
				return true, nil
			case logicalplan.OpNotEq:
				return false, nil
			}
		}

		// only handling string for now.
		if e.Right.Kind() == parquet.ByteArray || e.Right.Kind() == parquet.FixedLenByteArray {
			switch {
			case e.Op == logicalplan.OpEq && e.Right.String() == "":
				return true, nil
			case e.Op == logicalplan.OpNotEq && e.Right.String() != "":
				return true, nil
			}
		}
		return false, nil
	}

	return BinaryScalarOperation(leftData, e.Right, e.Op)
}

var ErrUnsupportedBinaryOperation = errors.New("unsupported binary operation")

// BinaryScalarOperation applies the given operator between the given column
// chunk and value. If BinaryScalarOperation returns true, it means that the
// operator may be satisfied by at least one value in the column chunk. If it
// returns false, it means that the operator will definitely not be satisfied
// by any value in the column chunk.
func BinaryScalarOperation(left parquet.ColumnChunk, right parquet.Value, operator logicalplan.Op) (bool, error) {
	numNulls := NullCount(left)
	fullOfNulls := numNulls == left.NumValues()
	if operator == logicalplan.OpEq {
		if right.IsNull() {
			return numNulls > 0, nil
		}
		if fullOfNulls {
			// Right value is not null and there are no non-null values, so
			// there is definitely not a match.
			return false, nil
		}

		bloomFilter := left.BloomFilter()
		if bloomFilter == nil {
			// If there is no bloom filter then we cannot make a statement about true negative, instead check the min max values of the column chunk
			return compare(right, Max(left)) <= 0 && compare(right, Min(left)) >= 0, nil
		}

		ok, err := bloomFilter.Check(right)
		if err != nil {
			return true, err
		}
		if !ok {
			// Bloom filters may return false positives, but never return false
			// negatives, we know this column chunk does not contain the value.
			return false, nil
		}

		return true, nil
	}

	// If right is NULL automatically return that the column chunk needs further
	// processing. According to SQL semantics we might be able to elide column
	// chunks in these cases since NULL is not comparable to anything else, but
	// play it safe (delegate to execution engine) for now since we shouldn't
	// have many of these cases.
	if right.IsNull() {
		return true, nil
	}

	if numNulls == left.NumValues() {
		// In this case min/max values are meaningless and not comparable to the
		// right expression, so we can automatically discard the column chunk.
		return false, nil
	}

	switch operator {
	case logicalplan.OpLtEq:
		min := Min(left)
		if min.IsNull() {
			// If min is null, we don't know what the non-null min value is, so
			// we need to let the execution engine scan this column chunk
			// further.
			return true, nil
		}
		return compare(min, right) <= 0, nil
	case logicalplan.OpLt:
		min := Min(left)
		if min.IsNull() {
			// If min is null, we don't know what the non-null min value is, so
			// we need to let the execution engine scan this column chunk
			// further.
			return true, nil
		}
		return compare(min, right) < 0, nil
	case logicalplan.OpGt:
		max := Max(left)
		if max.IsNull() {
			// If max is null, we don't know what the non-null max value is, so
			// we need to let the execution engine scan this column chunk
			// further.
			return true, nil
		}
		return compare(max, right) > 0, nil
	case logicalplan.OpGtEq:
		max := Max(left)
		if max.IsNull() {
			// If max is null, we don't know what the non-null max value is, so
			// we need to let the execution engine scan this column chunk
			// further.
			return true, nil
		}
		return compare(max, right) >= 0, nil
	default:
		return true, nil
	}
}

// Min returns the minimum value found in the column chunk across all pages.
func Min(chunk parquet.ColumnChunk) parquet.Value {
	columnIndex := chunk.ColumnIndex()
	min := columnIndex.MinValue(0)
	for i := 1; i < columnIndex.NumPages(); i++ {
		v := columnIndex.MinValue(i)
		if min.IsNull() {
			min = v
			continue
		}

		if compare(min, v) == 1 {
			min = v
		}
	}

	return min
}

func NullCount(chunk parquet.ColumnChunk) int64 {
	columnIndex := chunk.ColumnIndex()
	numNulls := int64(0)
	for i := 0; i < columnIndex.NumPages(); i++ {
		numNulls += columnIndex.NullCount(i)
	}
	return numNulls
}

// Max returns the maximum value found in the column chunk across all pages.
func Max(chunk parquet.ColumnChunk) parquet.Value {
	columnIndex := chunk.ColumnIndex()
	max := columnIndex.MaxValue(0)
	for i := 1; i < columnIndex.NumPages(); i++ {
		v := columnIndex.MaxValue(i)
		if max.IsNull() {
			max = v
			continue
		}

		if compare(max, v) == -1 {
			max = v
		}
	}

	return max
}

// compares two parquet values. 0 if they are equal, -1 if v1 < v2, 1 if v1 > v2.
func compare(v1, v2 parquet.Value) int {
	switch v1.Kind() {
	case parquet.Int32:
		return parquet.Int32Type.Compare(v1, v2)
	case parquet.Int64:
		return parquet.Int64Type.Compare(v1, v2)
	case parquet.Float:
		return parquet.FloatType.Compare(v1, v2)
	case parquet.Double:
		return parquet.DoubleType.Compare(v1, v2)
	case parquet.ByteArray, parquet.FixedLenByteArray:
		return parquet.ByteArrayType.Compare(v1, v2)
	case parquet.Boolean:
		return parquet.BooleanType.Compare(v1, v2)
	default:
		panic(fmt.Sprintf("unsupported value comparison: %v", v1.Kind()))
	}
}
