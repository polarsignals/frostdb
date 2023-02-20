package frostdb

import (
	"errors"
	"fmt"

	"github.com/segmentio/parquet-go"

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

func BinaryScalarOperation(left parquet.ColumnChunk, right parquet.Value, operator logicalplan.Op) (bool, error) {
	switch operator {
	case logicalplan.OpEq:
		if right == parquet.NullValue() {
			// Assume all ColumnChunk have NULLs for now.
			// They will be read and added to a bitmap later on.
			// TODO: Maybe there's a nice way of reading the NumNulls from the Pages, for me they always return 0
			return true, nil
		}

		bloomFilter := left.BloomFilter()
		if bloomFilter == nil {
			// If there is no bloom filter then we cannot make a statement about true negative, instead check the min max values of the column chunk
			return compare(right, Max(left)) <= 0 || compare(right, Min(left)) >= -1, nil
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
	case logicalplan.OpLtEq:
		return compare(Min(left), right) <= 0, nil
	case logicalplan.OpLt:
		return compare(Min(left), right) < 0, nil
	case logicalplan.OpGt:
		return compare(Max(left), right) > 0, nil
	case logicalplan.OpGtEq:
		return compare(Max(left), right) >= 0, nil
	default:
		return true, nil
	}
}

// Min returns the minimum value found in the column chunk across all pages.
func Min(chunk parquet.ColumnChunk) parquet.Value {
	columnIndex := chunk.ColumnIndex()
	min := columnIndex.MinValue(0)
	for i := 1; i < columnIndex.NumPages(); i++ {
		if v := columnIndex.MinValue(i); compare(min, v) == 1 {
			min = v
		}
	}

	return min
}

// Max returns the maximum value found in the column chunk across all pages.
func Max(chunk parquet.ColumnChunk) parquet.Value {
	columnIndex := chunk.ColumnIndex()
	max := columnIndex.MaxValue(0)
	for i := 1; i < columnIndex.NumPages(); i++ {
		if v := columnIndex.MaxValue(i); compare(max, v) == -1 {
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
