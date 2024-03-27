package expr

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/scalar"
	"github.com/parquet-go/parquet-go"

	"github.com/polarsignals/frostdb/pqarrow"
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
	Right scalar.Scalar
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
		if !e.Right.IsValid() {
			switch e.Op {
			case logicalplan.OpEq:
				return true, nil
			case logicalplan.OpNotEq:
				return false, nil
			}
		}

		if arrow.IsBaseBinary(e.Right.DataType().ID()) {
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
func BinaryScalarOperation(left parquet.ColumnChunk, right scalar.Scalar, operator logicalplan.Op) (bool, error) {
	leftColumnIndex, err := left.ColumnIndex()
	if err != nil {
		return true, err
	}
	numNulls := NullCount(leftColumnIndex)
	fullOfNulls := numNulls == left.NumValues()
	leftColumnMax := Max(leftColumnIndex)
	leftColumnMin := Min(leftColumnIndex)

	// Because parquet doesn't support some types that arrow does, we need to
	// convert the parquet min/max values to arrow scalars to be able to compare,
	// using the right scalar as the type reference.
	leftColumnMaxScalar, err := pqarrow.ParquetValueToArrowScalar(leftColumnMax, right.DataType())
	if err != nil {
		return false, err
	}
	leftColumnMinScalar, err := pqarrow.ParquetValueToArrowScalar(leftColumnMin, right.DataType())
	if err != nil {
		return false, err
	}

	if operator == logicalplan.OpEq {
		if !right.IsValid() {
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
			return compareArrowScalars(right, leftColumnMaxScalar) <= 0 && compareArrowScalars(right, leftColumnMinScalar) >= 0, nil
		}

		rightParquet, err := pqarrow.ArrowScalarToParquetValue(right)
		if err != nil {
			return false, err
		}
		ok, err := bloomFilter.Check(rightParquet)
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
	if !right.IsValid() {
		return true, nil
	}

	if numNulls == left.NumValues() {
		// In this case min/max values are meaningless and not comparable to the
		// right expression, so we can automatically discard the column chunk.
		return false, nil
	}

	switch operator {
	case logicalplan.OpLtEq:
		min := leftColumnMinScalar
		if !min.IsValid() {
			// If min is null, we don't know what the non-null min value is, so
			// we need to let the execution engine scan this column chunk
			// further.
			return true, nil
		}
		return compareArrowScalars(min, right) <= 0, nil
	case logicalplan.OpLt:
		min := leftColumnMinScalar
		if !min.IsValid() {
			// If min is null, we don't know what the non-null min value is, so
			// we need to let the execution engine scan this column chunk
			// further.
			return true, nil
		}
		return compareArrowScalars(min, right) < 0, nil
	case logicalplan.OpGt:
		max := leftColumnMaxScalar
		if !max.IsValid() {
			// If max is null, we don't know what the non-null max value is, so
			// we need to let the execution engine scan this column chunk
			// further.
			return true, nil
		}
		return compareArrowScalars(max, right) > 0, nil
	case logicalplan.OpGtEq:
		max := leftColumnMaxScalar
		if !max.IsValid() {
			// If max is null, we don't know what the non-null max value is, so
			// we need to let the execution engine scan this column chunk
			// further.
			return true, nil
		}
		return compareArrowScalars(max, right) >= 0, nil
	default:
		return true, nil
	}
}

// Min returns the minimum value found in the column chunk across all pages.
func Min(columnIndex parquet.ColumnIndex) parquet.Value {
	min := columnIndex.MinValue(0)
	for i := 1; i < columnIndex.NumPages(); i++ {
		v := columnIndex.MinValue(i)
		if min.IsNull() {
			min = v
			continue
		}

		if compareParquetValues(min, v) == 1 {
			min = v
		}
	}

	return min
}

func NullCount(columnIndex parquet.ColumnIndex) int64 {
	numNulls := int64(0)
	for i := 0; i < columnIndex.NumPages(); i++ {
		numNulls += columnIndex.NullCount(i)
	}
	return numNulls
}

// Max returns the maximum value found in the column chunk across all pages.
func Max(columnIndex parquet.ColumnIndex) parquet.Value {
	max := columnIndex.MaxValue(0)
	for i := 1; i < columnIndex.NumPages(); i++ {
		v := columnIndex.MaxValue(i)
		if max.IsNull() {
			max = v
			continue
		}

		if compareParquetValues(max, v) == -1 {
			max = v
		}
	}

	return max
}

func compareParquetValues(v1, v2 parquet.Value) int {
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

// compares two parquet values. 0 if they are equal, -1 if v1 < v2, 1 if v1 > v2.
// TODO(metalmatze): Move upstream into arrow to go/arrow/scalar/compareArrowScalar.go
func compareArrowScalars(v1, v2 scalar.Scalar) int {
	if scalar.Equals(v1, v2) {
		return 0
	}

	if !arrow.TypeEqual(v1.DataType(), v2.DataType()) {
		// this should never happen and if it does, it's a bug in your code.
		// check the types before calling this function.
		panic("unsupported scalar value comparison: different data types")
	}

	switch v1.DataType().ID() {
	case arrow.INT8:
		return cmp.Compare[int8](v1.(*scalar.Int8).Value, v2.(*scalar.Int8).Value)
	case arrow.INT16:
		return cmp.Compare[int16](v1.(*scalar.Int16).Value, v2.(*scalar.Int16).Value)
	case arrow.INT32:
		return cmp.Compare[int32](v1.(*scalar.Int32).Value, v2.(*scalar.Int32).Value)
	case arrow.INT64:
		return cmp.Compare[int64](v1.(*scalar.Int64).Value, v2.(*scalar.Int64).Value)
	case arrow.UINT8:
		return cmp.Compare[uint8](v1.(*scalar.Uint8).Value, v2.(*scalar.Uint8).Value)
	case arrow.UINT16:
		return cmp.Compare[uint16](v1.(*scalar.Uint16).Value, v2.(*scalar.Uint16).Value)
	case arrow.UINT32:
		return cmp.Compare[uint32](v1.(*scalar.Uint32).Value, v2.(*scalar.Uint32).Value)
	case arrow.UINT64:
		return cmp.Compare[uint64](v1.(*scalar.Uint64).Value, v2.(*scalar.Uint64).Value)
	case arrow.FLOAT32:
		return cmp.Compare[float32](v1.(*scalar.Float32).Value, v2.(*scalar.Float32).Value)
	case arrow.FLOAT64:
		return cmp.Compare[float64](v1.(*scalar.Float64).Value, v2.(*scalar.Float64).Value)
	case arrow.STRING:
		return bytes.Compare(v1.(*scalar.String).Value.Bytes(), v2.(*scalar.String).Value.Bytes())
	case arrow.BINARY:
		return bytes.Compare(v1.(*scalar.Binary).Value.Bytes(), v2.(*scalar.Binary).Value.Bytes())
	case arrow.BOOL:
		bv1 := v1.(*scalar.Boolean).Value
		bv2 := v2.(*scalar.Boolean).Value
		switch {
		case !bv1 && bv2:
			return -1
		case bv1 && !bv2:
			return +1
		default:
			return 0
		}
	default:
		panic(fmt.Sprintf("unsupported scalar value comparison: %v", v1.DataType()))
	}
}
