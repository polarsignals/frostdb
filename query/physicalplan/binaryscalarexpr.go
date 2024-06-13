package physicalplan

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"unsafe"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/compute"
	"github.com/apache/arrow/go/v16/arrow/scalar"
	"github.com/parquet-go/parquet-go"

	"github.com/polarsignals/frostdb/query/logicalplan"
)

type ArrayRef struct {
	ColumnName string
}

func (a *ArrayRef) ColumnChunk(rg parquet.RowGroup) (parquet.ColumnChunk, int, bool) {
	leaf, ok := rg.Schema().Lookup(a.ColumnName)
	if !ok {
		return nil, -1, false
	}

	return rg.ColumnChunks()[leaf.ColumnIndex], leaf.ColumnIndex, true
}

func (a *ArrayRef) ArrowArray(r arrow.Record) (arrow.Array, bool, error) {
	fields := r.Schema().FieldIndices(a.ColumnName)
	if len(fields) != 1 {
		return nil, false, nil
	}

	return r.Column(fields[0]), true, nil
}

func (a *ArrayRef) String() string {
	return a.ColumnName
}

type BinaryScalarExpr struct {
	Left  *ArrayRef
	Op    logicalplan.Op
	Right scalar.Scalar
}

func (e BinaryScalarExpr) EvalParquet(rg parquet.RowGroup, in [][]parquet.Value) (*Bitmap, [][]parquet.Value, error) {
	leftData, index, exists := e.Left.ColumnChunk(rg)

	if !exists {
		res := NewBitmap()
		switch e.Op {
		case logicalplan.OpEq:
			if e.Right.IsValid() { // missing column; looking for == non-nil
				switch t := e.Right.(type) {
				case *scalar.Binary:
					if t.String() != "" { // treat empty string equivalent to nil
						return res, in, nil
					}
				case *scalar.String:
					if t.String() != "" { // treat empty string equivalent to nil
						return res, in, nil
					}
				}
			}
		case logicalplan.OpNotEq: // missing column; looking for != nil
			if !e.Right.IsValid() {
				return res, in, nil
			}
		case logicalplan.OpLt, logicalplan.OpLtEq, logicalplan.OpGt, logicalplan.OpGtEq:
			return res, in, nil
		}

		res.AddRange(0, uint64(rg.NumRows()))
		return res, in, nil
	}

	// Reuse the input slice if it's already been allocated
	var buf []parquet.Value
	if len(in) != 0 {
		if len(in[index]) < int(leftData.NumValues()) {
			in[index] = make([]parquet.Value, leftData.NumValues())
			buf = in[index]
		}
	} else {
		buf = make([]parquet.Value, leftData.NumValues())
	}
	bm, col, err := BinaryScalarParquetOperation(leftData, e.Right, e.Op, buf)
	if err != nil {
		return nil, nil, err
	}

	if len(in) != 0 {
		in[index] = col
	}
	return bm, in, nil
}

func BinaryScalarParquetOperation(left parquet.ColumnChunk, right scalar.Scalar, operator logicalplan.Op, scratch []parquet.Value) (*Bitmap, []parquet.Value, error) {
	bm := NewBitmap()
	switch operator { // TODO(optimize): Use the bloom filter or index to speed up the operation for pages with no matching values
	case logicalplan.OpContains, logicalplan.OpNotContains:
		var r []byte
		switch s := right.(type) {
		case *scalar.Binary:
			r = s.Data()
		case *scalar.String:
			r = s.Data()
		}

		col, err := forEachParquetValue(left, scratch, func(i int, value parquet.Value) error {
			if value.IsNull() {
				return nil
			}
			contains := bytes.Contains(value.Bytes(), r)
			if contains && operator == logicalplan.OpContains || !contains && operator == logicalplan.OpNotContains {
				bm.AddInt(i)
			}
			return nil
		})
		return bm, col, err
	case logicalplan.OpEq:
		col, err := forEachParquetValue(left, scratch, func(i int, value parquet.Value) error {
			if !right.IsValid() {
				if value.IsNull() {
					bm.AddInt(i)
				}
				return nil
			}
			if value.IsNull() {
				return nil
			}
			if ParquetValueCompareArrowScalar(value, right) == 0 {
				bm.AddInt(i)
			}
			return nil
		})
		return bm, col, err
	case logicalplan.OpNotEq:
		col, err := forEachParquetValue(left, scratch, func(i int, value parquet.Value) error {
			if !right.IsValid() {
				if !value.IsNull() {
					bm.AddInt(i)
				}
				return nil
			}
			if value.IsNull() {
				return nil
			}
			if ParquetValueCompareArrowScalar(value, right) != 0 {
				bm.AddInt(i)
			}
			return nil
		})
		return bm, col, err
	case logicalplan.OpLt:
		col, err := forEachParquetValue(left, scratch, func(i int, value parquet.Value) error {
			if value.IsNull() {
				return nil
			}
			if ParquetValueCompareArrowScalar(value, right) < 0 {
				bm.AddInt(i)
			}
			return nil
		})
		return bm, col, err
	case logicalplan.OpLtEq:
		col, err := forEachParquetValue(left, scratch, func(i int, value parquet.Value) error {
			if value.IsNull() {
				return nil
			}
			if ParquetValueCompareArrowScalar(value, right) <= 0 {
				bm.AddInt(i)
			}
			return nil
		})
		return bm, col, err
	case logicalplan.OpGt:
		col, err := forEachParquetValue(left, scratch, func(i int, value parquet.Value) error {
			if value.IsNull() {
				return nil
			}
			if ParquetValueCompareArrowScalar(value, right) > 0 {
				bm.AddInt(i)
			}
			return nil
		})
		return bm, col, err
	case logicalplan.OpGtEq:
		col, err := forEachParquetValue(left, scratch, func(i int, value parquet.Value) error {
			if value.IsNull() {
				return nil
			}
			if ParquetValueCompareArrowScalar(value, right) >= 0 {
				bm.AddInt(i)
			}
			return nil
		})
		return bm, col, err
	case logicalplan.OpRegexMatch, logicalplan.OpRegexNotMatch:
		col, err := forEachParquetValue(left, scratch, func(i int, value parquet.Value) error {
			if value.IsNull() {
				return nil
			}
			match, err := regexp.MatchString(right.String(), value.String())
			if err != nil {
				return err
			}
			if match && operator == logicalplan.OpRegexMatch || !match && operator == logicalplan.OpRegexNotMatch {
				bm.AddInt(i)
			}
			return nil
		})
		return bm, col, err
	default:
		return nil, nil, fmt.Errorf("unsupported operator: %v", operator)
	}
}

// ParquetValueCompareArrowScalar compares a parquet.Value to a scalar.Scalar
// It returns 0 if they are equal, -1 if the parquet.Value is less than the scalar.Scalar, and 1 if the parquet.Value is greater than the scalar.Scalar.
func ParquetValueCompareArrowScalar(v parquet.Value, s scalar.Scalar) int {
	switch v.Kind() {
	case parquet.Boolean:
		if v.Boolean() == s.(*scalar.Boolean).Value {
			return 0
		} else if v.Boolean() {
			return 1
		} else {
			return -1
		}
	case parquet.Int32:
		if v.Int32() == s.(*scalar.Int32).Value {
			return 0
		} else if v.Int32() > s.(*scalar.Int32).Value {
			return 1
		} else {
			return -1
		}
	case parquet.Int64:
		if v.Int64() == s.(*scalar.Int64).Value {
			return 0
		} else if v.Int64() > s.(*scalar.Int64).Value {
			return 1
		} else {
			return -1
		}
	case parquet.Float:
		if v.Float() == s.(*scalar.Float32).Value {
			return 0
		} else if v.Float() > s.(*scalar.Float32).Value {
			return 1
		} else {
			return -1
		}
	case parquet.Double:
		if v.Double() == s.(*scalar.Float64).Value {
			return 0
		} else if v.Double() > s.(*scalar.Float64).Value {
			return 1
		} else {
			return -1
		}
	case parquet.ByteArray:
		if string(v.ByteArray()) == s.(*scalar.String).String() {
			return 0
		} else if string(v.ByteArray()) > s.(*scalar.String).String() {
			return 1
		} else {
			return -1
		}
	default:
		panic(fmt.Sprintf("unsupported type: %v", v.Kind()))
	}
}

func forEachParquetValue(chunk parquet.ColumnChunk, vals []parquet.Value, f func(i int, value parquet.Value) error) ([]parquet.Value, error) {
	pages := chunk.Pages()
	defer pages.Close()
	i := 0
	for { // Read all the pages into the vals slice
		p, err := pages.ReadPage()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		reader := p.Values()
		n, err := reader.ReadValues(vals[i:])
		if err != nil && err != io.EOF {
			return nil, err
		}
		i += n
	}

	// Callback for each value in the vals slice
	for i, v := range vals {
		if err := f(i, v); err != nil {
			return nil, err
		}
	}
	return vals, nil
}

func (e BinaryScalarExpr) Eval(r arrow.Record) (*Bitmap, error) {
	leftData, exists, err := e.Left.ArrowArray(r)
	if err != nil {
		return nil, err
	}

	if !exists {
		res := NewBitmap()
		switch e.Op {
		case logicalplan.OpEq:
			if e.Right.IsValid() { // missing column; looking for == non-nil
				switch t := e.Right.(type) {
				case *scalar.Binary:
					if t.String() != "" { // treat empty string equivalent to nil
						return res, nil
					}
				case *scalar.String:
					if t.String() != "" { // treat empty string equivalent to nil
						return res, nil
					}
				}
			}
		case logicalplan.OpNotEq: // missing column; looking for != nil
			if !e.Right.IsValid() {
				return res, nil
			}
		case logicalplan.OpLt, logicalplan.OpLtEq, logicalplan.OpGt, logicalplan.OpGtEq:
			return res, nil
		}

		res.AddRange(0, uint64(r.NumRows()))
		return res, nil
	}

	return BinaryScalarOperation(leftData, e.Right, e.Op)
}

func (e BinaryScalarExpr) String() string {
	return e.Left.String() + " " + e.Op.String() + " " + e.Right.String()
}

var ErrUnsupportedBinaryOperation = errors.New("unsupported binary operation")

func BinaryScalarOperation(left arrow.Array, right scalar.Scalar, operator logicalplan.Op) (*Bitmap, error) {
	switch operator {
	case logicalplan.OpContains, logicalplan.OpNotContains:
		switch arr := left.(type) {
		case *array.Binary, *array.String:
			return ArrayScalarContains(left, right, operator == logicalplan.OpNotContains)
		case *array.Dictionary:
			return DictionaryArrayScalarContains(arr, right, operator == logicalplan.OpNotContains)
		default:
			panic("unsupported array type " + fmt.Sprintf("%T", arr))
		}
	}

	// TODO: Figure out dictionary arrays and lists with compute next
	leftType := left.DataType()
	switch arr := left.(type) {
	case *array.Dictionary:
		switch operator {
		case logicalplan.OpEq:
			return DictionaryArrayScalarEqual(arr, right)
		case logicalplan.OpNotEq:
			return DictionaryArrayScalarNotEqual(arr, right)
		default:
			return nil, fmt.Errorf("unsupported operator: %v", operator)
		}
	}

	switch leftType.(type) {
	case *arrow.ListType:
		panic("TODO: list comparisons unimplemented")
	}

	return ArrayScalarCompute(operator.ArrowString(), left, right)
}

func ArrayScalarCompute(funcName string, left arrow.Array, right scalar.Scalar) (*Bitmap, error) {
	leftData := compute.NewDatum(left)
	defer leftData.Release()
	rightData := compute.NewDatum(right)
	defer rightData.Release()
	equalsResult, err := compute.CallFunction(context.TODO(), funcName, nil, leftData, rightData)
	if err != nil {
		if errors.Unwrap(err).Error() == "not implemented" {
			return nil, ErrUnsupportedBinaryOperation
		}
		return nil, fmt.Errorf("error calling equal function: %w", err)
	}
	defer equalsResult.Release()
	equalsDatum, ok := equalsResult.(*compute.ArrayDatum)
	if !ok {
		return nil, fmt.Errorf("expected *compute.ArrayDatum, got %T", equalsResult)
	}
	equalsArray, ok := equalsDatum.MakeArray().(*array.Boolean)
	if !ok {
		return nil, fmt.Errorf("expected *array.Boolean, got %T", equalsDatum.MakeArray())
	}
	defer equalsArray.Release()

	res := NewBitmap()
	for i := 0; i < equalsArray.Len(); i++ {
		if equalsArray.IsNull(i) {
			continue
		}
		if equalsArray.Value(i) {
			res.AddInt(i)
		}
	}
	return res, nil
}

func DictionaryArrayScalarNotEqual(left *array.Dictionary, right scalar.Scalar) (*Bitmap, error) {
	res := NewBitmap()
	var data []byte
	switch r := right.(type) {
	case *scalar.Binary:
		data = r.Data()
	case *scalar.String:
		data = r.Data()
	}

	// This is a special case for where the left side should not equal NULL
	if right == scalar.ScalarNull {
		for i := 0; i < left.Len(); i++ {
			if !left.IsNull(i) {
				res.Add(uint32(i))
			}
		}
		return res, nil
	}

	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}

		switch dict := left.Dictionary().(type) {
		case *array.Binary:
			if !bytes.Equal(dict.Value(left.GetValueIndex(i)), data) {
				res.Add(uint32(i))
			}
		case *array.String:
			if dict.Value(left.GetValueIndex(i)) != string(data) {
				res.Add(uint32(i))
			}
		}
	}

	return res, nil
}

func DictionaryArrayScalarEqual(left *array.Dictionary, right scalar.Scalar) (*Bitmap, error) {
	res := NewBitmap()
	var data []byte
	switch r := right.(type) {
	case *scalar.Binary:
		data = r.Data()
	case *scalar.String:
		data = r.Data()
	}

	// This is a special case for where the left side should equal NULL
	if right == scalar.ScalarNull {
		for i := 0; i < left.Len(); i++ {
			if left.IsNull(i) {
				res.Add(uint32(i))
			}
		}
		return res, nil
	}

	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}

		switch dict := left.Dictionary().(type) {
		case *array.Binary:
			if bytes.Equal(dict.Value(left.GetValueIndex(i)), data) {
				res.Add(uint32(i))
			}
		case *array.String:
			if dict.Value(left.GetValueIndex(i)) == string(data) {
				res.Add(uint32(i))
			}
		}
	}

	return res, nil
}

func ArrayScalarContains(arr arrow.Array, right scalar.Scalar, not bool) (*Bitmap, error) {
	var r []byte
	switch s := right.(type) {
	case *scalar.Binary:
		r = s.Data()
	case *scalar.String:
		r = s.Data()
	}

	res := NewBitmap()
	switch a := arr.(type) {
	case *array.Binary:
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				continue
			}
			contains := bytes.Contains(a.Value(i), r)
			if contains && !not || !contains && not {
				res.Add(uint32(i))
			}
		}
		return res, nil
	case *array.String:
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				continue
			}
			contains := bytes.Contains(unsafeStringToBytes(a.Value(i)), r)
			if contains && !not || !contains && not {
				res.Add(uint32(i))
			}
		}
		return res, nil
	}
	return nil, fmt.Errorf("contains not implemented for %T", arr)
}

func DictionaryArrayScalarContains(left *array.Dictionary, right scalar.Scalar, not bool) (*Bitmap, error) {
	res := NewBitmap()
	var data []byte
	switch r := right.(type) {
	case *scalar.Binary:
		data = r.Data()
	case *scalar.String:
		data = r.Data()
	}

	// This is a special case for where the left side should not equal NULL
	if right == scalar.ScalarNull {
		for i := 0; i < left.Len(); i++ {
			if !left.IsNull(i) {
				res.Add(uint32(i))
			}
		}
		return res, nil
	}

	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}

		switch dict := left.Dictionary().(type) {
		case *array.Binary:
			contains := bytes.Contains(dict.Value(left.GetValueIndex(i)), data)
			if contains && !not || !contains && not {
				res.Add(uint32(i))
			}
		case *array.String:
			contains := bytes.Contains(unsafeStringToBytes(dict.Value(left.GetValueIndex(i))), data)
			if contains && !not || !contains && not {
				res.Add(uint32(i))
			}
		}
	}

	return res, nil
}

func unsafeStringToBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
