package physicalplan

import (
	"bytes"
	"errors"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/scalar"

	"github.com/polarsignals/frostdb/query/logicalplan"
)

type ArrayRef struct {
	ColumnName string
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

func (e BinaryScalarExpr) Eval(r arrow.Record) (*Bitmap, error) {
	leftData, exists, err := e.Left.ArrowArray(r)
	if err != nil {
		return nil, err
	}

	// TODO: This needs a bunch of test cases to validate edge cases like non
	// existant columns or null values. I'm pretty sure this is completely
	// wrong and needs per operation, per type specific behavior.
	if !exists {
		res := NewBitmap()
		for i := uint32(0); i < uint32(r.NumRows()); i++ {
			res.Add(i)
		}
		return res, nil
	}

	return BinaryScalarOperation(leftData, e.Right, e.Op)
}

func (e BinaryScalarExpr) String() string {
	return e.Left.String() + " " + e.Op.String() + " " + e.Right.String()
}

var ErrUnsupportedBinaryOperation = errors.New("unsupported binary operation")

func BinaryScalarOperation(left arrow.Array, right scalar.Scalar, operator logicalplan.Op) (*Bitmap, error) {
	leftType := left.DataType()
	switch leftType {
	case &arrow.FixedSizeBinaryType{ByteWidth: 16}:
		switch operator {
		case logicalplan.OpEq:
			return FixedSizeBinaryArrayScalarEqual(left.(*array.FixedSizeBinary), right.(*scalar.FixedSizeBinary))
		case logicalplan.OpNotEq:
			return FixedSizeBinaryArrayScalarNotEqual(left.(*array.FixedSizeBinary), right.(*scalar.FixedSizeBinary))
		default:
			panic("something terrible has happened, this should have errored previously during validation")
		}
	case arrow.BinaryTypes.String:
		switch operator {
		case logicalplan.OpEq:
			return StringArrayScalarEqual(left.(*array.String), right.(*scalar.String))
		case logicalplan.OpNotEq:
			return StringArrayScalarNotEqual(left.(*array.String), right.(*scalar.String))
		default:
			panic("something terrible has happened, this should have errored previously during validation")
		}
	case arrow.BinaryTypes.Binary:
		switch operator {
		case logicalplan.OpEq:
			switch r := right.(type) {
			case *scalar.Binary:
				return BinaryArrayScalarEqual(left.(*array.Binary), r)
			case *scalar.String:
				return BinaryArrayScalarEqual(left.(*array.Binary), r.Binary)
			default:
				panic("something terrible has happened, this should have errored previously during validation")
			}
		case logicalplan.OpNotEq:
			switch r := right.(type) {
			case *scalar.Binary:
				return BinaryArrayScalarNotEqual(left.(*array.Binary), r)
			case *scalar.String:
				return BinaryArrayScalarNotEqual(left.(*array.Binary), r.Binary)
			default:
				panic("something terrible has happened, this should have errored previously during validation")
			}
		default:
			panic("something terrible has happened, this should have errored previously during validation")
		}
	case arrow.PrimitiveTypes.Int64:
		switch operator {
		case logicalplan.OpEq:
			return Int64ArrayScalarEqual(left.(*array.Int64), right.(*scalar.Int64))
		case logicalplan.OpNotEq:
			return Int64ArrayScalarNotEqual(left.(*array.Int64), right.(*scalar.Int64))
		case logicalplan.OpLt:
			return Int64ArrayScalarLessThan(left.(*array.Int64), right.(*scalar.Int64))
		case logicalplan.OpLtEq:
			return Int64ArrayScalarLessThanOrEqual(left.(*array.Int64), right.(*scalar.Int64))
		case logicalplan.OpGt:
			return Int64ArrayScalarGreaterThan(left.(*array.Int64), right.(*scalar.Int64))
		case logicalplan.OpGtEq:
			return Int64ArrayScalarGreaterThanOrEqual(left.(*array.Int64), right.(*scalar.Int64))
		default:
			panic("something terrible has happened, this should have errored previously during validation")
		}
	}

	switch leftType.(type) {
	case *arrow.ListType:
		panic("TODO: list comparisons unimplemented")
	}

	return nil, ErrUnsupportedBinaryOperation
}

func FixedSizeBinaryArrayScalarEqual(left *array.FixedSizeBinary, right *scalar.FixedSizeBinary) (*Bitmap, error) {
	res := NewBitmap()
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if bytes.Compare(left.Value(i), right.Data()) == 0 {
			res.Add(uint32(i))
		}
	}

	return res, nil
}

func FixedSizeBinaryArrayScalarNotEqual(left *array.FixedSizeBinary, right *scalar.FixedSizeBinary) (*Bitmap, error) {
	res := NewBitmap()
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			res.Add(uint32(i))
			continue
		}
		if bytes.Compare(left.Value(i), right.Data()) != 0 {
			res.Add(uint32(i))
		}
	}

	return res, nil
}

func StringArrayScalarEqual(left *array.String, right *scalar.String) (*Bitmap, error) {
	res := NewBitmap()
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if left.Value(i) == string(right.Data()) {
			res.Add(uint32(i))
		}
	}

	return res, nil
}

func StringArrayScalarNotEqual(left *array.String, right *scalar.String) (*Bitmap, error) {
	res := NewBitmap()
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			res.Add(uint32(i))
			continue
		}
		if left.Value(i) != string(right.Data()) {
			res.Add(uint32(i))
		}
	}

	return res, nil
}

func BinaryArrayScalarEqual(left *array.Binary, right *scalar.Binary) (*Bitmap, error) {
	res := NewBitmap()
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if bytes.Equal(left.Value(i), right.Data()) {
			res.Add(uint32(i))
		}
	}

	return res, nil
}

func BinaryArrayScalarNotEqual(left *array.Binary, right *scalar.Binary) (*Bitmap, error) {
	res := NewBitmap()
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			res.Add(uint32(i))
			continue
		}
		if !bytes.Equal(left.Value(i), right.Data()) {
			res.Add(uint32(i))
		}
	}

	return res, nil
}

func Int64ArrayScalarEqual(left *array.Int64, right *scalar.Int64) (*Bitmap, error) {
	res := NewBitmap()

	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if left.Value(i) == right.Value {
			res.Add(uint32(i))
		}
	}

	return res, nil
}

func Int64ArrayScalarNotEqual(left *array.Int64, right *scalar.Int64) (*Bitmap, error) {
	res := NewBitmap()

	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			res.Add(uint32(i))
			continue
		}
		if left.Value(i) != right.Value {
			res.Add(uint32(i))
		}
	}

	return res, nil
}

func Int64ArrayScalarLessThan(left *array.Int64, right *scalar.Int64) (*Bitmap, error) {
	res := NewBitmap()

	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if left.Value(i) < right.Value {
			res.Add(uint32(i))
		}
	}

	return res, nil
}

func Int64ArrayScalarLessThanOrEqual(left *array.Int64, right *scalar.Int64) (*Bitmap, error) {
	res := NewBitmap()

	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if left.Value(i) <= right.Value {
			res.Add(uint32(i))
		}
	}

	return res, nil
}

func Int64ArrayScalarGreaterThan(left *array.Int64, right *scalar.Int64) (*Bitmap, error) {
	res := NewBitmap()

	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if left.Value(i) > right.Value {
			res.Add(uint32(i))
		}
	}

	return res, nil
}

func Int64ArrayScalarGreaterThanOrEqual(left *array.Int64, right *scalar.Int64) (*Bitmap, error) {
	res := NewBitmap()

	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if left.Value(i) >= right.Value {
			res.Add(uint32(i))
		}
	}

	return res, nil
}
