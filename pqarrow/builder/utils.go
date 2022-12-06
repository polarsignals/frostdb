package builder

import (
	"errors"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/memory"
)

func NewBuilder(mem memory.Allocator, t arrow.DataType) ColumnBuilder {
	switch t := t.(type) {
	case *arrow.BinaryType:
		return NewOptBinaryBuilder(arrow.BinaryTypes.Binary)
	case *arrow.Int64Type:
		return NewOptInt64Builder(arrow.PrimitiveTypes.Int64)
	case *arrow.ListType:
		return NewListBuilder(mem, t.Elem())
	case *arrow.BooleanType:
		return NewOptBooleanBuilder(arrow.FixedWidthTypes.Boolean)
	default:
		return array.NewBuilder(mem, t)
	}
}

func AppendValue(cb ColumnBuilder, arr arrow.Array, i int) error {
	if arr == nil || arr.IsNull(i) {
		cb.AppendNull()
		return nil
	}

	switch b := cb.(type) {
	case *OptBinaryBuilder:
		b.Append(arr.(*array.Binary).Value(i))
		return nil
	case *OptInt64Builder:
		b.Append(arr.(*array.Int64).Value(i))
		return nil
	case *OptBooleanBuilder:
		b.AppendSingle(arr.(*array.Boolean).Value(i))
		return nil
	case *array.Int64Builder:
		b.Append(arr.(*array.Int64).Value(i))
		return nil
	case *array.StringBuilder:
		b.Append(arr.(*array.String).Value(i))
		return nil
	case *array.BinaryBuilder:
		b.Append(arr.(*array.Binary).Value(i))
		return nil
	case *array.FixedSizeBinaryBuilder:
		b.Append(arr.(*array.FixedSizeBinary).Value(i))
		return nil
	case *array.BooleanBuilder:
		b.Append(arr.(*array.Boolean).Value(i))
		return nil
	// case *array.List:
	//	// TODO: This seems horribly inefficient, we already have the whole
	//	// array and are just doing an expensive copy, but arrow doesn't seem
	//	// to be able to append whole list scalars at once.
	//	length := s.Value.Len()
	//	larr := arr.(*array.ListBuilder)
	//	vb := larr.ValueBuilder()
	//	larr.Append(true)
	//	for i := 0; i < length; i++ {
	//		v, err := scalar.GetScalar(s.Value, i)
	//		if err != nil {
	//			return err
	//		}

	//		err = appendValue(vb, v)
	//		if err != nil {
	//			return err
	//		}
	//	}
	//	return nil
	default:
		return errors.New("unsupported type for arrow append")
	}
}
