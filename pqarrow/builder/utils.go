package builder

import (
	"fmt"
	"unsafe"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
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
	case *OptInt64Builder:
		b.Append(arr.(*array.Int64).Value(i))
	case *OptBooleanBuilder:
		b.AppendSingle(arr.(*array.Boolean).Value(i))
	case *array.Int64Builder:
		b.Append(arr.(*array.Int64).Value(i))
	case *array.StringBuilder:
		b.Append(arr.(*array.String).Value(i))
	case *array.BinaryBuilder:
		b.Append(arr.(*array.Binary).Value(i))
	case *array.FixedSizeBinaryBuilder:
		b.Append(arr.(*array.FixedSizeBinary).Value(i))
	case *array.BooleanBuilder:
		b.Append(arr.(*array.Boolean).Value(i))
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
		return fmt.Errorf("unsupported type for arrow append %T", b)
	}
	return nil
}

// TODO(asubiotto): This function doesn't handle NULLs in the case of optimized
// builders.
func AppendArray(cb ColumnBuilder, arr arrow.Array) error {
	switch b := cb.(type) {
	case *OptBinaryBuilder:
		v := arr.(*array.Binary)
		offsets := v.ValueOffsets()
		b.AppendData(v.ValueBytes(), *(*[]uint32)(unsafe.Pointer(&offsets)))
	case *OptInt64Builder:
		b.AppendData(arr.(*array.Int64).Int64Values())
	default:
		// TODO(asubiotto): Handle OptBooleanBuilder. It needs some way to
		// append data.
		for i := 0; i < arr.Len(); i++ {
			// This is an interface conversion on each call, but we should care
			// more about porting our uses of arrow builders to optimized
			// builders for exactly these use cases.
			if err := AppendValue(cb, arr, i); err != nil {
				return err
			}
		}
	}
	return nil
}

func AppendGoValue(cb ColumnBuilder, v any) error {
	if v == nil {
		cb.AppendNull()
		return nil
	}

	switch b := cb.(type) {
	case *OptBinaryBuilder:
		b.Append(v.([]byte))
	case *OptInt64Builder:
		b.Append(v.(int64))
	case *OptBooleanBuilder:
		b.AppendSingle(v.(bool))
	case *array.Int64Builder:
		b.Append(v.(int64))
	case *array.StringBuilder:
		b.Append(v.(string))
	case *array.BinaryBuilder:
		b.Append(v.([]byte))
	case *array.FixedSizeBinaryBuilder:
		b.Append(v.([]byte))
	case *array.BooleanBuilder:
		b.Append(v.(bool))
	default:
		return fmt.Errorf("unsupported type for append go value %T", b)
	}
	return nil
}
