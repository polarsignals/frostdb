package arrowutils

import (
	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/memory"

	"github.com/polarsignals/frostdb/pqarrow/builder"
)

// VirtualNullArray is an arrow.Array that will return that any element is null
// via the arrow.Array interface methods. This is useful if callers need to
// represent an array of len NULL values without allocating/storing a bitmap.
// This should only be used internally. If callers need a physical null array,
// call MakeNullArray.
type VirtualNullArray struct {
	dt  arrow.DataType
	len int
}

func MakeVirtualNullArray(dt arrow.DataType, len int) VirtualNullArray {
	return VirtualNullArray{
		dt:  dt,
		len: len,
	}
}

// MakeNullArray makes a physical arrow.Array full of NULLs of the given
// DataType.
func MakeNullArray(mem memory.Allocator, dt arrow.DataType, len int) arrow.Array {
	// TODO(asubiotto): This can be improved by using the optimized builders'
	// AppendNulls. Not sure whether this should be part of the builder package.
	b := builder.NewBuilder(mem, dt)
	b.Reserve(len)
	for i := 0; i < len; i++ {
		b.AppendNull()
	}
	return b.NewArray()
}

func (n VirtualNullArray) MarshalJSON() ([]byte, error) {
	panic("VirtualNullArray: MarshalJSON not implemented")
}

func (n VirtualNullArray) DataType() arrow.DataType {
	return n.dt
}

func (n VirtualNullArray) NullN() int {
	return n.len
}

func (n VirtualNullArray) NullBitmapBytes() []byte {
	panic("VirtualNullArray: NullBitmapBytes not implemented")
}

func (n VirtualNullArray) IsNull(_ int) bool {
	return true
}

func (n VirtualNullArray) IsValid(_ int) bool {
	return false
}

func (n VirtualNullArray) Data() arrow.ArrayData {
	panic("VirtualNullArray: Data not implemented")
}

func (n VirtualNullArray) Len() int {
	return n.len
}

func (n VirtualNullArray) Retain() {}

func (n VirtualNullArray) Release() {}

func (n VirtualNullArray) String() string { return "VirtualNullArray" }
