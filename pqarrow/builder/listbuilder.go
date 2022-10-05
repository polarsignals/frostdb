package builder

import (
	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
)

// ListBuilder is a wrapper over an array.ListBuilder that uses an
// ColumnBuilder as a values builder.
type ListBuilder struct {
	*array.ListBuilder
	valueBuilder ColumnBuilder
}

// NewListBuilder creates a ListBuilder with the given element type.
func NewListBuilder(mem memory.Allocator, etype arrow.DataType) *ListBuilder {
	return &ListBuilder{
		ListBuilder:  array.NewListBuilder(mem, etype),
		valueBuilder: NewBuilder(mem, etype),
	}
}

func (b *ListBuilder) ValueBuilder() ColumnBuilder {
	return b.valueBuilder
}

func (b *ListBuilder) NewArray() arrow.Array {
	listArr := b.ListBuilder.NewArray()
	// Overwrite the arrow listbuilder's child data with the wrapped
	// ColumnBuilder's data.
	listArr.Data().Children()[0] = b.valueBuilder.NewArray().Data()
	return listArr
}
