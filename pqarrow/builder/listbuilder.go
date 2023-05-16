// Copyright (c) The FrostDB Authors.
// Licensed under the Apache License 2.0.

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package builder

import (
	"sync/atomic"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/bitutil"
	"github.com/apache/arrow/go/v12/arrow/memory"
)

const (
	minBuilderCapacity = 1 << 5
)

// builder provides common functionality for managing the validity bitmap (nulls) when building arrays.
type builder struct {
	refCount   int64
	mem        memory.Allocator
	nullBitmap *memory.Buffer
	nulls      int
	length     int
	capacity   int
}

func (b *builder) init(capacity int) {
	toAlloc := bitutil.CeilByte(capacity) / 8
	b.nullBitmap = memory.NewResizableBuffer(b.mem)
	b.nullBitmap.Resize(toAlloc)
	b.capacity = capacity
	memory.Set(b.nullBitmap.Buf(), 0)
}

func (b *builder) reset() {
	if b.nullBitmap != nil {
		b.nullBitmap.Release()
		b.nullBitmap = nil
	}

	b.nulls = 0
	b.length = 0
	b.capacity = 0
}

func (b *builder) resize(newBits int, init func(int)) {
	if b.nullBitmap == nil {
		init(newBits)
		return
	}

	newBytesN := bitutil.CeilByte(newBits) / 8
	oldBytesN := b.nullBitmap.Len()
	b.nullBitmap.Resize(newBytesN)
	b.capacity = newBits
	if oldBytesN < newBytesN {
		// TODO(sgc): necessary?
		memory.Set(b.nullBitmap.Buf()[oldBytesN:], 0)
	}
	if newBits < b.length {
		b.length = newBits
		b.nulls = newBits - bitutil.CountSetBits(b.nullBitmap.Buf(), 0, newBits)
	}
}

func (b *builder) reserve(elements int, resize func(int)) {
	if b.nullBitmap == nil {
		b.nullBitmap = memory.NewResizableBuffer(b.mem)
	}
	if b.length+elements > b.capacity {
		newCap := bitutil.NextPowerOf2(b.length + elements)
		resize(newCap)
	}
}

// unsafeAppendBoolsToBitmap appends the contents of valid to the validity bitmap.
// As an optimization, if the valid slice is empty, the next length bits will be set to valid (not null).
func (b *builder) unsafeAppendBoolsToBitmap(valid []bool, length int) {
	if len(valid) == 0 {
		b.unsafeSetValid(length)
		return
	}

	byteOffset := b.length / 8
	bitOffset := byte(b.length % 8)
	nullBitmap := b.nullBitmap.Bytes()
	bitSet := nullBitmap[byteOffset]

	for _, v := range valid {
		if bitOffset == 8 {
			bitOffset = 0
			nullBitmap[byteOffset] = bitSet
			byteOffset++
			bitSet = nullBitmap[byteOffset]
		}

		if v {
			bitSet |= bitutil.BitMask[bitOffset]
		} else {
			bitSet &= bitutil.FlippedBitMask[bitOffset]
			b.nulls++
		}
		bitOffset++
	}

	if bitOffset != 0 {
		nullBitmap[byteOffset] = bitSet
	}
	b.length += len(valid)
}

// unsafeSetValid sets the next length bits to valid in the validity bitmap.
func (b *builder) unsafeSetValid(length int) {
	padToByte := min(8-(b.length%8), length)
	if padToByte == 8 {
		padToByte = 0
	}
	bits := b.nullBitmap.Bytes()
	for i := b.length; i < b.length+padToByte; i++ {
		bitutil.SetBit(bits, i)
	}

	start := (b.length + padToByte) / 8
	fastLength := (length - padToByte) / 8
	memory.Set(bits[start:start+fastLength], 0xff)

	newLength := b.length + length
	// trailing bytes
	for i := b.length + padToByte + (fastLength * 8); i < newLength; i++ {
		bitutil.SetBit(bits, i)
	}

	b.length = newLength
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// LargeListBuilder is a wrapper over an array.LargeLargeListBuilder that uses ColumnBuilder as a values buffer.
type LargeListBuilder struct {
	builder

	etype   arrow.DataType // data type of the list's elements.
	values  ColumnBuilder
	offsets *array.Int64Builder
}

func NewLargeListBuilder(mem memory.Allocator, etype arrow.DataType) *LargeListBuilder {
	return &LargeListBuilder{
		builder: builder{refCount: 1, mem: mem},
		etype:   etype,
		values:  NewBuilder(mem, etype),
		offsets: array.NewInt64Builder(mem),
	}
}

// Release decreases the reference count by 1.
// When the reference count goes to zero, the memory is freed.
func (b *LargeListBuilder) Release() {
	if atomic.AddInt64(&b.refCount, -1) == 0 {
		if b.nullBitmap != nil {
			b.nullBitmap.Release()
			b.nullBitmap = nil
		}
	}

	b.values.Release()
	b.offsets.Release()
}

func (b *LargeListBuilder) appendNextOffset() {
	b.offsets.Append(int64(b.values.Len()))
}

func (b *LargeListBuilder) Append(v bool) {
	b.Reserve(1)
	b.unsafeAppendBoolToBitmap(v)
	b.appendNextOffset()
}

func (b *LargeListBuilder) AppendNull() {
	b.Reserve(1)
	b.unsafeAppendBoolToBitmap(false)
	b.appendNextOffset()
}

func (b *LargeListBuilder) AppendValues(offsets []int64, valid []bool) {
	b.Reserve(len(valid))
	b.offsets.AppendValues(offsets, nil)
	b.builder.unsafeAppendBoolsToBitmap(valid, len(valid))
}

func (b *LargeListBuilder) unsafeAppendBoolToBitmap(isValid bool) {
	if isValid {
		bitutil.SetBit(b.nullBitmap.Bytes(), b.length)
	} else {
		b.nulls++
	}
	b.length++
}

func (b *LargeListBuilder) init(capacity int) {
	b.builder.init(capacity)
	b.offsets.Resize(capacity + 1)
}

// Reserve ensures there is enough space for appending n elements
// by checking the capacity and calling Resize if necessary.
func (b *LargeListBuilder) Reserve(n int) {
	b.builder.reserve(n, b.resizeHelper)
	b.offsets.Reserve(n)
}

// Resize adjusts the space allocated by b to n elements. If n is greater than b.Cap(),
// additional memory will be allocated. If n is smaller, the allocated memory may reduced.
func (b *LargeListBuilder) Resize(n int) {
	b.resizeHelper(n)
	b.offsets.Resize(n)
}

func (b *LargeListBuilder) resizeHelper(n int) {
	if n < minBuilderCapacity {
		n = minBuilderCapacity
	}

	if b.capacity == 0 {
		b.init(n)
	} else {
		b.builder.resize(n, b.builder.init)
	}
}

func (b *LargeListBuilder) ValueBuilder() ColumnBuilder {
	return b.values
}

// NewArray creates a List array from the memory buffers used by the builder and resets the LargeListBuilder
// so it can be used to build a new array.
func (b *LargeListBuilder) NewArray() arrow.Array {
	return b.NewListArray()
}

func (b *LargeListBuilder) Len() int {
	return b.length
}

// NewListArray creates a List array from the memory buffers used by the builder and resets the LargeListBuilder
// so it can be used to build a new array.
func (b *LargeListBuilder) NewListArray() (a *array.LargeList) {
	if b.offsets.Len() != b.length+1 {
		b.appendNextOffset()
	}
	data := b.newData()
	a = array.NewLargeListData(data)
	data.Release()
	return
}

func (b *LargeListBuilder) newData() (data *array.Data) {
	values := b.values.NewArray()
	defer values.Release()

	var offsets *memory.Buffer
	if b.offsets != nil {
		arr := b.offsets.NewInt64Array()
		defer arr.Release()
		offsets = arr.Data().Buffers()[1]
	}

	data = array.NewData(
		arrow.LargeListOf(b.etype), b.length,
		[]*memory.Buffer{
			b.nullBitmap,
			offsets,
		},
		[]arrow.ArrayData{values.Data()},
		b.nulls,
		0,
	)
	b.reset()

	return
}

func (b *LargeListBuilder) Retain() {
	b.values.Retain()
}
