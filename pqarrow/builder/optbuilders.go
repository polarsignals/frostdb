package builder

import (
	"reflect"
	"sync/atomic"
	"unsafe"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/bitutil"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/segmentio/parquet-go"
)

// ColumnBuilder is a subset of the array.Builder interface implemented by the
// optimized builders in this file.
type ColumnBuilder interface {
	Retain()
	Release()
	Len() int
	AppendNull()
	Reserve(int)
	NewArray() arrow.Array
}

// OptimizedBuilder is a set of FrostDB specific builder methods.
type OptimizedBuilder interface {
	ColumnBuilder
	AppendNulls(int)
	ResetToLength(int)
	RepeatLastValue(int)
}

type builderBase struct {
	dtype          arrow.DataType
	refCount       int64
	length         int
	validityBitmap []byte
}

func (b *builderBase) reset() {
	b.length = 0
	b.validityBitmap = b.validityBitmap[:0]
}

func (b *builderBase) Retain() {
	atomic.AddInt64(&b.refCount, 1)
}

func (b *builderBase) releaseInternal() {
	b.length = 0
	b.validityBitmap = nil
}

func (b *builderBase) Release() {
	atomic.AddInt64(&b.refCount, -1)
	b.releaseInternal()
}

// Len returns the number of elements in the array builder.
func (b *builderBase) Len() int {
	return b.length
}

func (b *builderBase) Reserve(int) {}

// AppendNulls appends n null values to the array being built. This is specific
// to distinct optimizations in FrostDB.
func (b *builderBase) AppendNulls(n int) {
	b.validityBitmap = resizeBitmap(b.validityBitmap, b.length+n)
	bitutil.SetBitsTo(b.validityBitmap, int64(b.length), int64(n), false)
	b.length += n
}

func resizeBitmap(bitmap []byte, valuesToRepresent int) []byte {
	bytesNeeded := int(bitutil.BytesForBits(int64(valuesToRepresent)))
	if cap(bitmap) < bytesNeeded {
		existingBitmap := bitmap
		bitmap = make([]byte, bitutil.NextPowerOf2(bytesNeeded))
		copy(bitmap, existingBitmap)
	}
	return bitmap[:bytesNeeded]
}

var _ OptimizedBuilder = (*OptBinaryBuilder)(nil)

// OptBinaryBuilder is an optimized array.BinaryBuilder.
type OptBinaryBuilder struct {
	builderBase

	data []byte
	// offsets are offsets into data. The ith value is
	// data[offsets[i]:offsets[i+1]]. Note however, that during normal operation,
	// len(data) is never appended to the slice until the next value is added,
	// i.e. the last offset is never closed until the offsets slice is appended
	// to or returned to the caller.
	offsets []uint32
}

func NewOptBinaryBuilder(dtype arrow.BinaryDataType) *OptBinaryBuilder {
	b := &OptBinaryBuilder{}
	b.dtype = dtype
	return b
}

// Release decreases the reference count by 1.
// When the reference count goes to zero, the memory is freed.
// Release may be called simultaneously from multiple goroutines.
func (b *OptBinaryBuilder) Release() {
	if atomic.AddInt64(&b.refCount, -1) == 0 {
		b.data = nil
		b.offsets = nil
		b.releaseInternal()
	}
}

// AppendNull adds a new null value to the array being built. This is slow,
// don't use it.
func (b *OptBinaryBuilder) AppendNull() {
	b.builderBase.AppendNulls(1)
}

// AppendNulls appends n null values to the array being built. This is specific
// to distinct optimizations in FrostDB.
func (b *OptBinaryBuilder) AppendNulls(n int) {
	for i := 0; i < n; i++ {
		b.offsets = append(b.offsets, uint32(len(b.data)))
	}
	b.builderBase.AppendNulls(n)
}

// NewArray creates a new array from the memory buffers used
// by the builder and resets the Builder so it can be used to build
// a new array.
func (b *OptBinaryBuilder) NewArray() arrow.Array {
	b.offsets = append(b.offsets, uint32(len(b.data)))
	var offsetsAsBytes []byte

	fromHeader := (*reflect.SliceHeader)(unsafe.Pointer(&b.offsets))
	toHeader := (*reflect.SliceHeader)(unsafe.Pointer(&offsetsAsBytes))
	toHeader.Data = fromHeader.Data
	toHeader.Len = fromHeader.Len * arrow.Uint32SizeBytes
	toHeader.Cap = fromHeader.Cap * arrow.Uint32SizeBytes

	data := array.NewData(
		b.dtype,
		b.length,
		[]*memory.Buffer{
			memory.NewBufferBytes(b.validityBitmap),
			memory.NewBufferBytes(offsetsAsBytes),
			memory.NewBufferBytes(b.data),
		},
		nil,
		b.length-bitutil.CountSetBits(b.validityBitmap, 0, b.length),
		0,
	)
	b.reset()
	b.offsets = b.offsets[:0]
	b.data = b.data[:0]
	return array.NewBinaryData(data)
}

// AppendData appends a flat slice of bytes to the builder, with an accompanying
// slice of offsets. This data is considered to be non-null.
func (b *OptBinaryBuilder) AppendData(data []byte, offsets []uint32) {
	// Trim the last offset since we want this last range to be "open".
	offsets = offsets[:len(offsets)-1]

	offsetConversion := uint32(len(b.data))
	b.data = append(b.data, data...)
	startOffset := len(b.offsets)
	b.offsets = append(b.offsets, offsets...)
	for curOffset := startOffset; curOffset < len(b.offsets); curOffset++ {
		b.offsets[curOffset] += offsetConversion
	}

	b.length += len(offsets)
	b.validityBitmap = resizeBitmap(b.validityBitmap, b.length)
	bitutil.SetBitsTo(b.validityBitmap, int64(startOffset), int64(len(offsets)), true)
}

// AppendParquetValues appends the given parquet values to the builder. The
// values may be null, but if it is known upfront that none of the values are
// null, AppendData offers a more efficient way of appending values.
func (b *OptBinaryBuilder) AppendParquetValues(values []parquet.Value) {
	for i := range values {
		b.offsets = append(b.offsets, uint32(len(b.data)))
		b.data = append(b.data, values[i].ByteArray()...)
	}

	oldLength := b.length
	b.length += len(values)

	b.validityBitmap = resizeBitmap(b.validityBitmap, b.length)
	for i := range values {
		bitutil.SetBitTo(b.validityBitmap, oldLength+i, !values[i].IsNull())
	}
}

// RepeatLastValue is specific to distinct optimizations in FrostDB.
func (b *OptBinaryBuilder) RepeatLastValue(n int) {
	if bitutil.BitIsNotSet(b.validityBitmap, b.length-1) {
		// Last value is null.
		b.AppendNulls(n)
		return
	}

	lastValue := b.data[b.offsets[len(b.offsets)-1]:]
	for i := 0; i < n; i++ {
		b.offsets = append(b.offsets, uint32(len(b.data)))
		b.data = append(b.data, lastValue...)
	}
	b.length += n
}

// ResetToLength is specific to distinct optimizations in FrostDB.
func (b *OptBinaryBuilder) ResetToLength(n int) {
	if n == b.length {
		return
	}

	b.length = n
	b.data = b.data[:b.offsets[n]]
	b.offsets = b.offsets[:n]
	b.validityBitmap = resizeBitmap(b.validityBitmap, n)
}
