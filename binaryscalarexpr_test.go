package frostdb

import (
	"testing"

	"github.com/segmentio/parquet-go"
	"github.com/stretchr/testify/require"
)

type FakeColumnChunk struct {
	index *FakeColumnIndex
}

func (f *FakeColumnChunk) Type() parquet.Type               { return nil }
func (f *FakeColumnChunk) Column() int                      { return 0 }
func (f *FakeColumnChunk) Pages() parquet.Pages             { return nil }
func (f *FakeColumnChunk) ColumnIndex() parquet.ColumnIndex { return f.index }
func (f *FakeColumnChunk) OffsetIndex() parquet.OffsetIndex { return nil }
func (f *FakeColumnChunk) BloomFilter() parquet.BloomFilter { return nil }
func (f *FakeColumnChunk) NumValues() int64                 { return 0 }

type FakeColumnIndex struct {
	numpages func() int
}

func (f *FakeColumnIndex) NumPages() int {
	if f.numpages != nil {
		return f.numpages()
	}
	return 0
}
func (f *FakeColumnIndex) NullCount(int) int64        { return 0 }
func (f *FakeColumnIndex) NullPage(int) bool          { return false }
func (f *FakeColumnIndex) MinValue(int) parquet.Value { return parquet.ValueOf(nil) }
func (f *FakeColumnIndex) MaxValue(int) parquet.Value { return parquet.ValueOf(nil) }
func (f *FakeColumnIndex) IsAscending() bool          { return false }
func (f *FakeColumnIndex) IsDescending() bool         { return false }

// This is a regression test that ensures the Min/Max functions return a null
// value (instead of panicing) should they be passed a column chunk that only
// has null values.
func Test_MinMax_EmptyColumnChunk(t *testing.T) {
	fakeChunk := &FakeColumnChunk{
		index: &FakeColumnIndex{
			numpages: func() int {
				return 10
			},
		},
	}

	v := Min(fakeChunk)
	require.True(t, v.IsNull())

	v = Max(fakeChunk)
	require.True(t, v.IsNull())
}
