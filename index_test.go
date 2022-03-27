package arcticdb

import (
	"testing"

	"github.com/google/btree"
	"github.com/stretchr/testify/require"
)

type FakeGranule struct {
	islessThan bool // indicates this granule is being used during an Ascend action, and we need to peform a slightly different compare
	min, max   int  // min max represent the min and max of a Granule
}

func (f *FakeGranule) Less(than btree.Item) bool {
	thanf := than.(*FakeGranule)

	if f.islessThan || thanf.islessThan {
		return f.min <= thanf.min
	}

	return f.max < thanf.min
}

// This test validates the assumption the table maekes during iteration, that if we have each btree item having
// a range of values, that we can successfully use the btree index to filter out nodes we don't want to visit
func Test_Index_Search(t *testing.T) {
	index := btree.New(2)

	n := 10
	for i := 0; i < 1000; i += n {
		index.ReplaceOrInsert(&FakeGranule{
			min: i + 1,
			max: i + n,
		})
	}

	searchMin := 50
	searchMax := 96
	greaterOrEqual := &FakeGranule{
		min: searchMin,
		max: searchMin,
	}
	lessThan := &FakeGranule{
		islessThan: true,
		min:        searchMax,
		max:        searchMax,
	}

	min := -1
	max := -1
	index.AscendRange(greaterOrEqual, lessThan, func(item btree.Item) bool {
		granule := item.(*FakeGranule)

		if min == -1 || granule.min < min {
			min = granule.min
		}

		if max == -1 || granule.max > max {
			max = granule.max
		}

		return true
	})

	require.True(t, min <= searchMin)
	require.True(t, max >= searchMax)
}
