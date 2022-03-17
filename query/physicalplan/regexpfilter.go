package physicalplan

import (
	"regexp"

	"github.com/apache/arrow/go/v7/arrow"
	"github.com/apache/arrow/go/v7/arrow/array"
)

type RegExpFilter struct {
	left     *ArrayRef
	notMatch bool
	right    *regexp.Regexp
}

func (f *RegExpFilter) Eval(r arrow.Record) (*Bitmap, error) {
	leftData, exists, err := f.left.ArrowArray(r)
	if err != nil {
		return nil, err
	}

	// TODO: This needs a bunch of test cases to validate edge cases like non
	// existant columns or null values.
	if !exists {
		res := NewBitmap()
		if f.notMatch {
			for i := uint32(0); i < uint32(r.NumRows()); i++ {
				res.Add(i)
			}
			return res, nil
		}
		return res, nil
	}

	if f.notMatch {
		return StringArrayScalarRegexNotMatch(leftData.(*array.String), f.right)
	}

	return StringArrayScalarRegexMatch(leftData.(*array.String), f.right)
}

func StringArrayScalarRegexMatch(left *array.String, right *regexp.Regexp) (*Bitmap, error) {
	res := NewBitmap()
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if right.MatchString(left.Value(i)) {
			res.Add(uint32(i))
		}
	}

	return res, nil
}

func StringArrayScalarRegexNotMatch(left *array.String, right *regexp.Regexp) (*Bitmap, error) {
	res := NewBitmap()
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if !right.MatchString(left.Value(i)) {
			res.Add(uint32(i))
		}
	}

	return res, nil
}
