package physicalplan

import (
	"fmt"
	"regexp"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
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

	if !exists {
		res := NewBitmap()
		emptyMatch := f.right.Match(nil)
		if (f.notMatch && !emptyMatch) || (!f.notMatch && emptyMatch) {
			for i := uint32(0); i < uint32(r.NumRows()); i++ {
				res.Add(i)
			}
			return res, nil
		}
		return res, nil
	}

	if f.notMatch {
		return ArrayScalarRegexNotMatch(leftData, f.right)
	}

	return ArrayScalarRegexMatch(leftData, f.right)
}

func (f *RegExpFilter) String() string {
	if f.notMatch {
		return fmt.Sprintf("%s !~ \"%s\"", f.left.String(), f.right.String())
	}
	return fmt.Sprintf("%s =~ \"%s\"", f.left.String(), f.right.String())
}

func ArrayScalarRegexMatch(left arrow.Array, right *regexp.Regexp) (*Bitmap, error) {
	switch arr := left.(type) {
	case *array.Binary:
		return BinaryArrayScalarRegexMatch(arr, right)
	case *array.String:
		return StringArrayScalarRegexMatch(arr, right)
	case *array.Dictionary:
		switch dict := arr.Dictionary().(type) {
		case *array.Binary:
			return BinaryDictionaryArrayScalarRegexMatch(arr, dict, right)
		default:
			return nil, fmt.Errorf("ArrayScalarRegexMatch: unsupported dictionary type: %T", dict)
		}
	default:
		return nil, fmt.Errorf("ArrayScalarRegexMatch: unsupported type: %T", arr)
	}
}

func ArrayScalarRegexNotMatch(left arrow.Array, right *regexp.Regexp) (*Bitmap, error) {
	switch arr := left.(type) {
	case *array.Binary:
		return BinaryArrayScalarRegexNotMatch(arr, right)
	case *array.String:
		return StringArrayScalarRegexNotMatch(arr, right)
	case *array.Dictionary:
		switch dict := arr.Dictionary().(type) {
		case *array.Binary:
			return BinaryDictionaryArrayScalarRegexNotMatch(arr, dict, right)
		default:
			return nil, fmt.Errorf("ArrayScalarRegexNotMatch: unsupported dictionary type: %T", dict)
		}
	default:
		return nil, fmt.Errorf("ArrayScalarRegexNotMatch: unsupported type: %T", arr)
	}
}

func BinaryArrayScalarRegexMatch(left *array.Binary, right *regexp.Regexp) (*Bitmap, error) {
	res := NewBitmap()
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if right.MatchString(string(left.Value(i))) {
			res.Add(uint32(i))
		}
	}

	return res, nil
}

func BinaryArrayScalarRegexNotMatch(left *array.Binary, right *regexp.Regexp) (*Bitmap, error) {
	res := NewBitmap()
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if !right.MatchString(string(left.Value(i))) {
			res.Add(uint32(i))
		}
	}

	return res, nil
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

func BinaryDictionaryArrayScalarRegexMatch(dict *array.Dictionary, left *array.Binary, right *regexp.Regexp) (*Bitmap, error) {
	res := NewBitmap()
	for i := 0; i < dict.Len(); i++ {
		if dict.IsNull(i) {
			continue
		}
		if right.MatchString(string(left.Value(dict.GetValueIndex(i)))) {
			res.Add(uint32(i))
		}
	}
	return res, nil
}

func BinaryDictionaryArrayScalarRegexNotMatch(dict *array.Dictionary, left *array.Binary, right *regexp.Regexp) (*Bitmap, error) {
	res := NewBitmap()
	for i := 0; i < dict.Len(); i++ {
		if dict.IsNull(i) {
			continue
		}
		if !right.MatchString(string(left.Value(dict.GetValueIndex(i)))) {
			res.Add(uint32(i))
		}
	}

	return res, nil
}
