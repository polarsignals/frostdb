package arrowutils

import (
	"fmt"

	"github.com/apache/arrow/go/v14/arrow/array"
)

func ToConcreteList(arr *array.List) (*array.Dictionary, *array.Binary, error) {
	var (
		dictionaryList       *array.Dictionary
		binaryDictionaryList *array.Binary
	)
	switch list := arr.ListValues().(type) {
	case *array.Dictionary:
		dictionaryList = list
		switch dict := list.Dictionary().(type) {
		case *array.Binary:
			binaryDictionaryList = dict
		default:
			return nil, nil, fmt.Errorf("list dictionary not of expected type: %T", list)
		}
	default:
		return nil, nil, fmt.Errorf("list not of expected type: %T", list)
	}
	return dictionaryList, binaryDictionaryList, nil
}
