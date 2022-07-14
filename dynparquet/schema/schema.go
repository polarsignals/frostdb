package schema

import (
	"fmt"

	"github.com/segmentio/parquet-go"
	"github.com/segmentio/parquet-go/compress"
	"github.com/segmentio/parquet-go/encoding"
)

type Definition struct {
	Name           string             `json:"name"`
	Columns        []ColumnDefinition `json:"columns"`
	SortingColumns []SortingColumn    `json:"sorting_columns"`
}

type ColumnDefinition struct {
	Name          string        `json:"name"`
	StorageLayout StorageLayout `json:"storage_layout"`
	Dynamic       bool          `json:"dynamic"`
}

type StorageLayout struct {
	Type        string `json:"type"`
	Encoding    string `json:"encoding"`
	Optional    bool   `json:"optional"`
	Compression string `json:"compression"`
}

type SortingColumn struct {
	Name       string `json:"name"`
	Order      string `json:"ordering"`
	NullsFirst bool   `json:"nulls_first"`
}

func StorageLayoutToParquetNode(l StorageLayout) (parquet.Node, error) {
	var node parquet.Node
	switch l.Type {
	case "string":
		node = parquet.String()
	case "int64":
		node = parquet.Int(64)
	case "double":
		node = parquet.Leaf(parquet.DoubleType)
	default:
		return nil, fmt.Errorf("unknown storage layout type: %s", l.Type)
	}

	if l.Optional {
		node = parquet.Optional(node)
	}

	if l.Encoding != "" {
		enc, err := encodingFromString(l.Encoding)
		if err != nil {
			return nil, err
		}
		node = parquet.Encoded(node, enc)
	}

	if l.Compression != "" {
		comp, err := compressionFromString(l.Compression)
		if err != nil {
			return nil, err
		}
		node = parquet.Compressed(node, comp)
	}

	return node, nil
}

func encodingFromString(enc string) (encoding.Encoding, error) {
	switch enc {
	case "RLE_DICTIONARY":
		return &parquet.RLEDictionary, nil
	default:
		return nil, fmt.Errorf("unknown encoding: %s", enc)
	}
}

func compressionFromString(comp string) (compress.Codec, error) {
	switch comp {
	default:
		return nil, fmt.Errorf("unknown compression: %s", comp)
	}
}
