package cmd

import (
	"fmt"
	"os"

	"github.com/parquet-go/parquet-go"
)

func openParquetFile(file string) (*parquet.File, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	stats, err := f.Stat()
	if err != nil {
		return nil, err
	}
	pf, err := parquet.OpenFile(f, stats.Size())
	if err != nil {
		return nil, err
	}

	return pf, nil
}

func compare(v1, v2 parquet.Value) int {
	if v1.IsNull() {
		if v2.IsNull() {
			return 0
		}
		return 1
	}

	if v2.IsNull() {
		return -1
	}

	switch v1.Kind() {
	case parquet.Int32:
		return parquet.Int32Type.Compare(v1, v2)
	case parquet.Int64:
		return parquet.Int64Type.Compare(v1, v2)
	case parquet.Float:
		return parquet.FloatType.Compare(v1, v2)
	case parquet.Double:
		return parquet.DoubleType.Compare(v1, v2)
	case parquet.ByteArray, parquet.FixedLenByteArray:
		return parquet.ByteArrayType.Compare(v1, v2)
	case parquet.Boolean:
		return parquet.BooleanType.Compare(v1, v2)
	default:
		panic(fmt.Sprintf("unsupported value comparison: %v", v1.Kind()))
	}
}
