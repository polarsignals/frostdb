package convert

import (
	"errors"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/segmentio/parquet-go"

	"github.com/polarsignals/frostdb/pqarrow/writer"
)

func ParquetFieldToArrowField(pf parquet.Field) (arrow.Field, error) {
	typ, err := ParquetNodeToType(pf)
	if err != nil {
		return arrow.Field{}, err
	}

	return arrow.Field{
		Name:     pf.Name(),
		Type:     typ,
		Nullable: pf.Optional(),
	}, nil
}

// ParquetNodeToType converts a parquet node to an arrow type.
func ParquetNodeToType(n parquet.Node) (arrow.DataType, error) {
	typ, _, err := ParquetNodeToTypeWithWriterFunc(n)
	if err != nil {
		return nil, err
	}
	return typ, nil
}

// ParquetNodeToTypeWithWriterFunc converts a parquet node to an arrow type and a function to
// create a value writer.
func ParquetNodeToTypeWithWriterFunc(n parquet.Node) (arrow.DataType, func(b array.Builder, numValues int) writer.ValueWriter, error) {
	t := n.Type()
	lt := t.LogicalType()

	switch {
	case lt != nil:
		switch {
		case lt.UTF8 != nil:
			return &arrow.BinaryType{}, writer.NewBinaryValueWriter, nil
		case lt.Integer != nil:
			switch lt.Integer.BitWidth {
			case 64:
				if lt.Integer.IsSigned {
					return &arrow.Int64Type{}, writer.NewInt64ValueWriter, nil
				}
				return &arrow.Uint64Type{}, writer.NewUint64ValueWriter, nil
			default:
				return nil, nil, errors.New("unsupported int bit width")
			}
		default:
			return nil, nil, errors.New("unsupported logical type: " + n.Type().String())
		}
	case t.String() == "group": // NOTE: this needs to be perfomed before t.Kind() because t.Kind() will panic when called on a group
		return nil, nil, errors.New("unsupported type: " + n.Type().String())
	case t.Kind() == parquet.Double:
		return &arrow.Float64Type{}, writer.NewFloat64ValueWriter, nil
	default:
		return nil, nil, errors.New("unsupported type: " + n.Type().String())
	}
}
