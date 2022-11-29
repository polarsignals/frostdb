package convert

import (
	"errors"

	"github.com/apache/arrow/go/v8/arrow"
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
// TODO(asubiotto): The dependency on the writer package needs to be untangled.
func ParquetNodeToTypeWithWriterFunc(n parquet.Node) (arrow.DataType, writer.NewWriterFunc, error) {
	t := n.Type()
	lt := t.LogicalType()

	resultType, resultWriter, err := func() (arrow.DataType, writer.NewWriterFunc, error) {
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
		case t.Kind() == parquet.Boolean:
			return &arrow.BooleanType{}, writer.NewBooleanValueWriter, nil
		case t.Kind() == parquet.Double:
			return &arrow.Float64Type{}, writer.NewFloat64ValueWriter, nil
		default:
			return nil, nil, errors.New("unsupported type: " + n.Type().String())
		}
	}()
	if err != nil {
		return nil, nil, err
	}

	if n.Repeated() {
		// TODO(asubiotto): We should use arrow.ListOfNonNullable if
		// n.Optional(). The problem is that it doesn't seem like the arrow
		// builder stores the nullability (NewArray always uses ListOf).
		return arrow.ListOf(resultType), writer.NewListValueWriter(resultWriter), nil
	}
	return resultType, resultWriter, nil
}
