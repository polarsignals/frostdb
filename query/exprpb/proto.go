package exprpb

import (
	"errors"
	"fmt"
	"time"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/apache/arrow/go/v16/arrow/scalar"

	storagepb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/storage/v1alpha1"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

// ExprsFromProtos converts a slice of proto representations of expressions to
// the canonical logicalplan expressions representation.
func ExprsFromProtos(exprs []*storagepb.Expr) ([]logicalplan.Expr, error) {
	if exprs == nil {
		return nil, nil
	}

	res := make([]logicalplan.Expr, 0, len(exprs))
	for _, expr := range exprs {
		e, err := ExprFromProto(expr)
		if err != nil {
			return nil, err
		}
		res = append(res, e)
	}
	return res, nil
}

// ExprFromProto converts a proto representation of an expression to the
// canonical logicalplan expressions representation.
func ExprFromProto(expr *storagepb.Expr) (logicalplan.Expr, error) {
	if expr == nil {
		return nil, nil
	}

	switch e := expr.Def.Content.(type) {
	case *storagepb.ExprDef_BinaryExpr:
		left, err := ExprFromProto(e.BinaryExpr.Left)
		if err != nil {
			return nil, err
		}

		right, err := ExprFromProto(e.BinaryExpr.Right)
		if err != nil {
			return nil, err
		}

		op, err := protoOpToLogicalOp(e.BinaryExpr.Op)
		if err != nil {
			return nil, err
		}

		return &logicalplan.BinaryExpr{
			Left:  left,
			Right: right,
			Op:    op,
		}, nil
	case *storagepb.ExprDef_Column:
		return &logicalplan.Column{ColumnName: e.Column.Name}, nil
	case *storagepb.ExprDef_Literal:
		s, err := protoLiteralToArrowScalar(e.Literal)
		if err != nil {
			return nil, err
		}

		return &logicalplan.LiteralExpr{Value: s}, nil
	case *storagepb.ExprDef_DynamicColumn:
		return &logicalplan.DynamicColumn{ColumnName: e.DynamicColumn.Name}, nil
	case *storagepb.ExprDef_AggregationFunction:
		expr, err := ExprFromProto(e.AggregationFunction.Expr)
		if err != nil {
			return nil, err
		}

		f, err := protoAggFuncToLogicalAggFunc(e.AggregationFunction.Type)
		if err != nil {
			return nil, err
		}

		return &logicalplan.AggregationFunction{
			Func: f,
			Expr: expr,
		}, nil
	case *storagepb.ExprDef_Alias:
		expr, err := ExprFromProto(e.Alias.Expr)
		if err != nil {
			return nil, err
		}

		return &logicalplan.AliasExpr{
			Expr:  expr,
			Alias: e.Alias.Name,
		}, nil
	case *storagepb.ExprDef_Duration:
		return logicalplan.Duration(time.Duration(e.Duration.Milliseconds) * time.Millisecond), nil
	case *storagepb.ExprDef_Convert:
		expr, err := ExprFromProto(e.Convert.Expr)
		if err != nil {
			return nil, err
		}

		t, err := protoTypeToArrow(e.Convert.Type)
		if err != nil {
			return nil, err
		}

		return &logicalplan.ConvertExpr{
			Expr: expr,
			Type: t,
		}, nil
	case *storagepb.ExprDef_If:
		cond, err := ExprFromProto(e.If.Condition)
		if err != nil {
			return nil, err
		}

		then, err := ExprFromProto(e.If.Then)
		if err != nil {
			return nil, err
		}

		els, err := ExprFromProto(e.If.Else)
		if err != nil {
			return nil, err
		}

		return &logicalplan.IfExpr{
			Cond: cond,
			Then: then,
			Else: els,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported expression type: %T", e)
	}
}

func protoTypeToArrow(t storagepb.Type) (arrow.DataType, error) {
	switch t {
	case storagepb.Type_TYPE_FLOAT64:
		return arrow.PrimitiveTypes.Float64, nil
	default:
		return nil, fmt.Errorf("unsupported type: %v", t)
	}
}

func protoOpToLogicalOp(op storagepb.Op) (logicalplan.Op, error) {
	switch op {
	case storagepb.Op_OP_EQ:
		return logicalplan.OpEq, nil
	case storagepb.Op_OP_NOT_EQ:
		return logicalplan.OpNotEq, nil
	case storagepb.Op_OP_LT:
		return logicalplan.OpLt, nil
	case storagepb.Op_OP_LT_EQ:
		return logicalplan.OpLtEq, nil
	case storagepb.Op_OP_GT:
		return logicalplan.OpGt, nil
	case storagepb.Op_OP_GT_EQ:
		return logicalplan.OpGtEq, nil
	case storagepb.Op_OP_REGEX_MATCH:
		return logicalplan.OpRegexMatch, nil
	case storagepb.Op_OP_REGEX_NOT_MATCH:
		return logicalplan.OpRegexNotMatch, nil
	case storagepb.Op_OP_AND:
		return logicalplan.OpAnd, nil
	case storagepb.Op_OP_OR:
		return logicalplan.OpOr, nil
	case storagepb.Op_OP_ADD:
		return logicalplan.OpAdd, nil
	case storagepb.Op_OP_SUB:
		return logicalplan.OpSub, nil
	case storagepb.Op_OP_MUL:
		return logicalplan.OpMul, nil
	case storagepb.Op_OP_DIV:
		return logicalplan.OpDiv, nil
	case storagepb.Op_OP_CONTAINS:
		return logicalplan.OpContains, nil
	case storagepb.Op_OP_NOT_CONTAINS:
		return logicalplan.OpNotContains, nil
	default:
		return logicalplan.OpUnknown, fmt.Errorf("unsupported op: %v", op)
	}
}

func protoAggFuncToLogicalAggFunc(f storagepb.AggregationFunction_Type) (logicalplan.AggFunc, error) {
	switch f {
	case storagepb.AggregationFunction_TYPE_SUM:
		return logicalplan.AggFuncSum, nil
	case storagepb.AggregationFunction_TYPE_MIN:
		return logicalplan.AggFuncMin, nil
	case storagepb.AggregationFunction_TYPE_MAX:
		return logicalplan.AggFuncMax, nil
	case storagepb.AggregationFunction_TYPE_COUNT:
		return logicalplan.AggFuncCount, nil
	default:
		return logicalplan.AggFuncUnknown, fmt.Errorf("unsupported agg func: %v", f)
	}
}

func protoLiteralToArrowScalar(lit *storagepb.Literal) (scalar.Scalar, error) {
	switch val := lit.Content.Value.(type) {
	case *storagepb.LiteralContent_NullValue:
		return scalar.ScalarNull, nil
	case *storagepb.LiteralContent_BoolValue:
		return scalar.NewBooleanScalar(val.BoolValue), nil
	case *storagepb.LiteralContent_Int32Value:
		return scalar.NewInt32Scalar(val.Int32Value), nil
	case *storagepb.LiteralContent_Uint32Value:
		return scalar.NewUint32Scalar(val.Uint32Value), nil
	case *storagepb.LiteralContent_Int64Value:
		return scalar.NewInt64Scalar(val.Int64Value), nil
	case *storagepb.LiteralContent_Uint64Value:
		return scalar.NewUint64Scalar(val.Uint64Value), nil
	case *storagepb.LiteralContent_FloatValue:
		return scalar.NewFloat32Scalar(val.FloatValue), nil
	case *storagepb.LiteralContent_DoubleValue:
		return scalar.NewFloat64Scalar(val.DoubleValue), nil
	case *storagepb.LiteralContent_BinaryValue:
		return scalar.NewBinaryScalar(memory.NewBufferBytes(val.BinaryValue), arrow.BinaryTypes.Binary), nil
	case *storagepb.LiteralContent_StringValue:
		return scalar.NewStringScalar(val.StringValue), nil
	default:
		return nil, fmt.Errorf("unsupported literal type: %T", val)
	}
}

func ExprsToProtos(exprs []logicalplan.Expr) ([]*storagepb.Expr, error) {
	res := make([]*storagepb.Expr, 0, len(exprs))
	for _, e := range exprs {
		expr, err := ExprToProto(e)
		if err != nil {
			return nil, err
		}
		res = append(res, expr)
	}
	return res, nil
}

func ExprToProto(expr logicalplan.Expr) (*storagepb.Expr, error) {
	if expr == nil {
		return nil, nil
	}

	switch e := expr.(type) {
	case *logicalplan.BinaryExpr:
		return BinaryExprToProto(e)
	case *logicalplan.Column:
		return ColumnExprToProto(e)
	case *logicalplan.LiteralExpr:
		return LiteralExprToProto(e)
	case *logicalplan.DynamicColumn:
		return DynamicColumnExprToProto(e)
	case *logicalplan.AggregationFunction:
		return AggregationFunctionToProto(e)
	case *logicalplan.AliasExpr:
		return AliasExprToProto(e)
	case *logicalplan.DurationExpr:
		return DurationExprToProto(e)
	case *logicalplan.ConvertExpr:
		return ConvertExprToProto(e)
	case *logicalplan.IfExpr:
		return IfExprToProto(e)
	default:
		return nil, fmt.Errorf("unsupported expression type: %T", e)
	}
}

func IfExprToProto(e *logicalplan.IfExpr) (*storagepb.Expr, error) {
	cond, err := ExprToProto(e.Cond)
	if err != nil {
		return nil, err
	}
	yes, err := ExprToProto(e.Then)
	if err != nil {
		return nil, err
	}
	no, err := ExprToProto(e.Else)
	if err != nil {
		return nil, err
	}
	return &storagepb.Expr{
		Def: &storagepb.ExprDef{
			Content: &storagepb.ExprDef_If{
				If: &storagepb.IfExpr{
					Condition: cond,
					Then:      yes,
					Else:      no,
				},
			},
		},
	}, nil
}

func ConvertExprToProto(e *logicalplan.ConvertExpr) (*storagepb.Expr, error) {
	expr, err := ExprToProto(e.Expr)
	if err != nil {
		return nil, err
	}

	t, err := arrowTypeToProto(e.Type)
	if err != nil {
		return nil, err
	}

	return &storagepb.Expr{
		Def: &storagepb.ExprDef{
			Content: &storagepb.ExprDef_Convert{
				Convert: &storagepb.ConvertExpr{
					Expr: expr,
					Type: t,
				},
			},
		},
	}, nil
}

func arrowTypeToProto(t arrow.DataType) (storagepb.Type, error) {
	switch t {
	case arrow.PrimitiveTypes.Float64:
		return storagepb.Type_TYPE_FLOAT64, nil
	default:
		return storagepb.Type_TYPE_UNKNOWN_UNSPECIFIED, fmt.Errorf("unsupported type: %v", t)
	}
}

func AliasExprToProto(e *logicalplan.AliasExpr) (*storagepb.Expr, error) {
	expr, err := ExprToProto(e.Expr)
	if err != nil {
		return nil, err
	}

	return &storagepb.Expr{
		Def: &storagepb.ExprDef{
			Content: &storagepb.ExprDef_Alias{
				Alias: &storagepb.Alias{
					Name: e.Alias,
					Expr: expr,
				},
			},
		},
	}, nil
}

func BinaryExprToProto(e *logicalplan.BinaryExpr) (*storagepb.Expr, error) {
	left, err := ExprToProto(e.Left)
	if err != nil {
		return nil, err
	}

	right, err := ExprToProto(e.Right)
	if err != nil {
		return nil, err
	}

	op, err := logicalOpToProtoOp(e.Op)
	if err != nil {
		return nil, err
	}

	return &storagepb.Expr{
		Def: &storagepb.ExprDef{
			Content: &storagepb.ExprDef_BinaryExpr{
				BinaryExpr: &storagepb.BinaryExpr{
					Left:  left,
					Op:    op,
					Right: right,
				},
			},
		},
	}, nil
}

func logicalOpToProtoOp(op logicalplan.Op) (storagepb.Op, error) {
	switch op {
	case logicalplan.OpEq:
		return storagepb.Op_OP_EQ, nil
	case logicalplan.OpNotEq:
		return storagepb.Op_OP_NOT_EQ, nil
	case logicalplan.OpLt:
		return storagepb.Op_OP_LT, nil
	case logicalplan.OpLtEq:
		return storagepb.Op_OP_LT_EQ, nil
	case logicalplan.OpGt:
		return storagepb.Op_OP_GT, nil
	case logicalplan.OpGtEq:
		return storagepb.Op_OP_GT_EQ, nil
	case logicalplan.OpRegexMatch:
		return storagepb.Op_OP_REGEX_MATCH, nil
	case logicalplan.OpRegexNotMatch:
		return storagepb.Op_OP_REGEX_NOT_MATCH, nil
	case logicalplan.OpAnd:
		return storagepb.Op_OP_AND, nil
	case logicalplan.OpOr:
		return storagepb.Op_OP_OR, nil
	case logicalplan.OpAdd:
		return storagepb.Op_OP_ADD, nil
	case logicalplan.OpSub:
		return storagepb.Op_OP_SUB, nil
	case logicalplan.OpMul:
		return storagepb.Op_OP_MUL, nil
	case logicalplan.OpDiv:
		return storagepb.Op_OP_DIV, nil
	case logicalplan.OpContains:
		return storagepb.Op_OP_CONTAINS, nil
	case logicalplan.OpNotContains:
		return storagepb.Op_OP_NOT_CONTAINS, nil
	default:
		return storagepb.Op_OP_UNKNOWN_UNSPECIFIED, fmt.Errorf("unsupported op: %v", op)
	}
}

func ColumnExprToProto(e *logicalplan.Column) (*storagepb.Expr, error) {
	return &storagepb.Expr{
		Def: &storagepb.ExprDef{
			Content: &storagepb.ExprDef_Column{
				Column: &storagepb.Column{
					Name: e.ColumnName,
				},
			},
		},
	}, nil
}

func DynamicColumnExprToProto(e *logicalplan.DynamicColumn) (*storagepb.Expr, error) {
	return &storagepb.Expr{
		Def: &storagepb.ExprDef{
			Content: &storagepb.ExprDef_DynamicColumn{
				DynamicColumn: &storagepb.DynamicColumn{
					Name: e.ColumnName,
				},
			},
		},
	}, nil
}

func LiteralExprToProto(e *logicalplan.LiteralExpr) (*storagepb.Expr, error) {
	val, err := scalarToLiteral(e.Value)
	if err != nil {
		return nil, err
	}

	return &storagepb.Expr{
		Def: &storagepb.ExprDef{
			Content: &storagepb.ExprDef_Literal{
				Literal: val,
			},
		},
	}, nil
}

func scalarToLiteral(s scalar.Scalar) (*storagepb.Literal, error) {
	switch s := s.(type) {
	case *scalar.Null:
		return &storagepb.Literal{
			Content: &storagepb.LiteralContent{
				Value: &storagepb.LiteralContent_NullValue{
					NullValue: &storagepb.Null{},
				},
			},
		}, nil
	case *scalar.Boolean:
		return &storagepb.Literal{
			Content: &storagepb.LiteralContent{
				Value: &storagepb.LiteralContent_BoolValue{
					BoolValue: s.Value,
				},
			},
		}, nil
	case *scalar.Int32:
		return &storagepb.Literal{
			Content: &storagepb.LiteralContent{
				Value: &storagepb.LiteralContent_Int32Value{
					Int32Value: s.Value,
				},
			},
		}, nil
	case *scalar.Uint32:
		return &storagepb.Literal{
			Content: &storagepb.LiteralContent{
				Value: &storagepb.LiteralContent_Uint32Value{
					Uint32Value: s.Value,
				},
			},
		}, nil
	case *scalar.Int64:
		return &storagepb.Literal{
			Content: &storagepb.LiteralContent{
				Value: &storagepb.LiteralContent_Int64Value{
					Int64Value: s.Value,
				},
			},
		}, nil
	case *scalar.Uint64:
		return &storagepb.Literal{
			Content: &storagepb.LiteralContent{
				Value: &storagepb.LiteralContent_Uint64Value{
					Uint64Value: s.Value,
				},
			},
		}, nil
	case *scalar.Float32:
		return &storagepb.Literal{
			Content: &storagepb.LiteralContent{
				Value: &storagepb.LiteralContent_FloatValue{
					FloatValue: s.Value,
				},
			},
		}, nil
	case *scalar.Float64:
		return &storagepb.Literal{
			Content: &storagepb.LiteralContent{
				Value: &storagepb.LiteralContent_DoubleValue{
					DoubleValue: s.Value,
				},
			},
		}, nil
	case *scalar.Binary:
		return &storagepb.Literal{
			Content: &storagepb.LiteralContent{
				Value: &storagepb.LiteralContent_BinaryValue{
					BinaryValue: s.Data(),
				},
			},
		}, nil
	case *scalar.String:
		return &storagepb.Literal{
			Content: &storagepb.LiteralContent{
				Value: &storagepb.LiteralContent_StringValue{
					StringValue: s.String(),
				},
			},
		}, nil
	default:
		return nil, errors.New("unsupported scalar type " + s.DataType().Name())
	}
}

func AggregationFunctionToProto(e *logicalplan.AggregationFunction) (*storagepb.Expr, error) {
	expr, err := ExprToProto(e.Expr)
	if err != nil {
		return nil, err
	}

	f, err := logicalAggFuncToProto(e.Func)
	if err != nil {
		return nil, err
	}

	return &storagepb.Expr{
		Def: &storagepb.ExprDef{
			Content: &storagepb.ExprDef_AggregationFunction{
				AggregationFunction: &storagepb.AggregationFunction{
					Type: f,
					Expr: expr,
				},
			},
		},
	}, nil
}

func logicalAggFuncToProto(f logicalplan.AggFunc) (storagepb.AggregationFunction_Type, error) {
	switch f {
	case logicalplan.AggFuncSum:
		return storagepb.AggregationFunction_TYPE_SUM, nil
	case logicalplan.AggFuncMin:
		return storagepb.AggregationFunction_TYPE_MIN, nil
	case logicalplan.AggFuncMax:
		return storagepb.AggregationFunction_TYPE_MAX, nil
	case logicalplan.AggFuncCount:
		return storagepb.AggregationFunction_TYPE_COUNT, nil
	default:
		return storagepb.AggregationFunction_TYPE_UNKNOWN_UNSPECIFIED, errors.New("unsupported aggregation function")
	}
}

func DurationExprToProto(e *logicalplan.DurationExpr) (*storagepb.Expr, error) {
	return &storagepb.Expr{
		Def: &storagepb.ExprDef{
			Content: &storagepb.ExprDef_Duration{
				Duration: &storagepb.DurationExpr{
					Milliseconds: e.Value().Milliseconds(),
				},
			},
		},
	}, nil
}
