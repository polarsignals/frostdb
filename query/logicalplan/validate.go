package logicalplan

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/parquet-go/parquet-go/format"
)

// PlanValidationError is the error representing a logical plan that is not valid.
type PlanValidationError struct {
	message  string
	plan     *LogicalPlan
	children []*ExprValidationError
	input    *PlanValidationError
}

// PlanValidationError.Error prints the error message in a human-readable format.
// implements the error interface.
func (e *PlanValidationError) Error() string {
	message := make([]string, 0)
	message = append(message, e.message)
	message = append(message, "\n")
	message = append(message, e.plan.String())

	for _, child := range e.children {
		message = append(message, "\n  -> invalid expression: ")
		message = append(message, child.Error())
		message = append(message, "\n")
	}

	if e.input != nil {
		message = append(message, "-> invalid input: ")
		message = append(message, e.input.Error())
	}

	return strings.Join(message, "")
}

// ExprValidationError is the error for an invalid expression that was found during validation.
type ExprValidationError struct {
	message  string
	expr     Expr
	children []*ExprValidationError
}

// ExprValidationError.Error prints the error message in a human-readable format.
// implements the error interface.
func (e *ExprValidationError) Error() string {
	message := make([]string, 0)
	message = append(message, e.message)
	message = append(message, ": ")
	message = append(message, e.expr.String())
	for _, child := range e.children {
		message = append(message, "\n     -> invalid sub-expression: ")
		message = append(message, child.Error())
	}

	return strings.Join(message, "")
}

// Validate validates the logical plan.
func Validate(plan *LogicalPlan) error {
	err := ValidateSingleFieldSet(plan)
	if err == nil {
		switch {
		case plan.SchemaScan != nil:
			err = ValidateSchemaScan(plan)
		case plan.TableScan != nil:
			err = ValidateTableScan(plan)
		case plan.Filter != nil:
			err = ValidateFilter(plan)
		case plan.Distinct != nil:
			err = nil
		case plan.Projection != nil:
			err = nil
		case plan.Aggregation != nil:
			err = ValidateAggregation(plan)
		}
	}

	// traverse backwards up the plan to validate all inputs
	inputErr := ValidateInput(plan)
	if inputErr != nil {
		if err == nil {
			err = inputErr
		} else {
			err.input = inputErr
		}
	}

	if err != nil {
		return err
	}
	return nil
}

// ValidateSingleFieldSet checks that only a single field is set on the plan.
func ValidateSingleFieldSet(plan *LogicalPlan) *PlanValidationError {
	fieldsSet := make([]int, 0)
	if plan.SchemaScan != nil {
		fieldsSet = append(fieldsSet, 0)
	}
	if plan.TableScan != nil {
		fieldsSet = append(fieldsSet, 1)
	}
	if plan.Filter != nil {
		fieldsSet = append(fieldsSet, 2)
	}
	if plan.Distinct != nil {
		fieldsSet = append(fieldsSet, 3)
	}
	if plan.Projection != nil {
		fieldsSet = append(fieldsSet, 4)
	}
	if plan.Aggregation != nil {
		fieldsSet = append(fieldsSet, 5)
	}
	if plan.Limit != nil {
		fieldsSet = append(fieldsSet, 6)
	}
	if plan.Sample != nil {
		fieldsSet = append(fieldsSet, 7)
	}

	if len(fieldsSet) != 1 {
		fieldsFound := make([]string, 0)
		fields := []string{"SchemaScan", "TableScan", "Filter", "Distinct", "Projection", "Aggregation", "Limit", "Sample"}
		for _, i := range fieldsSet {
			fieldsFound = append(fieldsFound, fields[i])
		}

		message := make([]string, 0)
		message = append(message,
			fmt.Sprintf("invalid number of fields. expected: 1, found: %d (%s). plan must only have one of the following: ",
				len(fieldsSet),
				strings.Join(fieldsFound, ", "),
			),
		)
		message = append(message, strings.Join(fields, ", "))

		return &PlanValidationError{
			plan:    plan,
			message: strings.Join(message, ""),
		}
	}
	return nil
}

func ValidateSchemaScan(plan *LogicalPlan) *PlanValidationError {
	if plan.SchemaScan.TableProvider == nil {
		return &PlanValidationError{
			plan:    plan,
			message: "table provider must not be nil",
		}
	}

	if plan.SchemaScan.TableName == "" {
		return &PlanValidationError{
			plan:    plan,
			message: "table name must not be empty",
		}
	}

	tableReader, err := plan.SchemaScan.TableProvider.GetTable(plan.SchemaScan.TableName)
	if err != nil {
		return &PlanValidationError{
			plan:    plan,
			message: fmt.Sprintf("failed to get table: %s", err),
		}
	}
	if tableReader == nil {
		return &PlanValidationError{
			plan:    plan,
			message: "table not found",
		}
	}

	schema := tableReader.Schema()
	if schema == nil {
		return &PlanValidationError{
			plan:    plan,
			message: "table schema must not be nil",
		}
	}

	return nil
}

func ValidateTableScan(plan *LogicalPlan) *PlanValidationError {
	if plan.TableScan.TableProvider == nil {
		return &PlanValidationError{
			plan:    plan,
			message: "table provider must not be nil",
		}
	}

	if plan.TableScan.TableName == "" {
		return &PlanValidationError{
			plan:    plan,
			message: "table name must not be empty",
		}
	}

	tableReader, err := plan.TableScan.TableProvider.GetTable(plan.TableScan.TableName)
	if err != nil {
		return &PlanValidationError{
			plan:    plan,
			message: fmt.Sprintf("failed to get table: %s", err),
		}
	}
	if tableReader == nil {
		return &PlanValidationError{
			plan:    plan,
			message: "table not found",
		}
	}

	schema := tableReader.Schema()
	if schema == nil {
		return &PlanValidationError{
			plan:    plan,
			message: "table schema must not be nil",
		}
	}

	return nil
}

// ValidateAggregation validates the logical plan's aggregation step.
func ValidateAggregation(plan *LogicalPlan) *PlanValidationError {
	// check that the expression is not nil
	if len(plan.Aggregation.AggExprs) == 0 {
		return &PlanValidationError{
			plan:    plan,
			message: "invalid aggregation: expression cannot be nil",
		}
	}

	// check that the expression is valid
	aggExprError := ValidateAggregationExpr(plan)
	if aggExprError != nil {
		return &PlanValidationError{
			plan:     plan,
			message:  "invalid aggregation",
			children: []*ExprValidationError{aggExprError},
		}
	}

	return nil
}

type Named interface {
	Name() string
}

func ValidateAggregationExpr(plan *LogicalPlan) *ExprValidationError {
	for _, expr := range plan.Aggregation.AggExprs {
		t, err := expr.Expr.DataType(plan.Input)
		if err != nil {
			return &ExprValidationError{
				expr:    expr.Expr,
				message: fmt.Errorf("get type of expression to aggregate: %w", err).Error(),
			}
		}

		if t == nil {
			return &ExprValidationError{
				expr:    expr.Expr,
				message: "invalid aggregation: expression type cannot be determined",
			}
		}

		switch expr.Func {
		case AggFuncSum, AggFuncMin, AggFuncMax, AggFuncCount, AggFuncAvg, AggFuncUnique:
			switch t {
			case
				arrow.PrimitiveTypes.Int64,
				arrow.PrimitiveTypes.Uint64,
				arrow.PrimitiveTypes.Float64:
				// valid
			default:
				return &ExprValidationError{
					expr:    expr.Expr,
					message: fmt.Errorf("invalid aggregation: expression type %s is not supported", t).Error(),
				}
			}
		case AggFuncAnd:
			if t != arrow.FixedWidthTypes.Boolean {
				return &ExprValidationError{
					expr:    expr.Expr,
					message: fmt.Errorf("invalid aggregation: and aggregations can only aggregate bool type expressions, not %s", t).Error(),
				}
			}
		}
	}

	return nil
}

// ValidateInput validates that the current logical plans input is valid.
// It returns nil if the plan has no input.
func ValidateInput(plan *LogicalPlan) *PlanValidationError {
	if plan.Input != nil {
		inputErr := Validate(plan.Input)
		if inputErr != nil {
			inputValidationErr, ok := inputErr.(*PlanValidationError)
			if !ok {
				// if we are here it is a bug in the code
				panic(fmt.Sprintf("Unexpected error: %v expected a PlanValidationError", inputErr))
			}
			return inputValidationErr
		}
	}
	return nil
}

// ValidateFilter validates the logical plan's filter step.
func ValidateFilter(plan *LogicalPlan) *PlanValidationError {
	if err := ValidateFilterExpr(plan, plan.Filter.Expr); err != nil {
		return &PlanValidationError{
			message:  "invalid filter",
			plan:     plan,
			children: []*ExprValidationError{err},
		}
	}
	return nil
}

// ValidateFilterExpr validates filter's expression.
func ValidateFilterExpr(plan *LogicalPlan, e Expr) *ExprValidationError {
	switch expr := e.(type) {
	case *BinaryExpr:
		err := ValidateFilterBinaryExpr(plan, expr)
		return err
	}

	return nil
}

// ValidateFilterBinaryExpr validates the filter's binary expression.
func ValidateFilterBinaryExpr(plan *LogicalPlan, expr *BinaryExpr) *ExprValidationError {
	if expr.Op == OpAnd || expr.Op == OpOr {
		return ValidateFilterAndBinaryExpr(plan, expr)
	}

	// try to find the column expression on the left side of the binary expression
	leftColumnFinder := newTypeFinder((*Column)(nil))
	expr.Left.Accept(&leftColumnFinder)
	if leftColumnFinder.result == nil {
		return &ExprValidationError{
			message: "left side of binary expression must be a column",
			expr:    expr,
		}
	}

	// try to find the column in the schema
	columnExpr := leftColumnFinder.result.(*Column)
	schema := plan.InputSchema()
	if schema != nil {
		column, found := schema.ColumnByName(columnExpr.ColumnName)
		if found {
			// try to find the literal on the other side of the expression
			rightLiteralFinder := newTypeFinder((*LiteralExpr)(nil))
			expr.Right.Accept(&rightLiteralFinder)
			if rightLiteralFinder.result != nil {
				// ensure that the column type is compatible with the literal being compared to it
				t := column.StorageLayout.Type()
				literalExpr := rightLiteralFinder.result.(*LiteralExpr)
				if err := ValidateComparingTypes(t.LogicalType(), literalExpr.Value); err != nil {
					err.expr = expr
					return err
				}
			}
		}
	}

	return nil
}

// ValidateComparingTypes validates if the types being compared by a binary expression are compatible.
func ValidateComparingTypes(columnType *format.LogicalType, literal scalar.Scalar) *ExprValidationError {
	switch {
	// if the columns logical type is nil, it may be of type bool
	case columnType == nil:
		switch t := literal.(type) {
		case *scalar.Boolean:
			return nil
		default:
			return &ExprValidationError{
				// TODO: this is probably correct? We should probably rewrite the query to be comparing against a bool I think...
				message: fmt.Sprintf("incompatible types: nil logical type column cannot be compared with %v", t),
			}
		}

	// if the column is a string type, it shouldn't be compared to a number
	case columnType.UTF8 != nil:
		switch literal.(type) {
		case *scalar.Float64:
			return &ExprValidationError{
				message: "incompatible types: string column cannot be compared with numeric literal",
			}
		case *scalar.Int64:
			return &ExprValidationError{
				message: "incompatible types: string column cannot be compared with numeric literal",
			}
		}
	// if the column is a numeric type, it shouldn't be compared to a string
	case columnType.Integer != nil:
		switch literal.(type) {
		case *scalar.String:
			return &ExprValidationError{
				message: "incompatible types: numeric column cannot be compared with string literal",
			}
		}
	}
	return nil
}

// ValidateFilterAndBinaryExpr validates the filter's binary expression where Op = AND.
func ValidateFilterAndBinaryExpr(plan *LogicalPlan, expr *BinaryExpr) *ExprValidationError {
	leftErr := ValidateFilterExpr(plan, expr.Left)
	rightErr := ValidateFilterExpr(plan, expr.Right)

	if leftErr != nil || rightErr != nil {
		message := make([]string, 0, 3)
		message = append(message, "invalid children:")

		validationErr := ExprValidationError{
			expr:     expr,
			children: make([]*ExprValidationError, 0),
		}

		if leftErr != nil {
			lve := leftErr
			message = append(message, "left")
			validationErr.children = append(validationErr.children, lve)
		}

		if rightErr != nil {
			lve := rightErr
			message = append(message, "right")
			validationErr.children = append(validationErr.children, lve)
		}

		validationErr.message = strings.Join(message, " ")
		return &validationErr
	}
	return nil
}

// NewTypeFinder returns an instance of the findExpressionForTypeVisitor for the
// passed type. It expects to receive a pointer to the  type it is will find.
func newTypeFinder(val interface{}) findExpressionForTypeVisitor {
	return findExpressionForTypeVisitor{exprType: reflect.TypeOf(val)}
}

// findExpressionForTypeVisitor is an instance of Visitor that will try to find
// an expression of the given type while visiting the expressions.
type findExpressionForTypeVisitor struct {
	exprType reflect.Type
	// if an expression of the type is found, it will be set on this field after
	// visiting. Other-wise this field will be nil
	result Expr
}

func (v *findExpressionForTypeVisitor) PreVisit(_ Expr) bool {
	return true
}

func (v *findExpressionForTypeVisitor) Visit(_ Expr) bool {
	return true
}

func (v *findExpressionForTypeVisitor) PostVisit(expr Expr) bool {
	found := v.exprType == reflect.TypeOf(expr)
	if found {
		v.result = expr
	}
	return !found
}
