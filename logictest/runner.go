package logictest

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/cockroachdb/datadriven"
	"github.com/google/uuid"
	"github.com/segmentio/parquet-go"

	"github.com/polarsignals/frostdb"
	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/query"
	"github.com/polarsignals/frostdb/sqlparse"
)

const nullString = "null"

const (
	// createtable creates a table and sets it as the active table. All
	// following commands will execute against this table.
	// Example usage: createtable schema=default
	// Arguments:
	// - schema
	// The schema flag specifies the schema to use for the given table. The
	// default value specifies the default schema (as returned by
	// dynparquet.NewSampleSchema).
	// TODO(asubiotto): Implement custom schemas. The idea is to introduce a new
	// schema=custom value that will then allow the user to specify columns
	// as part of the input. Insert logic must also be extended to support
	// these.
	createTableCmd = "createtable"
	// insert inserts data into the active table. The columns to insert data
	// into are specified with the cols=(<colname1>, <colname2>) argument. Only
	// the columns to insert data into must be specified. Columns that are not
	// specified will have a NULL value for each row inserted.
	insertCmd = "insert"
	// exec has no arguments and executes a SQL statement specified in the
	// input against the active table.
	execCmd = "exec"
)

type Runner struct {
	db                        *frostdb.DB
	activeTable               *frostdb.Table
	activeTableName           string
	activeTableDynamicColumns []string
	sqlParser                 *sqlparse.Parser
}

func NewRunner(db *frostdb.DB) *Runner {
	return &Runner{
		db:        db,
		sqlParser: sqlparse.NewParser(),
	}
}

// RunCmd parses and runs datadriven command with the associated arguments, and
// returns the result.
func (r *Runner) RunCmd(ctx context.Context, c *datadriven.TestData) string {
	result, err := r.handleCmd(ctx, c)
	if err != nil {
		return err.Error()
	}
	return result
}

func (r *Runner) handleCmd(ctx context.Context, c *datadriven.TestData) (string, error) {
	switch c.Cmd {
	case createTableCmd:
		return r.handleCreateTable(ctx, c)
	case insertCmd:
		return r.handleInsert(ctx, c)
	case execCmd:
		return r.handleExec(ctx, c)
	}
	return "", fmt.Errorf("unknown command %s", c.Cmd)
}

func (r *Runner) handleCreateTable(ctx context.Context, c *datadriven.TestData) (string, error) {
	var schema *dynparquet.Schema
	for _, arg := range c.CmdArgs {
		if arg.Key == "schema" {
			if len(arg.Vals) != 1 && arg.Vals[0] != "default" {
				return "", fmt.Errorf("createtable: unexpected schema values %v", arg.Vals)
			}
			schema = dynparquet.NewSampleSchema()
		}
	}

	if schema == nil {
		return "", fmt.Errorf("createtable: schema not found")
	}

	name := uuid.NewString()
	table, err := r.db.Table(name, frostdb.NewTableConfig(schema))
	if err != nil {
		return "", nil
	}
	r.activeTable = table
	r.activeTableName = name
	for _, c := range schema.Columns() {
		if c.Dynamic {
			r.activeTableDynamicColumns = append(r.activeTableDynamicColumns, c.Name)
		}
	}
	return c.Expected, nil
}

type colDef struct {
	dynparquet.ColumnDefinition
	// This is the "label1" in "labels.label1". Only set if
	// ColumnDefinition.Dynamic is true.
	dynColName string
}

func (r *Runner) handleInsert(ctx context.Context, c *datadriven.TestData) (string, error) {
	var colDefs []colDef

	schema := r.activeTable.Schema()
	for _, arg := range c.CmdArgs {
		if arg.Key == "cols" {
			for _, cname := range arg.Vals {
				def := colDef{}
				dotIndex := strings.IndexRune(cname, '.')
				if dotIndex != -1 {
					// This is a dynamic column.
					def.dynColName = cname[dotIndex+1:]
					// Only search for the name before the ".".
					cname = cname[:dotIndex]
				}
				var found bool
				def.ColumnDefinition, found = schema.ColumnByName(cname)
				if !found {
					return "", fmt.Errorf(
						"insert: column %s specified in insert not found in schema %v",
						cname,
						schema,
					)
				}
				colDefs = append(colDefs, def)
			}
		}
	}

	if len(colDefs) == 0 {
		return "", fmt.Errorf("insert: no input schema provided")
	}

	dynCols := make(map[string][]string)
	specifiedCols := make(map[string]struct{})
	for _, def := range colDefs {
		if def.Dynamic {
			dynCols[def.Name] = append(dynCols[def.Name], def.dynColName)
		}
		specifiedCols[def.Name] = struct{}{}
	}

	inputLines := strings.Split(c.Input, "\n")
	rows := make([]parquet.Row, len(inputLines))
	for i := range rows {
		colIdx := 0
		values := strings.Fields(inputLines[i])
		if len(values) != len(colDefs) {
			return "", fmt.Errorf(
				"insert: row %d (%d values) does not match expected schema (%d cols)",
				i+1,
				len(values),
				len(colDefs),
			)
		}

		valueForCol := make(map[string]string)
		for j, def := range colDefs {
			key := def.Name
			if def.Dynamic {
				key += "." + def.dynColName
			}
			valueForCol[key] = values[j]
		}

		for _, col := range schema.Columns() {
			if _, ok := specifiedCols[col.Name]; !ok {
				// Column not specified. Insert a NULL value.
				rows[i] = append(rows[i], parquet.ValueOf(nil).Level(0, 0, colIdx))
				colIdx++
				continue
			}

			if !col.Dynamic {
				// Column is not dynamic.
				v, err := stringToValue(col.StorageLayout.Type(), valueForCol[col.Name])
				if err != nil {
					return "", fmt.Errorf("insert: %w", err)
				}
				rows[i] = append(rows[i], parquet.ValueOf(v).Level(0, 0, colIdx))
				colIdx++
				continue
			}

			for _, dynCol := range dynCols[col.Name] {
				v, err := stringToValue(col.StorageLayout.Type(), valueForCol[col.Name+"."+dynCol])
				if err != nil {
					return "", fmt.Errorf("insert: %w", err)
				}
				rows[i] = append(rows[i], parquet.ValueOf(v).Level(0, 1, colIdx))
				colIdx++
			}
		}
	}

	buf, err := schema.NewBuffer(dynCols)
	if err != nil {
		return "", fmt.Errorf("insert: %w", err)
	}

	if _, err := buf.WriteRows(rows); err != nil {
		return "", fmt.Errorf("insert: %w", err)
	}

	if _, err := r.activeTable.InsertBuffer(ctx, buf); err != nil {
		return "", fmt.Errorf("insert: %w", err)
	}

	return c.Expected, nil
}

func stringToValue(t parquet.Type, stringValue string) (any, error) {
	if stringValue == nullString {
		return nil, nil
	}

	switch t.Kind() {
	case parquet.ByteArray:
		return stringValue, nil
	case parquet.Int64:
		intValue, err := strconv.Atoi(stringValue)
		if err != nil {
			return nil, fmt.Errorf("unexpected error converting %s to int: %w", stringValue, err)
		}
		return intValue, nil
	default:
		return nil, fmt.Errorf("unhandled type %T", t.Kind())
	}
}

func (r *Runner) handleExec(ctx context.Context, c *datadriven.TestData) (string, error) {
	plan, err := r.parseSQL(r.activeTableDynamicColumns, c.Input)
	if err != nil {
		return "", fmt.Errorf("exec: %w", err)
	}

	var b bytes.Buffer
	const (
		minWidth = 8
		tabWidth = 8
		padding  = 2
		padChar  = ' '
		noFlags  = 0
	)
	w := tabwriter.NewWriter(&b, minWidth, tabWidth, padding, padChar, noFlags)

	if err := plan.Execute(ctx, func(_ context.Context, ar arrow.Record) error {
		colStrings := make([][]string, ar.NumCols())
		for i, col := range ar.Columns() {
			stringVals, err := arrayToStringVals(col)
			if err != nil {
				return fmt.Errorf("exec: %w", err)
			}
			colStrings[i] = stringVals
		}

		for i := 0; i < int(ar.NumRows()); i++ {
			rowStrings := make([]string, 0, ar.NumCols())
			for _, col := range colStrings {
				rowStrings = append(rowStrings, col[i])
			}
			w.Write([]byte(strings.Join(rowStrings, "\t") + "\n"))
		}
		return nil
	}); err != nil {
		return "", err
	}

	w.Flush()
	return b.String(), nil
}

func (r *Runner) parseSQL(dynColNames []string, sql string) (query.Builder, error) {
	queryEngine := query.NewEngine(
		memory.NewGoAllocator(),
		r.db.TableProvider(),
	)

	query, err := r.sqlParser.ExperimentalParse(
		queryEngine.ScanTable(r.activeTableName), dynColNames, sql,
	)
	if err != nil {
		return nil, err
	}

	return query, nil
}

func arrayToStringVals(a arrow.Array) ([]string, error) {
	result := make([]string, a.Len())
	switch col := a.(type) {
	case *array.Binary:
		for i := range result {
			if col.IsNull(i) {
				result[i] = nullString
				continue
			}
			result[i] = col.ValueString(i)
		}
	case *array.Int64:
		for i := range result {
			if col.IsNull(i) {
				result[i] = nullString
				continue
			}
			result[i] = strconv.Itoa(int(col.Value(i)))
		}
	case *array.Boolean:
		for i := range result {
			if col.IsNull(i) {
				result[i] = nullString
				continue
			}
			result[i] = fmt.Sprintf("%t", col.Value(i))
		}
	default:
		return nil, fmt.Errorf("unhandled type %T", col)
	}
	return result, nil
}
