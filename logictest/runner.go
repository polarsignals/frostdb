package logictest

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/cockroachdb/datadriven"
	"github.com/google/uuid"

	"github.com/polarsignals/frostdb/dynparquet"
	schemapb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/schema/v1alpha1"
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
	// exec executes a SQL statement specified in the input against the active
	// table.
	// Example usage: exec [unordered]
	// Arguments:
	// - unordered
	// The unordered flag is optional and specifies that the expected output
	// should not be compared in ordered fashion with the actual output.
	execCmd = "exec"
)

type DB interface {
	CreateTable(name string, schema *schemapb.Schema) (Table, error)
	// ScanTable returns a query.Builder prepared to scan the given table.
	ScanTable(name string) query.Builder
}

type Table interface {
	Schema() *dynparquet.Schema
	InsertRecord(context.Context, arrow.Record) (uint64, error)
}

type Runner struct {
	db      DB
	schemas map[string]*schemapb.Schema

	// maps table name -> schema
	tableSchema               map[string]*schemapb.Schema
	activeTable               Table
	activeTableName           string
	activeTableDynamicColumns []string
	sqlParser                 *sqlparse.Parser
}

func NewRunner(db DB, schemas map[string]*schemapb.Schema) *Runner {
	return &Runner{
		db:          db,
		schemas:     schemas,
		tableSchema: make(map[string]*schemapb.Schema),
		sqlParser:   sqlparse.NewParser(),
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

func (r *Runner) handleCreateTable(_ context.Context, c *datadriven.TestData) (string, error) {
	var schema *schemapb.Schema
	for _, arg := range c.CmdArgs {
		if arg.Key == "schema" {
			if len(arg.Vals) != 1 {
				return "", fmt.Errorf("createtable: unexpected schema values %v", arg.Vals)
			}
			schema = r.schemas[arg.Vals[0]]
		}
	}

	if schema == nil {
		return "", fmt.Errorf("createtable: schema not found")
	}

	name := uuid.NewString()
	table, err := r.db.CreateTable(name, schema)
	if err != nil {
		return "", nil
	}
	r.activeTable = table
	r.activeTableName = name
	r.tableSchema[name] = schema
	for _, c := range table.Schema().Columns() {
		if c.Dynamic {
			r.activeTableDynamicColumns = append(r.activeTableDynamicColumns, c.Name)
		}
	}
	return c.Expected, nil
}

func (r *Runner) handleInsert(ctx context.Context, c *datadriven.TestData) (string, error) {
	schema := r.tableSchema[r.activeTableName]
	var build *array.RecordBuilder
	var builds []buildFunc
	var err error
	for _, arg := range c.CmdArgs {
		if arg.Key == "cols" {
			build, builds, err = recordBuilder(schema, arg.Vals)
			if err != nil {
				return "", err
			}
		}
	}

	if len(builds) == 0 {
		return "", fmt.Errorf("insert: no input schema provided")
	}
	defer build.Release()
	inputLines := strings.Split(c.Input, "\n")
	for i, line := range inputLines {
		values := strings.Fields(line)
		if len(values) != len(builds) {
			return "", fmt.Errorf(
				"insert: row %d (%d values) does not match expected schema (%d cols)",
				i+1,
				len(values),
				len(builds),
			)
		}
		for col := range values {
			err := builds[col](values[col])
			if err != nil {
				return "", err
			}
		}
	}

	rec := build.NewRecord()
	defer rec.Release()

	if _, err := r.activeTable.InsertRecord(ctx, rec); err != nil {
		return "", fmt.Errorf("insert: %w", err)
	}

	return c.Expected, nil
}

func recordBuilder(schema *schemapb.Schema, columns []string) (*array.RecordBuilder, []buildFunc, error) {
	names := make(map[string]*schemapb.Column)
	for _, col := range schema.Columns {
		names[col.Name] = col
	}
	fields := make([]arrow.Field, len(columns))
	for i := range columns {
		column := columns[i]
		if strings.Contains(column, ".") {
			base := strings.Split(column, ".")[0]
			def, ok := names[base]
			if !ok {
				return nil, nil, fmt.Errorf("column %q is missing from active schema", column)
			}
			fields[i] = arrow.Field{
				Name:     column,
				Nullable: true,
				Type:     layoutToArrowField(def.StorageLayout),
			}
			continue
		}
		def, ok := names[column]
		if !ok {
			return nil, nil, fmt.Errorf("column %q is missing from active schema", column)
		}
		fields[i] = arrow.Field{
			Name:     def.Name,
			Nullable: def.StorageLayout.Nullable,
			Type:     layoutToArrowField(def.StorageLayout),
		}
	}
	r := array.NewRecordBuilder(memory.NewGoAllocator(),
		arrow.NewSchema(fields, nil),
	)
	builds := make([]buildFunc, len(fields))
	for i := range fields {
		builds[i] = newBuild(r.Field(i), fields[i])
	}
	return r, builds, nil
}

type buildFunc func(s string) error

func newBuild(b array.Builder, f arrow.Field) buildFunc {
	switch e := b.(type) {
	case *array.Int64Builder:
		return func(s string) error {
			if f.Nullable {
				if s == "null" {
					e.AppendNull()
					return nil
				}
			}
			v, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return err
			}
			e.Append(v)
			return nil
		}
	case *array.Float64Builder:
		return func(s string) error {
			if f.Nullable {
				if s == "null" {
					e.AppendNull()
					return nil
				}
			}
			v, err := strconv.ParseFloat(s, 64)
			if err != nil {
				return err
			}
			e.Append(v)
			return nil
		}
	case *array.BooleanBuilder:
		return func(s string) error {
			if f.Nullable {
				if s == "null" {
					e.AppendNull()
					return nil
				}
			}
			v, err := strconv.ParseBool(s)
			if err != nil {
				return err
			}
			e.Append(v)
			return nil
		}
	case *array.StringBuilder:
		return func(s string) error {
			e.AppendString(s)
			return nil
		}
		// There is no need to handle more dictionaries . For now only string
		// dictionary is enough. More can be added when needed.
	case *array.BinaryDictionaryBuilder:
		return func(s string) error {
			e.AppendString(s)
			return nil
		}
	default:
		panic(fmt.Sprintf("unexpected  array builder type %T", e))
	}
}

func layoutToArrowField(layout *schemapb.StorageLayout) arrow.DataType {
	var typ arrow.DataType
	switch layout.Type {
	case schemapb.StorageLayout_TYPE_INT64:
		typ = arrow.PrimitiveTypes.Int64
	case schemapb.StorageLayout_TYPE_DOUBLE:
		typ = arrow.PrimitiveTypes.Float64
	case schemapb.StorageLayout_TYPE_BOOL:
		typ = arrow.FixedWidthTypes.Boolean
	case schemapb.StorageLayout_TYPE_STRING:
		typ = arrow.BinaryTypes.Binary
	}
	if layout.Encoding == schemapb.StorageLayout_ENCODING_RLE_DICTIONARY {
		typ = &arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Int32,
			ValueType: typ,
		}
	}
	if layout.Repeated {
		typ = arrow.ListOf(typ)
	}
	return typ
}

func (r *Runner) handleExec(ctx context.Context, c *datadriven.TestData) (string, error) {
	unordered := false
	for _, arg := range c.CmdArgs {
		if arg.Key == "unordered" {
			unordered = true
			break
		}
	}
	res, err := r.parseSQL(r.activeTableDynamicColumns, c.Input)
	if err != nil {
		return "", fmt.Errorf("exec: parse err: %w", err)
	}

	if res.Explain {
		// This plan should be explained. Note that we need to execute an
		// explain this way because we have no notion of building an explain
		// statement.
		return res.Plan.Explain(ctx)
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

	var results []string
	if err := res.Plan.Execute(ctx, func(_ context.Context, ar arrow.Record) error {
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
			results = append(results, strings.Join(rowStrings, "\t")+"\n")
		}
		return nil
	}); err != nil {
		return "", err
	}

	if unordered {
		// The test doesn't want to verify the ordering of the output. Sort the
		// output so that the results are deterministically ordered
		// independently of the execution engine.
		sort.Strings(results)
	}

	for _, result := range results {
		if _, err := w.Write([]byte(result)); err != nil {
			return "", err
		}
	}
	if err := w.Flush(); err != nil {
		return "", err
	}
	return b.String(), nil
}

func (r *Runner) parseSQL(dynColNames []string, sql string) (sqlparse.ParseResult, error) {
	res, err := r.sqlParser.ExperimentalParse(
		r.db.ScanTable(r.activeTableName), dynColNames, sql,
	)
	if err != nil {
		return sqlparse.ParseResult{}, err
	}

	return res, nil
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
	case *array.Float64:
		for i := range result {
			if col.IsNull(i) {
				result[i] = nullString
				continue
			}
			result[i] = fmt.Sprintf("%f", float64(col.Value(i)))
		}
	case *array.Boolean:
		for i := range result {
			if col.IsNull(i) {
				result[i] = nullString
				continue
			}
			result[i] = fmt.Sprintf("%t", col.Value(i))
		}
	case *array.Dictionary:
		switch dict := col.Dictionary().(type) {
		case *array.Binary:
			for i := range result {
				if col.IsNull(i) {
					result[i] = nullString
					continue
				}
				result[i] = string(dict.Value(col.GetValueIndex(i)))
			}
		default:
			return nil, fmt.Errorf("unhandled dictionary type: %T", dict)
		}
	default:
		return nil, fmt.Errorf("unhandled type %T", col)
	}
	return result, nil
}
