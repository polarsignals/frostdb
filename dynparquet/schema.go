package dynparquet

import (
	"bytes"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	"github.com/segmentio/parquet-go"
	"github.com/segmentio/parquet-go/compress"
	"github.com/segmentio/parquet-go/encoding"
	"github.com/segmentio/parquet-go/format"
	"google.golang.org/protobuf/proto"

	schemapb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/schema/v1alpha1"
	schemav2pb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/schema/v1alpha2"
)

const (
	// The size of the column indicies in parquet files.
	ColumnIndexSize = 16
)

// ColumnDefinition describes a column in a dynamic parquet schema.
type ColumnDefinition struct {
	Name          string
	StorageLayout parquet.Node
	Dynamic       bool
}

// SortingColumn describes a column to sort by in a dynamic parquet schema.
type SortingColumn interface {
	parquet.SortingColumn
	ColumnName() string
}

// Ascending constructs a SortingColumn value which dictates to sort by the column in ascending order.
func Ascending(column string) SortingColumn { return ascending(column) }

// Descending constructs a SortingColumn value which dictates to sort by the column in descending order.
func Descending(column string) SortingColumn { return descending(column) }

// NullsFirst wraps the SortingColumn passed as argument so that it instructs
// the row group to place null values first in the column.
func NullsFirst(sortingColumn SortingColumn) SortingColumn { return nullsFirst{sortingColumn} }

type ascending string

func (asc ascending) String() string     { return fmt.Sprintf("ascending(%s)", string(asc)) }
func (asc ascending) ColumnName() string { return string(asc) }
func (asc ascending) Path() []string     { return []string{string(asc)} }
func (asc ascending) Descending() bool   { return false }
func (asc ascending) NullsFirst() bool   { return false }

type descending string

func (desc descending) String() string     { return fmt.Sprintf("descending(%s)", string(desc)) }
func (desc descending) ColumnName() string { return string(desc) }
func (desc descending) Path() []string     { return []string{string(desc)} }
func (desc descending) Descending() bool   { return true }
func (desc descending) NullsFirst() bool   { return false }

type nullsFirst struct{ SortingColumn }

func (nf nullsFirst) String() string   { return fmt.Sprintf("nulls_first+%s", nf.SortingColumn) }
func (nf nullsFirst) NullsFirst() bool { return true }

func makeDynamicSortingColumn(dynamicColumnName string, sortingColumn SortingColumn) SortingColumn {
	return dynamicSortingColumn{
		dynamicColumnName: dynamicColumnName,
		SortingColumn:     sortingColumn,
	}
}

// dynamicSortingColumn is a SortingColumn which is a dynamic column.
type dynamicSortingColumn struct {
	SortingColumn
	dynamicColumnName string
}

func (dyn dynamicSortingColumn) String() string {
	return fmt.Sprintf("dynamic(%s, %v)", dyn.dynamicColumnName, dyn.SortingColumn)
}

func (dyn dynamicSortingColumn) ColumnName() string {
	return dyn.SortingColumn.ColumnName() + "." + dyn.dynamicColumnName
}

func (dyn dynamicSortingColumn) Path() []string { return []string{dyn.ColumnName()} }

// Schema is a dynamic parquet schema. It extends a parquet schema with the
// ability that any column definition that is dynamic will have columns
// dynamically created as their column name is seen for the first time.
type Schema struct {
	def            proto.Message
	columns        []ColumnDefinition
	columnIndexes  map[string]int
	sortingColumns []SortingColumn
	dynamicColumns []int

	writers *sync.Map
	buffers *sync.Map
}

// IsDynamicColumn returns true if the passed in column is a dynamic column.
func (s *Schema) IsDynamicColumn(col string) bool {
	return s.columns[s.columnIndexes[col]].Dynamic
}

func findLeavesFromNode(node *schemav2pb.Node) []ColumnDefinition {
	switch n := node.Type.(type) {
	case *schemav2pb.Node_Leaf:
		ret, err := storageLayoutToParquetNode(&v2storageLayoutWrapper{n.Leaf.StorageLayout})
		if err != nil {
			panic("sheisse")
		}
		return []ColumnDefinition{
			{
				Name:          n.Leaf.Name,
				StorageLayout: ret,
				Dynamic:       false, // TODO(can we get rid of dynamic cols): do we need dynamic columns to be separate?
			},
		}
	case *schemav2pb.Node_Group:
		columns := make([]ColumnDefinition, 0, len(n.Group.Nodes))
		for _, g := range n.Group.Nodes {
			columns = append(columns, findLeavesFromNode(g)...)
		}

		return columns
	default:
		panic(fmt.Sprintf("unknown node type: %v", n))
	}
}

func ParquetSchemaFromV2Definition(def *schemav2pb.Schema) *parquet.Schema {
	root := parquet.Group{}
	for _, node := range def.Root.Nodes {
		root[nameFromNodeDef(node)] = nodeFromDefinition(node)
	}

	return parquet.NewSchema(def.Root.Name, root)
}

func nodeFromDefinition(node *schemav2pb.Node) parquet.Node {
	switch n := node.Type.(type) {
	case *schemav2pb.Node_Leaf:
		ret, err := storageLayoutToParquetNode(&v2storageLayoutWrapper{n.Leaf.StorageLayout})
		if err != nil {
			panic("sheisse")
		}
		return ret
	case *schemav2pb.Node_Group:
		group := parquet.Group{}
		for _, g := range n.Group.Nodes {
			group[nameFromNodeDef(g)] = nodeFromDefinition(g)
		}

		var node parquet.Node
		node = group
		if n.Group.Nullable {
			node = parquet.Optional(node)
		}

		if n.Group.Repeated {
			node = parquet.Repeated(node)
		}

		return node
	default:
		panic(fmt.Sprintf("unknown node type: %v", n))
	}
}

func nameFromNodeDef(node *schemav2pb.Node) string {
	switch n := node.Type.(type) {
	case *schemav2pb.Node_Leaf:
		return n.Leaf.Name
	case *schemav2pb.Node_Group:
		return n.Group.Name
	default:
		panic(fmt.Sprintf("unknown node type: %v", n))
	}
}

func SchemaFromDefinition(msg proto.Message) (*Schema, error) {
	var columns []ColumnDefinition
	var sortingColumns []SortingColumn
	switch def := msg.(type) {
	case *schemapb.Schema:
		columns = make([]ColumnDefinition, 0, len(def.Columns))
		for _, col := range def.Columns {
			layout, err := storageLayoutToParquetNode(&v1storageLayoutWrapper{col.StorageLayout})
			if err != nil {
				return nil, err
			}
			columns = append(columns, ColumnDefinition{
				Name:          col.Name,
				StorageLayout: layout,
				Dynamic:       col.Dynamic,
			})
		}

		sortingColumns = make([]SortingColumn, 0, len(def.SortingColumns))
		for _, col := range def.SortingColumns {
			var sortingColumn SortingColumn
			switch col.Direction {
			case schemapb.SortingColumn_DIRECTION_ASCENDING:
				sortingColumn = Ascending(col.Name)
			case schemapb.SortingColumn_DIRECTION_DESCENDING:
				sortingColumn = Descending(col.Name)
			default:
				return nil, fmt.Errorf("unknown sorting direction %q, only \"ascending\", \"descending\" are valid choices", col.Direction)
			}
			if col.NullsFirst {
				sortingColumn = NullsFirst(sortingColumn)
			}
			sortingColumns = append(sortingColumns, sortingColumn)
		}
	case *schemav2pb.Schema:
		columns = []ColumnDefinition{}
		for _, node := range def.Root.Nodes {
			columns = append(columns, findLeavesFromNode(node)...)
		}

		sortingColumns = make([]SortingColumn, 0, len(def.SortingColumns))
		for _, col := range def.SortingColumns {
			var sortingColumn SortingColumn
			switch col.Direction {
			case schemav2pb.SortingColumn_DIRECTION_ASCENDING:
				sortingColumn = Ascending(col.Path)
			case schemav2pb.SortingColumn_DIRECTION_DESCENDING:
				sortingColumn = Descending(col.Path)
			default:
				return nil, fmt.Errorf("unknown sorting direction %q, only \"ascending\", \"descending\" are valid choices", col.Direction)
			}
			if col.NullsFirst {
				sortingColumn = NullsFirst(sortingColumn)
			}
			sortingColumns = append(sortingColumns, sortingColumn)
		}
	}

	return newSchema(
		msg,
		columns,
		sortingColumns,
	), nil
}

// DefinitionFromParquetFile converts a parquet file into a schemapb.Schema.
func DefinitionFromParquetFile(file *parquet.File) (*schemapb.Schema, error) {
	schema := file.Schema()

	buf, err := NewSerializedBuffer(file)
	if err != nil {
		return nil, err
	}
	dyncols := buf.DynamicColumns()
	found := map[string]struct{}{}
	columns := []*schemapb.Column{}
	metadata := file.Metadata()
	sortingCols := []*schemapb.SortingColumn{}
	foundSortingCols := map[string]struct{}{}
	for _, rg := range metadata.RowGroups {
		// Extract the sorting column information
		for _, sc := range rg.SortingColumns {
			name := rg.Columns[sc.ColumnIdx].MetaData.PathInSchema[0]
			isDynamic := false
			split := strings.Split(name, ".")
			colName := split[0]
			if len(split) > 1 && len(dyncols[colName]) != 0 {
				isDynamic = true
			}

			if isDynamic {
				name = colName
			}

			// we need a set to filter out duplicates
			if _, ok := foundSortingCols[name]; ok {
				continue
			}
			foundSortingCols[name] = struct{}{}

			direction := schemapb.SortingColumn_DIRECTION_ASCENDING
			if sc.Descending {
				direction = schemapb.SortingColumn_DIRECTION_DESCENDING
			}
			sortingCols = append(sortingCols, &schemapb.SortingColumn{
				Name:       name,
				Direction:  direction,
				NullsFirst: sc.NullsFirst,
			})
		}

		for _, col := range rg.Columns {
			name := col.MetaData.PathInSchema[0] // we only support flat schemas

			// Check if the column is optional
			nullable := false
			for _, node := range schema.Fields() {
				if node.Name() == name {
					nullable = node.Optional()
				}
			}

			isDynamic := false
			split := strings.Split(name, ".")
			colName := split[0]
			if len(split) > 1 && len(dyncols[colName]) != 0 {
				isDynamic = true
			}

			// Mark the dynamic column as being found
			if _, ok := found[colName]; ok {
				continue
			}
			found[colName] = struct{}{}

			columns = append(columns, &schemapb.Column{
				Name:          split[0],
				StorageLayout: parquetColumnMetaDataToStorageLayout(col.MetaData, nullable),
				Dynamic:       isDynamic,
			})
		}
	}

	return &schemapb.Schema{
		Name:           schema.Name(),
		Columns:        columns,
		SortingColumns: sortingCols,
	}, nil
}

// SchemaFromParquetFile converts a parquet file into a dnyparquet.Schema.
func SchemaFromParquetFile(file *parquet.File) (*Schema, error) {
	def, err := DefinitionFromParquetFile(file)
	if err != nil {
		return nil, err
	}

	return SchemaFromDefinition(def)
}

func parquetColumnMetaDataToStorageLayout(metadata format.ColumnMetaData, nullable bool) *schemapb.StorageLayout {
	layout := &schemapb.StorageLayout{
		Nullable: nullable,
	}

	switch metadata.Encoding[len(metadata.Encoding)-1] {
	case format.RLEDictionary:
		layout.Encoding = schemapb.StorageLayout_ENCODING_RLE_DICTIONARY
	case format.DeltaBinaryPacked:
		layout.Encoding = schemapb.StorageLayout_ENCODING_DELTA_BINARY_PACKED
	}

	switch metadata.Codec {
	case format.Snappy:
		layout.Compression = schemapb.StorageLayout_COMPRESSION_SNAPPY
	case format.Gzip:
		layout.Compression = schemapb.StorageLayout_COMPRESSION_GZIP
	case format.Brotli:
		layout.Compression = schemapb.StorageLayout_COMPRESSION_BROTLI
	case format.Lz4Raw:
		layout.Compression = schemapb.StorageLayout_COMPRESSION_LZ4_RAW
	case format.Zstd:
		layout.Compression = schemapb.StorageLayout_COMPRESSION_ZSTD
	}

	switch metadata.Type {
	case format.ByteArray:
		layout.Type = schemapb.StorageLayout_TYPE_STRING
	case format.Int64:
		layout.Type = schemapb.StorageLayout_TYPE_INT64
	case format.Double:
		layout.Type = schemapb.StorageLayout_TYPE_DOUBLE
	case format.Boolean:
		layout.Type = schemapb.StorageLayout_TYPE_BOOL
	}

	return layout
}

type StorageLayout interface {
	GetTypeInt32() int32
	GetRepeated() bool
	GetNullable() bool
	GetEncodingInt32() int32
	GetCompressionInt32() int32
}

type v1storageLayoutWrapper struct {
	*schemapb.StorageLayout
}

func (s *v1storageLayoutWrapper) GetRepeated() bool {
	return false
}

func (s *v1storageLayoutWrapper) GetTypeInt32() int32 {
	return int32(s.StorageLayout.GetType())
}

func (s *v1storageLayoutWrapper) GetEncodingInt32() int32 {
	return int32(s.StorageLayout.GetEncoding())
}

func (s *v1storageLayoutWrapper) GetCompressionInt32() int32 {
	return int32(s.StorageLayout.GetCompression())
}

type v2storageLayoutWrapper struct {
	*schemav2pb.StorageLayout
}

func (s *v2storageLayoutWrapper) GetTypeInt32() int32 {
	return int32(s.StorageLayout.GetType())
}

func (s *v2storageLayoutWrapper) GetEncodingInt32() int32 {
	return int32(s.StorageLayout.GetEncoding())
}

func (s *v2storageLayoutWrapper) GetCompressionInt32() int32 {
	return int32(s.StorageLayout.GetCompression())
}

func StorageLayoutWrapper(layout *schemav2pb.StorageLayout) StorageLayout {
	return nil
}

func storageLayoutToParquetNode(l StorageLayout) (parquet.Node, error) {
	var node parquet.Node
	switch l.GetTypeInt32() {
	case int32(schemapb.StorageLayout_TYPE_STRING):
		node = parquet.String()
	case int32(schemapb.StorageLayout_TYPE_INT64):
		node = parquet.Int(64)
	case int32(schemapb.StorageLayout_TYPE_DOUBLE):
		node = parquet.Leaf(parquet.DoubleType)
	case int32(schemapb.StorageLayout_TYPE_BOOL):
		node = parquet.Leaf(parquet.BooleanType)
	default:
		return nil, fmt.Errorf("unknown storage layout type: %v", l.GetTypeInt32())
	}

	if l.GetNullable() {
		node = parquet.Optional(node)
	}

	if l.GetEncodingInt32() != int32(schemapb.StorageLayout_ENCODING_PLAIN_UNSPECIFIED) {
		enc, err := encodingFromDefinition(l.GetEncodingInt32())
		if err != nil {
			return nil, err
		}
		node = parquet.Encoded(node, enc)
	}

	if l.GetCompressionInt32() != int32(schemapb.StorageLayout_COMPRESSION_NONE_UNSPECIFIED) {
		comp, err := compressionFromDefinition(l.GetCompressionInt32())
		if err != nil {
			return nil, err
		}
		node = parquet.Compressed(node, comp)
	}

	if l.GetRepeated() {
		node = parquet.Repeated(node)
	}

	return node, nil
}

func encodingFromDefinition(enc int32) (encoding.Encoding, error) {
	switch enc {
	case int32(schemapb.StorageLayout_ENCODING_RLE_DICTIONARY):
		return &parquet.RLEDictionary, nil
	case int32(schemapb.StorageLayout_ENCODING_DELTA_BINARY_PACKED):
		return &parquet.DeltaBinaryPacked, nil
	case int32(schemapb.StorageLayout_ENCODING_DELTA_BYTE_ARRAY):
		return &parquet.DeltaByteArray, nil
	case int32(schemapb.StorageLayout_ENCODING_DELTA_LENGTH_BYTE_ARRAY):
		return &parquet.DeltaLengthByteArray, nil
	default:
		return nil, fmt.Errorf("unknown encoding: %v", enc)
	}
}

func compressionFromDefinition(comp int32) (compress.Codec, error) {
	switch comp {
	case int32(schemapb.StorageLayout_COMPRESSION_SNAPPY):
		return &parquet.Snappy, nil
	case int32(schemapb.StorageLayout_COMPRESSION_GZIP):
		return &parquet.Gzip, nil
	case int32(schemapb.StorageLayout_COMPRESSION_BROTLI):
		return &parquet.Brotli, nil
	case int32(schemapb.StorageLayout_COMPRESSION_LZ4_RAW):
		return &parquet.Lz4Raw, nil
	case int32(schemapb.StorageLayout_COMPRESSION_ZSTD):
		return &parquet.Zstd, nil
	default:
		return nil, fmt.Errorf("unknown compression: %v", comp)
	}
}

// NewSchema creates a new dynamic parquet schema with the given name, column
// definitions and sorting columns. The order of the sorting columns is
// important as it determines the order in which data is written to a file or
// laid out in memory.
func newSchema(
	def proto.Message,
	columns []ColumnDefinition,
	sortingColumns []SortingColumn,
) *Schema {
	sort.Slice(columns, func(i, j int) bool {
		return columns[i].Name < columns[j].Name
	})

	columnIndexes := make(map[string]int, len(columns))
	for i, col := range columns {
		columnIndexes[col.Name] = i
	}

	s := &Schema{
		def:            def,
		columns:        columns,
		sortingColumns: sortingColumns,
		columnIndexes:  columnIndexes,
		writers:        &sync.Map{},
		buffers:        &sync.Map{},
	}

	for i, col := range columns {
		if col.Dynamic {
			s.dynamicColumns = append(s.dynamicColumns, i)
		}
	}

	return s
}

func (s *Schema) Name() string {
	switch sc := s.def.(type) {
	case *schemapb.Schema:
		return sc.GetName()
	case *schemav2pb.Schema:
		return sc.Root.GetName()
	default:
		panic("unknown schema version")
	}
}

func (s *Schema) Definition() proto.Message {
	return s.def
}

func (s *Schema) ColumnByName(name string) (ColumnDefinition, bool) {
	i, ok := s.columnIndexes[name]
	if !ok {
		return ColumnDefinition{}, false
	}
	return s.columns[i], true
}

func (s *Schema) Columns() []ColumnDefinition {
	return s.columns
}

func (s *Schema) ParquetSchema() *parquet.Schema {
	g := parquet.Group{}
	for _, col := range s.columns {
		g[col.Name] = col.StorageLayout
	}
	return parquet.NewSchema(s.Name(), g)
}

// parquetSchema returns the parquet schema for the dynamic schema with the
// concrete dynamic column names given in the argument.
func (s Schema) parquetSchema(
	dynamicColumns map[string][]string,
) (
	*parquet.Schema,
	error,
) {
	if len(dynamicColumns) != len(s.dynamicColumns) {
		return nil, fmt.Errorf("expected %d dynamic column names, got %d", len(s.dynamicColumns), len(dynamicColumns))
	}

	switch def := s.def.(type) {
	case *schemav2pb.Schema:
		return ParquetSchemaFromV2Definition(def), nil
	case *schemapb.Schema:
		g := parquet.Group{}
		for _, col := range s.columns {
			if col.Dynamic {
				dyn := dynamicColumnsFor(col.Name, dynamicColumns)
				for _, name := range dyn {
					g[col.Name+"."+name] = col.StorageLayout
				}
				continue
			}
			g[col.Name] = col.StorageLayout
		}

		return parquet.NewSchema(s.Name(), g), nil
	default:
		return nil, fmt.Errorf("unsupported schema definition version")
	}
}

// parquetSortingColumns returns the parquet sorting columns for the dynamic
// sorting columns with the concrete dynamic column names given in the
// argument.
func (s Schema) parquetSortingColumns(
	dynamicColumns map[string][]string,
) []parquet.SortingColumn {
	cols := make([]parquet.SortingColumn, 0, len(s.sortingColumns))
	for _, col := range s.sortingColumns {
		colName := col.ColumnName()
		if !s.columns[s.columnIndexes[colName]].Dynamic {
			cols = append(cols, col)
			continue
		}
		dyn := dynamicColumnsFor(colName, dynamicColumns)
		for _, name := range dyn {
			cols = append(cols, makeDynamicSortingColumn(name, col))
		}
	}
	return cols
}

// dynamicColumnsFor returns the concrete dynamic column names for the given dynamic column name.
func dynamicColumnsFor(column string, dynamicColumns map[string][]string) []string {
	return dynamicColumns[column]
}

// Buffer represents an batch of rows with a concrete set of dynamic column
// names representing how its parquet schema was created off of a dynamic
// parquet schema.
type Buffer struct {
	buffer         *parquet.Buffer
	dynamicColumns map[string][]string
	fields         []parquet.Field
}

func (b *Buffer) Reset() {
	b.buffer.Reset()
}

// DynamicRowGroup is a parquet.RowGroup that can describe the concrete dynamic
// columns.
type DynamicRowGroup interface {
	parquet.RowGroup
	// DynamicColumns returns the concrete dynamic column names that were used
	// create its concrete parquet schema with a dynamic parquet schema.
	DynamicColumns() map[string][]string
	// DynamicRows return an iterator over the rows in the row group.
	DynamicRows() DynamicRowReader
}

// DynamicRowReaders is an iterator over the rows in a DynamicRowGroup.
type DynamicRowReader interface {
	parquet.RowSeeker
	ReadRows(*DynamicRows) (int, error)
	Close() error
}

type dynamicRowGroupReader struct {
	schema         *parquet.Schema
	dynamicColumns map[string][]string
	rows           parquet.Rows
	fields         []parquet.Field
}

func newDynamicRowGroupReader(rg DynamicRowGroup, fields []parquet.Field) *dynamicRowGroupReader {
	return &dynamicRowGroupReader{
		schema:         rg.Schema(),
		dynamicColumns: rg.DynamicColumns(),
		rows:           rg.Rows(),
		fields:         fields,
	}
}

func (r *dynamicRowGroupReader) SeekToRow(i int64) error {
	return r.rows.SeekToRow(i)
}

// Implements the DynamicRows interface.
func (r *dynamicRowGroupReader) ReadRows(rows *DynamicRows) (int, error) {
	if rows.DynamicColumns == nil {
		rows.DynamicColumns = r.dynamicColumns
	}
	if rows.Schema == nil {
		rows.Schema = r.schema
	}
	if rows.fields == nil {
		rows.fields = r.fields
	}

	n, err := r.rows.ReadRows(rows.Rows)
	if err == io.EOF {
		rows.Rows = rows.Rows[:n]
		return n, io.EOF
	}
	if err != nil {
		return n, fmt.Errorf("read row: %w", err)
	}
	rows.Rows = rows.Rows[:n]

	return n, nil
}

func (r *dynamicRowGroupReader) Close() error {
	return r.rows.Close()
}

// ColumnChunks returns the list of parquet.ColumnChunk for the given index.
// It contains all the pages associated with this row group's column.
// Implements the parquet.RowGroup interface.
func (b *Buffer) ColumnChunks() []parquet.ColumnChunk {
	return b.buffer.ColumnChunks()
}

// NumRows returns the number of rows in the buffer. Implements the
// parquet.RowGroup interface.
func (b *Buffer) NumRows() int64 {
	return b.buffer.NumRows()
}

func (b *Buffer) Sort() {
	sort.Sort(b.buffer)
}

func (b *Buffer) Clone() (*Buffer, error) {
	buf := parquet.NewBuffer(
		b.buffer.Schema(),
		parquet.SortingRowGroupConfig(
			parquet.SortingColumns(b.buffer.SortingColumns()...),
		),
	)

	rows := b.buffer.Rows()
	defer rows.Close()
	for {
		rowBuf := make([]parquet.Row, 64)
		n, err := rows.ReadRows(rowBuf)
		if err == io.EOF && n == 0 {
			break
		}
		if err != nil && err != io.EOF {
			return nil, err
		}
		rowBuf = rowBuf[:n]
		_, err = buf.WriteRows(rowBuf)
		if err != nil {
			return nil, err
		}
		if err == io.EOF {
			break
		}
	}

	return &Buffer{
		buffer:         buf,
		dynamicColumns: b.dynamicColumns,
		fields:         b.fields,
	}, nil
}

// Schema returns the concrete parquet.Schema of the buffer. Implements the
// parquet.RowGroup interface.
func (b *Buffer) Schema() *parquet.Schema {
	return b.buffer.Schema()
}

// SortingColumns returns the concrete slice of parquet.SortingColumns of the
// buffer. Implements the parquet.RowGroup interface.
func (b *Buffer) SortingColumns() []parquet.SortingColumn {
	return b.buffer.SortingColumns()
}

// DynamicColumns returns the concrete dynamic column names of the buffer. It
// implements the DynamicRowGroup interface.
func (b *Buffer) DynamicColumns() map[string][]string {
	return b.dynamicColumns
}

// WriteRow writes a single row to the buffer.
func (b *Buffer) WriteRows(rows []parquet.Row) (int, error) {
	return b.buffer.WriteRows(rows)
}

// WriteRowGroup writes a single row to the buffer.
func (b *Buffer) WriteRowGroup(rg parquet.RowGroup) (int64, error) {
	return b.buffer.WriteRowGroup(rg)
}

// Rows returns an iterator for the rows in the buffer. It implements the
// parquet.RowGroup interface.
func (b *Buffer) Rows() parquet.Rows {
	return b.buffer.Rows()
}

// DynamicRows returns an iterator for the rows in the buffer. It implements the
// DynamicRowGroup interface.
func (b *Buffer) DynamicRows() DynamicRowReader {
	return newDynamicRowGroupReader(b, b.fields)
}

// NewBuffer returns a new buffer with a concrete parquet schema generated
// using the given concrete dynamic column names.
func (s *Schema) NewBuffer(dynamicColumns map[string][]string) (*Buffer, error) {
	ps, err := s.parquetSchema(dynamicColumns)
	if err != nil {
		return nil, fmt.Errorf("create parquet schema for buffer: %w", err)
	}

	cols := s.parquetSortingColumns(dynamicColumns)
	return &Buffer{
		dynamicColumns: dynamicColumns,
		buffer: parquet.NewBuffer(
			ps,
			parquet.SortingRowGroupConfig(
				parquet.SortingColumns(cols...),
			),
		),
		fields: ps.Fields(),
	}, nil
}

func (s *Schema) SerializeBuffer(w io.Writer, buffer *Buffer) error {
	pw, err := s.GetWriter(w, buffer.DynamicColumns())
	if err != nil {
		return fmt.Errorf("create writer: %w", err)
	}
	defer s.PutWriter(pw)

	rows := buffer.Rows()
	defer rows.Close()
	_, err = parquet.CopyRows(pw, rows)
	if err != nil {
		return fmt.Errorf("copy rows: %w", err)
	}

	if err := pw.Close(); err != nil {
		return fmt.Errorf("close writer: %w", err)
	}

	return nil
}

// NewWriter returns a new parquet writer with a concrete parquet schema
// generated using the given concrete dynamic column names.
func (s *Schema) NewWriter(w io.Writer, dynamicColumns map[string][]string) (*parquet.Writer, error) {
	ps, err := s.parquetSchema(dynamicColumns)
	if err != nil {
		return nil, err
	}

	cols := s.parquetSortingColumns(dynamicColumns)
	bloomFilterColumns := make([]parquet.BloomFilterColumn, 0, len(cols))
	for _, col := range cols {
		// Don't add bloom filters to boolean columns
		colName := strings.Split(col.Path()[0], ".")[0]
		def, ok := s.ColumnByName(colName)
		if !ok {
			continue
		}
		if def.StorageLayout.Type().Kind() == parquet.Boolean {
			continue
		}

		bloomFilterColumns = append(bloomFilterColumns, parquet.SplitBlockFilter(col.Path()...))
	}

	return parquet.NewWriter(w,
		ps,
		parquet.ColumnIndexSizeLimit(ColumnIndexSize),
		parquet.BloomFilters(bloomFilterColumns...),
		parquet.KeyValueMetadata(
			DynamicColumnsKey,
			serializeDynamicColumns(dynamicColumns),
		),
		parquet.SortingWriterConfig(
			parquet.SortingColumns(cols...),
		),
	), nil
}

type PooledWriter struct {
	pool *sync.Pool
	*parquet.Writer
}

func (s *Schema) GetWriter(w io.Writer, dynamicColumns map[string][]string) (*PooledWriter, error) {
	key := serializeDynamicColumns(dynamicColumns)
	pool, _ := s.writers.LoadOrStore(key, &sync.Pool{})
	pooled := pool.(*sync.Pool).Get()
	if pooled == nil {
		new, err := s.NewWriter(w, dynamicColumns)
		if err != nil {
			return nil, err
		}
		return &PooledWriter{
			pool:   pool.(*sync.Pool),
			Writer: new,
		}, nil
	}
	pooled.(*PooledWriter).Writer.Reset(w)
	return pooled.(*PooledWriter), nil
}

func (s *Schema) PutWriter(w *PooledWriter) {
	w.Writer.Reset(bytes.NewBuffer(nil))
	w.pool.Put(w)
}

type PooledBuffer struct {
	pool *sync.Pool
	*Buffer
}

func (s *Schema) GetBuffer(dynamicColumns map[string][]string) (*PooledBuffer, error) {
	key := serializeDynamicColumns(dynamicColumns)
	pool, _ := s.buffers.LoadOrStore(key, &sync.Pool{})
	pooled := pool.(*sync.Pool).Get()
	if pooled == nil {
		new, err := s.NewBuffer(dynamicColumns)
		if err != nil {
			return nil, err
		}
		return &PooledBuffer{
			pool:   pool.(*sync.Pool),
			Buffer: new,
		}, nil
	}
	return pooled.(*PooledBuffer), nil
}

func (s *Schema) PutBuffer(b *PooledBuffer) {
	b.Buffer.Reset()
	b.pool.Put(b)
}

// MergedRowGroup allows wrapping any parquet.RowGroup to implement the
// DynamicRowGroup interface by specifying the concrete dynamic column names
// the RowGroup's schema contains.
type MergedRowGroup struct {
	parquet.RowGroup
	DynCols map[string][]string
	fields  []parquet.Field
}

// DynamicColumns returns the concrete dynamic column names that were used
// create its concrete parquet schema with a dynamic parquet schema. Implements
// the DynamicRowGroup interface.
func (r *MergedRowGroup) DynamicColumns() map[string][]string {
	return r.DynCols
}

// DynamicRows returns an iterator over the rows in the row group. Implements
// the DynamicRowGroup interface.
func (r *MergedRowGroup) DynamicRows() DynamicRowReader {
	return newDynamicRowGroupReader(r, r.fields)
}

// MergeDynamicRowGroups merges the given dynamic row groups into a single
// dynamic row group. It merges the parquet schema in a non-conflicting way by
// merging all the concrete dynamic column names and generating a superset
// parquet schema that all given dynamic row groups are compatible with.
func (s *Schema) MergeDynamicRowGroups(rowGroups []DynamicRowGroup) (DynamicRowGroup, error) {
	if len(rowGroups) == 1 {
		return rowGroups[0], nil
	}

	dynamicColumns := mergeDynamicRowGroupDynamicColumns(rowGroups)
	ps, err := s.parquetSchema(dynamicColumns)
	if err != nil {
		return nil, fmt.Errorf("create merged parquet schema merging %d row groups: %w", len(rowGroups), err)
	}

	cols := s.parquetSortingColumns(dynamicColumns)

	adapters := make([]parquet.RowGroup, 0, len(rowGroups))
	for _, rowGroup := range rowGroups {
		adapters = append(adapters, NewDynamicRowGroupMergeAdapter(
			ps,
			cols,
			dynamicColumns,
			rowGroup,
		))
	}

	merge, err := parquet.MergeRowGroups(
		adapters,
		parquet.SortingRowGroupConfig(
			parquet.SortingColumns(cols...),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create merge row groups: %w", err)
	}

	return &MergedRowGroup{
		RowGroup: merge,
		DynCols:  dynamicColumns,
		fields:   ps.Fields(),
	}, nil
}

// mergeDynamicRowGroupDynamicColumns merges the concrete dynamic column names
// of multiple DynamicRowGroups into a single, merged, superset of dynamic
// column names.
func mergeDynamicRowGroupDynamicColumns(rowGroups []DynamicRowGroup) map[string][]string {
	sets := []map[string][]string{}
	for _, batch := range rowGroups {
		sets = append(sets, batch.DynamicColumns())
	}

	return mergeDynamicColumnSets(sets)
}

func mergeDynamicColumnSets(sets []map[string][]string) map[string][]string {
	dynamicColumns := map[string][][]string{}
	for _, set := range sets {
		for k, v := range set {
			_, seen := dynamicColumns[k]
			if !seen {
				dynamicColumns[k] = [][]string{}
			}
			dynamicColumns[k] = append(dynamicColumns[k], v)
		}
	}

	resultDynamicColumns := map[string][]string{}
	for name, dynCols := range dynamicColumns {
		resultDynamicColumns[name] = mergeDynamicColumns(dynCols)
	}

	return resultDynamicColumns
}

// mergeDynamicColumns merges the given concrete dynamic column names into a
// single superset. It assumes that the given DynamicColumns are all for the
// same dynamic column name.
func mergeDynamicColumns(dyn [][]string) []string {
	return mergeStrings(dyn)
}

// mergeStrings merges the given sorted string slices into a single sorted and
// deduplicated slice of strings.
func mergeStrings(str [][]string) []string {
	result := []string{}
	seen := map[string]struct{}{}
	for _, s := range str {
		for _, n := range s {
			if _, ok := seen[n]; !ok {
				result = append(result, n)
				seen[n] = struct{}{}
			}
		}
	}
	sort.Strings(result)
	return result
}

// NewDynamicRowGroupMergeAdapter returns a *DynamicRowGroupMergeAdapter, which
// maps the columns of the original row group to the columns in the super-set
// schema provided. This allows row groups that have non-conflicting dynamic
// schemas to be merged into a single row group with a superset parquet schema.
// The provided schema must not conflict with the original row group's schema
// it must be strictly a superset, this property is not checked, it is assumed
// to be true for performance reasons.
func NewDynamicRowGroupMergeAdapter(
	schema *parquet.Schema,
	sortingColumns []parquet.SortingColumn,
	mergedDynamicColumns map[string][]string,
	originalRowGroup parquet.RowGroup,
) *DynamicRowGroupMergeAdapter {
	return &DynamicRowGroupMergeAdapter{
		schema:               schema,
		sortingColumns:       sortingColumns,
		mergedDynamicColumns: mergedDynamicColumns,
		originalRowGroup:     originalRowGroup,
		indexMapping: mapMergedColumnNameIndexes(
			schemaRootFieldNames(schema),
			schemaRootFieldNames(originalRowGroup.Schema()),
		),
	}
}

func schemaRootFieldNames(schema *parquet.Schema) []string {
	fields := schema.Fields()
	names := make([]string, 0, len(fields))
	for _, field := range fields {
		names = append(names, field.Name())
	}
	return names
}

// mapMergedColumnNameIndexes maps the column indexes of the original row group
// to the indexes of the merged schema.
func mapMergedColumnNameIndexes(merged, original []string) []int {
	origColsLen := len(original)
	indexMapping := make([]int, len(merged))
	j := 0
	for i, col := range merged {
		if j < origColsLen && original[j] == col {
			indexMapping[i] = j
			j++
			continue
		}
		indexMapping[i] = -1
	}
	return indexMapping
}

// DynamicRowGroupMergeAdapter maps a RowBatch with a Schema with a subset of dynamic
// columns to a Schema with a superset of dynamic columns. It implements the
// parquet.RowGroup interface.
type DynamicRowGroupMergeAdapter struct {
	schema               *parquet.Schema
	sortingColumns       []parquet.SortingColumn
	mergedDynamicColumns map[string][]string
	originalRowGroup     parquet.RowGroup
	indexMapping         []int
}

// Returns the number of rows in the group.
func (a *DynamicRowGroupMergeAdapter) NumRows() int64 {
	return a.originalRowGroup.NumRows()
}

func FieldByName(fields []parquet.Field, name string) parquet.Field {
	for _, field := range fields {
		if field.Name() == name {
			return field
		}
	}
	return nil
}

// Returns the leaf column at the given index in the group. Searches for the
// same column in the original batch. If not found returns a column chunk
// filled with nulls.
func (a *DynamicRowGroupMergeAdapter) ColumnChunks() []parquet.ColumnChunk {
	// This only works because we currently only support flat schemas.
	fields := a.schema.Fields()
	columnChunks := a.originalRowGroup.ColumnChunks()
	remappedColumnChunks := make([]parquet.ColumnChunk, len(fields))
	for i, field := range fields {
		colIndex := a.indexMapping[i]
		if colIndex == -1 {
			schemaField := FieldByName(fields, field.Name())
			remappedColumnChunks[i] = NewNilColumnChunk(schemaField.Type(), i, int(a.NumRows()))
		} else {
			remappedColumnChunks[i] = &remappedColumnChunk{
				ColumnChunk:   columnChunks[colIndex],
				remappedIndex: i,
			}
		}
	}
	return remappedColumnChunks
}

// Returns the schema of rows in the group. The schema is the configured
// merged, superset schema.
func (a *DynamicRowGroupMergeAdapter) Schema() *parquet.Schema {
	return a.schema
}

// Returns the list of sorting columns describing how rows are sorted in the
// group.
//
// The method will return an empty slice if the rows are not sorted.
func (a *DynamicRowGroupMergeAdapter) SortingColumns() []parquet.SortingColumn {
	return a.sortingColumns
}

// Returns a reader exposing the rows of the row group.
func (a *DynamicRowGroupMergeAdapter) Rows() parquet.Rows {
	return parquet.NewRowGroupRowReader(a)
}

// remappedColumnChunk is a ColumnChunk that wraps a ColumnChunk and makes it
// appear to the called as if its index in the schema was always the index it
// was remapped to. Implements the parquet.ColumnChunk interface.
type remappedColumnChunk struct {
	parquet.ColumnChunk
	remappedIndex int
}

// Column returns the column chunk's index in the schema. It returns the
// configured remapped index. Implements the parquet.ColumnChunk interface.
func (c *remappedColumnChunk) Column() int {
	return c.remappedIndex
}

// Pages returns the column chunk's pages ensuring that all pages read will be
// remapped to the configured remapped index. Implements the
// parquet.ColumnChunk interface.
func (c *remappedColumnChunk) Pages() parquet.Pages {
	return &remappedPages{
		Pages:         c.ColumnChunk.Pages(),
		remappedIndex: c.remappedIndex,
	}
}

// remappedPages is an iterator of the column chunk's pages. It ensures that
// all pages returned will appear to belong to the configured remapped column
// index. Implements the parquet.Pages interface.
type remappedPages struct {
	parquet.Pages
	remappedIndex int
}

// ReadPage reads the next page from the page iterator. It ensures that any
// page read from the underlying iterator will appear to belong to the
// configured remapped column index. Implements the parquet.Pages interface.
func (p *remappedPages) ReadPage() (parquet.Page, error) {
	page, err := p.Pages.ReadPage()
	if err == io.EOF {
		return nil, err
	}
	if err != nil {
		return nil, fmt.Errorf("read page: %w", err)
	}

	return &remappedPage{
		Page:          page,
		remappedIndex: p.remappedIndex,
	}, nil
}

// remappedPage is a Page that wraps a Page and makes it appear as if its
// column index is the remapped index. Implements the parquet.Page interface.
type remappedPage struct {
	parquet.Page
	remappedIndex int
}

// Column returns the page's column index in the schema. It returns the
// configured remapped index. Implements the parquet.Page interface.
func (p *remappedPage) Column() int {
	return p.remappedIndex
}

// Values returns a parquet.ValueReader that ensures that all values read will
// be remapped to have the configured remapped index. Implements the
// parquet.Page interface.
func (p *remappedPage) Values() parquet.ValueReader {
	return &remappedValueReader{
		ValueReader:   p.Page.Values(),
		remappedIndex: p.remappedIndex,
	}
}

// Values returns the page's values. It ensures that all values read will be
// remapped to have the configured remapped index. Implements the
// parquet.ValueReader interface.
type remappedValueReader struct {
	parquet.ValueReader
	remappedIndex int
}

// ReadValues reads the next batch of values from the value reader. It ensures
// that any value read will be remapped to have the configured remapped index.
// Implements the parquet.ValueReader interface.
func (r *remappedValueReader) ReadValues(v []parquet.Value) (int, error) {
	n, err := r.ValueReader.ReadValues(v)
	for i := 0; i < len(v[:n]); i++ {
		v[i] = v[i].Level(v[i].RepetitionLevel(), v[i].DefinitionLevel(), r.remappedIndex)
	}

	if err == io.EOF {
		return n, err
	}
	if err != nil {
		return n, fmt.Errorf("read values: %w", err)
	}

	return n, nil
}

func SortingColumnsFromDef(def *schemav2pb.Schema) ([]parquet.SortingColumn, error) {
	sortingColumns := make([]parquet.SortingColumn, 0, len(def.SortingColumns))
	for _, col := range def.SortingColumns {
		var sortingColumn SortingColumn
		switch col.Direction {
		case schemav2pb.SortingColumn_DIRECTION_ASCENDING:
			sortingColumn = Ascending(col.Path)
		case schemav2pb.SortingColumn_DIRECTION_DESCENDING:
			sortingColumn = Descending(col.Path)
		default:
			return nil, fmt.Errorf("unknown sorting direction %q, only \"ascending\", \"descending\" are valid choices", col.Direction)
		}
		if col.NullsFirst {
			sortingColumn = NullsFirst(sortingColumn)
		}
		sortingColumns = append(sortingColumns, sortingColumn)
	}

	return sortingColumns, nil
}
