package records

import (
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/google/uuid"

	schemapb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/schema/v1alpha1"
	"github.com/polarsignals/frostdb/pqarrow/arrowutils"
)

const (
	TagName = "frostdb"
)

type Record struct {
	arrow.Record
	SortingColumns []arrowutils.SortingColumn
}

// Build is a generic arrow.Record builder that ingests structs of type T. The
// generated record can be passed to (*Table).InsertRecord.
//
// Struct tag `frostdb` is used to pass options for the schema for T and use
// (*Build[T]).Schema to obtain schema v1alpha1.
//
// This api is opinionated.
//
//   - Nested Columns are not supported
//
// # Tags
//
// Use `frostdb` to define tags that customizes field values. You can express
// everything needed to construct schema v1alpha1.
//
// Tags are defined as a comma separated list. The first item is the column
// name. Column name is optional, when omitted it is derived from the field name
// (snake_cased)
//
// Supported Tags
//
//	    delta_binary_packed | Delta binary packed encoding.
//	                 brotli | Brotli compression.
//	                    asc | Sorts in ascending order.Use asc(n) where n is an integer for sorting order.
//	                   gzip | GZIP compression.
//	                 snappy | Snappy compression.
//	delta_length_byte_array | Delta Length Byte Array encoding.
//	       delta_byte_array | Delta Byte Array encoding.
//	                   desc | Sorts in descending order.Use desc(n) where n is an integer for sorting order
//	                lz4_raw | LZ4_RAW compression.
//	               pre_hash | Prehash the column before storing it.
//	             null_first | When used wit asc nulls are smallest and with des nulls are largest.
//	                   zstd | ZSTD compression.
//	               rle_dict | Dictionary run-length encoding.
//	                  plain | Plain encoding.
//
// Example tagged Sample struct
//
//	type Sample struct {
//		ExampleType string      `frostdb:"example_type,rle_dict,asc(0)"`
//		Labels      []Label     `frostdb:"labels,rle_dict,null,dyn,asc(1),null_first"`
//		Stacktrace  []uuid.UUID `frostdb:"stacktrace,rle_dict,asc(3),null_first"`
//		Timestamp   int64       `frostdb:"timestamp,asc(2)"`
//		Value       int64       `frostdb:"value"`
//	}
//
// # Dynamic columns
//
// Field of type map<string, T> is a dynamic column by default.
//
//	type Example struct {
//		// Use supported tags to customize the column value
//		Labels map[string]string `frostdb:"labels"`
//	}
//
// # Repeated columns
//
// Fields of type []int64, []float64, []bool, and []string are supported. These
// are represented as arrow.LIST.
//
// Generated schema for the repeated columns applies all supported tags. By
// default repeated fields are nullable. You can safely pass nil slices for
// repeated columns.
type Build[T any] struct {
	fields []*fieldRecord
	buffer []arrow.Array
	sort   []*fieldRecord
}

func NewBuild[T any](mem memory.Allocator) *Build[T] {
	var a T
	r := reflect.TypeOf(a)
	for r.Kind() == reflect.Ptr {
		r = r.Elem()
	}
	if r.Kind() != reflect.Struct {
		panic("frostdb/dynschema: " + r.String() + " is not supported")
	}
	b := &Build[T]{}
	for i := 0; i < r.NumField(); i++ {
		f := r.Field(i)
		var (
			typ        arrow.DataType
			dictionary bool
			preHash    bool
			nullable   bool
			sortColumn bool
			nullFirst  bool
			sortOrder  int
			direction  schemapb.SortingColumn_Direction

			encoding    schemapb.StorageLayout_Encoding
			compression schemapb.StorageLayout_Compression
			styp        schemapb.StorageLayout_Type
		)
		name, tag := fieldName(f)
		if tag != "" {
			walkTag(tag, func(key, value string) {
				switch key {
				case "null_first":
					nullFirst = true
				case "asc", "desc":
					sortColumn = true
					sortOrder, _ = strconv.Atoi(value)
					if key == "asc" {
						direction = schemapb.SortingColumn_DIRECTION_ASCENDING
					} else {
						direction = schemapb.SortingColumn_DIRECTION_DESCENDING
					}
				case "pre_hash":
					preHash = true
				case "plain":
					encoding = schemapb.StorageLayout_ENCODING_PLAIN_UNSPECIFIED
				case "rle_dict":
					encoding = schemapb.StorageLayout_ENCODING_RLE_DICTIONARY
					dictionary = true
				case "delta_binary_packed":
					encoding = schemapb.StorageLayout_ENCODING_DELTA_BINARY_PACKED
				case "delta_byte_array":
					encoding = schemapb.StorageLayout_ENCODING_DELTA_BINARY_PACKED
				case "delta_length_byte_array":
					encoding = schemapb.StorageLayout_ENCODING_DELTA_LENGTH_BYTE_ARRAY
				case "snappy":
					compression = schemapb.StorageLayout_COMPRESSION_SNAPPY
				case "gzip":
					compression = schemapb.StorageLayout_COMPRESSION_GZIP
				case "brotli":
					compression = schemapb.StorageLayout_COMPRESSION_BROTLI
				case "lz4_raw":
					compression = schemapb.StorageLayout_COMPRESSION_LZ4_RAW
				case "zstd":
					compression = schemapb.StorageLayout_COMPRESSION_ZSTD
				}
			})
		}
		fr := &fieldRecord{
			name:        name,
			preHash:     preHash,
			sort:        sortColumn,
			sortOrder:   sortOrder,
			nullFirst:   nullFirst,
			direction:   direction,
			compression: compression,
			encoding:    encoding,
		}
		fty := f.Type
		for fty.Kind() == reflect.Ptr {
			nullable = true
			fty = fty.Elem()
		}
		switch fty.Kind() {
		case reflect.Map:
			typ, styp = baseType(fty.Elem(), dictionary)
			fr.typ = styp
			fr.dynamic = true
			fr.nullable = true
			fr.build = newMapFieldBuilder(newFieldFunc(typ, mem, name,
				// Pointer base types needs to be property handled even for dynamic columns
				// so map[string]string and map[string]*string should all work the same.
				fty.Elem().Kind() == reflect.Ptr),
				newRowsBeforeFunc(i, b.numRowsBefore),
			)
		case reflect.Slice:
			switch {
			case isBinary(fty):
				typ, styp = baseType(fty, dictionary)
				fr.typ = styp
				fr.nullable = true
				fr.build = newFieldBuild(typ, mem, name, true)
			case isUUIDSlice(fty):
				fr.typ = schemapb.StorageLayout_TYPE_STRING
				fr.build = newUUIDSliceField(mem, name)
			default:
				typ, styp = baseType(fty.Elem(), dictionary)
				fr.typ = styp
				fr.repeated = true
				// Repeated columns are always nullable
				fr.nullable = true
				typ = arrow.ListOf(typ)
				fr.build = newFieldBuild(typ, mem, name, true)
			}
		case reflect.Int64, reflect.Float64, reflect.Bool, reflect.String:
			typ, styp = baseType(fty, dictionary)
			fr.typ = styp
			fr.nullable = nullable
			fr.build = newFieldBuild(typ, mem, name, nullable)
		default:
			panic("frostdb/dynschema: " + fty.String() + " is npt supported")
		}
		b.fields = append(b.fields, fr)
	}
	return b
}

// For dynamic columns we need to know the state of row counts to adjust nulls to
// match the record row count.
//
// This handles the case where a series of T without any dynamic columns is
// followed by dynamic columns.
func (b *Build[T]) numRowsBefore(fieldIdx int) int {
	for i := 0; i <= len(b.fields) && i != fieldIdx; i++ {
		before := i < fieldIdx
		f := b.fields[i]
		if f.dynamic {
			// If we have dynamic columns before/after fieldIdx. We can stop looking if
			// the columns were appended to.
			if size := f.build.Len(); size != 0 {
				if before {
					// The field has already been processed. Adjust the size because we care
					// about rows count before current T appending
					size--
				}
				return size
			}
			continue
		}
		size := b.fields[i].build.Len()
		if before {
			// The field has already been processed. Adjust the size because we care
			// about rows count before current T appending
			size--
		}
		return size
	}
	return 0
}

func (b *Build[T]) Append(values ...T) error {
	for _, value := range values {
		v := reflect.ValueOf(value)
		for v.Kind() == reflect.Ptr {
			v = v.Elem()
		}
		for i := 0; i < v.NumField(); i++ {
			err := b.fields[i].build.Append(v.Field(i))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *Build[T]) NewRecord() *Record {
	fields := make([]arrow.Field, 0, len(b.fields))
	for _, f := range b.fields {
		fs := f.build.Fields()
		if f.sort {
			if f.dynamic {
				for j := 0; j < len(fs); j++ {
					b.sort = append(b.sort, f)
				}
			} else {
				b.sort = append(b.sort, f)
			}
		}
		fields = append(fields, fs...)
		b.buffer = f.build.NewArray(b.buffer)
	}
	defer func() {
		for i := range b.buffer {
			b.buffer[i].Release()
		}
		b.buffer = b.buffer[:0]
		b.sort = b.sort[:0]
	}()
	sort.Slice(b.sort, func(i, j int) bool {
		return b.sort[i].sortOrder < b.sort[j].sortOrder
	})
	sortingCols := make([]arrowutils.SortingColumn, 0, len(b.sort))
	for idx, f := range b.sort {
		direction := arrowutils.Ascending
		if f.direction == schemapb.SortingColumn_DIRECTION_DESCENDING {
			direction = arrowutils.Descending
		}
		sortingCols = append(sortingCols, arrowutils.SortingColumn{
			Index:      idx,
			Direction:  direction,
			NullsFirst: f.nullFirst,
		})
	}
	return &Record{
		Record: array.NewRecord(
			arrow.NewSchema(fields, nil),
			b.buffer,
			int64(b.buffer[0].Len()),
		),
		SortingColumns: sortingCols,
	}
}

func (b Build[T]) Schema(name string) (s *schemapb.Schema) {
	s = &schemapb.Schema{Name: name, Columns: make([]*schemapb.Column, 0, len(b.fields))}
	var toSort []*fieldRecord
	for _, f := range b.fields {
		s.Columns = append(s.Columns, &schemapb.Column{
			Name:    f.name,
			Dynamic: f.dynamic,
			Prehash: f.preHash,
			StorageLayout: &schemapb.StorageLayout{
				Type:        f.typ,
				Encoding:    f.encoding,
				Compression: f.compression,
				Nullable:    f.nullable,
				Repeated:    f.repeated,
			},
		})
		if f.sort {
			toSort = append(toSort, f)
		}
	}
	sort.Slice(toSort, func(i, j int) bool {
		return toSort[i].sortOrder < toSort[j].sortOrder
	})
	for _, f := range toSort {
		s.SortingColumns = append(s.SortingColumns, &schemapb.SortingColumn{
			Name:       f.name,
			Direction:  f.direction,
			NullsFirst: f.nullFirst,
		})
	}
	return
}

func (b *Build[T]) Release() {
	for _, f := range b.fields {
		f.build.Release()
	}
	b.buffer = b.buffer[:0]
}

type fieldBuilder interface {
	Fields() []arrow.Field
	Len() int
	AppendNull()
	Append(reflect.Value) error
	NewArray([]arrow.Array) []arrow.Array
	Release()
}

type mapFieldBuilder struct {
	newField   func(string) fieldBuilder
	rowsBefore func() int
	columns    map[string]fieldBuilder
	seen       map[string]struct{}
	keys       []string
}

func newFieldFunc(dt arrow.DataType, mem memory.Allocator, name string, nullable bool) func(string) fieldBuilder {
	return func(s string) fieldBuilder {
		return newFieldBuild(dt, mem, name+"."+s, nullable)
	}
}

func newRowsBeforeFunc(i int, f func(int) int) func() int {
	return func() int {
		return f(i)
	}
}

func newMapFieldBuilder(newField func(string) fieldBuilder, rowsBefore func() int) *mapFieldBuilder {
	return &mapFieldBuilder{
		newField:   newField,
		rowsBefore: rowsBefore,
		columns:    make(map[string]fieldBuilder),
		seen:       make(map[string]struct{}),
	}
}

var _ fieldBuilder = (*mapFieldBuilder)(nil)

func (m *mapFieldBuilder) Fields() (o []arrow.Field) {
	if len(m.columns) == 0 {
		return []arrow.Field{}
	}
	o = make([]arrow.Field, 0, len(m.columns))
	m.keys = slices.Grow(m.keys, len(m.columns))
	for k := range m.columns {
		m.keys = append(m.keys, k)
	}
	sort.Strings(m.keys)
	for _, key := range m.keys {
		o = append(o, m.columns[key].Fields()...)
	}
	return
}

func (m *mapFieldBuilder) NewArray(a []arrow.Array) []arrow.Array {
	if len(m.columns) == 0 {
		return a
	}
	m.keys = m.keys[:0]
	for k := range m.columns {
		m.keys = append(m.keys, k)
	}
	sort.Strings(m.keys)
	for _, key := range m.keys {
		a = m.columns[key].NewArray(a)
	}
	for _, v := range m.columns {
		v.Release()
	}
	clear(m.columns)
	m.keys = m.keys[:0]
	return a
}

func (m *mapFieldBuilder) AppendNull() {}

func (m *mapFieldBuilder) Release() {
	for _, v := range m.columns {
		v.Release()
	}
	clear(m.columns)
	m.keys = m.keys[:0]
}

func (m *mapFieldBuilder) Append(v reflect.Value) error {
	if v.IsNil() || v.Len() == 0 {
		for _, v := range m.columns {
			v.AppendNull()
		}
		return nil
	}
	clear(m.seen)
	keys := v.MapKeys()
	size := m.Len()
	if size == 0 {
		// Maybe we never supplied dynamic columns before but other columns were
		// appended.
		size = m.rowsBefore()
	}
	for _, key := range keys {
		name := key.Interface().(string)
		m.seen[name] = struct{}{}
		err := m.get(name, size).Append(v.MapIndex(key))
		if err != nil {
			return err
		}
	}
	for k, v := range m.columns {
		_, ok := m.seen[k]
		if !ok {
			// All record columns must have the same length. Set columns not present in v
			// to null
			v.AppendNull()
		}
	}
	return nil
}

func (m *mapFieldBuilder) Len() int {
	for _, v := range m.columns {
		return v.Len()
	}
	return 0
}

func (m *mapFieldBuilder) get(name string, size int) fieldBuilder {
	f, ok := m.columns[name]
	if ok {
		return f
	}
	f = m.newField(name)
	for i := 0; i < size; i++ {
		f.AppendNull()
	}

	m.columns[name] = f
	return f
}

func baseType(fty reflect.Type, dictionary bool) (typ arrow.DataType, sty schemapb.StorageLayout_Type) {
	for fty.Kind() == reflect.Ptr {
		fty = fty.Elem()
	}
	switch fty.Kind() {
	case reflect.Int64:
		typ = arrow.PrimitiveTypes.Int64
		sty = schemapb.StorageLayout_TYPE_INT64
	case reflect.Float64:
		typ = arrow.PrimitiveTypes.Float64
		sty = schemapb.StorageLayout_TYPE_DOUBLE
	case reflect.Bool:
		typ = arrow.FixedWidthTypes.Boolean
		sty = schemapb.StorageLayout_TYPE_BOOL
	case reflect.String:
		typ = arrow.BinaryTypes.String
		sty = schemapb.StorageLayout_TYPE_STRING
	default:
		if !isBinary(fty) {
			panic("frostdb/dynschema: " + fty.String() + " is npt supported")
		}
		typ = arrow.BinaryTypes.Binary
		sty = schemapb.StorageLayout_TYPE_BINARY
	}
	if dictionary {
		typ = &arrow.DictionaryType{
			IndexType: &arrow.Uint32Type{},
			ValueType: typ,
		}
	}
	return
}

func fieldName(f reflect.StructField) (name, tag string) {
	name, tag, _ = strings.Cut(f.Tag.Get(TagName), ",")
	if name == "" {
		name = ToSnakeCase(f.Name)
	}
	return
}

func newFieldBuild(dt arrow.DataType, mem memory.Allocator, name string, nullable bool) (f *fieldBuilderFunc) {
	b := array.NewBuilder(mem, dt)
	f = &fieldBuilderFunc{
		col: arrow.Field{
			Name:     name,
			Type:     dt,
			Nullable: nullable,
		},
		releaseFunc: b.Release,
		nilFunc:     b.AppendNull,
		len:         b.Len,
		newArraysFunc: func(a []arrow.Array) []arrow.Array {
			return append(a, b.NewArray())
		},
	}
	switch e := b.(type) {
	case *array.Int64Builder:
		f.buildFunc = func(v reflect.Value) error {
			if nullable {
				if v.IsNil() {
					e.AppendNull()
					return nil
				}
				v = v.Elem()
			}
			e.Append(v.Int())
			return nil
		}
	case *array.Int64DictionaryBuilder:
		f.buildFunc = func(v reflect.Value) error {
			if nullable {
				if v.IsNil() {
					e.AppendNull()
					return nil
				}
				v = v.Elem()
			}
			return e.Append(v.Int())
		}
	case *array.Float64Builder:
		f.buildFunc = func(v reflect.Value) error {
			if nullable {
				if v.IsNil() {
					e.AppendNull()
					return nil
				}
				v = v.Elem()
			}
			e.Append(v.Float())
			return nil
		}
	case *array.Float64DictionaryBuilder:
		f.buildFunc = func(v reflect.Value) error {
			if nullable {
				if v.IsNil() {
					e.AppendNull()
					return nil
				}
				v = v.Elem()
			}
			return e.Append(v.Float())
		}
	case *array.BooleanBuilder:
		f.buildFunc = func(v reflect.Value) error {
			if nullable {
				if v.IsNil() {
					e.AppendNull()
					return nil
				}
				v = v.Elem()
			}
			e.Append(v.Bool())
			return nil
		}
	case *array.StringBuilder:
		f.buildFunc = func(v reflect.Value) error {
			if nullable {
				if v.IsNil() {
					e.AppendNull()
					return nil
				}
				v = v.Elem()
			}
			e.Append(v.Interface().(string))
			return nil
		}

	case *array.BinaryBuilder:
		f.buildFunc = func(v reflect.Value) error {
			e.Append(v.Bytes())
			return nil
		}

	case *array.BinaryDictionaryBuilder:
		f.buildFunc = func(v reflect.Value) error {
			if v.Kind() == reflect.String {
				return e.AppendString(v.String())
			}
			return e.Append(v.Bytes())
		}
	case *array.ListBuilder:
		switch build := e.ValueBuilder().(type) {
		case *array.Int64Builder:
			f.buildFunc = func(v reflect.Value) error {
				if v.IsNil() {
					e.AppendNull()
					return nil
				}
				e.Append(true)
				build.Reserve(v.Len())
				return applyInt(v, func(i int64) error {
					build.Append(i)
					return nil
				})
			}
		case *array.Int64DictionaryBuilder:
			f.buildFunc = func(v reflect.Value) error {
				if v.IsNil() {
					e.AppendNull()
					return nil
				}
				e.Append(true)
				build.Reserve(v.Len())
				return applyInt(v, build.Append)
			}

		case *array.Float64Builder:
			f.buildFunc = func(v reflect.Value) error {
				if v.IsNil() {
					e.AppendNull()
					return nil
				}
				e.Append(true)
				build.Reserve(v.Len())
				return applyFloat64(v, func(i float64) error {
					build.Append(i)
					return nil
				})
			}
		case *array.Float64DictionaryBuilder:
			f.buildFunc = func(v reflect.Value) error {
				if v.IsNil() {
					e.AppendNull()
					return nil
				}
				e.Append(true)
				build.Reserve(v.Len())
				return applyFloat64(v, build.Append)
			}

		case *array.StringBuilder:
			f.buildFunc = func(v reflect.Value) error {
				if v.IsNil() {
					e.AppendNull()
					return nil
				}
				e.Append(true)
				build.Reserve(v.Len())
				return applyString(v, func(i string) error {
					build.Append(i)
					return nil
				})
			}
		case *array.BinaryBuilder:
			f.buildFunc = func(v reflect.Value) error {
				if v.IsNil() {
					e.AppendNull()
					return nil
				}
				e.Append(true)
				build.Reserve(v.Len())
				return applyBinary(v, func(i []byte) error {
					build.Append(i)
					return nil
				})
			}
		case *array.BinaryDictionaryBuilder:
			f.buildFunc = func(v reflect.Value) error {
				if v.Len() == 0 {
					e.AppendNull()
					return nil
				}
				e.Append(true)
				build.Reserve(v.Len())
				if v.Type().Elem().Kind() == reflect.String {
					return applyString(v, build.AppendString)
				}
				return applyBinary(v, build.Append)
			}
		case *array.BooleanBuilder:
			f.buildFunc = func(v reflect.Value) error {
				if v.IsNil() {
					e.AppendNull()
					return nil
				}
				e.Append(true)
				build.Reserve(v.Len())
				return applyBool(v, func(i bool) error {
					build.Append(i)
					return nil
				})
			}
		}
	default:
		panic("frostdb:dynschema: unsupported array builder " + b.Type().String())
	}
	return
}

func applyBinary(v reflect.Value, apply func([]byte) error) error {
	return listApply[[]byte](v, func(v reflect.Value) []byte {
		return v.Bytes()
	}, apply)
}

func applyString(v reflect.Value, apply func(string) error) error {
	return listApply[string](v, func(v reflect.Value) string {
		return v.Interface().(string)
	}, apply)
}

func applyFloat64(v reflect.Value, apply func(float64) error) error {
	return listApply[float64](v, func(v reflect.Value) float64 {
		return v.Float()
	}, apply)
}

func applyBool(v reflect.Value, apply func(bool) error) error {
	return listApply[bool](v, func(v reflect.Value) bool {
		return v.Bool()
	}, apply)
}

func applyInt(v reflect.Value, apply func(int64) error) error {
	return listApply[int64](v, func(v reflect.Value) int64 {
		return v.Int()
	}, apply)
}

func listApply[T any](v reflect.Value, fn func(reflect.Value) T, apply func(T) error) error {
	for i := 0; i < v.Len(); i++ {
		err := apply(fn(v.Index(i)))
		if err != nil {
			return err
		}
	}
	return nil
}

func newUUIDSliceField(mem memory.Allocator, name string) (f *fieldBuilderFunc) {
	dt := &arrow.DictionaryType{
		IndexType: &arrow.Int32Type{},
		ValueType: &arrow.BinaryType{},
	}
	b := array.NewBuilder(mem, dt)
	f = &fieldBuilderFunc{
		col: arrow.Field{
			Name: name,
			Type: dt,
		},
		releaseFunc: b.Release,
		nilFunc:     b.AppendNull,
		len:         b.Len,
		newArraysFunc: func(a []arrow.Array) []arrow.Array {
			return append(a, b.NewArray())
		},
	}
	bd := b.(*array.BinaryDictionaryBuilder)
	f.buildFunc = func(v reflect.Value) error {
		return bd.Append(ExtractLocationIDs(v.Interface().([]uuid.UUID)))
	}
	return
}

func ExtractLocationIDs(locs []uuid.UUID) []byte {
	b := make([]byte, len(locs)*16) // UUID are 16 bytes thus multiply by 16
	index := 0
	for i := len(locs) - 1; i >= 0; i-- {
		copy(b[index:index+16], locs[i][:])
		index += 16
	}
	return b
}

type fieldBuilderFunc struct {
	len           func() int
	col           arrow.Field
	nilFunc       func()
	buildFunc     func(reflect.Value) error
	newArraysFunc func([]arrow.Array) []arrow.Array
	releaseFunc   func()
}

var _ fieldBuilder = (*fieldBuilderFunc)(nil)

func (f *fieldBuilderFunc) Fields() []arrow.Field                  { return []arrow.Field{f.col} }
func (f *fieldBuilderFunc) Len() int                               { return f.len() }
func (f *fieldBuilderFunc) AppendNull()                            { f.nilFunc() }
func (f *fieldBuilderFunc) Append(v reflect.Value) error           { return f.buildFunc(v) }
func (f *fieldBuilderFunc) NewArray(a []arrow.Array) []arrow.Array { return f.newArraysFunc(a) }
func (f *fieldBuilderFunc) Release()                               { f.releaseFunc() }

type fieldRecord struct {
	name        string
	dynamic     bool
	preHash     bool
	nullable    bool
	repeated    bool
	sort        bool
	nullFirst   bool
	sortOrder   int
	direction   schemapb.SortingColumn_Direction
	encoding    schemapb.StorageLayout_Encoding
	compression schemapb.StorageLayout_Compression
	typ         schemapb.StorageLayout_Type
	build       fieldBuilder
}

func walkTag(tag string, f func(key, value string)) {
	if tag == "" {
		return
	}
	value, tag, _ := strings.Cut(tag, ",")
	if value != "" {
		k, v, _ := strings.Cut(value, "(")
		v, _, _ = strings.Cut(v, ")")
		f(k, v)
	}
	walkTag(tag, f)
}

var (
	uuidSliceType = reflect.TypeOf([]uuid.UUID{})
	binaryType    = reflect.TypeOf([]byte{})
)

func isUUIDSlice(typ reflect.Type) bool {
	return typ.AssignableTo(uuidSliceType)
}

func isBinary(typ reflect.Type) bool {
	return typ == binaryType
}

var (
	matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
	matchAllCap   = regexp.MustCompile("([a-z0-9])([A-Z])")
)

func ToSnakeCase(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}
