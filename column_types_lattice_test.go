package frostdb

// The column_types lattice (temper/lattices/column_types.yml): every column
// type a schema can declare, in plain, nullable and repeated form, written
// and read back on both read paths. Each check states the member and read
// path it proves with temper.Covers, so temper matrix shows every type
// against every read path.

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
	"github.com/turbolytics/temper"

	schemapb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/schema/v1alpha1"
	"github.com/polarsignals/frostdb/index"
	"github.com/polarsignals/frostdb/query"
)

var latticeTypes = []struct {
	name   string
	layout schemapb.StorageLayout_Type
	dt     arrow.DataType
	vals   [2]string
}{
	{"string", schemapb.StorageLayout_TYPE_STRING, arrow.BinaryTypes.Binary, [2]string{"YQ==", "Yg=="}},
	{"int64", schemapb.StorageLayout_TYPE_INT64, arrow.PrimitiveTypes.Int64, [2]string{"1", "2"}},
	{"double", schemapb.StorageLayout_TYPE_DOUBLE, arrow.PrimitiveTypes.Float64, [2]string{"1.5", "2.5"}},
	{"bool", schemapb.StorageLayout_TYPE_BOOL, arrow.FixedWidthTypes.Boolean, [2]string{"true", "false"}},
	{"int32", schemapb.StorageLayout_TYPE_INT32, arrow.PrimitiveTypes.Int32, [2]string{"1", "2"}},
	{"uint64", schemapb.StorageLayout_TYPE_UINT64, arrow.PrimitiveTypes.Uint64, [2]string{"1", "2"}},
}

// latticeRecord returns two rows. Column k ("a", "b") sorts the table, and
// column v holds the type under test in the given form.
func latticeRecord(t *testing.T, dt arrow.DataType, vals [2]string, form string) arrow.Record {
	t.Helper()
	vType := dt
	if form == "repeated" {
		vType = arrow.ListOf(dt)
	}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "k", Type: arrow.BinaryTypes.Binary},
		{Name: "v", Type: vType, Nullable: form == "nullable"},
	}, nil)
	rb := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer rb.Release()
	k := rb.Field(0).(*array.BinaryBuilder)
	k.AppendString("a")
	k.AppendString("b")
	switch form {
	case "plain":
		require.NoError(t, rb.Field(1).AppendValueFromString(vals[0]))
		require.NoError(t, rb.Field(1).AppendValueFromString(vals[1]))
	case "nullable":
		require.NoError(t, rb.Field(1).AppendValueFromString(vals[0]))
		rb.Field(1).AppendNull()
	case "repeated":
		lb := rb.Field(1).(*array.ListBuilder)
		lb.Append(true)
		require.NoError(t, lb.ValueBuilder().AppendValueFromString(vals[0]))
		require.NoError(t, lb.ValueBuilder().AppendValueFromString(vals[1]))
		lb.Append(true)
		require.NoError(t, lb.ValueBuilder().AppendValueFromString(vals[0]))
	}
	return rb.NewRecord()
}

func columnValues(col arrow.Array) []string {
	out := make([]string, 0, col.Len())
	for i := 0; i < col.Len(); i++ {
		out = append(out, col.ValueStr(i))
	}
	return out
}

// latticeTable creates a table with columns k and v. Its level config lets
// EnsureCompaction move the inserted data to Parquet.
func latticeTable(t *testing.T, layout schemapb.StorageLayout_Type, form string) (*DB, *Table, func()) {
	t.Helper()
	c, err := New(WithIndexConfig([]*index.LevelConfig{
		{Level: index.L0, MaxSize: 1 << 30, Type: index.CompactionTypeParquetMemory},
		{Level: index.L1, MaxSize: 1 << 40},
	}))
	require.NoError(t, err)
	db, err := c.DB(context.Background(), "test")
	require.NoError(t, err)
	def := &schemapb.Schema{
		Name: "lattice",
		Columns: []*schemapb.Column{
			{Name: "k", StorageLayout: &schemapb.StorageLayout{Type: schemapb.StorageLayout_TYPE_STRING}},
			{Name: "v", StorageLayout: &schemapb.StorageLayout{
				Type: layout, Nullable: form == "nullable", Repeated: form == "repeated",
			}},
		},
		SortingColumns: []*schemapb.SortingColumn{{Name: "k", Direction: schemapb.SortingColumn_DIRECTION_ASCENDING}},
	}
	table, err := db.Table(def.Name, NewTableConfig(def))
	require.NoError(t, err)
	return db, table, func() { c.Close() }
}

func readLatticeColumn(t *testing.T, db *DB) []string {
	t.Helper()
	engine := query.NewEngine(memory.NewGoAllocator(), db.TableProvider())
	var got []string
	err := engine.ScanTable("lattice").Execute(context.Background(), func(_ context.Context, r arrow.Record) error {
		got = append(got, columnValues(r.Column(r.Schema().FieldIndices("v")[0]))...)
		return nil
	})
	require.NoError(t, err)
	return got
}

func TestColumnTypes_EveryTypeOnEveryReadPath(t *testing.T) {
	for _, ct := range latticeTypes {
		for _, form := range []string{"plain", "nullable", "repeated"} {
			ct, form := ct, form
			key := ct.name
			if form != "plain" {
				key += " " + form
			}
			member := "column_types/" + key

			t.Run(key+"/memory", func(t *testing.T) {
				temper.Covers(t, "table.insert")
				temper.Covers(t, member, temper.Subject("read.memory"))
				db, table, closeDB := latticeTable(t, ct.layout, form)
				defer closeDB()
				rec := latticeRecord(t, ct.dt, ct.vals, form)
				defer rec.Release()
				want := columnValues(rec.Column(1))

				_, err := table.InsertRecord(context.Background(), rec)
				require.NoError(t, err)
				require.Equal(t, want, readLatticeColumn(t, db))
			})

			t.Run(key+"/parquet", func(t *testing.T) {
				temper.Covers(t, "compaction.parquet")
				temper.Covers(t, member, temper.Subject("read.parquet"))
				db, table, closeDB := latticeTable(t, ct.layout, form)
				defer closeDB()
				rec := latticeRecord(t, ct.dt, ct.vals, form)
				defer rec.Release()
				want := columnValues(rec.Column(1))

				_, err := table.InsertRecord(context.Background(), rec)
				require.NoError(t, err)
				require.NoError(t, table.EnsureCompaction(), "compacting to Parquet")
				require.Equal(t, want, readLatticeColumn(t, db), "values after compaction to Parquet")
			})
		}
	}
}
