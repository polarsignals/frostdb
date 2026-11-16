package frostdb

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"

	schemapb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/schema/v1alpha1"
	"github.com/polarsignals/frostdb/index"
	"github.com/polarsignals/frostdb/query"
)

// columnTypes is every storage layout type a schema can declare, with two
// values in the form arrow's AppendValueFromString accepts.
var columnTypes = []struct {
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

// columnTypeRecord returns two rows. Column k ("a", "b") sorts the table, and
// column v holds the type under test in plain, nullable or repeated form.
func columnTypeRecord(t *testing.T, dt arrow.DataType, vals [2]string, form string) arrow.Record {
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

// readColumn returns every value of column v, rendered with ValueStr.
func readColumn(ctx context.Context, db *DB, table string) ([]string, error) {
	engine := query.NewEngine(memory.NewGoAllocator(), db.TableProvider())
	var vals []string
	err := engine.ScanTable(table).Execute(ctx, func(_ context.Context, r arrow.Record) error {
		col := r.Column(r.Schema().FieldIndices("v")[0])
		for i := 0; i < col.Len(); i++ {
			vals = append(vals, col.ValueStr(i))
		}
		return nil
	})
	return vals, err
}

// Every column type a schema can declare must read back the same after
// compaction to Parquet as it does from memory: the same values, and the same
// nulls. Compaction runs only after a level fills up (15 MiB by default), so
// a test with a few rows never reaches it unless it forces compaction.
func TestCompaction_RoundTripsEveryColumnType(t *testing.T) {
	for _, ct := range columnTypes {
		for _, form := range []string{"plain", "nullable", "repeated"} {
			ct, form := ct, form
			t.Run(fmt.Sprintf("%s/%s", ct.name, form), func(t *testing.T) {
				ctx := context.Background()
				c, err := New(WithIndexConfig([]*index.LevelConfig{
					{Level: index.L0, MaxSize: 1 << 30, Type: index.CompactionTypeParquetMemory},
					{Level: index.L1, MaxSize: 1 << 40},
				}))
				require.NoError(t, err)
				defer c.Close()
				db, err := c.DB(ctx, "test")
				require.NoError(t, err)

				def := &schemapb.Schema{
					Name: ct.name + "_" + form,
					Columns: []*schemapb.Column{
						{Name: "k", StorageLayout: &schemapb.StorageLayout{Type: schemapb.StorageLayout_TYPE_STRING}},
						{Name: "v", StorageLayout: &schemapb.StorageLayout{
							Type: ct.layout, Nullable: form == "nullable", Repeated: form == "repeated",
						}},
					},
					SortingColumns: []*schemapb.SortingColumn{{Name: "k", Direction: schemapb.SortingColumn_DIRECTION_ASCENDING}},
				}
				table, err := db.Table(def.Name, NewTableConfig(def))
				require.NoError(t, err)

				rec := columnTypeRecord(t, ct.dt, ct.vals, form)
				defer rec.Release()
				_, err = table.InsertRecord(ctx, rec)
				require.NoError(t, err)

				fromMemory, err := readColumn(ctx, db, def.Name)
				require.NoError(t, err)
				require.Len(t, fromMemory, 2)

				require.NoError(t, table.EnsureCompaction(), "compacting to Parquet")
				fromParquet, err := readColumn(ctx, db, def.Name)
				require.NoError(t, err, "reading after compaction to Parquet")
				require.Equal(t, fromMemory, fromParquet, "values after compaction to Parquet")
			})
		}
	}
}
