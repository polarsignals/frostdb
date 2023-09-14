package logictest

import (
	"context"
	"testing"

	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"

	"github.com/polarsignals/frostdb"
	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/query"
	"github.com/polarsignals/frostdb/query/physicalplan"

	schemapb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/schema/v1alpha1"
)

const testdataDirectory = "testdata"

type frostDB struct {
	*frostdb.DB
	allocator *query.LimitAllocator
}

func (db frostDB) CreateTable(name string, schema *schemapb.Schema) (Table, error) {
	return db.DB.Table(name, frostdb.NewTableConfig(schema))
}

func (db frostDB) ScanTable(name string) query.Builder {
	queryEngine := query.NewEngine(
		db.allocator,
		db.DB.TableProvider(),
		query.WithPhysicalplanOptions(
			physicalplan.WithOrderedAggregations(),
		),
	)
	return queryEngine.ScanTable(name)
}

var schemas = map[string]*schemapb.Schema{
	"default": dynparquet.SampleDefinition(),
	"simple_bool": {
		Name: "simple_bool",
		Columns: []*schemapb.Column{{
			Name: "name",
			StorageLayout: &schemapb.StorageLayout{
				Type:     schemapb.StorageLayout_TYPE_STRING,
				Encoding: schemapb.StorageLayout_ENCODING_RLE_DICTIONARY,
			},
		}, {
			Name: "found",
			StorageLayout: &schemapb.StorageLayout{
				Type: schemapb.StorageLayout_TYPE_BOOL,
			},
		}},
		SortingColumns: []*schemapb.SortingColumn{{
			Name:      "found",
			Direction: schemapb.SortingColumn_DIRECTION_ASCENDING,
		}},
	},
	"prehashed": {
		Name: "test",
		Columns: []*schemapb.Column{{
			Name: "labels",
			StorageLayout: &schemapb.StorageLayout{
				Type:     schemapb.StorageLayout_TYPE_STRING,
				Nullable: true,
				Encoding: schemapb.StorageLayout_ENCODING_RLE_DICTIONARY,
			},
			Dynamic: true,
			Prehash: true,
		}, {
			Name: "stacktrace",
			StorageLayout: &schemapb.StorageLayout{
				Type:     schemapb.StorageLayout_TYPE_STRING,
				Encoding: schemapb.StorageLayout_ENCODING_RLE_DICTIONARY,
			},
			Dynamic: false,
			Prehash: true,
		}, {
			Name: "timestamp",
			StorageLayout: &schemapb.StorageLayout{
				Type: schemapb.StorageLayout_TYPE_INT64,
			},
			Dynamic: false,
		}, {
			Name: "value",
			StorageLayout: &schemapb.StorageLayout{
				Type: schemapb.StorageLayout_TYPE_INT64,
			},
			Dynamic: false,
		}},
		SortingColumns: []*schemapb.SortingColumn{{
			Name:      "example_type",
			Direction: schemapb.SortingColumn_DIRECTION_ASCENDING,
		}, {
			Name:       "labels",
			Direction:  schemapb.SortingColumn_DIRECTION_ASCENDING,
			NullsFirst: true,
		}, {
			Name:      "timestamp",
			Direction: schemapb.SortingColumn_DIRECTION_ASCENDING,
		}, {
			Name:       "stacktrace",
			Direction:  schemapb.SortingColumn_DIRECTION_ASCENDING,
			NullsFirst: true,
		}},
	},
}

// TestLogic runs all the datadriven tests in the testdata directory. Refer to
// the RunCmd method of the Runner struct for more information on the expected
// syntax of these tests. If this test fails but the results look the same, it
// might be because the test returns tab-separated expected results and your
// IDE inserts tabs instead of spaces. Just run this test with the -rewrite flag
// to rewrite expected results.
// TODO(asubiotto): Add metamorphic testing to logic tests. The idea is to
// randomly generate variables that should have no effect on output (e.g. row
// group split points, granule split size).
func TestLogic(t *testing.T) {
	ctx := context.Background()
	t.Parallel()
	isArrow := []bool{true, false}
	for _, arrow := range isArrow {
		datadriven.Walk(t, testdataDirectory, func(t *testing.T, path string) {
			columnStore, err := frostdb.New()
			require.NoError(t, err)
			defer columnStore.Close()
			db, err := columnStore.DB(ctx, "test")
			require.NoError(t, err)
			fdb := frostDB{
				DB:        db,
				allocator: query.NewLimitAllocator(1024*1024*1024, memory.DefaultAllocator),
			}
			r := NewRunner(fdb, schemas)
			datadriven.RunTest(t, path, func(t *testing.T, c *datadriven.TestData) string {
				return r.RunCmd(ctx, c, arrow)
			})
			if path != "testdata/exec/aggregate/ordered_aggregate" { // NOTE: skip checking the limit for the ordered aggregator as it still leaks memory.
				require.Equal(t, 0, fdb.allocator.Allocated())
			}
		})
	}
}

func SchemaMust(def *schemapb.Schema) *dynparquet.Schema {
	schema, err := dynparquet.SchemaFromDefinition(def)
	if err != nil {
		panic(err.Error())
	}

	return schema
}
