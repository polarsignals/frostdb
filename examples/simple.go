package main

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/segmentio/parquet-go"

	"github.com/polarsignals/frostdb"
	"github.com/polarsignals/frostdb/dynparquet"
	schemapb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/schema/v1alpha1"
	"github.com/polarsignals/frostdb/query"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

// This example demonstrates how to create a simple FrostDB with a dynamic labels column that stores float values.
func main() {

	// Create a new column store
	columnstore, _ := frostdb.New()

	// Open up a database in the column store
	database, _ := columnstore.DB(context.Background(), "simple_db")

	// Define our simple schema of labels and values
	schema, _ := simpleSchema()

	// Create a table named simple in our database
	table, _ := database.Table(
		"simple_table",
		frostdb.NewTableConfig(schema),
	)

	// Create values to insert into the database these first rows havel dynamic label names of 'firstname' and 'surname'
	buf, _ := schema.NewBuffer(map[string][]string{
		"names": {"firstname", "surname"},
	})

	// firstname:Frederic surname:Brancz 100
	_, _ = buf.WriteRows([]parquet.Row{{
		parquet.ValueOf("Frederic").Level(0, 1, 0),
		parquet.ValueOf("Brancz").Level(0, 1, 1),
		parquet.ValueOf(100).Level(0, 0, 2),
	}})

	// firstname:Thor surname:Hansen 10
	_, _ = buf.WriteRows([]parquet.Row{{
		parquet.ValueOf("Thor").Level(0, 1, 0),
		parquet.ValueOf("Hansen").Level(0, 1, 1),
		parquet.ValueOf(10).Level(0, 0, 2),
	}})
	_, _ = table.InsertBuffer(context.Background(), buf)

	// Now we can insert rows that have middle names into our dynamic column
	buf, _ = schema.NewBuffer(map[string][]string{
		"names": {"firstname", "middlename", "surname"},
	})
	// firstname:Matthias middlename:Oliver surname:Loibl 1
	_, _ = buf.WriteRows([]parquet.Row{{
		parquet.ValueOf("Matthias").Level(0, 1, 0),
		parquet.ValueOf("Oliver").Level(0, 1, 1),
		parquet.ValueOf("Loibl").Level(0, 1, 2),
		parquet.ValueOf(1).Level(0, 0, 3),
	}})
	_, _ = table.InsertBuffer(context.Background(), buf)

	// Create a new query engine to retrieve data and print the results
	engine := query.NewEngine(memory.DefaultAllocator, database.TableProvider())
	_ = engine.ScanTable("simple_table").
		Project(logicalplan.DynCol("names")). // We don't know all dynamic columns at query time, but we want all of them to be returned.
		Filter(
			logicalplan.Col("names.firstname").Eq(logicalplan.Literal("Frederic")),
		).Execute(context.Background(), func(ctx context.Context, r arrow.Record) error {
		fmt.Println(r)
		return nil
	})
}

func simpleSchema() (*dynparquet.Schema, error) {
	return dynparquet.SchemaFromDefinition(&schemapb.Schema{
		Name: "simple_schema",
		Columns: []*schemapb.Column{{
			Name: "names",
			StorageLayout: &schemapb.StorageLayout{
				Type:     schemapb.StorageLayout_TYPE_STRING,
				Encoding: schemapb.StorageLayout_ENCODING_RLE_DICTIONARY,
				Nullable: true,
			},
			Dynamic: true,
		}, {
			Name: "value",
			StorageLayout: &schemapb.StorageLayout{
				Type: schemapb.StorageLayout_TYPE_INT64,
			},
			Dynamic: false,
		}},
		SortingColumns: []*schemapb.SortingColumn{{
			Name:      "names",
			Direction: schemapb.SortingColumn_DIRECTION_ASCENDING,
		}},
	})
}
