package main

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/go-kit/log"
	"github.com/polarsignals/frostdb"
	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/query"
	"github.com/polarsignals/frostdb/query/logicalplan"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/parquet-go"
)

// This example demonstrates how to create a simple FrostDB with a dynamic labels column that stores float values.
func main() {

	// Create a new column store
	columnstore := frostdb.New(
		prometheus.NewRegistry(),
		8192,
		10*1024*1024, // 10MiB
	)

	// Open up a database in the column store
	database, _ := columnstore.DB("simple_db")

	// Define our simple schema of labels and values
	schema := simpleSchema()

	// Create a table named simple in our database
	table, _ := database.Table(
		"simple_table",
		frostdb.NewTableConfig(schema),
		log.NewNopLogger(),
	)

	// Create values to insert into the database these first rows havel dynamic label names of 'firstname' and 'surname'
	buf, _ := schema.NewBuffer(map[string][]string{
		"names": {"firstname", "surname"},
	})

	// firstname:Frederic surname:Brancz 100
	buf.WriteRows([]parquet.Row{{
		parquet.ValueOf("Frederic").Level(0, 1, 0),
		parquet.ValueOf("Brancz").Level(0, 1, 1),
		parquet.ValueOf(100).Level(0, 0, 2),
	}})

	// firstname:Thor surname:Hansen 10
	buf.WriteRows([]parquet.Row{{
		parquet.ValueOf("Thor").Level(0, 1, 0),
		parquet.ValueOf("Hansen").Level(0, 1, 1),
		parquet.ValueOf(10).Level(0, 0, 2),
	}})
	table.InsertBuffer(context.Background(), buf)

	// Now we can insert rows that have middle names into our dynamic column
	buf, _ = schema.NewBuffer(map[string][]string{
		"names": {"firstname", "middlename", "surname"},
	})
	// firstname:Matthias middlename:Oliver surname:Loibl 1
	buf.WriteRows([]parquet.Row{{
		parquet.ValueOf("Matthias").Level(0, 1, 0),
		parquet.ValueOf("Oliver").Level(0, 1, 1),
		parquet.ValueOf("Loibl").Level(0, 1, 2),
		parquet.ValueOf(1).Level(0, 0, 3),
	}})
	table.InsertBuffer(context.Background(), buf)

	// Create a new query engine to retrieve data and print the results
	engine := query.NewEngine(memory.DefaultAllocator, database.TableProvider())
	engine.ScanTable("simple_table").
		Filter(
			logicalplan.Col("names.firstname").Eq(logicalplan.Literal("Frederic")),
		).Execute(context.Background(), func(r arrow.Record) error {
		fmt.Println(r)
		return nil
	})
}

func simpleSchema() *dynparquet.Schema {
	return dynparquet.NewSchema(
		"simple_schema",
		[]dynparquet.ColumnDefinition{{
			Name:          "names",
			StorageLayout: parquet.Encoded(parquet.Optional(parquet.String()), &parquet.RLEDictionary),
			Dynamic:       true,
		}, {
			Name:          "value",
			StorageLayout: parquet.Int(64),
			Dynamic:       false,
		}},
		[]dynparquet.SortingColumn{
			dynparquet.Ascending("names"),
		},
	)
}
