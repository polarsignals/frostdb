package main

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/go-kit/log"
	"github.com/polarsignals/arcticdb"
	"github.com/polarsignals/arcticdb/dynparquet"
	"github.com/polarsignals/arcticdb/query"
	"github.com/polarsignals/arcticdb/query/logicalplan"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/parquet-go"
)

// This example demonstrates how to create a simple ArcticDB with a dynamic labels column that stores float values.
func main() {

	// Create a new column store
	columnstore := arcticdb.New(
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
		arcticdb.NewTableConfig(schema),
		log.NewNopLogger(),
	)

	_, err := table.Write([]arcticdb.Row{
		{
			"names": map[string]interface{}{
				"firstname": "Frederic",
				"surname":   "Brancz",
			},
			"value": 60,
		}, {
			"names": map[string]interface{}{
				"firstname": "Thor",
				"surname":   "Hansen",
			},
			"value": 10,
		}, {
			"names": map[string]interface{}{
				"firstname":  "Matthias",
				"middlename": "Oliver",
				"surname":    "Loibl",
			},
			"value": 100,
		},
	})
	if err != nil {
		panic(err.Error())
	}

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
			StorageLayout: parquet.Leaf(parquet.DoubleType),
			Dynamic:       false,
		}},
		[]dynparquet.SortingColumn{
			dynparquet.Ascending("names"),
		},
	)
}
