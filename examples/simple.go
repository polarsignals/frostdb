package main

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/memory"

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
	defer columnstore.Close()

	// Open up a database in the column store
	database, _ := columnstore.DB(context.Background(), "simple_db")

	// Define our simple schema of labels and values
	schema, _ := simpleSchema()

	// Create a table named simple in our database
	table, _ := database.Table(
		"simple_table",
		frostdb.NewTableConfig(schema),
	)

	type FirstLast struct {
		FirstName string
		Surname   string
	}

	type Simple struct {
		Names FirstLast
		Value int64
	}
	// Create values to insert into the database these first rows havel dynamic label names of 'firstname' and 'surname'
	frederic := Simple{
		Names: FirstLast{
			FirstName: "Frederic",
			Surname:   "Brancz",
		},
		Value: 100,
	}

	thor := Simple{
		Names: FirstLast{
			FirstName: "Thor",
			Surname:   "Hansen",
		},
		Value: 99,
	}
	_, _ = table.Write(context.Background(), frederic, thor)

	// Now we can insert rows that have middle names into our dynamic column
	matthias := struct {
		Names struct {
			FirstName  string
			MiddleName string
			Surname    string
		}
		Value int64
	}{
		Names: struct {
			FirstName  string
			MiddleName string
			Surname    string
		}{
			FirstName:  "Matthias",
			MiddleName: "Oliver Rainer",
			Surname:    "Loibl",
		},
		Value: 101,
	}
	_, _ = table.Write(context.Background(), matthias)

	// Create a new query engine to retrieve data and print the results
	engine := query.NewEngine(memory.DefaultAllocator, database.TableProvider())
	_ = engine.ScanTable("simple_table").
		Project(logicalplan.DynCol("names")). // We don't know all dynamic columns at query time, but we want all of them to be returned.
		Filter(
			logicalplan.Col("names.first_name").Eq(logicalplan.Literal("Frederic")),
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
