package main

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/memory"

	"github.com/polarsignals/frostdb"
	schemapb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/schema/v1alpha1"
	"github.com/polarsignals/frostdb/query"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

// This example demonstrates how to use FrostDB's to aggregate data
func main() {

	// Create a new column store
	columnstore, _ := frostdb.New()
	defer columnstore.Close()

	// Open up a database in the column store
	database, _ := columnstore.DB(context.Background(), "weather_db")

	// Define our aggregation schema of labels and values
	schema := aggregationSchema()

	// Create a table named snowfall_table in our database
	table, _ := database.Table(
		"snowfall_table",
		frostdb.NewTableConfig(schema),
	)

	// Create values to insert into the database. We support a dynamic structure for city to
	// accommodate cities in different regions
	type WeatherRecord struct {
		City     interface{}
		Day      string
		Snowfall int64
		// Snowfall2 float64
	}

	type CityInProvince struct {
		Name     string
		Province string
	}

	type CityInState struct {
		Name  string
		State string
	}

	montreal := CityInProvince{Name: "Montreal", Province: "Quebec"}
	toronto := CityInProvince{Name: "Toronto", Province: "Ontario"}
	minneapolis := CityInState{Name: "Minneapolis", State: "Minnesota"}

	_, _ = table.Write(context.Background(),
		WeatherRecord{Day: "Mon", Snowfall: 20, City: montreal},
		WeatherRecord{Day: "Tue", Snowfall: 00, City: montreal},
		WeatherRecord{Day: "Wed", Snowfall: 30, City: montreal},
		WeatherRecord{Day: "Thu", Snowfall: 25, City: montreal},
		WeatherRecord{Day: "Fri", Snowfall: 10, City: montreal},
		WeatherRecord{Day: "Mon", Snowfall: 15, City: toronto},
		WeatherRecord{Day: "Tue", Snowfall: 25, City: toronto},
		WeatherRecord{Day: "Wed", Snowfall: 30, City: toronto},
		WeatherRecord{Day: "Thu", Snowfall: 00, City: toronto},
		WeatherRecord{Day: "Fri", Snowfall: 05, City: toronto},
		WeatherRecord{Day: "Mon", Snowfall: 40.0, City: minneapolis},
		WeatherRecord{Day: "Tue", Snowfall: 15, City: minneapolis},
		WeatherRecord{Day: "Wed", Snowfall: 32, City: minneapolis},
		WeatherRecord{Day: "Thu", Snowfall: 10, City: minneapolis},
		WeatherRecord{Day: "Fri", Snowfall: 12, City: minneapolis},
	)

	// Create a new query engine to retrieve data
	engine := query.NewEngine(memory.DefaultAllocator, database.TableProvider())

	// snowfall statistics by city:
	_ = engine.ScanTable("snowfall_table").
		Aggregate(
			[]logicalplan.Expr{
				logicalplan.Max(logicalplan.Col("snowfall")),
				logicalplan.Min(logicalplan.Col("snowfall")),
				// logicalplan.Avg(logicalplan.Col("snowfall")),
			},
			[]logicalplan.Expr{logicalplan.Col("city.name")},
		).
		Execute(context.Background(), func(ctx context.Context, r arrow.Record) error {
			// print the results
			fmt.Println(r)
			return nil
		})

	// Total snowfall on each day of week:
	// _ = engine.ScanTable("snowfall_table").
	// 	Aggregate(
	// 		[]logicalplan.Expr{
	// 			logicalplan.Sum(logicalplan.Col("snowfall2")),
	// 		},
	// 		[]logicalplan.Expr{logicalplan.Col("day")},
	// 	).
	// 	Execute(context.Background(), func(ctx context.Context, r arrow.Record) error {
	// 		// print the results
	// 		fmt.Println(r)
	// 		return nil
	// 	})
}

func aggregationSchema() *schemapb.Schema {
	return &schemapb.Schema{
		Name: "snowfall_table",
		Columns: []*schemapb.Column{
			{
				Name: "city",
				StorageLayout: &schemapb.StorageLayout{
					Type:     schemapb.StorageLayout_TYPE_STRING,
					Encoding: schemapb.StorageLayout_ENCODING_RLE_DICTIONARY,
					Nullable: false,
				},
				Dynamic: true,
			},
			{
				Name: "day",
				StorageLayout: &schemapb.StorageLayout{
					Type:     schemapb.StorageLayout_TYPE_STRING,
					Encoding: schemapb.StorageLayout_ENCODING_RLE_DICTIONARY,
					Nullable: false,
				},
			},
			{
				Name: "snowfall",
				StorageLayout: &schemapb.StorageLayout{
					Type: schemapb.StorageLayout_TYPE_DOUBLE,
				},
				Dynamic: false,
			},
		},
		SortingColumns: []*schemapb.SortingColumn{
			{
				Name:      "city",
				Direction: schemapb.SortingColumn_DIRECTION_ASCENDING,
			},
			{
				Name:      "day",
				Direction: schemapb.SortingColumn_DIRECTION_ASCENDING,
			},
		},
	}
}
