package main

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/polarsignals/frostdb"
	"github.com/polarsignals/frostdb/query"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

// This example demonstrates how to use FrostDB's to aggregate data.
func main() {
	// Create a new column store
	columnstore, _ := frostdb.New()
	defer columnstore.Close()

	// Open up a database in the column store
	database, _ := columnstore.DB(context.Background(), "weather_db")

	// Create values to insert into the database. We support a dynamic structure for city to
	// accommodate cities in different regions
	type WeatherRecord struct {
		City     map[string]string `frostdb:",rle_dict,asc(0)"`
		Day      string            `frostdb:",rle_dict,asc(1)"`
		Snowfall float64
	}

	// Create a table named snowfall_table in our database
	table, _ := frostdb.NewGenericTable[WeatherRecord](
		database, "snowfall_table", memory.DefaultAllocator,
	)
	defer table.Release()

	montreal := map[string]string{
		"name":     "Montreal",
		"province": "Quebec",
	}
	toronto := map[string]string{
		"name":     "Toronto",
		"province": "Ontario",
	}
	minneapolis := map[string]string{
		"name":  "Minneapolis",
		"state": "Minnesota",
	}

	_, _ = table.Write(context.Background(),
		WeatherRecord{Day: "Mon", Snowfall: 20, City: montreal},
		WeatherRecord{Day: "Tue", Snowfall: 0o0, City: montreal},
		WeatherRecord{Day: "Wed", Snowfall: 30, City: montreal},
		WeatherRecord{Day: "Thu", Snowfall: 25.1, City: montreal},
		WeatherRecord{Day: "Fri", Snowfall: 10, City: montreal},
		WeatherRecord{Day: "Mon", Snowfall: 15, City: toronto},
		WeatherRecord{Day: "Tue", Snowfall: 25, City: toronto},
		WeatherRecord{Day: "Wed", Snowfall: 30, City: toronto},
		WeatherRecord{Day: "Thu", Snowfall: 0o0, City: toronto},
		WeatherRecord{Day: "Fri", Snowfall: 0o5, City: toronto},
		WeatherRecord{Day: "Mon", Snowfall: 40.8, City: minneapolis},
		WeatherRecord{Day: "Tue", Snowfall: 15, City: minneapolis},
		WeatherRecord{Day: "Wed", Snowfall: 32.3, City: minneapolis},
		WeatherRecord{Day: "Thu", Snowfall: 10, City: minneapolis},
		WeatherRecord{Day: "Fri", Snowfall: 12, City: minneapolis},
	)

	// Create a new query engine to retrieve data
	engine := query.NewEngine(memory.DefaultAllocator, database.TableProvider())

	// snowfall statistics by city:
	err := engine.ScanTable("snowfall_table").
		Aggregate(
			[]*logicalplan.AggregationFunction{
				logicalplan.Max(logicalplan.Col("snowfall")),
				logicalplan.Min(logicalplan.Col("snowfall")),
				logicalplan.Avg(logicalplan.Col("snowfall")),
			},
			[]logicalplan.Expr{logicalplan.Col("city.name")},
		).
		Execute(context.Background(), func(_ context.Context, r arrow.Record) error {
			// print the results
			fmt.Println(r)
			return nil
		})
	if err != nil {
		log.Fatal("snowfall statistics by city:", err)
	}

	// Total snowfall on each day of week:
	err = engine.ScanTable("snowfall_table").
		Aggregate(
			[]*logicalplan.AggregationFunction{
				logicalplan.Sum(logicalplan.Col("snowfall")),
			},
			[]logicalplan.Expr{logicalplan.Col("day")},
		).
		Execute(context.Background(), func(_ context.Context, r arrow.Record) error {
			// print the results
			fmt.Println(r)
			return nil
		})
	if err != nil {
		log.Fatal("total snowfall on each day of week:", err)
	}
}
