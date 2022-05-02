[![Go Reference](https://pkg.go.dev/badge/github.com/polarsignals/arcticdb.svg)](https://pkg.go.dev/github.com/polarsignals/arcticdb)
[![Go Report Card](https://goreportcard.com/badge/github.com/polarsignals/arcticdb)](https://goreportcard.com/report/github.com/polarsignals/arcticdb)
![Build](https://github.com/polarsignals/arcticdb/actions/workflows/go.yml/badge.svg)


# ArcticDB

> This project is still in its infancy, consider it not production-ready, probably has various consistency and correctness problems and all API will change!

ArcticDB is an embeddable columnar database written in Go. It features semi-structured schemas (could also be described as typed wide-columns), and uses [Apache Parquet](https://parquet.apache.org/) for storage, and [Apache Arrow](https://arrow.apache.org/) at query time. Building on top of Apache Arrow, ArcticDB provides a query builder and various optimizers (it reminds of DataFrame-like APIs).

ArcticDB is optimized for use cases where the majority of interactions are writes, and when data is queried, a lot of data is queried at once (our use case at Polar Signals can be broadly described as Observability and specifically for [Parca](https://parca.dev/)). It could also be described as a wide-column columnar database.

## Design choices

ArcticDB was specifically built for Observability workloads. This resulted in several characteristics that make it unique in its combination.

Table Of Contents:

* [Columnar Layout](#columnar-layout)
* [Dynamic Columns](#dynamic-columns)
* [Immutable & Sorted](#immutable--sorted)
* [Consistency trade-offs](#consistency-trade-offs)

### Columnar layout

Observability data is most useful when highly dimensional and those dimensions can be searched and aggregated by efficiently. Contrary to many relational databases like (MySQL, PostgreSQL, CockroachDB, TiDB, etc.) that store data all data belonging to a single row together, in a columnar layout all data of the same column in a table is available in one contiguous chunk of data, making it very efficient to scan and more importantly, only the data truly necessary for a query is loaded in the first place. ArcticDB uses [Apache Parquet](https://parquet.apache.org/) for storage, and [Apache Arrow](https://arrow.apache.org/) at query time. Apache Parquet is used for storage to make use of its efficient encodings to save on memory and disk space. Apache Arrow is used at query time as a foundation to vectorize the query execution.

### Dynamic Columns

While columnar databases already exist, most require a static schema, however, Observability workloads differ in that their schemas are not static, meaning not all columns are pre-defined. On the other hand, wide column databases also already exist, but typically are not strictly typed, and most wide-column databases are row-based databases, not columnar databases.

Take a [Prometheus](https://prometheus.io/) time-series for example. Prometheus time-series are uniquely identified by the combination of their label-sets:

```
http_requests_total{path="/api/v1/users", code="200"} 12
```

This model does not map well into a static schema, as label-names cannot be known upfront. The most suitable data-type some columnar databases have to offer is a map, however, maps have the same problems as row-based databases, where all values of a map in a row are stored together, unable to exploit the advantages of a columnar layout. An ArcticDB schema can define a column to be dynamic, causing a column to be created on the fly when a new label-name is seen.

An ArcticDB schema for Prometheus could look like this:

```go
package arcticprometheus

import (
	"github.com/polarsignals/arcticdb/dynparquet"
	"github.com/segmentio/parquet-go"
)

func Schema() *dynparquet.Schema {
	return dynparquet.NewSchema(
		"prometheus",
		[]dynparquet.ColumnDefinition{{
			Name:          "labels",
			StorageLayout: parquet.Encoded(parquet.Optional(parquet.String()), &parquet.RLEDictionary),
			Dynamic:       true,
		}, {
			Name:          "timestamp",
			StorageLayout: parquet.Int(64),
			Dynamic:       false,
		}, {
			Name:          "value",
			StorageLayout: parquet.Leaf(parquet.DoubleType),
			Dynamic:       false,
		}},
		[]dynparquet.SortingColumn{
			dynparquet.NullsFirst(dynparquet.Ascending("labels")),
			dynparquet.Ascending("timestamp"),
		},
	)
}
```

> Note: We are aware that Prometheus uses double-delta encoding for timestamps and XOR encoding for values. This schema is purely an example to highlight the dynamic columns feature.

With this schema, all rows are expected to have a `timestamp` and a `value` but can vary in their columns prefixed with `labels.`. In this schema all dynamically created columns are still Dictionary and run-length encoded and must be of type `string`.

### Immutable & Sorted

There are only writes and reads. All data is immutable and sorted. Having all data sorted allows ArcticDB to avoid maintaining an index per column, and still serve queries with low latency.

To maintain global sorting ArcticDB requires all inserts to be sorted if they contain multiple rows. Combined with immutability, global sorting of all data can be maintained at a reasonable cost. To optimize throughput, it is preferable to perform inserts in as large batches as possible. ArcticDB maintains inserted data in batches of a configurable amount of rows (by default 8192), called a _Granule_. To directly jump to data needed for a query, ArcticDB maintains a sparse index of Granules. The sparse index is small enough to fully reside in memory, it is currently implemented as a [b-tree](https://github.com/google/btree) of Granules.

![Sparse index of Granules](https://docs.google.com/drawings/d/1DbGqLKsloKAEG7ydJ5n5-Vr03j4jQMqdipJyEu0goIE/export/svg)

At insert time, ArcticDB splits the inserted rows into the appropriate Granule according to their lower and upper bound, to maintain global sorting. Once a Granule exceeds the configured amount, the Granule is split into `N` new Granules depending.

![Split of Granule](https://docs.google.com/drawings/d/1c38HQfpTPVtzatGenQaqF7oA_7NiEDbfeudxiUV5lSg/export/svg)

Under the hood, Granules are a list of sorted Parts, and only if a query requires it are all parts merged into a sorted stream using a [direct k-way merge](https://en.wikipedia.org/wiki/K-way_merge_algorithm#Direct_k-way_merge) using a [min-heap](https://en.wikipedia.org/wiki/Binary_heap). An example of an operation that requires the whole Granule to be read as a single sorted stream are the aforementioned Granule splits.

![A Granule is organized in Parts](https://docs.google.com/drawings/d/1Ex4hKLwoQ_IgYARj0aEjoFEjQRt6-B0fO8K9E7syyHc/export/svg)

### Consistency trade-offs

ArcticDB has a weak consistency model. It does not have read-after-write consistency as the intended use is for users reading data that are not the same as the entity writing data to it. To see new data the user re-runs a query. Choosing to trade-off read-after-write consistency allows for mechanisms to increase throughput significantly. ArcticDB releases write transactions in batches. It essentially only ensures write atomicity and that writes are not torn when reading.

ArcticDB maintains a watermark indicating that all transactions equal and lower to the watermark are safe to be read. Only write transactions obtain a _new_ transaction ID, while reads use the transaction ID of the watermark to identify data that is safe to be read. The watermark is only increased when strictly monotonic, consecutive transactions have finished. This means that a low write transaction can block higher write transactions to become available to be read. To ensure progress is made, write transactions have a timeout.

![Transactions are released in batches indicated by the watermark](https://docs.google.com/drawings/d/1qmcMg9sXnDZix9eWSvOtWJD06yHsLpgho8M-DGF84bU/export/svg)

## Roadmap

* Persistence: ArcticDB is currently fully in-memory.

## Acknowledgments

ArcticDB stands on the shoulders of giants. Shout out to Segment for creating the incredible [`parquet-go`](https://github.com/segmentio/parquet-go) library as well as InfluxData for starting and various contributors after them working on [Go support for Apache Arrow](https://pkg.go.dev/github.com/apache/arrow/go/arrow).
