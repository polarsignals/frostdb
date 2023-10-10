<p align="center">
  <img width="700" src="https://user-images.githubusercontent.com/4546722/174077529-810726ff-a3c6-4ac4-b0f5-77f065124b09.png">
</p>


---

[![Go Reference](https://pkg.go.dev/badge/github.com/polarsignals/frostdb.svg)](https://pkg.go.dev/github.com/polarsignals/frostdb)
[![Go Report Card](https://goreportcard.com/badge/github.com/polarsignals/frostdb)](https://goreportcard.com/report/github.com/polarsignals/frostdb)
[![Build](https://github.com/polarsignals/frostdb/actions/workflows/go.yml/badge.svg)](https://github.com/polarsignals/frostdb/actions/workflows/go.yml)
[![Discord](https://img.shields.io/discord/813669360513056790?label=Discord)](https://discord.com/invite/ZgUpYgpzXy)

> This project is still in its infancy, consider it not production-ready, probably has various consistency and correctness problems and all API will change!

FrostDB is an embeddable wide-column columnar database written in Go. It features semi-structured schemas, uses [Apache Parquet](https://parquet.apache.org/) for storage, and [Apache Arrow](https://arrow.apache.org/) at query time. Building on top of Apache Arrow, FrostDB provides a query builder and various optimizers (using DataFrame-like APIs).

FrostDB is optimized for use cases where the majority of interactions are writes, with occasional analytical queries over this data. FrostDB was built specifically for [Parca](https://parca.dev/) for Observability use cases.

Read the announcement blog post to learn about what made us create it: https://www.polarsignals.com/blog/posts/2022/05/04/introducing-arcticdb/ (FrostDB was originally called ArcticDB)

## Why you should use FrostDB

Columnar data stores have become incredibly popular for analytics. Structuring data in columns instead of rows leverages the architecture of modern hardware, allowing for efficient processing of data.
A columnar data store might be right for you if you have workloads where you write a lot of data and need to perform analytics on that data.

FrostDB is similar to many other in-memory columnar databases such as [DuckDB](https://duckdb.org/) or [InfluxDB IOx](https://github.com/influxdata/influxdb_iox). 

FrostDB may be a better fit for you if:
- Are developing a Go program
- Want to embed a columnar database in your program instead of running a separate database server
- Have immutable datasets that don't require updating or deleting
- Your data contains dynamic columns, where the number of columns in the schema may increase at runtime

FrostDB is likely not suitable for your needs if:
- You aren't developing in Go
- You require a standalone database server
- You need to modify or delete your data
- You query by rows instead of columns

## Getting Started

You can explore the [examples](https://github.com/polarsignals/frostdb/tree/main/examples) directory for sample code using FrostDB. Below is a snippet from the simple database example. It creates a database with a dynamic column schema, inserts some data, and queries it back out.

https://github.com/polarsignals/frostdb/blob/d0eea82a3fcbd3e7275c9ed6c89546370516448c/examples/simple.go#L20-L94

## Design choices

FrostDB was specifically built for Observability workloads. This resulted in several characteristics that make it unique.

Table Of Contents:

* [Columnar Layout](#columnar-layout)
* [Dynamic Columns](#dynamic-columns)
* [Immutable & Sorted](#immutable--sorted)
* [Snapshot isolation](#snapshot-isolation)

### Columnar layout

Observability data is most useful when it is highly dimensional and those dimensions can be searched and aggregated by efficiently. Contrary to many relational databases (MySQL, PostgreSQL, CockroachDB, TiDB, etc.) that store data all data belonging to a single row together, a columnar layout stores all data of the same column in one contiguous chunk of data, making it very efficient to scan and aggregate data for any column. FrostDB uses [Apache Parquet](https://parquet.apache.org/) for storage, and [Apache Arrow](https://arrow.apache.org/) at query time. Apache Parquet is used for storage to make use of its efficient encodings to save on memory and disk space. Apache Arrow is used at query time as a foundation to vectorize the query execution.

### Dynamic Columns

While columnar databases already exist, most require a static schema. However, Observability workloads differ in that data their schemas are not static, meaning not all columns are pre-defined. Wide column databases already exist, but typically are not strictly typed (e.g. document databases), and most wide-column databases are row-based databases, not columnar databases.

Take a [Prometheus](https://prometheus.io/) time-series for example. Prometheus time-series are uniquely identified by the combination of their label-sets:

```
http_requests_total{path="/api/v1/users", code="200"} 12
```

This model does not map well into a static schema, as label-names cannot be known upfront. The most suitable data-type some columnar databases have to offer is a map, however, maps have the same problems as row-based databases, where all values of a map in a row are stored together, resulting in an inability to exploit the advantages of a columnar layout. A FrostDB schema can define a column to be dynamic, causing a column to be created on the fly when a new label-name is seen.

A FrostDB schema for Prometheus could look like this:

```go
package frostprometheus

import (
	"github.com/polarsignals/frostdb/dynparquet"
	schemapb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/schema/v1alpha1"
)

func Schema() (*dynparquet.Schema, error) {
	return dynparquet.SchemaFromDefinition(&schemapb.Schema{
		Name: "prometheus",
		Columns: []*schemapb.Column{{
			Name: "labels",
			StorageLayout: &schemapb.StorageLayout{
				Type:     schemapb.StorageLayout_TYPE_STRING,
				Encoding: schemapb.StorageLayout_ENCODING_RLE_DICTIONARY,
				Nullable: true,
			},
			Dynamic: true,
		}, {
			Name: "timestamp",
			StorageLayout: &schemapb.StorageLayout{
				Type: schemapb.StorageLayout_TYPE_INT64,
			},
			Dynamic: false,
		}, {
			Name: "value",
			StorageLayout: &schemapb.StorageLayout{
				Type: schemapb.StorageLayout_TYPE_DOUBLE,
			},
			Dynamic: false,
		}},
		SortingColumns: []*schemapb.SortingColumn{{
			Name:      "timestamp",
			Direction: schemapb.SortingColumn_DIRECTION_ASCENDING,
		}, {
			Name:       "labels",
			NullsFirst: true,
			Direction:  schemapb.SortingColumn_DIRECTION_ASCENDING,
		}},
	})
}
```

> Note: We are aware that Prometheus uses double-delta encoding for timestamps and XOR encoding for values. This schema is purely an example to highlight the dynamic columns feature.

With this schema, all rows are expected to have a `timestamp` and a `value` but can vary in their columns prefixed with `labels.`. In this schema all dynamically created columns are still Dictionary and run-length encoded and must be of type `string`.

### Immutable

There are only writes and reads. All data is immutable. 

FrostDB maintains inserted data in an Log-structured merge-tree(LSM) like index. This index is implemented as lists of Parts. A Part containers either an Arrow record or a 
Parquet file. The first level (L0) contains a list of Arrrow records inserted as-is into the list. Upon reaching the maximum configured size of the level the level will be compacted
 into a single Parquet file and added to the next level of the index. This process continues for each configured level of the index until a file is written into the final level of the index.

![LSM Index compacting into higher levels](https://docs.google.com/drawings/d/e/2PACX-1vRckTzb-D57UaxDUSCQjyD4mZN_3_Cu032oA-2kLCu_owrYeaT4nrYCKBP1QYS8EMqv3bI3Kiudt_jV/pub?w=960&h=720)

Upon the size of the entire index reaching the configured max in-memory size the index is rotated out. It can be either configured to be dropped entirely or to be written out to 
your storage of choice.

At query time FrostDB will scan each part in the in the index. To maintain fast queries FrostDB leverages the sparse index features of Parquet files, such as bloom filters and min
and max values of columns in each row group such that only the row groups that contain data that can satisfy the query are processed.

### Snapshot isolation

FrostDB has snapshot isolation, however, it comes with a few caveats that should be well understood. It does not have read-after-write consistency as the intended use is for users reading data that are not the same as the entity writing data to it. To see new data the user re-runs a query. Choosing to trade-off read-after-write consistency allows for mechanisms to increase throughput significantly. FrostDB releases write transactions in batches. It essentially only ensures write atomicity and that writes are not torn when reading. Since data is immutable, those characteristics together result in snapshot isolation. 

More concretely, FrostDB maintains a watermark indicating that all transactions equal and lower to the watermark are safe to be read. Only write transactions obtain a _new_ transaction ID, while reads use the transaction ID of the watermark to identify data that is safe to be read. The watermark is only increased when strictly monotonic, consecutive transactions have finished. This means that a low write transaction can block higher write transactions to become available to be read. To ensure progress is made, write transactions have a timeout.

This mechanism is inspired by a mix of [Google Spanner](https://research.google/pubs/pub39966/), [Google Percolator](https://research.google/pubs/pub36726/) and [Highly Available Transactions](https://www.vldb.org/pvldb/vol7/p181-bailis.pdf).

![Transactions are released in batches indicated by the watermark](https://docs.google.com/drawings/d/1qmcMg9sXnDZix9eWSvOtWJD06yHsLpgho8M-DGF84bU/export/svg)

## Acknowledgments

FrostDB stands on the shoulders of giants. Shout out to Segment for creating the incredible [`parquet-go`](https://github.com/parquet-go/parquet-go) library as well as InfluxData for starting and various contributors after them working on [Go support for Apache Arrow](https://pkg.go.dev/github.com/apache/arrow/go/arrow).
