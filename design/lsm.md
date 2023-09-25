# Log-structured merge-tree (LSM) style index

Authored: Sept 21, 2023
Author: Thor Hansen (@thorfour)

## Abstract

FrostDB was built using a BTree as it's index. This decision was made based on early assumptions and before the query engine
had taken shape. Now with a better understanding of how FrostDB works in practice it is my recommendation that we replace the Btree index with
an LSM-like index.

## Problem

Insertion spends the largest portion of CPU cycles on sorting the individual rows of records into their corresponding granules. However due to the multi-key primary key 
schemas coupled with dynamic columns, the query engine does not leverage the sorted nature of rows during query time. The scan layer simply passes the records into the query engine.
Which means the only benefit from sorting the rows is that during compaction a granule already contains the rows that will likely offer the best compaction benefits.

However this means that we're slowing down insertion times for potential gains only during compaction time. We've witnessed things like replay and recovery become dominated by the 
insert path.

## Proposal

Instead of using a Btree where we sort the individual rows of each record into granules, we instead use an LSM-style index where the entire record is simply appended to the first level
of the LSM index. Because of the immutable nature of the data in FrostDB we do not require all the features of an LSM trree. But are instead lifting the ideas of having different
levels of compacted data. With the first level being a raw list of arrow records that have been written, and subsequent levels being compacted records.


|---------|    |---------|    |---------|
|         |    |         |    |         |
|  Record |--->|  Record |--->|  Record |   L0: Raw written Arrow records (memtable in most LSM definitions)
|         |    |         |    |         |
|_________|    |_________|    |_________|

|---------|    |---------|    |---------|
|         |    |         |    |         |
| Parquet |--->| Parquet |--->| Parquet |   L1: Compacted L0 in Parquet files.
|         |    |         |    |         |
|_________|    |_________|    |_________|

                Fig 1.

Note that this is NOT a traditional LSM tree that needs to perform a merging of data at different levels due to the fact that neither deletes nor inserts are performed. Due to the 
nature of querying in FrostDB where each file is scanned we need not sort the inserted records into a tree at all but instead keep all data in a single linked-list. 
Therefore each subsequent level of the index is compacted and sorting solely for the purpose of reducing the memory footprint of the previous level of unsorted uncompacted files.


### Compaction

Compaction would work much like it does today except instead of compacting at the Granule level it takes all of L0 and compacts those records into a Parquet file (potentially in the future another larger arrow record).
This compacted file would get added to the L1 list. For brevity there's only 2 levels in the diagram but there's no reason should workloads require it to only have two levels. Further levels could be added
that compact the level above it, and even potentially use disk or remote storage to retain one or more levels.

Because the data is immutable and there is no support for updates or deletions during compaction the compactor need not try and seach for conflicting rows of updates or deletes but
can simply compact all the data into a combined record/file.
