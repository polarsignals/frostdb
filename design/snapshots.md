# Snapshots

Authored: Mar 8, 2023
Author: Alfonso Subiotto (@asubiotto)

## Abstract

Up until now, FrostDB has offered crash recovery solely with a write ahead log (WAL), where database operations are written one-by-one to an on-disk log. This document proposes complementing the WAL with point-in-time snapshots to reduce recovery time, WAL disk space, and WAL corruption issues.

## Problem

Running FrostDB with a WAL in production we've noticed three problems:
- Running FrostDB with multiple small tables implies that the WAL is rarely, if ever, truncated. This is because the WAL is shared amongst tables and when persisting a table, the WAL can only be truncated at the minimum persisted transaction across all tables. This causes unnecessarily large WALs.
- WAL replay times upon recovery can be unreasonably long (on the order of many minutes). This is a problem because database requests cannot be served until the previous state has been recovered. The cause of this performance issue is that writes are replayed one at a time.
- WAL files are sometimes corrupted on preemptible VMs even though the library we use uses fsync. It's unclear whether non-atomic truncation is also an issue.

## Proposal

This document proposes to complement the WAL with point-in-time snapshots. Snapshots should help us with the mentioned problems in the following ways:
- Snapshots are performed at database granularity. This means that regardless of how many tables the database has or their sizes, the WAL can be safely truncated at the transaction a snapshot occurred, reducing WAL size.
- Snapshot big-o load times are a function of the number of parts in the database, rather than the number of inserts. Additionally, since the index structure is stored in a snapshot file, snapshots avoid having to perform an index search per insert which is a huge contributing factor to slow WAL replay performance.
- Relying on snapshots implies that the WAL only needs to be replayed from the transaction of the last snapshot, adding redundancy for recovery. It's unclear whether this will solve our corruption problems, but reducing our reliance on the WAL should help us in theory.

### Format

Refer to [the snapshot .proto file](https://github.com/polarsignals/frostdb/blob/main/proto/frostdb/snapshot/v1alpha1/snapshot.proto) for information on snapshot metadata. Note that only metadata is stored in protobufs. This metadata is stored in the footer of a snapshot file and includes offsets to look up actual table data in the rest of the file. Refer to the [snapshot.go file](https://github.com/polarsignals/frostdb/blob/974038eeb6072e915f8e28eea5a18609f4f7ac74/snapshot.go#L28) for an in-depth explanation of the format of the full snapshot file.

### Interval

An option can be provided when opening a column store to define a "trigger size" in bytes. This is the number of bytes a table block has to grow by since the last snapshot to trigger a new snapshot. Future work should change this check to use the database size (sum of all tables). Additionally, snapshots will be triggered on block rotation (see [block rotation section as to why](#block-rotation).

### Recovery

When recovering a database, there will be both a `wal/` and a `snapshots/` folder. The snapshot file names will be encoded with the transaction the snapshot occurred at, similar to how WAL file names contain the first index of that file. Therefore, recovery will conceptually reverse iterate over the lexicographically-sorted snapshot files and load the latest (in tx order) possible snapshot. On successful load, the WAL will be replayed from this snapshot transaction, which has a 1:1 relationship to a WAL index.

### Truncation

The safest time to delete old snapshots is when a snapshot at txn `n` has been successfully loaded. This is implies that all snapshots up to txn `n-1` can be deleted. We can be more aggressive with our truncation, but leave that to future implementations.

With respect to WAL truncation, the same can be done. Once a snapshot at txn n has been successfully loaded, the WAL can be truncated up to txn `n`.

### Block Rotation

Snapshot recovery in the presence of block rotations can be subtle. The situation we want to avoid is recovering from a snapshot that has data from a table block that has been rotated (i.e. persisted). If the aforementioned [snapshot intervals](#interval) and [recovery](#recovery) algorithms are followed without triggering a snapshot on block rotation, it is possible to load a snapshot at txn `n` when a block rotation happened at txn `n+k`. When serving queries, FrostDB could return duplicate data for writes with txn ids `<= n` since the same row could be both in-memory (originating from the snapshot) and in block storage (previously persisted).  Triggering snapshots on block rotation will not include the rotated block. When the snapshot is loaded during recovery, persisted data will not be reflected in the snapshot.

However, there might be cases where the latest snapshot loaded during recovery is at a txn less than the block rotation. For example, the vm might be preempted after persisting a block, but before a snapshot can be written to disk. To handle these cases, the WAL replay code will have take into account that the replay is happening against a non-empty database loaded from a snapshot at the same txn the WAL replay started at. Given a table block persistence WAL entry indicates successful persistence of the table block, it is safe to ignore the snapshot data for that table, so the active table block index will be reset, deleting all data in the in-memory table and inserting only the WAL records starting from the snapshot txn.

## Future work

### Database size trigger

For a first version, the easiest snapshot trigger was to look at the block size on insert, similar to how block rotation is triggered. Since snapshots are done at the database granularity, it would be more correct to trigger a snapshot based on the sum of all the table sizes.

### Snapshot compression

Currently, snapshot data regions are not compressed. It's unclear whether file sizes will be reduced much given existing parquet data compression, but it is something to be explored if file sizes become too large.

## Alternatives

### Improving the WAL

Another approach to solving the [problems](#problem) outlined at the start of this document is to:
- Persist blocks based on time as well as byte size. This should solve the WAL size problem by increasing the minimum persisted transaction across all tables.
- Improve insert performance to reduce WAL replay time.

Time-based block persistence was discarded given that snapshots solved this problem as well as the performance problem while not introducing more complexity to block persistence.

Improving insert performance is something we should do regardless of WAL replay times. However, WAL replay times are fundamentally a function of the number of inserts performed against the database, while snapshot load times are a function of the number of parts independent of the number of inserts and avoid having to perform index searches on each insert. This is something that we can't get around with inserts given that we need to support inserting arbitrary data. Thus, snapshots still seemed like the better solution.