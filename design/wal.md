# Write-Ahead Log

Authored: Jul 18, 2022
Author: Frederic Branczyk (@brancz)

## Abstract

A minimally functional, per database, single-threaded replay write-ahead log (WAL) based on tidwall/wal, where FrostDB-specific record types are appended as serialized protobuf.

## Problem

Both for crash recovery as well as in-memory state reproducibility (for example for benchmarking the in-memory portion), FrostDB needs a write-ahead log.

## Proposal

As this proposal’s need primarily stems from the need to reproduce the in-memory state of a FrostDB instance, this proposal focuses on the correctness of record and replay but does not lay an emphasis on the performance of replaying data, as the intended use is in offline environments, not for production crash recovery, though since write-ahead logs are often used for that purpose it is engineered to keep that use in mind for the future (for example it could be extended to concurrently replay).

FrostDB is capable of managing multiple logical databases within one instance, each of these databases has its own WAL. The WAL is organized into records to replay the history of actions against or initiated by the database since FrostDB is write-only, the WAL records the data of writes as well as their transaction ID to reconstruct the timeline of its history.

### Record

Each record is indexed by its transaction ID. The types of records to accurately reconstruct and prevent duplication of actions are:

* New Table Block creation records contain the schema of the table block as well as its ID.
* Successful persistence of a table block. There will ultimately always be a race between the write succeeding and logging the WAL entry. Therefore persisting a table block must be idempotent. This record is therefore mostly an optimization to attempt to prevent large amounts of network traffic when it’s unnecessary.
* Write-Transactions: Includes the inserted payload as well as the table that was written to. Write transactions to a table that occur after a new table block creation, are guaranteed to have been written to the new block.

### Truncation

When a table block is successfully persisted, the durability of all write transactions it included is ensured, so the WAL entries for those transactions are no longer necessary, therefore the WAL can be truncated to the point where the table block ended. However, because transactions are per database, the WAL is also per database, meaning it can span multiple tables. Therefore, WAL maintenance keeps track of blocks persisted and only truncates the WAL to the minimum transaction still necessary to reconstruct the active table block with the lowest starting transaction ID.

### Replay

When replaying the WAL, in reality, it is replayed twice, first, it is replayed to extract which table blocks were already successfully persisted, in order to infer on the second replay whether a write-transaction can be ignored since its durability was already ensured through the block persistence, otherwise causing unnecessary inserts in the database at replay time.

### Scenarios
Since there is a multi-table situation, a set of transactions belonging to a single table can be truncated at any point in time.

The following timeline illustrates a write transaction at multiple points in time. Note each of the the moments in time t1…t12 represent a transaction ID, therefore those that have additional labels do not represent a write transaction but they obtain their own transaction and log record. 

![Timeline of Transactions in FrostDB](https://docs.google.com/drawings/d/1Y5QbjeuKWUebJP9L9AvvAVWhC-GVNUYCXVKxueCzYV8/export/svg)

1) If truncation happened up until t0 (as it’s represented in the above graphic) the first replay would identify that table block with ID1 persisted successfully, so all its writes can be ignored. At second replay, the write that occurred at t1 will be ignored, as no table block creation precedes it. Then transactions from t2 to t10 are ignored as we already know the table block with ID1 successfully persisted. Only the table block with the ID2 was incomplete, therefore it is replayed, and the two transactions t11 and t12 are replayed into that table block.
2) If truncation happened up until including t1 to t9, the first replay would not find any persisted block that was also started in the WAL, therefore all of the writes until t11 are ignored.

## Future work

### Concurrent replay

Since the WAL is already replayed twice, the first iteration could be used to determine the table blocks a write transaction wrote into and perform these inserts concurrently, not only allowing inserts into a block to be concurrent but even allowing all blocks to be concurrently replayed at once.

### FlatBuffers instead of protobuf

FlatBuffers are probably the better choice of technology for a WAL since it does not have any deserialization cost, which will have a large impact on the performance of replaying the write-ahead log. Since the initial purpose is for offline replay, this proposal rather opted for tooling already well established and known at Polar Signals rather than introducing a new tool for serializing/deserializing.

## Alternatives

### Record requests

Record the ingest requests made against a FrostDB (more specifically the application that embeds FrostDB eg. a [Parca](https://github.com/parca-dev/parca) server. Ultimately this seemed to entail the same amount of complexity without the possibility of later on using it for crash recovery purposes if we wanted to.

