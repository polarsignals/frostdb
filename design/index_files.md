# On-Disk persistent index files

Authored: Jan 22, 2024
Author: Thor Hansen (@thorfour)

## Abstract

FrostDB currently uses temporary index files to store Parquet data. These are lost on shutdown, and need to be recreated from snapshots and WAL.
Furthermore snapshots are less effecient than necessary because snapshotting copies Parquet data that is already on disk, and writes it to a different file (snapshot file)
when we could just reference the index file instead.

## Proposal

Instead FrostDB should use persistent index files that don't requrie snapshots to copy data but instead hard link the files in the snapshot directory for recovery.

Index files have the format of:
|                    |
| -----------------  |
|   `<ParquetFile>`  |
|   `<uint64 size>`  |
|   `<ParquetFile>`  |
|   `<uint64 size>`  |
|   `<ParquetFile>`  |
|   `<uint64 size>`  |

Where Parquet files are appending togehter with a uint64 that represents the number of bytes in the previous ParquetFile.
Each Parquet file represents a single Part in the LSM index. The Part's transaction id is stored in the Parquet file with a metadata key of 'compaction_tx'

Index files will be stored in a directory with the prefix of:
databases/<database>/index/<table>/<block>/<index_level>/

With the filename format of 000000000001.parquet with a 20 byte representation of the ordering of the index file.

### Index File Lifecycle

There are 4 events that influence the lifecycle of an index file.
- Writes
- Snapshots
- Compaction
- Block Rotation

Writes: 
    These are compaction from a lower level that write into the index file. These writes shall write into the currently active index file in the level,
    appending the newly compacted Parquet file into the index file with the uint64 at the end of the number of bytes written.
    This file shall be fsync'd after a completed write.

Snapshots: 
    When a snapshot occurs the index files in every level of the index shall be rotated. This means a new index file is created the old one is fsync'd
    and all future writes to the level are writen into that new file leaving the previous file immutable. The old files are then hard linked into the snapshot directory,
    such that when the index deletes those files they can still be recovered from the snapshot.

Compaction: 
    This is when a level is compacted into a new Parquet file and written to the next level. All the files in that compacted level shall be deleted, as they are now
    covered by the latest write into the next level.

Rotation:
    When an entire table block is rotated out, after rotation is completed the block directory that holds all levels of index files can be deleted.


### Replay

When a table block is created it shall walk through the table blocks directory. Upon discovering index files it will open and read all parts of that file.
Returning a list of parts that were recovered with the block was created. The Table shall than replay those recovered parts into the LSM.
