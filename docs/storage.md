# Storage

A partition lives in `{log.dirs}/{topic}-{partition}/`.

```
00000000000000000000.log
00000000000000000000.index
high-watermark
```

The 20-digit prefix is the base offset of that segment, in decimal, so the names sort in offset order. A new segment starts when the active file would otherwise grow past `log.segment.bytes`. A single record larger than the limit still occupies its own segment.

## Record

```
int32  length         bytes after this field
int8   magic          1
int32  crc            CRC32 of the body below
int64  offset
int64  timestamp
int32  keyLength      -1 when there is no key
bytes  key
int32  valueLength    -1 when there is no value
bytes  value
```

The CRC covers the offset, timestamp, key, and value, including their length fields. It does not cover `length`, `magic`, or itself. A short or mismatched tail is a torn write. Recovery truncates the file at the first bad record and rewrites the index.

## Index

The index is sparse. The first record of a segment is always indexed. Another entry is written when the byte gap since the previous entry is at least `log.index.interval.bytes`. Each entry is:

```
int32  relativeOffset    offset - baseOffset
int32  position          byte position of the record in the log
```

A read binary-searches the index for the greatest entry at or before the target offset, then scans forward. Scanning is bounded by the index interval.

## Watermark

`high-watermark` is a single big-endian int64: the first offset that consumers are not allowed to read. It is fsynced when it moves forward. After a crash the log may contain records at or past the watermark. Those records stay on disk and become visible only when the watermark advances again, or when this replica is promoted to leader.

## Concurrency

Appends and truncation take a write lock. Reads take a read lock and use absolute file positions, so two readers do not share a cursor. Waiters for a new log end or a new watermark block on a condition and do not hold the log lock, which is what lets a follower fetch complete a produce that is waiting on the watermark.
