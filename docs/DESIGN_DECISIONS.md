# Design decisions

| Decision | Alternatives considered | Tradeoff in Ember |
| --- | --- | --- |
| Skip-list memtables | B-tree, hash map with sort on flush | Ordered iteration and insertion share one structure. Random levels keep the implementation small, with more pointer chasing than a packed tree. |
| One writer | Sharded writers, concurrent memtable mutation | One sequence allocator and WAL owner simplify publication and group commit. The writer can become the throughput ceiling. |
| Group commit | One sync per request, asynchronous-only logging | Nearby writers share one append and sync while retaining separately replayable batches. A large group occupies the writer longer. |
| Three sync policies | Always sync, periodic only | The caller can trade acknowledgment durability for latency. Periodic and never can lose writes acknowledged before a successful sync. |
| Block-framed WAL | Simple length prefix, WAL per batch | Physical fragments allow a large logical batch and give recovery fixed block boundaries. Fragment transitions and padding add parser states. |
| Append-only MANIFEST and CURRENT | Scan filenames, replace a full metadata image on each change | One edit commits a file-set change atomically and leaves a history for inspection. Startup work grows with that history. |
| Sync before publishing a file reference | Rename alone, publish then repair | The recovery record only references names that have completed installation. File and directory syncs add metadata I/O. |
| Prefix-compressed data blocks | Store full keys, compress the entire file | Restart points trade a little space for bounded seek work. Short or unshared keys may see limited savings. |
| Per-table Bloom filter | Per-block filters, exact index lookups alone | A small probabilistic filter can skip an entire table. A hit still requires lookup, and the filter consumes memory per table. |
| Sharded LRU | One global LRU, clock cache | Sharding reduces lock contention with straightforward eviction. Fixed shard budgets can leave space in a cold shard while another evicts. |
| Immutable versions with pins | Hold the state lock across reads, copy all visible data | Readers and iterators can use stable file metadata after publication. Slow readers delay reclamation. |
| Sequence and time in read views | Latest reads only, read expiry against current time | Snapshots remain stable across overwrites and expiry. Compaction uses the oldest registered sequence and read time when deciding what to retain. |
| Leveled compaction | Size-tiered compaction, no compaction | Non-overlapping deeper levels limit point-read fanout. Rewriting overlapping ranges costs write bandwidth. |
| One maintenance worker and backpressure | Unbounded flush backlog, independent compaction pools | A simple scheduler controls memory and L0 growth. Flush and compaction can block each other's progress. |
| RESP2 string subset | Custom protocol, complete Redis compatibility | Existing clients can exercise the storage path. Ember owns request bounds and exposes only documented commands and transaction behavior. |
| Offline verification | Automatic repair in the inspector, lock-free verification | The inspector sees one stable live set and reports damage without truncating files. Verification requires downtime. |
