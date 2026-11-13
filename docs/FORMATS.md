# On-disk formats

These layouts describe version 1. Multibyte fixed-width integers are
little-endian. A `uvarint` uses Go's unsigned base-128 varint encoding.
Byte counts exclude their own length fields unless stated otherwise. Keys
and values are arbitrary byte strings. CRC32C uses the Castagnoli polynomial.

## Directory layout

| Name | Contents |
| --- | --- |
| `LOCK` | Advisory lock file. The inode is kept in place across opens. |
| `CURRENT` | Canonical MANIFEST basename in ASCII followed by one LF. |
| `MANIFEST-000001` | Version edits, replay WAL membership, and allocation state. |
| `000002.wal` | Numbered write-ahead log. |
| `000003.sst` | Numbered immutable table. |
| `.destination.number.tmp` | Temporary sibling used during file installation. |

Numbers use at least six decimal digits with zero padding. The number
space is shared across numbered files. File installation syncs contents
and the parent directory before an edit can publish the installed name.

## WAL

### Physical fragments

A WAL has no file header. It is divided into 32 KiB blocks. A fragment
header begins at the current write offset. Fragments never cross blocks.

| Offset | Bytes | Field |
| ---: | ---: | --- |
| 0 | 4 | CRC32C over the one-byte fragment type followed by payload bytes |
| 4 | 2 | Payload length |
| 6 | 1 | Fragment type |
| 7 | length | Payload |

| Type | Meaning |
| ---: | --- |
| 1 | FULL, the whole logical record |
| 2 | FIRST, the initial part of a split record |
| 3 | MIDDLE, an intermediate part of a split record |
| 4 | LAST, the final part of a split record |

FIRST and MIDDLE fill the remainder of their physical blocks. MIDDLE and
LAST start at a block boundary. When the next fragment cannot fit a
seven-byte header and one payload byte, the writer fills the block
remainder with zeroes. The final block may end immediately after the last
complete fragment.

### Logical write batch

| Offset or order | Type | Field |
| --- | --- | --- |
| 0 | `u8` | Batch format version, currently `1` |
| 1 | `u64` | First sequence number, nonzero |
| 9 | `u32` | Number of operations, between 1 and 262,144 |
| 13 onward | Repeated | One operation per count |

Each operation has `kind:u8`, `key_length:uvarint`, and that many key
bytes. Kind `0` is a tombstone and has no further fields. Kind `1` is a
put and continues with `expiry:i64`, `value_length:uvarint`, and that many
value bytes. Expiry is Unix nanoseconds, with `0` reserved for no expiry.

The sequence of operation `i` is `first_sequence + i` using zero-based
indexing. The reconstructed batch is at most 64 MiB. Group commit appends
multiple framed batches in one write. Each batch remains a separately
checked logical record.

## SSTable

An SSTable consists of adjacent data blocks, one index block, one Bloom
filter block, and a fixed-size footer.

### Block trailer

Every data, index, and Bloom block ends with `codec_id:u8` and
`checksum:u32`. The checksum covers the stored payload followed by the
codec byte. Codec `0` is raw. Codec `1` is raw DEFLATE. Only data blocks can
be compressed, and the builder uses compression when it saves space.
The reader bounds decoded block payloads at 80 MiB.

A block handle is `offset:u64` followed by `length:u64`. The offset is
relative to the start of the file. The length includes the five-byte
trailer.

### Internal key and data block

An encoded internal key consists of the user key, `sequence:u64`, then
`kind:u8`. The key length comes from the enclosing field. Readers compare
the decoded user key ascending, sequence descending, and kind ascending.
Bytewise comparison of the encoded key would give the wrong sequence
order.

A data block begins with repeated entries.

| Order | Type | Field |
| ---: | --- | --- |
| 1 | `uvarint` | Bytes shared with the previous encoded internal key |
| 2 | `uvarint` | Bytes in the unshared suffix |
| 3 | `uvarint` | Bytes in the entry value |
| 4 | byte string | Unshared key suffix |
| 5 | byte string | Entry value |

A tombstone has a zero-length value. A put value starts with the same
`expiry:i64` field used by the WAL, followed by the raw application value.

The entry area is followed by an array of `u32` restart offsets and a
`u32` restart count. Offsets are relative to the data block payload, start
at zero, and increase strictly. A restart entry stores the full key with
shared length zero. The default interval is 16 entries. The default
data-block target is 4 KiB.

### Index block

The payload starts with `block_count:u32`. For each data block it stores
`key_length:uvarint`, the last encoded internal key in that block, and its
16-byte block handle. The keys are ordered by the internal comparator.
A seek binary-searches these upper bounds.

### Bloom filter block

| Offset | Type | Field |
| ---: | --- | --- |
| 0 | `u8` | Hash format version, currently `1` |
| 1 | `u8` | Probe count, between 1 and 30 |
| 2 | `u64` | Number of addressable bits |
| 10 | byte array | Packed filter bits |

The bit count is at least 64 and divisible by eight. Bit `i` is stored as
`1 << (i % 8)` in byte `i / 8`. Version 1 starts at FNV-1a-64 of the user
key modulo the bit count. It advances by CRC64-ECMA of the user key with
the low bit set, again modulo the bit count. The filter includes user keys
that only have tombstones.

### Footer

| Offset | Bytes | Field |
| ---: | ---: | --- |
| 0 | 16 | Index block handle |
| 16 | 16 | Bloom filter block handle |
| 32 | 8 | Data block count |
| 40 | 8 | Internal entry count |
| 48 | 4 | Format version, currently `1` |
| 52 | 4 | CRC32C over bytes 0–51 followed by bytes 56–63 |
| 56 | 8 | ASCII magic `EMBERSST` |

## MANIFEST

Each record has `payload_length:u32`, `checksum:u32`, then the payload.
The checksum covers the encoded length followed by the payload. The
payload is at most 128 MiB. It starts with `version:u8` (currently `1`) and
`field_count:uvarint`. Each field consists of `tag:u8`,
`field_length:uvarint`, and the field bytes.

| Tag | Operation | Field bytes in order |
| ---: | --- | --- |
| 1 | Add table | level `uvarint`, file number `u64`, file size `u64`, minimum sequence `u64`, maximum sequence `u64`, length-prefixed minimum key, length-prefixed maximum key |
| 2 | Delete table | level `uvarint`, file number `u64` |
| 3 | Add replay WAL | file number `u64` |
| 4 | Remove replay WAL | file number `u64` |
| 5 | Set active WAL | file number `u64` |
| 6 | Set next file number | next file number `u64` |
| 7 | Set last sequence | sequence `u64` |

The key lengths in tag 1 are `uvarint` values. The key range is inclusive.
Tags 5–7 can each appear at most once in a record. An absent scalar leaves
its state unchanged. A new database starts at sequence zero. One complete
record applies as one version edit.

`CURRENT` contains text such as `MANIFEST-000001\n`, where `\n` denotes a
single LF byte. It uses no checksum. The reader checks that its contents
match the canonical filename exactly.
