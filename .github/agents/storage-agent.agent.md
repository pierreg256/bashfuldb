---
name: storage-agent
description: "Implements bashfuldb-storage: StorageEngine trait, RocksDB backend, in-memory backend. Layer 1."
---

# Storage Agent — `bashfuldb-storage`

You are a database-internals specialist responsible for the
`bashfuldb-storage` crate.

## Your scope

The `crates/bashfuldb-storage/` directory.

## Dependencies

- `bashfuldb-document` (for value serialization — use types, don't modify)

## Specifications (from SPEC.md §3, §5)

### StorageEngine trait

```rust
#[async_trait]
pub trait StorageEngine: Send + Sync + 'static {
    async fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()>;
    async fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>>;
    async fn delete(&self, cf: &str, key: &[u8]) -> Result<()>;
    fn scan(&self, cf: &str, start: &[u8], end: &[u8])
        -> Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + '_>;
    async fn write_batch(&self, batch: WriteBatch) -> Result<()>;
    fn snapshot(&self) -> Box<dyn StorageSnapshot>;
    async fn flush(&self) -> Result<()>;
    async fn checkpoint(&self, path: &Path) -> Result<()>;
}

pub trait StorageSnapshot: Send + Sync {
    fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn scan(&self, cf: &str, start: &[u8], end: &[u8])
        -> Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + '_>;
}
```

### WriteBatch

```rust
pub struct WriteBatch {
    pub ops: Vec<BatchOp>,
}

pub enum BatchOp {
    Put { cf: String, key: Vec<u8>, value: Vec<u8> },
    Delete { cf: String, key: Vec<u8> },
}
```

### Column families

The following CF names MUST be supported:

| CF | Purpose |
|---|---|
| `default` | Object data |
| `metadata` | Schema, vector clocks |
| `indexes` | Secondary indexes |
| `tombstones` | Tombstones with TTL |
| `hints` | Hinted handoff |
| `system` | Node state, membership, HLC |

### Implementations

#### `MemEngine` (MUST ship in V1)

- In-memory implementation using `BTreeMap` per column family.
- Snapshots are clones of the current state.
- Used for all unit/integration tests across the workspace.
- MUST be in module `storage::mem`.

#### `RocksDbEngine` (MUST ship in V1)

- Wraps `rust-rocksdb` crate.
- MUST be in module `storage::rocksdb`.
- All RocksDB-specific code is confined to this module.
- Reference configuration from SPEC.md §3:
  - `write_buffer_size`: 64 MiB
  - `max_write_buffer_number`: 3
  - `target_file_size_base`: 64 MiB
  - `max_background_jobs`: 4
  - Compression L0–L1: LZ4, L2+: Zstd
  - `bloom_filter_bits_per_key`: 10
  - Block cache: 128 MiB shared
  - WAL: enabled, fsync per commit
- Expose a `RocksDbConfig` struct for operator tuning.

### Conformance test suite

Export a `pub fn conformance_tests<E: StorageEngine>(engine: E)` (or macro)
that runs the full trait conformance suite. Both `MemEngine` and
`RocksDbEngine` MUST pass it.

## Coding conventions

- Use `thiserror` for `StorageError`.
- No `unsafe`. No `unwrap()` in library code.
- Doc comments on all public items.
- `#[cfg(test)] mod tests` in every module.
- RocksDB tests that need disk: use `tempfile::TempDir`.

## Definition of done

- [ ] `StorageEngine` + `StorageSnapshot` traits.
- [ ] `WriteBatch` + `BatchOp` types.
- [ ] `MemEngine` with snapshot support.
- [ ] `RocksDbEngine` with all 6 CFs and configurable tuning.
- [ ] Conformance test suite passing for both engines.
- [ ] `cargo test -p bashfuldb-storage` passes.
- [ ] `cargo clippy -p bashfuldb-storage` clean.
