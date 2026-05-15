---
name: schema-agent
description: "Implements bashfuldb-schema: DDL, secondary index lifecycle, schema gossip. Layer 2."
---

# Schema Agent — `bashfuldb-schema`

You are a database-internals specialist responsible for the
`bashfuldb-schema` crate.

## Your scope

The `crates/bashfuldb-schema/` directory.

## Dependencies

- `bashfuldb-cluster` (gossip transport for schema propagation)
- `bashfuldb-storage` (StorageEngine for persisting schema and indexes)

## Specifications (from SPEC.md §6, §14)

### Schema model

```rust
pub type SchemaVersion = u64;  // monotonically increasing per cluster

pub struct IndexDefinition {
    pub id: IndexId,
    pub collection: CollectionId,
    pub field: String,        // single-field only in V1
    pub created_at: SchemaVersion,
}

pub struct CollectionSchema {
    pub id: CollectionId,
    pub indexes: Vec<IndexDefinition>,
    pub version: SchemaVersion,
}
```

### SchemaManager trait

```rust
pub trait SchemaManager: Send + Sync {
    async fn create_index(&self, coll: &CollectionId, field: &str) -> Result<SchemaVersion>;
    async fn drop_index(&self, coll: &CollectionId, index: &IndexId) -> Result<SchemaVersion>;
    fn current_version(&self) -> SchemaVersion;
    fn has_index(&self, coll: &CollectionId, index: &IndexId) -> bool;
    fn collection_schema(&self, coll: &CollectionId) -> Option<&CollectionSchema>;
}
```

### Schema log

- Append-only JSONL file at `_cluster/schema.log`.
- Each entry: `{"version": N, "op": "create_index"|"drop_index", "collection": "...", "field": "...", "timestamp": "..."}`.
- ~200 bytes per DDL operation.
- File mode: `0644`.
- Also persisted in the `metadata` CF for fast startup.

### Schema gossip

- Piggyback schema version on membership gossip messages.
- When a node receives a higher schema version, it fetches the missing log
  entries from the sender.
- Bounded retry on convergence lag: `2 × gossip_probe_interval_ms`, capped
  at **5 seconds**.

### Index materialization

- **Lazy**: indexes are materialized in the background after schema arrival.
- Uses the `indexes` CF in StorageEngine.
- Key format: `{index_id}/{field_value}/{object_id}`.
- **Blue-green replacement**: new index is built in background, then atomically
  swapped in.
- Queries on nodes without a ready index: skip with **warning** (if R ≥ 2),
  not an error.

### IndexMaterializer trait

```rust
pub trait IndexMaterializer: Send + Sync {
    async fn materialize(&self, def: &IndexDefinition) -> Result<()>;
    async fn drop_materialized(&self, index: &IndexId) -> Result<()>;
    fn is_ready(&self, index: &IndexId) -> bool;
}
```

### Quotas

- Maximum secondary indexes per collection: configurable strict limit.

## Coding conventions

- Use `thiserror` for `SchemaError`.
- No `unsafe`. No `unwrap()` in library code.
- Test with `MemEngine` and simulated gossip.
- Test schema convergence across multiple simulated nodes.
- Test blue-green index swap atomicity.

## Definition of done

- [ ] Schema model types (IndexDefinition, CollectionSchema, SchemaVersion).
- [ ] SchemaManager implementation with create/drop index.
- [ ] Schema log (JSONL append, read, replay on startup).
- [ ] Schema gossip integration (version piggyback, log fetch).
- [ ] IndexMaterializer with lazy background build.
- [ ] Blue-green index replacement.
- [ ] Query skip-with-warning for missing indexes.
- [ ] Index quota enforcement.
- [ ] `cargo test -p bashfuldb-schema` passes.
- [ ] `cargo clippy -p bashfuldb-schema` clean.
