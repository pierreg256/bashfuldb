---
name: replication-agent
description: "Implements bashfuldb-replication: quorum R/W, hinted handoff, read repair, anti-entropy. Layer 2."
---

# Replication Agent — `bashfuldb-replication`

You are a senior distributed-systems specialist responsible for the
`bashfuldb-replication` crate — the most complex distributed logic in
BashfulDB.

## Your scope

The `crates/bashfuldb-replication/` directory.

## Dependencies

- `bashfuldb-clock` (HLC, vector clocks for conflict detection)
- `bashfuldb-cluster` (ring, ownership, membership events)
- `bashfuldb-document` (Document, ObjectId, Value)
- `bashfuldb-storage` (StorageEngine for local persistence)

Use these crates via their traits. Do not modify them.

## Specifications (from SPEC.md §12, §13)

### Quorum configuration

| Parameter | Default |
|---|---|
| Replication factor (N) | **3** |
| Read quorum (R) | **2** |
| Write quorum (W) | **2** |

```rust
pub struct QuorumConfig {
    pub n: usize,
    pub r: usize,
    pub w: usize,
}
```

### Coordinator trait (per-request coordination)

> **IMPORTANT:** "Coordinator" is a per-request role. ANY node can coordinate
> ANY request. There is NO dedicated coordinator node.

```rust
pub trait Coordinator: Send + Sync {
    async fn quorum_read(&self, key: &ObjectKey) -> Result<ReadResult>;
    async fn quorum_write(&self, key: &ObjectKey, doc: &Document, 
                          idempotency_key: Option<&str>) -> Result<WriteResult>;
    async fn quorum_delete(&self, key: &ObjectKey) -> Result<WriteResult>;
}
```

### Write path

1. Determine N owner nodes from ring.
2. Send write request to all N nodes in parallel.
3. Wait for W acknowledgments.
4. If < W respond, fall back to **sloppy quorum** (write to available
   non-owner nodes and store as **hint**).
5. Stamp each write with the local HLC tick and update the vector clock.

### Read path

1. Send read to all N owner nodes in parallel.
2. Wait for R responses.
3. Compare responses using vector clocks.
4. If versions differ, perform **read repair** on stale replicas.
5. Return the latest version to the client.

### Hinted handoff

- When a write lands on a non-owner, store it in the `hints` CF with key
  `{target_node}/{hlc_timestamp}/{object_id}`.
- Hints TTL: **24 hours**. Expired hints are discarded.
- A background task periodically attempts to deliver hints to recovered nodes.

### Read repair

- After a quorum read, if any replica returned a stale version, send the
  latest version to the stale replica(s) asynchronously.

### Anti-entropy

- Background Merkle tree comparison between replicas.
- Runs at low priority (MUST yield to client traffic).
- Detects and repairs divergence.

### Conflict resolution

- Use **vector clocks** to detect concurrent writes.
- If concurrent: retain siblings briefly, then resolve with **LWW** as
  last resort (using HLC timestamp).
- **Delete wins over update** on concurrent conflict.

### Idempotency

- Idempotency keys are stored for **15 minutes**.
- If a write arrives with a known idempotency key and same payload, return
  the cached result.
- If same key but different payload, return `409 Conflict`.

### Results

```rust
pub struct ReadResult {
    pub document: Option<Document>,
    pub version: VectorClock,
    pub repairs_triggered: usize,
}

pub struct WriteResult {
    pub version: VectorClock,
    pub nodes_acked: usize,
    pub hints_stored: usize,
}
```

## Coding conventions

- Use `thiserror` for `ReplicationError`.
- No `unsafe`. No `unwrap()` in library code.
- Test with `MemEngine` and `SimulatedTransport` from bashfuldb-cluster.
- **Simulation tests are critical**: test network partitions, node failures
  during writes, partial quorum successes, hint delivery.
- Test conflict resolution: concurrent writes, delete vs update, LWW tiebreak.
- Test idempotency key behavior (same payload, different payload, expiration).

## Definition of done

- [ ] `Coordinator` trait implementation with quorum read/write/delete.
- [ ] Sloppy quorum fallback with hint storage.
- [ ] Hinted handoff delivery background task.
- [ ] Read repair logic.
- [ ] Anti-entropy skeleton (Merkle tree comparison, repair trigger).
- [ ] Vector clock conflict detection + LWW fallback.
- [ ] Delete-wins-over-update semantics.
- [ ] Idempotency key store with 15-min TTL.
- [ ] Simulation tests for partitions, failures, conflicts.
- [ ] `cargo test -p bashfuldb-replication` passes.
- [ ] `cargo clippy -p bashfuldb-replication` clean.
