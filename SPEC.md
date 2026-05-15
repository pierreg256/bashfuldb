# BashfulDB — Consolidated Specifications

> This document synthesizes the specifications from the **GrumpyDB** project
> (embedded storage engine / single-node server) and the **DopeyDB** project
> (distributed layer, HTTP API, security, operations). It serves as the
> normative reference for the BashfulDB project.
>
> **Language convention:** the keywords MUST, MUST NOT, SHOULD, SHOULD NOT, and
> MAY follow RFC 2119.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Data Model](#2-data-model)
3. [Storage Engine](#3-storage-engine)
4. [WAL & Crash Recovery](#4-wal--crash-recovery)
5. [Cache & Memory](#5-cache--memory)
6. [Secondary Indexes](#6-secondary-indexes)
7. [MVCC & Snapshot Reads](#7-mvcc--snapshot-reads)
8. [Wire Protocol (TCP/TLS)](#8-wire-protocol-tcptls)
9. [Authentication & RBAC](#9-authentication--rbac)
10. [Public HTTP API](#10-public-http-api)
11. [Cluster & Topology](#11-cluster--topology)
12. [Replication & Quorum](#12-replication--quorum)
13. [Conflict Resolution](#13-conflict-resolution)
14. [Gossip & Schema Propagation](#14-gossip--schema-propagation)
15. [Security & Transport](#15-security--transport)
16. [Operations & Observability](#16-operations--observability)
17. [Backup & Restore](#17-backup--restore)
18. [Client SDK & Drivers](#18-client-sdk--drivers)
19. [Quotas & Guardrails](#19-quotas--guardrails)
20. [V1 Constraints & Deferred Items](#20-v1-constraints--deferred-items)
21. [Reference Numeric Parameters](#21-reference-numeric-parameters)

---

## 1. Overview

BashfulDB is a distributed document database written in Rust. It combines:

- **An embedded storage engine** (derived from GrumpyDB): RocksDB-backed,
  with HLC/vector clocks, MVCC snapshots, and secondary indexes.
- **A distributed layer** (derived from DopeyDB): ring of nodes with vnodes,
  configurable quorum, gossip, replication, HTTP API, and hardened security.

### Core Principles

| Principle | Source | Detail |
|---|---|---|
| Availability first | Dopey | Under partition, favor availability (AP) |
| No coordinator node | Dopey | Every node exposes the same API; LB optional |
| Non-negotiable security | Dopey | HTTPS, inter-node mTLS, strict RBAC, audit |
| Pluggable storage engine | Grumpy + BashfulDB | RocksDB in V1, abstracted behind a trait |
| Native multi-tenancy | Grumpy + Dopey | Tenant → Database → Collection → Object isolation |
| Strict compatibility | Dopey | Breaking changes = major version; no API removal without deprecation |

---

## 2. Data Model

### Hierarchy

```
Tenant → Database → Collection → Object
```

- Names MUST match the pattern `[a-z0-9_]{1,64}`.
- The names `_default` and `_system` are reserved.

### Object (document)

- An object is a strict JSON document (no BSON, no extended types in V1).
- Updates are **full replacements** in V1.

### `Value` Types (internal binary codec)

| Tag | Type | Limits |
|-----|------|--------|
| `0x00` | Null | — |
| `0x01` | Boolean | — |
| `0x02` | Integer (i64) | — |
| `0x03` | Float (f64) | — |
| `0x04` | String | ≤ 16 MiB |
| `0x05` | Blob | ≤ 16 MiB |
| `0x06` | Array | ≤ 1,000,000 elements |
| `0x07` | Object | ≤ 100,000 keys, deterministic `BTreeMap` |
| `0x08` | (reserved) | — |

- Maximum nesting depth: **64 levels**.

### Keys & Identifiers

- Primary key: UUID.
- Variable keys: up to **256 bytes**.
- No strong entropy requirement on client-side keys.

### Deletion

- **Hard delete** only from the client perspective (no public soft delete).
- Tombstones are retained internally for **24 h** for convergence.
- Semantics: **delete wins over update** on concurrent conflict.

---

## 3. Storage Engine

### Architecture Principle: Storage Engine Abstraction

Storage MUST be isolated behind a **`StorageEngine` trait** to allow replacing
the underlying engine without impacting upper layers (document model,
replication, API).

```rust
/// Minimal contract that any storage engine must implement.
#[async_trait]
pub trait StorageEngine: Send + Sync + 'static {
    /// Writes a key/value pair into the given column family.
    async fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()>;

    /// Reads a value by key.
    async fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Deletes a key.
    async fn delete(&self, cf: &str, key: &[u8]) -> Result<()>;

    /// Iterates over a key range (inclusive/exclusive).
    fn scan(&self, cf: &str, start: &[u8], end: &[u8])
        -> Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + '_>;

    /// Writes an atomic batch of mutations.
    async fn write_batch(&self, batch: WriteBatch) -> Result<()>;

    /// Creates a consistent snapshot for reads.
    fn snapshot(&self) -> Box<dyn StorageSnapshot>;

    /// Forces persistence to disk.
    async fn flush(&self) -> Result<()>;

    /// Creates a checkpoint/backup in the given directory.
    async fn checkpoint(&self, path: &Path) -> Result<()>;
}
```

Any direct dependency on RocksDB MUST be confined to the `storage::rocksdb`
module and MUST NOT leak into upper layers.

### V1 Implementation: RocksDB

The default storage engine in V1 is **RocksDB** (via the `rust-rocksdb` crate).

#### Rationale

| Criterion | Detail |
|---|---|
| Built-in WAL | RocksDB provides its own WAL; no custom WAL needed |
| Native snapshots | Immutable `Snapshot` for consistent reads |
| Column families | Logical separation of data / metadata / indexes |
| Checkpoints | Inexpensive incremental backup creation |
| Compaction | Configurable (leveled, universal, FIFO) |
| Maturity | Battle-tested at scale (Meta, CockroachDB, TiKV…) |

#### Column Families

| CF | Content |
|---|---|
| `default` | Object data (key = `{collection_id}/{object_id}`) |
| `metadata` | Schema, collection configuration, vector clocks |
| `indexes` | Secondary indexes (key = `{index_id}/{field_value}/{object_id}`) |
| `tombstones` | Tombstones with TTL (key = `{collection_id}/{object_id}`) |
| `hints` | Hinted handoff (key = `{target_node}/{timestamp}/{object_id}`) |
| `system` | Node state, membership, persisted HLC |

#### Reference Configuration

| RocksDB Parameter | Default Value |
|---|---|
| `write_buffer_size` | 64 MiB |
| `max_write_buffer_number` | 3 |
| `target_file_size_base` | 64 MiB |
| `max_background_jobs` | 4 |
| `compression` (L0–L1) | LZ4 |
| `compression` (L2+) | Zstd |
| `bloom_filter_bits_per_key` | 10 |
| WAL | Enabled, `fsync` on every commit |
| Block cache | 128 MiB (shared across CFs) |

#### On-Disk Directory Layout

```
{data_dir}/
├── {database_name}/
│   ├── rocksdb/          # SST files, WAL, MANIFEST, etc.
│   └── snapshots/        # Periodic checkpoints
└── _system/
    └── rocksdb/          # Cluster state, membership, HLC
```

### Future Engine Candidates

The `StorageEngine` abstraction SHOULD enable future integration of:

| Engine | Use Case |
|---|---|
| **Sled** | Pure Rust embedded, no C++ dependency |
| **SQLite** | Lightweight embedded, maximum compatibility |
| **Custom page-based engine** | Full control (inspired by the GrumpyDB engine) |
| **In-memory** | Tests, CI, benchmarks |

An **in-memory** engine (`storage::mem`) MUST be provided from V1 for unit
and integration tests.

---

## 4. WAL & Crash Recovery

*Delegated to RocksDB — no custom WAL.*

### Principle

RocksDB manages its own Write-Ahead Log. BashfulDB MUST NOT implement a
separate WAL for application data.

### WAL Configuration

| Parameter | Value |
|---|---|
| `manual_wal_flush` | `false` (automatic flush) |
| Sync mode | `fsync` on every commit (strict durability) |
| WAL TTL | Managed by RocksDB (purged after checkpoint) |
| WAL size limit | Operator-configurable |

### Clock & Versioning

- Every mutation is stamped with an **HLC** (Hybrid Logical Clock).
  - Physical bits: `63..16` (timestamp ms).
  - Logical bits: `15..0` (counter, max **65,535** per ms).
- **Vector clocks** are persisted in the `metadata` CF with a cap of
  **4,096 entries**.
- The current HLC is persisted in the `system` CF to survive restarts.

### Recovery

- On startup, RocksDB automatically replays its WAL.
- BashfulDB then restores the persisted HLC and vector clocks from the
  `system` / `metadata` CFs.
- Undelivered hints (CF `hints`) are retried.

---

## 5. Cache & Memory

*Delegated to RocksDB — no custom buffer pool.*

### RocksDB Block Cache

- RocksDB uses a shared **LRU block cache** across column families.
- Default size: **128 MiB** (operator-configurable).
- Bloom filters and index blocks can be pinned in memory.

### Exposed Tuning Parameters

| Parameter | Default | Configurable |
|---|---|---|
| Block cache size | 128 MiB | Yes |
| Write buffer size | 64 MiB | Yes |
| Max write buffers | 3 | Yes |
| Pin L0 filter/index | `true` | Yes |

### Concurrency

- RocksDB natively supports concurrent reads and writes.
- RocksDB **snapshots** provide consistent views without locking.
- At the application level, concurrency is managed by tokio (async runtime).

---

## 6. Secondary Indexes

### Model

- Secondary indexes are stored in the **`indexes` CF** of RocksDB.
- Index key: `{index_id}/{field_value}/{object_id}` (native lexicographic ordering).
- Single-field, user-created.
- Synchronous with writes (updated in the same `WriteBatch`).

### Lifecycle Management

- **Blue-green replacement** for index modifications.
- Maximum number of indexes per collection: strict quota (configurable).
- **Lazy materialization** after schema propagation via gossip.

### Scans

- Scans on non-indexed fields are **forbidden** for standard clients.
- Range scans use RocksDB iterators with prefix seek.

---

## 7. MVCC & Snapshot Reads

### Model

- Consistent reads use **RocksDB snapshots** (immutable view at a point in
  time).
- Each read creates a snapshot associated with the current HLC.
- The snapshot guarantees a consistent view without locking.

### Object Versioning

- Each object version is stamped with its **HLC** and its **vector clock**.
- Concurrent versions (siblings) are detected by comparing vector clocks.
- GC of old versions is driven by RocksDB compaction and tombstone TTLs.

---

## 8. Wire Protocol (TCP/TLS)

### Characteristics

| Parameter | Value |
|---|---|
| Default port | **6380** |
| Protocol version | `4.0.0` |
| Server banner | `+BASHFULDB 4.0.0` |
| Max line length | **1 MiB** (1,048,576 bytes) |
| Max bulk size | **16 MiB** (16,777,216 bytes) |
| Max connections | **1,024** |

### Transport

- RESP-like line-based protocol.
- Optional TLS 1.3 (rustls).
- Automatic self-signed certificate generation in development mode.

### Error Handling

- Connection is dropped after **> 10 consecutive errors**.

---

## 9. Authentication & RBAC

### Authentication

| Parameter | Value |
|---|---|
| Password hashing | **Argon2id** (default parameters) |
| JWT algorithm | **RS256** (v5+), HS256 internal legacy |
| Access token TTL | **1 h** (3,600 s) |
| Refresh token TTL | **48 h** (172,800 s) |
| Secret key (HS256) | 32 bytes |
| Bootstrap user | `_system__admin` |
| JWKS endpoint | Supported (v5+) |

### RBAC

- Model: Roles → Actions → Resources.
- Scope: Tenant / Database / Collection.
- **No automatic scope inheritance**.
- One user per tenant (except server admin).

---

## 10. Public HTTP API

### Canonical Routes

```
POST   /v1/auth/login
POST   /v1/auth/refresh
POST   /v1/auth/logout

GET    /v1/tenants/{tenant}/databases/{db}/collections/{coll}/objects
POST   /v1/tenants/{tenant}/databases/{db}/collections/{coll}/objects
GET    /v1/tenants/{tenant}/databases/{db}/collections/{coll}/objects/{id}
PUT    /v1/tenants/{tenant}/databases/{db}/collections/{coll}/objects/{id}
DELETE /v1/tenants/{tenant}/databases/{db}/collections/{coll}/objects/{id}

GET    /v1/admin/...
```

### Semantics

| Verb | Behavior |
|---|---|
| `PUT` | Idempotent upsert |
| `DELETE` | Hard delete |
| Conditional write | Via `If-Match` (ETag / version) |
| Idempotency | Idempotency key, retention **5–15 min** (runtime: 15 min) |

### Pagination

- **Cursor-based** by default (no offset).
- Scans on non-indexed fields are **forbidden** for standard clients.

### Errors & Retries

- `409 Conflict` for version conflicts.
- `504 Gateway Timeout` for quorum timeouts.
- Clients SHOULD implement retries with **jitter** and a bounded budget.

---

## 11. Cluster & Topology

### Architecture

- **Ring** with virtual nodes (vnodes).
- Target: **256 vnodes per node**.
- **No dedicated coordinator node**: every node is an API node.
- Optional load balancer in front of the cluster.
- Direct node access is officially supported.

### Node States

| State | Client Traffic | Description |
|---|---|---|
| Joining | No | Being admitted to the cluster |
| Bootstrapping | No | Initial data transfer |
| **Healthy** | **Yes** | Only state serving client traffic |
| Leaving | No | Voluntarily leaving the cluster |
| **Draining** | **Read-only** | Evacuating data |
| Suspect | No | Detected as potentially failed |
| Down | No | Confirmed failed |
| Rebuilding | **Never** | Full reconstruction |

### Failure Detection

- **Phi accrual failure detector**.
- Gossip at **1 Hz**.
- Membership change journaling.

---

## 12. Replication & Quorum

### Default Parameters

| Parameter | Value |
|---|---|
| Replication factor (N) | **3** |
| Read quorum (R) | **2** |
| Write quorum (W) | **2** |

### Convergence Mechanisms

All of the following mechanisms are **mandatory**:

1. **Sloppy quorum**: writes may land on non-owner nodes during failures.
2. **Hinted handoff**: hints are retained for **24 h** then expired.
3. **Read repair**: opportunistic repair during reads.
4. **Anti-entropy**: background synchronization via Merkle trees.

### Acknowledgment

- A write is acknowledged once **W nodes** have confirmed.
- A read returns its result once **R nodes** have responded.

---

## 13. Conflict Resolution

### Clocks

- **Hybrid Logical Clocks (HLC)** for local timestamping.
- **Vector clocks** for concurrency detection.

### Strategy

1. Concurrent versions (siblings) are detected via vector clocks.
2. Siblings are retained briefly to allow resolution.
3. **Last-Write-Wins (LWW)** is the last-resort mechanism only.
4. **Delete wins over update** on concurrent conflict.

### Tombstones

- Retention: **24 h** (for propagation to replicas).
- Configurable `gc_grace_seconds` parameter.
- Tombstones are invisible to clients but present internally.

---

## 14. Gossip & Schema Propagation

### Membership Gossip

- Frequency: **1 Hz**.
- Cassandra-like protocol.

### Schema Gossip

- Monotonically increasing schema version per cluster.
- Schema log: `_cluster/schema.log` (JSONL, append-only).
- ~**200 bytes** per DDL operation.
- File mode: `0644`.

### Index Materialization

- Lazy: indexes are materialized after receiving the schema via gossip.
- In case of lag, queries on a node without the local index:
  - If `R ≥ 2`: the node is skipped with a **warning** (not an error).
  - Bounded retry: `2 × gossip_probe_interval_ms`, capped at **5 s**.

---

## 15. Security & Transport

### Transport

| Channel | Protocol |
|---|---|
| Client → Node | **HTTPS** (strict TLS 1.3) |
| Node → Node | **mTLS** (gRPC/HTTP2) |
| Wire protocol (legacy) | Optional TLS 1.3 (rustls) |

### Principles

- Automatic certificate rotation.
- Node admission via CA + ephemeral bootstrap tokens.
- Backup keys are **separate** from runtime keys.
- Strict redaction of sensitive fields in logs/API.
- All 4xx errors are logged systematically.
- Immutable audit (hot + cold retention).
- No two-man rule in V1.

---

## 16. Operations & Observability

### Observability Port

- Dedicated HTTP port: **6381** (health, Prometheus metrics).

### Traffic Priority

- **Client traffic** MUST **always take priority** over background tasks (repair,
  compaction, backfill, anti-entropy).
- No global maintenance mode.

### Mandatory Metrics

MUST cover:
- Quorum (success/failure/latency)
- Cache (hit rate, evictions)
- Gossip (convergence, lag)
- Repair (read repair, anti-entropy)
- Hints (queue size, TTL expirations)
- Indexes (materialization activity)

### SLOs

- Two p95 latency classes (operator-defined).
- Automatic rollback on error budget breach.

---

## 17. Backup & Restore

### Strategy

| Component | Mechanism |
|---|---|
| Storage engine | **RocksDB** (built-in WAL + SST files) |
| Snapshots | **RocksDB checkpoints** every **15 minutes** |
| WAL shipping | Continuous (RocksDB WAL files) |
| Encryption | Mandatory, dedicated keys |

### Restore

- **Global** in V1 (no per-tenant restore).
- Post-restore validation is **mandatory** before reopening to traffic:
  - Checksums
  - Schema consistency
  - Quorum verification
  - Smoke tests
- Recovery procedures MUST be observable and testable.

---

## 18. Client SDK & Drivers

### Existing Drivers (from GrumpyDB)

- **Rust**: native async driver.
- **TypeScript**: Node.js ≥ 18, transport via `node:net` / `node:tls`.

### Required Client Behavior

| Behavior | Detail |
|---|---|
| Retry with jitter | Bounded budget, exponential backoff |
| Auto-refresh tokens | Transparent to the caller |
| Reconnection | Backoff: 100 ms → 500 ms → 2 s → 5 s |
| Bootstrap list refresh | Every **5 minutes** |
| Failover | Automatic failover to another node |
| Idempotency | Helpers for idempotency keys |
| Query warnings | Support for `QueryResult { rows, warnings }` |

### Conformance

- A **conformance suite** SHOULD cover: auth, retry, failover,
  idempotency, warnings.

---

## 19. Quotas & Guardrails

### Strict Quotas

| Category | Examples |
|---|---|
| Structure | Max collections per DB, max DBs per tenant |
| Writes | Per-tenant rate limiting |
| Storage | Max object size (strict global limit) |
| Indexes | Max secondary indexes per collection |
| Expansion | Reference resolution depth (server ceiling) |

### Query Guardrails

- **Forbidden**: scans on non-indexed fields (standard clients).
- **Guarded**: search fan-out, reference expansion.
- Abusive queries MUST be rejected with an explicit error code.

---

## 20. V1 Constraints & Deferred Items

### Present in V1

- ✅ Coordinator-less ring
- ✅ Quorum N=3/R=2/W=2
- ✅ HTTPS + mTLS
- ✅ Strict RBAC
- ✅ Hard delete only
- ✅ Cursor pagination
- ✅ Snapshots every 15 min
- ✅ Immutable audit

### Deferred (post-V1)

- ❌ No multi-object transactions
- ❌ No multi-object batch writes
- ❌ No per-tenant restore (global only)
- ❌ No two-man rule for administration
- ❌ No public soft delete
- ❌ No open non-indexed scans
- ❌ Alternative storage engine backends

---

## 21. Reference Numeric Parameters

### Storage Engine (RocksDB)

| Parameter | Value | Configurable |
|---|---|---|
| `write_buffer_size` | 64 MiB | Yes |
| `max_write_buffer_number` | 3 | Yes |
| `target_file_size_base` | 64 MiB | Yes |
| `max_background_jobs` | 4 | Yes |
| Compression L0–L1 | LZ4 | Yes |
| Compression L2+ | Zstd | Yes |
| Bloom filter bits/key | 10 | Yes |
| Block cache | 128 MiB | Yes |
| WAL sync | fsync per commit | No (invariant) |

### Clocks & Convergence

| Parameter | Value |
|---|---|
| HLC logical max | 65,535 /ms |
| Vector clock max entries | 4,096 |
| Gossip frequency | 1 Hz |
| Schema gossip retry cap | 5 s |
| Tombstone retention | 24 h |
| Hinted handoff TTL | 24 h |

### Wire Protocol & Network

| Parameter | Value |
|---|---|
| Server port (wire) | 6,380 |
| Observability port | 6,381 |
| Max connections | 1,024 |
| Protocol max line | 1 MiB |
| Protocol max bulk | 16 MiB |
| Protocol version | `4.0.0` |
| Max consecutive errors | 10 (connection dropped) |

### Authentication & Sessions

| Parameter | Value |
|---|---|
| Access token TTL | 1 h (3,600 s) |
| Refresh token TTL | 48 h (172,800 s) |
| Idempotency key retention | 15 min |
| Client bootstrap refresh | 5 min |
| Reconnect backoff | 100 ms → 500 ms → 2 s → 5 s |

### Cluster & Replication

| Parameter | Value |
|---|---|
| Replication factor (N) | 3 |
| Read quorum (R) | 2 |
| Write quorum (W) | 2 |
| Target vnodes/node | 256 |
| Snapshot interval | 15 min |

### Data Model

| Parameter | Value |
|---|---|
| Max nesting depth | 64 |
| String/Blob max | 16 MiB |
| Array max elements | 1,000,000 |
| Object max keys | 100,000 |
| Variable key max | 256 bytes |
| Naming pattern | `[a-z0-9_]{1,64}` |
