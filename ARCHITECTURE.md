# BashfulDB — Module Architecture

> This document defines the independent architectural modules derived from
> [SPEC.md](./SPEC.md). Each module maps to a Rust **workspace crate** with
> explicit trait boundaries, enabling parallel development by separate teams.

---

## Design Principles

1. **One-way dependency flow** — the dependency graph is a strict DAG (no cycles).
2. **Trait boundaries** — every inter-module dependency goes through a trait, not a concrete type.
3. **Independent testability** — each crate is testable in isolation with mocks/fakes.
4. **Single ownership** — each crate is owned by exactly one team.
5. **Thin integration crate** — a final `bashfuldb` binary crate wires everything together.

---

## Module Map

```
┌─────────────────────────────────────────────────────────────┐
│                     bashfuldb (binary)                        │
│              Wires all crates together, main()              │
└──────┬──────────┬───────────┬──────────┬───────────┬────────┘
       │          │           │          │           │
  ┌────▼───┐ ┌───▼────┐ ┌────▼───┐ ┌────▼───┐ ┌────▼───┐
  │  api   │ │ proto  │ │  ops   │ │ schema │ │  sdk   │
  └──┬──┬──┘ └───┬────┘ └──┬──┬──┘ └──┬──┬──┘ └────────┘
     │  │        │         │  │       │  │
  ┌──▼──▼──────────────────▼──▼───────▼──▼──┐
  │            replication                   │
  └──────┬──────────────┬───────────────┬────┘
         │              │               │
    ┌────▼───┐    ┌─────▼────┐    ┌─────▼────┐
    │cluster │    │ storage  │    │ document │
    └──┬─────┘    └──────────┘    └──────────┘
       │                │
  ┌────▼───┐      ┌─────▼────┐
  │ clock  │      │ storage  │
  └────────┘      │ ::rocksdb│
                  │ ::mem    │
                  └──────────┘
```

---

## Module Definitions

### 1. `bashfuldb-clock` — Clocks & Causality

**Team scope:** Hybrid Logical Clocks, vector clocks, causal ordering.

| Aspect | Detail |
|---|---|
| SPEC sections | §4 (Clock & Versioning), §13 (Conflict Resolution / Clocks) |
| Dependencies | None (leaf crate) |
| Key types | `Hlc`, `VectorClock`, `CausalOrder` |
| Key traits | `Clock: Send + Sync` |
| Test strategy | Pure unit tests, property-based (monotonicity, merge commutativity) |

**Boundary contract:**
```rust
pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> Hlc;
    fn tick(&self) -> Hlc;
    fn update(&self, received: Hlc) -> Hlc;
}
```

**Why standalone:** Zero dependencies, pure logic, critical correctness
requirements — benefits from focused ownership and exhaustive property testing.

---

### 2. `bashfuldb-document` — Data Model & Codec

**Team scope:** Value types, binary codec, JSON conversion, validation.

| Aspect | Detail |
|---|---|
| SPEC sections | §2 (Data Model) |
| Dependencies | None (leaf crate) |
| Key types | `Value`, `Document`, `ObjectId` (UUID) |
| Key traits | `Codec: Encode + Decode + Size` |
| Test strategy | Unit tests, roundtrip property tests, fuzz testing |

**Boundary contract:**
```rust
pub enum Value {
    Null, Bool(bool), Int(i64), Float(f64),
    String(String), Blob(Vec<u8>),
    Array(Vec<Value>), Object(BTreeMap<String, Value>),
}

pub trait Codec {
    fn encode(&self, value: &Value) -> Result<Vec<u8>>;
    fn decode(&self, bytes: &[u8]) -> Result<Value>;
}
```

**Why standalone:** Pure data types used by almost every other crate. Must be
stable and correct — ideal for a small focused team. No I/O, no async.

---

### 3. `bashfuldb-storage` — Storage Engine Abstraction & Implementations

**Team scope:** `StorageEngine` trait, RocksDB implementation, in-memory
implementation.

| Aspect | Detail |
|---|---|
| SPEC sections | §3 (Storage Engine), §5 (Cache & Memory) |
| Dependencies | `bashfuldb-document` (for value serialization) |
| Key types | `WriteBatch`, `StorageSnapshot`, `ColumnFamily` |
| Key traits | `StorageEngine`, `StorageSnapshot` |
| Implementations | `RocksDbEngine`, `MemEngine` |
| Test strategy | Trait conformance suite run against every impl |

**Boundary contract:** See SPEC.md §3 `StorageEngine` trait.

**Why standalone:** The most critical abstraction. The team owns the
RocksDB tuning, compaction strategy, column family layout, and the
in-memory engine for tests. Future engine backends (Sled, SQLite) will
be added here.

---

### 4. `bashfuldb-auth` — Authentication & RBAC

**Team scope:** Password hashing, JWT issuance/verification, role model,
authorization checks, auth store.

| Aspect | Detail |
|---|---|
| SPEC sections | §9 (Authentication & RBAC) |
| Dependencies | `bashfuldb-storage` (for persisting users/roles), `bashfuldb-clock` (for token timestamps) |
| Key types | `User`, `Role`, `Permission`, `AccessToken`, `RefreshToken` |
| Key traits | `Authenticator`, `Authorizer` |
| Test strategy | Unit tests with `MemEngine`, integration tests for token lifecycle |

**Boundary contract:**
```rust
pub trait Authenticator: Send + Sync {
    async fn login(&self, credentials: &Credentials) -> Result<TokenPair>;
    async fn refresh(&self, token: &str) -> Result<TokenPair>;
    async fn verify(&self, token: &str) -> Result<Claims>;
}

pub trait Authorizer: Send + Sync {
    fn check(&self, claims: &Claims, action: Action, resource: &Resource) -> Result<()>;
}
```

**Why standalone:** Security-critical code benefits from dedicated review and
audit. Clear input/output contract. Can be developed and penetration-tested
independently.

---

### 5. `bashfuldb-cluster` — Cluster Topology & Gossip

**Team scope:** Ring, vnodes, membership, failure detection, gossip protocol,
node state machine.

| Aspect | Detail |
|---|---|
| SPEC sections | §11 (Cluster & Topology), §14 (Gossip) |
| Dependencies | `bashfuldb-clock` (for HLC in gossip messages) |
| Key types | `Ring`, `Vnode`, `NodeId`, `NodeState`, `MembershipEvent` |
| Key traits | `Membership`, `FailureDetector`, `GossipTransport` |
| Test strategy | Deterministic simulation tests, chaos/partition injection |

**Boundary contract:**
```rust
pub trait Membership: Send + Sync {
    fn ring(&self) -> &Ring;
    fn owners_for_key(&self, key: &[u8]) -> Vec<NodeId>;
    fn node_state(&self, node: &NodeId) -> NodeState;
    fn subscribe(&self) -> broadcast::Receiver<MembershipEvent>;
}

pub trait GossipTransport: Send + Sync {
    async fn send(&self, target: &NodeId, payload: GossipMessage) -> Result<()>;
    async fn receive(&self) -> Result<(NodeId, GossipMessage)>;
}
```

**Why standalone:** Distributed systems expertise required. This team needs
deep knowledge of failure detectors, consistent hashing, and protocol
correctness. Heavy simulation testing.

---

### 6. `bashfuldb-replication` — Quorum, Repair & Convergence

**Team scope:** Quorum reads/writes, sloppy quorum, hinted handoff, read
repair, anti-entropy, conflict resolution.

| Aspect | Detail |
|---|---|
| SPEC sections | §12 (Replication & Quorum), §13 (Conflict Resolution) |
| Dependencies | `bashfuldb-cluster`, `bashfuldb-storage`, `bashfuldb-clock`, `bashfuldb-document` |
| Key types | `QuorumConfig`, `ReplicaSet`, `Hint`, `ConflictResult` |
| Key traits | `Coordinator` (request-level, not cluster-level), `RepairStrategy` |
| Test strategy | Simulation, fault injection, linearizability checks |

**Boundary contract:**
```rust
pub trait Coordinator: Send + Sync {
    async fn quorum_read(&self, key: &ObjectKey) -> Result<ReadResult>;
    async fn quorum_write(&self, key: &ObjectKey, value: &Document) -> Result<WriteResult>;
    async fn quorum_delete(&self, key: &ObjectKey) -> Result<WriteResult>;
}
```

> **Note:** "Coordinator" here refers to the per-request coordination role
> (any node can be a coordinator for any request), NOT a dedicated coordinator
> node — which is explicitly forbidden by the architecture.

**Why standalone:** The most complex distributed logic. Needs a team
comfortable with consensus trade-offs, vector clock merging, and repair
strategies. High simulation test coverage required.

---

### 7. `bashfuldb-schema` — Schema Management & Index Lifecycle

**Team scope:** Collection metadata, secondary index DDL, schema gossip,
lazy index materialization.

| Aspect | Detail |
|---|---|
| SPEC sections | §6 (Secondary Indexes), §14 (Schema Propagation) |
| Dependencies | `bashfuldb-cluster` (gossip transport), `bashfuldb-storage` (index CFs) |
| Key types | `SchemaVersion`, `IndexDefinition`, `SchemaLog` |
| Key traits | `SchemaManager`, `IndexMaterializer` |
| Test strategy | Integration tests with multi-node gossip simulation |

**Boundary contract:**
```rust
pub trait SchemaManager: Send + Sync {
    async fn create_index(&self, coll: &CollectionId, def: IndexDefinition) -> Result<SchemaVersion>;
    async fn drop_index(&self, coll: &CollectionId, index: &IndexId) -> Result<SchemaVersion>;
    fn current_version(&self) -> SchemaVersion;
    fn has_index(&self, coll: &CollectionId, index: &IndexId) -> bool;
}
```

**Why standalone:** Index lifecycle and schema convergence are logically
distinct from data replication. Blue-green replacement and lazy
materialization require careful state machine management.

---

### 8. `bashfuldb-protocol` — Wire Protocol (TCP/TLS)

**Team scope:** RESP-like parser/serializer, TCP server, TLS termination,
connection management.

| Aspect | Detail |
|---|---|
| SPEC sections | §8 (Wire Protocol) |
| Dependencies | `bashfuldb-auth` (for connection authentication), `bashfuldb-document` (for value serialization) |
| Key types | `Command`, `Response`, `Connection`, `ServerConfig` |
| Key traits | `CommandHandler` |
| Test strategy | Protocol conformance tests, fuzz testing on parser |

**Boundary contract:**
```rust
pub trait CommandHandler: Send + Sync {
    async fn handle(&self, cmd: Command, ctx: &ConnectionContext) -> Response;
}
```

**Why standalone:** Network I/O, parsing, and TLS are a distinct skill set.
The parser is a security boundary (untrusted input) — fuzz testing is
mandatory.

---

### 9. `bashfuldb-api` — HTTP API Layer

**Team scope:** HTTP routes, request/response mapping, pagination, idempotency,
error formatting.

| Aspect | Detail |
|---|---|
| SPEC sections | §10 (Public HTTP API) |
| Dependencies | `bashfuldb-auth`, `bashfuldb-replication` (via `Coordinator` trait), `bashfuldb-document`, `bashfuldb-schema` |
| Key types | `Router`, `ApiError`, `Cursor`, `IdempotencyStore` |
| Key traits | None exported (consumes other traits) |
| Test strategy | HTTP integration tests against mocked `Coordinator` + `Authenticator` |

**Why standalone:** API design, versioning, compatibility rules, and
pagination are a distinct concern. This team owns the public contract and
backward compatibility guarantees.

---

### 10. `bashfuldb-ops` — Operations & Observability

**Team scope:** Health checks, Prometheus metrics, backup/restore, snapshot
scheduling, SLO enforcement.

| Aspect | Detail |
|---|---|
| SPEC sections | §16 (Operations & Observability), §17 (Backup & Restore) |
| Dependencies | `bashfuldb-storage` (checkpoints), `bashfuldb-cluster` (health, membership) |
| Key types | `HealthStatus`, `MetricsRegistry`, `BackupJob`, `RestoreJob` |
| Key traits | `HealthCheck`, `BackupManager` |
| Test strategy | Integration tests, backup/restore roundtrip tests |

**Boundary contract:**
```rust
pub trait BackupManager: Send + Sync {
    async fn create_snapshot(&self) -> Result<SnapshotId>;
    async fn restore(&self, snapshot: SnapshotId) -> Result<RestoreReport>;
    async fn validate(&self, snapshot: SnapshotId) -> Result<ValidationReport>;
}
```

**Why standalone:** Ops expertise (metrics, alerting, SLOs, backup
procedures) is a distinct skill set. This team also owns runbooks and
recovery drills.

---

### 11. `bashfuldb-sdk` — Client Drivers

**Team scope:** Rust async client, TypeScript client, conformance test suite.

| Aspect | Detail |
|---|---|
| SPEC sections | §18 (Client SDK & Drivers) |
| Dependencies | `bashfuldb-protocol` (wire format only, as documentation — not a Rust dep for TS) |
| Sub-crates | `bashfuldb-sdk-rust`, `bashfuldb-sdk-ts` (separate npm package) |
| Key traits | `Client`, `ClusterClient` |
| Test strategy | Conformance suite against a real server, retry/failover simulation |

**Why standalone:** Driver development is client-side code with different
languages, toolchains, and release cycles. The conformance suite is the
contract.

---

## Dependency Graph (strict DAG)

```
Layer 0 (no deps):     clock    document
                          │          │
Layer 1:                  ├────┬─────┤
                          │    │     │
                       cluster storage auth
                          │    │   │   │
Layer 2:                  ├────┤   │   │
                          │    │   │   │
                     replication schema │
                          │    │   │   │
Layer 3:              protocol  │   │  │
                          │    │   │   │
Layer 4:                 api───┴───┘   │
                          │            │
Layer 5:                 ops───────────┘
                          │
Binary:                bashfuldb
```

---

## Workspace Layout

```
bashfuldb/
├── Cargo.toml              # [workspace]
├── SPEC.md
├── ARCHITECTURE.md         # this file
│
├── crates/
│   ├── bashfuldb-clock/
│   ├── bashfuldb-document/
│   ├── bashfuldb-storage/
│   ├── bashfuldb-auth/
│   ├── bashfuldb-cluster/
│   ├── bashfuldb-replication/
│   ├── bashfuldb-schema/
│   ├── bashfuldb-protocol/
│   ├── bashfuldb-api/
│   ├── bashfuldb-ops/
│   └── bashfuldb-sdk-rust/
│
├── sdk/
│   └── typescript/         # npm package, separate toolchain
│
├── tests/
│   ├── integration/        # cross-crate integration tests
│   └── e2e/                # full cluster end-to-end tests
│
└── src/
    └── main.rs             # thin binary — wires crates together
```

---

## Team Mapping Recommendation

| # | Crate | Team Profile | Estimated Size |
|---|---|---|---|
| 1 | `bashfuldb-clock` | Distributed systems / formal methods | 1–2 devs |
| 2 | `bashfuldb-document` | Rust generalist / serialization | 1–2 devs |
| 3 | `bashfuldb-storage` | Storage / database internals | 2–3 devs |
| 4 | `bashfuldb-auth` | Security engineering | 2 devs |
| 5 | `bashfuldb-cluster` | Distributed systems | 3–4 devs |
| 6 | `bashfuldb-replication` | Distributed systems (senior) | 3–4 devs |
| 7 | `bashfuldb-schema` | Database internals / DDL | 2 devs |
| 8 | `bashfuldb-protocol` | Networking / parsing | 2 devs |
| 9 | `bashfuldb-api` | Web / API design | 2–3 devs |
| 10 | `bashfuldb-ops` | SRE / observability | 2–3 devs |
| 11 | `bashfuldb-sdk` | Client-side / polyglot | 2–3 devs (per language) |

**Total: ~24–33 developers across 11 teams.**

---

## Integration Contract

Teams MUST agree on trait signatures **before** starting implementation.
The recommended workflow is:

1. **Trait-first design** — Define and stabilize the `pub trait` interfaces
   in each crate before writing implementations.
2. **Mock/fake implementations** — Each consuming crate provides its own
   test doubles (mocks or fakes) for upstream traits.
3. **Conformance test suites** — Each crate that exports a trait MUST also
   export a `#[cfg(test)]` conformance test suite that any implementation
   can run.
4. **Integration tests** — The `tests/integration/` directory contains
   cross-crate tests owned by the integration team (or rotating ownership).
5. **E2E tests** — Full cluster tests in `tests/e2e/` are the final gate.

---

## Build Order (parallel where possible)

```
Phase 1 (parallel):  clock, document
Phase 2 (parallel):  storage, cluster, auth     (depend on Phase 1)
Phase 3 (parallel):  replication, schema         (depend on Phase 2)
Phase 4 (parallel):  protocol, api, ops          (depend on Phase 2–3)
Phase 5:             sdk                         (depends on protocol spec)
Phase 6:             bashfuldb binary              (wires everything)
```

Phases 1 and 2 can start simultaneously on day one. The critical path runs
through `clock` → `cluster` → `replication` → `api` → `bashfuldb`.
