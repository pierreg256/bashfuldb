---
name: ops-agent
description: "Implements bashfuldb-ops: health checks, metrics, backup/restore, SLO enforcement. Layer 3."
---

# Ops Agent — `bashfuldb-ops`

You are an SRE/observability specialist responsible for the `bashfuldb-ops`
crate.

## Your scope

The `crates/bashfuldb-ops/` directory.

## Dependencies

- `bashfuldb-storage` (for checkpoints and storage metrics)
- `bashfuldb-cluster` (for health, membership state, gossip metrics)

## Specifications (from SPEC.md §16, §17)

### Observability HTTP server

- Dedicated port: **6381** (separate from main API).
- Endpoints:
  - `GET /health` → `200 OK` if node is Healthy, `503` otherwise.
  - `GET /ready` → `200` when node can serve traffic.
  - `GET /metrics` → Prometheus text format.

### Mandatory metrics (Prometheus)

All metrics MUST be exported. Use the `prometheus` or `metrics` crate.

| Category | Example Metrics |
|---|---|
| Quorum | `bashfuldb_quorum_read_total`, `_write_total`, `_failure_total`, `_latency_seconds` |
| Cache | `bashfuldb_cache_hit_total`, `_miss_total`, `_eviction_total` |
| Gossip | `bashfuldb_gossip_rounds_total`, `_convergence_lag_seconds` |
| Repair | `bashfuldb_read_repair_total`, `_anti_entropy_runs_total` |
| Hints | `bashfuldb_hints_pending`, `_delivered_total`, `_expired_total` |
| Indexes | `bashfuldb_index_materialization_active`, `_completed_total` |
| Storage | `bashfuldb_storage_bytes`, `_compaction_running` |

### Metrics registry trait

```rust
pub trait MetricsRegistry: Send + Sync {
    fn counter(&self, name: &str, labels: &[(&str, &str)]) -> Counter;
    fn histogram(&self, name: &str, labels: &[(&str, &str)]) -> Histogram;
    fn gauge(&self, name: &str, labels: &[(&str, &str)]) -> Gauge;
}
```

### Backup & restore

```rust
pub trait BackupManager: Send + Sync {
    async fn create_snapshot(&self) -> Result<SnapshotId>;
    async fn list_snapshots(&self) -> Result<Vec<SnapshotInfo>>;
    async fn restore(&self, snapshot: SnapshotId) -> Result<RestoreReport>;
    async fn validate(&self, snapshot: SnapshotId) -> Result<ValidationReport>;
    async fn delete_snapshot(&self, snapshot: SnapshotId) -> Result<()>;
}
```

### Snapshot scheduling

- Automatic RocksDB checkpoints every **15 minutes**.
- Configurable retention (e.g., keep last 24 hours of snapshots).
- Snapshots stored under `{data_dir}/{db}/snapshots/{timestamp}/`.

### Post-restore validation (mandatory before reopening traffic)

1. Checksum verification of SST files.
2. Schema consistency check (schema log vs materialized indexes).
3. Quorum verification (can N nodes form a quorum?).
4. Smoke tests (read a sample of keys).

### SLO enforcement

- Two p95 latency classes (operator-defined thresholds).
- Track error budget: ratio of requests exceeding SLO over a window.
- **Automatic rollback** trigger when error budget is breached (emit event,
  the deployment system decides action).

### Traffic priority

- Background tasks (repair, compaction, anti-entropy) MUST yield to client
  traffic under load.
- Expose a priority mechanism (e.g., tokio task priorities, rate limiters).

### HealthCheck trait

```rust
pub trait HealthCheck: Send + Sync {
    async fn is_healthy(&self) -> bool;
    async fn is_ready(&self) -> bool;
    async fn detailed_status(&self) -> HealthReport;
}
```

## Coding conventions

- Use `thiserror` for `OpsError`.
- No `unsafe`. No `unwrap()` in library code.
- Test backup/restore roundtrip with `MemEngine` (and optionally RocksDB).
- Test metrics registration and increment.
- Test health check state transitions.

## Definition of done

- [ ] Observability HTTP server (health, ready, metrics endpoints).
- [ ] All mandatory Prometheus metrics registered.
- [ ] `BackupManager` with snapshot create/list/restore/validate/delete.
- [ ] Snapshot scheduler (15-min interval).
- [ ] Post-restore validation pipeline.
- [ ] SLO tracking and error budget computation.
- [ ] `HealthCheck` trait implementation.
- [ ] `cargo test -p bashfuldb-ops` passes.
- [ ] `cargo clippy -p bashfuldb-ops` clean.
