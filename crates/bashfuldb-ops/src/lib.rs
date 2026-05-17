//! Health checks, Prometheus metrics, backup/restore, and SLO enforcement.

use async_trait::async_trait;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use base64::Engine;
use bashfuldb_cluster::{Membership, NodeState};
use bashfuldb_storage::{ColumnFamily, StorageEngine, WriteBatch};
use prometheus::{
    Counter, CounterVec, Encoder, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec, Opts,
    Registry, TextEncoder,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::sync::{Notify, broadcast, oneshot};
use tokio::task::JoinHandle;

/// Default observability server port.
pub const OBSERVABILITY_PORT: u16 = 6381;
const SNAPSHOT_INTERVAL_SECS: u64 = 900;
const SNAPSHOT_INTERVAL: Duration = Duration::from_secs(SNAPSHOT_INTERVAL_SECS);
const BASE64_STANDARD: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;
const FNV1A_OFFSET_BASIS_64: u64 = 1469598103934665603_u64;
const FNV1A_PRIME_64: u64 = 1099511628211_u64;

/// Result type for ops operations.
pub type Result<T> = std::result::Result<T, OpsError>;

/// Error type for ops workflows.
#[derive(Debug, Error)]
pub enum OpsError {
    /// IO error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// Storage error.
    #[error("storage error: {0}")]
    Storage(#[from] bashfuldb_storage::StorageError),
    /// JSON error.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    /// Prometheus error.
    #[error("metrics error: {0}")]
    Metrics(#[from] prometheus::Error),
    /// Snapshot not found.
    #[error("snapshot not found: {0}")]
    SnapshotNotFound(String),
    /// Validation failed.
    #[error("validation failed: {0}")]
    ValidationFailed(String),
    /// Internal lock poisoned.
    #[error("lock poisoned: {0}")]
    LockPoisoned(&'static str),
}

/// Metrics registry abstraction.
pub trait MetricsRegistry: Send + Sync {
    /// Returns (and lazily registers) a counter metric.
    fn counter(&self, name: &str, labels: &[(&str, &str)]) -> Counter;
    /// Returns (and lazily registers) a histogram metric.
    fn histogram(&self, name: &str, labels: &[(&str, &str)]) -> Histogram;
    /// Returns (and lazily registers) a gauge metric.
    fn gauge(&self, name: &str, labels: &[(&str, &str)]) -> Gauge;
}

/// Health-check abstraction for node lifecycle.
#[async_trait]
pub trait HealthCheck: Send + Sync {
    /// Returns true when the node is healthy.
    async fn is_healthy(&self) -> bool;
    /// Returns true when the node is ready to serve traffic.
    async fn is_ready(&self) -> bool;
    /// Returns detailed health data.
    async fn detailed_status(&self) -> HealthReport;
}

/// Backup manager abstraction.
#[async_trait]
pub trait BackupManager: Send + Sync {
    /// Creates a new snapshot.
    async fn create_snapshot(&self) -> Result<SnapshotId>;
    /// Lists available snapshots.
    async fn list_snapshots(&self) -> Result<Vec<SnapshotInfo>>;
    /// Restores the database from a snapshot.
    async fn restore(&self, snapshot: SnapshotId) -> Result<RestoreReport>;
    /// Validates a snapshot.
    async fn validate(&self, snapshot: SnapshotId) -> Result<ValidationReport>;
    /// Deletes a snapshot.
    async fn delete_snapshot(&self, snapshot: SnapshotId) -> Result<()>;
}

/// Node health report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthReport {
    /// Node state from cluster membership.
    pub node_state: NodeState,
    /// Whether dependencies are reachable.
    pub dependencies_ok: bool,
    /// Whether node can serve traffic.
    pub ready: bool,
    /// Whether node is healthy.
    pub healthy: bool,
}

/// Simple health checker implementation.
#[derive(Debug)]
pub struct NodeHealth {
    state: RwLock<NodeState>,
    dependencies_ok: AtomicBool,
    restoring: AtomicBool,
}

impl NodeHealth {
    /// Creates a new health state.
    pub fn new(initial_state: NodeState) -> Self {
        Self {
            state: RwLock::new(initial_state),
            dependencies_ok: AtomicBool::new(true),
            restoring: AtomicBool::new(false),
        }
    }

    /// Sets node state.
    pub fn set_state(&self, state: NodeState) -> Result<()> {
        let mut guard = self
            .state
            .write()
            .map_err(|_| OpsError::LockPoisoned("node health write lock"))?;
        *guard = state;
        Ok(())
    }

    /// Sets dependency status.
    pub fn set_dependencies_ok(&self, ok: bool) {
        self.dependencies_ok.store(ok, Ordering::Relaxed);
    }

    /// Marks restore in-progress state.
    pub fn set_restoring(&self, restoring: bool) {
        self.restoring.store(restoring, Ordering::Relaxed);
    }
}

#[async_trait]
impl HealthCheck for NodeHealth {
    async fn is_healthy(&self) -> bool {
        match self.state.read() {
            Ok(state) => {
                *state == NodeState::Healthy
                    && self.dependencies_ok.load(Ordering::Relaxed)
                    && !self.restoring.load(Ordering::Relaxed)
            }
            Err(_) => false,
        }
    }

    async fn is_ready(&self) -> bool {
        match self.state.read() {
            Ok(state) => {
                state.can_serve_writes()
                    && self.dependencies_ok.load(Ordering::Relaxed)
                    && !self.restoring.load(Ordering::Relaxed)
            }
            Err(_) => false,
        }
    }

    async fn detailed_status(&self) -> HealthReport {
        let state = self
            .state
            .read()
            .map(|guard| *guard)
            .unwrap_or(NodeState::Down);
        let dependencies_ok = self.dependencies_ok.load(Ordering::Relaxed);
        let restoring = self.restoring.load(Ordering::Relaxed);
        let healthy = state == NodeState::Healthy && dependencies_ok && !restoring;
        let ready = state.can_serve_writes() && dependencies_ok && !restoring;
        HealthReport {
            node_state: state,
            dependencies_ok,
            ready,
            healthy,
        }
    }
}

#[derive(Clone)]
struct CounterDef {
    vec: CounterVec,
}

#[derive(Clone)]
struct GaugeDef {
    vec: GaugeVec,
}

#[derive(Clone)]
struct HistogramDef {
    vec: HistogramVec,
}

/// Prometheus-backed metrics registry implementation.
#[derive(Clone)]
pub struct PrometheusMetricsRegistry {
    registry: Registry,
    counters: Arc<Mutex<HashMap<String, CounterDef>>>,
    gauges: Arc<Mutex<HashMap<String, GaugeDef>>>,
    histograms: Arc<Mutex<HashMap<String, HistogramDef>>>,
}

impl fmt::Debug for PrometheusMetricsRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PrometheusMetricsRegistry")
            .finish_non_exhaustive()
    }
}

impl Default for PrometheusMetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl PrometheusMetricsRegistry {
    /// Creates a new empty registry.
    pub fn new() -> Self {
        Self {
            registry: Registry::new(),
            counters: Arc::new(Mutex::new(HashMap::new())),
            gauges: Arc::new(Mutex::new(HashMap::new())),
            histograms: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Encodes all metrics in Prometheus text format.
    pub fn render(&self) -> Result<String> {
        let mut buf = Vec::new();
        TextEncoder::new().encode(&self.registry.gather(), &mut buf)?;
        String::from_utf8(buf).map_err(|e| OpsError::ValidationFailed(e.to_string()))
    }
}

impl MetricsRegistry for PrometheusMetricsRegistry {
    fn counter(&self, name: &str, labels: &[(&str, &str)]) -> Counter {
        let label_names: Vec<String> = labels.iter().map(|(k, _)| (*k).to_string()).collect();
        let label_values: Vec<&str> = labels.iter().map(|(_, v)| *v).collect();
        let mut counters = self
            .counters
            .lock()
            .expect("counter lock poisoned in metrics registry");
        let entry = counters.entry(name.to_string()).or_insert_with(|| {
            let opts = Opts::new(name, format!("bashfuldb metric {name}"));
            let label_refs: Vec<&str> = label_names.iter().map(String::as_str).collect();
            let vec = CounterVec::new(opts, &label_refs).expect("valid counter metric definition");
            let _ = self.registry.register(Box::new(vec.clone()));
            CounterDef { vec }
        });
        entry
            .vec
            .get_metric_with_label_values(&label_values)
            .expect("counter label mismatch")
            .clone()
    }

    fn histogram(&self, name: &str, labels: &[(&str, &str)]) -> Histogram {
        let label_names: Vec<String> = labels.iter().map(|(k, _)| (*k).to_string()).collect();
        let label_values: Vec<&str> = labels.iter().map(|(_, v)| *v).collect();
        let mut histograms = self
            .histograms
            .lock()
            .expect("histogram lock poisoned in metrics registry");
        let entry = histograms.entry(name.to_string()).or_insert_with(|| {
            let opts = HistogramOpts::new(name, format!("bashfuldb metric {name}"));
            let label_refs: Vec<&str> = label_names.iter().map(String::as_str).collect();
            let vec =
                HistogramVec::new(opts, &label_refs).expect("valid histogram metric definition");
            let _ = self.registry.register(Box::new(vec.clone()));
            HistogramDef { vec }
        });
        entry
            .vec
            .get_metric_with_label_values(&label_values)
            .expect("histogram label mismatch")
            .clone()
    }

    fn gauge(&self, name: &str, labels: &[(&str, &str)]) -> Gauge {
        let label_names: Vec<String> = labels.iter().map(|(k, _)| (*k).to_string()).collect();
        let label_values: Vec<&str> = labels.iter().map(|(_, v)| *v).collect();
        let mut gauges = self
            .gauges
            .lock()
            .expect("gauge lock poisoned in metrics registry");
        let entry = gauges.entry(name.to_string()).or_insert_with(|| {
            let opts = Opts::new(name, format!("bashfuldb metric {name}"));
            let label_refs: Vec<&str> = label_names.iter().map(String::as_str).collect();
            let vec = GaugeVec::new(opts, &label_refs).expect("valid gauge metric definition");
            let _ = self.registry.register(Box::new(vec.clone()));
            GaugeDef { vec }
        });
        entry
            .vec
            .get_metric_with_label_values(&label_values)
            .expect("gauge label mismatch")
            .clone()
    }
}

/// Mandatory metrics bundle required by spec.
#[derive(Clone)]
pub struct OpsMetrics {
    /// Quorum read counter.
    pub quorum_read_total: Counter,
    /// Quorum write counter.
    pub quorum_write_total: Counter,
    /// Quorum failure counter.
    pub quorum_failure_total: Counter,
    /// Quorum latency histogram.
    pub quorum_latency_seconds: Histogram,
    /// Cache hit counter.
    pub cache_hit_total: Counter,
    /// Cache miss counter.
    pub cache_miss_total: Counter,
    /// Cache eviction counter.
    pub cache_eviction_total: Counter,
    /// Gossip rounds counter.
    pub gossip_rounds_total: Counter,
    /// Gossip convergence lag gauge.
    pub gossip_convergence_lag_seconds: Gauge,
    /// Read repair counter.
    pub read_repair_total: Counter,
    /// Anti-entropy run counter.
    pub anti_entropy_runs_total: Counter,
    /// Pending hints gauge.
    pub hints_pending: Gauge,
    /// Delivered hints counter.
    pub hints_delivered_total: Counter,
    /// Expired hints counter.
    pub hints_expired_total: Counter,
    /// Active index materialization gauge.
    pub index_materialization_active: Gauge,
    /// Completed index materialization counter.
    pub index_materialization_completed_total: Counter,
    /// Storage bytes gauge.
    pub storage_bytes: Gauge,
    /// Storage compaction running gauge.
    pub storage_compaction_running: Gauge,
}

impl OpsMetrics {
    /// Registers and returns all mandatory metrics.
    pub fn register(registry: &dyn MetricsRegistry) -> Self {
        Self {
            quorum_read_total: registry.counter("bashfuldb_quorum_read_total", &[]),
            quorum_write_total: registry.counter("bashfuldb_quorum_write_total", &[]),
            quorum_failure_total: registry.counter("bashfuldb_quorum_failure_total", &[]),
            quorum_latency_seconds: registry.histogram("bashfuldb_quorum_latency_seconds", &[]),
            cache_hit_total: registry.counter("bashfuldb_cache_hit_total", &[]),
            cache_miss_total: registry.counter("bashfuldb_cache_miss_total", &[]),
            cache_eviction_total: registry.counter("bashfuldb_cache_eviction_total", &[]),
            gossip_rounds_total: registry.counter("bashfuldb_gossip_rounds_total", &[]),
            gossip_convergence_lag_seconds: registry
                .gauge("bashfuldb_gossip_convergence_lag_seconds", &[]),
            read_repair_total: registry.counter("bashfuldb_read_repair_total", &[]),
            anti_entropy_runs_total: registry.counter("bashfuldb_anti_entropy_runs_total", &[]),
            hints_pending: registry.gauge("bashfuldb_hints_pending", &[]),
            hints_delivered_total: registry.counter("bashfuldb_hints_delivered_total", &[]),
            hints_expired_total: registry.counter("bashfuldb_hints_expired_total", &[]),
            index_materialization_active: registry
                .gauge("bashfuldb_index_materialization_active", &[]),
            index_materialization_completed_total: registry
                .counter("bashfuldb_index_materialization_completed_total", &[]),
            storage_bytes: registry.gauge("bashfuldb_storage_bytes", &[]),
            storage_compaction_running: registry.gauge("bashfuldb_storage_compaction_running", &[]),
        }
    }
}

/// Server state for observability HTTP handlers.
#[derive(Clone)]
pub struct ObservabilityState {
    health: Arc<dyn HealthCheck>,
    metrics: Arc<PrometheusMetricsRegistry>,
}

impl ObservabilityState {
    /// Creates shared handler state.
    pub fn new(health: Arc<dyn HealthCheck>, metrics: Arc<PrometheusMetricsRegistry>) -> Self {
        Self { health, metrics }
    }
}

/// Running observability server handle.
pub struct ObservabilityServerHandle {
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<()>>,
    addr: SocketAddr,
}

impl ObservabilityServerHandle {
    /// Returns bound address.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Requests graceful shutdown and waits for completion.
    pub async fn shutdown(mut self) -> Result<()> {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        self.task
            .await
            .map_err(|e| OpsError::ValidationFailed(e.to_string()))?
    }
}

/// Builds the observability router.
pub fn observability_router(state: ObservabilityState) -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .route("/ready", get(ready_handler))
        .route("/metrics", get(metrics_handler))
        .with_state(state)
}

/// Starts the observability server on the default ops port.
pub async fn start_observability_server(
    health: Arc<dyn HealthCheck>,
    metrics: Arc<PrometheusMetricsRegistry>,
) -> Result<ObservabilityServerHandle> {
    start_observability_server_on(
        SocketAddr::from(([0, 0, 0, 0], OBSERVABILITY_PORT)),
        health,
        metrics,
    )
    .await
}

/// Starts the observability server on a custom socket address.
pub async fn start_observability_server_on(
    addr: SocketAddr,
    health: Arc<dyn HealthCheck>,
    metrics: Arc<PrometheusMetricsRegistry>,
) -> Result<ObservabilityServerHandle> {
    let state = ObservabilityState::new(health, metrics);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    let (tx, rx) = oneshot::channel::<()>();
    let router = observability_router(state);
    let task = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = rx.await;
            })
            .await
            .map_err(OpsError::Io)
    });
    Ok(ObservabilityServerHandle {
        shutdown: Some(tx),
        task,
        addr: local_addr,
    })
}

async fn health_handler(State(state): State<ObservabilityState>) -> impl IntoResponse {
    if state.health.is_healthy().await {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn ready_handler(State(state): State<ObservabilityState>) -> impl IntoResponse {
    if state.health.is_ready().await {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn metrics_handler(State(state): State<ObservabilityState>) -> Response {
    match state.metrics.render() {
        Ok(body) => (
            StatusCode::OK,
            [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
            body,
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            [("content-type", "text/plain; charset=utf-8")],
            err.to_string(),
        )
            .into_response(),
    }
}

/// Snapshot identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SnapshotId(pub String);

/// Snapshot metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotInfo {
    /// Snapshot identifier.
    pub id: SnapshotId,
    /// Snapshot creation time (unix seconds).
    pub created_at: u64,
    /// Full path on disk.
    pub path: PathBuf,
}

/// Restore report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreReport {
    /// Number of rows restored.
    pub restored_rows: usize,
    /// Validation report executed after restore.
    pub validation: ValidationReport,
}

/// Validation report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationReport {
    /// SST/checkpoint checksum verification.
    pub checksum_ok: bool,
    /// Schema consistency verification.
    pub schema_ok: bool,
    /// Quorum verification.
    pub quorum_ok: bool,
    /// Smoke test verification.
    pub smoke_ok: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotManifest {
    created_at: u64,
    checksums: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotDump {
    rows: BTreeMap<String, Vec<SerializedRow>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SerializedRow {
    key_b64: String,
    value_b64: String,
}

/// Backup manager configuration.
#[derive(Debug, Clone)]
pub struct BackupConfig {
    /// Base data directory.
    pub data_dir: PathBuf,
    /// Logical database name.
    pub db_name: String,
    /// Retention duration for snapshots.
    pub retention: Duration,
    /// Quorum requirement (minimum healthy nodes).
    pub quorum_nodes: usize,
    /// Number of keys for smoke tests.
    pub smoke_keys: usize,
}

impl BackupConfig {
    /// Creates a config with 24h retention and quorum=2.
    pub fn new(data_dir: PathBuf, db_name: impl Into<String>) -> Self {
        Self {
            data_dir,
            db_name: db_name.into(),
            retention: Duration::from_secs(24 * 60 * 60),
            quorum_nodes: 2,
            smoke_keys: 16,
        }
    }
}

/// Filesystem snapshot manager.
pub struct FsBackupManager {
    engine: Arc<dyn StorageEngine>,
    membership: Arc<dyn Membership>,
    config: BackupConfig,
}

impl FsBackupManager {
    /// Creates a new backup manager.
    pub fn new(
        engine: Arc<dyn StorageEngine>,
        membership: Arc<dyn Membership>,
        config: BackupConfig,
    ) -> Self {
        Self {
            engine,
            membership,
            config,
        }
    }

    /// Snapshot root path: `{data_dir}/{db}/snapshots`.
    pub fn snapshots_root(&self) -> PathBuf {
        self.config
            .data_dir
            .join(&self.config.db_name)
            .join("snapshots")
    }

    fn snapshot_dir(&self, id: &SnapshotId) -> PathBuf {
        self.snapshots_root().join(&id.0)
    }

    fn now_unix_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_secs()
    }

    fn encode_dump(dump: &SnapshotDump) -> BTreeMap<String, u64> {
        dump.rows
            .iter()
            .map(|(cf, rows)| {
                let checksum = rows.iter().fold(FNV1A_OFFSET_BASIS_64, |acc, row| {
                    row.key_b64
                        .as_bytes()
                        .iter()
                        .chain(row.value_b64.as_bytes().iter())
                        .fold(acc, |h, byte| {
                            (h ^ (*byte as u64)).wrapping_mul(FNV1A_PRIME_64)
                        })
                });
                (cf.clone(), checksum)
            })
            .collect()
    }

    async fn dump_from_engine(&self) -> Result<SnapshotDump> {
        let mut rows = BTreeMap::new();
        for cf in ColumnFamily::all() {
            let iter = self.engine.scan(cf, &[], &[])?;
            let mut cf_rows = Vec::new();
            for item in iter {
                let (key, value) = item?;
                cf_rows.push(SerializedRow {
                    key_b64: BASE64_STANDARD.encode(key),
                    value_b64: BASE64_STANDARD.encode(value),
                });
            }
            rows.insert((*cf).to_string(), cf_rows);
        }
        Ok(SnapshotDump { rows })
    }

    fn parse_snapshot_id(path: &Path) -> Option<SnapshotId> {
        path.file_name()
            .and_then(|name| name.to_str())
            .map(|value| SnapshotId(value.to_string()))
    }

    async fn write_snapshot_artifacts(
        &self,
        snapshot_dir: &Path,
        dump: &SnapshotDump,
    ) -> Result<()> {
        let checksums = Self::encode_dump(dump);
        let manifest = SnapshotManifest {
            created_at: Self::now_unix_secs(),
            checksums,
        };
        tokio::fs::create_dir_all(snapshot_dir).await?;
        tokio::fs::write(
            snapshot_dir.join("data.json"),
            serde_json::to_vec_pretty(dump)?,
        )
        .await?;
        tokio::fs::write(
            snapshot_dir.join("manifest.json"),
            serde_json::to_vec_pretty(&manifest)?,
        )
        .await?;
        Ok(())
    }

    async fn read_snapshot_artifacts(
        &self,
        snapshot: &SnapshotId,
    ) -> Result<(SnapshotManifest, SnapshotDump)> {
        let dir = self.snapshot_dir(snapshot);
        if !dir.exists() {
            return Err(OpsError::SnapshotNotFound(snapshot.0.clone()));
        }
        let manifest_raw = tokio::fs::read(dir.join("manifest.json")).await?;
        let dump_raw = tokio::fs::read(dir.join("data.json")).await?;
        let manifest = serde_json::from_slice::<SnapshotManifest>(&manifest_raw)?;
        let dump = serde_json::from_slice::<SnapshotDump>(&dump_raw)?;
        Ok((manifest, dump))
    }

    async fn apply_dump(&self, dump: &SnapshotDump) -> Result<usize> {
        let mut clear_batch = WriteBatch::new();
        for cf in ColumnFamily::all() {
            let iter = self.engine.scan(cf, &[], &[])?;
            for item in iter {
                let (key, _) = item?;
                clear_batch.delete(*cf, key);
            }
        }
        if !clear_batch.is_empty() {
            self.engine.write_batch(clear_batch).await?;
        }

        let mut restore_batch = WriteBatch::new();
        let mut restored = 0_usize;
        for (cf, rows) in &dump.rows {
            let cf_name = cf.clone();
            for row in rows {
                let key = BASE64_STANDARD
                    .decode(row.key_b64.as_bytes())
                    .map_err(|e| OpsError::ValidationFailed(e.to_string()))?;
                let value = BASE64_STANDARD
                    .decode(row.value_b64.as_bytes())
                    .map_err(|e| OpsError::ValidationFailed(e.to_string()))?;
                restore_batch.put(cf_name.clone(), key, value);
                restored += 1;
            }
        }
        if !restore_batch.is_empty() {
            self.engine.write_batch(restore_batch).await?;
        }
        Ok(restored)
    }

    fn schema_consistency_ok(&self, dump: &SnapshotDump) -> bool {
        dump.rows.contains_key(ColumnFamily::METADATA)
            && dump.rows.contains_key(ColumnFamily::INDEXES)
    }

    fn quorum_ok(&self) -> bool {
        let healthy = self
            .membership
            .all_nodes()
            .into_iter()
            .filter(|node| {
                self.membership
                    .node_state(node)
                    .is_some_and(|state| state.can_serve_writes())
            })
            .count();
        healthy >= self.config.quorum_nodes
    }

    async fn smoke_test_ok(&self, dump: &SnapshotDump) -> Result<bool> {
        let mut sampled = 0_usize;
        for (cf, rows) in &dump.rows {
            for row in rows {
                if sampled >= self.config.smoke_keys {
                    return Ok(true);
                }
                let key = BASE64_STANDARD
                    .decode(row.key_b64.as_bytes())
                    .map_err(|e| OpsError::ValidationFailed(e.to_string()))?;
                let expected = BASE64_STANDARD
                    .decode(row.value_b64.as_bytes())
                    .map_err(|e| OpsError::ValidationFailed(e.to_string()))?;
                let actual = self.engine.get(cf, &key).await?;
                if actual != Some(expected) {
                    return Ok(false);
                }
                sampled += 1;
            }
        }
        Ok(true)
    }

    async fn prune_retention(&self) -> Result<()> {
        let mut snapshots = self.list_snapshots().await?;
        snapshots.sort_by_key(|snap| snap.created_at);
        let cutoff = Self::now_unix_secs().saturating_sub(self.config.retention.as_secs());
        for snapshot in snapshots {
            if snapshot.created_at < cutoff {
                self.delete_snapshot(snapshot.id).await?;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl BackupManager for FsBackupManager {
    async fn create_snapshot(&self) -> Result<SnapshotId> {
        let snapshot_id = SnapshotId(Self::now_unix_secs().to_string());
        let snapshot_dir = self.snapshot_dir(&snapshot_id);
        self.engine.flush().await?;
        self.engine.checkpoint(&snapshot_dir).await?;
        let dump = self.dump_from_engine().await?;
        self.write_snapshot_artifacts(&snapshot_dir, &dump).await?;
        self.prune_retention().await?;
        Ok(snapshot_id)
    }

    async fn list_snapshots(&self) -> Result<Vec<SnapshotInfo>> {
        let root = self.snapshots_root();
        tokio::fs::create_dir_all(&root).await?;
        let mut entries = tokio::fs::read_dir(&root).await?;
        let mut snapshots = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.is_dir()
                && let Some(id) = Self::parse_snapshot_id(&path)
                && let Ok(created_at) = id.0.parse::<u64>()
            {
                snapshots.push(SnapshotInfo {
                    id,
                    created_at,
                    path,
                });
            }
        }
        snapshots.sort_by_key(|snap| snap.created_at);
        Ok(snapshots)
    }

    async fn restore(&self, snapshot: SnapshotId) -> Result<RestoreReport> {
        let (_, dump) = self.read_snapshot_artifacts(&snapshot).await?;
        let restored_rows = self.apply_dump(&dump).await?;
        let validation = self.validate(snapshot).await?;
        if !(validation.checksum_ok
            && validation.schema_ok
            && validation.quorum_ok
            && validation.smoke_ok)
        {
            return Err(OpsError::ValidationFailed(
                "post-restore validation pipeline failed".to_string(),
            ));
        }
        Ok(RestoreReport {
            restored_rows,
            validation,
        })
    }

    async fn validate(&self, snapshot: SnapshotId) -> Result<ValidationReport> {
        let (manifest, dump) = self.read_snapshot_artifacts(&snapshot).await?;
        let checksum_ok = manifest.checksums == Self::encode_dump(&dump);
        let schema_ok = self.schema_consistency_ok(&dump);
        let quorum_ok = self.quorum_ok();
        let smoke_ok = self.smoke_test_ok(&dump).await?;
        Ok(ValidationReport {
            checksum_ok,
            schema_ok,
            quorum_ok,
            smoke_ok,
        })
    }

    async fn delete_snapshot(&self, snapshot: SnapshotId) -> Result<()> {
        let dir = self.snapshot_dir(&snapshot);
        if dir.exists() {
            tokio::fs::remove_dir_all(dir).await?;
            Ok(())
        } else {
            Err(OpsError::SnapshotNotFound(snapshot.0))
        }
    }
}

/// Automatic snapshot scheduler (15-minute checkpoints).
pub struct SnapshotScheduler<M: BackupManager> {
    manager: Arc<M>,
    interval: Duration,
}

impl<M: BackupManager + 'static> SnapshotScheduler<M> {
    /// Creates scheduler with default 15-minute period.
    pub fn new(manager: Arc<M>) -> Self {
        Self {
            manager,
            interval: SNAPSHOT_INTERVAL,
        }
    }

    /// Creates scheduler with custom period (useful for tests).
    pub fn with_interval(manager: Arc<M>, interval: Duration) -> Self {
        Self { manager, interval }
    }

    /// Runs scheduler until shutdown signal is received.
    pub async fn run(self, mut shutdown: oneshot::Receiver<()>) {
        let mut ticker = tokio::time::interval(self.interval);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let _ = self.manager.create_snapshot().await;
                }
                _ = &mut shutdown => break,
            }
        }
    }
}

/// Latency classes used for SLO tracking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LatencyClass {
    /// User-facing traffic.
    Interactive,
    /// Lower-priority/background traffic.
    Background,
}

/// Rollback event emitted when error budget is breached.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RollbackEvent {
    /// Class that breached budget.
    pub class: LatencyClass,
    /// Current breach ratio.
    pub error_budget_ratio: f64,
    /// Configured threshold.
    pub threshold: f64,
}

#[derive(Debug, Clone)]
struct RequestSample {
    latency: Duration,
    violation: bool,
}

/// Sliding-window SLO tracker with p95/error-budget computations.
pub struct SloTracker {
    thresholds: HashMap<LatencyClass, Duration>,
    windows: Mutex<HashMap<LatencyClass, VecDeque<RequestSample>>>,
    window_size: usize,
    error_budget_threshold: f64,
    rollback_tx: broadcast::Sender<RollbackEvent>,
}

impl SloTracker {
    /// Creates a new SLO tracker.
    pub fn new(
        interactive_p95: Duration,
        background_p95: Duration,
        window_size: usize,
        error_budget_threshold: f64,
    ) -> Self {
        let (rollback_tx, _) = broadcast::channel(64);
        Self {
            thresholds: HashMap::from([
                (LatencyClass::Interactive, interactive_p95),
                (LatencyClass::Background, background_p95),
            ]),
            windows: Mutex::new(HashMap::new()),
            window_size,
            error_budget_threshold,
            rollback_tx,
        }
    }

    /// Subscribes to rollback events.
    pub fn subscribe_rollbacks(&self) -> broadcast::Receiver<RollbackEvent> {
        self.rollback_tx.subscribe()
    }

    /// Records a request and emits rollback event when budget breaches.
    pub fn record_request(&self, class: LatencyClass, latency: Duration, error: bool) {
        let Some(threshold) = self.thresholds.get(&class).copied() else {
            return;
        };
        let violation = error || latency > threshold;
        if let Ok(mut windows) = self.windows.lock() {
            let queue = windows.entry(class).or_default();
            queue.push_back(RequestSample { latency, violation });
            while queue.len() > self.window_size {
                let _ = queue.pop_front();
            }
            let ratio = Self::error_budget_ratio(queue);
            if ratio > self.error_budget_threshold {
                let _ = self.rollback_tx.send(RollbackEvent {
                    class,
                    error_budget_ratio: ratio,
                    threshold: self.error_budget_threshold,
                });
            }
        }
    }

    /// Returns p95 latency for a class.
    pub fn p95_latency(&self, class: LatencyClass) -> Option<Duration> {
        let windows = self.windows.lock().ok()?;
        let queue = windows.get(&class)?;
        if queue.is_empty() {
            return None;
        }
        let mut latencies: Vec<Duration> = queue.iter().map(|sample| sample.latency).collect();
        latencies.sort_unstable();
        let idx = ((latencies.len() as f64) * 0.95).ceil() as usize;
        let selected = idx.saturating_sub(1).min(latencies.len().saturating_sub(1));
        latencies.get(selected).copied()
    }

    /// Returns current error budget ratio for class.
    pub fn error_budget_ratio_for(&self, class: LatencyClass) -> Option<f64> {
        let windows = self.windows.lock().ok()?;
        let queue = windows.get(&class)?;
        Some(Self::error_budget_ratio(queue))
    }

    fn error_budget_ratio(queue: &VecDeque<RequestSample>) -> f64 {
        if queue.is_empty() {
            return 0.0;
        }
        let violations = queue.iter().filter(|sample| sample.violation).count();
        violations as f64 / queue.len() as f64
    }
}

/// Traffic controller that gives client traffic priority over background jobs.
#[derive(Debug)]
pub struct TrafficPriorityController {
    client_in_flight: AtomicUsize,
    background_limit_while_busy: usize,
    active_background: AtomicUsize,
    notify: Notify,
}

impl TrafficPriorityController {
    /// Creates a new controller.
    pub fn new(background_limit_while_busy: usize) -> Self {
        Self {
            client_in_flight: AtomicUsize::new(0),
            background_limit_while_busy,
            active_background: AtomicUsize::new(0),
            notify: Notify::new(),
        }
    }

    /// Acquires a client-traffic guard.
    pub fn acquire_client(&self) -> ClientTrafficGuard<'_> {
        self.client_in_flight.fetch_add(1, Ordering::SeqCst);
        ClientTrafficGuard { controller: self }
    }

    /// Waits until background work can proceed.
    pub async fn background_yield_if_needed(&self) {
        loop {
            let clients = self.client_in_flight.load(Ordering::SeqCst);
            let active_background = self.active_background.load(Ordering::SeqCst);
            if clients == 0 || active_background < self.background_limit_while_busy {
                break;
            }
            self.notify.notified().await;
        }
    }

    /// Runs a background operation with traffic-aware yielding.
    pub async fn run_background<F, Fut, T>(&self, f: F) -> T
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        self.background_yield_if_needed().await;
        self.active_background.fetch_add(1, Ordering::SeqCst);
        let out = f().await;
        self.active_background.fetch_sub(1, Ordering::SeqCst);
        self.notify.notify_waiters();
        out
    }
}

/// Guard representing active client work.
pub struct ClientTrafficGuard<'a> {
    controller: &'a TrafficPriorityController,
}

impl Drop for ClientTrafficGuard<'_> {
    fn drop(&mut self) {
        self.controller
            .client_in_flight
            .fetch_sub(1, Ordering::SeqCst);
        self.controller.notify.notify_waiters();
    }
}

/// A no-op membership for standalone ops/testing.
#[derive(Debug)]
pub struct StandaloneMembership {
    local: bashfuldb_clock::NodeId,
}

impl StandaloneMembership {
    /// Creates a standalone membership with a local healthy node.
    pub fn new() -> Self {
        Self {
            local: bashfuldb_clock::NodeId::random(),
        }
    }
}

impl Default for StandaloneMembership {
    fn default() -> Self {
        Self::new()
    }
}

impl Membership for StandaloneMembership {
    fn ring_snapshot(&self) -> Arc<bashfuldb_cluster::Ring> {
        Arc::new(bashfuldb_cluster::Ring::new())
    }

    fn local_node(&self) -> bashfuldb_clock::NodeId {
        self.local
    }

    fn owners_for_key(&self, _key: &[u8], _n: usize) -> Vec<bashfuldb_clock::NodeId> {
        vec![self.local]
    }

    fn node_info(&self, _node: &bashfuldb_clock::NodeId) -> Option<bashfuldb_cluster::NodeInfo> {
        None
    }

    fn node_state(&self, node: &bashfuldb_clock::NodeId) -> Option<NodeState> {
        if *node == self.local {
            Some(NodeState::Healthy)
        } else {
            None
        }
    }

    fn all_nodes(&self) -> Vec<bashfuldb_clock::NodeId> {
        vec![self.local]
    }

    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<bashfuldb_cluster::MembershipEvent> {
        let (_tx, rx) = tokio::sync::broadcast::channel(1);
        rx
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bashfuldb_clock::NodeId;
    use bashfuldb_storage::{MemEngine, StorageEngine};
    use std::str::FromStr;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[derive(Debug)]
    struct TestMembership {
        nodes: Vec<(NodeId, NodeState)>,
        local: NodeId,
    }

    impl TestMembership {
        fn healthy_nodes(count: usize) -> Self {
            let mut nodes = Vec::new();
            for _ in 0..count {
                nodes.push((NodeId::random(), NodeState::Healthy));
            }
            let local = nodes.first().map(|entry| entry.0).unwrap_or(NodeId::nil());
            Self { nodes, local }
        }
    }

    impl Membership for TestMembership {
        fn ring_snapshot(&self) -> Arc<bashfuldb_cluster::Ring> {
            Arc::new(bashfuldb_cluster::Ring::new())
        }

        fn local_node(&self) -> NodeId {
            self.local
        }

        fn owners_for_key(&self, _key: &[u8], _n: usize) -> Vec<NodeId> {
            self.nodes.iter().map(|(id, _)| *id).collect()
        }

        fn node_info(&self, _node: &NodeId) -> Option<bashfuldb_cluster::NodeInfo> {
            None
        }

        fn node_state(&self, node: &NodeId) -> Option<NodeState> {
            self.nodes
                .iter()
                .find(|(id, _)| id == node)
                .map(|(_, state)| *state)
        }

        fn all_nodes(&self) -> Vec<NodeId> {
            self.nodes.iter().map(|(id, _)| *id).collect()
        }

        fn subscribe(
            &self,
        ) -> tokio::sync::broadcast::Receiver<bashfuldb_cluster::MembershipEvent> {
            let (_tx, rx) = tokio::sync::broadcast::channel(1);
            rx
        }
    }

    async fn http_get(addr: SocketAddr, path: &str) -> Result<(u16, String)> {
        let mut stream = tokio::net::TcpStream::connect(addr).await?;
        let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
        stream.write_all(req.as_bytes()).await?;
        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).await?;
        let text = String::from_utf8(raw).map_err(|e| OpsError::ValidationFailed(e.to_string()))?;
        let mut split = text.split("\r\n\r\n");
        let headers = split.next().unwrap_or_default();
        let body = split.next().unwrap_or_default().to_string();
        let status = headers
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|v| u16::from_str(v).ok())
            .unwrap_or(0);
        Ok((status, body))
    }

    #[tokio::test]
    async fn metrics_registry_registers_and_renders() {
        let registry = Arc::new(PrometheusMetricsRegistry::new());
        let metrics = OpsMetrics::register(registry.as_ref());
        metrics.quorum_read_total.inc();
        metrics.quorum_latency_seconds.observe(0.01);

        let text = registry.render().expect("render metrics");
        assert!(text.contains("bashfuldb_quorum_read_total"));
        assert!(text.contains("bashfuldb_quorum_latency_seconds"));
        assert!(text.contains("bashfuldb_storage_compaction_running"));
    }

    #[tokio::test]
    async fn health_check_state_transitions() {
        let health = NodeHealth::new(NodeState::Healthy);
        assert!(health.is_healthy().await);
        assert!(health.is_ready().await);

        health.set_state(NodeState::Draining).expect("set state");
        assert!(!health.is_ready().await);

        health.set_state(NodeState::Healthy).expect("set state");
        health.set_dependencies_ok(false);
        assert!(!health.is_healthy().await);
    }

    #[tokio::test]
    async fn observability_server_endpoints_work() {
        let health = Arc::new(NodeHealth::new(NodeState::Healthy));
        let registry = Arc::new(PrometheusMetricsRegistry::new());
        let metrics = OpsMetrics::register(registry.as_ref());
        metrics.quorum_read_total.inc();

        let handle = start_observability_server_on(
            SocketAddr::from(([127, 0, 0, 1], 0)),
            health.clone(),
            registry,
        )
        .await
        .expect("start server");
        let addr = handle.addr();

        let (health_status, _) = http_get(addr, "/health").await.expect("health");
        assert_eq!(health_status, 200);

        health.set_state(NodeState::Suspect).expect("set state");
        let (ready_status, _) = http_get(addr, "/ready").await.expect("ready");
        assert_eq!(ready_status, 503);

        let (metrics_status, body) = http_get(addr, "/metrics").await.expect("metrics");
        assert_eq!(metrics_status, 200);
        assert!(body.contains("bashfuldb_quorum_read_total"));

        handle.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn backup_restore_roundtrip_mem_engine() {
        let engine: Arc<dyn StorageEngine> = Arc::new(MemEngine::new());
        engine
            .put(ColumnFamily::DEFAULT, b"k1", b"v1")
            .await
            .expect("put k1");
        engine
            .put(ColumnFamily::METADATA, b"schema/users", b"{}")
            .await
            .expect("put schema");
        engine
            .put(ColumnFamily::INDEXES, b"idx/users/name/alice", b"k1")
            .await
            .expect("put index");

        let tmp = std::env::temp_dir().join(format!("bashfuldb-ops-test-{}", NodeId::random()));
        let membership: Arc<dyn Membership> = Arc::new(TestMembership::healthy_nodes(3));
        let mut config = BackupConfig::new(tmp.clone(), "db");
        config.quorum_nodes = 2;
        let manager = FsBackupManager::new(engine.clone(), membership, config);

        let snapshot = manager.create_snapshot().await.expect("create snapshot");
        engine
            .put(ColumnFamily::DEFAULT, b"k1", b"changed")
            .await
            .expect("mutate");

        let report = manager.restore(snapshot.clone()).await.expect("restore");
        assert!(report.validation.checksum_ok);
        let value = engine
            .get(ColumnFamily::DEFAULT, b"k1")
            .await
            .expect("get")
            .expect("exists");
        assert_eq!(value, b"v1".to_vec());

        let validation = manager.validate(snapshot.clone()).await.expect("validate");
        assert!(validation.schema_ok);
        manager
            .delete_snapshot(snapshot)
            .await
            .expect("delete snapshot");
        let _ = tokio::fs::remove_dir_all(tmp).await;
    }

    #[tokio::test]
    async fn snapshot_scheduler_triggers_backups() {
        #[derive(Default)]
        struct TestBackupManager {
            calls: AtomicUsize,
        }

        #[async_trait]
        impl BackupManager for TestBackupManager {
            async fn create_snapshot(&self) -> Result<SnapshotId> {
                let count = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
                Ok(SnapshotId(count.to_string()))
            }
            async fn list_snapshots(&self) -> Result<Vec<SnapshotInfo>> {
                Ok(Vec::new())
            }
            async fn restore(&self, _snapshot: SnapshotId) -> Result<RestoreReport> {
                Err(OpsError::ValidationFailed("not needed".to_string()))
            }
            async fn validate(&self, _snapshot: SnapshotId) -> Result<ValidationReport> {
                Err(OpsError::ValidationFailed("not needed".to_string()))
            }
            async fn delete_snapshot(&self, _snapshot: SnapshotId) -> Result<()> {
                Ok(())
            }
        }

        let manager = Arc::new(TestBackupManager::default());
        let scheduler =
            SnapshotScheduler::with_interval(manager.clone(), Duration::from_millis(25));
        let (tx, rx) = oneshot::channel();
        let task = tokio::spawn(scheduler.run(rx));
        tokio::time::sleep(Duration::from_millis(80)).await;
        let _ = tx.send(());
        let _ = task.await;
        assert!(manager.calls.load(Ordering::SeqCst) >= 2);
    }

    #[tokio::test]
    async fn slo_tracker_emits_rollback_event_on_breach() {
        let tracker = SloTracker::new(
            Duration::from_millis(20),
            Duration::from_millis(100),
            10,
            0.3,
        );
        let mut rx = tracker.subscribe_rollbacks();
        for _ in 0..5 {
            tracker.record_request(LatencyClass::Interactive, Duration::from_millis(50), false);
        }
        let event = rx.recv().await.expect("rollback event");
        assert_eq!(event.class, LatencyClass::Interactive);
        assert!(event.error_budget_ratio > 0.3);
    }

    #[tokio::test]
    async fn traffic_priority_blocks_background_while_busy() {
        let controller = Arc::new(TrafficPriorityController::new(0));
        let client_guard = controller.acquire_client();

        let ctl = controller.clone();
        let bg = tokio::spawn(async move { ctl.run_background(|| async { 42_u8 }).await });
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!bg.is_finished());
        drop(client_guard);

        let value = bg.await.expect("background completion");
        assert_eq!(value, 42);
    }
}
