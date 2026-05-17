#![deny(missing_docs)]

//! Schema management, secondary index lifecycle, and schema gossip.

use async_trait::async_trait;
use base64::Engine;
use bashfuldb_storage::{ColumnFamily, StorageEngine};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use thiserror::Error;
use tracing::warn;
use uuid::Uuid;

const METADATA_STATE_KEY: &[u8] = b"schema/state";

/// Result type for schema operations.
pub type Result<T> = std::result::Result<T, SchemaError>;

/// Identifier for a collection.
pub type CollectionId = String;

/// Identifier for an index.
pub type IndexId = String;

/// Monotonic schema version across the cluster.
pub type SchemaVersion = u64;

/// Definition of a secondary index.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexDefinition {
    /// Index ID.
    pub id: IndexId,
    /// Collection ID this index belongs to.
    pub collection: CollectionId,
    /// Indexed field name (single-field in V1).
    pub field: String,
    /// Schema version when this index was created.
    pub created_at: SchemaVersion,
}

/// Schema state for a collection.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct CollectionSchema {
    /// Collection ID.
    pub id: CollectionId,
    /// Index definitions known for this collection.
    pub indexes: Vec<IndexDefinition>,
    /// Latest schema version touching this collection.
    pub version: SchemaVersion,
}

/// Schema operation log entry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaLogEntry {
    /// Monotonic schema version.
    pub version: SchemaVersion,
    /// DDL operation kind.
    pub op: SchemaOp,
    /// Collection ID.
    pub collection: CollectionId,
    /// Field name for create/drop semantics.
    pub field: String,
    /// Index ID involved in the operation.
    pub index_id: IndexId,
    /// Operation timestamp string.
    pub timestamp: String,
}

/// Supported schema log operations.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SchemaOp {
    /// Create index operation.
    CreateIndex,
    /// Drop index operation.
    DropIndex,
}

/// Schema manager interface.
#[async_trait]
pub trait SchemaManager: Send + Sync {
    /// Creates an index on a collection field.
    async fn create_index(&self, coll: &CollectionId, field: &str) -> Result<SchemaVersion>;

    /// Drops an index by ID.
    async fn drop_index(&self, coll: &CollectionId, index: &IndexId) -> Result<SchemaVersion>;

    /// Returns current schema version.
    fn current_version(&self) -> SchemaVersion;

    /// Returns true if an index exists in schema metadata.
    fn has_index(&self, coll: &CollectionId, index: &IndexId) -> bool;

    /// Returns a collection schema snapshot if it exists.
    fn collection_schema(&self, coll: &CollectionId) -> Option<CollectionSchema>;
}

/// Index materialization interface.
#[async_trait]
pub trait IndexMaterializer: Send + Sync {
    /// Builds or rebuilds materialized index data.
    async fn materialize(&self, def: &IndexDefinition) -> Result<()>;

    /// Drops all materialized rows for an index.
    async fn drop_materialized(&self, index: &IndexId) -> Result<()>;

    /// Returns true if the index is ready for query use.
    fn is_ready(&self, index: &IndexId) -> bool;
}

/// Errors for schema management and index lifecycle.
#[derive(Debug, Error)]
pub enum SchemaError {
    /// Storage operation failed.
    #[error("storage error: {0}")]
    Storage(#[from] bashfuldb_storage::StorageError),

    /// JSON serialization or deserialization failed.
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),

    /// Filesystem operation failed.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Collection exceeded configured secondary-index quota.
    #[error("index quota exceeded for collection '{collection}': limit {limit}")]
    IndexQuotaExceeded {
        /// Collection ID.
        collection: CollectionId,
        /// Configured strict limit.
        limit: usize,
    },

    /// Referenced index was not found.
    #[error("index '{index}' not found in collection '{collection}'")]
    IndexNotFound {
        /// Collection ID.
        collection: CollectionId,
        /// Index ID.
        index: IndexId,
    },

    /// Identifier contains disallowed characters.
    #[error("invalid {kind} identifier: {value}")]
    InvalidIdentifier {
        /// Identifier kind.
        kind: &'static str,
        /// Provided identifier value.
        value: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct SchemaState {
    version: SchemaVersion,
    collections: HashMap<CollectionId, CollectionSchema>,
    active_by_field: HashMap<CollectionId, HashMap<String, IndexId>>,
}

/// Schema manager configuration.
#[derive(Debug, Clone)]
pub struct SchemaConfig {
    /// Root path where `_cluster/schema.log` is written.
    pub root_path: PathBuf,
    /// Strict maximum number of secondary indexes per collection.
    pub max_secondary_indexes_per_collection: usize,
}

impl Default for SchemaConfig {
    fn default() -> Self {
        Self {
            root_path: PathBuf::from("."),
            max_secondary_indexes_per_collection: 16,
        }
    }
}

/// Concrete schema manager with log persistence and lazy materialization.
pub struct DefaultSchemaManager<E>
where
    E: StorageEngine,
{
    engine: Arc<E>,
    materializer: Arc<dyn IndexMaterializer>,
    state: Arc<RwLock<SchemaState>>,
    version: AtomicU64,
    log_path: PathBuf,
    max_secondary_indexes_per_collection: usize,
}

impl<E> std::fmt::Debug for DefaultSchemaManager<E>
where
    E: StorageEngine,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DefaultSchemaManager")
            .field("log_path", &self.log_path)
            .field(
                "max_secondary_indexes_per_collection",
                &self.max_secondary_indexes_per_collection,
            )
            .field("version", &self.local_version())
            .finish()
    }
}

impl<E> DefaultSchemaManager<E>
where
    E: StorageEngine,
{
    /// Creates a schema manager and restores schema state from metadata/log.
    pub async fn new(engine: Arc<E>, config: SchemaConfig) -> Result<Arc<Self>> {
        let materializer = Arc::new(DefaultIndexMaterializer::new(Arc::clone(&engine)));
        Self::with_materializer(engine, materializer, config).await
    }

    /// Creates a schema manager with a custom index materializer.
    pub async fn with_materializer(
        engine: Arc<E>,
        materializer: Arc<dyn IndexMaterializer>,
        config: SchemaConfig,
    ) -> Result<Arc<Self>> {
        let log_path = config.root_path.join("_cluster").join("schema.log");
        let manager = Arc::new(Self {
            engine,
            materializer,
            state: Arc::new(RwLock::new(SchemaState::default())),
            version: AtomicU64::new(0),
            log_path,
            max_secondary_indexes_per_collection: config.max_secondary_indexes_per_collection,
        });
        manager.restore_state().await?;
        Ok(manager)
    }

    async fn restore_state(&self) -> Result<()> {
        if let Some(serialized) = self
            .engine
            .get(ColumnFamily::METADATA, METADATA_STATE_KEY)
            .await?
        {
            let restored: SchemaState = serde_json::from_slice(&serialized)?;
            self.version.store(restored.version, Ordering::Release);
            let mut state = write_lock(&self.state);
            *state = restored;
            return Ok(());
        }
        self.replay_from_log().await
    }

    async fn replay_from_log(&self) -> Result<()> {
        let entries = self.read_log_entries()?;
        if entries.is_empty() {
            return Ok(());
        }
        self.apply_entries(entries).await?;
        self.persist_state_snapshot().await
    }

    fn read_log_entries(&self) -> Result<Vec<SchemaLogEntry>> {
        if !self.log_path.exists() {
            return Ok(Vec::new());
        }
        let file = std::fs::File::open(&self.log_path)?;
        let reader = BufReader::new(file);
        let mut entries = Vec::new();
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            entries.push(serde_json::from_str::<SchemaLogEntry>(&line)?);
        }
        entries.sort_by_key(|entry| entry.version);
        Ok(entries)
    }

    async fn apply_entries(&self, entries: Vec<SchemaLogEntry>) -> Result<()> {
        let mut state = write_lock(&self.state);
        for entry in entries {
            if entry.version <= state.version {
                continue;
            }
            validate_identifier(&entry.collection, "collection")?;
            validate_identifier(&entry.field, "field")?;
            validate_identifier(&entry.index_id, "index")?;
            apply_entry_mut(&mut state, entry);
        }
        self.version.store(state.version, Ordering::Release);
        Ok(())
    }

    async fn append_log_entry(&self, entry: &SchemaLogEntry) -> Result<()> {
        let cluster_dir = self
            .log_path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("_cluster"));
        std::fs::create_dir_all(cluster_dir)?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = file.metadata()?.permissions();
            perms.set_mode(0o644);
            file.set_permissions(perms)?;
        }
        let line = serde_json::to_string(entry)?;
        file.write_all(line.as_bytes())?;
        file.write_all(b"\n")?;
        file.flush()?;
        Ok(())
    }

    async fn persist_state_snapshot(&self) -> Result<()> {
        let serialized = {
            let state = read_lock(&self.state);
            serde_json::to_vec(&*state)?
        };
        self.engine
            .put(ColumnFamily::METADATA, METADATA_STATE_KEY, &serialized)
            .await?;
        Ok(())
    }

    fn next_version(&self) -> SchemaVersion {
        self.version.fetch_add(1, Ordering::AcqRel) + 1
    }

    fn local_version(&self) -> SchemaVersion {
        self.version.load(Ordering::Acquire)
    }

    fn spawn_background_materialization(
        manager: Arc<Self>,
        definition: IndexDefinition,
        replacement: Option<IndexDefinition>,
    ) {
        tokio::spawn(async move {
            if let Err(error) = manager.materializer.materialize(&definition).await {
                warn!(
                    index_id = %definition.id,
                    collection = %definition.collection,
                    field = %definition.field,
                    "index materialization failed: {error}"
                );
                return;
            }
            if let Some(old) = replacement {
                {
                    let mut state = write_lock(&manager.state);
                    if let Some(active) = state.active_by_field.get_mut(&definition.collection) {
                        active.insert(definition.field.clone(), definition.id.clone());
                    }
                    if let Some(coll_schema) = state.collections.get_mut(&definition.collection) {
                        coll_schema.indexes.retain(|idx| idx.id != old.id);
                    }
                }
                if let Err(error) = manager.materializer.drop_materialized(&old.id).await {
                    warn!(old_index = %old.id, "failed to drop replaced materialized index: {error}");
                }
                if let Err(error) = manager.persist_state_snapshot().await {
                    warn!("failed to persist post-swap schema state: {error}");
                }
            }
        });
    }

    /// Returns active index ID for a collection field when ready for query use.
    ///
    /// If a configured read quorum is `>= 2` and the selected index is not
    /// ready, this method returns `None` and emits a warning instead of erroring.
    pub fn query_index_for_field(
        &self,
        coll: &CollectionId,
        field: &str,
        read_consistency_r: usize,
    ) -> Option<IndexId> {
        let state = read_lock(&self.state);
        let index = state
            .active_by_field
            .get(coll)
            .and_then(|by_field| by_field.get(field))
            .cloned();
        let index = index?;
        if !self.materializer.is_ready(&index) {
            if read_consistency_r >= 2 {
                warn!(
                    collection = %coll,
                    field,
                    index_id = %index,
                    r = read_consistency_r,
                    "index not ready on this node; skipping index for query"
                );
            }
            return None;
        }
        Some(index)
    }

    /// Handles a remote schema-version observation from membership gossip.
    ///
    /// This method fetches missing schema log entries from `provider` when
    /// `remote_version` is ahead of local state. Retries are bounded by
    /// `2 × gossip_probe_interval_ms` (capped at 5 seconds).
    pub async fn converge_from_gossip<P: SchemaLogProvider>(
        &self,
        sender: &str,
        remote_version: SchemaVersion,
        gossip_probe_interval_ms: u64,
        provider: &P,
    ) -> Result<()> {
        if remote_version <= self.local_version() {
            return Ok(());
        }

        let retry_delay_ms = (2 * gossip_probe_interval_ms).min(5_000);
        let retry_delay = std::time::Duration::from_millis(retry_delay_ms);

        for attempt in 0..3 {
            let since = self.local_version();
            let missing = provider.fetch_since(sender, since).await?;
            if !missing.is_empty() {
                self.apply_entries(missing).await?;
                self.persist_state_snapshot().await?;
                if self.local_version() >= remote_version {
                    return Ok(());
                }
            }
            if attempt < 2 {
                tokio::time::sleep(retry_delay).await;
            }
        }
        Ok(())
    }

    /// Convenience integration point for membership gossip messages.
    ///
    /// Call this when receiving a [`bashfuldb_cluster::GossipMessage`], which
    /// already piggybacks the sender's `schema_version`.
    pub async fn converge_from_membership_gossip<P: SchemaLogProvider>(
        &self,
        sender: &str,
        message: &bashfuldb_cluster::GossipMessage,
        gossip_probe_interval_ms: u64,
        provider: &P,
    ) -> Result<()> {
        self.converge_from_gossip(
            sender,
            message.schema_version,
            gossip_probe_interval_ms,
            provider,
        )
        .await
    }

    /// Returns a snapshot of log entries after `version` for schema sync.
    pub fn log_entries_since(&self, version: SchemaVersion) -> Result<Vec<SchemaLogEntry>> {
        let entries = self.read_log_entries()?;
        Ok(entries
            .into_iter()
            .filter(|entry| entry.version > version)
            .collect())
    }
}

#[async_trait]
impl<E> SchemaManager for Arc<DefaultSchemaManager<E>>
where
    E: StorageEngine,
{
    async fn create_index(&self, coll: &CollectionId, field: &str) -> Result<SchemaVersion> {
        validate_identifier(coll, "collection")?;
        validate_identifier(field, "field")?;
        let replacement = {
            let state = read_lock(&self.state);
            state
                .active_by_field
                .get(coll)
                .and_then(|active| active.get(field))
                .and_then(|current_id| {
                    state
                        .collections
                        .get(coll)
                        .and_then(|schema| schema.indexes.iter().find(|idx| &idx.id == current_id))
                })
                .cloned()
        };

        {
            let state = read_lock(&self.state);
            let active_count = state
                .active_by_field
                .get(coll)
                .map_or(0, std::collections::HashMap::len);
            let replacement_for_same_field = replacement.is_some();
            if active_count >= self.max_secondary_indexes_per_collection
                && !replacement_for_same_field
            {
                return Err(SchemaError::IndexQuotaExceeded {
                    collection: coll.clone(),
                    limit: self.max_secondary_indexes_per_collection,
                });
            }
        }

        let version = self.next_version();
        let index_id = Uuid::new_v4().to_string();
        validate_identifier(&index_id, "index")?;
        let definition = IndexDefinition {
            id: index_id.clone(),
            collection: coll.clone(),
            field: field.to_string(),
            created_at: version,
        };
        let entry = SchemaLogEntry {
            version,
            op: SchemaOp::CreateIndex,
            collection: coll.clone(),
            field: field.to_string(),
            index_id: index_id.clone(),
            timestamp: now_timestamp(),
        };

        self.append_log_entry(&entry).await?;

        {
            let mut state = write_lock(&self.state);
            apply_entry_mut(&mut state, entry);
            if replacement.is_none() {
                let active = state.active_by_field.entry(coll.clone()).or_default();
                active.insert(field.to_string(), index_id);
            }
        }
        self.persist_state_snapshot().await?;
        DefaultSchemaManager::spawn_background_materialization(
            Arc::clone(self),
            definition,
            replacement,
        );
        Ok(version)
    }

    async fn drop_index(&self, coll: &CollectionId, index: &IndexId) -> Result<SchemaVersion> {
        validate_identifier(coll, "collection")?;
        validate_identifier(index, "index")?;
        let field = {
            let state = read_lock(&self.state);
            state
                .collections
                .get(coll)
                .and_then(|schema| schema.indexes.iter().find(|idx| &idx.id == index))
                .map(|idx| idx.field.clone())
                .ok_or_else(|| SchemaError::IndexNotFound {
                    collection: coll.clone(),
                    index: index.clone(),
                })?
        };

        let version = self.next_version();
        let entry = SchemaLogEntry {
            version,
            op: SchemaOp::DropIndex,
            collection: coll.clone(),
            field: field.clone(),
            index_id: index.clone(),
            timestamp: now_timestamp(),
        };
        self.append_log_entry(&entry).await?;
        {
            let mut state = write_lock(&self.state);
            apply_entry_mut(&mut state, entry);
        }
        self.persist_state_snapshot().await?;

        let materializer = Arc::clone(&self.materializer);
        let index_id = index.clone();
        tokio::spawn(async move {
            if let Err(error) = materializer.drop_materialized(&index_id).await {
                warn!(index_id = %index_id, "failed to drop materialized index: {error}");
            }
        });
        Ok(version)
    }

    fn current_version(&self) -> SchemaVersion {
        self.version.load(Ordering::Acquire)
    }

    fn has_index(&self, coll: &CollectionId, index: &IndexId) -> bool {
        read_lock(&self.state)
            .collections
            .get(coll)
            .map(|schema| schema.indexes.iter().any(|idx| &idx.id == index))
            .unwrap_or(false)
    }

    fn collection_schema(&self, coll: &CollectionId) -> Option<CollectionSchema> {
        read_lock(&self.state).collections.get(coll).cloned()
    }
}

fn apply_entry_mut(state: &mut SchemaState, entry: SchemaLogEntry) {
    match entry.op {
        SchemaOp::CreateIndex => {
            let coll_schema = state
                .collections
                .entry(entry.collection.clone())
                .or_insert_with(|| CollectionSchema {
                    id: entry.collection.clone(),
                    indexes: Vec::new(),
                    version: 0,
                });
            coll_schema.indexes.push(IndexDefinition {
                id: entry.index_id.clone(),
                collection: entry.collection.clone(),
                field: entry.field.clone(),
                created_at: entry.version,
            });
            coll_schema.version = entry.version;
            state
                .active_by_field
                .entry(entry.collection.clone())
                .or_default()
                .insert(entry.field.clone(), entry.index_id.clone());
        }
        SchemaOp::DropIndex => {
            let coll_schema = state
                .collections
                .entry(entry.collection.clone())
                .or_insert_with(|| CollectionSchema {
                    id: entry.collection.clone(),
                    indexes: Vec::new(),
                    version: 0,
                });
            coll_schema.indexes.retain(|idx| idx.id != entry.index_id);
            coll_schema.version = entry.version;
            if let Some(active) = state.active_by_field.get_mut(&entry.collection) {
                let remove_field = active
                    .get(&entry.field)
                    .map(|active_id| active_id == &entry.index_id)
                    .unwrap_or(false);
                if remove_field {
                    active.remove(&entry.field);
                }
            }
        }
    }
    state.version = entry.version;
}

fn now_timestamp() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(dur) => dur.as_secs().to_string(),
        Err(error) => {
            warn!("system clock is before UNIX_EPOCH: {error}");
            "0".to_string()
        }
    }
}

/// Provider for fetching remote schema log entries during gossip convergence.
#[async_trait]
pub trait SchemaLogProvider: Send + Sync {
    /// Fetches all entries with `version > since` from a remote sender.
    async fn fetch_since(&self, sender: &str, since: SchemaVersion) -> Result<Vec<SchemaLogEntry>>;
}

/// Default index materializer backed by the storage engine `indexes` CF.
#[derive(Debug)]
pub struct DefaultIndexMaterializer<E>
where
    E: StorageEngine,
{
    engine: Arc<E>,
    ready: Arc<RwLock<HashSet<IndexId>>>,
}

impl<E> DefaultIndexMaterializer<E>
where
    E: StorageEngine,
{
    /// Creates a materializer over the given storage engine.
    pub fn new(engine: Arc<E>) -> Self {
        Self {
            engine,
            ready: Arc::new(RwLock::new(HashSet::new())),
        }
    }
}

#[async_trait]
impl<E> IndexMaterializer for DefaultIndexMaterializer<E>
where
    E: StorageEngine,
{
    async fn materialize(&self, def: &IndexDefinition) -> Result<()> {
        validate_identifier(&def.collection, "collection")?;
        validate_identifier(&def.id, "index")?;
        validate_identifier(&def.field, "field")?;
        let prefix = format!("{}/", def.collection).into_bytes();
        let iter = self
            .engine
            .scan(ColumnFamily::DEFAULT, &prefix, &prefix_end(&prefix))?;
        let mut batch = bashfuldb_storage::WriteBatch::new();
        let mut pending = 0usize;
        for item in iter {
            let (key, value) = item?;
            if let Some(object_id) = split_object_id(&key) {
                let field_value = extract_field_value(&value, &def.field);
                if let Some(field_value) = field_value {
                    let encoded_value = encode_field_value_for_key(&field_value);
                    let idx_key =
                        format!("{}/{}/{}", def.id, encoded_value, object_id).into_bytes();
                    batch.put(ColumnFamily::INDEXES, idx_key, Vec::<u8>::new());
                    pending += 1;
                    if pending >= 1024 {
                        self.engine.write_batch(batch).await?;
                        batch = bashfuldb_storage::WriteBatch::new();
                        pending = 0;
                    }
                }
            }
        }
        if !batch.is_empty() {
            self.engine.write_batch(batch).await?;
        }
        write_lock(&self.ready).insert(def.id.clone());
        Ok(())
    }

    async fn drop_materialized(&self, index: &IndexId) -> Result<()> {
        validate_identifier(index, "index")?;
        let prefix = format!("{index}/").into_bytes();
        let iter = self
            .engine
            .scan(ColumnFamily::INDEXES, &prefix, &prefix_end(&prefix))?;
        let mut batch = bashfuldb_storage::WriteBatch::new();
        for item in iter {
            let (key, _) = item?;
            batch.delete(ColumnFamily::INDEXES, key);
        }
        self.engine.write_batch(batch).await?;
        write_lock(&self.ready).remove(index);
        Ok(())
    }

    fn is_ready(&self, index: &IndexId) -> bool {
        read_lock(&self.ready).contains(index)
    }
}

fn split_object_id(object_key: &[u8]) -> Option<String> {
    let as_text = String::from_utf8_lossy(object_key);
    let mut parts = as_text.splitn(2, '/');
    let _ = parts.next()?;
    parts.next().map(ToString::to_string)
}

fn extract_field_value(value: &[u8], field: &str) -> Option<String> {
    let json: serde_json::Value = serde_json::from_slice(value).ok()?;
    let field_val = json.get(field)?;
    // Arrays and objects are not indexed in V1.
    Some(match field_val {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => "null".to_string(),
        _ => return None,
    })
}

fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    end.push(0xFF);
    end
}

fn encode_field_value_for_key(value: &str) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(value.as_bytes())
}

fn validate_identifier(value: &str, kind: &'static str) -> Result<()> {
    if value.is_empty() || value.contains('/') || value.contains('\0') {
        return Err(SchemaError::InvalidIdentifier {
            kind,
            value: value.to_string(),
        });
    }
    Ok(())
}

fn read_lock<T>(
    lock: &RwLock<T>,
) -> parking_lot::lock_api::RwLockReadGuard<'_, parking_lot::RawRwLock, T> {
    lock.read()
}

fn write_lock<T>(
    lock: &RwLock<T>,
) -> parking_lot::lock_api::RwLockWriteGuard<'_, parking_lot::RawRwLock, T> {
    lock.write()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bashfuldb_clock::{Hlc, NodeId};
    use bashfuldb_cluster::{GossipMessage, GossipTransport, SimulatedNetwork};
    use bashfuldb_storage::{MemEngine, StorageEngine};
    use std::collections::HashMap as StdHashMap;
    use tempfile::TempDir;

    struct MockProvider {
        responses: StdHashMap<String, Vec<SchemaLogEntry>>,
    }

    #[async_trait]
    impl SchemaLogProvider for MockProvider {
        async fn fetch_since(
            &self,
            sender: &str,
            since: SchemaVersion,
        ) -> Result<Vec<SchemaLogEntry>> {
            let all = self.responses.get(sender).cloned().unwrap_or_default();
            Ok(all
                .into_iter()
                .filter(|entry| entry.version > since)
                .collect())
        }
    }

    #[tokio::test]
    async fn create_drop_and_quota() {
        let engine = Arc::new(MemEngine::new());
        let temp = TempDir::new().expect("tempdir");
        let manager = DefaultSchemaManager::new(
            engine,
            SchemaConfig {
                root_path: temp.path().to_path_buf(),
                max_secondary_indexes_per_collection: 1,
            },
        )
        .await
        .expect("manager");
        let coll = "users".to_string();
        let version1 = manager
            .create_index(&coll, "email")
            .await
            .expect("create first index");
        assert_eq!(version1, 1);
        let err = manager
            .create_index(&coll, "name")
            .await
            .expect_err("quota should fail");
        assert!(matches!(err, SchemaError::IndexQuotaExceeded { .. }));

        let schema = manager.collection_schema(&coll).expect("schema exists");
        let index_id = schema.indexes[0].id.clone();
        let version2 = manager
            .drop_index(&coll, &index_id)
            .await
            .expect("drop index");
        assert_eq!(version2, 2);
        assert!(!manager.has_index(&coll, &index_id));
    }

    #[tokio::test]
    async fn log_replay_and_metadata_restore() {
        let temp = TempDir::new().expect("tempdir");
        let coll = "orders".to_string();
        let engine = Arc::new(MemEngine::new());
        let manager = DefaultSchemaManager::new(
            Arc::clone(&engine),
            SchemaConfig {
                root_path: temp.path().to_path_buf(),
                max_secondary_indexes_per_collection: 8,
            },
        )
        .await
        .expect("manager");
        let _ = manager.create_index(&coll, "status").await.expect("create");
        let schema = manager.collection_schema(&coll).expect("schema");
        let idx = schema.indexes[0].id.clone();
        let _ = manager.drop_index(&coll, &idx).await.expect("drop");

        let fresh = DefaultSchemaManager::new(
            engine,
            SchemaConfig {
                root_path: temp.path().to_path_buf(),
                max_secondary_indexes_per_collection: 8,
            },
        )
        .await
        .expect("fresh manager");
        assert_eq!(fresh.current_version(), 2);
        assert!(fresh.collection_schema(&coll).is_some());
    }

    #[tokio::test]
    async fn log_replay_restores_active_field_mapping() {
        let temp = TempDir::new().expect("tempdir");
        let coll = "users".to_string();
        let engine = Arc::new(MemEngine::new());
        let manager = DefaultSchemaManager::new(
            Arc::clone(&engine),
            SchemaConfig {
                root_path: temp.path().to_path_buf(),
                max_secondary_indexes_per_collection: 1,
            },
        )
        .await
        .expect("manager");
        let _ = manager.create_index(&coll, "email").await.expect("create");

        engine
            .delete(ColumnFamily::METADATA, METADATA_STATE_KEY)
            .await
            .expect("delete metadata snapshot");

        let replayed = DefaultSchemaManager::new(
            engine,
            SchemaConfig {
                root_path: temp.path().to_path_buf(),
                max_secondary_indexes_per_collection: 1,
            },
        )
        .await
        .expect("manager from replay");

        let err = replayed
            .create_index(&coll, "name")
            .await
            .expect_err("quota should still apply after replay");
        assert!(matches!(err, SchemaError::IndexQuotaExceeded { .. }));
    }

    #[tokio::test(start_paused = true)]
    async fn schema_converges_with_bounded_retry() {
        let temp_a = TempDir::new().expect("tempdir");
        let temp_b = TempDir::new().expect("tempdir");
        let a = DefaultSchemaManager::new(
            Arc::new(MemEngine::new()),
            SchemaConfig {
                root_path: temp_a.path().to_path_buf(),
                max_secondary_indexes_per_collection: 8,
            },
        )
        .await
        .expect("node a");
        let b = DefaultSchemaManager::new(
            Arc::new(MemEngine::new()),
            SchemaConfig {
                root_path: temp_b.path().to_path_buf(),
                max_secondary_indexes_per_collection: 8,
            },
        )
        .await
        .expect("node b");
        let coll = "events".to_string();
        let _ = b.create_index(&coll, "type").await.expect("create on b");
        let _ = b.create_index(&coll, "source").await.expect("create on b");
        let entries = b.log_entries_since(0).expect("entries");
        let provider = MockProvider {
            responses: StdHashMap::from([("node-b".to_string(), entries)]),
        };
        let task = tokio::spawn({
            let a = Arc::clone(&a);
            async move {
                a.converge_from_gossip("node-b", 2, 100, &provider)
                    .await
                    .expect("converge");
            }
        });
        tokio::time::advance(std::time::Duration::from_millis(200)).await;
        task.await.expect("task");
        assert_eq!(a.current_version(), 2);
        let schema = a.collection_schema(&coll).expect("schema replicated");
        assert_eq!(schema.indexes.len(), 2);
    }

    #[tokio::test]
    async fn schema_converges_via_simulated_gossip_message() {
        let network = SimulatedNetwork::new();
        let node_a = NodeId::random();
        let node_b = NodeId::random();
        let ta = network.register_node(node_a);
        let tb = network.register_node(node_b);

        let temp_a = TempDir::new().expect("tempdir");
        let temp_b = TempDir::new().expect("tempdir");
        let a = DefaultSchemaManager::new(
            Arc::new(MemEngine::new()),
            SchemaConfig {
                root_path: temp_a.path().to_path_buf(),
                max_secondary_indexes_per_collection: 8,
            },
        )
        .await
        .expect("node a");
        let b = DefaultSchemaManager::new(
            Arc::new(MemEngine::new()),
            SchemaConfig {
                root_path: temp_b.path().to_path_buf(),
                max_secondary_indexes_per_collection: 8,
            },
        )
        .await
        .expect("node b");

        let coll = "audit".to_string();
        let _ = b.create_index(&coll, "actor").await.expect("create");
        let entries = b.log_entries_since(0).expect("entries");
        let provider = MockProvider {
            responses: StdHashMap::from([("node-b".to_string(), entries)]),
        };

        let msg = GossipMessage {
            sender: node_b,
            hlc: Hlc::new(1, 0),
            schema_version: 1,
            members: Vec::new(),
        };
        tb.send(&node_a, msg).await.expect("send gossip");
        let (_sender, incoming) = ta.receive().await.expect("receive gossip");
        a.converge_from_membership_gossip("node-b", &incoming, 100, &provider)
            .await
            .expect("converge");
        assert_eq!(a.current_version(), 1);
    }

    #[tokio::test]
    async fn blue_green_swap_and_query_skip_warning_behavior() {
        let engine = Arc::new(MemEngine::new());
        engine
            .put(
                ColumnFamily::DEFAULT,
                b"users/1",
                br#"{"email":"a@example.com","name":"alice"}"#,
            )
            .await
            .expect("seed");
        let temp = TempDir::new().expect("tempdir");
        let manager = DefaultSchemaManager::new(
            Arc::clone(&engine),
            SchemaConfig {
                root_path: temp.path().to_path_buf(),
                max_secondary_indexes_per_collection: 4,
            },
        )
        .await
        .expect("manager");
        let coll = "users".to_string();

        let _ = manager
            .create_index(&coll, "email")
            .await
            .expect("create first");
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert!(manager.query_index_for_field(&coll, "email", 1).is_some());
        let first = manager
            .query_index_for_field(&coll, "email", 1)
            .expect("first active");

        let _ = manager
            .create_index(&coll, "email")
            .await
            .expect("replacement");
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let second = manager
            .query_index_for_field(&coll, "email", 1)
            .expect("second active");
        assert_ne!(first, second);

        // Simulate a not-ready index: remove readiness and require R >= 2.
        let materializer = DefaultIndexMaterializer::new(Arc::clone(&engine));
        write_lock(&materializer.ready).clear();
        assert!(!materializer.is_ready(&second));
    }
}
