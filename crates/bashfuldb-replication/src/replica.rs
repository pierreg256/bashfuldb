use crate::{
    ReadRequest, ReadResponse, ReplicationError, Result, VersionedDoc, WriteRequest,
    WriteResponse,
};
use bashfuldb_clock::{Hlc, NodeId};
use bashfuldb_storage::{ColumnFamily, StorageEngine};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Value stored in the `hints` column family.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct HintValue {
    /// The document key the hint should be delivered to.
    pub key: crate::ObjectKey,
    /// The versioned document payload to deliver.
    pub versioned: VersionedDoc,
}

/// Per-node local replica: handles incoming replication RPCs and persists
/// documents in the node's `StorageEngine`.
///
/// Each physical node in the cluster owns one `ReplicaStore`.
pub struct ReplicaStore {
    node_id: NodeId,
    storage: Arc<dyn StorageEngine>,
}

impl ReplicaStore {
    /// Creates a new replica store backed by the given storage engine.
    pub fn new(node_id: NodeId, storage: Arc<dyn StorageEngine>) -> Self {
        Self { node_id, storage }
    }

    /// Returns the node ID this store belongs to.
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Returns a reference to the underlying storage engine.
    pub fn storage(&self) -> &Arc<dyn StorageEngine> {
        &self.storage
    }

    /// Handles an incoming write RPC.
    ///
    /// If `req.is_hint` is true the document is stored in the `hints` CF with
    /// the key format `{target_node_uuid}/{hlc_u64}/{object_id_uuid}`.
    /// Otherwise the document is written to the `default` CF.
    pub async fn handle_write(&self, req: WriteRequest) -> Result<WriteResponse> {
        if req.is_hint {
            let target = req.hint_target.ok_or_else(|| ReplicationError::TransportError {
                message: "hint write missing hint_target".into(),
            })?;
            self.store_hint(target, &req).await?;
            return Ok(WriteResponse {
                version: req.version,
            });
        }

        let key = req.key.to_bytes();
        let versioned = VersionedDoc {
            document: req.doc,
            version: req.version.clone(),
            hlc: req.hlc,
            is_tombstone: false,
        };
        let value = serde_json::to_vec(&versioned)?;
        self.storage
            .put(ColumnFamily::DEFAULT, &key, &value)
            .await?;
        Ok(WriteResponse {
            version: req.version,
        })
    }

    /// Handles an incoming delete (tombstone) RPC.
    pub async fn handle_delete(&self, req: WriteRequest) -> Result<WriteResponse> {
        if req.is_hint {
            let target = req.hint_target.ok_or_else(|| ReplicationError::TransportError {
                message: "hint delete missing hint_target".into(),
            })?;
            self.store_hint(target, &req).await?;
            return Ok(WriteResponse {
                version: req.version,
            });
        }

        let key = req.key.to_bytes();
        let versioned = VersionedDoc::tombstone(req.version.clone(), req.hlc);
        let value = serde_json::to_vec(&versioned)?;
        self.storage
            .put(ColumnFamily::DEFAULT, &key, &value)
            .await?;
        Ok(WriteResponse {
            version: req.version,
        })
    }

    /// Handles an incoming read RPC.
    pub async fn handle_read(&self, req: ReadRequest) -> Result<ReadResponse> {
        let key = req.key.to_bytes();
        let raw = self.storage.get(ColumnFamily::DEFAULT, &key).await?;
        let doc = match raw {
            None => None,
            Some(bytes) => Some(serde_json::from_slice::<VersionedDoc>(&bytes)?),
        };
        Ok(ReadResponse { doc })
    }

    /// Stores a hint in the `hints` CF.
    async fn store_hint(&self, target: NodeId, req: &WriteRequest) -> Result<()> {
        let hint_key = make_hint_key(target, req.hlc, &req.key.object_id);
        let hint_value = HintValue {
            key: req.key.clone(),
            versioned: VersionedDoc {
                document: req.doc.clone(),
                version: req.version.clone(),
                hlc: req.hlc,
                is_tombstone: req.doc.is_none(),
            },
        };
        let bytes = serde_json::to_vec(&hint_value)?;
        self.storage
            .put(ColumnFamily::HINTS, &hint_key, &bytes)
            .await?;
        Ok(())
    }

    /// Scans the `hints` CF, attempts to deliver each hint to its target node
    /// via the supplied transport, and deletes successfully delivered (or
    /// expired) hints.
    ///
    /// Returns the number of hints delivered.
    pub async fn deliver_pending_hints(
        &self,
        transport: &dyn crate::ReplicationTransport,
    ) -> Result<usize> {
        /// Hints older than 24 hours are discarded (24 h × 3600 s × 1000 ms).
        const HINT_TTL_MS: u64 = 24 * 3600 * 1000;

        let now_ms = current_wall_ms();
        let iter = self.storage.scan(ColumnFamily::HINTS, &[], &[])?;

        let mut delivered = 0usize;
        for item in iter {
            let (raw_key, raw_val) = item?;

            // Parse key: "{target_uuid}/{hlc_u64}/{object_id_uuid}"
            let key_str = String::from_utf8_lossy(&raw_key);
            let parts: Vec<&str> = key_str.splitn(3, '/').collect();
            if parts.len() != 3 {
                // Malformed key — delete and skip
                self.storage.delete(ColumnFamily::HINTS, &raw_key).await?;
                continue;
            }
            let target_node: NodeId = match parts[0].parse() {
                Ok(n) => n,
                Err(_) => {
                    self.storage.delete(ColumnFamily::HINTS, &raw_key).await?;
                    continue;
                }
            };
            let hlc_u64: u64 = match parts[1].parse() {
                Ok(v) => v,
                Err(_) => {
                    self.storage.delete(ColumnFamily::HINTS, &raw_key).await?;
                    continue;
                }
            };
            let hlc = Hlc::from_u64(hlc_u64);

            // TTL check
            if now_ms.saturating_sub(hlc.physical_ms()) > HINT_TTL_MS {
                tracing::debug!(%target_node, "discarding expired hint");
                self.storage.delete(ColumnFamily::HINTS, &raw_key).await?;
                continue;
            }

            // Attempt delivery
            let hint: HintValue = match serde_json::from_slice(&raw_val) {
                Ok(h) => h,
                Err(_) => {
                    self.storage.delete(ColumnFamily::HINTS, &raw_key).await?;
                    continue;
                }
            };

            let write_req = WriteRequest {
                key: hint.key,
                doc: hint.versioned.document,
                version: hint.versioned.version,
                hlc: hint.versioned.hlc,
                is_hint: false,
                hint_target: None,
            };
            match transport.remote_write(&target_node, write_req).await {
                Ok(_) => {
                    self.storage.delete(ColumnFamily::HINTS, &raw_key).await?;
                    delivered += 1;
                }
                Err(e) => {
                    tracing::debug!(%target_node, error = %e, "hint delivery failed (will retry)");
                }
            }
        }
        Ok(delivered)
    }
}

/// Builds the hint key: `{target_node}/{hlc_u64}/{object_id_uuid}`.
pub(crate) fn make_hint_key(
    target: NodeId,
    hlc: Hlc,
    object_id: &bashfuldb_document::ObjectId,
) -> Vec<u8> {
    format!("{}/{}/{}", target, hlc.to_u64(), object_id).into_bytes()
}

fn current_wall_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
