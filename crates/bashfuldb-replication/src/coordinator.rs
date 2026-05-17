use crate::{
    idempotency::IdempotencyStore, ObjectKey, QuorumConfig, ReadResult, ReplicationError,
    ReplicationTransport, Result, VersionedDoc, WriteRequest, WriteResult,
};
use async_trait::async_trait;
use bashfuldb_clock::{CausalOrder, Clock, NodeId, VectorClock};
use bashfuldb_cluster::Membership;
use bashfuldb_document::Document;
use std::sync::Arc;
use tokio::sync::Mutex;

// ── Coordinator trait ─────────────────────────────────────────────────────────

/// Per-request coordinator interface.
///
/// Any node may act as coordinator for any request; there is no dedicated
/// coordinator node.
#[async_trait]
pub trait Coordinator: Send + Sync {
    /// Reads a document using quorum reads, performing read-repair on stale
    /// replicas.
    async fn quorum_read(&self, key: &ObjectKey) -> Result<ReadResult>;

    /// Writes a document using quorum writes, falling back to sloppy quorum
    /// with hinted handoff when fewer than W owner replicas are reachable.
    async fn quorum_write(
        &self,
        key: &ObjectKey,
        doc: &Document,
        idempotency_key: Option<&str>,
    ) -> Result<WriteResult>;

    /// Deletes a document (writes a tombstone) using quorum writes.
    ///
    /// Delete semantics take priority over concurrent updates (delete wins).
    async fn quorum_delete(&self, key: &ObjectKey) -> Result<WriteResult>;
}

// ── QuorumCoordinator ─────────────────────────────────────────────────────────

/// Default `Coordinator` implementation.
///
/// Implements quorum reads / writes, sloppy quorum with hinted handoff,
/// read repair, and idempotency key management.
pub struct QuorumCoordinator {
    local_node: NodeId,
    membership: Arc<dyn Membership>,
    transport: Arc<dyn ReplicationTransport>,
    clock: Arc<dyn Clock>,
    config: QuorumConfig,
    idempotency: Arc<Mutex<IdempotencyStore>>,
}

impl QuorumCoordinator {
    /// Creates a new coordinator.
    pub fn new(
        local_node: NodeId,
        membership: Arc<dyn Membership>,
        transport: Arc<dyn ReplicationTransport>,
        clock: Arc<dyn Clock>,
        config: QuorumConfig,
    ) -> Self {
        Self {
            local_node,
            membership,
            transport,
            clock,
            config,
            idempotency: Arc::new(Mutex::new(IdempotencyStore::new())),
        }
    }
}

#[async_trait]
impl Coordinator for QuorumCoordinator {
    // ── quorum_read ───────────────────────────────────────────────────────────

    async fn quorum_read(&self, key: &ObjectKey) -> Result<ReadResult> {
        let key_bytes = key.to_bytes();
        let owners = self.membership.owners_for_key(&key_bytes, self.config.n);

        if owners.is_empty() {
            return Err(ReplicationError::InsufficientReplicas {
                needed: self.config.r,
                available: 0,
            });
        }

        // Fan-out reads to all N owners.
        let mut join_set = tokio::task::JoinSet::new();
        for owner in owners.clone() {
            let transport = Arc::clone(&self.transport);
            let req = crate::ReadRequest { key: key.clone() };
            join_set.spawn(async move {
                let result = transport.remote_read(&owner, req).await;
                (owner, result)
            });
        }

        // Collect up to N responses; we need at least R.
        let mut responses: Vec<(NodeId, Option<VersionedDoc>)> = Vec::new();
        while let Some(task_result) = join_set.join_next().await {
            match task_result {
                Ok((node, Ok(resp))) => responses.push((node, resp.doc)),
                Ok((_, Err(e))) => tracing::debug!(error = %e, "read replica unavailable"),
                Err(e) => tracing::warn!(error = %e, "read task panicked"),
            }
        }

        let r_count = responses.len();
        if r_count < self.config.r {
            return Err(ReplicationError::InsufficientReplicas {
                needed: self.config.r,
                available: r_count,
            });
        }

        // Find the winning version among all responses.
        let docs: Vec<VersionedDoc> = responses
            .iter()
            .filter_map(|(_, d)| d.clone())
            .collect();

        if docs.is_empty() {
            return Ok(ReadResult {
                document: None,
                version: VectorClock::new(),
                repairs_triggered: 0,
            });
        }

        let winner = resolve_conflict(docs);

        // Identify stale replicas and trigger async read repair.
        let stale_nodes: Vec<NodeId> = responses
            .iter()
            .filter_map(|(node, doc)| {
                let order = match doc {
                    None => CausalOrder::Before,
                    Some(vdoc) => vdoc.version.compare(&winner.version),
                };
                match order {
                    CausalOrder::Before | CausalOrder::Concurrent => Some(*node),
                    CausalOrder::Equal | CausalOrder::After => None,
                }
            })
            .collect();

        let repairs_triggered = stale_nodes.len();
        if !stale_nodes.is_empty() {
            let transport = Arc::clone(&self.transport);
            let winner_clone = winner.clone();
            let key_clone = key.clone();
            tokio::spawn(async move {
                for node in stale_nodes {
                    let repair_req = WriteRequest {
                        key: key_clone.clone(),
                        doc: winner_clone.document.clone(),
                        version: winner_clone.version.clone(),
                        hlc: winner_clone.hlc,
                        is_hint: false,
                        hint_target: None,
                    };
                    if let Err(e) = transport.remote_write(&node, repair_req).await {
                        tracing::debug!(error = %e, %node, "read repair write failed");
                    }
                }
            });
        }

        Ok(ReadResult {
            document: winner.document,
            version: winner.version,
            repairs_triggered,
        })
    }

    // ── quorum_write ──────────────────────────────────────────────────────────

    async fn quorum_write(
        &self,
        key: &ObjectKey,
        doc: &Document,
        idempotency_key: Option<&str>,
    ) -> Result<WriteResult> {
        // 1. Idempotency check.
        if let Some(ikey) = idempotency_key {
            let mut cache = self.idempotency.lock().await;
            if let Some(cached) = cache.get(ikey, doc)? {
                return Ok(cached);
            }
        }

        // 2. Determine N owner nodes.
        let key_bytes = key.to_bytes();
        let owners = self.membership.owners_for_key(&key_bytes, self.config.n);

        // 3. Stamp with HLC + new vector clock.
        let hlc = self.clock.tick()?;
        let mut version = VectorClock::new();
        version
            .increment(self.local_node, hlc)
            .map_err(ReplicationError::VectorClock)?;

        let req = WriteRequest {
            key: key.clone(),
            doc: Some(doc.clone()),
            version: version.clone(),
            hlc,
            is_hint: false,
            hint_target: None,
        };

        // 4. Fan-out writes to all N owners.
        let (acked_owners, failed_owners) = broadcast_write(&self.transport, &owners, req).await;
        let ack_count = acked_owners.len();

        if ack_count >= self.config.w {
            let result = WriteResult {
                version: version.clone(),
                nodes_acked: ack_count,
                hints_stored: 0,
            };
            if let Some(ikey) = idempotency_key {
                let mut cache = self.idempotency.lock().await;
                let _ = cache.store(ikey.to_string(), doc, result.clone());
            }
            return Ok(result);
        }

        // 5. Sloppy quorum: write hints to available non-owner nodes.
        let hints_stored = self
            .store_sloppy_hints(key, Some(doc), &version, hlc, &owners, &failed_owners)
            .await;

        let extra_acks = hints_stored;
        let total = ack_count + extra_acks;
        if total >= self.config.w {
            let result = WriteResult {
                version: version.clone(),
                nodes_acked: ack_count,
                hints_stored,
            };
            if let Some(ikey) = idempotency_key {
                let mut cache = self.idempotency.lock().await;
                let _ = cache.store(ikey.to_string(), doc, result.clone());
            }
            Ok(result)
        } else {
            Err(ReplicationError::InsufficientReplicas {
                needed: self.config.w,
                available: total,
            })
        }
    }

    // ── quorum_delete ─────────────────────────────────────────────────────────

    async fn quorum_delete(&self, key: &ObjectKey) -> Result<WriteResult> {
        let key_bytes = key.to_bytes();
        let owners = self.membership.owners_for_key(&key_bytes, self.config.n);

        let hlc = self.clock.tick()?;
        let mut version = VectorClock::new();
        version
            .increment(self.local_node, hlc)
            .map_err(ReplicationError::VectorClock)?;

        // Tombstone: doc = None.
        let req = WriteRequest {
            key: key.clone(),
            doc: None,
            version: version.clone(),
            hlc,
            is_hint: false,
            hint_target: None,
        };

        let (acked_owners, failed_owners) = broadcast_write(&self.transport, &owners, req).await;
        let ack_count = acked_owners.len();

        if ack_count >= self.config.w {
            return Ok(WriteResult {
                version,
                nodes_acked: ack_count,
                hints_stored: 0,
            });
        }

        let hints_stored = self
            .store_sloppy_hints(key, None, &version, hlc, &owners, &failed_owners)
            .await;

        let total = ack_count + hints_stored;
        if total >= self.config.w {
            Ok(WriteResult {
                version,
                nodes_acked: ack_count,
                hints_stored,
            })
        } else {
            Err(ReplicationError::InsufficientReplicas {
                needed: self.config.w,
                available: total,
            })
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

impl QuorumCoordinator {
    /// Sends hint writes to non-owner fallback nodes for each unreachable owner.
    ///
    /// Returns the number of successful hint stores (each counts as one
    /// additional "ack" toward the write quorum under sloppy quorum semantics).
    async fn store_sloppy_hints(
        &self,
        key: &ObjectKey,
        doc: Option<&Document>,
        version: &VectorClock,
        hlc: bashfuldb_clock::Hlc,
        original_owners: &[NodeId],
        failed_owners: &[NodeId],
    ) -> usize {
        if failed_owners.is_empty() {
            return 0;
        }

        let all_nodes = self.membership.all_nodes();
        // Candidate fallback nodes: not one of the original N owners.
        let fallbacks: Vec<NodeId> = all_nodes
            .into_iter()
            .filter(|n| !original_owners.contains(n))
            .collect();

        let needed = failed_owners.len().min(self.config.w);
        let mut stored = 0usize;
        let mut fallback_iter = fallbacks.iter();

        for &failed_owner in failed_owners.iter().take(needed) {
            let Some(&fallback_node) = fallback_iter.next() else {
                break;
            };
            let hint_req = WriteRequest {
                key: key.clone(),
                doc: doc.cloned(),
                version: version.clone(),
                hlc,
                is_hint: true,
                hint_target: Some(failed_owner),
            };
            if self
                .transport
                .remote_write(&fallback_node, hint_req)
                .await
                .is_ok()
            {
                stored += 1;
            }
        }
        stored
    }
}

/// Fans a write out to all nodes in `targets`, returning `(acked, failed)`.
async fn broadcast_write(
    transport: &Arc<dyn ReplicationTransport>,
    targets: &[NodeId],
    req: WriteRequest,
) -> (Vec<NodeId>, Vec<NodeId>) {
    let mut join_set = tokio::task::JoinSet::new();
    for &node in targets {
        let t = Arc::clone(transport);
        let r = req.clone();
        join_set.spawn(async move {
            let result = t.remote_write(&node, r).await;
            (node, result)
        });
    }

    let mut acked = Vec::new();
    let mut failed = Vec::new();
    while let Some(task) = join_set.join_next().await {
        match task {
            Ok((node, Ok(_))) => acked.push(node),
            Ok((node, Err(e))) => {
                tracing::debug!(error = %e, %node, "write replica failed");
                failed.push(node);
            }
            Err(e) => tracing::warn!(error = %e, "write task panicked"),
        }
    }
    (acked, failed)
}

// ── Conflict resolution ───────────────────────────────────────────────────────

/// Resolves a non-empty list of versioned documents to a single winner.
///
/// Rules (in priority order):
/// 1. Causally-later version wins outright.
/// 2. On concurrent conflict: tombstone (delete) wins over a live document.
/// 3. On tie (both tombstones or both documents): LWW by HLC.
pub fn resolve_conflict(mut docs: Vec<VersionedDoc>) -> VersionedDoc {
    debug_assert!(!docs.is_empty());
    let mut winner = docs.remove(0);
    for candidate in docs {
        winner = resolve_two(winner, candidate);
    }
    winner
}

fn resolve_two(a: VersionedDoc, b: VersionedDoc) -> VersionedDoc {
    match a.version.compare(&b.version) {
        CausalOrder::After => a,
        CausalOrder::Before => b,
        CausalOrder::Equal => a,
        CausalOrder::Concurrent => {
            // Delete wins over update.
            match (a.is_tombstone, b.is_tombstone) {
                (true, false) => a,
                (false, true) => b,
                // Both same type → LWW (higher HLC wins; ties go to `a`).
                _ => {
                    if a.hlc >= b.hlc {
                        a
                    } else {
                        b
                    }
                }
            }
        }
    }
}
