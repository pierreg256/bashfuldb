//! Simulation helpers for deterministic replication tests.
//!
//! Provides an in-memory [`SimulatedReplicationNetwork`] and its
//! [`SimulatedTransport`] that allow tests to:
//!
//! * Register per-node [`ReplicaStore`]s.
//! * Inject network partitions and node failures.
//! * Wire together a full multi-node cluster without real networking.
//!
//! Also exports [`SimpleMembership`] — a minimal [`Membership`] implementation
//! backed by a [`Ring`].

use crate::{
    ReadRequest, ReadResponse, ReplicationError, ReplicationTransport, ReplicaStore, Result,
    WriteRequest, WriteResponse,
};
use async_trait::async_trait;
use bashfuldb_clock::NodeId;
use bashfuldb_cluster::{
    Membership, MembershipEvent, NodeInfo, NodeState, Ring,
};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

// ── SimulatedReplicationNetwork ───────────────────────────────────────────────

struct NetworkState {
    nodes: HashMap<NodeId, Arc<ReplicaStore>>,
    /// Directed set of partitioned links (a→b means a cannot reach b).
    partitions: HashSet<(NodeId, NodeId)>,
    /// Nodes that are entirely offline.
    down: HashSet<NodeId>,
}

/// Shared in-memory network for multi-node simulation tests.
#[derive(Clone)]
pub struct SimulatedReplicationNetwork {
    inner: Arc<Mutex<NetworkState>>,
}

impl Default for SimulatedReplicationNetwork {
    fn default() -> Self {
        Self::new()
    }
}

impl SimulatedReplicationNetwork {
    /// Creates an empty simulation network.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(NetworkState {
                nodes: HashMap::new(),
                partitions: HashSet::new(),
                down: HashSet::new(),
            })),
        }
    }

    /// Registers a node and its replica store.
    pub fn register_node(&self, store: Arc<ReplicaStore>) {
        let node_id = store.node_id();
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .nodes
            .insert(node_id, store);
    }

    /// Returns a [`SimulatedTransport`] that routes RPCs as `from_node`.
    pub fn transport_for(&self, from_node: NodeId) -> SimulatedTransport {
        SimulatedTransport {
            from: from_node,
            network: self.clone(),
        }
    }

    /// Creates a bidirectional partition between two nodes.
    pub fn partition(&self, a: NodeId, b: NodeId) {
        let mut st = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        st.partitions.insert((a, b));
        st.partitions.insert((b, a));
    }

    /// Removes a bidirectional partition.
    pub fn heal_partition(&self, a: NodeId, b: NodeId) {
        let mut st = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        st.partitions.remove(&(a, b));
        st.partitions.remove(&(b, a));
    }

    /// Marks a node as down (all RPCs to/from it fail).
    pub fn set_node_down(&self, node: NodeId) {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .down
            .insert(node);
    }

    /// Marks a node as back up.
    pub fn set_node_up(&self, node: NodeId) {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .down
            .remove(&node);
    }

    /// Returns a snapshot of all registered node IDs.
    pub fn all_nodes(&self) -> Vec<NodeId> {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .nodes
            .keys()
            .copied()
            .collect()
    }

    fn resolve_target(&self, from: NodeId, target: NodeId) -> Result<Arc<ReplicaStore>> {
        let st = self.inner.lock().unwrap_or_else(|p| p.into_inner());

        if st.down.contains(&target) {
            return Err(ReplicationError::TransportError {
                message: format!("{target} is down"),
            });
        }
        if st.partitions.contains(&(from, target)) {
            return Err(ReplicationError::TransportError {
                message: format!("{from} is partitioned from {target}"),
            });
        }
        st.nodes
            .get(&target)
            .cloned()
            .ok_or_else(|| ReplicationError::TransportError {
                message: format!("unknown node {target}"),
            })
    }
}

// ── SimulatedTransport ────────────────────────────────────────────────────────

/// Per-node transport endpoint for [`SimulatedReplicationNetwork`].
pub struct SimulatedTransport {
    from: NodeId,
    network: SimulatedReplicationNetwork,
}

#[async_trait]
impl ReplicationTransport for SimulatedTransport {
    async fn remote_write(&self, target: &NodeId, req: WriteRequest) -> Result<WriteResponse> {
        let store = self.network.resolve_target(self.from, *target)?;
        if req.doc.is_none() && !req.is_hint {
            store.handle_delete(req).await
        } else {
            store.handle_write(req).await
        }
    }

    async fn remote_read(&self, target: &NodeId, req: ReadRequest) -> Result<ReadResponse> {
        let store = self.network.resolve_target(self.from, *target)?;
        store.handle_read(req).await
    }
}

// ── SimpleMembership ──────────────────────────────────────────────────────────

/// Minimal [`Membership`] implementation backed by a static [`Ring`].
///
/// Suitable for tests. Does not support dynamic membership changes.
pub struct SimpleMembership {
    local_node: NodeId,
    ring: Arc<Ring>,
    all_nodes: Vec<NodeId>,
}

impl SimpleMembership {
    /// Creates a new membership view.
    ///
    /// `all_nodes` is used to answer [`Membership::all_nodes`]; they should
    /// already be registered in `ring`.
    pub fn new(local_node: NodeId, ring: Ring, all_nodes: Vec<NodeId>) -> Self {
        Self {
            local_node,
            ring: Arc::new(ring),
            all_nodes,
        }
    }
}

impl Membership for SimpleMembership {
    fn ring_snapshot(&self) -> Arc<Ring> {
        Arc::clone(&self.ring)
    }

    fn local_node(&self) -> NodeId {
        self.local_node
    }

    fn owners_for_key(&self, key: &[u8], n: usize) -> Vec<NodeId> {
        self.ring.owners_for_key(key, n)
    }

    fn node_info(&self, _node: &NodeId) -> Option<NodeInfo> {
        None
    }

    fn node_state(&self, _node: &NodeId) -> Option<NodeState> {
        None
    }

    fn all_nodes(&self) -> Vec<NodeId> {
        self.all_nodes.clone()
    }

    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<MembershipEvent> {
        let (tx, rx) = tokio::sync::broadcast::channel(1);
        drop(tx);
        rx
    }
}
