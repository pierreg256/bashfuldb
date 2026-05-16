use async_trait::async_trait;
use bashfuldb_clock::NodeId;
use crate::{GossipMessage, MembershipEvent, NodeInfo, NodeState, Ring};
use std::sync::Arc;

/// Cluster membership and ring topology.
///
/// Provides read access to the current cluster state. The implementation
/// manages the ring, node states, and broadcasts membership events.
pub trait Membership: Send + Sync + 'static {
    /// Returns a snapshot of the current hash ring.
    ///
    /// The returned `Arc` provides a consistent, immutable view of the ring
    /// at a point in time. Callers may hold it across async boundaries.
    fn ring_snapshot(&self) -> Arc<Ring>;

    /// Returns this node's ID.
    fn local_node(&self) -> NodeId;

    /// Returns the `n` distinct physical nodes responsible for a key.
    fn owners_for_key(&self, key: &[u8], n: usize) -> Vec<NodeId>;

    /// Returns information about a specific node, or `None` if unknown.
    fn node_info(&self, node: &NodeId) -> Option<NodeInfo>;

    /// Returns the current state of a node, or `None` if unknown.
    fn node_state(&self, node: &NodeId) -> Option<NodeState>;

    /// Returns all known node IDs.
    fn all_nodes(&self) -> Vec<NodeId>;

    /// Subscribes to membership change events.
    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<MembershipEvent>;
}

/// Network transport for gossip messages.
///
/// Abstractions allows swapping in a simulated transport for
/// deterministic testing (partitions, delays, message loss).
#[async_trait]
pub trait GossipTransport: Send + Sync + 'static {
    /// Sends a gossip message to the target node.
    async fn send(&self, target: &NodeId, msg: GossipMessage) -> crate::Result<()>;

    /// Receives the next incoming gossip message.
    ///
    /// Returns the sender's ID and the message payload.
    async fn receive(&self) -> crate::Result<(NodeId, GossipMessage)>;
}

/// Phi accrual failure detector.
///
/// Tracks heartbeat arrivals and computes a suspicion level (phi) for
/// each node. Higher phi = more likely the node has failed.
pub trait FailureDetector: Send + Sync + 'static {
    /// Records a heartbeat arrival from the given node.
    fn heartbeat(&self, node: &NodeId);

    /// Returns the current phi (suspicion level) for a node.
    ///
    /// A phi of 0.0 means no suspicion. The threshold for marking a node
    /// as `Suspect` is operator-configurable (default: 8.0).
    fn phi(&self, node: &NodeId) -> f64;

    /// Returns true if the node's phi exceeds the suspicion threshold.
    fn is_suspect(&self, node: &NodeId) -> bool;

    /// Sets the phi threshold above which a node is considered suspect.
    fn set_threshold(&self, threshold: f64);
}
