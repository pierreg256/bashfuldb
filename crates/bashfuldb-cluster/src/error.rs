use thiserror::Error;

/// Errors that can occur in cluster operations.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ClusterError {
    /// Invalid node state transition.
    #[error("invalid state transition: {from:?} -> {to:?}")]
    InvalidStateTransition {
        from: crate::NodeState,
        to: crate::NodeState,
    },

    /// Node not found in the cluster.
    #[error("node not found: {node_id}")]
    NodeNotFound { node_id: String },

    /// Gossip transport error.
    #[error("gossip transport error: {message}")]
    TransportError { message: String },

    /// Not enough nodes to satisfy the requested replication factor.
    #[error("insufficient nodes: have {available}, need {required}")]
    InsufficientNodes { available: usize, required: usize },

    /// The ring is empty (no nodes have joined).
    #[error("ring is empty")]
    EmptyRing,

    /// Clock operation failed.
    #[error("clock error: {message}")]
    ClockError { message: String },
}
