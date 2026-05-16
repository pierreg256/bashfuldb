//! Cluster ring topology, virtual nodes, gossip, and failure detection.
//!
//! This crate manages cluster membership, the consistent-hash ring, gossip
//! protocol, and failure detection. It does NOT handle data replication
//! (that belongs in `bashfuldb-replication`).
//!
//! # Key traits
//!
//! - [`Membership`] — Ring topology and node state queries.
//! - [`GossipTransport`] — Network abstraction for gossip messages.
//! - [`FailureDetector`] — Phi accrual failure detection.
//!
//! # Key types
//!
//! - [`Ring`] — Consistent hash ring with virtual nodes.
//! - [`NodeState`] — State machine for node lifecycle.
//! - [`MembershipEvent`] — Events emitted on membership changes.

mod error;
mod node_state;
mod ring;
mod traits;
mod types;

pub use error::ClusterError;
pub use node_state::NodeState;
pub use ring::Ring;
pub use traits::{FailureDetector, GossipTransport, Membership};
pub use types::{GossipMessage, MembershipEvent, NodeInfo, VnodeId};

/// Result type for cluster operations.
pub type Result<T> = std::result::Result<T, ClusterError>;

