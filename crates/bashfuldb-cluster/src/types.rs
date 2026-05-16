use bashfuldb_clock::{Hlc, NodeId};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

/// A virtual node identifier on the hash ring.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct VnodeId(pub u64);

/// Information about a physical node in the cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    /// The unique node identifier.
    pub id: NodeId,
    /// The node's client-facing address.
    pub address: SocketAddr,
    /// The node's inter-node gossip address (may differ from client address).
    pub gossip_address: SocketAddr,
    /// Current lifecycle state.
    pub state: crate::NodeState,
    /// Virtual node IDs owned by this node.
    pub vnodes: Vec<VnodeId>,
}

/// Events emitted when cluster membership changes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MembershipEvent {
    /// A new node has been admitted to the cluster.
    NodeJoined(NodeId),
    /// A node has left the cluster.
    NodeLeft(NodeId),
    /// A node's state has changed.
    StateChanged {
        node: NodeId,
        from: crate::NodeState,
        to: crate::NodeState,
    },
    /// The ring has been rebalanced (vnodes redistributed).
    RingRebalanced,
}

/// A gossip message exchanged between nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipMessage {
    /// The sender's node ID.
    pub sender: NodeId,
    /// The sender's current HLC.
    pub hlc: Hlc,
    /// The sender's current schema version.
    pub schema_version: u64,
    /// Membership state digest: list of (NodeId, NodeState, generation).
    pub members: Vec<MemberDigest>,
}

/// A compact digest of a single node's membership state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberDigest {
    /// The node ID.
    pub id: NodeId,
    /// The node's current state.
    pub state: crate::NodeState,
    /// A monotonically increasing generation number (bumped on state change).
    pub generation: u64,
    /// The HLC at which this state was last updated.
    pub updated_at: Hlc,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vnode_id_ordering() {
        assert!(VnodeId(0) < VnodeId(1));
        assert!(VnodeId(100) < VnodeId(200));
    }

    #[test]
    fn gossip_message_serializable() {
        let msg = GossipMessage {
            sender: NodeId::random(),
            hlc: Hlc::new(1000, 0),
            schema_version: 1,
            members: vec![],
        };
        let json = serde_json::to_string(&msg).unwrap();
        let _: GossipMessage = serde_json::from_str(&json).unwrap();
    }
}
