use serde::{Deserialize, Serialize};

/// Node lifecycle states.
///
/// State transitions are validated — not all transitions are legal.
/// Only `Healthy` nodes serve client traffic. `Draining` is read-only.
/// `Rebuilding` never serves any traffic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NodeState {
    /// Being admitted to the cluster.
    Joining,
    /// Receiving initial data transfer from existing nodes.
    Bootstrapping,
    /// Fully operational — the only state serving client traffic.
    Healthy,
    /// Voluntarily leaving the cluster.
    Leaving,
    /// Evacuating data before shutdown (read-only traffic allowed).
    Draining,
    /// Detected as potentially failed by the failure detector.
    Suspect,
    /// Confirmed failed.
    Down,
    /// Full reconstruction in progress (no traffic).
    Rebuilding,
}

impl NodeState {
    /// Returns true if this node can serve client read traffic.
    pub fn can_serve_reads(&self) -> bool {
        matches!(self, NodeState::Healthy | NodeState::Draining)
    }

    /// Returns true if this node can serve client write traffic.
    pub fn can_serve_writes(&self) -> bool {
        matches!(self, NodeState::Healthy)
    }

    /// Returns the set of valid transitions from this state.
    pub fn valid_transitions(&self) -> &'static [NodeState] {
        match self {
            NodeState::Joining => &[NodeState::Bootstrapping, NodeState::Down],
            NodeState::Bootstrapping => &[NodeState::Healthy, NodeState::Down],
            NodeState::Healthy => &[
                NodeState::Leaving,
                NodeState::Draining,
                NodeState::Suspect,
                NodeState::Down,
            ],
            NodeState::Leaving => &[NodeState::Draining, NodeState::Down],
            NodeState::Draining => &[NodeState::Down],
            NodeState::Suspect => &[NodeState::Healthy, NodeState::Down],
            NodeState::Down => &[NodeState::Rebuilding, NodeState::Joining],
            NodeState::Rebuilding => &[NodeState::Healthy, NodeState::Down],
        }
    }

    /// Returns true if transitioning to `target` is valid from this state.
    pub fn can_transition_to(&self, target: NodeState) -> bool {
        self.valid_transitions().contains(&target)
    }
}

impl std::fmt::Display for NodeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn healthy_can_serve_all_traffic() {
        assert!(NodeState::Healthy.can_serve_reads());
        assert!(NodeState::Healthy.can_serve_writes());
    }

    #[test]
    fn draining_is_read_only() {
        assert!(NodeState::Draining.can_serve_reads());
        assert!(!NodeState::Draining.can_serve_writes());
    }

    #[test]
    fn rebuilding_serves_nothing() {
        assert!(!NodeState::Rebuilding.can_serve_reads());
        assert!(!NodeState::Rebuilding.can_serve_writes());
    }

    #[test]
    fn valid_transitions_from_healthy() {
        let h = NodeState::Healthy;
        assert!(h.can_transition_to(NodeState::Leaving));
        assert!(h.can_transition_to(NodeState::Suspect));
        assert!(!h.can_transition_to(NodeState::Joining));
        assert!(!h.can_transition_to(NodeState::Rebuilding));
    }

    #[test]
    fn down_can_rejoin_or_rebuild() {
        let d = NodeState::Down;
        assert!(d.can_transition_to(NodeState::Joining));
        assert!(d.can_transition_to(NodeState::Rebuilding));
        assert!(!d.can_transition_to(NodeState::Healthy));
    }

    #[test]
    fn suspect_can_recover_or_die() {
        let s = NodeState::Suspect;
        assert!(s.can_transition_to(NodeState::Healthy));
        assert!(s.can_transition_to(NodeState::Down));
        assert!(!s.can_transition_to(NodeState::Leaving));
    }
}
