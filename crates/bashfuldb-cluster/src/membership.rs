use crate::{
    ClusterError, MemberDigest, Membership, MembershipEvent, NodeInfo, NodeState, Result, Ring,
};
use bashfuldb_clock::{Hlc, NodeId};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::broadcast;

fn unknown_node_address() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 0))
}

#[derive(Debug, Clone)]
struct MemberRecord {
    info: NodeInfo,
    generation: u64,
    updated_at: Hlc,
}

/// In-memory membership table and ring topology implementation.
#[derive(Debug)]
pub struct ClusterMembership {
    local_node: NodeId,
    ring: Arc<Ring>,
    members: HashMap<NodeId, MemberRecord>,
    events: broadcast::Sender<MembershipEvent>,
    schema_version: u64,
}

impl ClusterMembership {
    /// Creates a membership table with a single local node entry.
    pub fn new(local: NodeInfo, now: Hlc) -> Self {
        let (events, _) = broadcast::channel(1024);
        let mut members = HashMap::new();
        members.insert(
            local.id,
            MemberRecord {
                info: local.clone(),
                generation: 0,
                updated_at: now,
            },
        );

        let mut membership = Self {
            local_node: local.id,
            ring: Arc::new(Ring::new()),
            members,
            events,
            schema_version: 0,
        };
        membership.rebuild_ring();
        membership
    }

    /// Returns the current schema version.
    pub fn schema_version(&self) -> u64 {
        self.schema_version
    }

    /// Sets the schema version propagated in gossip.
    pub fn set_schema_version(&mut self, schema_version: u64) {
        self.schema_version = schema_version;
    }

    /// Returns all member digests for gossip exchange.
    pub fn member_digests(&self) -> Vec<MemberDigest> {
        self.members
            .values()
            .map(|record| MemberDigest {
                id: record.info.id,
                state: record.info.state,
                generation: record.generation,
                updated_at: record.updated_at,
            })
            .collect()
    }

    /// Inserts or updates a known node from local management actions.
    pub fn upsert_local_member(&mut self, info: NodeInfo, updated_at: Hlc) -> Result<()> {
        match self.members.get(&info.id) {
            Some(existing) => {
                if !existing.info.state.can_transition_to(info.state)
                    && existing.info.state != info.state
                {
                    return Err(ClusterError::InvalidStateTransition {
                        from: existing.info.state,
                        to: info.state,
                    });
                }

                let old_state = existing.info.state;
                let new_generation = existing.generation.saturating_add(1);
                self.members.insert(
                    info.id,
                    MemberRecord {
                        info: info.clone(),
                        generation: new_generation,
                        updated_at,
                    },
                );
                self.emit_state_events(info.id, old_state, info.state);
            }
            None => {
                self.members.insert(
                    info.id,
                    MemberRecord {
                        info: info.clone(),
                        generation: 0,
                        updated_at,
                    },
                );
                let _ = self.events.send(MembershipEvent::NodeJoined(info.id));
                self.emit_rebalance_if_needed(None, Some(info.state));
            }
        }

        Ok(())
    }

    /// Applies a state transition to an existing node and bumps generation.
    pub fn transition_state(
        &mut self,
        node: &NodeId,
        to: NodeState,
        updated_at: Hlc,
    ) -> Result<()> {
        let (from, mut info, generation) = {
            let current = self
                .members
                .get(node)
                .ok_or_else(|| ClusterError::NodeNotFound {
                    node_id: node.to_string(),
                })?;
            (current.info.state, current.info.clone(), current.generation)
        };

        if from != to && !from.can_transition_to(to) {
            return Err(ClusterError::InvalidStateTransition { from, to });
        }

        info.state = to;
        self.members.insert(
            *node,
            MemberRecord {
                info,
                generation: generation.saturating_add(1),
                updated_at,
            },
        );
        self.emit_state_events(*node, from, to);
        Ok(())
    }

    /// Merges digests received from a remote gossip message.
    pub fn merge_remote_digests(&mut self, remote: &[MemberDigest]) -> bool {
        let mut changed = false;
        for digest in remote {
            let local = self.members.get(&digest.id).cloned();
            match local {
                Some(local_record) => {
                    let remote_is_newer = digest.generation > local_record.generation
                        || (digest.generation == local_record.generation
                            && digest.updated_at > local_record.updated_at);
                    if !remote_is_newer {
                        continue;
                    }

                    if digest.state != local_record.info.state
                        && !local_record.info.state.can_transition_to(digest.state)
                    {
                        continue;
                    }

                    let mut info = local_record.info.clone();
                    let from = info.state;
                    info.state = digest.state;
                    self.members.insert(
                        digest.id,
                        MemberRecord {
                            info,
                            generation: digest.generation,
                            updated_at: digest.updated_at,
                        },
                    );
                    self.emit_state_events(digest.id, from, digest.state);
                    changed = true;
                }
                None => {
                    if digest.state != NodeState::Joining && digest.state != NodeState::Healthy {
                        continue;
                    }
                    let info = NodeInfo {
                        id: digest.id,
                        // Remote digests may arrive before full node metadata.
                        address: unknown_node_address(),
                        gossip_address: unknown_node_address(),
                        state: digest.state,
                        vnodes: Vec::new(),
                    };
                    self.members.insert(
                        digest.id,
                        MemberRecord {
                            info,
                            generation: digest.generation,
                            updated_at: digest.updated_at,
                        },
                    );
                    let _ = self.events.send(MembershipEvent::NodeJoined(digest.id));
                    self.emit_rebalance_if_needed(None, Some(digest.state));
                    changed = true;
                }
            }
        }
        changed
    }

    fn emit_state_events(&mut self, node: NodeId, from: NodeState, to: NodeState) {
        if from != to {
            let _ = self
                .events
                .send(MembershipEvent::StateChanged { node, from, to });
            if to == NodeState::Down {
                let _ = self.events.send(MembershipEvent::NodeLeft(node));
            }
            self.emit_rebalance_if_needed(Some(from), Some(to));
        }
    }

    fn emit_rebalance_if_needed(&mut self, from: Option<NodeState>, to: Option<NodeState>) {
        let before_healthy = from
            .map(|state| state == NodeState::Healthy)
            .unwrap_or(false);
        let after_healthy = to.map(|state| state == NodeState::Healthy).unwrap_or(false);
        if before_healthy != after_healthy || (from.is_none() && after_healthy) {
            self.rebuild_ring();
            let _ = self.events.send(MembershipEvent::RingRebalanced);
        }
    }

    fn rebuild_ring(&mut self) {
        let mut ring = Ring::new();
        for member in self.members.values() {
            if member.info.state == NodeState::Healthy {
                ring.add_node(member.info.id);
            }
        }
        self.ring = Arc::new(ring);
    }
}

impl Membership for ClusterMembership {
    fn ring_snapshot(&self) -> Arc<Ring> {
        Arc::clone(&self.ring)
    }

    fn local_node(&self) -> NodeId {
        self.local_node
    }

    fn owners_for_key(&self, key: &[u8], n: usize) -> Vec<NodeId> {
        self.ring.owners_for_key(key, n)
    }

    fn node_info(&self, node: &NodeId) -> Option<NodeInfo> {
        self.members.get(node).map(|record| record.info.clone())
    }

    fn node_state(&self, node: &NodeId) -> Option<NodeState> {
        self.members.get(node).map(|record| record.info.state)
    }

    fn all_nodes(&self) -> Vec<NodeId> {
        let mut nodes: Vec<_> = self.members.keys().copied().collect();
        nodes.sort();
        nodes
    }

    fn subscribe(&self) -> broadcast::Receiver<MembershipEvent> {
        self.events.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info(id: NodeId, state: NodeState) -> NodeInfo {
        NodeInfo {
            id,
            address: std::net::SocketAddr::from(([127, 0, 0, 1], 9000)),
            gossip_address: std::net::SocketAddr::from(([127, 0, 0, 1], 9001)),
            state,
            vnodes: Vec::new(),
        }
    }

    #[test]
    fn validates_state_transitions() {
        let id = NodeId::random();
        let mut membership = ClusterMembership::new(info(id, NodeState::Joining), Hlc::new(1, 0));
        assert!(
            membership
                .transition_state(&id, NodeState::Healthy, Hlc::new(2, 0))
                .is_err()
        );
        assert!(
            membership
                .transition_state(&id, NodeState::Bootstrapping, Hlc::new(2, 0))
                .is_ok()
        );
    }

    #[test]
    fn join_and_leave_emit_events() {
        let local = NodeId::random();
        let joining = NodeId::random();
        let mut membership =
            ClusterMembership::new(info(local, NodeState::Healthy), Hlc::new(1, 0));
        let mut rx = membership.subscribe();

        let _ = membership.upsert_local_member(info(joining, NodeState::Joining), Hlc::new(2, 0));
        let _ = membership.transition_state(&joining, NodeState::Bootstrapping, Hlc::new(3, 0));
        let _ = membership.transition_state(&joining, NodeState::Healthy, Hlc::new(4, 0));
        let _ = membership.transition_state(&joining, NodeState::Suspect, Hlc::new(5, 0));
        let _ = membership.transition_state(&joining, NodeState::Down, Hlc::new(6, 0));

        let mut saw_join = false;
        let mut saw_left = false;
        while let Ok(event) = rx.try_recv() {
            match event {
                MembershipEvent::NodeJoined(node) if node == joining => saw_join = true,
                MembershipEvent::NodeLeft(node) if node == joining => saw_left = true,
                _ => {}
            }
        }
        assert!(saw_join);
        assert!(saw_left);
    }
}
