use crate::{ClusterError, GossipMessage, GossipTransport, Result};
use async_trait::async_trait;
use bashfuldb_clock::NodeId;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use tokio::sync::mpsc;

#[derive(Debug)]
struct SimulatedNetworkState {
    endpoints: HashMap<NodeId, mpsc::UnboundedSender<(NodeId, GossipMessage)>>,
    default_delay: Duration,
    link_delay: HashMap<(NodeId, NodeId), Duration>,
    partitions: HashSet<(NodeId, NodeId)>,
    drop_links: HashSet<(NodeId, NodeId)>,
    drop_next: HashMap<(NodeId, NodeId), usize>,
}

/// Shared deterministic network for simulation tests.
#[derive(Debug, Clone)]
pub struct SimulatedNetwork {
    inner: Arc<Mutex<SimulatedNetworkState>>,
}

impl SimulatedNetwork {
    /// Creates an empty simulation network.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(SimulatedNetworkState {
                endpoints: HashMap::new(),
                default_delay: Duration::from_millis(0),
                link_delay: HashMap::new(),
                partitions: HashSet::new(),
                drop_links: HashSet::new(),
                drop_next: HashMap::new(),
            })),
        }
    }

    /// Creates and registers a transport endpoint for a node.
    pub fn register_node(&self, node: NodeId) -> SimulatedTransport {
        let (tx, rx) = mpsc::unbounded_channel();
        {
            let mut state = self.lock_state();
            state.endpoints.insert(node, tx);
        }
        SimulatedTransport {
            node_id: node,
            network: self.clone(),
            inbox: tokio::sync::Mutex::new(rx),
        }
    }

    /// Sets default delivery delay for all links.
    pub fn set_default_delay(&self, delay: Duration) {
        let mut state = self.lock_state();
        state.default_delay = delay;
    }

    /// Sets a deterministic delay for a specific directed link.
    pub fn set_link_delay(&self, from: NodeId, to: NodeId, delay: Duration) {
        let mut state = self.lock_state();
        state.link_delay.insert((from, to), delay);
    }

    /// Drops all traffic over a directed link.
    pub fn drop_link(&self, from: NodeId, to: NodeId) {
        let mut state = self.lock_state();
        state.drop_links.insert((from, to));
    }

    /// Restores traffic over a directed link.
    pub fn restore_link(&self, from: NodeId, to: NodeId) {
        let mut state = self.lock_state();
        state.drop_links.remove(&(from, to));
    }

    /// Drops the next `count` messages on a directed link.
    pub fn drop_next(&self, from: NodeId, to: NodeId, count: usize) {
        let mut state = self.lock_state();
        state.drop_next.insert((from, to), count);
    }

    /// Creates a bidirectional partition between two nodes.
    pub fn partition(&self, a: NodeId, b: NodeId) {
        let mut state = self.lock_state();
        state.partitions.insert((a, b));
        state.partitions.insert((b, a));
    }

    /// Removes a bidirectional partition between two nodes.
    pub fn heal_partition(&self, a: NodeId, b: NodeId) {
        let mut state = self.lock_state();
        state.partitions.remove(&(a, b));
        state.partitions.remove(&(b, a));
    }

    fn lock_state(&self) -> MutexGuard<'_, SimulatedNetworkState> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Default for SimulatedNetwork {
    fn default() -> Self {
        Self::new()
    }
}

/// In-memory deterministic gossip transport for simulation and tests.
#[derive(Debug)]
pub struct SimulatedTransport {
    node_id: NodeId,
    network: SimulatedNetwork,
    inbox: tokio::sync::Mutex<mpsc::UnboundedReceiver<(NodeId, GossipMessage)>>,
}

#[async_trait]
impl GossipTransport for SimulatedTransport {
    async fn send(&self, target: &NodeId, msg: GossipMessage) -> Result<()> {
        let (sender, delay, should_drop) = {
            let mut state = self.network.lock_state();
            let link = (self.node_id, *target);
            let mut should_drop =
                state.partitions.contains(&link) || state.drop_links.contains(&link);
            if let Some(remaining) = state.drop_next.get_mut(&link) {
                if *remaining > 0 {
                    should_drop = true;
                    *remaining -= 1;
                }
                if *remaining == 0 {
                    state.drop_next.remove(&link);
                }
            }

            let sender = state.endpoints.get(target).cloned();
            let delay = state
                .link_delay
                .get(&link)
                .copied()
                .unwrap_or(state.default_delay);
            (sender, delay, should_drop)
        };

        if should_drop {
            return Ok(());
        }

        let Some(sender) = sender else {
            return Err(ClusterError::TransportError {
                message: format!("unknown target node: {target}"),
            });
        };

        let from = self.node_id;
        tokio::spawn(async move {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            let _ = sender.send((from, msg));
        });
        Ok(())
    }

    async fn receive(&self) -> Result<(NodeId, GossipMessage)> {
        let mut inbox = self.inbox.lock().await;
        inbox
            .recv()
            .await
            .ok_or_else(|| ClusterError::TransportError {
                message: format!("transport closed for {}", self.node_id),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bashfuldb_clock::Hlc;

    #[tokio::test(start_paused = true)]
    async fn respects_delay_and_partition_controls() {
        let network = SimulatedNetwork::new();
        let a = NodeId::random();
        let b = NodeId::random();
        let ta = network.register_node(a);
        let tb = network.register_node(b);
        network.set_link_delay(a, b, Duration::from_secs(5));

        let msg = GossipMessage {
            sender: a,
            hlc: Hlc::new(1, 0),
            schema_version: 1,
            members: Vec::new(),
        };
        let _ = ta.send(&b, msg.clone()).await;
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        let pending = tokio::time::timeout(Duration::from_secs(0), tb.receive()).await;
        assert!(pending.is_err());

        tokio::time::advance(Duration::from_secs(5)).await;
        tokio::task::yield_now().await;
        let delivered = tokio::time::timeout(Duration::from_secs(1), tb.receive()).await;
        assert!(delivered.is_ok());

        network.partition(a, b);
        let _ = ta.send(&b, msg).await;
        tokio::time::advance(Duration::from_secs(10)).await;
        tokio::task::yield_now().await;
        let dropped = tokio::time::timeout(Duration::from_secs(0), tb.receive()).await;
        assert!(dropped.is_err());
    }
}
