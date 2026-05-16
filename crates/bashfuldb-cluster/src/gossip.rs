use crate::Membership;
use crate::{
    ClusterError, ClusterMembership, FailureDetector, GossipMessage, GossipTransport, NodeState,
    Result,
};
use bashfuldb_clock::{Clock, NodeId};
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, oneshot};
use tokio::task::JoinHandle;

/// Background gossip engine task handle.
#[derive(Debug)]
pub struct GossipEngineHandle {
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
}

impl GossipEngineHandle {
    /// Stops the gossip task and waits for it to finish.
    pub async fn shutdown(mut self) -> Result<()> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        self.task
            .await
            .map_err(|error| ClusterError::TransportError {
                message: format!("gossip task join error: {error}"),
            })?;
        Ok(())
    }
}

/// Cassandra-style periodic gossip engine.
#[derive(Debug)]
pub struct GossipEngine<T, C, F>
where
    T: GossipTransport,
    C: Clock,
    F: FailureDetector,
{
    membership: Arc<Mutex<ClusterMembership>>,
    transport: Arc<T>,
    clock: Arc<C>,
    detector: Arc<F>,
    gossip_interval: Duration,
    rng: Mutex<StdRng>,
}

impl<T, C, F> GossipEngine<T, C, F>
where
    T: GossipTransport + 'static,
    C: Clock + 'static,
    F: FailureDetector + 'static,
{
    /// Creates a new gossip engine with configurable tick interval.
    pub fn new(
        membership: Arc<Mutex<ClusterMembership>>,
        transport: Arc<T>,
        clock: Arc<C>,
        detector: Arc<F>,
        gossip_interval: Duration,
        random_seed: u64,
    ) -> Self {
        Self {
            membership,
            transport,
            clock,
            detector,
            gossip_interval,
            rng: Mutex::new(StdRng::seed_from_u64(random_seed)),
        }
    }

    /// Spawns the gossip loop.
    pub fn start(self: Arc<Self>) -> GossipEngineHandle {
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(self.gossip_interval);
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => {
                        break;
                    }
                    _ = ticker.tick() => {
                        let _ = self.gossip_tick().await;
                        let _ = self.evaluate_failure_detector().await;
                    }
                    receive_result = self.transport.receive() => {
                        if let Ok((sender, message)) = receive_result {
                            let _ = self.handle_incoming(sender, message).await;
                        }
                    }
                }
            }
        });
        GossipEngineHandle {
            shutdown_tx: Some(shutdown_tx),
            task,
        }
    }

    async fn gossip_tick(&self) -> Result<()> {
        let (local_node, schema_version, peers, members) = {
            let membership = self.membership.lock().await;
            let local = membership.local_node();
            let schema_version = membership.schema_version();
            let peers = membership
                .all_nodes()
                .into_iter()
                .filter(|node| node != &local)
                .collect::<Vec<_>>();
            (local, schema_version, peers, membership.member_digests())
        };

        if peers.is_empty() {
            return Ok(());
        }

        let target = {
            let mut rng = self.rng.lock().await;
            peers.choose(&mut *rng).copied()
        };

        let Some(target) = target else {
            return Ok(());
        };
        let hlc = self
            .clock
            .tick()
            .map_err(|error| ClusterError::ClockError {
                message: error.to_string(),
            })?;
        let message = GossipMessage {
            sender: local_node,
            hlc,
            schema_version,
            members,
        };
        self.transport.send(&target, message).await
    }

    async fn handle_incoming(&self, sender: NodeId, message: GossipMessage) -> Result<()> {
        self.detector.heartbeat(&sender);
        self.clock
            .update(message.hlc)
            .map_err(|error| ClusterError::ClockError {
                message: error.to_string(),
            })?;
        {
            let mut membership = self.membership.lock().await;
            let _ = membership.merge_remote_digests(&message.members);
            if message.schema_version > membership.schema_version() {
                membership.set_schema_version(message.schema_version);
            }
        }
        self.reply_with_local_digest(sender).await?;
        Ok(())
    }

    async fn reply_with_local_digest(&self, target: NodeId) -> Result<()> {
        let (sender, schema_version, members) = {
            let membership = self.membership.lock().await;
            (
                membership.local_node(),
                membership.schema_version(),
                membership.member_digests(),
            )
        };
        let hlc = self
            .clock
            .tick()
            .map_err(|error| ClusterError::ClockError {
                message: error.to_string(),
            })?;
        self.transport
            .send(
                &target,
                GossipMessage {
                    sender,
                    hlc,
                    schema_version,
                    members,
                },
            )
            .await
    }

    async fn evaluate_failure_detector(&self) -> Result<()> {
        let nodes = {
            let membership = self.membership.lock().await;
            membership.all_nodes()
        };
        for node in nodes {
            let state = {
                let membership = self.membership.lock().await;
                membership.node_state(&node)
            };
            let Some(state) = state else {
                continue;
            };
            if state == NodeState::Down {
                continue;
            }
            let next_state = if state == NodeState::Suspect {
                Some(NodeState::Down)
            } else if self.detector.is_suspect(&node) {
                Some(NodeState::Suspect)
            } else {
                None
            };

            if let Some(target) = next_state {
                let hlc = self
                    .clock
                    .tick()
                    .map_err(|error| ClusterError::ClockError {
                        message: error.to_string(),
                    })?;
                let mut membership = self.membership.lock().await;
                let _ = membership.transition_state(&node, target, hlc);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Membership, NodeInfo, PhiAccrualFailureDetector, SimulatedNetwork};
    use bashfuldb_clock::{Hlc, ManualClock};
    use std::net::SocketAddr;

    fn node_info(port: u16) -> NodeInfo {
        NodeInfo {
            id: NodeId::random(),
            address: SocketAddr::from(([127, 0, 0, 1], port)),
            gossip_address: SocketAddr::from(([127, 0, 0, 1], port + 100)),
            state: NodeState::Healthy,
            vnodes: Vec::new(),
        }
    }

    async fn membership_with_nodes(
        local: NodeInfo,
        peers: &[NodeInfo],
    ) -> Arc<Mutex<ClusterMembership>> {
        let mut membership = ClusterMembership::new(local.clone(), Hlc::new(1, 0));
        for peer in peers {
            let _ = membership.upsert_local_member(peer.clone(), Hlc::new(1, 0));
        }
        Arc::new(Mutex::new(membership))
    }

    #[tokio::test(start_paused = true)]
    async fn gossip_converges_under_message_loss() {
        let network = SimulatedNetwork::new();
        let a = node_info(9101);
        let b = node_info(9102);
        let c = node_info(9103);
        let ma = membership_with_nodes(a.clone(), &[b.clone(), c.clone()]).await;
        let mb = membership_with_nodes(b.clone(), &[a.clone(), c.clone()]).await;

        {
            let mut m = ma.lock().await;
            let _ = m.transition_state(&c.id, NodeState::Leaving, Hlc::new(2, 0));
        }

        network.drop_next(a.id, b.id, 4);
        let ta = Arc::new(network.register_node(a.id));
        let tb = Arc::new(network.register_node(b.id));

        let ca = Arc::new(ManualClock::new(1));
        let cb = Arc::new(ManualClock::new(1));
        let da = Arc::new(PhiAccrualFailureDetector::new());
        let db = Arc::new(PhiAccrualFailureDetector::new());

        let ea = Arc::new(GossipEngine::new(
            ma.clone(),
            ta,
            ca,
            da,
            Duration::from_secs(1),
            11,
        ));
        let eb = Arc::new(GossipEngine::new(
            mb.clone(),
            tb,
            cb,
            db,
            Duration::from_secs(1),
            22,
        ));
        let ha = ea.start();
        let hb = eb.start();

        for _ in 0..60 {
            tokio::time::advance(Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
        }

        {
            let mb = mb.lock().await;
            assert_eq!(mb.node_state(&c.id), Some(NodeState::Leaving));
        }

        let _ = ha.shutdown().await;
        let _ = hb.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn failure_detection_transitions_suspect_then_down() {
        let network = SimulatedNetwork::new();
        let a = node_info(9201);
        let b = node_info(9202);

        let ma = membership_with_nodes(a.clone(), std::slice::from_ref(&b)).await;
        let mb = membership_with_nodes(b.clone(), std::slice::from_ref(&a)).await;

        let ta = Arc::new(network.register_node(a.id));
        let tb = Arc::new(network.register_node(b.id));
        let ca = Arc::new(ManualClock::new(1));
        let cb = Arc::new(ManualClock::new(1));
        let da = Arc::new(PhiAccrualFailureDetector::with_threshold(0.5));
        let db = Arc::new(PhiAccrualFailureDetector::with_threshold(0.5));
        da.heartbeat(&b.id);
        db.heartbeat(&a.id);
        tokio::time::advance(Duration::from_secs(1)).await;
        da.heartbeat(&b.id);
        db.heartbeat(&a.id);

        let ea = Arc::new(GossipEngine::new(
            ma.clone(),
            ta,
            ca,
            da,
            Duration::from_secs(1),
            44,
        ));
        let eb = Arc::new(GossipEngine::new(
            mb.clone(),
            tb,
            cb,
            db,
            Duration::from_secs(1),
            55,
        ));
        let ha = ea.start();
        let hb = eb.start();

        for _ in 0..3 {
            tokio::time::advance(Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
        }
        network.partition(a.id, b.id);
        for _ in 0..30 {
            tokio::time::advance(Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
        }

        let state = {
            let membership = ma.lock().await;
            membership.node_state(&b.id)
        };
        assert_eq!(state, Some(NodeState::Down));

        let _ = ha.shutdown().await;
        let _ = hb.shutdown().await;
    }
}
