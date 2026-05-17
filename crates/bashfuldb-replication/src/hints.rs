use crate::{ReplicationTransport, ReplicaStore, Result};
use std::sync::Arc;
use std::time::Duration;

/// Background task that periodically drains the `hints` CF and delivers
/// hinted-handoff writes to their target nodes.
///
/// Spawn with [`HintDeliveryTask::spawn`].
pub struct HintDeliveryTask {
    store: Arc<ReplicaStore>,
    transport: Arc<dyn ReplicationTransport>,
    interval: Duration,
}

impl HintDeliveryTask {
    /// Creates a new hint delivery task.
    ///
    /// # Parameters
    ///
    /// * `store` – the local node's replica store (owns the `hints` CF).
    /// * `transport` – transport used to deliver hints to recovered nodes.
    /// * `interval` – how often to run the delivery loop.
    pub fn new(
        store: Arc<ReplicaStore>,
        transport: Arc<dyn ReplicationTransport>,
        interval: Duration,
    ) -> Self {
        Self {
            store,
            transport,
            interval,
        }
    }

    /// Spawns the delivery loop as a Tokio background task.
    ///
    /// The returned `JoinHandle` can be aborted to shut down the task.
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(self.interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                // Yield to allow client traffic to proceed first.
                tokio::task::yield_now().await;
                match self.store.deliver_pending_hints(self.transport.as_ref()).await {
                    Ok(n) if n > 0 => tracing::info!(delivered = n, "hint handoff completed"),
                    Ok(_) => {}
                    Err(e) => tracing::warn!(error = %e, "hint delivery iteration failed"),
                }
            }
        })
    }

    /// Runs a single delivery pass (useful in tests).
    pub async fn run_once(&self) -> Result<usize> {
        self.store.deliver_pending_hints(self.transport.as_ref()).await
    }
}
