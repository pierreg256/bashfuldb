use crate::FailureDetector;
use bashfuldb_clock::NodeId;
use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, MutexGuard};
use tokio::time::Instant;

const DEFAULT_PHI_THRESHOLD: f64 = 8.0;
const MAX_SAMPLES: usize = 100;
/// Floor for average heartbeat interval to avoid division-by-zero and unstable phi spikes.
const MIN_MEAN_INTERVAL_SECONDS: f64 = 0.001;
const LOG10_E: f64 = std::f64::consts::LOG10_E;

#[derive(Debug, Clone)]
struct NodeHeartbeatHistory {
    last_seen: Option<Instant>,
    inter_arrivals: VecDeque<f64>,
}

impl NodeHeartbeatHistory {
    fn new() -> Self {
        Self {
            last_seen: None,
            inter_arrivals: VecDeque::new(),
        }
    }

    fn record_heartbeat(&mut self, now: Instant) {
        if let Some(last) = self.last_seen {
            let interval = now.saturating_duration_since(last).as_secs_f64();
            self.inter_arrivals
                .push_back(interval.max(MIN_MEAN_INTERVAL_SECONDS));
            if self.inter_arrivals.len() > MAX_SAMPLES {
                let _ = self.inter_arrivals.pop_front();
            }
        }
        self.last_seen = Some(now);
    }

    fn phi(&self, now: Instant) -> f64 {
        let Some(last_seen) = self.last_seen else {
            return 0.0;
        };

        if self.inter_arrivals.is_empty() {
            return 0.0;
        }

        let elapsed = now.saturating_duration_since(last_seen).as_secs_f64();
        let mean = self.inter_arrivals.iter().sum::<f64>() / self.inter_arrivals.len() as f64;
        (elapsed / mean.max(MIN_MEAN_INTERVAL_SECONDS)) * LOG10_E
    }
}

#[derive(Debug)]
struct DetectorState {
    threshold: f64,
    nodes: HashMap<NodeId, NodeHeartbeatHistory>,
}

/// Phi accrual failure detector with per-node heartbeat inter-arrival tracking.
#[derive(Debug)]
pub struct PhiAccrualFailureDetector {
    state: Mutex<DetectorState>,
}

impl PhiAccrualFailureDetector {
    /// Creates a detector with the default phi threshold of `8.0`.
    pub fn new() -> Self {
        Self::with_threshold(DEFAULT_PHI_THRESHOLD)
    }

    /// Creates a detector with a custom phi threshold.
    pub fn with_threshold(threshold: f64) -> Self {
        Self {
            state: Mutex::new(DetectorState {
                threshold,
                nodes: HashMap::new(),
            }),
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, DetectorState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Default for PhiAccrualFailureDetector {
    fn default() -> Self {
        Self::new()
    }
}

impl FailureDetector for PhiAccrualFailureDetector {
    fn heartbeat(&self, node: &NodeId) {
        let now = Instant::now();
        let mut state = self.lock_state();
        let history = state
            .nodes
            .entry(*node)
            .or_insert_with(NodeHeartbeatHistory::new);
        history.record_heartbeat(now);
    }

    fn phi(&self, node: &NodeId) -> f64 {
        let now = Instant::now();
        let state = self.lock_state();
        state
            .nodes
            .get(node)
            .map_or(0.0, |history| history.phi(now))
    }

    fn is_suspect(&self, node: &NodeId) -> bool {
        let state = self.lock_state();
        let now = Instant::now();
        let phi = state
            .nodes
            .get(node)
            .map_or(0.0, |history| history.phi(now));
        phi >= state.threshold
    }

    fn set_threshold(&self, threshold: f64) {
        let mut state = self.lock_state();
        state.threshold = threshold;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn detector_marks_node_suspect_after_timeout() {
        let detector = PhiAccrualFailureDetector::with_threshold(1.0);
        let node = NodeId::random();
        detector.heartbeat(&node);
        thread::sleep(Duration::from_millis(10));
        detector.heartbeat(&node);
        thread::sleep(Duration::from_secs(1));
        assert!(detector.is_suspect(&node));
    }
}
