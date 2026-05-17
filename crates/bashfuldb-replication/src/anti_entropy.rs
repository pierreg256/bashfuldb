use crate::{ReplicationTransport, ReplicaStore, Result};
use bashfuldb_cluster::Membership;
use bashfuldb_storage::ColumnFamily;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Anti-entropy skeleton using Merkle-tree range hashing.
///
/// Runs at low priority; yields to client traffic via `tokio::task::yield_now`.
/// In a production deployment a background task would call
/// `run_comparison_round` periodically. Only the structural skeleton and the
/// range-hash primitive are implemented here; the full repair protocol is left
/// as `todo!` placeholders clearly marked below.
pub struct AntiEntropy {
    /// This node's local replica store.
    pub(crate) store: Arc<ReplicaStore>,
    /// Transport for sending repair writes to peers (used in future repair protocol).
    #[allow(dead_code)]
    pub(crate) transport: Arc<dyn ReplicationTransport>,
    /// Membership for discovering peer nodes.
    pub(crate) membership: Arc<dyn Membership>,
}

impl AntiEntropy {
    /// Creates a new anti-entropy engine.
    pub fn new(
        store: Arc<ReplicaStore>,
        transport: Arc<dyn ReplicationTransport>,
        membership: Arc<dyn Membership>,
    ) -> Self {
        Self {
            store,
            transport,
            membership,
        }
    }

    /// Computes a 64-bit hash over all keys in `[start, end)` within the
    /// `default` column family.
    ///
    /// This serves as a Merkle-leaf for a range of the key space. Two nodes
    /// with identical data in a range will produce identical hashes.
    pub fn range_hash(&self, start: &[u8], end: &[u8]) -> Result<u64> {
        let iter = self
            .store
            .storage()
            .scan(ColumnFamily::DEFAULT, start, end)?;

        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        for item in iter {
            let (key, val) = item?;
            key.hash(&mut hasher);
            val.hash(&mut hasher);
        }
        Ok(hasher.finish())
    }

    /// Runs one anti-entropy comparison round with all known peers.
    ///
    /// 1. Divides the key space into segments.  
    /// 2. Computes local hashes.  
    /// 3. Exchanges hashes with peers (skeleton — transport not yet wired).  
    /// 4. Triggers repair writes for divergent segments (skeleton).  
    ///
    /// Returns the number of repairs triggered.
    pub async fn run_comparison_round(&self) -> Result<usize> {
        // Yield to allow client traffic to proceed.
        tokio::task::yield_now().await;

        let peers = self.membership.all_nodes();
        let local_node = self.membership.local_node();
        let peers: Vec<_> = peers.into_iter().filter(|&n| n != local_node).collect();

        if peers.is_empty() {
            return Ok(0);
        }

        // Compute a full-range hash of local state as the root Merkle node.
        let _local_hash = self.range_hash(&[], &[])?;

        // TODO: exchange `_local_hash` with each peer via a dedicated
        //       AntiEntropy RPC (not implemented in this skeleton).
        //
        // TODO: on mismatch, recursively bisect the key space to find the
        //       divergent leaf range, then scan + repair individual keys.
        //
        // TODO: to keep anti-entropy low-priority, insert
        //       `tokio::task::yield_now().await` between every sub-range scan.

        tracing::debug!(
            peers = peers.len(),
            "anti-entropy skeleton: comparison round complete (repair not yet implemented)"
        );

        Ok(0) // placeholder: no repairs until full protocol is implemented
    }
}
