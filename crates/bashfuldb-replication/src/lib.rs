//! Quorum-based replication, conflict resolution, and convergence.
//!
//! Implements multi-node quorum reads/writes (N=3, R=2, W=2 by default),
//! sloppy quorum with hinted handoff, read repair, vector-clock conflict
//! detection with LWW fallback, and a Merkle-tree anti-entropy skeleton.

mod anti_entropy;
pub mod coordinator;
mod error;
mod hints;
mod idempotency;
mod replica;
mod transport;

pub mod simulated;

pub use anti_entropy::AntiEntropy;
pub use coordinator::{Coordinator, QuorumCoordinator};
pub use error::ReplicationError;
pub use hints::HintDeliveryTask;
pub use idempotency::IdempotencyStore;
pub use replica::ReplicaStore;
pub use transport::{ReadRequest, ReadResponse, ReplicationTransport, WriteRequest, WriteResponse};
pub use types::{ObjectKey, QuorumConfig, ReadResult, VersionedDoc, WriteResult};

mod types {
    use bashfuldb_clock::{Hlc, VectorClock};
    use bashfuldb_document::{Document, ObjectId};
    use serde::{Deserialize, Serialize};

    /// A key identifying a document: collection name + object UUID.
    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    pub struct ObjectKey {
        /// Collection (namespace) this document belongs to.
        pub collection: String,
        /// Unique document identifier within the collection.
        pub object_id: ObjectId,
    }

    impl ObjectKey {
        /// Creates a new object key.
        pub fn new(collection: impl Into<String>, object_id: ObjectId) -> Self {
            Self {
                collection: collection.into(),
                object_id,
            }
        }

        /// Serialises the key as `collection/object_id_uuid` bytes.
        pub fn to_bytes(&self) -> Vec<u8> {
            format!("{}/{}", self.collection, self.object_id).into_bytes()
        }
    }

    /// Quorum configuration for replication.
    #[derive(Debug, Clone)]
    pub struct QuorumConfig {
        /// Replication factor: number of replicas per key.
        pub n: usize,
        /// Read quorum: minimum replicas that must respond for a read.
        pub r: usize,
        /// Write quorum: minimum replicas that must ack for a write.
        pub w: usize,
    }

    impl Default for QuorumConfig {
        fn default() -> Self {
            Self { n: 3, r: 2, w: 2 }
        }
    }

    /// The result of a successful quorum read.
    #[derive(Debug, Clone)]
    pub struct ReadResult {
        /// The document, or `None` if deleted / not found.
        pub document: Option<Document>,
        /// Vector clock of the winning version.
        pub version: VectorClock,
        /// Number of stale replicas to which read-repair was dispatched.
        pub repairs_triggered: usize,
    }

    /// The result of a successful quorum write or delete.
    #[derive(Debug, Clone)]
    pub struct WriteResult {
        /// Vector clock assigned to this write.
        pub version: VectorClock,
        /// Number of owner replicas that acknowledged the write.
        pub nodes_acked: usize,
        /// Number of hinted-handoff hints stored on non-owner nodes.
        pub hints_stored: usize,
    }

    /// A versioned document as persisted in a replica's storage engine.
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct VersionedDoc {
        /// Document body; `None` for tombstones.
        pub document: Option<Document>,
        /// Causal version.
        pub version: VectorClock,
        /// HLC timestamp used for LWW tiebreaks.
        pub hlc: Hlc,
        /// `true` when this entry represents a deletion tombstone.
        pub is_tombstone: bool,
    }

    impl VersionedDoc {
        /// Creates a live (non-tombstone) versioned document.
        pub fn new(document: Document, version: VectorClock, hlc: Hlc) -> Self {
            Self {
                document: Some(document),
                version,
                hlc,
                is_tombstone: false,
            }
        }

        /// Creates a tombstone (deletion marker).
        pub fn tombstone(version: VectorClock, hlc: Hlc) -> Self {
            Self {
                document: None,
                version,
                hlc,
                is_tombstone: true,
            }
        }
    }
}

/// Result type for replication operations.
pub type Result<T> = std::result::Result<T, ReplicationError>;
