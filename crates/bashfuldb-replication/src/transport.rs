use crate::{ReplicationError, Result};
use async_trait::async_trait;
use bashfuldb_clock::NodeId;

/// Network transport for replication RPCs between replicas.
///
/// The coordinator uses this to fan-out reads and writes to owner nodes.
/// Implementations must route the request even when the target is the
/// local node (the simulated transport handles this uniformly).
#[async_trait]
pub trait ReplicationTransport: Send + Sync + 'static {
    /// Sends a write (or tombstone, or hint) to the target replica.
    async fn remote_write(&self, target: &NodeId, req: WriteRequest) -> Result<WriteResponse>;

    /// Sends a read request to the target replica.
    async fn remote_read(&self, target: &NodeId, req: ReadRequest) -> Result<ReadResponse>;
}

/// A write request sent from a coordinator to a replica.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WriteRequest {
    /// The document key.
    pub key: crate::ObjectKey,
    /// Document body (`None` means "delete / tombstone").
    pub doc: Option<bashfuldb_document::Document>,
    /// Causal version for this write.
    pub version: bashfuldb_clock::VectorClock,
    /// HLC timestamp for LWW tiebreaks.
    pub hlc: bashfuldb_clock::Hlc,
    /// `true` when this is a hinted-handoff write (goes to a non-owner).
    pub is_hint: bool,
    /// The owner node that should eventually receive this hint.
    pub hint_target: Option<NodeId>,
}

/// Acknowledgement returned by a replica after a successful write.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WriteResponse {
    /// The version that was durably stored.
    pub version: bashfuldb_clock::VectorClock,
}

/// A read request sent from a coordinator to a replica.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ReadRequest {
    /// The document key to look up.
    pub key: crate::ObjectKey,
}

/// The replica's response to a read request.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ReadResponse {
    /// The versioned document stored on this replica, or `None` if absent.
    pub doc: Option<crate::VersionedDoc>,
}

/// A no-op transport that always returns `TransportError`.
///
/// Useful as a default placeholder in tests or as a type-safe sentinel.
#[allow(dead_code)]
pub struct NullTransport;

#[async_trait]
impl ReplicationTransport for NullTransport {
    async fn remote_write(&self, target: &NodeId, _req: WriteRequest) -> Result<WriteResponse> {
        Err(ReplicationError::TransportError {
            message: format!("NullTransport: no route to {target}"),
        })
    }

    async fn remote_read(&self, target: &NodeId, _req: ReadRequest) -> Result<ReadResponse> {
        Err(ReplicationError::TransportError {
            message: format!("NullTransport: no route to {target}"),
        })
    }
}
