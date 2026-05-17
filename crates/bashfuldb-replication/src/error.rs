use thiserror::Error;

/// Errors that can occur during replication operations.
#[derive(Debug, Error)]
pub enum ReplicationError {
    /// Not enough replicas acknowledged the operation.
    #[error("insufficient replicas: need {needed}, got {available}")]
    InsufficientReplicas {
        /// Number of acks required (R or W).
        needed: usize,
        /// Number of acks received.
        available: usize,
    },

    /// An idempotency key was reused with a different payload.
    #[error("idempotency conflict: same key submitted with a different payload")]
    IdempotencyConflict,

    /// The request could not be routed to the target node.
    #[error("transport error: {message}")]
    TransportError {
        /// Human-readable description.
        message: String,
    },

    /// JSON serialisation / deserialisation failed.
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// An underlying storage operation failed.
    #[error("storage error: {0}")]
    Storage(#[from] bashfuldb_storage::StorageError),

    /// A clock operation failed.
    #[error("clock error: {0}")]
    Clock(bashfuldb_clock::ClockError),

    /// A document operation failed.
    #[error("document error: {0}")]
    Document(#[from] bashfuldb_document::DocumentError),

    /// A vector-clock operation failed (e.g. capacity exceeded).
    #[error("vector clock error: {0}")]
    VectorClock(bashfuldb_clock::ClockError),
}

impl From<bashfuldb_clock::ClockError> for ReplicationError {
    fn from(e: bashfuldb_clock::ClockError) -> Self {
        ReplicationError::Clock(e)
    }
}
