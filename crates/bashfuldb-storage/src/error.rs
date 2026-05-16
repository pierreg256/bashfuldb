use thiserror::Error;

/// Errors that can occur in storage operations.
#[derive(Debug, Error)]
pub enum StorageError {
    /// The requested column family does not exist.
    #[error("unknown column family: {name:?}")]
    UnknownColumnFamily { name: String },

    /// An I/O error from the underlying engine.
    #[error("storage I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// The storage engine encountered an internal error.
    #[error("storage engine error: {message}")]
    Engine { message: String },

    /// A checkpoint/snapshot operation failed.
    #[error("checkpoint failed: {message}")]
    CheckpointFailed { message: String },

    /// The requested key was not found (used internally, not for get()).
    #[error("key not found")]
    KeyNotFound,
}
