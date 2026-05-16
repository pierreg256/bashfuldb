//! Storage engine trait and implementations (RocksDB, in-memory).
//!
//! This crate defines the [`StorageEngine`] trait — the core abstraction that
//! isolates all upper layers from the underlying storage backend. V1 ships
//! with a RocksDB implementation and an in-memory implementation for tests.
//!
//! # Key traits
//!
//! - [`StorageEngine`] — Async read/write/scan/batch/snapshot/checkpoint.
//! - [`StorageSnapshot`] — Point-in-time consistent read view.
//!
//! # Key types
//!
//! - [`WriteBatch`] / [`BatchOp`] — Atomic multi-operation writes.
//! - [`ColumnFamily`] — Well-known column family names.

pub mod conformance;
mod error;
pub mod storage;
mod traits;
mod types;

pub use error::StorageError;
pub use storage::mem::{MemEngine, MemSnapshot};
pub use storage::rocksdb::{RocksDbConfig, RocksDbEngine, RocksDbSnapshot};
pub use traits::{ScanIter, StorageEngine, StorageSnapshot};
pub use types::{BatchOp, ColumnFamily, ScanItem, WriteBatch};

/// Result type for storage operations.
pub type Result<T> = std::result::Result<T, StorageError>;
