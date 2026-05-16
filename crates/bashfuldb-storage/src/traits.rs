use crate::{Result, ScanItem, WriteBatch};
use async_trait::async_trait;
use std::path::Path;
use std::sync::Arc;

/// A sendable, owned iterator over scan results.
pub type ScanIter = Box<dyn Iterator<Item = Result<ScanItem>> + Send + 'static>;

/// Core storage engine abstraction.
///
/// All direct access to the underlying storage (RocksDB, in-memory, etc.)
/// MUST go through this trait. Upper layers (replication, API, schema) MUST
/// NOT depend on any concrete engine implementation.
///
/// Every implementation MUST support all column families listed in
/// [`ColumnFamily::all()`](crate::ColumnFamily::all).
#[async_trait]
pub trait StorageEngine: Send + Sync + 'static {
    /// Writes a key-value pair into the given column family.
    async fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()>;

    /// Reads a value by key from the given column family.
    ///
    /// Returns `None` if the key does not exist.
    async fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Deletes a key from the given column family.
    ///
    /// Deleting a non-existent key is a no-op (not an error).
    async fn delete(&self, cf: &str, key: &[u8]) -> Result<()>;

    /// Iterates over a key range `[start, end)` in the given column family.
    ///
    /// Returns an owned, `Send` iterator. Keys are in lexicographic order.
    /// An empty `start` means "from the beginning"; an empty `end` means
    /// "to the end".
    fn scan(&self, cf: &str, start: &[u8], end: &[u8]) -> Result<ScanIter>;

    /// Atomically applies a batch of mutations.
    ///
    /// Either all operations in the batch succeed, or none do.
    async fn write_batch(&self, batch: WriteBatch) -> Result<()>;

    /// Creates a point-in-time consistent snapshot for reads.
    ///
    /// The snapshot sees all writes committed before its creation and none
    /// committed after. Returned as `Arc` so it can be shared across tasks.
    fn snapshot(&self) -> Result<Arc<dyn StorageSnapshot>>;

    /// Forces all buffered writes to persistent storage.
    async fn flush(&self) -> Result<()>;

    /// Creates a checkpoint (backup) of the current state in the given directory.
    async fn checkpoint(&self, path: &Path) -> Result<()>;

    /// Returns true if the given column family exists.
    fn has_column_family(&self, cf: &str) -> bool;
}

/// A point-in-time consistent read view of the storage.
///
/// Snapshots are immutable — they see the state as of their creation time.
/// They MUST NOT block writes on the main engine.
pub trait StorageSnapshot: Send + Sync {
    /// Reads a value by key from the snapshot.
    fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Iterates over a key range `[start, end)` in the snapshot.
    ///
    /// Returns an owned, `Send` iterator.
    fn scan(&self, cf: &str, start: &[u8], end: &[u8]) -> Result<ScanIter>;
}
