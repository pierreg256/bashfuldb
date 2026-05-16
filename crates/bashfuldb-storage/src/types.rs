/// Well-known column family names used across BashfulDB.
///
/// All storage engine implementations MUST support these column families.
pub struct ColumnFamily;

impl ColumnFamily {
    /// Object data: key = `{collection_id}/{object_id}`.
    pub const DEFAULT: &'static str = "default";

    /// Schema, collection configuration, vector clocks.
    pub const METADATA: &'static str = "metadata";

    /// Secondary indexes: key = `{index_id}/{field_value}/{object_id}`.
    pub const INDEXES: &'static str = "indexes";

    /// Tombstones with TTL: key = `{collection_id}/{object_id}`.
    pub const TOMBSTONES: &'static str = "tombstones";

    /// Hinted handoff: key = `{target_node}/{timestamp}/{object_id}`.
    pub const HINTS: &'static str = "hints";

    /// Node state, membership, persisted HLC.
    pub const SYSTEM: &'static str = "system";

    /// Returns all well-known column family names.
    pub fn all() -> &'static [&'static str] {
        &[
            Self::DEFAULT,
            Self::METADATA,
            Self::INDEXES,
            Self::TOMBSTONES,
            Self::HINTS,
            Self::SYSTEM,
        ]
    }
}

/// A single key-value pair returned from a scan.
pub type ScanItem = (Vec<u8>, Vec<u8>);

/// An atomic batch of mutations to apply together.
#[derive(Debug, Clone, Default)]
pub struct WriteBatch {
    ops: Vec<BatchOp>,
}

impl WriteBatch {
    /// Creates an empty write batch.
    pub fn new() -> Self {
        Self { ops: Vec::new() }
    }

    /// Creates a write batch with pre-allocated capacity.
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            ops: Vec::with_capacity(cap),
        }
    }

    /// Adds a put operation to the batch.
    pub fn put(&mut self, cf: impl Into<String>, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) {
        self.ops.push(BatchOp::Put {
            cf: cf.into(),
            key: key.into(),
            value: value.into(),
        });
    }

    /// Adds a delete operation to the batch.
    pub fn delete(&mut self, cf: impl Into<String>, key: impl Into<Vec<u8>>) {
        self.ops.push(BatchOp::Delete {
            cf: cf.into(),
            key: key.into(),
        });
    }

    /// Returns the operations in this batch.
    pub fn ops(&self) -> &[BatchOp] {
        &self.ops
    }

    /// Returns the number of operations in this batch.
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// Returns true if the batch has no operations.
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Consumes the batch and returns the operations.
    pub fn into_ops(self) -> Vec<BatchOp> {
        self.ops
    }
}

/// A single mutation operation within a [`WriteBatch`].
#[derive(Debug, Clone)]
pub enum BatchOp {
    /// Write a key-value pair into a column family.
    Put {
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    /// Delete a key from a column family.
    Delete {
        cf: String,
        key: Vec<u8>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_batch_builder() {
        let mut batch = WriteBatch::new();
        batch.put(ColumnFamily::DEFAULT, b"key1".to_vec(), b"val1".to_vec());
        batch.delete(ColumnFamily::METADATA, b"key2".to_vec());
        assert_eq!(batch.len(), 2);
        assert!(!batch.is_empty());
    }

    #[test]
    fn column_family_all() {
        let all = ColumnFamily::all();
        assert_eq!(all.len(), 6);
        assert!(all.contains(&"default"));
        assert!(all.contains(&"system"));
    }
}
