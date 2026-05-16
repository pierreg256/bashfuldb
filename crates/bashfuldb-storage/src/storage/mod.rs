//! Concrete storage engine implementations.

pub mod mem;
pub mod rocksdb;

#[cfg(test)]
mod tests {
    use crate::{ColumnFamily, MemEngine, StorageEngine};

    #[tokio::test]
    async fn mem_engine_has_known_cf() {
        let engine = MemEngine::new();
        assert!(engine.has_column_family(ColumnFamily::DEFAULT));
    }
}
