//! RocksDB storage engine implementation.

use crate::{
    BatchOp, ColumnFamily, Result, ScanIter, StorageEngine, StorageError, StorageSnapshot,
    WriteBatch,
};
use async_trait::async_trait;
use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamilyDescriptor, DBCompressionType, DBWithThreadMode,
    Direction, IteratorMode, MultiThreaded, Options, ReadOptions, SnapshotWithThreadMode,
    WriteBatch as RocksWriteBatch, WriteOptions,
};
use std::path::Path;
use std::sync::Arc;

type RocksDbHandle = DBWithThreadMode<MultiThreaded>;

/// Configuration values used to open and tune RocksDB.
#[derive(Debug, Clone)]
pub struct RocksDbConfig {
    /// LSM write buffer size in bytes.
    pub write_buffer_size: usize,
    /// Number of memtables before flush/slowdown.
    pub max_write_buffer_number: i32,
    /// Target SST file size in bytes.
    pub target_file_size_base: u64,
    /// Maximum number of background jobs.
    pub max_background_jobs: i32,
    /// Bloom filter bits per key.
    pub bloom_filter_bits_per_key: f64,
    /// Shared block cache size in bytes.
    pub block_cache_size: usize,
    /// Whether to create the database if missing.
    pub create_if_missing: bool,
    /// Whether to create missing column families.
    pub create_missing_column_families: bool,
    /// Whether writes must fsync WAL before returning.
    pub wal_sync: bool,
}

impl Default for RocksDbConfig {
    fn default() -> Self {
        Self {
            write_buffer_size: 64 * 1024 * 1024,
            max_write_buffer_number: 3,
            target_file_size_base: 64 * 1024 * 1024,
            max_background_jobs: 4,
            bloom_filter_bits_per_key: 10.0,
            block_cache_size: 128 * 1024 * 1024,
            create_if_missing: true,
            create_missing_column_families: true,
            wal_sync: true,
        }
    }
}

/// RocksDB-backed implementation of [`StorageEngine`].
pub struct RocksDbEngine {
    db: Arc<RocksDbHandle>,
    write_options: WriteOptions,
}

impl RocksDbEngine {
    /// Opens (or creates) a RocksDB database at `path` with the provided config.
    pub fn open(path: &Path, config: RocksDbConfig) -> Result<Self> {
        let cache = Cache::new_lru_cache(config.block_cache_size);
        let db_opts = base_options(&config);
        // V1 applies a uniform baseline profile across CFs; operators can tune
        // the global values through `RocksDbConfig`.
        let descriptors = ColumnFamily::all()
            .iter()
            .map(|name| {
                let mut cf_opts = base_options(&config);
                let mut block_opts = BlockBasedOptions::default();
                block_opts.set_block_cache(&cache);
                block_opts.set_bloom_filter(config.bloom_filter_bits_per_key, false);
                cf_opts.set_block_based_table_factory(&block_opts);
                ColumnFamilyDescriptor::new(*name, cf_opts)
            })
            .collect::<Vec<_>>();

        let db =
            RocksDbHandle::open_cf_descriptors(&db_opts, path, descriptors).map_err(engine_err)?;
        let mut write_options = WriteOptions::default();
        write_options.set_sync(config.wal_sync);
        Ok(Self {
            db: Arc::new(db),
            write_options,
        })
    }

    fn cf_handle(&self, cf: &str) -> Result<Arc<rocksdb::BoundColumnFamily<'_>>> {
        validate_cf_name(cf)?;
        self.db
            .cf_handle(cf)
            .ok_or_else(|| StorageError::UnknownColumnFamily {
                name: cf.to_string(),
            })
    }

    fn scan_with_read_options(
        &self,
        cf: &str,
        start: &[u8],
        end: &[u8],
        mut read_options: ReadOptions,
    ) -> Result<ScanIter> {
        let cf_handle = self.cf_handle(cf)?;
        if !end.is_empty() {
            read_options.set_iterate_upper_bound(end.to_vec());
        }
        let mode = if start.is_empty() {
            IteratorMode::Start
        } else {
            IteratorMode::From(start, Direction::Forward)
        };
        let iter = self.db.iterator_cf_opt(&cf_handle, read_options, mode);
        let rows = iter
            .map(|item| {
                item.map(|(k, v)| (k.to_vec(), v.to_vec()))
                    .map_err(engine_err)
            })
            .collect::<Vec<_>>();
        Ok(Box::new(rows.into_iter()))
    }
}

#[async_trait]
impl StorageEngine for RocksDbEngine {
    async fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let cf_handle = self.cf_handle(cf)?;
        self.db
            .put_cf_opt(&cf_handle, key, value, &self.write_options)
            .map_err(engine_err)
    }

    async fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let cf_handle = self.cf_handle(cf)?;
        self.db.get_cf(&cf_handle, key).map_err(engine_err)
    }

    async fn delete(&self, cf: &str, key: &[u8]) -> Result<()> {
        let cf_handle = self.cf_handle(cf)?;
        self.db
            .delete_cf_opt(&cf_handle, key, &self.write_options)
            .map_err(engine_err)
    }

    fn scan(&self, cf: &str, start: &[u8], end: &[u8]) -> Result<ScanIter> {
        self.scan_with_read_options(cf, start, end, ReadOptions::default())
    }

    async fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        for op in batch.ops() {
            match op {
                BatchOp::Put { cf, .. } | BatchOp::Delete { cf, .. } => {
                    validate_cf_name(cf)?;
                }
            }
        }

        let mut rocks_batch = RocksWriteBatch::default();
        for op in batch.into_ops() {
            match op {
                BatchOp::Put { cf, key, value } => {
                    let cf_handle = self.cf_handle(&cf)?;
                    rocks_batch.put_cf(&cf_handle, key, value);
                }
                BatchOp::Delete { cf, key } => {
                    let cf_handle = self.cf_handle(&cf)?;
                    rocks_batch.delete_cf(&cf_handle, key);
                }
            }
        }
        self.db
            .write_opt(rocks_batch, &self.write_options)
            .map_err(engine_err)
    }

    fn snapshot(&self) -> Result<Arc<dyn StorageSnapshot>> {
        let snapshot = self.db.snapshot();
        let snap = RocksDbSnapshot::from_snapshot(self.db.clone(), snapshot)?;
        Ok(Arc::new(snap))
    }

    async fn flush(&self) -> Result<()> {
        for cf in ColumnFamily::all() {
            let cf_handle = self.cf_handle(cf)?;
            self.db.flush_cf(&cf_handle).map_err(engine_err)?;
        }
        Ok(())
    }

    async fn checkpoint(&self, path: &Path) -> Result<()> {
        let checkpoint = rocksdb::checkpoint::Checkpoint::new(&self.db).map_err(engine_err)?;
        checkpoint
            .create_checkpoint(path)
            .map_err(|e| StorageError::CheckpointFailed {
                message: e.to_string(),
            })
    }

    fn has_column_family(&self, cf: &str) -> bool {
        ColumnFamily::all().contains(&cf)
    }
}

/// Point-in-time consistent snapshot for [`RocksDbEngine`].
///
/// This snapshot materializes a stable copy of all column families from a
/// native RocksDB snapshot at creation time.
#[derive(Debug, Clone)]
pub struct RocksDbSnapshot {
    data: std::collections::HashMap<String, std::collections::BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl RocksDbSnapshot {
    fn from_snapshot(
        db: Arc<RocksDbHandle>,
        snapshot: SnapshotWithThreadMode<'_, RocksDbHandle>,
    ) -> Result<Self> {
        let mut data = std::collections::HashMap::new();
        for cf in ColumnFamily::all() {
            let cf_handle = db
                .cf_handle(cf)
                .ok_or_else(|| StorageError::UnknownColumnFamily {
                    name: (*cf).to_string(),
                })?;
            let mut read_options = ReadOptions::default();
            read_options.set_snapshot(&snapshot);
            let iter = db.iterator_cf_opt(&cf_handle, read_options, IteratorMode::Start);
            let cf_data = iter
                .map(|item| {
                    item.map(|(k, v)| (k.to_vec(), v.to_vec()))
                        .map_err(engine_err)
                })
                .collect::<Result<std::collections::BTreeMap<Vec<u8>, Vec<u8>>>>()?;
            data.insert((*cf).to_string(), cf_data);
        }
        Ok(Self { data })
    }
}

impl StorageSnapshot for RocksDbSnapshot {
    fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        validate_cf_name(cf)?;
        Ok(self
            .data
            .get(cf)
            .and_then(|cf_map| cf_map.get(key).cloned()))
    }

    fn scan(&self, cf: &str, start: &[u8], end: &[u8]) -> Result<ScanIter> {
        use std::ops::Bound;

        validate_cf_name(cf)?;
        let cf_map = self
            .data
            .get(cf)
            .ok_or_else(|| StorageError::UnknownColumnFamily {
                name: cf.to_string(),
            })?;

        let start_bound = if start.is_empty() {
            Bound::Unbounded
        } else {
            Bound::Included(start.to_vec())
        };
        let end_bound = if end.is_empty() {
            Bound::Unbounded
        } else {
            Bound::Excluded(end.to_vec())
        };
        let rows = cf_map
            .range((start_bound, end_bound))
            .map(|(k, v)| Ok((k.clone(), v.clone())))
            .collect::<Vec<_>>();
        Ok(Box::new(rows.into_iter()))
    }
}

fn base_options(config: &RocksDbConfig) -> Options {
    let mut options = Options::default();
    options.create_if_missing(config.create_if_missing);
    options.create_missing_column_families(config.create_missing_column_families);
    options.set_write_buffer_size(config.write_buffer_size);
    options.set_max_write_buffer_number(config.max_write_buffer_number);
    options.set_target_file_size_base(config.target_file_size_base);
    options.set_max_background_jobs(config.max_background_jobs);
    options.set_compression_per_level(&[
        DBCompressionType::Lz4,
        DBCompressionType::Lz4,
        DBCompressionType::Zstd,
        DBCompressionType::Zstd,
        DBCompressionType::Zstd,
        DBCompressionType::Zstd,
        DBCompressionType::Zstd,
    ]);
    options.set_manual_wal_flush(false);
    options
}

fn validate_cf_name(cf: &str) -> Result<()> {
    if ColumnFamily::all().contains(&cf) {
        Ok(())
    } else {
        Err(StorageError::UnknownColumnFamily {
            name: cf.to_string(),
        })
    }
}

fn engine_err(err: impl std::fmt::Display) -> StorageError {
    StorageError::Engine {
        message: err.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ColumnFamily, StorageEngine};
    use tempfile::TempDir;

    #[tokio::test]
    async fn rocksdb_roundtrip() {
        let temp = TempDir::new().expect("tempdir should be created");
        let engine = RocksDbEngine::open(temp.path(), RocksDbConfig::default())
            .expect("rocksdb should open");
        engine
            .put(ColumnFamily::DEFAULT, b"k", b"v")
            .await
            .expect("put should succeed");
        assert_eq!(
            engine
                .get(ColumnFamily::DEFAULT, b"k")
                .await
                .expect("get should succeed"),
            Some(b"v".to_vec())
        );
    }
}
