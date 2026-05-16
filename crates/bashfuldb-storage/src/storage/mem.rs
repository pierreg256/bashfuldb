//! In-memory storage engine implementation.

use crate::{
    BatchOp, ColumnFamily, Result, ScanIter, StorageEngine, StorageError, StorageSnapshot,
    WriteBatch,
};
use async_trait::async_trait;
use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::path::Path;
use std::sync::{Arc, RwLock};

type KvMap = BTreeMap<Vec<u8>, Vec<u8>>;
type CfMap = HashMap<&'static str, KvMap>;

/// In-memory storage engine backed by a [`BTreeMap`] per column family.
#[derive(Debug, Default)]
pub struct MemEngine {
    cfs: RwLock<CfMap>,
}

impl MemEngine {
    /// Creates a new empty in-memory engine with all known column families.
    pub fn new() -> Self {
        Self {
            cfs: RwLock::new(init_cf_map()),
        }
    }
}

#[async_trait]
impl StorageEngine for MemEngine {
    async fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let cf_name = validate_cf_name(cf)?;
        let mut guard = self.cfs.write().map_err(|_| StorageError::Engine {
            message: "mem engine write lock poisoned".to_string(),
        })?;
        guard
            .get_mut(cf_name)
            .ok_or_else(|| StorageError::UnknownColumnFamily {
                name: cf.to_string(),
            })?
            .insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    async fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let cf_name = validate_cf_name(cf)?;
        let guard = self.cfs.read().map_err(|_| StorageError::Engine {
            message: "mem engine read lock poisoned".to_string(),
        })?;
        Ok(guard
            .get(cf_name)
            .and_then(|cf_map| cf_map.get(key).cloned()))
    }

    async fn delete(&self, cf: &str, key: &[u8]) -> Result<()> {
        let cf_name = validate_cf_name(cf)?;
        let mut guard = self.cfs.write().map_err(|_| StorageError::Engine {
            message: "mem engine write lock poisoned".to_string(),
        })?;
        let cf_map = guard
            .get_mut(cf_name)
            .ok_or_else(|| StorageError::UnknownColumnFamily {
                name: cf.to_string(),
            })?;
        cf_map.remove(key);
        Ok(())
    }

    fn scan(&self, cf: &str, start: &[u8], end: &[u8]) -> Result<ScanIter> {
        let cf_name = validate_cf_name(cf)?;
        let guard = self.cfs.read().map_err(|_| StorageError::Engine {
            message: "mem engine read lock poisoned".to_string(),
        })?;
        let cf_map = guard
            .get(cf_name)
            .ok_or_else(|| StorageError::UnknownColumnFamily {
                name: cf.to_string(),
            })?;
        let (start_bound, end_bound) = range_bounds(start, end);
        let rows: Vec<_> = cf_map
            .range((start_bound, end_bound))
            .map(|(k, v)| Ok((k.clone(), v.clone())))
            .collect();
        Ok(Box::new(rows.into_iter()))
    }

    async fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        for op in batch.ops() {
            match op {
                BatchOp::Put { cf, .. } | BatchOp::Delete { cf, .. } => {
                    validate_cf_name(cf)?;
                }
            }
        }

        let mut guard = self.cfs.write().map_err(|_| StorageError::Engine {
            message: "mem engine write lock poisoned".to_string(),
        })?;
        for op in batch.into_ops() {
            match op {
                BatchOp::Put { cf, key, value } => {
                    let cf_name = validate_cf_name(&cf)?;
                    guard
                        .get_mut(cf_name)
                        .ok_or(StorageError::UnknownColumnFamily { name: cf })?
                        .insert(key, value);
                }
                BatchOp::Delete { cf, key } => {
                    let cf_name = validate_cf_name(&cf)?;
                    guard
                        .get_mut(cf_name)
                        .ok_or(StorageError::UnknownColumnFamily { name: cf })?
                        .remove(&key);
                }
            }
        }
        Ok(())
    }

    fn snapshot(&self) -> Result<Arc<dyn StorageSnapshot>> {
        let guard = self.cfs.read().map_err(|_| StorageError::Engine {
            message: "mem engine read lock poisoned".to_string(),
        })?;
        let snap = MemSnapshot { cfs: guard.clone() };
        Ok(Arc::new(snap))
    }

    async fn flush(&self) -> Result<()> {
        Ok(())
    }

    async fn checkpoint(&self, path: &Path) -> Result<()> {
        std::fs::create_dir_all(path)?;
        Ok(())
    }

    fn has_column_family(&self, cf: &str) -> bool {
        ColumnFamily::all().contains(&cf)
    }
}

/// A point-in-time snapshot for [`MemEngine`].
#[derive(Debug, Clone)]
pub struct MemSnapshot {
    cfs: CfMap,
}

impl StorageSnapshot for MemSnapshot {
    fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let cf_name = validate_cf_name(cf)?;
        Ok(self
            .cfs
            .get(cf_name)
            .and_then(|cf_map| cf_map.get(key).cloned()))
    }

    fn scan(&self, cf: &str, start: &[u8], end: &[u8]) -> Result<ScanIter> {
        let cf_name = validate_cf_name(cf)?;
        let cf_map = self
            .cfs
            .get(cf_name)
            .ok_or_else(|| StorageError::UnknownColumnFamily {
                name: cf.to_string(),
            })?;
        let (start_bound, end_bound) = range_bounds(start, end);
        let rows: Vec<_> = cf_map
            .range((start_bound, end_bound))
            .map(|(k, v)| Ok((k.clone(), v.clone())))
            .collect();
        Ok(Box::new(rows.into_iter()))
    }
}

fn init_cf_map() -> CfMap {
    let mut map = HashMap::new();
    for cf in ColumnFamily::all() {
        map.insert(*cf, BTreeMap::new());
    }
    map
}

fn validate_cf_name(cf: &str) -> Result<&'static str> {
    ColumnFamily::all()
        .iter()
        .copied()
        .find(|name| *name == cf)
        .ok_or_else(|| StorageError::UnknownColumnFamily {
            name: cf.to_string(),
        })
}

fn range_bounds(start: &[u8], end: &[u8]) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
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
    (start_bound, end_bound)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ColumnFamily, StorageEngine};

    #[tokio::test]
    async fn mem_engine_roundtrip() {
        let engine = MemEngine::new();
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
