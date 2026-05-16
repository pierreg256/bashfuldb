//! Storage engine conformance test suite.

use crate::{ColumnFamily, Result, StorageEngine, StorageError, WriteBatch};

/// Runs the storage conformance suite for any [`StorageEngine`] implementation.
pub async fn conformance_tests<E: StorageEngine>(engine: &E) -> Result<()> {
    test_put_get_delete(engine).await?;
    test_write_batch_atomicity(engine).await?;
    test_scan_range_ordering(engine).await?;
    test_snapshot_isolation(engine).await?;
    test_column_family_validation(engine).await?;
    Ok(())
}

async fn test_put_get_delete<E: StorageEngine>(engine: &E) -> Result<()> {
    let cf = ColumnFamily::DEFAULT;
    engine.put(cf, b"cf_put_get_k", b"v1").await?;
    let got = engine.get(cf, b"cf_put_get_k").await?;
    if got != Some(b"v1".to_vec()) {
        return Err(StorageError::Engine {
            message: "put/get roundtrip failed".to_string(),
        });
    }

    engine.delete(cf, b"cf_put_get_k").await?;
    let got = engine.get(cf, b"cf_put_get_k").await?;
    if got.is_some() {
        return Err(StorageError::Engine {
            message: "delete should remove value".to_string(),
        });
    }
    Ok(())
}

async fn test_write_batch_atomicity<E: StorageEngine>(engine: &E) -> Result<()> {
    let cf = ColumnFamily::DEFAULT;
    engine.delete(cf, b"cf_batch_guard").await?;
    engine.delete(cf, b"cf_batch_new").await?;
    engine.put(cf, b"cf_batch_guard", b"before").await?;

    let mut bad_batch = WriteBatch::new();
    bad_batch.put(cf, b"cf_batch_guard", b"after");
    bad_batch.put("unknown_cf", b"cf_batch_new", b"new");
    let err = engine
        .write_batch(bad_batch)
        .await
        .expect_err("batch with invalid cf should fail");
    match err {
        StorageError::UnknownColumnFamily { .. } => {}
        other => {
            return Err(StorageError::Engine {
                message: format!("unexpected error for invalid cf in batch: {other}"),
            });
        }
    }

    let guard = engine.get(cf, b"cf_batch_guard").await?;
    if guard != Some(b"before".to_vec()) {
        return Err(StorageError::Engine {
            message: "write_batch was not atomic".to_string(),
        });
    }
    if engine.get(cf, b"cf_batch_new").await?.is_some() {
        return Err(StorageError::Engine {
            message: "write_batch inserted partial data".to_string(),
        });
    }

    let mut good_batch = WriteBatch::new();
    good_batch.put(cf, b"cf_batch_guard", b"after");
    good_batch.delete(cf, b"cf_batch_new");
    engine.write_batch(good_batch).await?;
    Ok(())
}

async fn test_scan_range_ordering<E: StorageEngine>(engine: &E) -> Result<()> {
    let cf = ColumnFamily::INDEXES;
    let keys = [
        (b"scan_a".as_slice(), b"1".as_slice()),
        (b"scan_b".as_slice(), b"2".as_slice()),
        (b"scan_c".as_slice(), b"3".as_slice()),
    ];
    for (k, v) in keys {
        engine.put(cf, k, v).await?;
    }

    let middle = engine
        .scan(cf, b"scan_b", b"scan_d")?
        .collect::<Result<Vec<_>>>()?;
    if middle.iter().map(|(k, _)| k.as_slice()).collect::<Vec<_>>()
        != vec![b"scan_b".as_slice(), b"scan_c".as_slice()]
    {
        return Err(StorageError::Engine {
            message: "scan [start,end) ordering is incorrect".to_string(),
        });
    }

    let unbounded = engine.scan(cf, b"", b"")?.collect::<Result<Vec<_>>>()?;
    if unbounded.len() < 3 {
        return Err(StorageError::Engine {
            message: "scan with empty bounds should be unbounded".to_string(),
        });
    }
    Ok(())
}

async fn test_snapshot_isolation<E: StorageEngine>(engine: &E) -> Result<()> {
    let cf = ColumnFamily::METADATA;
    engine.put(cf, b"snap_key", b"before").await?;
    let snapshot = engine.snapshot()?;
    engine.put(cf, b"snap_key", b"after").await?;

    let snap_val = snapshot.get(cf, b"snap_key")?;
    if snap_val != Some(b"before".to_vec()) {
        return Err(StorageError::Engine {
            message: "snapshot is not isolated".to_string(),
        });
    }
    let live_val = engine.get(cf, b"snap_key").await?;
    if live_val != Some(b"after".to_vec()) {
        return Err(StorageError::Engine {
            message: "live view should see latest write".to_string(),
        });
    }
    Ok(())
}

async fn test_column_family_validation<E: StorageEngine>(engine: &E) -> Result<()> {
    if !engine.has_column_family(ColumnFamily::SYSTEM) {
        return Err(StorageError::Engine {
            message: "known column family should exist".to_string(),
        });
    }
    if engine.has_column_family("unknown_cf") {
        return Err(StorageError::Engine {
            message: "unknown column family should not exist".to_string(),
        });
    }
    match engine.get("unknown_cf", b"k").await {
        Err(StorageError::UnknownColumnFamily { .. }) => Ok(()),
        Ok(_) => Err(StorageError::Engine {
            message: "invalid column family should return an error".to_string(),
        }),
        Err(err) => Err(StorageError::Engine {
            message: format!("unexpected error for invalid column family: {err}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MemEngine, RocksDbConfig, RocksDbEngine};
    use tempfile::TempDir;

    #[tokio::test]
    async fn conformance_for_mem_engine() {
        let engine = MemEngine::new();
        conformance_tests(&engine)
            .await
            .expect("mem engine conformance should pass");
    }

    #[tokio::test]
    async fn conformance_for_rocksdb_engine() {
        let temp = TempDir::new().expect("tempdir should be created");
        let engine = RocksDbEngine::open(temp.path(), RocksDbConfig::default())
            .expect("rocksdb should open");
        conformance_tests(&engine)
            .await
            .expect("rocksdb engine conformance should pass");
    }
}
