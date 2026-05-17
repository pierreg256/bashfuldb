use crate::{ReplicationError, Result, WriteResult};
use bashfuldb_document::Document;
use std::collections::HashMap;
use std::time::{Duration, Instant};

const TTL: Duration = Duration::from_secs(15 * 60);

struct Entry {
    /// Serialised document body for equality comparison.
    doc_bytes: Vec<u8>,
    /// Cached result to return on a matching replay.
    result: WriteResult,
    /// Wall-clock time this entry was created.
    created_at: Instant,
}

/// In-memory idempotency key store with a 15-minute TTL.
///
/// Callers must hold a `tokio::sync::Mutex` around this type to share it
/// between async tasks.
pub struct IdempotencyStore {
    cache: HashMap<String, Entry>,
}

impl Default for IdempotencyStore {
    fn default() -> Self {
        Self::new()
    }
}

impl IdempotencyStore {
    /// Creates an empty store.
    pub fn new() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }

    /// Looks up an idempotency key.
    ///
    /// Returns:
    /// - `Ok(Some(result))` – same key, same payload → return cached result.
    /// - `Err(IdempotencyConflict)` – same key, different payload → 409.
    /// - `Ok(None)` – key not found (or expired).
    pub fn get(&mut self, key: &str, doc: &Document) -> Result<Option<WriteResult>> {
        self.evict_expired();
        match self.cache.get(key) {
            None => Ok(None),
            Some(entry) => {
                let incoming_bytes =
                    serde_json::to_vec(doc).map_err(ReplicationError::Serialization)?;
                if entry.doc_bytes == incoming_bytes {
                    Ok(Some(entry.result.clone()))
                } else {
                    Err(ReplicationError::IdempotencyConflict)
                }
            }
        }
    }

    /// Stores a new idempotency entry.
    pub fn store(&mut self, key: String, doc: &Document, result: WriteResult) -> Result<()> {
        self.evict_expired();
        let doc_bytes = serde_json::to_vec(doc).map_err(ReplicationError::Serialization)?;
        self.cache.insert(
            key,
            Entry {
                doc_bytes,
                result,
                created_at: Instant::now(),
            },
        );
        Ok(())
    }

    fn evict_expired(&mut self) {
        self.cache.retain(|_, e| e.created_at.elapsed() < TTL);
    }
}
