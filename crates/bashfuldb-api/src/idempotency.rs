//! Idempotency key cache with 15-minute TTL.
//!
//! Stores the serialised response body keyed by `(idempotency_key, payload_hash)`
//! so that replayed requests with the same key *and* payload return the cached
//! response, while a replay with a different payload returns 409 Conflict.

use serde_json::Value as JsonValue;
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

/// Retention duration for idempotency cache entries (15 minutes).
pub const RETENTION: Duration = Duration::from_secs(15 * 60);

/// A cached response entry.
#[derive(Debug, Clone)]
pub struct CachedResponse {
    /// The serialised response body.
    pub body: JsonValue,
    /// HTTP status code.
    pub status: u16,
    /// FNV-1a hash of the original request payload.
    pub payload_hash: u64,
    /// Time at which the entry was stored.
    pub stored_at: Instant,
}

/// In-memory idempotency cache.
///
/// Pruning happens lazily on every `get` / `store` call.
#[derive(Debug, Default)]
pub struct IdempotencyCache {
    entries: HashMap<String, CachedResponse>,
}

impl IdempotencyCache {
    /// Creates an empty cache.
    pub fn new() -> Self {
        Self::default()
    }

    /// Hashes a request payload using FNV-1a.
    pub fn hash_payload(payload: &[u8]) -> u64 {
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for &byte in payload {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
        hash
    }

    /// Looks up an idempotency key.
    ///
    /// Returns:
    /// - `Ok(Some(entry))` — key exists and payload matches → replay cached response.
    /// - `Ok(None)` — key not seen before → proceed normally.
    /// - `Err(())` — key exists but payload differs → 409 Conflict.
    pub fn get(&mut self, key: &str, payload_hash: u64) -> Result<Option<CachedResponse>, ()> {
        self.prune();
        match self.entries.get(key) {
            None => Ok(None),
            Some(entry) if entry.payload_hash == payload_hash => Ok(Some(entry.clone())),
            Some(_) => Err(()),
        }
    }

    /// Stores a completed response in the cache.
    pub fn store(&mut self, key: String, payload_hash: u64, body: JsonValue, status: u16) {
        self.entries.insert(
            key,
            CachedResponse {
                body,
                status,
                payload_hash,
                stored_at: Instant::now(),
            },
        );
    }

    /// Removes expired entries.
    fn prune(&mut self) {
        self.entries
            .retain(|_, v| v.stored_at.elapsed() < RETENTION);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_key_returns_none() {
        let mut cache = IdempotencyCache::new();
        assert!(cache.get("k1", 42).unwrap().is_none());
    }

    #[test]
    fn same_key_same_payload_returns_cached() {
        let mut cache = IdempotencyCache::new();
        cache.store("k1".into(), 42, serde_json::json!({"id": "1"}), 201);
        let entry = cache.get("k1", 42).unwrap().unwrap();
        assert_eq!(entry.status, 201);
    }

    #[test]
    fn same_key_different_payload_returns_conflict() {
        let mut cache = IdempotencyCache::new();
        cache.store("k1".into(), 42, serde_json::json!({}), 201);
        assert!(cache.get("k1", 99).is_err());
    }

    #[test]
    fn hash_payload_is_deterministic() {
        let a = IdempotencyCache::hash_payload(b"hello world");
        let b = IdempotencyCache::hash_payload(b"hello world");
        assert_eq!(a, b);
    }

    #[test]
    fn hash_payload_differs_for_different_inputs() {
        let a = IdempotencyCache::hash_payload(b"hello");
        let b = IdempotencyCache::hash_payload(b"world");
        assert_ne!(a, b);
    }
}
