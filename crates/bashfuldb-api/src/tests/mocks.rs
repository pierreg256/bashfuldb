//! Mock implementations of all dependency traits for integration testing.

use async_trait::async_trait;
use bashfuldb_auth::{
    Action, AuthError, Authenticator, Authorizer, Claims, Credentials, Resource, Role, TokenPair,
};
use bashfuldb_clock::VectorClock;
use bashfuldb_document::Document;
use bashfuldb_replication::{ObjectKey, ReadResult, WriteResult, coordinator::Coordinator};
use bashfuldb_schema::{CollectionId, CollectionSchema, IndexDefinition, IndexId, SchemaVersion};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::AppState;

// ─── Token constants ──────────────────────────────────────────────────────────

pub const VALID_TOKEN: &str = "valid_test_token";
pub const ADMIN_TOKEN: &str = "admin_test_token";
#[allow(dead_code)]
pub const INVALID_TOKEN: &str = "bad_token";

// ─── MockAuthenticator ────────────────────────────────────────────────────────

pub struct MockAuthenticator;

impl MockAuthenticator {
    fn make_claims(sub: &str, roles: Vec<Role>) -> Claims {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs();
        Claims::new(sub.into(), Some("acme".into()), roles, now, now + 3600)
    }
}

#[async_trait]
impl Authenticator for MockAuthenticator {
    async fn login(&self, creds: &Credentials) -> bashfuldb_auth::Result<TokenPair> {
        if creds.username == "admin" && creds.password == "password" {
            Ok(TokenPair {
                access_token: ADMIN_TOKEN.into(),
                refresh_token: "refresh_admin".into(),
                access_ttl: Duration::from_secs(3600),
                refresh_ttl: Duration::from_secs(172800),
            })
        } else if creds.username == "user" && creds.password == "password" {
            Ok(TokenPair {
                access_token: VALID_TOKEN.into(),
                refresh_token: "refresh_user".into(),
                access_ttl: Duration::from_secs(3600),
                refresh_ttl: Duration::from_secs(172800),
            })
        } else {
            Err(AuthError::InvalidCredentials)
        }
    }

    async fn refresh(&self, refresh_token: &str) -> bashfuldb_auth::Result<TokenPair> {
        if refresh_token == "refresh_user" || refresh_token == "refresh_admin" {
            let token = if refresh_token == "refresh_admin" {
                ADMIN_TOKEN
            } else {
                VALID_TOKEN
            };
            Ok(TokenPair {
                access_token: token.into(),
                refresh_token: refresh_token.into(),
                access_ttl: Duration::from_secs(3600),
                refresh_ttl: Duration::from_secs(172800),
            })
        } else {
            Err(AuthError::InvalidToken {
                reason: "unknown refresh token".into(),
            })
        }
    }

    async fn verify(&self, access_token: &str) -> bashfuldb_auth::Result<Claims> {
        match access_token {
            VALID_TOKEN => Ok(Self::make_claims("user1", vec![Role::ReadWrite])),
            ADMIN_TOKEN => Ok(Self::make_claims("admin1", vec![Role::ServerAdmin])),
            _ => Err(AuthError::InvalidToken {
                reason: "token not recognised".into(),
            }),
        }
    }

    async fn logout(&self, _token: &str) -> bashfuldb_auth::Result<()> {
        Ok(())
    }
}

// ─── MockAuthorizer ───────────────────────────────────────────────────────────

pub struct MockAuthorizer;

#[async_trait]
impl Authorizer for MockAuthorizer {
    async fn check(
        &self,
        claims: &Claims,
        action: Action,
        resource: &Resource,
    ) -> bashfuldb_auth::Result<()> {
        let highest = claims.highest_role().copied().unwrap_or(Role::ReadOnly);
        if highest.permits(action) {
            Ok(())
        } else {
            Err(AuthError::PermissionDenied {
                action,
                resource: resource.to_string(),
            })
        }
    }
}

// ─── MockCoordinator ─────────────────────────────────────────────────────────

#[derive(Clone, Default)]
pub struct MockCoordinator {
    /// In-memory store: key → document
    pub store: Arc<Mutex<HashMap<String, Document>>>,
}

impl MockCoordinator {
    pub fn new() -> Self {
        Self::default()
    }

    fn key_str(key: &ObjectKey) -> String {
        format!("{}/{}", key.collection, key.object_id)
    }

    /// Pre-seeds a document into the mock store.
    pub fn seed(&self, key: &ObjectKey, doc: Document) {
        let mut store = self.store.lock().unwrap();
        store.insert(Self::key_str(key), doc);
    }
}

#[async_trait]
impl Coordinator for MockCoordinator {
    async fn quorum_read(&self, key: &ObjectKey) -> bashfuldb_replication::Result<ReadResult> {
        let store = self.store.lock().unwrap();
        let doc = store.get(&Self::key_str(key)).cloned();
        Ok(ReadResult {
            document: doc,
            version: VectorClock::new(),
            repairs_triggered: 0,
        })
    }

    async fn quorum_write(
        &self,
        key: &ObjectKey,
        doc: &Document,
        _idempotency_key: Option<&str>,
    ) -> bashfuldb_replication::Result<WriteResult> {
        let mut store = self.store.lock().unwrap();
        store.insert(Self::key_str(key), doc.clone());
        Ok(WriteResult {
            version: VectorClock::new(),
            nodes_acked: 2,
            hints_stored: 0,
        })
    }

    async fn quorum_delete(&self, key: &ObjectKey) -> bashfuldb_replication::Result<WriteResult> {
        let mut store = self.store.lock().unwrap();
        store.remove(&Self::key_str(key));
        Ok(WriteResult {
            version: VectorClock::new(),
            nodes_acked: 2,
            hints_stored: 0,
        })
    }
}

// ─── MockSchemaManager ────────────────────────────────────────────────────────

pub struct MockSchemaManager {
    /// Collections that have indexed fields.
    pub indexed_fields: HashMap<String, Vec<String>>,
}

impl MockSchemaManager {
    pub fn new() -> Self {
        Self {
            indexed_fields: HashMap::new(),
        }
    }

    pub fn with_index(mut self, collection: &str, field: &str) -> Self {
        self.indexed_fields
            .entry(collection.to_string())
            .or_default()
            .push(field.to_string());
        self
    }
}

#[async_trait]
impl bashfuldb_schema::SchemaManager for MockSchemaManager {
    async fn create_index(
        &self,
        _coll: &CollectionId,
        _field: &str,
    ) -> bashfuldb_schema::Result<SchemaVersion> {
        Ok(2)
    }

    async fn drop_index(
        &self,
        _coll: &CollectionId,
        _index: &IndexId,
    ) -> bashfuldb_schema::Result<SchemaVersion> {
        Ok(2)
    }

    fn current_version(&self) -> SchemaVersion {
        1
    }

    fn has_index(&self, coll: &CollectionId, index: &IndexId) -> bool {
        self.indexed_fields
            .get(coll.as_str())
            .is_some_and(|fields| fields.iter().any(|f| f == index))
    }

    fn collection_schema(&self, coll: &CollectionId) -> Option<CollectionSchema> {
        let fields = self.indexed_fields.get(coll.as_str())?;
        let indexes: Vec<IndexDefinition> = fields
            .iter()
            .enumerate()
            .map(|(i, f)| IndexDefinition {
                id: format!("idx_{i}"),
                collection: coll.clone(),
                field: f.clone(),
                created_at: 1,
            })
            .collect();
        Some(CollectionSchema {
            id: coll.clone(),
            indexes,
            version: 1,
        })
    }
}

// ─── Test app builder ─────────────────────────────────────────────────────────

/// Creates an [`AppState`] wired with all mocks.
pub fn mock_state() -> AppState {
    mock_state_with_coordinator(MockCoordinator::new())
}

/// Creates an [`AppState`] with a specific coordinator (for mutation tests).
pub fn mock_state_with_coordinator(coordinator: MockCoordinator) -> AppState {
    AppState::new(
        Arc::new(MockAuthenticator),
        Arc::new(MockAuthorizer),
        Arc::new(coordinator),
        Arc::new(MockSchemaManager::new()),
    )
}

/// Builds the test HTTP app.
pub fn test_app() -> axum_test::TestServer {
    let router = crate::build_router(mock_state());
    axum_test::TestServer::new(router)
}

/// Builds the test HTTP app with a pre-seeded coordinator.
pub fn test_app_with_coordinator(coordinator: MockCoordinator) -> axum_test::TestServer {
    let router = crate::build_router(mock_state_with_coordinator(coordinator));
    axum_test::TestServer::new(router)
}
