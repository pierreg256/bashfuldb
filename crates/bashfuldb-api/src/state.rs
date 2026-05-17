//! Shared application state injected into every handler via axum's `State`.

use crate::idempotency::IdempotencyCache;
use bashfuldb_auth::{Authenticator, Authorizer};
use bashfuldb_replication::coordinator::Coordinator;
use bashfuldb_schema::SchemaManager;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Configuration knobs for the API layer.
#[derive(Debug, Clone)]
pub struct ApiConfig {
    /// Default page size for list endpoints.
    pub default_page_size: usize,
    /// Maximum page size for list endpoints.
    pub max_page_size: usize,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            default_page_size: 100,
            max_page_size: 1000,
        }
    }
}

/// Application-wide state shared between all axum handlers.
#[derive(Clone)]
pub struct AppState {
    /// Authentication service: login, refresh, verify, logout.
    pub authenticator: Arc<dyn Authenticator>,
    /// Authorization service: RBAC checks.
    pub authorizer: Arc<dyn Authorizer>,
    /// Replication coordinator: quorum reads/writes/deletes.
    pub coordinator: Arc<dyn Coordinator>,
    /// Schema manager: index lifecycle and query guardrails.
    pub schema: Arc<dyn SchemaManager>,
    /// Idempotency cache (15-minute retention).
    pub idempotency: Arc<Mutex<IdempotencyCache>>,
    /// API configuration.
    pub config: ApiConfig,
}

impl AppState {
    /// Constructs a new [`AppState`].
    pub fn new(
        authenticator: Arc<dyn Authenticator>,
        authorizer: Arc<dyn Authorizer>,
        coordinator: Arc<dyn Coordinator>,
        schema: Arc<dyn SchemaManager>,
    ) -> Self {
        Self::with_config(
            authenticator,
            authorizer,
            coordinator,
            schema,
            ApiConfig::default(),
        )
    }

    /// Constructs a new [`AppState`] with a custom [`ApiConfig`].
    pub fn with_config(
        authenticator: Arc<dyn Authenticator>,
        authorizer: Arc<dyn Authorizer>,
        coordinator: Arc<dyn Coordinator>,
        schema: Arc<dyn SchemaManager>,
        config: ApiConfig,
    ) -> Self {
        Self {
            authenticator,
            authorizer,
            coordinator,
            schema,
            idempotency: Arc::new(Mutex::new(IdempotencyCache::new())),
            config,
        }
    }
}
