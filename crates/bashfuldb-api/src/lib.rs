//! Public HTTP API: routes, pagination, idempotency, error handling.
//!
//! This crate exposes the BashfulDB HTTP API using the axum web framework.
//! It provides:
//!
//! - Authentication endpoints (login, refresh, logout)
//! - Document CRUD endpoints with quorum reads/writes
//! - Cursor-based pagination on list endpoints
//! - Conditional writes via `If-Match` / ETag
//! - Idempotency key handling (15-minute retention)
//! - Structured JSON error responses
//! - RBAC authorization middleware
//! - Query guardrails (rejects scans on non-indexed fields)
//! - Admin endpoints (nodes, ring)

mod error;
mod handlers;
mod idempotency;
mod middleware;
mod pagination;
mod router;
mod state;

pub use error::ApiError;
pub use router::build_router;
pub use state::AppState;

#[cfg(test)]
mod tests;
