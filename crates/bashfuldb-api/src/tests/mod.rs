//! Integration tests for the BashfulDB HTTP API.
//!
//! These tests use `axum-test` for full HTTP request/response cycles with
//! mock implementations of all dependencies.

mod mocks;
mod test_admin;
mod test_auth;
mod test_error_codes;
mod test_idempotency;
mod test_objects;
mod test_pagination;
