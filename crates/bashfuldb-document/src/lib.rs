//! Document data model, Value types, and binary codec.
//!
//! This crate defines the core data primitives for BashfulDB: the schema-less
//! [`Value`] enum, the [`Document`] wrapper, the binary [`Codec`] trait, and
//! naming/validation rules.
//!
//! # Key types
//!
//! - [`Value`] — A schema-less value (Null, Bool, Int, Float, String, Blob, Array, Object).
//! - [`Document`] — An identified object (`ObjectId` + top-level `Value::Object`).
//! - [`ObjectId`] — A UUID wrapper for object identity.
//!
//! # Key traits
//!
//! - [`Codec`] — Encode/decode [`Value`] to/from bytes.

mod codec;
mod document;
mod error;
mod object_id;
mod validate;
mod value;

pub use codec::{BinaryCodec, Codec};
pub use document::Document;
pub use error::DocumentError;
pub use object_id::ObjectId;
pub use validate::validate_name;
pub use value::Value;

/// Result type for document operations.
pub type Result<T> = std::result::Result<T, DocumentError>;

/// Maximum nesting depth for values.
pub const MAX_NESTING_DEPTH: usize = 64;

/// Maximum size for String and Blob values (16 MiB).
pub const MAX_STRING_BLOB_SIZE: usize = 16 * 1024 * 1024;

/// Maximum number of elements in an Array.
pub const MAX_ARRAY_ELEMENTS: usize = 1_000_000;

/// Maximum number of keys in an Object.
pub const MAX_OBJECT_KEYS: usize = 100_000;

/// Maximum length of a variable key in bytes.
pub const MAX_KEY_SIZE: usize = 256;
