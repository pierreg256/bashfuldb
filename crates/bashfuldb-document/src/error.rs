use thiserror::Error;

/// Errors that can occur in document operations.
#[derive(Debug, Error, Clone, PartialEq)]
pub enum DocumentError {
    /// Nesting depth exceeded the maximum of 64 levels.
    #[error("nesting depth exceeded: {depth} > {max}")]
    NestingDepthExceeded { depth: usize, max: usize },

    /// String or Blob value exceeds 16 MiB.
    #[error("{kind} size {size} exceeds maximum {max}")]
    ValueTooLarge {
        kind: &'static str,
        size: usize,
        max: usize,
    },

    /// Array has too many elements (> 1,000,000).
    #[error("array has {count} elements, maximum is {max}")]
    ArrayTooLarge { count: usize, max: usize },

    /// Object has too many keys (> 100,000).
    #[error("object has {count} keys, maximum is {max}")]
    ObjectTooManyKeys { count: usize, max: usize },

    /// Name does not match the required pattern `[a-z0-9_]{1,64}`.
    #[error("invalid name {name:?}: must match [a-z0-9_]{{1,64}}")]
    InvalidName { name: String },

    /// Name is reserved (`_default` or `_system`).
    #[error("name {name:?} is reserved")]
    ReservedName { name: String },

    /// Document top-level value is not an Object.
    #[error("document top-level value must be an Object, got {actual}")]
    NotAnObject { actual: &'static str },

    /// Codec encountered an unknown type tag.
    #[error("unknown type tag: 0x{tag:02x}")]
    UnknownTypeTag { tag: u8 },

    /// Codec ran out of bytes during decoding.
    #[error("unexpected end of input at offset {offset}")]
    UnexpectedEof { offset: usize },

    /// Codec encountered invalid UTF-8 in a string.
    #[error("invalid UTF-8 at offset {offset}")]
    InvalidUtf8 { offset: usize },

    /// Float value is not finite (NaN or Infinity).
    #[error("non-finite float value: {value}")]
    NonFiniteFloat { value: f64 },

    /// Codec version is not supported.
    #[error("unsupported codec version: {version}")]
    UnsupportedCodecVersion { version: u8 },

    /// Trailing bytes after the decoded value.
    #[error("trailing bytes: {count} bytes remaining after offset {offset}")]
    TrailingBytes { offset: usize, count: usize },

    /// Object key exceeds maximum size (256 bytes).
    #[error("object key size {size} exceeds maximum {max}")]
    KeyTooLarge { size: usize, max: usize },
}
