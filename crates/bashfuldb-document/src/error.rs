use thiserror::Error;

/// Errors that can occur in document operations.
#[derive(Debug, Error, Clone, PartialEq)]
pub enum DocumentError {
    /// Nesting depth exceeded the maximum of 64 levels.
    #[error("nesting depth exceeded: {depth} > {max}")]
    NestingDepthExceeded {
        /// Actual encountered nesting depth.
        depth: usize,
        /// Maximum allowed nesting depth.
        max: usize,
    },

    /// String or Blob value exceeds 16 MiB.
    #[error("{kind} size {size} exceeds maximum {max}")]
    ValueTooLarge {
        /// Value kind (`String` or `Blob`).
        kind: &'static str,
        /// Actual value size in bytes.
        size: usize,
        /// Maximum allowed size in bytes.
        max: usize,
    },

    /// Array has too many elements (> 1,000,000).
    #[error("array has {count} elements, maximum is {max}")]
    ArrayTooLarge {
        /// Actual array element count.
        count: usize,
        /// Maximum allowed array element count.
        max: usize,
    },

    /// Object has too many keys (> 100,000).
    #[error("object has {count} keys, maximum is {max}")]
    ObjectTooManyKeys {
        /// Actual object key count.
        count: usize,
        /// Maximum allowed object key count.
        max: usize,
    },

    /// Name does not match the required pattern `[a-z0-9_]{1,64}`.
    #[error("invalid name {name:?}: must match [a-z0-9_]{{1,64}}")]
    InvalidName {
        /// Invalid provided name.
        name: String,
    },

    /// Name is reserved (`_default` or `_system`).
    #[error("name {name:?} is reserved")]
    ReservedName {
        /// Reserved provided name.
        name: String,
    },

    /// Document top-level value is not an Object.
    #[error("document top-level value must be an Object, got {actual}")]
    NotAnObject {
        /// Actual top-level value type name.
        actual: &'static str,
    },

    /// Codec encountered an unknown type tag.
    #[error("unknown type tag: 0x{tag:02x}")]
    UnknownTypeTag {
        /// Unknown tag byte value.
        tag: u8,
    },

    /// Codec ran out of bytes during decoding.
    #[error("unexpected end of input at offset {offset}")]
    UnexpectedEof {
        /// Byte offset where input ended unexpectedly.
        offset: usize,
    },

    /// Codec encountered invalid UTF-8 in a string.
    #[error("invalid UTF-8 at offset {offset}")]
    InvalidUtf8 {
        /// Byte offset where invalid UTF-8 starts.
        offset: usize,
    },

    /// Float value is not finite (NaN or Infinity).
    #[error("non-finite float value: {value}")]
    NonFiniteFloat {
        /// Invalid non-finite float value.
        value: f64,
    },

    /// Codec version is not supported.
    #[error("unsupported codec version: {version}")]
    UnsupportedCodecVersion {
        /// Unsupported version value from header.
        version: u8,
    },

    /// Trailing bytes after the decoded value.
    #[error("trailing bytes: {count} bytes remaining after offset {offset}")]
    TrailingBytes {
        /// Byte offset where trailing bytes start.
        offset: usize,
        /// Number of trailing bytes.
        count: usize,
    },

    /// Object key exceeds maximum size (256 bytes).
    #[error("object key size {size} exceeds maximum {max}")]
    KeyTooLarge {
        /// Actual key size in bytes.
        size: usize,
        /// Maximum allowed key size in bytes.
        max: usize,
    },
}
