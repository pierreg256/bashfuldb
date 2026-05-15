use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// A unique identifier for a document object, wrapping a UUID v4.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ObjectId(uuid::Uuid);

impl ObjectId {
    /// Generates a new random object ID (UUID v4).
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4())
    }

    /// Creates an ObjectId from an existing UUID.
    pub fn from_uuid(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }

    /// Returns the inner UUID.
    pub fn as_uuid(&self) -> &uuid::Uuid {
        &self.0
    }

    /// Converts to a 16-byte array.
    pub fn to_bytes(self) -> [u8; 16] {
        *self.0.as_bytes()
    }

    /// Creates from a 16-byte array.
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(uuid::Uuid::from_bytes(bytes))
    }

    /// Returns the nil (all-zeros) object ID.
    pub fn nil() -> Self {
        Self(uuid::Uuid::nil())
    }
}

impl Default for ObjectId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.as_hyphenated())
    }
}

impl fmt::Debug for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ObjectId({})", self.0.as_hyphenated())
    }
}

impl FromStr for ObjectId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        uuid::Uuid::parse_str(s).map(Self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_is_unique() {
        let a = ObjectId::new();
        let b = ObjectId::new();
        assert_ne!(a, b);
    }

    #[test]
    fn bytes_roundtrip() {
        let id = ObjectId::new();
        assert_eq!(ObjectId::from_bytes(id.to_bytes()), id);
    }

    #[test]
    fn string_roundtrip() {
        let id = ObjectId::new();
        let s = id.to_string();
        let parsed: ObjectId = s.parse().unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn nil_is_all_zeros() {
        let nil = ObjectId::nil();
        assert_eq!(nil.to_bytes(), [0u8; 16]);
    }
}
