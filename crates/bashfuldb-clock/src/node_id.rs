use serde::{Deserialize, Serialize};
use std::fmt;

/// A unique identifier for a node in the cluster.
///
/// Wraps a UUID v4. Used as the key in [`VectorClock`](crate::VectorClock).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId(uuid::Uuid);

impl NodeId {
    /// Creates a new random node ID.
    pub fn random() -> Self {
        Self(uuid::Uuid::new_v4())
    }

    /// Creates a node ID from a UUID.
    pub fn from_uuid(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }

    /// Returns the inner UUID.
    pub fn as_uuid(&self) -> &uuid::Uuid {
        &self.0
    }

    /// Returns the nil (all-zeros) node ID.
    pub fn nil() -> Self {
        Self(uuid::Uuid::nil())
    }

    /// Converts to a 16-byte array.
    pub fn to_bytes(self) -> [u8; 16] {
        *self.0.as_bytes()
    }

    /// Creates from a 16-byte array.
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(uuid::Uuid::from_bytes(bytes))
    }
}

impl std::str::FromStr for NodeId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        uuid::Uuid::parse_str(s).map(Self)
    }
}

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NodeId({})", &self.0.to_string()[..8])
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_id_ordering_is_deterministic() {
        let a = NodeId::from_uuid(uuid::Uuid::nil());
        let b = NodeId::random();
        // Nil UUID should be less than any random UUID (all zeros vs random)
        assert!(a < b);
    }

    #[test]
    fn node_id_debug_is_short() {
        let id = NodeId::random();
        let dbg = format!("{:?}", id);
        assert!(dbg.starts_with("NodeId("));
        assert!(dbg.len() < 20); // Short form, not full UUID
    }
}
