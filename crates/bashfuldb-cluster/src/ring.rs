use bashfuldb_clock::NodeId;
use crate::VnodeId;
use std::collections::BTreeMap;

/// Default number of virtual nodes per physical node.
pub const DEFAULT_VNODES_PER_NODE: usize = 256;

/// The consistent hash ring.
///
/// Maps virtual nodes (identified by their position on a u64 ring) to
/// physical node IDs. Uses SHA-256 hashing of `{node_id}:{vnode_index}`
/// for placement.
#[derive(Debug, Clone)]
pub struct Ring {
    /// Sorted mapping of ring position → (VnodeId, owning NodeId).
    ring: BTreeMap<u64, (VnodeId, NodeId)>,
    /// Number of vnodes per physical node.
    vnodes_per_node: usize,
}

impl Ring {
    /// Creates an empty ring with the default vnodes-per-node count.
    pub fn new() -> Self {
        Self {
            ring: BTreeMap::new(),
            vnodes_per_node: DEFAULT_VNODES_PER_NODE,
        }
    }

    /// Creates an empty ring with a custom vnodes-per-node count.
    pub fn with_vnodes_per_node(vnodes_per_node: usize) -> Self {
        Self {
            ring: BTreeMap::new(),
            vnodes_per_node,
        }
    }

    /// Returns the number of vnodes per physical node.
    pub fn vnodes_per_node(&self) -> usize {
        self.vnodes_per_node
    }

    /// Returns the total number of vnodes on the ring.
    pub fn vnode_count(&self) -> usize {
        self.ring.len()
    }

    /// Returns true if the ring has no vnodes.
    pub fn is_empty(&self) -> bool {
        self.ring.is_empty()
    }

    /// Adds a node to the ring, creating `vnodes_per_node` virtual nodes.
    pub fn add_node(&mut self, node: NodeId) {
        for i in 0..self.vnodes_per_node {
            let position = hash_vnode(&node, i);
            let vnode_id = VnodeId(position);
            self.ring.insert(position, (vnode_id, node));
        }
    }

    /// Removes a node and all its virtual nodes from the ring.
    pub fn remove_node(&mut self, node: &NodeId) {
        self.ring.retain(|_, (_, owner)| owner != node);
    }

    /// Returns the `n` distinct physical nodes responsible for a key.
    ///
    /// Walks clockwise from the key's hash position, collecting distinct
    /// physical node IDs until `n` are found or all nodes are exhausted.
    pub fn owners_for_key(&self, key: &[u8], n: usize) -> Vec<NodeId> {
        if self.ring.is_empty() {
            return Vec::new();
        }

        let position = hash_key(key);
        let mut owners = Vec::with_capacity(n);
        let mut seen = std::collections::HashSet::new();

        // Walk clockwise from position
        for (_, (_, node)) in self.ring.range(position..) {
            if seen.insert(*node) {
                owners.push(*node);
                if owners.len() >= n {
                    return owners;
                }
            }
        }
        // Wrap around
        for (_, (_, node)) in self.ring.range(..position) {
            if seen.insert(*node) {
                owners.push(*node);
                if owners.len() >= n {
                    return owners;
                }
            }
        }

        owners
    }

    /// Returns all distinct physical node IDs on the ring.
    pub fn all_nodes(&self) -> Vec<NodeId> {
        let mut nodes: Vec<NodeId> = self
            .ring
            .values()
            .map(|(_, node)| *node)
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        nodes.sort();
        nodes
    }
}

impl Default for Ring {
    fn default() -> Self {
        Self::new()
    }
}

/// Hash a key to a ring position.
///
/// Uses a fixed, deterministic algorithm: FNV-1a on the raw bytes.
/// This is a protocol-level contract — the algorithm MUST NOT change
/// across versions without a coordinated migration.
fn hash_key(key: &[u8]) -> u64 {
    fnv1a_hash(key)
}

/// Hash a vnode placement: FNV-1a of `{node_uuid_bytes}:{index_le_bytes}`.
///
/// Deterministic across platforms and Rust versions.
fn hash_vnode(node: &NodeId, index: usize) -> u64 {
    let mut data = Vec::with_capacity(24);
    data.extend_from_slice(&node.to_bytes());
    data.extend_from_slice(&(index as u64).to_le_bytes());
    fnv1a_hash(&data)
}

/// FNV-1a 64-bit hash — simple, fast, deterministic, no seed.
///
/// This is the canonical ring hash for BashfulDB. It MUST NOT be changed
/// without a ring migration protocol.
fn fnv1a_hash(data: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    let mut hash = FNV_OFFSET;
    for &byte in data {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_ring_returns_no_owners() {
        let ring = Ring::new();
        assert!(ring.owners_for_key(b"test", 3).is_empty());
    }

    #[test]
    fn single_node_is_always_owner() {
        let mut ring = Ring::new();
        let node = NodeId::random();
        ring.add_node(node);

        let owners = ring.owners_for_key(b"any-key", 3);
        assert_eq!(owners.len(), 1);
        assert_eq!(owners[0], node);
    }

    #[test]
    fn three_nodes_return_three_owners() {
        let mut ring = Ring::new();
        let n1 = NodeId::random();
        let n2 = NodeId::random();
        let n3 = NodeId::random();
        ring.add_node(n1);
        ring.add_node(n2);
        ring.add_node(n3);

        let owners = ring.owners_for_key(b"some-key", 3);
        assert_eq!(owners.len(), 3);
        // All owners are distinct
        let unique: std::collections::HashSet<_> = owners.iter().collect();
        assert_eq!(unique.len(), 3);
    }

    #[test]
    fn remove_node_removes_all_vnodes() {
        let mut ring = Ring::new();
        let n1 = NodeId::random();
        let n2 = NodeId::random();
        ring.add_node(n1);
        ring.add_node(n2);
        assert_eq!(ring.all_nodes().len(), 2);

        ring.remove_node(&n1);
        assert_eq!(ring.all_nodes().len(), 1);
        assert_eq!(ring.all_nodes()[0], n2);
    }

    #[test]
    fn vnode_count_matches_expectations() {
        let mut ring = Ring::with_vnodes_per_node(32);
        let n1 = NodeId::random();
        ring.add_node(n1);
        // May be slightly less than 32 due to hash collisions, but close
        assert!(ring.vnode_count() <= 32);
        assert!(ring.vnode_count() >= 28); // Allow some collision tolerance
    }

    #[test]
    fn requesting_more_owners_than_nodes() {
        let mut ring = Ring::new();
        let n1 = NodeId::random();
        let n2 = NodeId::random();
        ring.add_node(n1);
        ring.add_node(n2);

        let owners = ring.owners_for_key(b"key", 5);
        assert_eq!(owners.len(), 2); // Only 2 distinct nodes exist
    }
}
