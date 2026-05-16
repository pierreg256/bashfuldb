use crate::{Hlc, NodeId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;

/// Maximum number of entries in a vector clock.
pub const MAX_ENTRIES: usize = 4_096;

/// The result of comparing two vector clocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CausalOrder {
    /// `self` happened before `other`.
    Before,
    /// `self` happened after `other`.
    After,
    /// `self` and `other` are concurrent (conflict).
    Concurrent,
    /// `self` and `other` are identical.
    Equal,
}

/// A vector clock mapping node IDs to HLC values.
///
/// Used to detect causal relationships and concurrent writes. Entries are
/// kept sorted by [`NodeId`] for deterministic iteration. The clock is
/// capped at [`MAX_ENTRIES`] (4,096); inserting a new node above this limit
/// returns [`crate::ClockError::VectorClockCapacityExceeded`].
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorClock {
    entries: BTreeMap<NodeId, Hlc>,
}

impl VectorClock {
    /// Creates an empty vector clock.
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }

    /// Returns the number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns `true` if the clock has no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns the HLC for a given node, or `None`.
    pub fn get(&self, node: &NodeId) -> Option<Hlc> {
        self.entries.get(node).copied()
    }

    /// Returns an iterator over `(NodeId, Hlc)` pairs in node-ID order.
    pub fn iter(&self) -> impl Iterator<Item = (&NodeId, &Hlc)> {
        self.entries.iter()
    }

    /// Increments (or sets) the clock for the given node.
    ///
    /// Returns an error if the clock is at capacity and the node is new.
    pub fn increment(&mut self, node: NodeId, hlc: Hlc) -> crate::Result<()> {
        if !self.entries.contains_key(&node) && self.entries.len() >= MAX_ENTRIES {
            return Err(crate::ClockError::VectorClockCapacityExceeded);
        }
        self.entries
            .entry(node)
            .and_modify(|existing| {
                if hlc > *existing {
                    *existing = hlc;
                }
            })
            .or_insert(hlc);
        Ok(())
    }

    /// Merges another vector clock into this one (element-wise max).
    ///
    /// Returns an error if the merged result would exceed capacity.
    pub fn merge(&mut self, other: &VectorClock) -> crate::Result<()> {
        // Check capacity before mutating
        let new_nodes = other
            .entries
            .keys()
            .filter(|k| !self.entries.contains_key(k))
            .count();
        if self.entries.len() + new_nodes > MAX_ENTRIES {
            return Err(crate::ClockError::VectorClockCapacityExceeded);
        }

        for (&node, &hlc) in &other.entries {
            self.entries
                .entry(node)
                .and_modify(|existing| {
                    if hlc > *existing {
                        *existing = hlc;
                    }
                })
                .or_insert(hlc);
        }
        Ok(())
    }

    /// Compares this clock to another, returning the causal relationship.
    pub fn compare(&self, other: &VectorClock) -> CausalOrder {
        let mut self_le = true; // all of self <= other
        let mut other_le = true; // all of other <= self

        // Check all nodes present in either clock
        let all_nodes: std::collections::BTreeSet<&NodeId> = self
            .entries
            .keys()
            .chain(other.entries.keys())
            .collect();

        for node in all_nodes {
            let self_hlc = self.entries.get(node).copied().unwrap_or(Hlc::zero());
            let other_hlc = other.entries.get(node).copied().unwrap_or(Hlc::zero());

            if self_hlc > other_hlc {
                self_le = false;
            }
            if other_hlc > self_hlc {
                other_le = false;
            }
        }

        match (self_le, other_le) {
            (true, true) => CausalOrder::Equal,
            (true, false) => CausalOrder::Before,
            (false, true) => CausalOrder::After,
            (false, false) => CausalOrder::Concurrent,
        }
    }

    /// Computes a deterministic hash of this vector clock (for ETags).
    pub fn fingerprint(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        for (node, hlc) in &self.entries {
            node.hash(&mut hasher);
            hlc.hash(&mut hasher);
        }
        hasher.finish()
    }

    /// Evicts the entry with the lowest HLC value.
    /// Reserved for explicit caller-controlled eviction if needed.
    #[allow(dead_code)]
    fn evict_oldest(&mut self) {
        if let Some((&oldest_node, _)) = self.entries.iter().min_by_key(|(_, hlc)| **hlc) {
            self.entries.remove(&oldest_node);
        }
    }
}

impl Default for VectorClock {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for VectorClock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "VectorClock({} entries)", self.entries.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn node(n: u8) -> NodeId {
        NodeId::from_uuid(uuid::Uuid::from_bytes([
            n, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ]))
    }

    fn node_from_u16(n: u16) -> NodeId {
        NodeId::from_uuid(uuid::Uuid::from_u128(u128::from(n)))
    }

    fn build_clock(entries: &[(u16, u32, u16)]) -> VectorClock {
        let mut vc = VectorClock::new();
        for &(node_seed, physical, logical) in entries {
            vc.increment(node_from_u16(node_seed), Hlc::new(u64::from(physical), logical))
                .expect("bounded property-test clocks should not exceed capacity");
        }
        vc
    }

    fn next_hlc(current: Option<Hlc>) -> Hlc {
        match current {
            Some(existing) if existing.logical() < Hlc::MAX_LOGICAL => {
                Hlc::new(existing.physical_ms(), existing.logical() + 1)
            }
            Some(existing) => Hlc::new(existing.physical_ms() + 1, 0),
            None => Hlc::new(1, 0),
        }
    }

    #[test]
    fn empty_clocks_are_equal() {
        let a = VectorClock::new();
        let b = VectorClock::new();
        assert_eq!(a.compare(&b), CausalOrder::Equal);
    }

    #[test]
    fn increment_creates_entry() {
        let mut vc = VectorClock::new();
        vc.increment(node(1), Hlc::new(100, 0)).unwrap();
        assert_eq!(vc.len(), 1);
        assert_eq!(vc.get(&node(1)), Some(Hlc::new(100, 0)));
    }

    #[test]
    fn increment_keeps_max() {
        let mut vc = VectorClock::new();
        vc.increment(node(1), Hlc::new(100, 5)).unwrap();
        vc.increment(node(1), Hlc::new(100, 3)).unwrap(); // lower — should be ignored
        assert_eq!(vc.get(&node(1)), Some(Hlc::new(100, 5)));
    }

    #[test]
    fn before_relationship() {
        let mut a = VectorClock::new();
        a.increment(node(1), Hlc::new(100, 0)).unwrap();

        let mut b = VectorClock::new();
        b.increment(node(1), Hlc::new(200, 0)).unwrap();

        assert_eq!(a.compare(&b), CausalOrder::Before);
        assert_eq!(b.compare(&a), CausalOrder::After);
    }

    #[test]
    fn concurrent_relationship() {
        let mut a = VectorClock::new();
        a.increment(node(1), Hlc::new(100, 0)).unwrap();

        let mut b = VectorClock::new();
        b.increment(node(2), Hlc::new(100, 0)).unwrap();

        assert_eq!(a.compare(&b), CausalOrder::Concurrent);
        assert_eq!(b.compare(&a), CausalOrder::Concurrent);
    }

    #[test]
    fn merge_takes_max() {
        let mut a = VectorClock::new();
        a.increment(node(1), Hlc::new(100, 0)).unwrap();
        a.increment(node(2), Hlc::new(50, 0)).unwrap();

        let mut b = VectorClock::new();
        b.increment(node(1), Hlc::new(50, 0)).unwrap();
        b.increment(node(2), Hlc::new(200, 0)).unwrap();
        b.increment(node(3), Hlc::new(75, 0)).unwrap();

        a.merge(&b).unwrap();
        assert_eq!(a.get(&node(1)), Some(Hlc::new(100, 0)));
        assert_eq!(a.get(&node(2)), Some(Hlc::new(200, 0)));
        assert_eq!(a.get(&node(3)), Some(Hlc::new(75, 0)));
    }

    #[test]
    fn merge_is_commutative_for_ordering() {
        let mut a = VectorClock::new();
        a.increment(node(1), Hlc::new(100, 0)).unwrap();

        let mut b = VectorClock::new();
        b.increment(node(2), Hlc::new(200, 0)).unwrap();

        let mut ab = a.clone();
        ab.merge(&b).unwrap();

        let mut ba = b.clone();
        ba.merge(&a).unwrap();

        assert_eq!(ab.compare(&ba), CausalOrder::Equal);
    }

    #[test]
    fn error_at_capacity() {
        let mut vc = VectorClock::new();
        for i in 0..MAX_ENTRIES {
            let id = NodeId::from_uuid(uuid::Uuid::from_u128(i as u128));
            vc.increment(id, Hlc::new(i as u64, 0)).unwrap();
        }
        assert_eq!(vc.len(), MAX_ENTRIES);

        // One more should error
        let new_node = NodeId::from_uuid(uuid::Uuid::from_u128(MAX_ENTRIES as u128));
        let result = vc.increment(new_node, Hlc::new(MAX_ENTRIES as u64, 0));
        assert!(result.is_err());
    }

    proptest! {
        #[test]
        fn merge_is_commutative_and_idempotent(
            a_entries in proptest::collection::vec((0u16..2048u16, 0u32..10_000u32, 0u16..1024u16), 0..128),
            b_entries in proptest::collection::vec((0u16..2048u16, 0u32..10_000u32, 0u16..1024u16), 0..128),
        ) {
            let a = build_clock(&a_entries);
            let b = build_clock(&b_entries);

            let mut ab = a.clone();
            ab.merge(&b).expect("bounded property-test clocks should merge");
            let mut ba = b.clone();
            ba.merge(&a).expect("bounded property-test clocks should merge");
            prop_assert_eq!(ab.compare(&ba), CausalOrder::Equal);

            let mut aa = a.clone();
            aa.merge(&a).expect("self merge should succeed");
            prop_assert_eq!(aa.compare(&a), CausalOrder::Equal);
        }

        #[test]
        fn partial_order_transitivity(
            base_entries in proptest::collection::vec((0u16..2048u16, 0u32..10_000u32, 0u16..1024u16), 0..128),
            node_b in 0u16..2048u16,
            node_c in 0u16..2048u16,
        ) {
            let a = build_clock(&base_entries);

            let mut b = a.clone();
            let node_b_id = node_from_u16(node_b);
            b.increment(node_b_id, next_hlc(a.get(&node_b_id)))
                .expect("single increment should succeed");

            let mut c = b.clone();
            let node_c_id = node_from_u16(node_c);
            c.increment(node_c_id, next_hlc(b.get(&node_c_id)))
                .expect("single increment should succeed");

            prop_assert_eq!(a.compare(&b), CausalOrder::Before);
            prop_assert_eq!(b.compare(&c), CausalOrder::Before);
            prop_assert_eq!(a.compare(&c), CausalOrder::Before);
        }
    }
}
