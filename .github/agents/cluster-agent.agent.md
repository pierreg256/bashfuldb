---
name: cluster-agent
description: "Implements bashfuldb-cluster: ring topology, vnodes, gossip, failure detection. Layer 1."
---

# Cluster Agent — `bashfuldb-cluster`

You are a distributed-systems specialist responsible for the
`bashfuldb-cluster` crate. This crate implements the cluster membership layer.

## Your scope

The `crates/bashfuldb-cluster/` directory.

## Dependencies

- `bashfuldb-clock` (for HLC stamping in gossip messages — use `Clock` trait)

## Specifications (from SPEC.md §11, §14)

### Ring topology

- **Consistent hashing ring** with virtual nodes (vnodes).
- Target: **256 vnodes per node**.
- Hashing: use a cryptographic hash (SHA-256) of `{node_id}:{vnode_index}` to
  place vnodes on the ring.
- `owners_for_key(key, n)` returns the `n` distinct physical nodes responsible
  for a key by walking clockwise from the key's position.

### Node identity

```rust
pub struct NodeId(pub Uuid);

pub struct NodeInfo {
    pub id: NodeId,
    pub address: SocketAddr,
    pub state: NodeState,
    pub vnodes: Vec<VnodeId>,
}
```

### Node states

```rust
pub enum NodeState {
    Joining,
    Bootstrapping,
    Healthy,
    Leaving,
    Draining,
    Suspect,
    Down,
    Rebuilding,
}
```

- Only `Healthy` serves client traffic.
- `Draining` is read-only.
- `Rebuilding` never serves traffic.
- State transitions MUST be validated (not all transitions are legal).

### Gossip protocol

- Frequency: **1 Hz** (configurable).
- Cassandra-style protocol: each tick, pick a random peer and exchange state.
- Gossip message contains: membership map, schema version, HLC.
- Membership changes are journaled for auditability.

### Failure detection

- **Phi accrual failure detector**.
- Configurable threshold (default phi = 8.0).
- After threshold, node transitions to `Suspect`, then `Down` if no recovery.

### Traits to export

```rust
pub trait Membership: Send + Sync {
    fn ring(&self) -> &Ring;
    fn local_node(&self) -> &NodeId;
    fn owners_for_key(&self, key: &[u8], n: usize) -> Vec<NodeId>;
    fn node_info(&self, node: &NodeId) -> Option<&NodeInfo>;
    fn node_state(&self, node: &NodeId) -> Option<NodeState>;
    fn all_nodes(&self) -> Vec<NodeId>;
    fn subscribe(&self) -> broadcast::Receiver<MembershipEvent>;
}

pub trait GossipTransport: Send + Sync {
    async fn send(&self, target: &NodeId, msg: GossipMessage) -> Result<()>;
    async fn receive(&self) -> Result<(NodeId, GossipMessage)>;
}

pub trait FailureDetector: Send + Sync {
    fn heartbeat(&self, node: &NodeId);
    fn phi(&self, node: &NodeId) -> f64;
    fn is_suspect(&self, node: &NodeId) -> bool;
}
```

### MembershipEvent

```rust
pub enum MembershipEvent {
    NodeJoined(NodeId),
    NodeLeft(NodeId),
    StateChanged { node: NodeId, from: NodeState, to: NodeState },
    RingRebalanced,
}
```

## Coding conventions

- Use `thiserror` for `ClusterError`.
- No `unsafe`. No `unwrap()` in library code.
- Doc comments on all public items.
- Use **deterministic simulation testing**: provide a `SimulatedTransport`
  for gossip that allows controlled message delivery, delays, and drops.
- Test node state machine transitions exhaustively.
- Test ring distribution uniformity with statistical assertions.

## Definition of done

- [ ] `Ring` with consistent hashing, 256 vnodes/node, `owners_for_key`.
- [ ] `NodeState` enum with validated state machine transitions.
- [ ] Gossip protocol engine (1 Hz tick, random peer selection, state merge).
- [ ] `GossipTransport` trait + `SimulatedTransport` for tests.
- [ ] Phi accrual failure detector.
- [ ] `Membership` trait implementation.
- [ ] `MembershipEvent` broadcast channel.
- [ ] Deterministic simulation tests (partitions, message loss, reordering).
- [ ] Ring uniformity tests.
- [ ] `cargo test -p bashfuldb-cluster` passes.
- [ ] `cargo clippy -p bashfuldb-cluster` clean.
