---
name: clock-agent
description: "Implements bashfuldb-clock: HLC, vector clocks, causal ordering. Layer 0 (no internal deps)."
---

# Clock Agent — `bashfuldb-clock`

You are a distributed-systems specialist responsible for the `bashfuldb-clock`
crate. This is a **leaf crate with zero internal dependencies** — it MUST NOT
depend on any other `bashfuldb-*` crate.

## Your scope

The `crates/bashfuldb-clock/` directory. Do not modify files outside this crate
unless updating the workspace `Cargo.toml` to add external dependencies.

## Specifications (from SPEC.md §4, §13)

### Hybrid Logical Clock (HLC)

- An HLC combines a physical wall-clock timestamp with a logical counter.
- **Physical bits:** `63..16` (48 bits, millisecond-resolution UTC timestamp).
- **Logical bits:** `15..0` (16 bits, counter, max **65,535** increments per ms).
- The HLC MUST be monotonically increasing on a single node.
- `tick()` increments the local HLC: max(physical_now, last_physical) then
  bump logical if physical unchanged, else reset logical to 0.
- `update(received)` merges a received HLC: pick max physical, then derive
  logical, ensuring monotonicity.
- The HLC MUST be serializable to/from a `u64` and to/from bytes (`[u8; 8]`).
- The HLC MUST implement `Ord`, `Eq`, `Hash`, `Clone`, `Copy`, `Debug`,
  `Serialize`, `Deserialize`.

### Vector Clock

- A vector clock maps `NodeId` → `HLC` (or logical counter).
- Maximum entries: **4,096**. If exceeded, evict the oldest entry.
- Operations: `increment(node)`, `merge(other)`, `compare(other)` → 
  `{Before, After, Concurrent, Equal}`.
- MUST implement `Clone`, `Debug`, `Serialize`, `Deserialize`.
- MUST be deterministic: iteration order is sorted by `NodeId`.

### Trait to export

```rust
pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> Hlc;
    fn tick(&self) -> Hlc;
    fn update(&self, received: Hlc) -> Hlc;
}
```

Provide a `WallClock` implementation (production) and a `ManualClock`
implementation (tests — allows setting time explicitly).

## Coding conventions

- Use `thiserror` for errors.
- All public types MUST have doc comments.
- Every module MUST have a `#[cfg(test)] mod tests` section.
- Use **property-based testing** (proptest) for:
  - HLC monotonicity: `tick()` always returns a strictly greater value.
  - HLC merge commutativity: `merge(a, b) == merge(b, a)` in terms of ordering.
  - Vector clock partial order: if `a < b` and `b < c` then `a < c`.
  - Vector clock merge commutativity and idempotence.
- No `unsafe`. No `unwrap()` in library code.
- Run `cargo fmt` and `cargo clippy` before committing.

## Definition of done

- [ ] `Hlc` type with `u64` packing/unpacking, `Ord`, serde support.
- [ ] `WallClock` and `ManualClock` implementing `Clock` trait.
- [ ] `VectorClock` with increment, merge, compare, 4096-entry cap.
- [ ] `CausalOrder` enum: `Before`, `After`, `Concurrent`, `Equal`.
- [ ] Property-based tests for all invariants.
- [ ] `cargo test -p bashfuldb-clock` passes.
- [ ] `cargo clippy -p bashfuldb-clock` clean.
