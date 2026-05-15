# BashfulDB — Copilot Instructions

## Project overview

BashfulDB is a distributed document database written in Rust. It combines a
RocksDB-backed storage engine with a distributed ring topology (no coordinator
node), quorum-based replication, and a hardened HTTP API with JWT/RBAC
authentication.

## Key documents (read order)

1. `SPEC.md` — Consolidated specifications (normative reference)
2. `ARCHITECTURE.md` — Module decomposition and dependency graph
3. `.github/agents/*.agent.md` — Per-crate agent definitions with detailed specs

## Workspace structure

This is a Cargo workspace with 11 crates under `crates/`. Each crate has a
clear trait boundary. See `ARCHITECTURE.md` for the dependency DAG.

## Coding conventions

- **Edition 2024**, Rust stable.
- Use `thiserror` for error types. Each crate has its own error enum.
- Use `async-trait` for async trait definitions.
- All public types and functions MUST have doc comments.
- Every `.rs` file MUST have a `#[cfg(test)] mod tests` section.
- No `unsafe` unless justified with a safety comment.
- No `unwrap()` in library code — use `?` or explicit error handling.
- Run `cargo fmt` and `cargo clippy` before committing.
- Tests: `cargo test --workspace` must pass.

## Architecture invariants (MUST NOT violate)

- **No coordinator node** — every node exposes the same API.
- **No scope inheritance** in RBAC — tenant A admin has zero access to tenant B.
- **No soft delete** from client perspective — hard delete only.
- **No non-indexed scans** for standard clients.
- **No multi-object transactions** in V1.
- **Storage abstraction** — all storage access goes through `StorageEngine` trait.
  Direct RocksDB usage is confined to `bashfuldb-storage::rocksdb` module.

## Dependency flow

```
Layer 0: clock, document (no internal deps)
Layer 1: storage, cluster, auth
Layer 2: replication, schema
Layer 3: protocol, api, ops
Layer 4: sdk
```

A crate MUST NOT depend on a crate in the same or higher layer.
