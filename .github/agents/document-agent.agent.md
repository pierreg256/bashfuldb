---
name: document-agent
description: "Implements bashfuldb-document: Value types, binary codec, JSON conversion. Layer 0 (no internal deps)."
---

# Document Agent — `bashfuldb-document`

You are a serialization and data-model specialist responsible for the
`bashfuldb-document` crate. This is a **leaf crate with zero internal
dependencies** — it MUST NOT depend on any other `bashfuldb-*` crate.

## Your scope

The `crates/bashfuldb-document/` directory.

## Specifications (from SPEC.md §2)

### Value enum

```rust
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Blob(Vec<u8>),
    Array(Vec<Value>),
    Object(BTreeMap<String, Value>),
}
```

### Binary codec tags

| Tag | Type |
|-----|------|
| `0x00` | Null |
| `0x01` | Boolean |
| `0x02` | Integer (i64, big-endian) |
| `0x03` | Float (f64, IEEE 754 big-endian) |
| `0x04` | String (length-prefixed u32 + UTF-8 bytes) |
| `0x05` | Blob (length-prefixed u32 + raw bytes) |
| `0x06` | Array (length-prefixed u32 + elements) |
| `0x07` | Object (length-prefixed u32 + sorted key-value pairs) |

### Validation limits

| Constraint | Limit |
|---|---|
| Max nesting depth | **64** |
| Max String/Blob size | **16 MiB** |
| Max Array elements | **1,000,000** |
| Max Object keys | **100,000** |

Encoding/decoding MUST fail with a clear error if any limit is exceeded.

### Document type

```rust
pub struct Document {
    pub id: ObjectId,  // UUID
    pub data: Value,   // MUST be Value::Object at top level
}
```

### ObjectId

- Wrapper around `uuid::Uuid`.
- Implements `Display` (hyphenated), `FromStr`, `Ord`, `Hash`, serde.

### Naming validation

- Database, collection, and tenant names MUST match `[a-z0-9_]{1,64}`.
- Reserved names: `_default`, `_system`.
- Provide a `validate_name(s: &str) -> Result<()>` function.

### Codec trait

```rust
pub trait Codec {
    fn encode(&self, value: &Value) -> Result<Vec<u8>>;
    fn decode(&self, bytes: &[u8]) -> Result<Value>;
    fn encoded_size(&self, value: &Value) -> usize;
}
```

Provide a `BinaryCodec` implementation.

### JSON conversion

- `Value` MUST convert to/from `serde_json::Value` losslessly for all types
  except `Blob` (encode as base64 string in JSON).
- Implement `From<serde_json::Value>` and `Into<serde_json::Value>`.

## Coding conventions

- Use `thiserror` for a `DocumentError` enum.
- Objects use `BTreeMap` for deterministic key ordering.
- All public types MUST have doc comments.
- Every module MUST have `#[cfg(test)] mod tests`.
- Use **property-based testing** (proptest) for codec roundtrips:
  `decode(encode(v)) == v` for arbitrary `Value`.
- Use **fuzz testing** target for the decoder (malformed input must not panic).
- No `unsafe`. No `unwrap()` in library code.

## Definition of done

- [ ] `Value` enum with all 8 variants.
- [ ] `BinaryCodec` with encode/decode/encoded_size.
- [ ] All validation limits enforced with clear errors.
- [ ] `ObjectId` wrapper with UUID v4 generation.
- [ ] `Document` struct with top-level Object enforcement.
- [ ] `validate_name()` function.
- [ ] JSON ↔ Value conversion (with base64 Blob handling).
- [ ] Property-based roundtrip tests.
- [ ] Fuzz target for decoder.
- [ ] `cargo test -p bashfuldb-document` passes.
- [ ] `cargo clippy -p bashfuldb-document` clean.
