use crate::error::DocumentError;
use crate::value::Value;
use crate::{MAX_ARRAY_ELEMENTS, MAX_NESTING_DEPTH, MAX_OBJECT_KEYS, MAX_STRING_BLOB_SIZE};

/// Trait for encoding and decoding [`Value`] to/from bytes.
pub trait Codec: Send + Sync {
    /// Encodes a value to its binary representation.
    fn encode(&self, value: &Value) -> crate::Result<Vec<u8>>;

    /// Decodes a value from its binary representation.
    fn decode(&self, bytes: &[u8]) -> crate::Result<Value>;

    /// Computes the encoded size of the payload (excluding header).
    fn encoded_size(&self, value: &Value) -> usize;

    /// Returns the codec version.
    fn version(&self) -> u8;
}

/// Binary codec using tagged length-prefixed encoding.
///
/// # Wire format
///
/// Encoded values start with a 5-byte header: `BDB\x01` (magic + version),
/// followed by the value payload.
///
/// Each value starts with a 1-byte type tag, followed by type-specific data:
///
/// | Tag | Encoding |
/// |-----|----------|
/// | `0x00` | (nothing — Null) |
/// | `0x01` | 1 byte: `0x00` = false, `0x01` = true |
/// | `0x02` | 8 bytes big-endian i64 |
/// | `0x03` | 8 bytes big-endian f64 |
/// | `0x04` | 4-byte BE length + UTF-8 bytes |
/// | `0x05` | 4-byte BE length + raw bytes |
/// | `0x06` | 4-byte BE count + encoded elements |
/// | `0x07` | 4-byte BE count + (4-byte key len + key bytes + encoded value) per entry |
pub struct BinaryCodec;

/// Magic bytes for the binary codec header.
const CODEC_MAGIC: &[u8; 3] = b"BDB";

/// Current codec version.
const CODEC_VERSION: u8 = 1;

/// Header size: 3 bytes magic + 1 byte version.
const HEADER_SIZE: usize = 4;

impl Codec for BinaryCodec {
    fn encode(&self, value: &Value) -> crate::Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(HEADER_SIZE + self.encoded_size(value));
        buf.extend_from_slice(CODEC_MAGIC);
        buf.push(CODEC_VERSION);
        encode_value(value, &mut buf, 0)?;
        Ok(buf)
    }

    fn decode(&self, bytes: &[u8]) -> crate::Result<Value> {
        if bytes.len() < HEADER_SIZE {
            return Err(DocumentError::UnexpectedEof { offset: 0 });
        }
        if &bytes[..3] != CODEC_MAGIC {
            return Err(DocumentError::UnknownTypeTag { tag: bytes[0] });
        }
        if bytes[3] != CODEC_VERSION {
            return Err(DocumentError::UnsupportedCodecVersion { version: bytes[3] });
        }
        let mut offset = HEADER_SIZE;
        let value = decode_value(bytes, &mut offset, 0)?;
        if offset < bytes.len() {
            return Err(DocumentError::TrailingBytes {
                offset,
                count: bytes.len() - offset,
            });
        }
        Ok(value)
    }

    fn encoded_size(&self, value: &Value) -> usize {
        compute_size(value)
    }

    fn version(&self) -> u8 {
        CODEC_VERSION
    }
}

fn encode_value(value: &Value, buf: &mut Vec<u8>, depth: usize) -> crate::Result<()> {
    if depth > MAX_NESTING_DEPTH {
        return Err(DocumentError::NestingDepthExceeded {
            depth,
            max: MAX_NESTING_DEPTH,
        });
    }

    buf.push(value.type_tag());

    match value {
        Value::Null => {}
        Value::Bool(b) => buf.push(if *b { 0x01 } else { 0x00 }),
        Value::Int(i) => buf.extend_from_slice(&i.to_be_bytes()),
        Value::Float(f) => {
            if !f.is_finite() {
                return Err(DocumentError::NonFiniteFloat { value: *f });
            }
            buf.extend_from_slice(&f.to_be_bytes());
        }
        Value::String(s) => {
            if s.len() > MAX_STRING_BLOB_SIZE {
                return Err(DocumentError::ValueTooLarge {
                    kind: "String",
                    size: s.len(),
                    max: MAX_STRING_BLOB_SIZE,
                });
            }
            buf.extend_from_slice(&(s.len() as u32).to_be_bytes());
            buf.extend_from_slice(s.as_bytes());
        }
        Value::Blob(b) => {
            if b.len() > MAX_STRING_BLOB_SIZE {
                return Err(DocumentError::ValueTooLarge {
                    kind: "Blob",
                    size: b.len(),
                    max: MAX_STRING_BLOB_SIZE,
                });
            }
            buf.extend_from_slice(&(b.len() as u32).to_be_bytes());
            buf.extend_from_slice(b);
        }
        Value::Array(items) => {
            if items.len() > MAX_ARRAY_ELEMENTS {
                return Err(DocumentError::ArrayTooLarge {
                    count: items.len(),
                    max: MAX_ARRAY_ELEMENTS,
                });
            }
            buf.extend_from_slice(&(items.len() as u32).to_be_bytes());
            for item in items {
                encode_value(item, buf, depth + 1)?;
            }
        }
        Value::Object(map) => {
            if map.len() > MAX_OBJECT_KEYS {
                return Err(DocumentError::ObjectTooManyKeys {
                    count: map.len(),
                    max: MAX_OBJECT_KEYS,
                });
            }
            buf.extend_from_slice(&(map.len() as u32).to_be_bytes());
            for (key, val) in map {
                if key.len() > crate::MAX_KEY_SIZE {
                    return Err(DocumentError::KeyTooLarge {
                        size: key.len(),
                        max: crate::MAX_KEY_SIZE,
                    });
                }
                buf.extend_from_slice(&(key.len() as u32).to_be_bytes());
                buf.extend_from_slice(key.as_bytes());
                encode_value(val, buf, depth + 1)?;
            }
        }
    }
    Ok(())
}

fn decode_value(bytes: &[u8], offset: &mut usize, depth: usize) -> crate::Result<Value> {
    if depth > MAX_NESTING_DEPTH {
        return Err(DocumentError::NestingDepthExceeded {
            depth,
            max: MAX_NESTING_DEPTH,
        });
    }

    let tag = read_u8(bytes, offset)?;

    match tag {
        0x00 => Ok(Value::Null),
        0x01 => {
            let b = read_u8(bytes, offset)?;
            Ok(Value::Bool(b != 0))
        }
        0x02 => {
            let i = i64::from_be_bytes(read_bytes::<8>(bytes, offset)?);
            Ok(Value::Int(i))
        }
        0x03 => {
            let f = f64::from_be_bytes(read_bytes::<8>(bytes, offset)?);
            Ok(Value::Float(f))
        }
        0x04 => {
            let len = u32::from_be_bytes(read_bytes::<4>(bytes, offset)?) as usize;
            if len > MAX_STRING_BLOB_SIZE {
                return Err(DocumentError::ValueTooLarge {
                    kind: "String",
                    size: len,
                    max: MAX_STRING_BLOB_SIZE,
                });
            }
            let data = read_slice(bytes, offset, len)?;
            let s = std::str::from_utf8(data).map_err(|_| DocumentError::InvalidUtf8 {
                offset: *offset - len,
            })?;
            Ok(Value::String(s.to_string()))
        }
        0x05 => {
            let len = u32::from_be_bytes(read_bytes::<4>(bytes, offset)?) as usize;
            if len > MAX_STRING_BLOB_SIZE {
                return Err(DocumentError::ValueTooLarge {
                    kind: "Blob",
                    size: len,
                    max: MAX_STRING_BLOB_SIZE,
                });
            }
            let data = read_slice(bytes, offset, len)?;
            Ok(Value::Blob(data.to_vec()))
        }
        0x06 => {
            let count = u32::from_be_bytes(read_bytes::<4>(bytes, offset)?) as usize;
            if count > MAX_ARRAY_ELEMENTS {
                return Err(DocumentError::ArrayTooLarge {
                    count,
                    max: MAX_ARRAY_ELEMENTS,
                });
            }
            let mut items = Vec::with_capacity(count.min(1024));
            for _ in 0..count {
                items.push(decode_value(bytes, offset, depth + 1)?);
            }
            Ok(Value::Array(items))
        }
        0x07 => {
            let count = u32::from_be_bytes(read_bytes::<4>(bytes, offset)?) as usize;
            if count > MAX_OBJECT_KEYS {
                return Err(DocumentError::ObjectTooManyKeys {
                    count,
                    max: MAX_OBJECT_KEYS,
                });
            }
            let mut map = std::collections::BTreeMap::new();
            for _ in 0..count {
                let key_len = u32::from_be_bytes(read_bytes::<4>(bytes, offset)?) as usize;
                let key_data = read_slice(bytes, offset, key_len)?;
                let key =
                    std::str::from_utf8(key_data).map_err(|_| DocumentError::InvalidUtf8 {
                        offset: *offset - key_len,
                    })?;
                let val = decode_value(bytes, offset, depth + 1)?;
                map.insert(key.to_string(), val);
            }
            Ok(Value::Object(map))
        }
        _ => Err(DocumentError::UnknownTypeTag { tag }),
    }
}

fn compute_size(value: &Value) -> usize {
    1 + match value {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Int(_) | Value::Float(_) => 8,
        Value::String(s) => 4 + s.len(),
        Value::Blob(b) => 4 + b.len(),
        Value::Array(items) => 4 + items.iter().map(compute_size).sum::<usize>(),
        Value::Object(map) => {
            4 + map
                .iter()
                .map(|(k, v)| 4 + k.len() + compute_size(v))
                .sum::<usize>()
        }
    }
}

fn read_u8(bytes: &[u8], offset: &mut usize) -> crate::Result<u8> {
    if *offset >= bytes.len() {
        return Err(DocumentError::UnexpectedEof { offset: *offset });
    }
    let b = bytes[*offset];
    *offset += 1;
    Ok(b)
}

fn read_bytes<const N: usize>(bytes: &[u8], offset: &mut usize) -> crate::Result<[u8; N]> {
    if *offset + N > bytes.len() {
        return Err(DocumentError::UnexpectedEof { offset: *offset });
    }
    let mut arr = [0u8; N];
    arr.copy_from_slice(&bytes[*offset..*offset + N]);
    *offset += N;
    Ok(arr)
}

fn read_slice<'a>(bytes: &'a [u8], offset: &mut usize, len: usize) -> crate::Result<&'a [u8]> {
    if *offset + len > bytes.len() {
        return Err(DocumentError::UnexpectedEof { offset: *offset });
    }
    let slice = &bytes[*offset..*offset + len];
    *offset += len;
    Ok(slice)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::collection::{btree_map, vec};
    use proptest::prelude::*;
    use std::collections::BTreeMap;

    fn finite_f64() -> impl Strategy<Value = f64> {
        any::<f64>().prop_filter("finite f64", |f| f.is_finite())
    }

    fn string_strategy(max_len: usize) -> impl Strategy<Value = String> {
        vec(any::<char>(), 0..=max_len).prop_map(|chars| chars.into_iter().collect())
    }

    fn value_strategy() -> impl Strategy<Value = Value> {
        let leaf = prop_oneof![
            Just(Value::Null),
            any::<bool>().prop_map(Value::Bool),
            any::<i64>().prop_map(Value::Int),
            finite_f64().prop_map(Value::Float),
            string_strategy(128).prop_map(Value::String),
            vec(any::<u8>(), 0..=256).prop_map(Value::Blob),
        ];

        // `prop_recursive(depth, max_size, max_items)`:
        // - depth: cap recursion depth to keep generated values valid/fast.
        // - max_size: target total generated tree size budget.
        // - max_items: branching factor for recursive collections.
        leaf.prop_recursive(8, 8_192, 16, |inner| {
            prop_oneof![
                vec(inner.clone(), 0..=16).prop_map(Value::Array),
                btree_map(string_strategy(32), inner, 0..=16).prop_map(Value::Object),
            ]
        })
    }

    fn roundtrip(value: &Value) {
        let codec = BinaryCodec;
        let encoded = codec.encode(value).unwrap();
        assert_eq!(encoded.len(), HEADER_SIZE + codec.encoded_size(value));
        let decoded = codec.decode(&encoded).unwrap();
        assert_eq!(value, &decoded);
    }

    #[test]
    fn roundtrip_null() {
        roundtrip(&Value::Null);
    }

    #[test]
    fn roundtrip_bool() {
        roundtrip(&Value::Bool(true));
        roundtrip(&Value::Bool(false));
    }

    #[test]
    fn roundtrip_int() {
        roundtrip(&Value::Int(0));
        roundtrip(&Value::Int(i64::MIN));
        roundtrip(&Value::Int(i64::MAX));
        roundtrip(&Value::Int(-42));
    }

    #[test]
    fn roundtrip_float() {
        roundtrip(&Value::Float(std::f64::consts::PI));
        roundtrip(&Value::Float(0.0));
        roundtrip(&Value::Float(-42.5));
        roundtrip(&Value::Float(f64::MIN));
        roundtrip(&Value::Float(f64::MAX));
    }

    #[test]
    fn roundtrip_string() {
        roundtrip(&Value::String("".into()));
        roundtrip(&Value::String("hello world".into()));
        roundtrip(&Value::String("émoji 🎉".into()));
    }

    #[test]
    fn roundtrip_blob() {
        roundtrip(&Value::Blob(vec![]));
        roundtrip(&Value::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]));
    }

    #[test]
    fn roundtrip_array() {
        roundtrip(&Value::Array(vec![
            Value::Int(1),
            Value::String("two".into()),
            Value::Null,
        ]));
    }

    #[test]
    fn roundtrip_object() {
        let mut map = BTreeMap::new();
        map.insert("name".into(), Value::String("alice".into()));
        map.insert("age".into(), Value::Int(30));
        map.insert(
            "tags".into(),
            Value::Array(vec![Value::String("admin".into())]),
        );
        roundtrip(&Value::Object(map));
    }

    #[test]
    fn roundtrip_nested() {
        let mut inner = BTreeMap::new();
        inner.insert("x".into(), Value::Int(1));
        let mut outer = BTreeMap::new();
        outer.insert("nested".into(), Value::Object(inner));
        roundtrip(&Value::Object(outer));
    }

    #[test]
    fn rejects_unknown_tag() {
        let codec = BinaryCodec;
        // Valid header but unknown tag
        let mut bytes = Vec::new();
        bytes.extend_from_slice(CODEC_MAGIC);
        bytes.push(CODEC_VERSION);
        bytes.push(0xFF);
        let result = codec.decode(&bytes);
        assert!(matches!(
            result,
            Err(DocumentError::UnknownTypeTag { tag: 0xFF })
        ));
    }

    #[test]
    fn rejects_truncated_input() {
        let codec = BinaryCodec;
        // Valid header + Int tag but not enough data
        let mut bytes = Vec::new();
        bytes.extend_from_slice(CODEC_MAGIC);
        bytes.push(CODEC_VERSION);
        bytes.extend_from_slice(&[0x02, 0x00]);
        let result = codec.decode(&bytes);
        assert!(matches!(result, Err(DocumentError::UnexpectedEof { .. })));
    }

    #[test]
    fn rejects_trailing_bytes() {
        let codec = BinaryCodec;
        let mut encoded = codec.encode(&Value::Null).unwrap();
        encoded.push(0x00); // garbage trailing byte
        let result = codec.decode(&encoded);
        assert!(matches!(result, Err(DocumentError::TrailingBytes { .. })));
    }

    #[test]
    fn rejects_bad_version() {
        let codec = BinaryCodec;
        let bytes = [b'B', b'D', b'B', 99, 0x00]; // version 99
        let result = codec.decode(&bytes);
        assert!(matches!(
            result,
            Err(DocumentError::UnsupportedCodecVersion { version: 99 })
        ));
    }

    #[test]
    fn rejects_nan() {
        let codec = BinaryCodec;
        let result = codec.encode(&Value::Float(f64::NAN));
        assert!(matches!(result, Err(DocumentError::NonFiniteFloat { .. })));
    }

    #[test]
    fn rejects_infinity() {
        let codec = BinaryCodec;
        let result = codec.encode(&Value::Float(f64::INFINITY));
        assert!(matches!(result, Err(DocumentError::NonFiniteFloat { .. })));
    }

    #[test]
    fn rejects_deep_nesting() {
        // Build a value nested 65 levels deep
        let mut value = Value::Null;
        for _ in 0..65 {
            value = Value::Array(vec![value]);
        }
        let codec = BinaryCodec;
        let result = codec.encode(&value);
        assert!(matches!(
            result,
            Err(DocumentError::NestingDepthExceeded { .. })
        ));
    }

    proptest! {
        #[test]
        fn decode_encode_roundtrip_holds(value in value_strategy()) {
            let codec = BinaryCodec;
            let encoded = codec.encode(&value)?;
            prop_assert_eq!(encoded.len(), HEADER_SIZE + codec.encoded_size(&value));
            let decoded = codec.decode(&encoded)?;
            prop_assert_eq!(decoded, value);
        }
    }
}
