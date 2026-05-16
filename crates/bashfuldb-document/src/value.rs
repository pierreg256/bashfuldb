use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;

/// A schema-less value representing any storable datum.
///
/// Objects use [`BTreeMap`] for deterministic key ordering.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    /// JSON null.
    Null,
    /// Boolean value.
    Bool(bool),
    /// Signed 64-bit integer.
    Int(i64),
    /// 64-bit floating point (IEEE 754).
    Float(f64),
    /// UTF-8 string (max 16 MiB).
    String(String),
    /// Raw binary blob (max 16 MiB).
    Blob(Vec<u8>),
    /// Ordered array of values (max 1,000,000 elements).
    Array(Vec<Value>),
    /// Ordered map of string keys to values (max 100,000 keys).
    Object(BTreeMap<String, Value>),
}

impl Value {
    /// Returns the type name as a static string (for error messages).
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => "Null",
            Value::Bool(_) => "Bool",
            Value::Int(_) => "Int",
            Value::Float(_) => "Float",
            Value::String(_) => "String",
            Value::Blob(_) => "Blob",
            Value::Array(_) => "Array",
            Value::Object(_) => "Object",
        }
    }

    /// Returns `true` if this value is an Object.
    pub fn is_object(&self) -> bool {
        matches!(self, Value::Object(_))
    }

    /// Returns `true` if this value is Null.
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Returns the binary type tag for this value.
    pub fn type_tag(&self) -> u8 {
        match self {
            Value::Null => 0x00,
            Value::Bool(_) => 0x01,
            Value::Int(_) => 0x02,
            Value::Float(_) => 0x03,
            Value::String(_) => 0x04,
            Value::Blob(_) => 0x05,
            Value::Array(_) => 0x06,
            Value::Object(_) => 0x07,
        }
    }

    /// Returns the string value if this is a `String`.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    /// Returns the integer value if this is an `Int`.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int(i) => Some(*i),
            _ => None,
        }
    }

    /// Returns the float value if this is a `Float`.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float(f) => Some(*f),
            _ => None,
        }
    }

    /// Returns the boolean value if this is a `Bool`.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Returns a reference to the inner array if this is an `Array`.
    pub fn as_array(&self) -> Option<&[Value]> {
        match self {
            Value::Array(a) => Some(a),
            _ => None,
        }
    }

    /// Returns a reference to the inner map if this is an `Object`.
    pub fn as_object(&self) -> Option<&BTreeMap<String, Value>> {
        match self {
            Value::Object(o) => Some(o),
            _ => None,
        }
    }

    /// Returns a reference to the blob bytes if this is a `Blob`.
    pub fn as_blob(&self) -> Option<&[u8]> {
        match self {
            Value::Blob(b) => Some(b),
            _ => None,
        }
    }
}

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "Null"),
            Value::Bool(v) => write!(f, "Bool({v})"),
            Value::Int(v) => write!(f, "Int({v})"),
            Value::Float(v) => write!(f, "Float({v})"),
            Value::String(v) if v.len() > 32 => {
                write!(f, "String(\"{}...\" len={})", &v[..32], v.len())
            }
            Value::String(v) => write!(f, "String({v:?})"),
            Value::Blob(v) => write!(f, "Blob({} bytes)", v.len()),
            Value::Array(v) => write!(f, "Array({} items)", v.len()),
            Value::Object(v) => write!(f, "Object({} keys)", v.len()),
        }
    }
}

// ── JSON conversion ──────────────────────────────────────────────────────

impl From<serde_json::Value> for Value {
    fn from(json: serde_json::Value) -> Self {
        match json {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(b) => Value::Bool(b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Value::Int(i)
                } else {
                    Value::Float(n.as_f64().unwrap_or(0.0))
                }
            }
            serde_json::Value::String(s) => Value::String(s),
            serde_json::Value::Array(a) => Value::Array(a.into_iter().map(Value::from).collect()),
            serde_json::Value::Object(o) => {
                Value::Object(o.into_iter().map(|(k, v)| (k, Value::from(v))).collect())
            }
        }
    }
}

impl From<Value> for serde_json::Value {
    fn from(value: Value) -> Self {
        match value {
            Value::Null => serde_json::Value::Null,
            Value::Bool(b) => serde_json::Value::Bool(b),
            Value::Int(i) => serde_json::Value::Number(i.into()),
            Value::Float(f) => serde_json::Number::from_f64(f)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            Value::String(s) => serde_json::Value::String(s),
            Value::Blob(b) => {
                use base64::Engine;
                serde_json::Value::String(base64::engine::general_purpose::STANDARD.encode(b))
            }
            Value::Array(a) => {
                serde_json::Value::Array(a.into_iter().map(serde_json::Value::from).collect())
            }
            Value::Object(o) => serde_json::Value::Object(
                o.into_iter()
                    .map(|(k, v)| (k, serde_json::Value::from(v)))
                    .collect(),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_tags() {
        assert_eq!(Value::Null.type_tag(), 0x00);
        assert_eq!(Value::Bool(true).type_tag(), 0x01);
        assert_eq!(Value::Int(42).type_tag(), 0x02);
        assert_eq!(Value::Float(std::f64::consts::PI).type_tag(), 0x03);
        assert_eq!(Value::String("hi".into()).type_tag(), 0x04);
        assert_eq!(Value::Blob(vec![]).type_tag(), 0x05);
        assert_eq!(Value::Array(vec![]).type_tag(), 0x06);
        assert_eq!(Value::Object(BTreeMap::new()).type_tag(), 0x07);
    }

    #[test]
    fn json_roundtrip_primitives() {
        let values = vec![
            Value::Null,
            Value::Bool(true),
            Value::Int(42),
            Value::Float(std::f64::consts::PI),
            Value::String("hello".into()),
        ];
        for v in values {
            let json: serde_json::Value = v.clone().into();
            let back = Value::from(json);
            assert_eq!(v, back);
        }
    }

    #[test]
    fn json_roundtrip_nested() {
        let mut obj = BTreeMap::new();
        obj.insert("a".into(), Value::Int(1));
        obj.insert("b".into(), Value::Array(vec![Value::Bool(false)]));
        let v = Value::Object(obj);

        let json: serde_json::Value = v.clone().into();
        let back = Value::from(json);
        assert_eq!(v, back);
    }

    #[test]
    fn blob_to_json_is_base64() {
        let v = Value::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        let json: serde_json::Value = v.into();
        assert!(json.is_string());
        assert_eq!(json.as_str().unwrap(), "3q2+7w==");
    }
}
