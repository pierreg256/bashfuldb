use crate::{DocumentError, ObjectId, Result, Value};
use serde::{Deserialize, Serialize};

/// A document is an identified object with a top-level Object value.
///
/// The `data` field is guaranteed to be [`Value::Object`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Document {
    id: ObjectId,
    data: Value,
}

impl Document {
    /// Creates a new document, validating that `data` is an Object.
    pub fn new(id: ObjectId, data: Value) -> Result<Self> {
        if !data.is_object() {
            return Err(DocumentError::NotAnObject {
                actual: data.type_name(),
            });
        }
        Ok(Self { id, data })
    }

    /// Creates a new document with a random ID.
    pub fn with_random_id(data: Value) -> Result<Self> {
        Self::new(ObjectId::new(), data)
    }

    /// Returns the document ID.
    pub fn id(&self) -> ObjectId {
        self.id
    }

    /// Returns a reference to the document data.
    pub fn data(&self) -> &Value {
        &self.data
    }

    /// Consumes the document and returns its parts.
    pub fn into_parts(self) -> (ObjectId, Value) {
        (self.id, self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn accepts_object() {
        let mut map = BTreeMap::new();
        map.insert("key".into(), Value::Int(42));
        let doc = Document::new(ObjectId::new(), Value::Object(map));
        assert!(doc.is_ok());
    }

    #[test]
    fn rejects_non_object() {
        let err = Document::new(ObjectId::new(), Value::Int(42)).unwrap_err();
        assert!(matches!(err, DocumentError::NotAnObject { actual: "Int" }));
    }

    #[test]
    fn rejects_null() {
        let err = Document::new(ObjectId::new(), Value::Null).unwrap_err();
        assert!(matches!(err, DocumentError::NotAnObject { actual: "Null" }));
    }
}
