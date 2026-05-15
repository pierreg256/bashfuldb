use crate::{DocumentError, Result};

/// Validates a tenant, database, or collection name.
///
/// Names must match the pattern `[a-z0-9_]{1,64}` and must not be one of the
/// reserved names (`_default`, `_system`).
pub fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > 64 {
        return Err(DocumentError::InvalidName {
            name: name.to_string(),
        });
    }

    if !name
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    {
        return Err(DocumentError::InvalidName {
            name: name.to_string(),
        });
    }

    if name == "_default" || name == "_system" {
        return Err(DocumentError::ReservedName {
            name: name.to_string(),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DocumentError;

    #[test]
    fn valid_names() {
        assert!(validate_name("users").is_ok());
        assert!(validate_name("my_collection_123").is_ok());
        assert!(validate_name("a").is_ok());
        assert!(validate_name(&"x".repeat(64)).is_ok());
    }

    #[test]
    fn empty_name() {
        assert!(matches!(
            validate_name(""),
            Err(DocumentError::InvalidName { .. })
        ));
    }

    #[test]
    fn too_long() {
        assert!(matches!(
            validate_name(&"x".repeat(65)),
            Err(DocumentError::InvalidName { .. })
        ));
    }

    #[test]
    fn uppercase_rejected() {
        assert!(matches!(
            validate_name("Users"),
            Err(DocumentError::InvalidName { .. })
        ));
    }

    #[test]
    fn special_chars_rejected() {
        assert!(matches!(
            validate_name("my-collection"),
            Err(DocumentError::InvalidName { .. })
        ));
        assert!(matches!(
            validate_name("my.collection"),
            Err(DocumentError::InvalidName { .. })
        ));
    }

    #[test]
    fn reserved_names() {
        assert!(matches!(
            validate_name("_default"),
            Err(DocumentError::ReservedName { .. })
        ));
        assert!(matches!(
            validate_name("_system"),
            Err(DocumentError::ReservedName { .. })
        ));
    }
}
