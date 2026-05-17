//! Cursor-based pagination helpers.
//!
//! Cursors are opaque base64-encoded strings that encode the last-seen object ID.
//! The client passes `?cursor=<value>` to advance to the next page; the server
//! includes `next_cursor` in its response (or `null` when no more pages remain).

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bashfuldb_document::ObjectId;
use serde::Serialize;

/// The pagination section included in every list response.
#[derive(Debug, Serialize)]
pub struct PageInfo {
    /// Cursor pointing to the next page, or `null` if this is the last page.
    pub next_cursor: Option<String>,
    /// Number of items returned in this page.
    pub count: usize,
}

/// Encodes an [`ObjectId`] as an opaque cursor string.
pub fn encode_cursor(id: ObjectId) -> String {
    URL_SAFE_NO_PAD.encode(id.to_bytes())
}

/// Decodes an opaque cursor string back into an [`ObjectId`].
///
/// Returns `None` if the cursor is malformed.
pub fn decode_cursor(cursor: &str) -> Option<ObjectId> {
    let bytes = URL_SAFE_NO_PAD.decode(cursor).ok()?;
    let arr: [u8; 16] = bytes.try_into().ok()?;
    Some(ObjectId::from_bytes(arr))
}

/// Parses the optional cursor query parameter, returning an [`ApiError`] on failure.
pub fn parse_cursor_param(s: Option<&str>) -> Result<Option<ObjectId>, crate::ApiError> {
    match s {
        None => Ok(None),
        Some(c) => decode_cursor(c)
            .map(Some)
            .ok_or_else(|| crate::ApiError::BadRequest(format!("invalid cursor: {c}"))),
    }
}

/// Clamp the requested page size to the configured maximum.
pub fn effective_limit(requested: Option<usize>, default: usize, max: usize) -> usize {
    requested.unwrap_or(default).min(max)
}

/// Given a full list of object IDs, applies cursor and limit to produce a page.
///
/// Returns `(page_of_ids, next_cursor)`.
pub fn paginate_ids(
    all_ids: &[ObjectId],
    cursor: Option<ObjectId>,
    limit: usize,
) -> (Vec<ObjectId>, Option<String>) {
    let start = match cursor {
        None => 0,
        Some(after_id) => all_ids
            .iter()
            .position(|id| *id == after_id)
            .map(|i| i + 1)
            .unwrap_or(0),
    };

    let page: Vec<ObjectId> = all_ids.iter().skip(start).take(limit).copied().collect();
    let next_cursor = if start + limit < all_ids.len() {
        page.last().map(|id| encode_cursor(*id))
    } else {
        None
    };

    (page, next_cursor)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_roundtrip() {
        let id = ObjectId::new();
        let encoded = encode_cursor(id);
        let decoded = decode_cursor(&encoded).unwrap();
        assert_eq!(id, decoded);
    }

    #[test]
    fn invalid_cursor_returns_none() {
        assert!(decode_cursor("not-base64!!!").is_none());
        assert!(decode_cursor("aGVsbG8=").is_none()); // valid base64 but wrong length
    }

    #[test]
    fn paginate_first_page() {
        let ids: Vec<ObjectId> = (0..10).map(|_| ObjectId::new()).collect();
        let (page, next) = paginate_ids(&ids, None, 3);
        assert_eq!(page.len(), 3);
        assert!(next.is_some());
    }

    #[test]
    fn paginate_last_page_has_no_next_cursor() {
        let ids: Vec<ObjectId> = (0..5).map(|_| ObjectId::new()).collect();
        let (page, next) = paginate_ids(&ids, None, 10);
        assert_eq!(page.len(), 5);
        assert!(next.is_none());
    }

    #[test]
    fn paginate_empty_list() {
        let (page, next) = paginate_ids(&[], None, 10);
        assert!(page.is_empty());
        assert!(next.is_none());
    }

    #[test]
    fn paginate_with_cursor() {
        let ids: Vec<ObjectId> = (0..10).map(|_| ObjectId::new()).collect();
        let cursor = ids[2];
        let (page, _) = paginate_ids(&ids, Some(cursor), 3);
        // should start at ids[3]
        assert_eq!(page[0], ids[3]);
        assert_eq!(page.len(), 3);
    }

    #[test]
    fn effective_limit_clamps() {
        assert_eq!(effective_limit(Some(500), 100, 1000), 500);
        assert_eq!(effective_limit(Some(2000), 100, 1000), 1000);
        assert_eq!(effective_limit(None, 100, 1000), 100);
    }
}
