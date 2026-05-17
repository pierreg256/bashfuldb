//! RESP response serialiser.
//!
//! Converts a [`Response`] value into the raw wire bytes that the server
//! sends back to the client.

use bytes::Bytes;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::error::ProtocolError;
use crate::types::Response;

// ─── Sync serialisation ───────────────────────────────────────────────────────

/// Serialise a [`Response`] to a `Vec<u8>`.
///
/// The output is a valid RESP frame suitable for writing directly to the wire.
pub fn serialize_response(resp: &Response) -> Vec<u8> {
    let mut buf = Vec::with_capacity(64);
    write_to_buf(resp, &mut buf);
    buf
}

/// Serialise into an existing buffer (avoids an allocation when batching).
pub fn serialize_response_into(resp: &Response, buf: &mut Vec<u8>) {
    write_to_buf(resp, buf);
}

fn write_to_buf(resp: &Response, buf: &mut Vec<u8>) {
    match resp {
        Response::Simple(s) => {
            buf.push(b'+');
            buf.extend_from_slice(s.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }

        Response::Error(s) => {
            buf.push(b'-');
            buf.extend_from_slice(s.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }

        Response::Integer(n) => {
            buf.push(b':');
            buf.extend_from_slice(n.to_string().as_bytes());
            buf.extend_from_slice(b"\r\n");
        }

        Response::Bulk(None) => {
            buf.extend_from_slice(b"$-1\r\n");
        }

        Response::Bulk(Some(data)) => {
            buf.push(b'$');
            buf.extend_from_slice(data.len().to_string().as_bytes());
            buf.extend_from_slice(b"\r\n");
            buf.extend_from_slice(data);
            buf.extend_from_slice(b"\r\n");
        }

        Response::Array(items) => {
            buf.push(b'*');
            buf.extend_from_slice(items.len().to_string().as_bytes());
            buf.extend_from_slice(b"\r\n");
            for item in items {
                write_to_buf(item, buf);
            }
        }

        Response::Warning(s) => {
            buf.extend_from_slice(b"+_warning ");
            buf.extend_from_slice(s.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
    }
}

// ─── Async write helper ───────────────────────────────────────────────────────

/// Serialise and write a [`Response`] directly to an async writer.
pub async fn write_response<W>(writer: &mut W, resp: &Response) -> Result<(), ProtocolError>
where
    W: AsyncWrite + Unpin,
{
    let buf = serialize_response(resp);
    writer.write_all(&buf).await.map_err(ProtocolError::Io)
}

/// Write a plain error message to the wire without constructing a full
/// [`Response`].  Convenient during the pre-auth phase.
pub(crate) async fn write_error_str<W>(writer: &mut W, msg: &str) -> Result<(), ProtocolError>
where
    W: AsyncWrite + Unpin,
{
    write_response(writer, &Response::Error(msg.to_string())).await
}

/// Convert a `Bytes` value into a `Response::Bulk`.
pub fn bulk(data: impl Into<Bytes>) -> Response {
    Response::Bulk(Some(data.into()))
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn ser(r: &Response) -> String {
        String::from_utf8(serialize_response(r)).unwrap()
    }

    #[test]
    fn serialise_simple() {
        assert_eq!(ser(&Response::Simple("OK".into())), "+OK\r\n");
    }

    #[test]
    fn serialise_error() {
        assert_eq!(
            ser(&Response::Error("ERR something".into())),
            "-ERR something\r\n"
        );
    }

    #[test]
    fn serialise_integer() {
        assert_eq!(ser(&Response::Integer(42)), ":42\r\n");
        assert_eq!(ser(&Response::Integer(-1)), ":-1\r\n");
        assert_eq!(ser(&Response::Integer(0)), ":0\r\n");
    }

    #[test]
    fn serialise_bulk_null() {
        assert_eq!(ser(&Response::Bulk(None)), "$-1\r\n");
    }

    #[test]
    fn serialise_bulk_data() {
        assert_eq!(
            ser(&Response::Bulk(Some(Bytes::from_static(b"foobar")))),
            "$6\r\nfoobar\r\n"
        );
    }

    #[test]
    fn serialise_bulk_empty() {
        assert_eq!(ser(&Response::Bulk(Some(Bytes::new()))), "$0\r\n\r\n");
    }

    #[test]
    fn serialise_array() {
        let r = Response::Array(vec![Response::Simple("PONG".into()), Response::Integer(7)]);
        assert_eq!(ser(&r), "*2\r\n+PONG\r\n:7\r\n");
    }

    #[test]
    fn serialise_empty_array() {
        assert_eq!(ser(&Response::Array(vec![])), "*0\r\n");
    }

    #[test]
    fn serialise_warning() {
        assert_eq!(
            ser(&Response::Warning("lag detected".into())),
            "+_warning lag detected\r\n"
        );
    }

    #[test]
    fn serialise_nested_array() {
        let inner = Response::Array(vec![Response::Integer(1), Response::Integer(2)]);
        let outer = Response::Array(vec![inner]);
        let s = ser(&outer);
        assert_eq!(s, "*1\r\n*2\r\n:1\r\n:2\r\n");
    }

    #[test]
    fn bulk_helper() {
        let r = bulk("hello");
        assert_eq!(r, Response::Bulk(Some(Bytes::from_static(b"hello"))));
    }

    #[tokio::test]
    async fn write_response_async() {
        let mut buf = Vec::new();
        write_response(&mut buf, &Response::Simple("OK".into()))
            .await
            .unwrap();
        assert_eq!(buf, b"+OK\r\n");
    }
}
