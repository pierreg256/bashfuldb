//! RESP-like wire protocol parser.
//!
//! Provides [`parse_frame`] for reading any RESP value and [`parse_command`]
//! for reading a full client command (a RESP array).
//!
//! Both functions are `async` and read from any [`tokio::io::AsyncBufRead`]
//! source.  Limits are enforced during reading; a malicious peer cannot cause
//! an out-of-memory condition by sending an unbounded line.

use std::pin::Pin;

use bytes::Bytes;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt};

use crate::error::ProtocolError;
use crate::types::{Command, MAX_NESTING_DEPTH};

/// Maximum number of array elements pre-allocated when parsing a RESP array
/// from untrusted input.
///
/// When a peer announces a large array count (e.g., `*1000000\r\n`) we must
/// not immediately allocate that much memory. We cap the initial allocation at
/// this value; the `Vec` will grow organically if the actual element count is
/// larger, bounded by the real data received and the nesting-depth limit.
const MAX_ARRAY_PREALLOC: usize = 256;

// ─── Internal RESP frame ──────────────────────────────────────────────────────

/// Internal representation of a single RESP value.
///
/// Used by the parser; callers typically receive a [`Command`] instead.
#[derive(Debug, Clone, PartialEq)]
pub enum RespFrame {
    /// `+<message>`
    SimpleString(String),
    /// `-<message>`
    Error(String),
    /// `:<n>`
    Integer(i64),
    /// `$<len>` bulk string, or null (`$-1`)
    Bulk(Option<Bytes>),
    /// `*<count>` array, or null (`*-1`)
    Array(Option<Vec<RespFrame>>),
    /// `+_warning <message>` — inline warning sentinel
    Warning(String),
}

// ─── Public entry points ──────────────────────────────────────────────────────

/// Parse one RESP frame from `reader`.
///
/// Returns `Ok(None)` on a clean EOF (zero bytes read on the first byte of a
/// new frame).  Any other incomplete read returns `Err(ProtocolError::UnexpectedEof)`.
///
/// # Errors
///
/// Returns [`ProtocolError`] if:
/// - a line exceeds `max_line` bytes,
/// - a bulk string exceeds `max_bulk` bytes,
/// - the input is malformed (invalid type byte, bad length, missing CRLF, etc.),
/// - array nesting exceeds [`MAX_NESTING_DEPTH`].
pub async fn parse_frame<R>(
    reader: &mut R,
    max_line: usize,
    max_bulk: usize,
) -> Result<Option<RespFrame>, ProtocolError>
where
    R: AsyncBufRead + Unpin + Send,
{
    parse_frame_inner(reader, max_line, max_bulk, 0).await
}

/// Parse one RESP array and convert it to a [`Command`].
///
/// Returns `Ok(None)` on clean EOF.
///
/// # Errors
///
/// Returns [`ProtocolError`] if the frame is not a non-empty array, or if any
/// other parse error occurs.
pub async fn parse_command<R>(
    reader: &mut R,
    max_line: usize,
    max_bulk: usize,
) -> Result<Option<Command>, ProtocolError>
where
    R: AsyncBufRead + Unpin + Send,
{
    match parse_frame_inner(reader, max_line, max_bulk, 0).await? {
        None => Ok(None),
        Some(RespFrame::Array(Some(elements))) if !elements.is_empty() => {
            frame_array_to_command(elements)
        }
        Some(RespFrame::Array(Some(_))) => Err(ProtocolError::Malformed(
            "command array must not be empty".to_string(),
        )),
        Some(RespFrame::Array(None)) => Err(ProtocolError::Malformed(
            "null array is not a valid command".to_string(),
        )),
        Some(_) => Err(ProtocolError::Malformed(
            "commands must be sent as RESP arrays".to_string(),
        )),
    }
}

// ─── Internal helpers ─────────────────────────────────────────────────────────

/// Recursive frame parser with nesting-depth guard.
///
/// Returns a boxed future to avoid infinitely-sized future types from
/// the recursive call site.
fn parse_frame_inner<'a, R>(
    reader: &'a mut R,
    max_line: usize,
    max_bulk: usize,
    depth: usize,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<Option<RespFrame>, ProtocolError>> + Send + 'a>,
>
where
    R: AsyncBufRead + Unpin + Send + 'a,
{
    Box::pin(async move {
        if depth > MAX_NESTING_DEPTH {
            return Err(ProtocolError::NestingTooDeep(MAX_NESTING_DEPTH));
        }

        // Read the header line (type byte + payload + CRLF)
        let header = match read_line_limited(reader, max_line).await? {
            None => return Ok(None),
            Some(h) => h,
        };

        if header.is_empty() {
            return Err(ProtocolError::Malformed(
                "received empty RESP header line".to_string(),
            ));
        }

        let type_byte = header.as_bytes()[0];
        let payload = &header[1..];

        match type_byte {
            b'+' => {
                // Check for the warning sentinel
                if let Some(msg) = payload.strip_prefix("_warning ") {
                    Ok(Some(RespFrame::Warning(msg.to_string())))
                } else {
                    Ok(Some(RespFrame::SimpleString(payload.to_string())))
                }
            }

            b'-' => Ok(Some(RespFrame::Error(payload.to_string()))),

            b':' => {
                let n = payload.parse::<i64>().map_err(|_| {
                    ProtocolError::Malformed(format!("invalid integer: {payload:?}"))
                })?;
                Ok(Some(RespFrame::Integer(n)))
            }

            b'$' => {
                let len_i = payload.parse::<i64>().map_err(|_| {
                    ProtocolError::Malformed(format!("invalid bulk length: {payload:?}"))
                })?;
                if len_i < 0 {
                    return Ok(Some(RespFrame::Bulk(None)));
                }
                let len = len_i as usize;
                if len > max_bulk {
                    return Err(ProtocolError::BulkTooLarge { len, max: max_bulk });
                }
                let mut data = vec![0u8; len];
                reader
                    .read_exact(&mut data)
                    .await
                    .map_err(|_| ProtocolError::UnexpectedEof)?;
                // Consume the trailing CRLF
                let mut crlf = [0u8; 2];
                reader
                    .read_exact(&mut crlf)
                    .await
                    .map_err(|_| ProtocolError::UnexpectedEof)?;
                if crlf != *b"\r\n" {
                    return Err(ProtocolError::Malformed(
                        "expected CRLF after bulk string data".to_string(),
                    ));
                }
                Ok(Some(RespFrame::Bulk(Some(Bytes::from(data)))))
            }

            b'*' => {
                let count_i = payload.parse::<i64>().map_err(|_| {
                    ProtocolError::Malformed(format!("invalid array count: {payload:?}"))
                })?;
                if count_i < 0 {
                    return Ok(Some(RespFrame::Array(None)));
                }
                let count = count_i as usize;
                // Pre-allocate conservatively to avoid OOM on untrusted large
                // counts (see `MAX_ARRAY_PREALLOC`).
                let mut elements = Vec::with_capacity(count.min(MAX_ARRAY_PREALLOC));
                for _ in 0..count {
                    let elem = parse_frame_inner(reader, max_line, max_bulk, depth + 1)
                        .await?
                        .ok_or(ProtocolError::UnexpectedEof)?;
                    elements.push(elem);
                }
                Ok(Some(RespFrame::Array(Some(elements))))
            }

            other => Err(ProtocolError::UnknownType(other)),
        }
    })
}

/// Read a single `\r\n`-terminated line, enforcing `max_len`.
///
/// Returns `Ok(None)` only on a completely clean EOF (zero bytes consumed).
///
/// The returned string has the CRLF stripped.
pub(crate) async fn read_line_limited<R>(
    reader: &mut R,
    max_len: usize,
) -> Result<Option<String>, ProtocolError>
where
    R: AsyncBufRead + Unpin,
{
    let mut buf: Vec<u8> = Vec::with_capacity(64);

    loop {
        // Scope ensures `available` borrow is released before `consume`.
        let (chunk, to_consume, found_newline) = {
            let available = AsyncBufReadExt::fill_buf(reader)
                .await
                .map_err(ProtocolError::Io)?;

            if available.is_empty() {
                // EOF
                return if buf.is_empty() {
                    Ok(None)
                } else {
                    Err(ProtocolError::UnexpectedEof)
                };
            }

            let (end, found) = match available.iter().position(|&b| b == b'\n') {
                Some(pos) => (pos + 1, true),
                None => (available.len(), false),
            };

            let new_total = buf.len() + end;
            if new_total > max_len + 2 {
                // +2 for \r\n
                (
                    Vec::new(),
                    end,
                    Err(ProtocolError::LineTooLong {
                        len: new_total,
                        max: max_len,
                    }),
                )
            } else {
                (available[..end].to_vec(), end, Ok(found))
            }
        }; // `available` borrow ends here

        Pin::new(&mut *reader).consume(to_consume);

        match found_newline {
            Err(e) => return Err(e),
            Ok(found) => {
                buf.extend_from_slice(&chunk);
                if found {
                    break;
                }
            }
        }
    }

    // Strip trailing CRLF
    if buf.ends_with(b"\r\n") {
        buf.truncate(buf.len() - 2);
    } else if buf.ends_with(b"\n") {
        buf.truncate(buf.len() - 1);
    }

    String::from_utf8(buf)
        .map(Some)
        .map_err(|_| ProtocolError::Malformed("invalid UTF-8 in RESP line".to_string()))
}

/// Convert a parsed RESP array into a [`Command`].
fn frame_array_to_command(elements: Vec<RespFrame>) -> Result<Option<Command>, ProtocolError> {
    let mut iter = elements.into_iter();

    let name_raw = iter
        .next()
        .ok_or_else(|| ProtocolError::Malformed("command array is empty".to_string()))?;

    let name = match name_raw {
        RespFrame::Bulk(Some(b)) => String::from_utf8(b.to_vec())
            .map_err(|_| ProtocolError::Malformed("command name is not valid UTF-8".to_string()))?,
        RespFrame::SimpleString(s) => s,
        _ => {
            return Err(ProtocolError::Malformed(
                "command name must be a bulk string".to_string(),
            ));
        }
    };

    let name = name.to_uppercase();

    let args: Result<Vec<Bytes>, ProtocolError> = iter
        .map(|f| match f {
            RespFrame::Bulk(Some(b)) => Ok(b),
            RespFrame::Bulk(None) => Ok(Bytes::new()),
            RespFrame::SimpleString(s) => Ok(Bytes::from(s.into_bytes())),
            _ => Err(ProtocolError::Malformed(
                "command arguments must be bulk strings".to_string(),
            )),
        })
        .collect();

    Ok(Some(Command { name, args: args? }))
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::BufReader;

    const ML: usize = 1_048_576;
    const MB: usize = 16_777_216;

    async fn parse(input: &[u8]) -> Result<Option<RespFrame>, ProtocolError> {
        let mut reader = BufReader::new(input);
        parse_frame(&mut reader, ML, MB).await
    }

    async fn cmd(input: &[u8]) -> Result<Option<Command>, ProtocolError> {
        let mut reader = BufReader::new(input);
        parse_command(&mut reader, ML, MB).await
    }

    #[tokio::test]
    async fn parse_simple_string() {
        let frame = parse(b"+OK\r\n").await.unwrap().unwrap();
        assert_eq!(frame, RespFrame::SimpleString("OK".to_string()));
    }

    #[tokio::test]
    async fn parse_error() {
        let frame = parse(b"-ERR something went wrong\r\n")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            frame,
            RespFrame::Error("ERR something went wrong".to_string())
        );
    }

    #[tokio::test]
    async fn parse_integer() {
        let frame = parse(b":42\r\n").await.unwrap().unwrap();
        assert_eq!(frame, RespFrame::Integer(42));

        let neg = parse(b":-1\r\n").await.unwrap().unwrap();
        assert_eq!(neg, RespFrame::Integer(-1));
    }

    #[tokio::test]
    async fn parse_bulk_string() {
        let frame = parse(b"$6\r\nfoobar\r\n").await.unwrap().unwrap();
        assert_eq!(frame, RespFrame::Bulk(Some(Bytes::from_static(b"foobar"))));
    }

    #[tokio::test]
    async fn parse_bulk_empty() {
        let frame = parse(b"$0\r\n\r\n").await.unwrap().unwrap();
        assert_eq!(frame, RespFrame::Bulk(Some(Bytes::new())));
    }

    #[tokio::test]
    async fn parse_null_bulk() {
        let frame = parse(b"$-1\r\n").await.unwrap().unwrap();
        assert_eq!(frame, RespFrame::Bulk(None));
    }

    #[tokio::test]
    async fn parse_array() {
        let frame = parse(b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            frame,
            RespFrame::Array(Some(vec![
                RespFrame::Bulk(Some(Bytes::from_static(b"GET"))),
                RespFrame::Bulk(Some(Bytes::from_static(b"key"))),
            ]))
        );
    }

    #[tokio::test]
    async fn parse_null_array() {
        let frame = parse(b"*-1\r\n").await.unwrap().unwrap();
        assert_eq!(frame, RespFrame::Array(None));
    }

    #[tokio::test]
    async fn parse_warning_sentinel() {
        let frame = parse(b"+_warning replication lag high\r\n")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            frame,
            RespFrame::Warning("replication lag high".to_string())
        );
    }

    #[tokio::test]
    async fn parse_clean_eof() {
        let frame = parse(b"").await.unwrap();
        assert!(frame.is_none());
    }

    #[tokio::test]
    async fn parse_command_get() {
        let c = cmd(b"*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(c.name, "GET");
        assert_eq!(c.args.len(), 1);
        assert_eq!(c.args[0], Bytes::from_static(b"hello"));
    }

    #[tokio::test]
    async fn parse_command_uppercases_name() {
        let c = cmd(b"*1\r\n$4\r\nping\r\n").await.unwrap().unwrap();
        assert_eq!(c.name, "PING");
    }

    #[tokio::test]
    async fn limit_line_too_long() {
        // Build a line of 100 bytes with no \n
        let mut input = vec![b'+'];
        input.extend(std::iter::repeat_n(b'A', 100));
        input.extend_from_slice(b"\r\n");

        let mut reader = BufReader::new(input.as_slice());
        let result = parse_frame(&mut reader, 50, MB).await;
        assert!(matches!(result, Err(ProtocolError::LineTooLong { .. })));
    }

    #[tokio::test]
    async fn limit_bulk_too_large() {
        // Declare a 100-byte bulk but limit is 50
        let input = b"$100\r\n";
        let mut reader = BufReader::new(input.as_slice());
        let result = parse_frame(&mut reader, ML, 50).await;
        assert!(matches!(result, Err(ProtocolError::BulkTooLarge { .. })));
    }

    #[tokio::test]
    async fn unknown_type_byte() {
        let result = parse(b"!hello\r\n").await;
        assert!(matches!(result, Err(ProtocolError::UnknownType(b'!'))));
    }

    #[tokio::test]
    async fn invalid_integer() {
        let result = parse(b":notanumber\r\n").await;
        assert!(matches!(result, Err(ProtocolError::Malformed(_))));
    }

    #[tokio::test]
    async fn bulk_missing_crlf() {
        let result = parse(b"$3\r\nabc--").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn nested_array() {
        let input = b"*1\r\n*1\r\n$2\r\nhi\r\n";
        let frame = parse(input).await.unwrap().unwrap();
        assert!(matches!(
            frame,
            RespFrame::Array(Some(ref v)) if v.len() == 1
        ));
    }

    #[tokio::test]
    async fn nesting_too_deep() {
        // Build a deeply nested array: *1\r\n repeated MAX_NESTING_DEPTH+2 times
        let depth = MAX_NESTING_DEPTH + 2;
        let mut input = Vec::new();
        for _ in 0..depth {
            input.extend_from_slice(b"*1\r\n");
        }
        input.extend_from_slice(b"$0\r\n\r\n");
        let result = parse(&input).await;
        assert!(matches!(result, Err(ProtocolError::NestingTooDeep(_))));
    }
}
