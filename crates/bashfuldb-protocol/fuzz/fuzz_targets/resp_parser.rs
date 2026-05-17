//! Fuzz target for the RESP-like parser.
//!
//! Runs arbitrary byte sequences through the parser and asserts that:
//!   1. The parser never panics.
//!   2. The parser always returns either `Ok(_)` or a typed `Err(_)`.
//!
//! To run:
//! ```sh
//! cargo fuzz run resp_parser
//! ```
#![no_main]

use bashfuldb_protocol::{parse_frame, DEFAULT_MAX_BULK_LENGTH, DEFAULT_MAX_LINE_LENGTH};
use libfuzzer_sys::fuzz_target;
use tokio::io::BufReader;

fuzz_target!(|data: &[u8]| {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("build rt");

    rt.block_on(async move {
        let mut reader = BufReader::new(data);
        // The parser must never panic regardless of the input.
        let _ = parse_frame(
            &mut reader,
            DEFAULT_MAX_LINE_LENGTH,
            DEFAULT_MAX_BULK_LENGTH,
        )
        .await;
    });
});
