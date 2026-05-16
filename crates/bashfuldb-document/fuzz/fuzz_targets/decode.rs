#![no_main]

use bashfuldb_document::{BinaryCodec, Codec};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let codec = BinaryCodec;
    let _ = codec.decode(data);
});
