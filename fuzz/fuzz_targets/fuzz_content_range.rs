#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    kdown_engine::fuzz_targets::fuzz_content_range(data);
});
