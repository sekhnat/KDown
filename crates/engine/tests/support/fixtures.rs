//! Deterministic fixture generators and byte-exact comparison helpers
//! (§36.6).

/// Boundary sizes from §36.6: 0, 1, chunk±1/exact, segment±1/exact.
#[must_use]
pub fn boundary_sizes(chunk: u64, segment: u64) -> Vec<u64> {
    vec![
        0,
        1,
        chunk - 1,
        chunk,
        chunk + 1,
        segment - 1,
        segment,
        segment + 1,
        4 * chunk, // multi-segment smoke size
    ]
}

/// Deterministic pseudo-random content of `len` bytes. Same seed produces
/// the same bytes across platforms and runs, so failure output is diffable.
#[must_use]
pub fn deterministic_bytes(len: u64, seed: u64) -> Vec<u8> {
    let len = len as usize;
    let mut out = Vec::with_capacity(len);
    let mut state = seed
        .wrapping_mul(0x9E3779B97F4A7C15)
        .wrapping_add(0x517CC1B727220A95);
    for _ in 0..len {
        // xorshift64
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.push((state >> 24) as u8);
    }
    out
}

/// Assert two byte slices are exactly equal, with a length-first failure
/// message that bounds diff output.
#[track_caller]
pub fn assert_bytes_exact(actual: &[u8], expected: &[u8]) {
    if actual.len() != expected.len() {
        panic!(
            "length mismatch: got {} bytes, expected {} bytes",
            actual.len(),
            expected.len()
        );
    }
    if actual != expected {
        let diff = actual
            .iter()
            .zip(expected)
            .position(|(a, e)| a != e)
            .unwrap_or(0);
        panic!(
            "content mismatch at byte {diff}: got {:#04x}, expected {:#04x}",
            actual[diff], expected[diff]
        );
    }
}

/// SHA-256 hex digest of `bytes` (fixture identity checks).
#[must_use]
pub fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(bytes);
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

/// SHA-256 hex digest of a file read sequentially (§16.2 verification
/// shape; used by byte-exact comparisons of completed outputs).
///
/// # Panics
/// When the file cannot be read (test fixture failure).
#[must_use]
pub fn file_sha256(path: &std::path::Path) -> String {
    use sha2::{Digest, Sha256};
    let file = std::fs::File::open(path).expect("open fixture output");
    let mut reader = std::io::BufReader::with_capacity(256 * 1024, file);
    let mut h = Sha256::new();
    std::io::copy(&mut reader, &mut h).expect("hash fixture output");
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

/// Synthetic per-block fixture constants shared with the process-isolated
/// fixture server and the benchmark matrix (task 1.3/1.4).
pub const SYNTHETIC_BLOCK: usize = 4096;

/// One synthetic block: the canonical xorshift run seeded with `seed ^ i`.
/// Any range of any size is derivable without whole-file memory.
#[must_use]
pub fn synthetic_block_bytes(block_index: u64, seed: u64) -> Vec<u8> {
    deterministic_bytes(SYNTHETIC_BLOCK as u64, seed ^ block_index)
}

/// Expected SHA-256 of the synthetic fixture content: block-wise hashing
/// with no whole-file allocation (parity reference for the isolated server).
#[must_use]
pub fn synthetic_expected_sha256(len: u64, seed: u64) -> String {
    use sha2::Digest;
    let mut h = sha2::Sha256::new();
    let mut block_index = 0u64;
    let mut remaining = len;
    while remaining > 0 {
        let block = synthetic_block_bytes(block_index, seed);
        let take = (block.len() as u64).min(remaining) as usize;
        h.update(&block[..take]);
        remaining -= take as u64;
        block_index += 1;
    }
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}
