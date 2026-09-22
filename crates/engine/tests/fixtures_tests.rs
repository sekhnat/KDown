//! Fixture generator round-trip tests (§36.6).

#[path = "support/mod.rs"]
mod support;

use support::fixtures::{assert_bytes_exact, boundary_sizes, deterministic_bytes, sha256_hex};

#[test]
fn deterministic_bytes_are_stable() {
    let a = deterministic_bytes(1024, 42);
    let b = deterministic_bytes(1024, 42);
    let c = deterministic_bytes(1024, 43);
    assert_bytes_exact(&a, &b);
    assert_ne!(a, c, "different seeds must differ");
}

#[test]
fn boundary_sizes_match_spec() {
    let chunk = 128 * 1024;
    let segment = 8 * 1024 * 1024;
    let sizes = boundary_sizes(chunk, segment);
    assert_eq!(
        sizes,
        vec![
            0,
            1,
            chunk - 1,
            chunk,
            chunk + 1,
            segment - 1,
            segment,
            segment + 1,
            4 * chunk
        ]
    );
}

#[test]
fn exact_comparison_helper_detects_mismatches() {
    let expected = deterministic_bytes(256, 7);
    // Same length, wrong byte: must panic with position.
    let mut corrupt = expected.clone();
    corrupt[100] ^= 0xFF;
    let result = std::panic::catch_unwind(|| assert_bytes_exact(&corrupt, &expected));
    let msg = result
        .err()
        .and_then(|e| e.downcast_ref::<String>().cloned())
        .unwrap_or_default();
    assert!(
        msg.contains("byte 100"),
        "mismatch message names the offset: {msg}"
    );

    // Length mismatch reported first.
    let result = std::panic::catch_unwind(|| assert_bytes_exact(&expected[..10], &expected));
    let msg = std::panic::catch_unwind(|| {
        let mut c = expected[..10].to_vec();
        c.extend_from_slice(&expected[10..]);
        assert_bytes_exact(&c, &expected);
    })
    .is_err();
    let _ = (result, msg, assert_bytes_exact(&expected, &expected));
}

#[test]
fn sha256_hex_known_vector() {
    assert_eq!(
        sha256_hex(b"abc"),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
}
