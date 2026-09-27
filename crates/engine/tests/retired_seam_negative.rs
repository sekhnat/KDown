//! Negative-visibility check for the retired injection seam
//! (consumer-api task 2.3): a downstream consumer must NOT be able to
//! import the retired scripted/execution seam or the removed result types.

#[test]
fn retired_seam_is_not_importable() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/retired_seam/*.rs");
}
