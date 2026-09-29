use kdown_app::domain::{ControlVersion, SourceUrl};

#[test]
fn signed_query_survives_while_display_is_redacted() {
    let source =
        SourceUrl::parse("https://example.test/file.iso?X-Amz-Signature=abc123&part=7").unwrap();

    assert_eq!(
        source.persisted(),
        "https://example.test/file.iso?X-Amz-Signature=abc123&part=7"
    );
    assert_eq!(source.redacted(), "https://example.test/file.iso?…");
    assert!(!format!("{source:?}").contains("abc123"));
}

#[test]
fn source_url_rejects_embedded_credentials() {
    let error = SourceUrl::parse("https://user:secret@example.test/file.iso").unwrap_err();
    assert_eq!(error.code(), "source_credentials_unsupported");
}

#[test]
fn control_versions_advance_without_wrapping() {
    assert_eq!(ControlVersion::new(4).next().unwrap().get(), 5);
    assert!(ControlVersion::new(ControlVersion::MAX_JSON_INTEGER)
        .next()
        .is_err());
}
