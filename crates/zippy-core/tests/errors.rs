#[test]
fn structured_error_code_does_not_depend_on_message_words() {
    use zippy_core::ZippyError;
    use zippy_types::ErrorCode;

    let not_found = ZippyError::NotFound {
        reason: "authentication_events is missing".to_owned(),
    };
    assert_eq!(not_found.code(), ErrorCode::NotFound);
    let authentication = ZippyError::Unauthenticated {
        reason: "expired credential".to_owned(),
    };
    assert_eq!(authentication.code(), ErrorCode::Unauthenticated);
    assert!(!authentication.code().retryable_by_default());
}
