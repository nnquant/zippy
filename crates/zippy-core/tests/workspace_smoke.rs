#[test]
fn workspace_builds_core_crate() {
    assert_eq!(zippy_core::crate_name(), "zippy-core");
}
