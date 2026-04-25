use zippy_operator_runtime::Operator;

struct NoopOperator;

impl Operator for NoopOperator {
    fn name(&self) -> &'static str {
        "noop"
    }

    fn required_columns(&self) -> &'static [&'static str] {
        &[]
    }
}

#[test]
fn operator_runtime_workspace_exports_operator_trait() {
    let op = NoopOperator;
    assert_eq!(op.name(), "noop");
}
