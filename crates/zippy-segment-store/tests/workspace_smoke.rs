use zippy_segment_store::{compile_schema, ColumnSpec, ColumnType};

#[test]
fn segment_store_workspace_exports_compile_schema() {
    let schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("last_price", ColumnType::Float64),
    ])
    .unwrap();

    assert_eq!(schema.columns().len(), 2);
}
