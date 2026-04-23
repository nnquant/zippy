use zippy_segment_store::{compile_schema, ColumnSpec, ColumnType, LayoutPlan};

#[test]
fn compile_schema_assigns_stable_column_order_and_layout() {
    let schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::nullable("bid_price_1", ColumnType::Float64),
    ])
    .unwrap();

    let layout = LayoutPlan::for_schema(&schema, 64).unwrap();

    assert_eq!(schema.schema_id(), 3);
    assert_eq!(layout.row_capacity(), 64);
    assert!(layout.column("instrument_id").unwrap().offsets_len > 0);
    assert!(layout.column("bid_price_1").unwrap().validity_len > 0);
}
