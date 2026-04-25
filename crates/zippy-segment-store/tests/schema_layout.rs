use zippy_segment_store::{compile_schema, ColumnSpec, ColumnType, LayoutPlan};

#[test]
fn compile_schema_assigns_content_based_schema_id_and_layout() {
    let schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::nullable("bid_price_1", ColumnType::Float64),
    ])
    .unwrap();
    let same_schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::nullable("bid_price_1", ColumnType::Float64),
    ])
    .unwrap();
    let different_schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::nullable("ask_price_1", ColumnType::Float64),
    ])
    .unwrap();

    let layout = LayoutPlan::for_schema(&schema, 64).unwrap();

    assert_eq!(schema.schema_id(), same_schema.schema_id());
    assert_ne!(schema.schema_id(), different_schema.schema_id());
    assert_eq!(layout.row_capacity(), 64);
    assert!(layout.column("instrument_id").unwrap().offsets_len > 0);
    assert!(layout.column("bid_price_1").unwrap().validity_len > 0);
}

#[test]
fn compile_schema_rejects_duplicate_column_names() {
    let result = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::Int64),
        ColumnSpec::new("dt", ColumnType::Float64),
    ]);

    assert!(result.is_err());
}

#[test]
fn layout_plan_rejects_capacity_overflow() {
    let schema = compile_schema(&[ColumnSpec::new("instrument_id", ColumnType::Utf8)]).unwrap();

    let result = LayoutPlan::for_schema(&schema, usize::MAX);

    assert!(result.is_err());
}
