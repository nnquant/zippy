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
fn compile_schema_accepts_runtime_owned_column_names_and_timezones() {
    let dt_column = "dt".to_string();
    let timezone = "Asia/Shanghai".to_string();
    let schema = compile_schema(&[
        ColumnSpec::new(
            dt_column.as_str(),
            ColumnType::timestamp_ns_tz(timezone.as_str()),
        ),
        ColumnSpec::new("instrument_id".to_string(), ColumnType::Utf8),
    ])
    .unwrap();
    let layout = LayoutPlan::for_schema(&schema, 64).unwrap();

    assert_eq!(schema.columns()[0].name(), "dt");
    assert!(layout.column("instrument_id").is_some());
}

#[test]
fn compile_schema_repeated_runtime_owned_inputs_keep_stable_schema_identity() {
    let expected_schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::nullable("last_price", ColumnType::Float64),
    ])
    .unwrap();

    for _ in 0..1024 {
        let dt_column = "dt".to_string();
        let id_column = "instrument_id".to_string();
        let price_column = "last_price".to_string();
        let timezone = "Asia/Shanghai".to_string();
        let schema = compile_schema(&[
            ColumnSpec::new(dt_column, ColumnType::timestamp_ns_tz(timezone)),
            ColumnSpec::new(id_column, ColumnType::Utf8),
            ColumnSpec::nullable(price_column, ColumnType::Float64),
        ])
        .unwrap();
        let layout = LayoutPlan::for_schema(&schema, 64).unwrap();

        assert_eq!(schema.schema_id(), expected_schema.schema_id());
        assert!(layout.column("dt").is_some());
        assert!(layout.column("instrument_id").is_some());
        assert!(layout.column("last_price").is_some());
    }
}

#[test]
fn layout_plan_rejects_capacity_overflow() {
    let schema = compile_schema(&[ColumnSpec::new("instrument_id", ColumnType::Utf8)]).unwrap();

    let result = LayoutPlan::for_schema(&schema, usize::MAX);

    assert!(result.is_err());
}
