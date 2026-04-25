#[test]
fn segment_batch_perf_smoke_compares_tick_rows() {
    let report = zippy_segment_store::perf::run_perf_smoke_for_test(2_000).unwrap();
    let active_reader_report =
        zippy_segment_store::perf::run_active_reader_perf_smoke_for_test(2_000).unwrap();
    let batch_report = zippy_segment_store::perf::run_batch_perf_smoke_for_test(2_000).unwrap();

    println!(
        "segment perf smoke total_rows=[{}] p50_us=[{:.3}] p95_us=[{:.3}] p99_us=[{:.3}]",
        report.total_rows, report.p50_latency_us, report.p95_latency_us, report.p99_latency_us
    );
    println!(
        "active reader perf smoke total_rows=[{}] p50_us=[{:.3}] p95_us=[{:.3}] p99_us=[{:.3}]",
        active_reader_report.total_rows,
        active_reader_report.p50_latency_us,
        active_reader_report.p95_latency_us,
        active_reader_report.p99_latency_us
    );
    println!(
        "batch perf smoke total_rows=[{}] p50_us=[{:.3}] p95_us=[{:.3}] p99_us=[{:.3}]",
        batch_report.total_rows,
        batch_report.p50_latency_us,
        batch_report.p95_latency_us,
        batch_report.p99_latency_us
    );

    assert_eq!(report.total_rows, 2_000);
    assert!(report.p50_latency_us > 0.0);
    assert!(report.p95_latency_us > 0.0);
    assert!(report.p99_latency_us > 0.0);
    assert_eq!(active_reader_report.total_rows, 2_000);
    assert!(active_reader_report.p50_latency_us > 0.0);
    assert!(active_reader_report.p95_latency_us > 0.0);
    assert!(active_reader_report.p99_latency_us > 0.0);
    assert_eq!(batch_report.total_rows, 2_000);
    assert!(batch_report.p50_latency_us > 0.0);
    assert!(batch_report.p95_latency_us > 0.0);
    assert!(batch_report.p99_latency_us > 0.0);
}
