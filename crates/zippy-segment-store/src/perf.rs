use std::cmp::Ordering;
use std::sync::Arc;
use std::time::Instant;

use arrow::{
    array::{Float64Array, StringArray, TimestampNanosecondArray},
    record_batch::RecordBatch,
};

use crate::{
    arrow_bridge::build_arrow_schema, compile_schema, ActiveSegmentReader, ActiveSegmentWriter,
    ColumnSpec, ColumnType, LayoutPlan, RowSpanView,
};

/// 最小性能 smoke 报告。
#[derive(Debug, Clone, PartialEq)]
pub struct PerfReport {
    pub total_rows: usize,
    pub p50_latency_us: f64,
    pub p95_latency_us: f64,
    pub p99_latency_us: f64,
}

/// 运行一条最小同线程 writer/read 回路，用于验证 segment 路径能产出可解释的延迟分位。
pub fn run_perf_smoke_for_test(rows: usize) -> Result<PerfReport, &'static str> {
    run_segment_perf_smoke_for_test(rows)
}

/// 运行一条最小单行 `RecordBatch` 回路，用于给 segment 路径提供同口径 batch 基线。
pub fn run_batch_perf_smoke_for_test(rows: usize) -> Result<PerfReport, &'static str> {
    if rows == 0 {
        return Err("rows must be greater than zero");
    }

    let schema = perf_schema()?;
    let arrow_schema = build_arrow_schema(&schema);
    let mut latencies_us = Vec::with_capacity(rows);

    for idx in 0..rows {
        let started_at = Instant::now();
        let (dt, instrument_id, last_price) = perf_row(idx);
        let batch = RecordBatch::try_new(
            Arc::clone(&arrow_schema),
            vec![
                Arc::new(
                    TimestampNanosecondArray::from(vec![dt])
                        .with_timezone(perf_timezone().to_string()),
                ),
                Arc::new(StringArray::from(vec![instrument_id])),
                Arc::new(Float64Array::from(vec![last_price])),
            ],
        )
        .map_err(|_| "failed to materialize record batch")?;

        if batch.num_rows() != 1 {
            return Err("perf smoke expected a single visible row");
        }

        latencies_us.push(started_at.elapsed().as_secs_f64() * 1_000_000.0);
    }

    Ok(build_perf_report(rows, latencies_us))
}

/// 运行 descriptor attach active reader 回路，覆盖跨进程热读的最小基线。
pub fn run_active_reader_perf_smoke_for_test(rows: usize) -> Result<PerfReport, &'static str> {
    if rows == 0 {
        return Err("rows must be greater than zero");
    }

    let schema = perf_schema()?;
    let layout = LayoutPlan::for_schema(&schema, rows)?;
    let mut writer = ActiveSegmentWriter::new_for_runtime(schema.clone(), layout.clone(), 1, 0)?;
    let descriptor_envelope = writer.active_descriptor().to_envelope_bytes()?;
    let mut reader =
        ActiveSegmentReader::from_descriptor_envelope(&descriptor_envelope, schema, layout)
            .map_err(|_| "failed to attach active segment reader")?;
    let mut latencies_us = Vec::with_capacity(rows);

    for idx in 0..rows {
        let started_at = Instant::now();
        let (dt, instrument_id, last_price) = perf_row(idx);
        writer.append_tick_for_test(dt, instrument_id, last_price)?;

        let span = reader
            .read_available()
            .map_err(|_| "failed to read active segment span")?
            .ok_or("active segment reader did not see appended row")?;
        let batch = span
            .as_record_batch()
            .map_err(|_| "failed to materialize active reader record batch")?;
        if batch.num_rows() != 1 {
            return Err("active reader perf smoke expected a single visible row");
        }

        latencies_us.push(started_at.elapsed().as_secs_f64() * 1_000_000.0);
    }

    Ok(build_perf_report(rows, latencies_us))
}

fn run_segment_perf_smoke_for_test(rows: usize) -> Result<PerfReport, &'static str> {
    if rows == 0 {
        return Err("rows must be greater than zero");
    }

    let schema = perf_schema()?;
    let layout = LayoutPlan::for_schema(&schema, rows)?;
    let mut writer = ActiveSegmentWriter::new_for_runtime(schema, layout, 1, 0)?;
    let mut latencies_us = Vec::with_capacity(rows);

    for idx in 0..rows {
        let started_at = Instant::now();
        let (dt, instrument_id, last_price) = perf_row(idx);
        writer.append_tick_for_test(dt, instrument_id, last_price)?;

        let row_count = writer.committed_row_count();
        let handle = writer.sealed_handle_for_test()?;
        let span = RowSpanView::new(handle, row_count - 1, row_count)?;
        let batch = span
            .as_record_batch()
            .map_err(|_| "failed to materialize record batch")?;
        if batch.num_rows() != 1 {
            return Err("perf smoke expected a single visible row");
        }

        latencies_us.push(started_at.elapsed().as_secs_f64() * 1_000_000.0);
    }

    Ok(build_perf_report(rows, latencies_us))
}

fn build_perf_report(rows: usize, mut latencies_us: Vec<f64>) -> PerfReport {
    latencies_us.sort_by(|left, right| match left.partial_cmp(right) {
        Some(ordering) => ordering,
        None => Ordering::Equal,
    });

    PerfReport {
        total_rows: rows,
        p50_latency_us: percentile(&latencies_us, 0.50),
        p95_latency_us: percentile(&latencies_us, 0.95),
        p99_latency_us: percentile(&latencies_us, 0.99),
    }
}

fn perf_row(idx: usize) -> (i64, &'static str, f64) {
    let instrument_id = if idx % 2 == 0 { "IF2606" } else { "IH2606" };
    (idx as i64, instrument_id, 4000.0 + idx as f64)
}

fn perf_schema() -> Result<crate::CompiledSchema, &'static str> {
    compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz(perf_timezone())),
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::new("last_price", ColumnType::Float64),
    ])
}

fn perf_timezone() -> &'static str {
    "Asia/Shanghai"
}

fn percentile(sorted: &[f64], fraction: f64) -> f64 {
    let index = ((sorted.len() - 1) as f64 * fraction).round() as usize;
    sorted[index]
}
