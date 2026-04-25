use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use zippy_shm_bridge::{ReadResult, SharedFrameRing};

const BUFFER_SIZE: usize = 131_072;
const FRAME_SIZE: usize = 65_536;

fn unique_mmap_path(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("zippy-shm-bench-{label}-{nanos}.mmap"))
}

fn payload_of_size(size: usize) -> Vec<u8> {
    vec![b'x'; size]
}

fn bench_publish(c: &mut Criterion) {
    let mut group = c.benchmark_group("frame_ring_publish");

    for size in [256_usize, 4_096, 65_536] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let mmap_path = unique_mmap_path("publish");
            let mut ring =
                SharedFrameRing::create_or_open(&mmap_path, BUFFER_SIZE, FRAME_SIZE).unwrap();
            let payload = payload_of_size(size);

            b.iter(|| {
                let seq = ring.publish(black_box(&payload)).unwrap();
                black_box(seq);
            });
        });
    }

    group.finish();
}

fn bench_read_ready(c: &mut Criterion) {
    let mut group = c.benchmark_group("frame_ring_read_ready");

    for size in [256_usize, 4_096, 65_536] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let mmap_path = unique_mmap_path("read-ready");
            let mut writer =
                SharedFrameRing::create_or_open(&mmap_path, BUFFER_SIZE, FRAME_SIZE).unwrap();
            let reader =
                SharedFrameRing::create_or_open(&mmap_path, BUFFER_SIZE, FRAME_SIZE).unwrap();
            let payload = payload_of_size(size);
            let seq = writer.publish(&payload).unwrap();

            b.iter(|| match reader.read(black_box(seq)).unwrap() {
                ReadResult::Ready(frame) => {
                    black_box(frame.payload.len());
                }
                other => panic!("unexpected read result: {other:?}"),
            });
        });
    }

    group.finish();
}

fn bench_seek_latest(c: &mut Criterion) {
    let mmap_path = unique_mmap_path("seek-latest");
    let mut ring = SharedFrameRing::create_or_open(&mmap_path, BUFFER_SIZE, FRAME_SIZE).unwrap();
    let payload = payload_of_size(4_096);
    for _ in 0..128 {
        ring.publish(&payload).unwrap();
    }

    c.bench_function("frame_ring_seek_latest", |b| {
        b.iter(|| {
            let seq = ring.seek_latest().unwrap();
            black_box(seq);
        });
    });
}

fn bench_roundtrip(c: &mut Criterion) {
    let mmap_path = unique_mmap_path("roundtrip");
    let mut writer = SharedFrameRing::create_or_open(&mmap_path, BUFFER_SIZE, FRAME_SIZE).unwrap();
    let reader = SharedFrameRing::create_or_open(&mmap_path, BUFFER_SIZE, FRAME_SIZE).unwrap();
    let payload = payload_of_size(4_096);

    c.bench_function("frame_ring_publish_read_roundtrip_4kb", |b| {
        b.iter(|| {
            let seq = writer.publish(black_box(&payload)).unwrap();
            match reader.read(seq).unwrap() {
                ReadResult::Ready(frame) => {
                    black_box(frame.payload.len());
                }
                other => panic!("unexpected read result: {other:?}"),
            }
        });
    });
}

criterion_group!(
    benches,
    bench_publish,
    bench_read_ready,
    bench_seek_latest,
    bench_roundtrip
);
criterion_main!(benches);
