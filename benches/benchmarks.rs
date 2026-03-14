//! Criterion benchmarks for shift hot paths: BLAKE3, LZ4, LE framing, RangeQueue.

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use shift::base::{split_file_ranges, transfer_num_ranges, FileRange};
use shift::range_queue::RangeQueue;
use std::sync::Arc;
use std::thread;

fn blake3_chunk(c: &mut Criterion) {
    let mut group = c.benchmark_group("blake3");
    for size in [4096, 64 * 1024, 1024 * 1024, 4 * 1024 * 1024] {
        let data = vec![0xABu8; size];
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(
            criterion::BenchmarkId::new("hash", size),
            &data,
            |b, data| {
                b.iter(|| {
                    let mut h = blake3::Hasher::new();
                    h.update(black_box(data));
                    black_box(h.finalize())
                })
            },
        );
    }
    group.finish();
}

fn lz4_compress_compressible(c: &mut Criterion) {
    let mut group = c.benchmark_group("lz4_compress");
    for size in [64 * 1024, 256 * 1024, 1024 * 1024] {
        let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(
            criterion::BenchmarkId::new("compressible", size),
            &data,
            |b, data| {
                b.iter(|| {
                    let mut enc = lz4::EncoderBuilder::new().build(Vec::new()).unwrap();
                    std::io::Write::write_all(&mut enc, black_box(data)).unwrap();
                    let (out, _) = enc.finish();
                    black_box(out)
                })
            },
        );
    }
    group.finish();
}

fn lz4_compress_incompressible(c: &mut Criterion) {
    let mut group = c.benchmark_group("lz4_compress");
    for size in [64 * 1024, 256 * 1024, 1024 * 1024] {
        let data = vec![0xFFu8; size];
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(
            criterion::BenchmarkId::new("incompressible", size),
            &data,
            |b, data| {
                b.iter(|| {
                    let mut enc = lz4::EncoderBuilder::new().build(Vec::new()).unwrap();
                    std::io::Write::write_all(&mut enc, black_box(data)).unwrap();
                    let (out, _) = enc.finish();
                    black_box(out)
                })
            },
        );
    }
    group.finish();
}

fn lz4_decompress(c: &mut Criterion) {
    let mut group = c.benchmark_group("lz4_decompress");
    for size in [64 * 1024, 256 * 1024, 1024 * 1024] {
        let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
        let mut enc = lz4::EncoderBuilder::new().build(Vec::new()).unwrap();
        std::io::Write::write_all(&mut enc, &data).unwrap();
        let (compressed, _) = enc.finish();
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(
            criterion::BenchmarkId::new("decompress", size),
            &compressed,
            |b, compressed| {
                b.iter(|| {
                    let mut dec = lz4::Decoder::new(black_box(compressed.as_slice())).unwrap();
                    let mut out = Vec::with_capacity(size);
                    std::io::copy(&mut dec, &mut out).unwrap();
                    black_box(out)
                })
            },
        );
    }
    group.finish();
}

/// LE framing: encode range header (start 8 + end 8 + flags 1) and packet (flag 1 + size 8 + payload).
fn le_framing_encode_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("le_framing");
    let range = FileRange::new(0, 64 * 1024);
    let payload = vec![0u8; 8192];

    group.bench_function("encode_header", |b| {
        b.iter(|| {
            let mut h = Vec::with_capacity(17);
            h.extend_from_slice(&black_box(range.start).to_le_bytes());
            h.extend_from_slice(&black_box(range.end).to_le_bytes());
            h.push(0x01u8);
            black_box(h)
        })
    });

    group.bench_function("decode_header", |b| {
        let mut buf = Vec::with_capacity(17);
        buf.extend_from_slice(&range.start.to_le_bytes());
        buf.extend_from_slice(&range.end.to_le_bytes());
        buf.push(0x01);
        b.iter(|| {
            let start = u64::from_le_bytes(buf[0..8].try_into().unwrap());
            let end = u64::from_le_bytes(buf[8..16].try_into().unwrap());
            let flags = buf[16];
            black_box((start, end, flags))
        })
    });

    group.bench_function("encode_packet", |b| {
        b.iter(|| {
            let mut p = Vec::with_capacity(9 + payload.len());
            p.push(0x01u8);
            p.extend_from_slice(&(black_box(payload.len()) as u64).to_le_bytes());
            p.extend_from_slice(&payload);
            black_box(p)
        })
    });

    group.finish();
}

fn range_queue_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_queue");
    for num_ranges in [16, 64, 128] {
        for num_workers in [4, 8, 16] {
            let file_size = 100 * 1024 * 1024u64;
            let ranges = split_file_ranges(file_size, num_ranges);
            group.bench_with_input(
                criterion::BenchmarkId::new(
                    "pop_contention",
                    format!("{}ranges_{}workers", num_ranges, num_workers),
                ),
                &(ranges, num_workers),
                |b, (ranges, num_workers)| {
                    b.iter(|| {
                        let queue = Arc::new(RangeQueue::new(ranges.clone()));
                        let mut handles = Vec::new();
                        for id in 0..*num_workers {
                            let q = Arc::clone(&queue);
                            handles.push(thread::spawn(move || {
                                let mut count = 0u64;
                                while let Some(range) = q.pop() {
                                    q.mark_in_flight(id, range);
                                    count += range.end - range.start;
                                    q.complete(id);
                                }
                                count
                            }));
                        }
                        let total: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
                        black_box(total)
                    })
                },
            );
        }
    }
    group.finish();
}

fn split_file_ranges_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("split_file_ranges");
    let file_size = 10 * 1024 * 1024 * 1024u64; // 10 GiB
    for max_streams in [8, 32, 128] {
        let num_ranges = transfer_num_ranges(max_streams);
        group.bench_with_input(
            criterion::BenchmarkId::new("split", num_ranges),
            &num_ranges,
            |b, &n| b.iter(|| black_box(split_file_ranges(file_size, n))),
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    blake3_chunk,
    lz4_compress_compressible,
    lz4_compress_incompressible,
    lz4_decompress,
    le_framing_encode_decode,
    range_queue_contention,
    split_file_ranges_bench,
);
criterion_main!(benches);
