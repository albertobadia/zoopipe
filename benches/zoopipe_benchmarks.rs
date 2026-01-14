use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use std::hint::black_box;
use std::fs::File;
use std::io::{BufWriter, Write, Read, Seek, SeekFrom};
use std::sync::atomic::Ordering;
use tempfile::TempDir;

fn create_test_csv(dir: &TempDir, rows: usize) -> String {
    let path = dir.path().join("test.csv");
    let file = File::create(&path).unwrap();
    let mut writer = BufWriter::new(file);
    
    writeln!(writer, "id,name,value,active").unwrap();
    for i in 0..rows {
        writeln!(writer, "{},user_{},{},{}", i, i, i * 100, i % 2 == 0).unwrap();
    }
    writer.flush().unwrap();
    
    path.to_string_lossy().to_string()
}

fn bench_csv_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("csv_parsing");
    
    for size in [100, 1000, 10000].iter() {
        let dir = TempDir::new().unwrap();
        let path = create_test_csv(&dir, *size);
        
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(
            BenchmarkId::new("std_csv_reader", size),
            &path,
            |b, path| {
                b.iter(|| {
                    let file = File::open(path).unwrap();
                    let mut rdr = csv::Reader::from_reader(file);
                    let mut count = 0;
                    for result in rdr.records() {
                        let _ = result.unwrap();
                        count += 1;
                    }
                    count
                });
            },
        );
    }
    
    group.finish();
}

fn bench_uuid_generation(c: &mut Criterion) {
    let mut group = c.benchmark_group("uuid_generation");
    
    group.bench_function("uuid_v4_single", |b| {
        b.iter(uuid::Uuid::new_v4)
    });
    
    group.bench_function("uuid_v4_batch_1000", |b| {
        b.iter(|| {
            let mut ids = Vec::with_capacity(1000);
            for _ in 0..1000 {
                ids.push(uuid::Uuid::new_v4());
            }
            ids
        })
    });
    
    group.finish();
}

fn bench_json_serialization(c: &mut Criterion) {
    use serde::{Deserialize, Serialize};
    
    #[derive(Serialize, Deserialize, Clone)]
    struct Record {
        id: u64,
        name: String,
        value: f64,
        active: bool,
    }
    
    let mut group = c.benchmark_group("json_serialization");
    
    let record = Record {
        id: 1,
        name: "test_user".to_string(),
        value: 123.456,
        active: true,
    };
    
    group.bench_function("serialize_single", |b| {
        b.iter(|| serde_json::to_string(&record).unwrap())
    });
    
    let records: Vec<Record> = (0..1000)
        .map(|i| Record {
            id: i,
            name: format!("user_{}", i),
            value: i as f64 * 1.5,
            active: i % 2 == 0,
        })
        .collect();

    group.bench_function("serialize_batch_1000", |b| {
        b.iter(|| serde_json::to_string(&records).unwrap())
    });

    let json_str = serde_json::to_string(&records).unwrap();
    group.bench_function("deserialize_batch_1000", |b| {
        b.iter(|| serde_json::from_str::<Vec<Record>>(&json_str).unwrap())
    });

    group.finish();
}

fn bench_boxed_reader(c: &mut Criterion) {
    use zoopipe_rust_core::io::BoxedReader;
    
    let mut group = c.benchmark_group("boxed_reader");
    
    for size in [1024, 10240, 102400].iter() {
        let data: Vec<u8> = (0..*size).map(|i| (i % 256) as u8).collect();
        
        group.throughput(Throughput::Bytes(*size as u64));
        
        group.bench_with_input(
            BenchmarkId::new("cursor_read_all", size),
            &data,
            |b, data| {
                b.iter(|| {
                    let cursor = std::io::Cursor::new(data.clone());
                    let mut reader = BoxedReader::Cursor(cursor);
                    let mut buf = Vec::new();
                    reader.read_to_end(&mut buf).unwrap();
                    buf.len()
                });
            },
        );
        
        group.bench_with_input(
            BenchmarkId::new("cursor_chunked_read", size),
            &data,
            |b, data| {
                b.iter(|| {
                    let cursor = std::io::Cursor::new(data.clone());
                    let mut reader = BoxedReader::Cursor(cursor);
                    let mut buf = [0u8; 1024];
                    let mut total = 0;
                    loop {
                        let n = reader.read(&mut buf).unwrap();
                        if n == 0 { break; }
                        total += n;
                    }
                    total
                });
            },
        );
    }
    
    group.finish();
}

fn bench_boxed_reader_seek(c: &mut Criterion) {
    use zoopipe_rust_core::io::BoxedReader;
    
    let mut group = c.benchmark_group("boxed_reader_seek");
    
    let data: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();
    
    group.bench_function("seek_start", |b| {
        let cursor = std::io::Cursor::new(data.clone());
        let mut reader = BoxedReader::Cursor(cursor);
        b.iter(|| {
            reader.seek(SeekFrom::Start(black_box(50_000))).unwrap()
        });
    });
    
    group.bench_function("seek_current", |b| {
        let cursor = std::io::Cursor::new(data.clone());
        let mut reader = BoxedReader::Cursor(cursor);
        reader.seek(SeekFrom::Start(25_000)).unwrap();
        b.iter(|| {
            reader.seek(SeekFrom::Current(black_box(100))).unwrap();
            reader.seek(SeekFrom::Current(black_box(-100))).unwrap()
        });
    });
    
    group.bench_function("seek_end", |b| {
        let cursor = std::io::Cursor::new(data.clone());
        let mut reader = BoxedReader::Cursor(cursor);
        b.iter(|| {
            reader.seek(SeekFrom::End(black_box(-1000))).unwrap()
        });
    });
    
    group.finish();
}

fn bench_storage_controller(c: &mut Criterion) {
    use zoopipe_rust_core::io::storage::StorageController;
    
    let mut group = c.benchmark_group("storage_controller");
    
    group.bench_function("parse_local_path", |b| {
        b.iter(|| {
            StorageController::new(black_box("/tmp/data/file.csv")).unwrap()
        });
    });
    
    group.bench_function("parse_local_deep_path", |b| {
        b.iter(|| {
            StorageController::new(black_box("/very/deep/nested/path/to/some/file.parquet")).unwrap()
        });
    });
    
    group.finish();
}

fn bench_memory_stats(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_stats");
    
    group.bench_function("get_rss", |b| {
        b.iter(|| {
            memory_stats::memory_stats().map(|m| m.physical_mem)
        });
    });
    
    group.bench_function("get_process_ram_rss", |b| {
        b.iter(|| {
            zoopipe_rust_core::pipeline::get_process_ram_rss()
        });
    });
    
    group.finish();
}

fn bench_pipe_counters(c: &mut Criterion) {
    use zoopipe_rust_core::pipeline::PipeCounters;
    
    let mut group = c.benchmark_group("pipe_counters");
    
    group.bench_function("new", |b| {
        b.iter(PipeCounters::new)
    });
    
    group.bench_function("fetch_add_single", |b| {
        let counters = PipeCounters::new();
        b.iter(|| {
            counters.total_processed.fetch_add(black_box(1), Ordering::Relaxed)
        });
    });
    
    group.bench_function("fetch_add_all_counters", |b| {
        let counters = PipeCounters::new();
        b.iter(|| {
            counters.total_processed.fetch_add(black_box(100), Ordering::Relaxed);
            counters.success_count.fetch_add(black_box(95), Ordering::Relaxed);
            counters.error_count.fetch_add(black_box(5), Ordering::Relaxed);
            counters.batches_processed.fetch_add(black_box(1), Ordering::Relaxed)
        });
    });
    
    group.bench_function("load_all_counters", |b| {
        let counters = PipeCounters::new();
        counters.total_processed.fetch_add(1000, Ordering::Relaxed);
        counters.success_count.fetch_add(950, Ordering::Relaxed);
        counters.error_count.fetch_add(50, Ordering::Relaxed);
        counters.batches_processed.fetch_add(10, Ordering::Relaxed);
        
        b.iter(|| {
            let _ = counters.total_processed.load(Ordering::Relaxed);
            let _ = counters.success_count.load(Ordering::Relaxed);
            let _ = counters.error_count.load(Ordering::Relaxed);
            counters.batches_processed.load(Ordering::Relaxed)
        });
    });
    
    group.finish();
}

fn bench_parallel_strategy(c: &mut Criterion) {
    use zoopipe_rust_core::executor::ParallelStrategy;
    
    let mut group = c.benchmark_group("parallel_strategy");
    
    group.bench_function("new_4_threads", |b| {
        b.iter(|| ParallelStrategy::new(black_box(4)))
    });
    
    group.bench_function("new_with_zero", |b| {
        b.iter(|| ParallelStrategy::new(black_box(0)))
    });
    
    group.finish();
}

fn bench_error_creation(c: &mut Criterion) {
    use zoopipe_rust_core::error::PipeError;
    
    let mut group = c.benchmark_group("error_creation");
    
    group.bench_function("field_conversion", |b| {
        b.iter(|| {
            PipeError::FieldConversion { 
                field: black_box("user_id").to_string() 
            }
        });
    });
    
    group.bench_function("invalid_config", |b| {
        b.iter(|| {
            PipeError::InvalidConfig(black_box("missing batch_size").to_string())
        });
    });
    
    group.bench_function("other", |b| {
        b.iter(|| {
            PipeError::Other(black_box("unexpected error").to_string())
        });
    });
    
    group.bench_function("error_to_string", |b| {
        let error = PipeError::FieldConversion { field: "test_field".to_string() };
        b.iter(|| {
            error.to_string()
        });
    });
    
    group.finish();
}

fn bench_arrow_builders(c: &mut Criterion) {
    use arrow::array::{Int64Builder, Float64Builder, StringBuilder, BooleanBuilder};
    
    let mut group = c.benchmark_group("arrow_builders");
    
    group.bench_function("int64_builder_1000", |b| {
        b.iter(|| {
            let mut builder = Int64Builder::with_capacity(1000);
            for i in 0..1000i64 {
                builder.append_value(i);
            }
            builder.finish()
        });
    });
    
    group.bench_function("float64_builder_1000", |b| {
        b.iter(|| {
            let mut builder = Float64Builder::with_capacity(1000);
            for i in 0..1000 {
                builder.append_value(i as f64 * 1.5);
            }
            builder.finish()
        });
    });
    
    group.bench_function("string_builder_1000", |b| {
        b.iter(|| {
            let mut builder = StringBuilder::with_capacity(1000, 1000 * 20);
            for i in 0..1000 {
                builder.append_value(format!("value_{}", i));
            }
            builder.finish()
        });
    });
    
    group.bench_function("boolean_builder_1000", |b| {
        b.iter(|| {
            let mut builder = BooleanBuilder::with_capacity(1000);
            for i in 0..1000 {
                builder.append_value(i % 2 == 0);
            }
            builder.finish()
        });
    });
    
    group.finish();
}

fn bench_rayon_threadpool(c: &mut Criterion) {
    use rayon::prelude::*;
    
    let mut group = c.benchmark_group("rayon_threadpool");
    
    group.bench_function("threadpool_build_4", |b| {
        b.iter(|| {
            rayon::ThreadPoolBuilder::new()
                .num_threads(black_box(4))
                .build()
                .unwrap()
        });
    });
    
    let data: Vec<i64> = (0..10000).collect();
    
    group.bench_function("par_iter_sum_10000", |b| {
        b.iter(|| {
            data.par_iter().sum::<i64>()
        });
    });
    
    group.bench_function("par_iter_map_10000", |b| {
        b.iter(|| {
            data.par_iter().map(|x| x * 2).collect::<Vec<_>>()
        });
    });
    
    group.finish();
}

fn bench_crossbeam_channel(c: &mut Criterion) {
    use crossbeam_channel::{bounded, unbounded};
    
    let mut group = c.benchmark_group("crossbeam_channel");
    
    group.bench_function("bounded_create", |b| {
        b.iter(|| bounded::<i64>(black_box(100)))
    });
    
    group.bench_function("unbounded_create", |b| {
        b.iter(unbounded::<i64>)
    });
    
    group.bench_function("bounded_send_recv_1000", |b| {
        let (tx, rx) = bounded(1000);
        b.iter(|| {
            for i in 0..1000i64 {
                tx.send(i).unwrap();
            }
            for _ in 0..1000 {
                rx.recv().unwrap();
            }
        });
    });
    
    group.finish();
}

fn bench_bytes_operations(c: &mut Criterion) {
    use bytes::Bytes;
    
    let mut group = c.benchmark_group("bytes_operations");
    
    let data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();
    
    group.bench_function("bytes_from_vec", |b| {
        b.iter(|| Bytes::from(data.clone()))
    });
    
    group.bench_function("bytes_copy_from_slice", |b| {
        b.iter(|| Bytes::copy_from_slice(&data))
    });
    
    let bytes = Bytes::from(data.clone());
    group.bench_function("bytes_slice", |b| {
        b.iter(|| bytes.slice(black_box(100)..black_box(5000)))
    });
    
    group.bench_function("bytes_clone", |b| {
        b.iter(|| bytes.clone())
    });
    
    group.finish();
}

fn bench_file_io(c: &mut Criterion) {
    let mut group = c.benchmark_group("file_io");
    
    let dir = TempDir::new().unwrap();
    let small_path = dir.path().join("small.txt");
    let large_path = dir.path().join("large.txt");
    
    std::fs::write(&small_path, "Hello, World!").unwrap();
    std::fs::write(&large_path, "x".repeat(100_000)).unwrap();
    
    group.bench_function("read_small_file", |b| {
        b.iter(|| std::fs::read_to_string(&small_path).unwrap())
    });
    
    group.bench_function("read_large_file", |b| {
        b.iter(|| std::fs::read(&large_path).unwrap())
    });
    
    group.bench_function("file_metadata", |b| {
        b.iter(|| std::fs::metadata(&small_path).unwrap())
    });
    
    group.finish();
}

criterion_group!(
    benches,
    bench_csv_parsing,
    bench_uuid_generation,
    bench_json_serialization,
    bench_boxed_reader,
    bench_boxed_reader_seek,
    bench_storage_controller,
    bench_memory_stats,
    bench_pipe_counters,
    bench_parallel_strategy,
    bench_error_creation,
    bench_arrow_builders,
    bench_rayon_threadpool,
    bench_crossbeam_channel,
    bench_bytes_operations,
    bench_file_io,
);

criterion_main!(benches);
