# Single-Client KV Benchmark

`examples/kv_bench.rs` compares the single-threaded blop reference implementation with redb and
SQLite used as a key-value store. It is a small, reproducible baseline, not a general database
ranking. Production database code is unchanged; the comparison libraries are development
dependencies.

## Run

```sh
cargo build --release --example kv_bench
target/release/examples/kv_bench --help
target/release/examples/kv_bench --dir /path/to/existing/directory
```

On Linux, pin the process and its worker to one logical CPU:

```sh
taskset -c 0 target/release/examples/kv_bench --dir /path/to/existing/directory
```

Defaults are 1,000 keys, 100,000 reads and three measured trials of each mode. Allow several minutes
for the durable trials. Use `--mode buffered` or `--mode durable` to run them separately. Choose a
directory on the filesystem you want to measure: an OS temporary directory can be a RAM filesystem.
The harness creates unique temporary subdirectories and removes them on successful completion. It
does not overwrite existing databases. Forced termination can leave a `blop-kv-bench-*` directory.

The output is CSV with comment lines beginning with `#`. Each row measures a whole operation loop;
`mean_us` is elapsed wall time divided by operation count, not a latency percentile. Use the median
of the three trial rates for a ballpark figure. Increase counts for more stable measurements, but
note that blop retains historical versions, pages, outcomes, logs and manifests. Larger durable
trials can take substantially longer and use substantial disk space.

## Measurement Contract

- One client, one outstanding operation and one key per write transaction. No pipelining, batching,
  group commit, parallel scheduler or concurrent readers. The durable blop API has a current-thread
  Tokio executor and a dedicated serial writer, so it uses two OS threads even though database
  execution is single-threaded. CPU affinity restricts both to one logical CPU.
- Keys are 8-byte unsigned integers. Values are 128 bytes and contain the key plus a phase marker.
  redb and SQLite store big-endian key bytes; blop uses its typed `u64` schema and canonical storage
  encoding. blop values include their normal schema framing on disk.
- Each fresh database receives all keys in a deterministic shuffled order, followed by one overwrite
  of every key in a different shuffled order. All stores use unconditional upsert semantics. Key
  order and values are identical across engines and trials.
- Cached reads sample existing keys uniformly with replacement after the update phase. All final
  values are verified outside timing, which also warms the read working set. All read paths produce
  owned values, including copying redb's borrowed value. The additional blop `get_mvcc` diagnostic
  returns encoded value bytes and is verified against the expected encoding.
- Data generation, database/table creation, full-value verification, closing, reopening and cleanup
  are outside timing. Write timers include transaction setup and commit. blop timers include runtime
  `tx!` binding and VM validation; macro compilation happens at Rust build time. SQLite reuses
  prepared statements. Read timers reuse a pinned view/read transaction, not a transaction per get.
- A separate 64-key trial warms up each engine and mode and is discarded. Engine order rotates
  between the three measured trials. No OS cache eviction is attempted.
- redb and SQLite have 64 MiB configured page caches during measurement; SQLite mmap is disabled.
  These settings do not equalise total memory usage or the OS page cache. This is a small,
  memory-resident working-set comparison, not a memory-budget or cold-I/O benchmark.

### Buffered Mode

| Engine | Write Path                                                                                                                  | Cached Read Path                                                                          |
| ------ | --------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| blop   | `vm::execute` on an isolated reference store; installs MVCC versions and outcomes, without a log or checkpoint publication. | `vm::interpret` (`get_vm`), plus a separate `storage::mvcc::get` diagnostic (`get_mvcc`). |
| redb   | One write transaction per key, `Durability::None`.                                                                          | One pinned read transaction.                                                              |
| SQLite | WAL, `synchronous=OFF`, one autocommit UPSERT per key.                                                                      | One read transaction, prepared SELECT.                                                    |

**Buffered blop is not a recoverable database mode.** It has weaker guarantees than merely disabling
sync in a complete engine. These figures isolate execution/storage cost and must not be presented as
durable throughput. Unsynchronised writes can still perform file I/O and encounter OS writeback.

The read figures are also engine-level measurements: blop does not yet expose a public snapshot API.
Submitting a result-only transaction through `database::execute` would log and checkpoint it; the
reported cached-read rates do not describe that public API.

### Durable Mode

| Engine | Commit Setting                                                                                                                         |
| ------ | -------------------------------------------------------------------------------------------------------------------------------------- |
| blop   | `database::execute`, waiting for each durability receipt and checking its outcome; log publication followed by checkpoint publication. |
| redb   | One write transaction per key, `Durability::Immediate`.                                                                                |
| SQLite | WAL, `synchronous=FULL`, one autocommit UPSERT per key; default automatic WAL checkpointing remains enabled.                           |

All final values are verified after clean close/reopen. blop verification uses the persisted storage
checkpoint without submitting extra logged read transactions. This checks the harness and persisted
data, not power-loss behaviour. Durable commits have comparable intent but different work: blop also
retains transaction programs, outcomes and all historical versions, and publishes a checkpoint for
every write. These are not identical storage or retention policies.

## Results: 2026-09-07

Environment: AMD Ryzen AI 9 HX 370, process pinned to logical CPU 0, Linux 7.2.3-arch1-2, ext4 on
`/dev/nvme0n1p3`, NVMe model `MTFDKBA1T0QFM-1BD1AABGB`. Rust 1.98.0, standard Cargo release profile,
no added target-CPU flags. blop production sources at `02fcd43c4f341a83898dfb9bec9fb23afa51c506`;
redb 4.2.0; rusqlite 0.40.2 with bundled SQLite 3.53.2. This was a local workstation run, not an
isolated performance lab.

These commands completed, including verification and cleanup:

```sh
taskset -c 0 target/release/examples/kv_bench --dir /tmp/opencode --keys 10000 --reads 100000 --repeats 3 --mode buffered
taskset -c 0 target/release/examples/kv_bench --dir /tmp/opencode --keys 1000 --reads 100000 --repeats 3 --mode durable
```

### Buffered: 10,000 Keys

Median operations per second over three trials, rounded. Reads use 100,000 operations per trial.

| Operation         | blop Reference VM |      redb | SQLite KV |
| ----------------- | ----------------: | --------: | --------: |
| Random insert     |             6,012 |   136,777 |   230,359 |
| Random update     |             4,907 |   143,188 |   268,977 |
| Cached random get |            23,520 | 4,887,407 | 2,437,233 |

The separate blop MVCC-only read path reached **42,962 gets/s**, about 23.3 us/get versus 42.5
us/get through binding and the VM. It is a diagnostic, not an alternative public API. The buffered
write rate is about 23 to 55 times lower than these baselines, depending on engine and operation. VM
read rates are about 100 to 200 times lower on this small cached workload.

### Durable: 1,000 Keys

Median one-key transactions per second, with the observed minimum and maximum trial rate in
brackets.

| Operation     |              blop |                     redb |            SQLite KV |
| ------------- | ----------------: | -----------------------: | -------------------: |
| Random insert | 63.4 [60.8, 67.2] |   985.4 [915.0, 1,188.9] | 921.7 [814.3, 934.3] |
| Random update | 48.8 [47.0, 49.7] | 1,211.2 [916.4, 1,223.7] | 915.6 [857.5, 942.5] |

For blop, that is about **15.8 ms per insert** and **20.5 ms per update**, using the median trial
mean. The update phase has accumulated more history and should not be interpreted as a steady-state
update-only workload. The baselines are roughly 15 to 25 times faster here.

Per-trial CSV output is kept locally rather than checked in. Files matching `benchmarks/*.csv` are
ignored by Git; the summary results above are retained in the repository.

## What To Investigate

The measurements describe the current reference implementation, not a ceiling on the database
design. Code inspection identifies concrete profiling targets:

- `src/storage/page.rs`, `PageReader::read`: each page read allocates a 16 KiB buffer, performs
  positional I/O and validates the page. OS-cached data still takes this path. VM reads additionally
  load catalogue/policy information and decode/validate their program for each request.
- `src/storage/store.rs`, `publish`: every publication validates the checkpoint and committed log.
  `validate_checkpoint` walks the trees and entries; `src/storage/metadata.rs`, `validate_logs`,
  walks committed records. Publication also synchronises files and directory metadata.
- `src/database/engine.rs`, `commit`: each transaction performs two publications, before and after
  execution. That combines synchronisation overhead with validation work that grows with history.

These are source-level observations, not profiler attribution. Profile them before estimating a
speedup or changing validation/durability guarantees. No engine optimisations were made for this
run. Deletes, scans, mixed workloads, multi-key transactions, large values, cold reads, large
datasets, long-running reclamation, concurrency and tail latency remain unmeasured.

## Checks

The example's tests run under `cargo test --workspace` through its `test = true` manifest entry.
They check deterministic workload generation, option rejection, value preservation for all engines
in both modes, persisted blop checkpoints and successful temporary-directory cleanup. Run the
project checks with:

```sh
cargo test --workspace
cargo +nightly fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
```
