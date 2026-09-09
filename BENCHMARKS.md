# KV Benchmarks

`examples/kv_bench.rs` compares blop with redb and SQLite used as a key-value store. The durable
comparison exercises independent transactions through the public async API and cached reads through
public snapshots. A separate, single-client buffered mode measures the reference VM. This is a
small, reproducible workload comparison, not a general database ranking. The optimized results below
include engine scaling changes; the comparison libraries remain development dependencies. The latest
write optimisation results are in [Deferred Checkpoints](#deferred-checkpoints-2026-09-08).

## Run

```sh
cargo build --release --example kv_bench
target/release/examples/kv_bench --help
target/release/examples/kv_bench --dir /path/to/existing/directory --mode durable --clients 4 --workers 4
```

Defaults are 1,000 keys, 100,000 reads, three measured trials, one client and both modes.
`--workers` defaults to `EngineOptions::default().workers`. `--clients` and `--workers` accept 1
through 256; keys and reads must each be at least the client count. Multiple clients require
`--mode durable`: the isolated buffered blop store has no parallel scheduler.

On Linux, give every configuration the same CPU set, including single-client controls. Do not pin a
parallel scaling test to one logical CPU. For example, after checking the machine's CPU topology:

```sh
taskset -c 0-3 target/release/examples/kv_bench --dir /path/to/existing/directory --mode durable --clients 4 --workers 4
```

Allow several minutes for durable trials. Choose a directory on the filesystem you want to measure:
an OS temporary directory can be a RAM filesystem. The harness creates unique temporary
subdirectories and removes them on successful completion. It does not overwrite existing databases.
Forced termination can leave a `blop-kv-bench-*` directory.

Output is CSV with comment lines beginning with `#`. Each row measures the whole operation phase.
`ops_per_second` is aggregate completed operations divided by elapsed wall time; `amortized_us` is
its inverse in microseconds, **not mean request latency or a latency percentile**. The timer runs
from release of the client start barrier through the last completed client loop. Thread creation,
joining and connection/runtime destruction are outside timing. Barrier release overhead is included.

The `clients` column records concurrency. `blop_workers` records the configured interpreter count
for durable blop and is zero, meaning not applicable, for the other paths. For cross-engine durable
read analysis, compare blop's `get_snapshot` rows with redb and SQLite `get` rows. The names
distinguish the public snapshot path from the buffered `get_vm` and `get_mvcc` diagnostics.

Use the median of the three trial rates for a ballpark figure. Increase counts for more stable
measurements, but note that blop retains historical versions, pages, outcomes, logs and manifests.
Larger durable trials can take substantially longer and use substantial disk space.

## Measurement Contract

- Each client has one outstanding operation. Every write is an independently committed, one-key
  transaction against one shared database. There is no client-side batching or sharding into
  separate databases. redb and SQLite still serialize their write transactions internally.
- Keys are 8-byte unsigned integers. Values are 128 bytes and contain the key plus a phase marker.
  redb and SQLite store big-endian key bytes; blop uses its typed `u64` schema and canonical storage
  encoding. blop values include their normal schema framing on disk.
- Each fresh database receives all keys in a deterministic shuffled order, followed by one overwrite
  of every key in a different shuffled order. Each client takes every Nth element of that phase's
  order, where N is the client count. Every key is written exactly once per phase, including when
  the count is not divisible by N. Phase boundaries wait for every client. Global interleaving is
  nondeterministic; per-client input order and values are identical across engines and trials.
- All writes use unconditional upsert semantics. Keys are disjoint within each phase, so the blop
  scheduler can treat the transactions as independent. This is not a conflicting read-modify-write,
  hot-key, CPU-heavy transaction or mixed read/write workload.
- Durable blop clients each run a current-thread Tokio executor on a native client thread and await
  every receipt, checking for semantic aborts. The database has a separate coordinator and the
  selected persistent worker count. Other `EngineOptions` and transaction `Limits` retain their
  defaults: in particular, the execution window, assigned backlog and checkpoint interval are 64.
  More clients can encounter ordinary admission backpressure rather than increase active execution.
- Cached reads sample existing keys uniformly with replacement after updates and verification. All
  read paths produce owned values, including copying redb's borrowed value. Durable blop uses
  `database::get` on one shared pinned snapshot; redb uses a shared pinned read transaction/table.
  SQLite uses one pinned read transaction per client connection. Snapshot reads run on client
  threads, not blop's interpreter workers. There are no concurrent writes during the read phase.
- Data generation, database/table creation, opening client connections, runtime creation, full-value
  verification, closing, reopening and cleanup are outside timing. Write timers include transaction
  setup and commit. blop includes runtime `tx!` binding, admission and validation; macro compilation
  happens at Rust build time. SQLite prepares one write statement per client per phase inside
  timing, then reuses it. Its read timer retrieves an already warmed cached statement once per
  client. Statement setup matters more with very few operations per client.
- All final values are verified after clean close/reopen in durable mode. blop also checks the
  public snapshot sequence and persisted checkpoint, without logging verification reads. This checks
  the harness and persisted data, not power-loss behaviour. SQLite closes its client connections
  between phases; close-time WAL checkpoints are outside timing, while automatic checkpoints during
  writes remain inside timing.
- A separate trial with `max(64, clients)` keys and `max(128, clients)` reads warms each engine and
  mode and is discarded. Engine order rotates between measured trials. Full-value verification
  before reads warms the working set, including each SQLite connection. No OS cache eviction is
  attempted. Concurrency configurations are run separately, not interleaved with one another.
- redb has a 64 MiB page cache. SQLite has an approximately 64 MiB aggregate configured page cache,
  divided evenly among active client connections, with mmap disabled. SQLite uses a 60-second busy
  timeout rather than dropping writes on lock contention; lock waits are timed. These settings do
  not equalize actual memory usage or the OS page cache. This is a small, memory-resident
  working-set comparison, not a memory-budget or cold-I/O benchmark.
- The optimized blop engine has a 64 MiB retained decoded-node cache per page-file descriptor and a
  one-table cache per snapshot claim. Verification warms these caches before measured reads. Root
  proofs, a bounded 1 MiB above-checkpoint edit index and at most 4096 pending log digests avoid
  revalidating retained history on ordinary live publications. Recovery, handover and unsupported
  transitions retain full validation. These settings do not establish equal total memory use.

### Durable Mode

| Engine | Commit Setting                                                                                                                          | Cached Read Path                                         |
| ------ | --------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------- |
| blop   | `database::execute`, waiting for a successful durability receipt after log publication and contiguous visibility; periodic checkpoints. | `database::get` (`get_snapshot`), after public reopen.   |
| redb   | One write transaction per key, `Durability::Immediate`.                                                                                 | One pinned read transaction.                             |
| SQLite | WAL, `synchronous=FULL`, one autocommit UPSERT per key; automatic WAL checkpointing remains enabled.                                    | One read transaction and prepared SELECT per connection. |

Durable commits have comparable intent but different work. blop also retains transaction programs,
outcomes and historical versions. Local transaction log groups contain up to 64 already queued
requests, within existing budgets, with no artificial wait to fill a group. Checkpoints now run
after 64 newly visible records, rather than every visible-prefix advance. The original baseline had
no log group commit. The engines do not have identical storage or retention policies, and no
maintenance is requested here.

### Buffered Mode

| Engine | Write Path                                                                                                                | Cached Read Path                                                                          |
| ------ | ------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| blop   | `vm::execute` on an isolated reference store; installs MVCC versions and outcomes, without log or checkpoint publication. | `vm::interpret` (`get_vm`), plus a separate `storage::mvcc::get` diagnostic (`get_mvcc`). |
| redb   | One write transaction per key, `Durability::None`.                                                                        | One pinned read transaction.                                                              |
| SQLite | WAL, `synchronous=OFF`, one autocommit UPSERT per key.                                                                    | One read transaction, prepared SELECT.                                                    |

**Buffered blop is not a recoverable database mode.** It has weaker guarantees than disabling sync
in a complete engine. These figures isolate execution/storage cost and must not be presented as
durable or parallel-engine throughput. Unsynchronized writes can still perform file I/O and
encounter OS writeback. The VM and MVCC read diagnostics are not measurements of the public snapshot
API; the MVCC diagnostic returns encoded bytes, verified against the expected encoding.

## Deferred Checkpoints: 2026-09-08

This follow-up compares production revision `838a303` with the deferred-checkpoint changes. It uses
the same, unchanged harness, release profile, CPU set 0 through 3 and filesystem as the earlier
runs. Each configuration has 1,000 independent inserts, 1,000 independent updates and 100,000 cached
reads per fresh database, with three measured trials and discarded warm-up. Configurations and
builds/tests ran separately, not concurrently with measured workloads.

The default checkpoint interval is now 64 visible records. Every receipt still follows durable
canonical logging, complete installation and contiguous visibility, but no longer promises that
materialized pages have been checkpointed. Set `EngineOptions::checkpoint_interval` to 1 for the
former checkpoint-before-receipt behaviour. The on-disk format, durability-before-execution rule and
manifest/CURRENT ordering are unchanged.

The benchmark still verifies full values and the final checkpoint after close/reopen. Periodic
checkpoints are inside the write timers. As before, close is outside timing; it now publishes the
remaining partial interval. The measurements are durable receipt throughput, not throughput for
checkpointing every transaction, and not request-latency percentiles.

Run each command on `838a303`, then rebuild and run on the changed sources:

```sh
cargo build --release --example kv_bench
taskset -c 0-3 target/release/examples/kv_bench --dir /tmp/opencode --keys 1000 --reads 100000 --repeats 3 --mode durable --clients 1 --workers 1
taskset -c 0-3 target/release/examples/kv_bench --dir /tmp/opencode --keys 1000 --reads 100000 --repeats 3 --mode durable --clients 16 --workers 4
taskset -c 0-3 target/release/examples/kv_bench --dir /tmp/opencode --keys 1000 --reads 100000 --repeats 3 --mode durable --clients 4 --workers 1
```

Median transactions per second:

| Clients | Workers | Operation | Before |  After | Speedup |
| ------: | ------: | --------- | -----: | -----: | ------: |
|       1 |       1 | Insert    |   93.0 |  181.2 |   1.95x |
|       1 |       1 | Update    |   92.4 |  181.1 |   1.96x |
|       4 |       1 | Insert    |   93.5 |  188.7 |   2.02x |
|       4 |       1 | Update    |   94.0 |  183.8 |   1.96x |
|      16 |       4 | Insert    |  329.7 | 1011.1 |   3.07x |
|      16 |       4 | Update    |  333.1 |  952.3 |   2.86x |

Per-trial rates, in trial order, preserve the variability behind those medians:

| Clients / Workers | Before Inserts      | After Inserts          | Before Updates      | After Updates       |
| ----------------- | ------------------- | ---------------------- | ------------------- | ------------------- |
| 1 / 1             | 53.0, 93.4, 93.0    | 181.2, 180.5, 181.2    | 92.1, 92.6, 92.4    | 181.3, 181.1, 179.3 |
| 4 / 1             | 93.5, 94.8, 93.0    | 189.0, 185.3, 188.7    | 94.6, 94.0, 93.3    | 186.0, 176.1, 183.8 |
| 16 / 4            | 329.7, 305.5, 342.1 | 1026.0, 1011.1, 1005.2 | 333.1, 316.3, 337.0 | 925.4, 952.3, 972.0 |

The first single-client baseline insert trial was substantially slower; it is retained rather than
discarded. These are workstation measurements, not isolated per-optimisation attribution. At 16
clients, the after-run comparison-engine medians were 1,231.2 inserts/s and 1,260.1 updates/s for
redb, and 626.4 inserts/s and 626.2 updates/s for SQLite. The result narrows the gap to redb on this
workload; it is not a general database ranking.

An ordinary single-client transaction on an existing log now requests five file/directory sync calls
for its log publication, instead of ten across log and checkpoint publications. A periodic
checkpoint adds another five calls. Runtime log-only publication keeps the selected roots' existing
page count, so reconstructible appended pages do not trigger a page flush. These are call counts,
not necessarily device-flush counts. New files and administrative operations add work. Removing the
completion publication also lets concurrent clients proceed without that coordinator I/O pause.

The trade-off is more replay work after interruption and a more conservative log-retention floor
until C advances. The interval bounds record count, not bytes, execution time or elapsed checkpoint
age. Large transactions, cold data, mixed workloads and tail latency remain unmeasured here.

A separate larger verification run also completed with full-value reopen checks: 10,000 inserts,
10,000 updates and one million cached reads, using 16 clients and four workers. blop reached 911.2
inserts/s and 801.3 updates/s. This was one measured trial, not a median or a matched before/after
comparison:

```sh
taskset -c 0-3 target/release/examples/kv_bench --dir /tmp/opencode --keys 10000 --reads 1000000 --repeats 1 --mode durable --clients 16 --workers 4
```

## Optimized Results: 2026-09-08

The benchmark baseline was committed as `5807d73` before engine changes. The following runs use that
unchanged harness with live caching, incremental validation, bounded checkpoint filtering,
opportunistic group commit, completion coalescing and omission of already-satisfied data flushes.
The on-disk format, durability receipt semantics and retained-history policy are unchanged.

The CPU set, filesystem, release profile and library versions are the same as the baseline
environment below. Configurations ran sequentially in the listed order, without concurrent builds or
tests. All trials completed with successful full-value reopen verification and temporary-directory
cleanup.

```sh
cargo build --release --example kv_bench
taskset -c 0-3 target/release/examples/kv_bench --dir /tmp/opencode --keys 1000 --reads 100000 --repeats 3 --mode durable --clients 1 --workers 1 > benchmarks/2026-09-08-optimized-c1-w1.csv
taskset -c 0-3 target/release/examples/kv_bench --dir /tmp/opencode --keys 1000 --reads 100000 --repeats 3 --mode durable --clients 4 --workers 4 > benchmarks/2026-09-08-optimized-c4-w4.csv
taskset -c 0-3 target/release/examples/kv_bench --dir /tmp/opencode --keys 1000 --reads 100000 --repeats 3 --mode durable --clients 4 --workers 1 > benchmarks/2026-09-08-optimized-c4-w1.csv
taskset -c 0-3 target/release/examples/kv_bench --dir /tmp/opencode --keys 1000 --reads 100000 --repeats 3 --mode durable --clients 16 --workers 4 > benchmarks/2026-09-08-optimized-c16-w4.csv
taskset -c 0-3 target/release/examples/kv_bench --dir /tmp/opencode --keys 10000 --reads 1000000 --repeats 3 --mode durable --clients 16 --workers 4 > benchmarks/2026-09-08-optimized-10k-c16-w4.csv
```

### Durable Writes

Median transactions per second over three fresh databases, with 1,000 transactions per phase:

| Clients | blop Workers | Operation |  blop |    redb | SQLite KV |
| ------: | -----------: | --------- | ----: | ------: | --------: |
|       1 |            1 | Insert    |  93.0 | 1,204.3 |     949.1 |
|       1 |            1 | Update    |  90.7 | 1,266.4 |     956.5 |
|       4 |            4 | Insert    | 217.0 | 1,210.2 |     781.1 |
|       4 |            4 | Update    | 208.9 | 1,255.4 |     770.1 |
|       4 |            1 | Insert    |  86.7 | 1,208.4 |     827.6 |
|       4 |            1 | Update    |  90.0 | 1,253.9 |     769.7 |
|      16 |            4 | Insert    | 312.6 | 1,208.2 |     619.0 |
|      16 |            4 | Update    | 315.8 | 1,213.4 |     556.7 |

At four clients/four workers, inserts improved **3.25 times** and updates **4.09 times** relative to
the matching baseline. At 16 clients/four workers, the gains were **4.69 times** and **6.23 times**.
Single-client writes improved less, because one outstanding transaction cannot share publication
cost with other transactions.

The four-client/one-worker control reached only 86.7 inserts/s and 90.0 updates/s. Multiple workers
now help this workload, including through completion coalescing; the result is not a measurement of
interpreter CPU scaling alone. The implementation groups already queued work without delaying it, so
achieved group size also depends on arrivals, completion timing and barriers.

Observed blop trial ranges:

| Clients | Workers |   Insert Range |   Update Range |
| ------: | ------: | -------------: | -------------: |
|       1 |       1 |   91.4 to 93.4 |   89.1 to 91.5 |
|       4 |       4 | 210.9 to 217.2 | 207.6 to 218.3 |
|       4 |       1 |   86.3 to 90.2 |   85.7 to 90.9 |
|      16 |       4 | 310.8 to 315.4 | 315.2 to 328.3 |

### Cached Snapshot Reads

Median aggregate gets/s, rounded, with 100,000 operations per trial:

| Clients | blop Workers | blop Public Snapshot |       redb | SQLite KV |
| ------: | -----------: | -------------------: | ---------: | --------: |
|       1 |            1 |            1,849,423 |  7,627,372 | 3,441,343 |
|       4 |            4 |            4,194,453 | 22,904,103 | 2,287,543 |
|       4 |            1 |            4,901,144 | 21,046,566 | 2,283,660 |
|      16 |            4 |            4,710,855 | 20,325,736 | 2,211,589 |

Matching blop baseline gains are about **52 times** at one client, **33 times** at four clients/four
workers, and **36 times** at 16 clients. These reads do not use the interpreter pool. Variation
between worker-count configurations is not evidence of workers accelerating snapshot reads.

The four-client blop trials ranged from 4.12 to 4.82 million gets/s; 16-client trials ranged from
4.01 to 5.15 million. Short read phases and workstation noise still limit precision, especially for
redb. The comparison engines' read rates also changed between baseline and rerun, so the gains are
not an isolated attribution to individual optimizations. The larger read run below lasts longer.

### Larger History

At 10,000 keys, with 10,000 inserts, 10,000 updates and one million cached reads per trial, using 16
clients and four blop workers:

| Operation         |      blop |       redb | SQLite KV |
| ----------------- | --------: | ---------: | --------: |
| Durable inserts/s |     308.0 |    1,218.1 |     846.2 |
| Durable updates/s |     286.2 |    1,255.6 |     842.4 |
| Cached reads/s    | 4,497,095 | 13,995,627 | 1,751,151 |

blop inserts ranged from 307.1 to 308.8 transactions/s and updates from 285.4 to 289.3. Increasing
the number of keys and writes tenfold took about ten times as long for inserts and eleven times for
updates, rather than multiplying per-transaction validation cost by retained history. This supports
the intended improvement over this tested size range; it is not an asymptotic proof.

### Remaining Costs

blop still trails redb on this workload. At 16 clients and 1,000 keys, durable writes are about 3.8
times slower and cached reads about 4.3 times slower. SQLite durable writes remain faster, while
blop cached reads are faster in this configuration.

The synchronous manifest/CURRENT protocol remains substantial: the ordinary single-client path on an
existing log requests ten file/directory synchronization calls across its two publications, instead
of fourteen. New files require additional directory synchronization. These counts describe calls,
not necessarily physical device flushes. Grouping amortizes publication but does not remove its
ordering requirements or the single coordinator. COW page writing, retained outcomes/history, VM
preparation, cache locking and typed value handling also remain.

Caches and proofs are bounded. Exceeding the edit or digest cache bounds can reinstate full scans,
and many retained segments still require descriptor processing. Full recovery and maintenance can
still scale with retained history. Cold reads, larger-than-cache data, large transactions, tail
latency and per-optimization CPU/I/O attribution remain unmeasured.

## Baseline Results: 2026-09-08

Environment: AMD Ryzen AI 9 HX 370, all configurations restricted to logical CPUs 0 through 3, which
are four distinct high-frequency physical cores on this machine. Linux 7.2.3-arch1-2, ext4 on
`/dev/nvme0n1p3`, NVMe model `MTFDKBA1T0QFM-1BD1AABGB`. Rust 1.98.0, standard Cargo release profile,
no added target-CPU flags. blop production sources at `4040dce4fd776e5423e1ca0f24982056a8a5297e`,
with the concurrent harness in this change; redb 4.2.0; rusqlite 0.40.2 with bundled SQLite 3.53.2.
This was a local workstation run, not an isolated performance lab.

These commands completed in the listed order, including verification and cleanup. Do not run them in
parallel with one another or with builds/tests when reproducing measurements. On a fresh checkout,
create the output directory first with `mkdir -p benchmarks`:

```sh
cargo build --release --example kv_bench
taskset -c 0-3 target/release/examples/kv_bench --dir /tmp/opencode --keys 1000 --reads 100000 --repeats 3 --mode durable --clients 1 --workers 1 > benchmarks/2026-09-08-durable-c1-w1.csv
taskset -c 0-3 target/release/examples/kv_bench --dir /tmp/opencode --keys 1000 --reads 100000 --repeats 3 --mode durable --clients 4 --workers 4 > benchmarks/2026-09-08-durable-c4-w4.csv
taskset -c 0-3 target/release/examples/kv_bench --dir /tmp/opencode --keys 1000 --reads 100000 --repeats 3 --mode durable --clients 4 --workers 1 > benchmarks/2026-09-08-durable-c4-w1.csv
taskset -c 0-3 target/release/examples/kv_bench --dir /tmp/opencode --keys 1000 --reads 100000 --repeats 3 --mode durable --clients 16 --workers 4 > benchmarks/2026-09-08-durable-c16-w4.csv
```

### Durable Writes

Median transactions per second over three fresh-database trials, with 1,000 transactions per phase.
Worker count applies only to blop; redb and SQLite were rerun in the one-worker control for
comparison.

| Clients | blop Workers | Operation | blop |    redb | SQLite KV |
| ------: | -----------: | --------- | ---: | ------: | --------: |
|       1 |            1 | Insert    | 66.4 | 1,235.0 |     969.0 |
|       1 |            1 | Update    | 50.8 | 1,250.4 |     957.6 |
|       4 |            4 | Insert    | 66.8 | 1,239.0 |     840.2 |
|       4 |            4 | Update    | 51.1 | 1,260.2 |     841.0 |
|       4 |            1 | Insert    | 66.1 | 1,240.6 |     809.0 |
|       4 |            1 | Update    | 50.6 | 1,275.0 |     839.9 |
|      16 |            4 | Insert    | 66.6 | 1,237.0 |     627.3 |
|      16 |            4 | Update    | 50.7 | 1,265.2 |     626.0 |

Observed minimum and maximum blop trial rates:

| Clients | Workers | Insert Range | Update Range |
| ------: | ------: | -----------: | -----------: |
|       1 |       1 | 63.5 to 66.8 | 50.8 to 51.6 |
|       4 |       4 | 65.8 to 67.1 | 50.6 to 53.9 |
|       4 |       1 | 65.4 to 66.3 | 50.5 to 51.3 |
|      16 |       4 | 64.7 to 66.8 | 50.7 to 51.1 |

Independent clients and additional workers did **not materially increase durable one-key write
throughput** in this run. At four clients, switching from one to four workers changed the median by
about 1%, within observed trial variation. redb was about 19 to 25 times faster than blop, and
SQLite about 13 to 16 times faster, at four clients/four blop workers. SQLite throughput fell with
more competing writer connections; redb was approximately flat.

The single-client blop medians correspond to about 15.1 ms per insert and 19.7 ms per update. Do not
apply those numbers to concurrent request latency. The update phase has accumulated more history and
is not a steady-state update-only workload.

### Cached Snapshot Reads

Median aggregate gets per second, rounded, using 100,000 reads per trial after durable writes,
reopen and full-value verification:

| Clients | blop Workers | blop Public Snapshot |       redb | SQLite KV |
| ------: | -----------: | -------------------: | ---------: | --------: |
|       1 |            1 |               35,487 |  7,861,444 | 2,677,800 |
|       4 |            4 |              128,645 | 16,661,307 | 1,641,080 |
|       4 |            1 |              129,556 | 12,705,008 | 1,654,157 |
|      16 |            4 |              130,437 | 13,294,957 | 1,622,484 |

blop's public snapshot reads scaled by about **3.6 times** from one to four clients on four cores,
then stayed approximately flat at 16 clients. The one-worker control confirms that this path uses
client threads rather than the transaction worker pool. At four clients, blop remained about 130
times slower than redb and 13 times slower than SQLite on this cached workload.

These are coarse throughput measurements, not precise microbenchmark estimates. The single-client
blop trials ranged from 35,354 to 46,964 gets/s, and the four-client/four-worker trials from 126,967
to 130,310 gets/s. redb's four-client read loops lasted only 5.6 to 8.1 ms and ranged from 12.4 to
18.0 million gets/s. Its different four-client results in the one-worker control are run-to-run
variation, not an effect of blop worker settings on redb. Longer read phases and more trials are
needed for fine comparisons. SQLite read scaling was negative in this connection/configuration
setup; no profiler attribution is claimed.

Per-trial CSV output is kept locally rather than checked in. Files matching `benchmarks/*.csv` are
ignored by Git; the summary results above are retained in the repository. Only durable mode was
remeasured for this update. The historical buffered figures below describe older production sources.

## What To Investigate

The flat write result is consistent with substantial serialized publication work, not evidence that
independent transaction execution is incorrect or never overlaps. Source inspection identifies these
profiling targets in the measured production revision:

- `src/database/scheduler.rs`, `register`, calls `src/database/engine.rs`, `append`, which publishes
  each log record before dispatch. The coordinator serializes this work even when worker execution
  is independent.
- `src/database/scheduler.rs`, `complete`, installs a completed outcome, advances the contiguous
  visible prefix, publishes its checkpoint, then releases receipts. One checkpoint can cover several
  completed records, but publication is still on the coordinator. If visibility is below durability,
  `src/storage/store.rs`, `prepare_checkpoint`, also scans and filters post-checkpoint versions.
- `src/storage/store.rs`, `publish`, validates checkpoint trees and committed logs on publication.
  `validate_checkpoint` walks trees and entries; `src/storage/metadata.rs`, `validate_logs`, walks
  retained records and verifies their bodies. This work grows with retained history.
- `src/storage/store.rs`, `publish_files`, synchronizes page/log files and publishes new manifest
  and CURRENT files with file and directory synchronization. More interpreter workers do not
  parallelize these durability operations.
- `src/storage/page.rs`, `PageReader::read`, allocates a 16 KiB buffer, performs positional I/O and
  validates the page even for OS-cached data. Public snapshot reads also resolve historical
  catalogue information and decode typed values.

These are source-level observations, not profiler attribution. Profile them before estimating a
speedup or changing validation/durability guarantees. No engine optimizations were made for this
run. Deletes, scans, mixed workloads, conflicting transactions, CPU-heavy independent programs,
multi-key transactions, large values, cold reads, large datasets, reclamation and tail latency
remain unmeasured. This small KV test cannot establish the worker pool's scaling on compute-heavy
programs.

## Historical Results: 2026-09-07

The earlier single-client harness used production revision
`02fcd43c4f341a83898dfb9bec9fb23afa51c506`, with the same machine, toolchain and comparison-library
versions, but affinity restricted to logical CPU 0. These are not an isolated before/after test of
the scheduler: the engine, harness, affinity and read APIs have changed.

```sh
taskset -c 0 target/release/examples/kv_bench --dir /tmp/opencode --keys 10000 --reads 100000 --repeats 3 --mode buffered
taskset -c 0 target/release/examples/kv_bench --dir /tmp/opencode --keys 1000 --reads 100000 --repeats 3 --mode durable
```

Buffered medians in operations per second, with 10,000 keys and 100,000 reads:

| Operation         | blop Reference VM |      redb | SQLite KV |
| ----------------- | ----------------: | --------: | --------: |
| Random insert     |             6,012 |   136,777 |   230,359 |
| Random update     |             4,907 |   143,188 |   268,977 |
| Cached random get |            23,520 | 4,887,407 | 2,437,233 |

The separate blop MVCC-only diagnostic reached 42,962 gets/s, about 23.3 us/get versus 42.5 us/get
through binding and the VM. Neither read path was a public snapshot API measurement.

Durable medians in one-key transactions per second, with 1,000 keys and the observed minimum and
maximum trial rates in brackets:

| Operation     |              blop |                     redb |            SQLite KV |
| ------------- | ----------------: | -----------------------: | -------------------: |
| Random insert | 63.4 [60.8, 67.2] |   985.4 [915.0, 1,188.9] | 921.7 [814.3, 934.3] |
| Random update | 48.8 [47.0, 49.7] | 1,211.2 [916.4, 1,223.7] | 915.6 [857.5, 942.5] |

## Checks

The example's tests run under `cargo test --workspace` through its `test = true` manifest entry.
They check deterministic workload generation, option rejection, uneven client partitioning, error
propagation, value preservation across serial and concurrent engines, public snapshot sequences,
persisted blop checkpoints and successful temporary-directory cleanup. Project checks passed:

Engine regression tests also cover grouped durability before dispatch, checkpoint coalescing,
admission limits and barriers, interrupted group replay, cache bounds and pinned-prefix rejection,
snapshot revocation, incremental log anchors, invalidation of root proofs, exact fast/full
checkpoint contents, and every I/O boundary in the optimized runtime publication path. Full
validation and reopen still reject corrupted disk pages even after the runtime cache has been
warmed.

```sh
cargo test --workspace
cargo +nightly fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
```
