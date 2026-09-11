# Key-value benchmarks

Use `examples/kv_bench.rs` to compare blop-db, redb, and SQLite on independent one-key writes and
cached reads. This report explains how to run the workload and interpret its results. It is for
developers evaluating performance changes, not for selecting a database from a general ranking.

The latest recorded change, version 1 write-ahead log (WAL) publication, increased blop-db's median
single-client insert rate from 184.0 to 807.2 operations/s on the test workstation. Cached-read
rates were approximately unchanged. See the [WAL results](#version-1-wal-publication) for
conditions, individual trials, and limits on this comparison.

The durable workload uses the public asynchronous write API and public snapshots. A separate
buffered mode measures the single-threaded reference virtual machine (VM). redb and SQLite are
development dependencies used for comparison.

## Find a measurement

- [Run the benchmark](#run-the-benchmark) and [read its output](#read-the-output).
- [Understand the workload](#measurement-contract), [durable mode](#durable-mode), and
  [buffered mode](#buffered-mode).
- [Version 1 WAL publication](#version-1-wal-publication): latest write results.
- [Single-client follow-up](#single-client-follow-up): reduced point-read copying.
- [Point reads and worker dispatch](#point-reads-and-worker-dispatch-2026-09-09).
- [Deferred checkpoints](#deferred-checkpoints-2026-09-08).
- [Initial optimization results](#optimized-results-2026-09-08).
- [Baseline and environment](#baseline-results-2026-09-08).
- [Earlier single-client results](#historical-results-2026-09-07) and [checks](#checks).

Each result section describes the implementation measured at that time. Earlier flush counts and
performance limits are historical. Do not apply them to the current WAL implementation.

## Run the benchmark

Run commands from the repository root with a Rust toolchain and Cargo. First choose an existing
parent directory on the filesystem you want to measure. The operating system's temporary directory
may be a RAM filesystem and therefore measure different storage behaviour.

The harness creates unique temporary databases and removes them on success. It does not overwrite
existing databases. Forced termination may leave a `blop-kv-bench-*` directory. Allow several
minutes and sufficient disk space for durable trials.

```sh
cargo build --release --example kv_bench
target/release/examples/kv_bench --help
target/release/examples/kv_bench --dir /path/to/existing/directory --mode durable --clients 4 --workers 4
```

The default run uses 1,000 keys, 100,000 reads, three measured trials, one client, and both modes.
`--workers` uses `EngineOptions::default().workers` unless specified.

Both `--clients` and `--workers` accept 1 through 256. Key and read counts must each be at least the
client count. For multiple clients, select `--mode durable`; the buffered reference store has no
parallel scheduler.

On Linux, give every configuration the same CPU set, including single-client controls. Do not pin a
parallel scaling test to one logical CPU. For example, after checking the machine's CPU topology:

```sh
taskset -c 0-3 target/release/examples/kv_bench --dir /path/to/existing/directory --mode durable --clients 4 --workers 4
```

Run configurations one at a time, separately from builds and tests. Use the same CPU set,
filesystem, toolchain, profile, and options for both revisions in a comparison. If you redirect
output to `benchmarks/`, create that directory first with `mkdir -p benchmarks`.

## Read the output

Output uses comma-separated values (CSV), with comment lines beginning with `#`. Each data row
measures one complete operation phase.

| Column           | Meaning                                                                              |
| ---------------- | ------------------------------------------------------------------------------------ |
| `ops_per_second` | Total completed operations divided by elapsed wall time.                             |
| `amortized_us`   | The inverse aggregate throughput, expressed in microseconds per operation.           |
| `clients`        | Number of concurrent clients, each with one outstanding operation.                   |
| `blop_workers`   | Interpreter count for durable blop-db; zero means not applicable to the other paths. |

**`amortized_us` is not mean request latency or a latency percentile.** Timing starts when the
client start barrier is released and ends when the last client loop completes. It includes
barrier-release overhead, but excludes thread creation, joining, and destruction of connections and
runtimes.

For durable read comparisons, use blop-db's `get_snapshot` rows and redb's and SQLite's `get` rows.
The buffered `get_vm` and `get_mvcc` rows measure different, lower-level paths. Multi-version
concurrency control (MVCC) means retaining sequence-tagged values; the
[README glossary](README.md#terms-used-in-this-guide) defines this and other engine terms.

Use the median of the three trial rates for an initial estimate, and inspect their range. Larger
counts can improve stability, but blop-db retains versions, pages, outcomes, logs, and manifests.
Larger durable trials can take much longer and use much more disk space.

## Measurement contract

The tables below are comparable only under these workload and timing conditions. In result tables,
"blop" means blop-db and "SQLite KV" means SQLite used as a key-value store.

### Writes and client ordering

- Each client has one outstanding operation. Every write is an independently committed, one-key
  transaction against one shared database. There is no client-side batching or sharding into
  separate databases. redb and SQLite still serialize their write transactions internally.
- Keys are 8-byte unsigned integers. Values are 128 bytes and contain the key plus a phase marker.
  redb and SQLite store big-endian key bytes; blop uses its typed `u64` schema and canonical storage
  encoding. blop values include their normal schema framing on disk.
- Insert every key into a fresh database in a reproducible shuffled order. Then overwrite every key
  in a different shuffled order. With N clients, each client takes every Nth element of that phase's
  order. Each key is written once per phase, even when the key count is not divisible by N. Wait for
  all clients before the next phase. Global interleaving may differ, but each client's input order
  and values are identical across engines and trials.
- All writes insert or replace a value unconditionally. Keys are disjoint within each phase, so the
  blop scheduler can treat the transactions as independent. This is not a conflicting
  read-modify-write, hot-key, CPU-heavy transaction or mixed read/write workload.
- Durable blop clients each run a current-thread Tokio executor on a native client thread and await
  every receipt, checking for semantic aborts. The database has a separate coordinator and the
  selected persistent worker count. Other `EngineOptions` and transaction `Limits` retain their
  defaults: in particular, the execution window, assigned backlog and checkpoint interval are 64.
  More clients can encounter ordinary admission backpressure rather than increase active execution.

### Read paths and timing

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

### Warm-up and memory settings

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

### Durable mode

| Engine | Commit Setting                                                                                                                          | Cached Read Path                                         |
| ------ | --------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------- |
| blop   | `database::execute`, waiting for a successful durability receipt after log publication and contiguous visibility; periodic checkpoints. | `database::get` (`get_snapshot`), after public reopen.   |
| redb   | One write transaction per key, `Durability::Immediate`.                                                                                 | One pinned read transaction.                             |
| SQLite | WAL, `synchronous=FULL`, one autocommit UPSERT per key; automatic WAL checkpointing remains enabled.                                    | One read transaction and prepared SELECT per connection. |

All durable paths wait for a durable commit, but they perform different work. blop-db also retains
programs, outcomes, and historical versions. Current local log groups hold up to 64 already queued
requests within the configured budgets, without waiting to fill a group. Checkpoints run after 64
newly visible records.

Earlier result sections identify their checkpoint and grouping behaviour; the original baseline did
not group log commits. The engines have different storage and retention policies. This workload does
not request maintenance.

### Buffered mode

| Engine | Write Path                                                                                                                | Cached Read Path                                                                          |
| ------ | ------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| blop   | `vm::execute` on an isolated reference store; installs MVCC versions and outcomes, without log or checkpoint publication. | `vm::interpret` (`get_vm`), plus a separate `storage::mvcc::get` diagnostic (`get_mvcc`). |
| redb   | One write transaction per key, `Durability::None`.                                                                        | One pinned read transaction.                                                              |
| SQLite | WAL, `synchronous=OFF`, one autocommit UPSERT per key.                                                                    | One read transaction, prepared SELECT.                                                    |

**Buffered blop is not recoverable.** It omits more work than merely disabling synchronization in a
complete engine. Use its figures only to study reference execution and storage costs, not durable or
parallel-engine throughput.

Unsynchronized writes can still perform file I/O and trigger operating-system writeback. Neither the
VM nor the MVCC diagnostic measures public snapshot reads. The MVCC path returns encoded bytes,
which the harness checks against the expected encoding.

## Version-1 WAL publication

This compares baseline `243b001` with WAL publication commit `87afb51`. The baseline already
includes all point-read and dispatch improvements documented below. Both revisions use the unchanged
harness, standard release profile, CPU set 0 through 3, filesystem and comparison libraries. Each
fresh database has 1,000 inserts, 1,000 updates and five million cached reads, with three measured
trials and discarded warm-up. Builds, tests and measured workloads ran separately.

The storage format uses version-1 headers and self-committing WAL groups. An ordinary append to an
existing log segment requests **one file flush instead of five file/directory flushes**. New segment
creation adds a directory flush. Checkpoints retain the existing publication ordering and default
64-record interval. Receipts still require complete installation and contiguous visibility after
durable logging. The final partial checkpoint remains in close, outside timing. Group framing adds
168 bytes per group, without changing canonical transaction or outcome bytes.

Run these commands on `243b001`, then rebuild and repeat on `87afb51`. The commands use the existing
`/tmp/opencode` parent directory from the recorded environment; substitute your measurement
directory when reproducing them elsewhere.

```sh
cargo build --release --example kv_bench
taskset -c 0-3 target/release/examples/kv_bench --dir /tmp/opencode --keys 1000 --reads 5000000 --repeats 3 --mode durable --clients 1 --workers 1
taskset -c 0-3 target/release/examples/kv_bench --dir /tmp/opencode --keys 1000 --reads 5000000 --repeats 3 --mode durable --clients 16 --workers 4
```

### Median and individual trials

Median blop operations/s, with read rates rounded:

| Clients | Workers | Operation    |    Before |     After | After / Before |
| ------: | ------: | ------------ | --------: | --------: | -------------: |
|       1 |       1 | Insert       |     184.0 |     807.2 |          4.39x |
|       1 |       1 | Update       |     181.3 |     786.4 |          4.34x |
|       1 |       1 | Snapshot get | 4,715,682 | 4,700,292 |          1.00x |
|      16 |       4 | Insert       |    1070.8 |    3063.3 |          2.86x |
|      16 |       4 | Update       |    1017.1 |    2789.1 |          2.74x |
|      16 |       4 | Snapshot get | 7,915,277 | 7,900,016 |          1.00x |

Per-trial write rates, in trial order:

| Clients / Workers | Operation | Before                 | After                  |
| ----------------- | --------- | ---------------------- | ---------------------- |
| 1 / 1             | Insert    | 184.4, 183.3, 184.0    | 807.2, 806.7, 807.2    |
| 1 / 1             | Update    | 181.1, 182.3, 181.3    | 798.1, 786.4, 786.0    |
| 16 / 4            | Insert    | 1046.3, 1072.4, 1070.8 | 3114.6, 3000.7, 3063.3 |
| 16 / 4            | Update    | 930.6, 1017.1, 1020.0  | 2928.9, 2680.6, 2789.1 |

### Interpretation and verification

Cached-read throughput changed little. Single-client reads ranged from 4.706 to 4.765 million/s
before and 4.636 to 4.744 million/s after; 16-client reads ranged from 7.846 to 8.030 million/s
before and 7.721 to 8.016 million/s after. These results are workstation throughput measurements for
independent one-key transactions, not latency percentiles or a general database ranking.

Every measured trial completed full-value and checkpoint verification after reopen. Separate tests
assert the actual flush count, recovery of complete groups after failed flushes, rejection of
complete damaged groups, every physically short group prefix, native process exit at append
boundaries, strict version and group-bound checks, and checkpoint/backup/replication behaviour. See
[DESIGN.md, appendix I](DESIGN.md#appendix-i-wal-commit-groups) for the tail-recovery contract.

A larger verification run also completed with 10,000 inserts, 10,000 updates and five million cached
reads, using 16 clients and four workers. It reached 2,527.6 inserts/s, 2,061.1 updates/s and
7,134,243 snapshot gets/s, with successful reopen verification. This was one trial, not a matched
larger-dataset speedup comparison:

```sh
taskset -c 0-3 target/release/examples/kv_bench --dir /tmp/opencode --keys 10000 --reads 5000000 --repeats 1 --mode durable --clients 16 --workers 4
```

## Single-client follow-up

This historical comparison measured an additional single-client read optimization after the
point-read and worker-dispatch changes below. Those changes were not yet committed when the baseline
binary was saved. Both stages were later included in `243b001`; the saved baseline is not `9676f05`.
The two binaries used fresh databases with the same harness and options.

The follow-up changed `src/database/snapshot.rs` and `src/storage/mvcc.rs` as follows:

- Cache the decoded key schema alongside the last resolved historical table.
- Borrow cached table metadata during a point read instead of cloning its `Arc` on each hit.
  Replacing that cache entry waits for in-flight cached readers; the cache still retains one table.
- Encode primitive input values from stack bytes through the canonical key codec.
- Identify the selected MVCC address by its exact escaped prefix, avoiding an allocated upper bound.
- Validate a Put once and retain its immutable leaf or owned overflow buffer. Typed snapshot reads
  decode that payload directly, avoiding the intermediate owned payload copy. Returned values still
  own their contents.

The same CPU set, filesystem, release profile and comparison libraries were used as below. Each
trial contains 1,000 independent inserts, 1,000 updates and five million cached reads, with one
client, one worker, discarded warm-up and three measured trials. Builds and tests ran separately.
Full-value and checkpoint verification after reopen succeeded in every trial.

```sh
# Before changing the sources, retain a separate baseline build:
cargo build --release --example kv_bench --target-dir /tmp/opencode/blop-single-baseline
taskset -c 0-3 /tmp/opencode/blop-single-baseline/release/examples/kv_bench --dir /tmp/opencode --keys 1000 --reads 5000000 --repeats 3 --mode durable --clients 1 --workers 1
# After the follow-up changes:
cargo build --release --example kv_bench
taskset -c 0-3 target/release/examples/kv_bench --dir /tmp/opencode --keys 1000 --reads 5000000 --repeats 3 --mode durable --clients 1 --workers 1
```

Median blop operations/s, with read rates rounded:

| Operation    |    Before |     After |
| ------------ | --------: | --------: |
| Insert       |     182.6 |     179.7 |
| Update       |     155.1 |     173.4 |
| Snapshot get | 3,374,776 | 4,499,538 |

Read throughput increased **1.33x**. Its inverse changed from about 296 to 222 ns per completed
read, including the harness loop. These are not request-latency percentiles. The individual trial
rates show the variation:

| Operation       | Before                          | After                           |
| --------------- | ------------------------------- | ------------------------------- |
| Inserts/s       | 182.7, 182.6, 123.9             | 179.7, 181.6, 179.1             |
| Updates/s       | 180.0, 155.1, 111.1             | 173.4, 177.5, 164.8             |
| Snapshot gets/s | 3,643,162, 3,008,995, 3,374,776 | 4,543,521, 4,499,538, 4,371,044 |

Write rates varied substantially, particularly in the baseline's last trial. No single-client
durable write speedup is attributed to this change. An instrumented investigation of the five
ordinary log-publication sync calls found roughly 1.0 to 1.3 ms per log/metadata file or database
directory flush on this filesystem. The instrumentation included warm-up and lifecycle operations
and was separate from the reported measurements. Early staging, data-only file sync and paired
private-metadata flush experiments did not establish a useful, repeatable improvement; none of those
experiments is included in the production changes. At that stage, fewer sequential durability steps
were the next write-optimization target. The later WAL results above measure that change.

A separate one-trial, four-client/one-worker check with the same key/read counts reached 7,096,978
snapshot gets/s before and 7,685,552 after, about 1.08x. Inserts were 316.0/s before and 303.9/s
after; updates were 310.9/s and 306.4/s. This is a scaling check, not a median. Use
`--clients 4 --repeats 1` with the commands above to reproduce it.

Regression coverage includes concurrent cache replacement between tables with different key schemas,
bound rejection, historical scans, revocation, close/reopen and retained value bytes that survive
page-tail truncation without pinning directory ownership. The complete workspace tests, all-target
Clippy check and nightly formatting check passed. Cold data, mixed reads/writes and large-value
performance remain outside these measurements.

## Point reads and worker dispatch: 2026-09-09

This historical comparison starts from `9676f05`. The changed build adds direct MVCC point seeks,
in-place key framing, and earlier dispatch of already durable work. The harness, release profile,
CPU set 0 through 3, filesystem, comparison libraries and default engine settings are unchanged. The
main runs use 1,000 inserts, 1,000 updates and **five million cached reads** per fresh database,
with three measured trials and discarded warm-up. Builds, tests and measured workloads ran
separately.

A separate `perf record` run of the baseline, with four clients and four workers, identified point
read costs in tree scan construction/destruction, key escaping, allocation, shared-node lookup and
snapshot/table locking. That profile includes all three engines and setup/recovery; it is not a
blop-only percentage breakdown. Source inspection also found the coordinator registering a prepared
submission before dispatching existing durable work to idle workers.

### Changes measured

- `src/storage/tree.rs`: a point seek retains just the selected leaf and the nearest right-subtree
  reference. It avoids the range iterator, ancestor allocation and copied scan bounds/keys.
- `src/storage/mvcc.rs`: compare the selected physical key with the already encoded address and
  validate its sequence directly. Validate inline value framing through borrowed page bytes, then
  return the owned payload. Overflow values still use the checked overflow reader.
- `src/storage/encoding.rs`: append escaped bytes into the destination buffer with reserved
  capacity, rather than constructing a separate frame and growing multiple buffers.
- `src/database/scheduler.rs`: dispatch ready, already durable transactions before preparing and
  publishing later submissions. Filesystem publication can then overlap worker execution instead of
  leaving available workers idle. Dependencies, execution windows, admission budgets and observed
  worker-failure priority still apply.

This change preserved cache budgets, dependencies, public APIs, and file formats. Receipts still
required durable logging and contiguous visibility. Checkpoint and file-flush rules stayed the same.
Point reads continued to check pinned page prefixes, tree identity, child levels, state framing,
historical table schemas, and revocation.

### Reproduction and results

Run each configuration on the baseline and changed sources:

```sh
cargo build --release --example kv_bench
taskset -c 0-3 target/release/examples/kv_bench --dir /tmp/opencode --keys 1000 --reads 5000000 --repeats 3 --mode durable --clients 1 --workers 1
taskset -c 0-3 target/release/examples/kv_bench --dir /tmp/opencode --keys 1000 --reads 5000000 --repeats 3 --mode durable --clients 4 --workers 1
taskset -c 0-3 target/release/examples/kv_bench --dir /tmp/opencode --keys 1000 --reads 5000000 --repeats 3 --mode durable --clients 16 --workers 4
```

Median blop operations/s from those runs, with read rates rounded:

| Clients | Workers | Operation    |    Before |     After | After / Before |
| ------: | ------: | ------------ | --------: | --------: | -------------: |
|       1 |       1 | Insert       |     180.9 |     138.3 |          0.76x |
|       1 |       1 | Update       |     176.4 |     113.7 |          0.64x |
|       1 |       1 | Snapshot get | 1,931,053 | 3,158,701 |          1.64x |
|       4 |       1 | Insert       |     166.5 |     314.9 |          1.89x |
|       4 |       1 | Update       |     169.4 |     302.2 |          1.78x |
|       4 |       1 | Snapshot get | 5,082,348 | 7,368,257 |          1.45x |
|      16 |       4 | Insert       |     970.7 |     971.8 |          1.00x |
|      16 |       4 | Update       |     879.9 |     841.3 |          0.96x |
|      16 |       4 | Snapshot get | 5,149,760 | 7,177,934 |          1.39x |

Per-trial blop rates in trial order, with reads expressed in millions/s:

| Clients / Workers | Operation | Before               | After                |
| ----------------- | --------- | -------------------- | -------------------- |
| 1 / 1             | Insert    | 180.9, 181.4, 166.3  | 138.3, 171.1, 121.8  |
| 1 / 1             | Update    | 173.1, 176.4, 178.9  | 72.6, 173.2, 113.7   |
| 1 / 1             | Get       | 1.950, 1.931, 1.667  | 3.532, 3.159, 2.992  |
| 4 / 1             | Insert    | 166.5, 156.3, 170.5  | 321.0, 298.4, 314.9  |
| 4 / 1             | Update    | 169.4, 169.7, 162.3  | 302.2, 325.7, 268.5  |
| 4 / 1             | Get       | 4.605, 5.112, 5.082  | 6.998, 7.742, 7.368  |
| 16 / 4            | Insert    | 831.3, 1041.0, 970.7 | 870.6, 1020.0, 971.8 |
| 16 / 4            | Update    | 879.9, 814.7, 912.4  | 841.3, 895.2, 834.6  |
| 16 / 4            | Get       | 5.150, 5.146, 5.245  | 7.018, 7.508, 7.178  |

### Interpret variation

The four-client/one-worker write gain and cached-read gains repeat across these trials. The
16-client write results overlap the baseline range and do not establish a throughput improvement.
Read scaling still encounters shared snapshot/table locks and the decoded-node cache; these changes
remove allocation and copying rather than all contention.

The single-client write medians were substantially worse in the initial after-run. Comparison-engine
timings also varied: redb insert rates were 732 to 1239/s before and 794 to 889/s after; SQLite
inserts were 651 to 958/s before and 547 to 944/s after. To investigate, separate baseline and
changed binaries were run with the same arguments and `--repeats 1`, first baseline/changed and then
changed/baseline:

| Run order | Revision | Inserts/s | Updates/s | Snapshot gets/s |
| --------: | -------- | --------: | --------: | --------------: |
|         1 | Baseline |     181.3 |     168.4 |       1,675,769 |
|         2 | Changed  |     117.2 |     149.9 |       3,525,125 |
|         3 | Changed  |     177.6 |     176.6 |       3,367,589 |
|         4 | Baseline |     168.2 |     169.4 |       2,000,878 |

The alternating checks did not show a consistent single-client write gain or regression. The tables
retain all original trials. Variation in workstation I/O prevents a clear attribution. With only one
outstanding request, dispatch cannot overlap another request's publication. This version also still
required five ordinary log-publication synchronization calls. These results do not establish a
single-client write improvement.

A separate, one-trial comparison with 10,000 keys and five million reads used 16 clients and four
workers. It completed full-value reopen checks in both revisions:

| Operation       |    Before |     After |
| --------------- | --------: | --------: |
| Inserts/s       |     845.8 |     848.6 |
| Updates/s       |     727.9 |     789.9 |
| Snapshot gets/s | 3,603,813 | 6,009,632 |

Use the last command above with `--keys 10000 --repeats 1` for that larger run. Its 1.67x read gain
is a single observation, not a median. It does not establish cold-I/O, mixed-workload or latency
behaviour. All measured trials verified final values and checkpoints after reopen and completed
temporary-directory cleanup.

New regressions compare point seeks with ordered successors across leaf and internal-subtree gaps
and pinned roots, and compare MVCC point results with snapshot scans across historical versions,
tombstones, key extensions, maximum-size keys and overflow values. Malformed matching frames,
unrelated invalid values, wrong tree identities, out-of-prefix roots and incorrect child levels are
also covered. The complete workspace tests, nightly formatting check and all-target Clippy check
passed, including the existing scheduler, crash/replay, revocation and publication-fault tests.

## Deferred checkpoints: 2026-09-08

This historical comparison starts from `838a303` and measures the deferred-checkpoint changes in
`9676f05`. It uses the same, unchanged harness, release profile, CPU set 0 through 3 and filesystem
as the earlier runs. Each configuration has 1,000 independent inserts, 1,000 independent updates and
100,000 cached reads per fresh database, with three measured trials and discarded warm-up.
Configurations and builds/tests ran separately, not concurrently with measured workloads.

This change set the default checkpoint interval to 64 visible records. Receipts still required
durable logging, complete installation, and contiguous visibility. Materialized pages could be
checkpointed later. Interval 1 preserved checkpoint-before-receipt behaviour. The change preserved
the on-disk format, durability-before-execution rule, and manifest/CURRENT ordering.

The benchmark still verifies full values and the final checkpoint after close/reopen. Periodic
checkpoints are inside the write timers. As before, close is outside timing; it now publishes the
remaining partial interval. The measurements are durable receipt throughput, not throughput for
checkpointing every transaction, and not request-latency percentiles.

Run each command on `838a303`, then rebuild and run on `9676f05`:

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

In this version, an ordinary single-client transaction on an existing log requested five file or
directory synchronization calls. The previous version requested ten across log and checkpoint
publication. A periodic checkpoint added five calls. Log-only publication kept the selected roots'
page count, so later reconstructible pages did not trigger a page flush.

These counts describe calls, not necessarily physical device flushes. New files and administrative
operations added work. Removing per-completion publication also allowed clients to proceed without
that coordinator I/O pause.

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

## Optimized results: 2026-09-08

The benchmark baseline was committed as `5807d73` before the engine changes in `838a303`. These runs
used the same harness with live caches, reusable validation results, bounded checkpoint filtering,
grouped commits, and grouped completion handling. The engine also omitted data flushes already
satisfied by a previous publication. The change preserved file formats, receipt semantics, and
retained-history policy.

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

### Durable writes

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

### Cached snapshot reads

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

### Larger history

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

### Costs remaining in this version

This version still trailed redb on the measured workload. At 16 clients and 1,000 keys, redb's
durable write rate was about 3.8 times blop's rate, and its cached-read rate was about 4.3 times
blop's rate. SQLite's durable writes were faster than blop's, while its cached reads were slower in
this configuration.

Publication still required substantial sequential work in this version. An ordinary single-client
transaction on an existing log requested ten file or directory synchronization calls across two
publications, down from fourteen. New files required extra directory synchronization. These are call
counts, not necessarily physical device flushes.

Grouping shared publication cost across transactions but retained its ordering requirements and
single coordinator. Copy-on-write (COW) pages, retained history and outcomes, VM preparation, cache
locking, and typed values also added work.

Caches and proofs are bounded. Exceeding the edit or digest cache bounds can reinstate full scans,
and many retained segments still require descriptor processing. Full recovery and maintenance can
still scale with retained history. Cold reads, larger-than-cache data, large transactions, tail
latency and per-optimization CPU/I/O attribution remain unmeasured.

## Baseline results: 2026-09-08

### Recorded environment

| Component               | Setting                                                                                         |
| ----------------------- | ----------------------------------------------------------------------------------------------- |
| Processor               | AMD Ryzen AI 9 HX 370.                                                                          |
| CPU affinity            | Logical CPUs 0 through 3, four distinct high-frequency physical cores on this machine.          |
| Operating system        | Linux 7.2.3-arch1-2.                                                                            |
| Filesystem              | ext4 on `/dev/nvme0n1p3`.                                                                       |
| NVMe drive              | `MTFDKBA1T0QFM-1BD1AABGB`.                                                                      |
| Rust build              | Rust 1.98.0; standard Cargo release profile; no added target-CPU flags.                         |
| blop production sources | `4040dce4fd776e5423e1ca0f24982056a8a5297e`, with the concurrent harness committed in `5807d73`. |
| Comparison libraries    | redb 4.2.0; rusqlite 0.40.2 with bundled SQLite 3.53.2.                                         |

The runs used a local workstation rather than an isolated performance lab.

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

### Durable writes

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

### Cached snapshot reads

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

### Historical profiling targets

The baseline's flat write rate was consistent with substantial sequential publication work. It did
not show whether independent execution overlapped. Source inspection identified these profiling
targets in the measured revision:

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

These were source-level observations, not measured cost percentages. The baseline run included no
engine optimizations. The later result sections describe the changes subsequently measured.

This workload does not measure deletes, scans, mixed reads and writes, conflicts, compute-heavy
programs, multi-key transactions, large values, cold reads, large datasets, reclamation, or tail
latency. It cannot establish worker-pool scaling for compute-heavy programs.

## Historical results: 2026-09-07

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

Run the commands below to check the harness. Its `test = true` manifest entry includes example tests
in `cargo test --workspace`. They check reproducible workload generation, invalid options, uneven
client partitions, error propagation, values across serial and concurrent engines, public snapshot
sequences, persisted checkpoints, and temporary-directory cleanup.

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
mdformat --wrap 100 --check BENCHMARKS.md
```

These checks passed for the recorded implementations. They validate the harness and selected engine
behaviours; rerun measurements to assess performance on a different system or revision.
