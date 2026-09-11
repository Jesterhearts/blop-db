# blop-db

blop-db is an embedded database for Rust applications. Write a transaction as a small `tx!` program
that reads and changes data atomically. The database records the program and its inputs durably
before running it. Independent transactions can run in parallel, but their results match execution
in log order.

Use the asynchronous `blop_db::database` API for application work. It provides writes,
fixed-sequence snapshots, changefeeds, logical replication, manual maintenance, and physical
backups. The lower-level virtual machine (VM) and storage APIs support engine development and
reference testing.

## Find the information you need

- [Run the example](#run-the-example) to create a database and transfer a balance.
- [Submit writes](#async-writes) and [handle results](#handle-results-and-retries).
- [Set execution capacity](#bounded-execution) and understand checkpoints.
- [Read snapshots and feeds](#snapshots-and-feeds).
- [Replicate a database](#logical-replication) or [maintain a SQLite index](#derived-sqlite).
- [Reclaim storage](#maintenance) and [back up or attach a database](#backup-and-attach).
- [Write transaction programs](#transaction-construction).
- [Use reference execution](#reference-execution) or [physical storage](#storage).
- [Check platform support](#platform-support).
- [Run development checks](#development).

For binary formats and required semantics, read the [design specification](DESIGN.md). For
implemented features and test coverage, read the [conformance guide](CONFORMANCE.md). For
measurements and their limits, read the [benchmark report](BENCHMARKS.md).

## Run the example

You need a Rust toolchain with Cargo. Runtime tests have run on Linux; see
[platform support](#platform-support) for other systems. Run this command from the repository root
and choose a database directory that does not yet exist. Its parent directory must exist.

```sh
cargo run --example transactions -- /tmp/blop-example-db
```

The [balance-transfer example](examples/transactions.rs) creates a balances table and two accounts,
then transfers 25 units atomically. It reopens the database and verifies that both balances are 75.
The example uses Tokio's current-thread executor and handles transaction aborts explicitly. It
refuses to overwrite an existing directory.

The verification transaction also enters the log. For reads that do not consume a transaction
sequence number, use [snapshots](#snapshots-and-feeds).

## Terms used in this guide

| Term                                     | Meaning                                                                                    |
| ---------------------------------------- | ------------------------------------------------------------------------------------------ |
| Sequence                                 | A record's permanent position in the database log.                                         |
| Prefix                                   | All records from the start of the log through a given sequence, with no gaps.              |
| Durable frontier, D                      | The last sequence in the contiguous durable log.                                           |
| Visible frontier, F                      | The last sequence through which every record is durable and fully resolved.                |
| Checkpoint, C                            | The sequence whose materialized state has been published for recovery.                     |
| Outcome                                  | A transaction's success result or deterministic abort result.                              |
| Catalogue                                | Table identities, names, schemas, and live or dropped status.                              |
| Canonical encoding                       | The stable byte representation required by a format.                                       |
| Resolved                                 | Finished with success or a deterministic abort, with the complete result installed.        |
| Access manifest                          | Declarations of the keys or tables a transaction may read or write.                        |
| Storage manifest                         | Published metadata identifying checkpoint roots, durable log bounds, and retained history. |
| Overlay                                  | A transaction's private pending writes, discarded if it aborts.                            |
| Multi-version concurrency control (MVCC) | Storage of sequence-tagged versions so readers can select a historical state.              |
| Tombstone                                | A version that records deletion and stops a lookup from returning an older value.          |
| Cursor                                   | A durable registration that protects history for a feed consumer.                          |
| Watermark                                | A database identity and sequence that identify the source state a consumer has processed.  |
| Pin                                      | A reference that keeps needed storage from being reclaimed.                                |
| Copy-on-write (COW)                      | Writing new pages and roots instead of modifying published pages.                          |
| UUID                                     | A universally unique identifier.                                                           |

`1 KiB = 1,024 bytes` and `1 MiB = 1,048,576 bytes`. Materialized state means the stored data and
metadata computed by executing the log.

A write-ahead log (WAL) group holds complete records and checksums. On an existing segment, one file
flush makes the group durable. Checkpoint and storage-metadata publication use manifests and the
`CURRENT` selection file. Version 1 uses 4 KiB-aligned WAL segment headers and commit groups. See
[WAL commit groups](DESIGN.md#appendix-i-wal-commit-groups) for its recovery rules.

`tx!` compiles to instruction set architecture (ISA) 1 bytecode during Rust compilation. It parses,
checks types, allocates registers, and resolves branches at that stage. At runtime, binding copies
captured inputs and resolves table IDs. Binding does not execute the program.

## Async writes

Create or open a database, submit a bound `tx!` program, check its outcome, and close the database
when you finish. The free functions in `blop_db::database` use a cloneable `Database` handle:

- `create(path, options).await` creates a new directory. `CreateOptions::default()` generates a UUID
  database ID and cursor namespace, and supplies default named limits. The parent directory must
  already exist.
- `open(path).await` restores the checkpoint and replays any later durable records before accepting
  work.
- `create_with_options(path, create_options, engine_options).await` and
  `open_with_options(path, engine_options).await` select nonpersistent worker and capacity settings.
  These settings belong to the process and are separate from `CreateOptions`.
- `execute(&db, transaction, claims).await` returns a `Receipt { sequence, outcome }` after the
  transaction is durable, resolved and included in the contiguous visible prefix. Its materialized
  checkpoint may lag behind; recovery replays the durable log suffix.
- `execute_with_manifest(&db, transaction, claims, manifest).await` independently validates supplied
  C.4 declarations before sequencing. Broader declarations are allowed and retained exactly.
- `execute_catalogue(&db, operation).await` creates, renames or drops tables. A successful create
  returns its table ID as `Value::U64` in the outcome.
- `execute_limits(&db, limits).await` changes the policy for subsequent records, even when the old
  policy prevents transaction submission.
- `close(&db).await` stops submissions from every clone, drains accepted requests, checkpoints the
  visible prefix and waits for the directory lock to be released. Dropping every handle also drains
  and checkpoints accepted work, but does not wait or report shutdown failures.
- `status(&db)` returns the latest `EngineStatus` sample without waiting for workers or storage I/O.

### Set transaction limits

Both transaction claims and database policy use `Limits`, which names all 17 logical resources. A
claim states the maximum a transaction may use. Defaults are the finite format ceilings. Override
individual fields for your application, and keep each transaction's claims within the current
database policy:

```rust
# #[cfg(any(unix, windows))]
# #[tokio::main(flavor = "current_thread")]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
use blop_db::{database, Limits, tx};
use database::CreateOptions;

let limits = Limits {
    instructions: 1024,
    writes: 100,
    overlay_bytes: 8 * 1024 * 1024,
    ..Limits::default()
};
# let temporary = tempfile::tempdir()?;
# let path = temporary.path().join("database");
let db = database::create(&path, CreateOptions {
    limits,
    ..CreateOptions::default()
}).await?;
let receipt = database::execute(&db, tx! { return 42; }?, limits).await?;
println!("Sequence {}: {:?}", receipt.sequence, receipt.outcome);
database::close(&db).await?;
# Ok(())
# }
# #[cfg(not(any(unix, windows)))]
# fn main() {}
```

Creation generates independent version-4 UUIDs using operating-system randomness. To supply existing
identities, set `CreateOptions::database_id` or `cursor_namespace` to `Some([u8; 16])`; explicit IDs
must be unique and nonzero. Inspect the selected identities with `db.database_id()` and
`db.cursor_namespace()`. Reopening preserves both identities and the persisted policy. Defaults are
never substituted during replay. For the low-level APIs, `LimitPolicy::try_from(limits)` validates
the same named fields and `Limits::from(&policy)` exposes a stored policy by name.

### Understand where work runs

The API uses Tokio channels and semaphores, but its futures can run on any executor. Validation,
blocking publication I/O and installation run on one coordinator thread per database. Persistent
standard-library worker threads interpret typed `vm::PreparedTransaction` values without mutating
storage. Recovery remains sequential and uses the recorded historical catalogue and semantic policy,
not the live scheduler's operational limits.

### Handle results and retries

Check both the call's `Result` and the receipt's `Outcome`:

| Result                                | Meaning and action                                                                                                     |
| ------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| `Ok(Receipt)` with `Outcome::Success` | The transaction is durable and visible. Use its returned value and effects.                                            |
| `Ok(Receipt)` with `Outcome::Aborted` | The abort is durable and consumes its sequence. All business writes were discarded. Handle the abort reason.           |
| `Error::Rejected`                     | Validation failed before sequencing. Correct the request before resubmitting.                                          |
| `Error::OperationalLimit`             | The request exceeds process capacity and was rejected before sequencing. Increase that capacity or reduce the request. |
| `Error::Storage`                      | A startup or pre-append system operation failed. Inspect the error.                                                    |
| `Error::Uncertain`                    | The request may have committed. Close and reopen the database to recover before proceeding.                            |
| `Error::Closed` on submission         | That request did not execute because the writer was closed.                                                            |

After a system failure, the writer stops. Close and reopen it before submitting more work.

**Dropping a future after enqueueing does not cancel its transaction.** Neither cancellation nor an
uncertain result proves that no writes occurred. Retrying creates a new transaction. If your
application needs deduplication, include its request-ID check and business writes in the same `tx!`
program.

### Bounded execution

`EngineOptions` controls recovery, live admission and scheduling independently of `Limits`:

| Field                    | Default                               | Purpose                                                                                |
| ------------------------ | ------------------------------------- | -------------------------------------------------------------------------------------- |
| `tail_recovery`          | `TailRecovery::DiscardInvalid`        | Recovery policy for malformed WAL suffixes when opening or attaching.                  |
| `workers`                | Available CPUs clamped to 2 through 4 | Persistent interpreters; configurable from 1 through 256.                              |
| `execution_window`       | 64                                    | Dispatch only `F < N <= min(D, F + W)`, using overflow-safe arithmetic.                |
| `checkpoint_interval`    | 64                                    | Newly visible records before checkpoint publication; configurable from 1 through 4096. |
| `submission_queue_count` | 64                                    | Count permits for unassigned submissions, including the prepared queue head.           |
| `submission_queue_bytes` | 64 MiB                                | Input byte reservations for those submissions.                                         |
| `assigned_backlog_count` | 64                                    | Maximum assigned records waiting for visible receipts.                                 |
| `assigned_backlog_bytes` | 64 MiB                                | Maximum canonical log bytes in that assigned backlog.                                  |
| `execution_bytes`        | 512 MiB                               | Aggregate lifetime reservations for assigned transactions.                             |
| `preparation_bytes`      | 512 MiB                               | Separate capacity for one active validation or prepared queue head.                    |

In the execution-window rule, N is the transaction's sequence and W is the configured window size. F
is the visible frontier and D is the durable frontier. Use overflow-safe sequence arithmetic.

#### Admission and queue limits

The engine acquires count and byte permits before enqueueing. Waiting producers retain their own
inputs; those inputs have not been accepted by the engine. Count permits can also be held while
waiting for byte permits, so diagnostics report admission reservations rather than only channel
occupancy. Cancelling such a wait releases its permits. The writer never drops a sequenced
transaction to make space.

#### Import reservations

Logical imports share the count and byte budgets. Only one import may be parsing, queued, or running
per database. Other callers wait while retaining their own borrowed input.

An accepted batch reserves its record count, canonical bytes, decoded commands, supplied outcomes,
and codec scratch space until completion. An empty poll reserves one count permit. The batch's
canonical record count and bytes must also fit the assigned-backlog limits.

Reference execution is sequential. Preparation and execution capacity therefore cover the retained
batch plus one record's interpreter reservation. H.1 decoding runs on the caller before enqueueing,
within format bounds. Historical validation and execution run on the coordinator. Disk space for the
prefix copy and retained history is outside these budgets.

#### Transaction reservations

Before assigning a transaction, the coordinator reserves capacity for its decoded program, scopes,
dependencies, registers, distinct addresses, overlay, pending outcome, and versions above the
visible frontier. The reservation covers data representation and temporary buffers, including
zero-width tuples and scan results built before destination checks.

The reservation remains charged until the record is installed and visible. Reserving each record's
full lifetime in sequence order, then dispatching the oldest ready work first, protects the oldest
record's ability to finish. Preparation separately reserves decoding and access-analysis scratch
space before running them. These conservative estimates may reject a program that would fit in
practice.

#### Respond to capacity errors

Resource pressure delays admission. If a single record cannot fit the configured capacity,
submission returns `Error::OperationalLimit { resource, required, limit }` before sequencing. It
does not append an abort or change semantic claims. Increase the relevant process capacity or reduce
the transaction. Reopening with smaller operational settings still replays already durable records
under their original semantics. A process must nevertheless have enough actual resources to perform
that replay.

Reservations limit accounted capacity. They do not cap resident memory or total disk space. Storage
traversal buffers, thread stacks, allocator/OS overhead, public read/feed results and retained
visible history are separate. Administrative operations drain transactions and use fixed
format-bounded scratch rather than transaction execution reservations. Their canonical record must
still fit the assigned byte limit. Retained history and maintenance scratch are separate from these
budgets. Manual maintenance can reclaim eligible history and obsolete files; without it, total disk
usage continues to grow.

### Dependencies and visibility

The coordinator registers dependencies in durable sequence order. A reader waits for **all**
unresolved earlier possible writers that overlap its reads, including table-wide declarations,
aborted predecessors and successful branches that omit writes. Blind writers remain independent.
When dependencies become ready, the coordinator captures the latest pinned roots; workers read only
versions below their own sequence. The coordinator installs each complete result with the
crate-private VM installation hook, against the latest store, before satisfying any dependencies.
Workers never publish stale roots or consume another transaction's tentative overlay.

### Checkpoints and recovery work

A flushed WAL group advances D while `CURRENT` can still select an older checkpoint at C. A receipt
can follow an advance of F without a new checkpoint: every record through F is already durable and
fully installed. Recovery discards materialized state after C and replays every record after C
through D, written `(C, D]`. This includes acknowledged transactions, aborts, and no-write outcomes.

Set `EngineOptions::checkpoint_interval` to 1 to checkpoint before every receipt.

The coordinator publishes a checkpoint when `F - C >= checkpoint_interval`, before releasing the
receipts for that advance. It also checkpoints the drained prefix before catalogue/policy barriers,
maintenance and normal shutdown. An idle database can keep a smaller suffix uncheckpointed; the
interval bounds record count, not elapsed time, bytes or replay cost. A completion group can cross
the threshold, and unresolved assigned records can extend D beyond F. Lower the interval for large
transactions or tighter recovery-work requirements. Logs needed for replay remain protected by C,
even when snapshots and cursor baselines have advanced beyond it.

### Grouped publication

Checkpoint roots exclude entries above F. When F equals D, the live roots already satisfy that
boundary. Otherwise, a bounded index of above-checkpoint edits avoids rescanning retained history
when possible. The coordinator groups up to 64 already queued local transactions within existing
count and byte budgets, without waiting to fill a group. All group records are published durably
before any dispatch. Administrative requests, queued controls and reported worker completions stop
group collection. There is no asynchronous checkpoint writer or semantic merging of transactions.
See [BENCHMARKS.md](BENCHMARKS.md) for measured workloads.

### Recover from a checkpoint failure

A failed checkpoint stops the writer and requires reopening. Previously successful receipts remain
durable through replay; receipts still waiting on the failed publication are uncertain. Diagnostics
retain F rather than lowering it. If `close` was accepted and the writer stops in a failed state, it
returns `Error::Storage(storage::Error::NeedsRecovery)` after releasing the storage handles. The
error detail is available in `status(&db).last_error`.

### Caches and validation

After startup recovery, each live page-file descriptor has a 64 MiB decoded-node cache. Workers and
snapshots share its immutable nodes. A cache hit still checks the requesting view's pinned prefix.
The cache excludes overflow values. Active readers and older descriptors pinned across compaction
can retain memory beyond that cache budget.

Each snapshot claim caches its last resolved historical table and decoded key schema until
revocation. Point reads borrow that metadata under a read guard and decode inline values from
retained leaf bytes. Replacing the cached table waits for those reads; scans retain their own shared
table metadata. Low-level reference stores do not use these caches.

Live publication reuses validation results, called proofs, for immutable roots and log prefixes. New
physical edits must pass system key/value framing checks before inheriting a root proof. Invalid
edits require full publication validation. New log suffixes still receive complete envelope,
checksum, hash-chain, and anchor checks.

The engine retains at most 4,096 pending record digests and 1 MiB of accounted above-checkpoint edit
entries. Unsupported transitions or full caches trigger full validation or filtering. These caches
are separate from admission reservations and do not persist across reopening. Full checkpoint
validation bypasses the page cache, and file handover resets proofs.

External changes to an owner's immutable files are unsupported while it is live. Caches do not
continuously detect later external corruption.

### File publication and damaged tails

Publication flushes changed page and log files, and flushes directories for new filenames. Already
published, unchanged prefixes need no repeated flush. Runtime publications that reuse the selected
roots also retain their published page count, leaving reconstructible post-checkpoint pages outside
the durable prefix until roots change. New manifests and CURRENT still follow the complete G.3
file-flush, rename and directory-flush ordering before durability is reported.

Log appends validate and append up to 64 canonical records inside one checksummed group, flush its
file, then advance D before dispatch. New segments also require a directory flush. Recovery
validates the selected checkpoint and log prefixes, discovers complete linked WAL groups beyond
them, flushes the recovered suffix, and replays `(C, D]`. Group framing adds 168 bytes per group;
canonical record bytes and logical replication digests are preserved. New segment headers occupy a
zero-padded 4 KiB block. Each group starts on a 4 KiB boundary and is zero-padded through the next
boundary before its existing file flush. Later appends never write into an earlier group's blocks,
including its padding. Checkpoints may end inside a group, but durable physical log bounds include
the whole padded group. This isolates writes on storage with 4 KiB write-failure isolation; it does
not assume atomic 4 KiB writes or protect against devices that damage neighbouring blocks.

#### Choose a tail recovery policy

`EngineOptions::tail_recovery` is a process-local option used by `open_with_options` and
`attach_with_options`:

- `TailRecovery::DiscardInvalid` is the default. Beyond the manifest's selected durable bounds,
  recovery retains complete valid groups, then discards the suffix from the first malformed group
  through the end of that segment. It never skips a damaged group to resume at a later group.
- `TailRecovery::Strict` rejects complete-sized malformed groups without modifying the WAL.

Both policies discard physically short terminal appends, including incomplete padding. Damage inside
selected log bounds or checkpoint data is always an error. Unsupported formats, checksum-valid
group-header sequence or predecessor mismatches, and complete segment forks or gaps remain errors. A
complete successor after a damaged accepted segment is also an error, before any tail is trimmed.

For strict recovery:

```rust,no_run
# #[cfg(any(unix, windows))]
# #[tokio::main(flavor = "current_thread")]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
use blop_db::database::{self, EngineOptions, TailRecovery};

let db = database::open_with_options("/path/to/database", EngineOptions {
    tail_recovery: TailRecovery::Strict,
    ..EngineOptions::default()
}).await?;
database::close(&db).await?;
# Ok(())
# }
# #[cfg(not(any(unix, windows)))]
# fn main() {}
```

The default favours automatic recovery from full-length torn appends. Later storage damage to an
acknowledged suffix outside selected bounds can be mistaken for an interrupted append and discarded.
Physical truncation of that suffix is ambiguous under either policy. Selected byte bounds remain
strict. Reopening starts a fresh segment using the same aligned layout.

### Administrative requests and controls

Catalogue and policy requests stop later sequencing, finish the preceding prefix, then execute their
durable barrier. Submissions queued behind a barrier are prepared against the resulting metadata.
Snapshots and cursor/feed operations use a separate bounded control queue and can run at F while a
worker or transaction admission is blocked. They serialize with coordinator I/O and installation, so
they are not a hard latency guarantee. Cursor publication preserves current retention metadata while
filtering the four logical checkpoint roots at C, including when `C < F < D`. On normal shutdown or
a handled system failure, unread control requests receive `Error::Closed` without executing, rather
than an uncertain result caused by dropping their replies.

### Inspect engine status

`status(&db)` reports the configured budgets, log tail, D/F/C, queue reservations, assigned backlog,
reserved execution and preparation bytes, active workers, oldest unresolved sequence, dependency
waiters, resolved-above-frontier count, pending administrative barrier, retention floors and
shutdown or poison state. It also retains the last system error. Except for queue permits sampled at
the call, these fields describe the last coordinator iteration and can lag during blocking I/O. They
are diagnostics, not receipts. Worker system errors and caught panics stop dispatch and require
reopening; they are never encoded as semantic aborts. Unwinding coordinator panics also close
admission. A process configured to abort on panic must recover after process restart instead.

### Access declarations

An access scope declares a key or table that a transaction may read or write. The writer derives
normalized scopes using the abstract value analysis in design section C.4. Known keys use canonical
point scopes; unknown keys and scans use table scopes. Both sides of every branch contribute, even
when a condition or runtime failure is predictable. Reads and writes remain separate: a table-wide
write does not broaden a point read. Unused table declarations add no scopes, but must still name
live tables. Resource 7 (`manifest_scopes`) charges the actual normalized manifest entry count, with
a read/write entry counted once.

Recovery supports the full C.4 codec, including older broad-table manifests. It validates supplied
coverage, canonical key encodings, table-array membership and counts under the historical catalogue
and policy. It preserves the recorded declarations rather than replacing them with a narrower
derivation. Log rotation and retention-aware reclamation are explicit local maintenance operations.

## Snapshots and feeds

Call `snapshot(&db).await` to capture the visible frontier. Use `get`, `scan`, and `catalogue` to
read that fixed sequence, including the table names, status, and schemas that applied then.

Reads perform synchronous local I/O on the calling thread. Use your executor's blocking facility for
large reads. Supply keys and range endpoints as `vm::Value` values. The API checks the stored key
schema before encoding them. Scans return typed `(Value, Value)` pairs in key order. `catalogue`
includes both live and dropped tables.

```rust
# #[cfg(any(unix, windows))]
# #[tokio::main(flavor = "current_thread")]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
use std::ops::Bound::Unbounded;
use blop_db::{database, Limits, tx};
use blop_db::vm::{CatalogueOperation, Outcome, Type, Value};
# let directory = tempfile::tempdir()?;
# let db = database::create(directory.path().join("db"), Default::default()).await?;
let created = database::execute_catalogue(&db, CatalogueOperation::Create {
    name: "balances".into(), key: Type::U64, value: Type::I64,
}).await?;
let Outcome::Success { value: Value::U64(table), .. } = created.outcome else {
    return Err("table creation aborted".into());
};
database::execute(&db, tx! {
    tables { balances: u64 => i64 = table }
    balances[7] = 100;
}?, Limits::default()).await?;

let snapshot = database::snapshot(&db).await?;
assert_eq!(database::get(&snapshot, table, &Value::U64(7))?, Some(Value::I64(100)));
for row in database::scan(&snapshot, table, Unbounded, Unbounded)? {
    let (key, value) = row?;
    println!("{key:?}: {value:?}");
}
database::revoke(&snapshot);
assert!(matches!(database::catalogue(&snapshot), Err(database::Error::SnapshotRevoked)));
database::close(&db).await?;
# Ok(())
# }
# #[cfg(not(any(unix, windows)))]
# fn main() {}
```

### Revoke a snapshot

Snapshot clones and iterators share one revocable claim. `revoke(&snapshot)` releases its view once,
after in-flight reads finish safely at their original sequence. New reads report `SnapshotRevoked`.
An iterator reports `Some(Err(SnapshotRevoked))`, not ordinary exhaustion, even if it previously
returned `None`. It is not a fused iterator. An iterator retains no separate permanent storage view;
each `next()` seeks within the guarded original view. This trades an extra tree seek per row for
bounded revocation and close behaviour.

`close(&db).await` revokes all snapshots and drains in-flight reads before releasing the directory
lock. Idle snapshots and iterators do not delay close and cannot read after it. Dropping the last
database handle also shuts down and revokes snapshots asynchronously. There is no automatic
pressure-based revocation policy yet.

### Start a snapshot build and its feed

For a rebuild, `snapshot_and_cursor` selects F and durably registers a cursor at F in one operation.
The cursor survives snapshot revocation, handle loss, close and ordinary restart. If the snapshot is
revoked, discard the incomplete build and explicitly release any cursor you no longer need.

```rust
# #[cfg(any(unix, windows))]
# #[tokio::main(flavor = "current_thread")]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
use blop_db::database::{self, BatchLimits, CursorKind, CursorToken, FeedBatch, Watermark};
# let directory = tempfile::tempdir()?;
# let db = database::create(directory.path().join("db"), Default::default()).await?;
let (snapshot, cursor) = database::snapshot_and_cursor(
    &db, CursorKind::Resolved, "search-index-build",
).await?;
let baseline = snapshot.watermark();
let saved_token = cursor.encode(); // Exactly 56 bytes; persist for restart.
// Build unpublished derived data from the snapshot. Publish the complete build
// and baseline.encode() (or baseline.to_hex()) in one atomic durable index commit.
database::revoke(&snapshot);

let cursor = CursorToken::decode(&saved_token)?;
database::reopen_cursor(&db, &cursor).await?;
let batch = database::read_feed(&db, &cursor, baseline, BatchLimits::default()).await?;
let wire = batch.encode()?;
assert_eq!(FeedBatch::decode(&wire)?, batch);
let next = batch.watermark()?;
let payload = next.to_hex(); // Exactly 80 lowercase ASCII hex characters.
let recovered = Watermark::from_hex(&payload)?;
// Apply every complete record, then commit derived data and payload atomically.
// Only after that durable consumer commit may its watermark be acknowledged:
database::acknowledge_cursor(&db, &cursor, recovered).await?;
database::release_cursor(&db, &cursor).await?;
database::close(&db).await?;
# Ok(())
# }
# #[cfg(not(any(unix, windows)))]
# fn main() {}
```

### Resume a consumer

If you have a saved token, call `reopen_cursor` to use its existing registration. Otherwise, call
`checkout_cursor(&db, recovered_watermark, kind, label)` to establish a claim, provided the required
history remains.

The API checks the token's database ID, local cursor namespace, issued ID, and registered kind.
Tokens identify registrations; they are not authorization credentials. A watermark identifies source
state, not an index library's internal operation stamp. Saving a watermark alone does not reserve
history.

### Read complete feed records

`read_feed` returns consecutive complete `vm::OutcomeRecord` values without advancing the cursor. It
includes aborts, no-write successes, catalogue events, and policy events. It does not omit source
sequences through table filtering.

Effects contain canonical key and value bytes checked against the exact sequence-tagged versions.
Consumers need the baseline catalogue and later catalogue events to interpret them.

Batch limits include the header and checksum, with a maximum of 256 MiB. If the first complete
record cannot fit, the call returns `BatchTooSmall { required }`. A zero record limit or a poll at F
returns a valid empty batch.

### Acknowledge and release cursors

Commit derived data and its watermark atomically before acknowledging progress. An acknowledgement
requires the matching database identity and may stay at the current position or advance through F.
Repeating it succeeds.

Release a cursor with its token. Releasing an already absent, previously issued ID succeeds, but
reopening or acknowledging that ID returns `CursorReleased`. IDs are never reused.

Use `list_cursors` to inspect tokens, baselines, and diagnostic labels, then pass the selected token
to `release_cursor`. Labels are not unique and cannot select a registration. A cancelled checkout
may still leave a durable registration, so inspect the list for abandoned claims.

`retention_status` reports conservative history and log claim floors. Cursor operations consume no
transaction sequence and do not themselves delete history.

### Choose a feed kind

The cursor kinds are resolved feed (1), logical feed (2), and log replica (3). Kinds 2 and 3
additionally check and protect original log availability from baseline + 1.
`read_logical_feed(&db, &cursor, after, limits).await` accepts either kind and returns
`FeedRecords::Logical`: the exact canonical record bytes together with each resolved outcome. It
uses the same complete-record limits, visible-only prefix and no-acknowledgement rules as
`read_feed`. Local log segments may have different IDs and boundaries on a replica; the canonical
record bytes, source sequences and SHA256 digests do not change.

H.1 codecs support both feed kinds, checking CRCs, bounded counts, consecutive sequences and logical
envelope/outcome hash-chain agreement, including decode-side inter-record chaining. Codec validation
alone does not prove that supplied effects match VM execution or the destination's prefix.

## Logical replication

To create and update a replica:

1. Retain a kind-2 or kind-3 cursor on the source to protect the required log history.
1. Create a physical image with `backup`, then attach it with
   `attach(path, AttachMode::ReadOnlyReplica)`.
1. Read the replica's recovered `snapshot(&replica).await?.watermark()`.
1. Request the source's logical feed after that watermark. The source must have made the baseline
   visible before it can serve the following records.
1. Encode the batch and call `import_logical(&replica, &encoded).await`.
1. After the replica returns its durable visible watermark, acknowledge that position to the source
   cursor.

Reading or importing does not acknowledge a source or local cursor automatically.

### How import validates records

Import has two separate execution stages:

1. Parse the entire bounded H.1 batch and its record bodies before enqueueing. On the coordinator,
   require a read-only replica at `C = F = D`, the matching database identity and baseline, and a
   first predecessor matching the local durable anchor. Copy the pinned prefix into a private
   disposable directory. Validate historical schemas, policies and C.4 scopes, then execute every
   record with the sequential reference VM in that copy. Compare every generated canonical outcome
   byte-for-byte with the supplied outcome, including returned values, effects and abort details.
1. Only after every comparison succeeds, append the exact validated canonical bytes through normal
   versioned log publication. Then use the normal durable replay and installation path, publish a
   checkpoint and return the new H.2 watermark. No supplied effects are substituted for VM
   execution.

### Import isolation and temporary storage

The coordinator runs one complete import at a time. The persisted read-only role prohibits local
canonical submissions. Snapshots, feeds, cursor changes, backups and maintenance controls wait until
the import finishes. Previously captured snapshots keep their original views and existing cursors
keep their baselines. A malformed, gapped, wrong-anchor or divergent batch is rejected without
changing the live prefix, and the replica remains usable. An empty logical batch still checks its
database identity and exact local baseline.

Validation uses real copied files, not snapshots that share mutable storage. Temporary copies are
removed on normal completion, rejection and unwinding. A process crash can leave a `blop-import-*`
directory in the OS temporary directory; it is not authoritative and database recovery never opens
it. Such orphans may be removed after establishing that their process has stopped. Import currently
copies the whole selected physical prefix per batch, so it favours a simple isolation boundary over
large-database replication throughput. It needs sufficient temporary disk space and working file and
directory synchronization there as well as at the replica.

### Resume an interrupted import

The importer checks all outcomes before the first append, but each append publishes D separately. A
system error or crash can therefore leave a shorter, already verified durable prefix. System
failures stop the replica until reopen. `Error::Uncertain` and cancellation after enqueueing do not
mean that nothing was imported. Close and reopen, inspect the recovered snapshot watermark, and
request only the remaining source suffix. Do not retry the previous batch without checking that
position. Import has no pending-verification journal, multi-record group commit, or raw-segment
transport API.

## Derived SQLite

The [SQLite example](examples/derived_sqlite.rs) builds a persistent mirror from a resolved feed.
Initial creation requires source history from sequence zero. Use one consumer process per index and
keep the SQLite file and its journals together. Run:

```sh
cargo run --example derived_sqlite -- /path/to/source-db /path/to/index.sqlite
```

The example maintains a SQLite mirror of live source keys and values as opaque canonical blobs, a
typed table catalogue with names and liveness, and a deterministic secondary index on SHA256 of each
encoded value. Table IDs and source sequences use eight-byte big-endian blobs, avoiding SQLite's
signed integer limit. The hash is a lookup aid, not a substitute for comparing complete values. An
audit table retains the canonical outcome at every source record boundary, including aborts and
ignored policy events. This demonstration retains that audit history without compaction.

### Commit progress and resume safely

Each SQLite transaction applies complete source records and writes their final H.2 watermark blob in
the same durable commit. Only then does it acknowledge the source cursor. SQLite uses rollback
journalling and `synchronous = FULL`; keep its file and any journals together. On startup it reads
the committed watermark and token from SQLite, checks the source identity and conservative cursor
baseline, advances an older cursor and resumes after the committed position. An uncertain SQLite
commit requires reopening SQLite and inspecting that committed watermark, not repeating staged
effects based on a guessed commit result.

Missing history, a wrong database or cursor namespace, a cursor ahead of the derived watermark, and
an unsupported saved format are errors, not reasons to publish an empty mirror. This example
deliberately does not implement a snapshot rebuild. It uses one consumer process per index,
processes bounded batches until it reaches the current source frontier, and can be invoked again to
consume later records. An interrupted initial cursor checkout may leave an unused source
registration; use the cursor administration APIs to release it explicitly.

### Deduplicate business requests

Resuming a derived consumer safely does not deduplicate business requests. A retried `execute` still
enters the canonical log at a new sequence. To deduplicate a business operation, atomically check
its request ID, verify that a previously stored payload matches, and store its result alongside the
business writes. A business abort rolls back that request-ID write too; the engine does not
automatically cache the aborted request as completed.

## Maintenance

Call `maintain(&db, MaintenanceOptions::default()).await` to collect eligible MVCC versions and
outcomes, retire eligible whole log segments, compact all five trees, and seal the current log. No
canonical sequence is consumed. Maintenance pauses assignment, finishes every durably logged record,
and builds a checkpoint at `C = F = D`. Snapshot, cursor and other controls wait during the
synchronous handover. Already queued but unassigned transactions resume afterwards. Cancelling the
maintenance waiter after enqueue does not cancel the operation.

### Maintain a read-only replica

Read-only replicas permit local garbage collection (GC) and compaction. Read-only means no new
canonical submissions, not immutable files: G.5 permits a replica to apply its own local retention
policy. Maintenance can change local roots, file IDs and retained-history floors without advancing
the canonical sequence or changing its resolved state and hash-chain anchors. The original source
directory and its cursor registrations are not affected by maintenance or explicit cursor release on
the replica.

### Choose maintenance work

`MaintenanceOptions` has three independent switches: `collect_history`, `compact` and `rotate_log`.
Set only `rotate_log` to seal a segment without collecting history. The next canonical record
creates a new nonempty segment with a fresh, noncolliding ID; reopening also starts a new segment.
Empty log segments are never published. Existing canonical record bytes and hash-chain anchors do
not change. Set all switches to false to validate the checkpoint and retry obsolete-file cleanup
without GC or compaction. There is no automatic history policy or background maintenance scheduler
yet.

### Understand what history remains

GC selects `G = min(F, current unrevoked snapshot floors, durable cursor baselines)`. It retains
every version above G, including every installed version above F, and the newest version at or below
G per key, including tombstones. Outcomes above G and all version-1 catalogue and policy history
remain. Resolved cursors do not require source bytecode; logical and replica cursors additionally
pin log coverage from baseline + 1. Only whole segments below the safe log floor retire, so physical
log retention can be more conservative than the claim floor. Old cursor checkout reports
`HistoryUnavailable` after the published floors advance.

### Publish compacted storage

The coordinator validates the source and candidate history and checks correspondence with retained
canonical records before retiring sources. Compaction copies reachable nodes into a fresh page file,
rewriting child and overflow references without inserting each entry through the COW tree. It
flushes and publishes the new file, then adopts its roots as the live roots before resuming
assignment. Published page and file identities are not recycled. An uncertain publication failure
stops the coordinator and reclamation until reopen establishes the selected manifest.

### Release pins before reclaiming files

File deletion uses a shared directory lease: **any** storage view, snapshot read, idle unrevoked
snapshot, worker, checkpoint or backup pin prevents deletion of **all** obsolete page, log and
manifest files, even unrelated ones. Durable cursors constrain logical floors but do not hold
physical files open. Revoking or dropping snapshots and finishing backups releases physical pins;
run maintenance again to retry deletion. Current files are never deleted. There is no in-place page
reuse. GC without compaction can append unreachable COW pages rather than reduce disk usage.

### Inspect the maintenance report

`MaintenanceReport` reports the frontier, selected generation, old and new floors, removed version
and outcome counts, retired segment count, page file IDs and page counts, rotation state, and
deleted or deferred file counts and bytes. `status(&db)` exposes `maintenance_pending`, sampled
`backup_jobs` and the last successful report. These counts are not a total storage or memory budget:
validation uses history indexes, GC appends temporary COW paths, and direct compaction holds a
traversal stack and at most one decoded overflow value at a time.

## Backup and attach

**Use `open` for normal crash recovery. Every explicit `attach` changes the cursor namespace and
invalidates old tokens.** This also applies to directories that are already attached or lack an
`ATTACH_REQUIRED` marker.

Before calling `attach`, check that the path identifies the copy you intend to use. For a writable
restore, retire the source primary first. The library supports explicit filesystem copies and attach
retries, but cannot determine which copy you intended.

### Create a physical backup

Choose a destination directory that does not exist, then call `backup(&db, new_directory).await`.
Failure may leave an incomplete directory; a later backup will not overwrite it.

The coordinator captures and pins a durable physical prefix. A blocking worker copies it while the
source continues executing. The result is the exact copied `storage::Manifest`, whose C and D may be
behind the source by the time copying finishes.

The image contains exact GENESIS bytes, `page_count` complete pages, and the captured committed log
prefixes. Its manifest and CURRENT describe D at capture and the selected checkpoint roots, so they
can differ from the source's older CURRENT-selected metadata. Destination files and directory
entries are flushed before the matching CURRENT is selected last.

At most four backup workers run per database. Excess requests receive `OperationalLimit` for
`backup_jobs`.

Backup workers own their pins, not the waiting future. Cancellation after enqueue cannot release
files while I/O continues. Close joins active backup workers before releasing the source directory
lock. An idle result or cancelled waiter cannot hold the lock indefinitely, although slow or stalled
filesystem I/O can delay close. Destination I/O failures do not poison the source database.

### Attach the copy

Backup images contain a flushed implementation-local `ATTACH_REQUIRED` marker before `CURRENT` is
written. Ordinary `open` refuses these images. Choose an explicit attachment mode:

- `attach(path, AttachMode::ReadOnlyReplica).await` preserves the canonical database identity but
  rejects transaction, catalogue and policy submissions. Local cursor operations, snapshots,
  maintenance and backups remain available. The local `READ_ONLY` marker preserves this role on
  normal reopen. Verified logical import is permitted only in this role.
- `attach(path, AttachMode::RestorePrimarySourceRetired).await` makes the explicit caller assertion
  that the source primary has been retired and permits canonical submissions. The library cannot
  fence a primary on another machine. Never run independent writable primaries with the same GENESIS
  identity. Promoting a read-only copy requires this same explicit choice.
- `attach_with_options(path, mode, engine_options).await` selects local worker and capacity
  settings.

Every explicit attach durably publishes a fresh random cursor namespace through the normal G.3
publication protocol before enabling APIs. It preserves GENESIS, copied registrations, baselines and
next-ID counters. It removes the attach-required marker only after namespace and role publication.
An interrupted attach can be retried explicitly; an extra namespace renewal is safe. Ordinary reopen
preserves the attached namespace and role. Arbitrary external filesystem copies cannot be recognised
automatically: attach them explicitly rather than using normal reopen, and do not remove or bypass
the implementation-local role and attachment markers.

### Rebind a copied cursor

Old tokens are invalid, even when the copied counter later issues the same numeric ID. After the
consumer has selected the correct copied registration and validated its saved watermark, use
`rebind_cursor(&db, existing_numeric_id, watermark).await` to obtain a token in the new namespace.
Rebind checks database identity, registration presence, the protected interval `[baseline, F]` and
required retained history. It does not select by label or old token, advance the baseline, or clear
registrations. Labels are nonunique diagnostics, and tokens are not authentication credentials. The
caller must validate that its derived data actually corresponds to the supplied watermark.

## Transaction construction

The `tx!` domain-specific language (DSL) describes the work the database will perform. Declare
external inputs in `captures`, declare table IDs and schemas in `tables`, then write the program
body.

This example constructs a transfer program. Submit it with `database::execute` to run it against a
database whose balances table matches the declared schema.

```rust
use blop_db::tx;

let from_id = 10_u64;
let to_id = 20_u64;
let amount = 25_i64;
let balances_id = 3_u64;

let transaction = tx! {
    captures {
        from: u64 = from_id,
        to: u64 = to_id,
        amount: i64 = amount,
    }
    tables {
        balances: u64 => i64 = balances_id,
    }
    -> i64 {
        require(amount > 0, 1);
        if from == to {
            abort(2);
        }
        require(balances[$from] >= amount, 3);
        balances[from] -= amount;
        balances[to] += amount;
        return balances[from];
    }
}?;

assert!(transaction.program_bytes().starts_with(b"BLOPVM01"));
assert_eq!(&transaction.argument_bytes()[..4], &3_u32.to_le_bytes());
# Ok::<(), blop_db::BuildError>(())
```

### Inputs and output

The macro returns `Result<Transaction, BuildError>`. Handle a binding error before submitting the
transaction. `Transaction::program_bytes()` contains the complete `BLOPVM01` container.
`Transaction::argument_bytes()` contains the separate ISA 1 Arguments encoding: a count followed by
one length-prefixed value per capture. `into_parts()` returns both owned byte vectors, program
first.

- `captures { name: type = rust_expression, ... }` declares external values. Initializers are
  ordinary Rust expressions, evaluated once in declaration order and borrowed rather than implicitly
  moved. Encoding copies their values into the transaction. Later changes to the originals do not
  affect it.
- A capture can be read as `name` or `$name`. The latter always refers to the capture, even when a
  local shadows its name. Captures are immutable. Repeated uses share an argument slot.
- `tables { name: key_type => value_type = rust_expression, ... }` declares table schemas and
  runtime `u64` IDs. IDs are evaluated once, after the capture initializers. Binding sorts the table
  array and remaps all table operands. Zero, `u64::MAX` and duplicate IDs are binding errors. Use
  one declaration repeatedly to access the same table.
- The optional `-> type` declares the result. Without it, explicit returns determine the result
  shape and the union of their bounds. Programs that can fall through must return Unit. Falling
  through emits a Unit `RETURN`; no path falls off the instruction array.
- The capture and table sections are optional. When both are present, captures precede tables. The
  program body may be enclosed in braces, as above, or follow the headers directly.

Changing capture values does not change the program bytes. Changing table IDs changes only the table
array and its instruction references. Rust functions may produce captures, but they never become VM
callbacks.

These two byte vectors are not the complete logged Transaction body. The reference VM independently
validates bytecode, actual catalogue schemas and explicit resource claims before interpreting them.
The macro alone cannot check whether a runtime table ID names a live table. The async writer adds
conservative access manifests, complete log bodies and durable sequencing; the macro alone does not
perform these steps.

### Types

| DSL type        | Rust capture value                      | Meaning                                          |
| --------------- | --------------------------------------- | ------------------------------------------------ |
| `()`            | `()`                                    | Unit.                                            |
| `bool`          | `bool`                                  | Boolean.                                         |
| `i64`, `u64`    | The corresponding Rust integer type.    | Checked 64-bit integers.                         |
| `bytes<N>`      | A value implementing `AsRef<[u8]>`.     | At most N bytes.                                 |
| `string<N>`     | A value implementing `AsRef<str>`.      | At most N UTF-8 bytes, not characters.           |
| `(T, U, ...)`   | A Rust tuple with exactly these fields. | Positional tuple; use `(T,)` for one field.      |
| `tuple<>`       | `()`                                    | Empty Tuple, distinct from Unit in the bytecode. |
| `rows<K, V, N>` | Not allowed as a capture.               | At most N rows; register or result type only.    |

Write bounds as integer literals. ISA 1 allows at most 16 nesting levels, 256 fields per tuple, and
65,535 rows. Encoded values must fit 16 MiB, and canonical table keys must fit 1,024 bytes. Rows
cannot be nested, used in table schemas, or supplied as captures.

If a captured value exceeds its bound, or the complete Arguments encoding exceeds 16 MiB, binding
returns `BuildError`.

Ordinary integer literals default to I64; a typed context can select U64. Use `i64` or `u64`
suffixes when needed. Other numeric types, floating point and implicit integer conversions are not
supported. String and byte-string literals use their actual lengths as bounds. Local annotations can
declare larger or smaller bounds, such as `let mut text: string<128> = "";`. The VM checks
destination bounds when copying or producing a value; equal shapes need not have equal bounds.

### Statements

| Syntax                                                        | Meaning                                                                                      |
| ------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| `let x = expression;`                                         | Initialize an immutable local.                                                               |
| `let mut x: type = expression;`                               | Initialize a mutable local with an optional type annotation.                                 |
| `x = expression;`                                             | Copy a new value into a mutable local.                                                       |
| `table[key] = expression;`                                    | Unconditional STORE, without loading the previous value.                                     |
| `x += value;`, `table[key] -= value;`                         | Read, compute and write. All supported arithmetic and bitwise operators have compound forms. |
| `if condition { ... } else if condition { ... } else { ... }` | Forward-only branches. `else` is optional.                                                   |
| `require(condition);`, `require(condition, code);`            | REQUIRE, with a literal u32 user code, default zero.                                         |
| `abort;`, `abort();`, `abort(code);`                          | ABORT, with a literal u32 user code, default zero.                                           |
| `return expression;`, `return;`                               | RETURN a value or Unit.                                                                      |
| `insert(table[key], value);`                                  | INSERT, aborting if the key exists.                                                          |
| `store(table[key], value);`                                   | Unconditional STORE.                                                                         |
| `delete(table[key]);`                                         | Unconditional DELETE.                                                                        |

Blocks have lexical scope and allow shadowing. Locals must have initializers and keep one declared
type. `if` is a statement, not a value-producing expression. Every explicit return must have the
same shape; ABORT can terminate a path for any result type. Unreachable statements are compile
errors. Compound table assignments evaluate the key once and load the prior value before the right
operand.

### Expressions

The DSL uses Rust operator precedence and left-to-right operand evaluation. These expressions cover
every ISA 1 instruction family. ARG and CONST come from captures and literals; structured control
flow provides JUMP_FORWARD and JUMP_IF_FALSE_FORWARD.

| Syntax or intrinsic                                                  | ISA operation                                                                                                                              |
| -------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| Literals, capture names, `copy(x)`                                   | CONST, ARG, MOVE.                                                                                                                          |
| `a + b`, `a - b`, `a * b`, `a / b`, `a % b`, `-a`                    | Checked arithmetic. Named forms: `add_checked`, `sub_checked`, `mul_checked`, `div_checked`, `rem_checked`, `neg_checked`.                 |
| `to_i64_checked(x)`, `to_u64_checked(x)`                             | Explicit checked integer conversions.                                                                                                      |
| `==`, `<`, `<=`, `>`, `>=`                                           | EQ, LT, LE, GT, GE. Named forms: `eq`, `lt`, `le`, `gt`, `ge`.                                                                             |
| `a != b`                                                             | EQ followed by BOOL_NOT.                                                                                                                   |
| `a && b`, `a \|\| b`                                                 | Short-circuit Boolean control flow. The skipped operand cannot load data or abort.                                                         |
| `bool_and(a, b)`, `bool_or(a, b)`, `bool_xor(a, b)`, `bool_not(a)`   | Eager BOOL_AND, BOOL_OR, BOOL_XOR, BOOL_NOT.                                                                                               |
| `&`, `\|`, `^`, `!`                                                  | Boolean operations on booleans; bitwise operations on equal integer types. Named integer forms: `bit_and`, `bit_or`, `bit_xor`, `bit_not`. |
| `a << b`, `a >> b`, `shl_wrap(a, b)`, `shr(a, b)`                    | SHL_WRAP and SHR. The shift count is U64.                                                                                                  |
| `table[key]`, `load(table[key])`, `exists(table[key])`               | LOAD and EXISTS. Keys may be computed.                                                                                                     |
| `rows_len(rows)`, `rows_key(rows, index)`, `rows_value(rows, index)` | Row-set access; indices are U64.                                                                                                           |
| `byte_len(x)`, `concat(a, b)`                                        | BYTE_LEN and CONCAT for Bytes or String.                                                                                                   |
| `slice_bytes(bytes, start, length)`                                  | SLICE_BYTES; start and length are U64.                                                                                                     |
| `utf8_bytes(text)`, `parse_utf8(bytes)`, `sha256(bytes)`             | UTF8_BYTES, PARSE_UTF8 and SHA256.                                                                                                         |
| `(a, b)`, `tuple(a, b)`, `tuple()`                                   | TUPLE construction, including the empty Tuple.                                                                                             |
| `value.0`, `field(value, 0)`                                         | FIELD with a statically checked index.                                                                                                     |

Arithmetic is not evaluated by Rust. Checked arithmetic failures, invalid shifts, missing LOAD keys,
failed REQUIRE conditions, invalid UTF-8 and other ISA-defined failures remain runtime VM aborts.
Rows supports neither comparison nor equality.

#### Bounded scans

`scan_bounded(table, lower, upper, flags, row_limit, byte_limit)` emits SCAN_BOUNDED. An endpoint is
a key expression or `unbounded`. The last three operands are unsigned integer literals:

- `flags`: bit 0 includes a present lower endpoint; bit 1 includes a present upper endpoint. Other
  bits are forbidden. An unbounded endpoint must have its inclusion bit clear.
- `row_limit`: from zero through 65,535.
- `byte_limit`: from zero through 64 MiB.

The result descriptor uses the table's key and value types and `row_limit` as its row-count bound.
Its maximum encoded size must still fit 16 MiB. The VM must enforce the scan limits and transaction
claims.

```rust
let transaction = blop_db::tx! {
    tables { balances: u64 => i64 = 3 }
    let rows = scan_bounded(balances, 10, 20, 1, 100, 4096);
    if rows_len(rows) == 0 {
        abort(4);
    }
    return (rows_key(rows, 0), rows_value(rows, 0));
}?;
# Ok::<(), blop_db::BuildError>(())
```

### Compile-time errors

Declare external names as captures. The compiler reports unsupported syntax at its source location.
This includes loops, arbitrary Rust calls, items, macros, attributes, closures, recursion, and
casts. The reserved VM keywords `abort` and `unbounded` cannot name captures, tables, or locals.

```compile_fail
let external = 5_i64;
let _ = blop_db::tx! { return external; };
```

```compile_fail
let _ = blop_db::tx! { loop { require(true); } };
```

Rust also checks capture representations. Tuple fields cannot be silently omitted, and integer
captures do not silently narrow or convert signedness.

```compile_fail
let _ = blop_db::tx! { captures { pair: (i64,) = (1_i64, 2_i64) } return pair; };
```

```compile_fail
let value = 1_u64;
let _ = blop_db::tx! { captures { value: i64 = value } return value; };
```

## Reference execution

Use `blop_db::vm` to compare engine behaviour with a single-threaded interpreter of all 48 ISA 1
opcodes. Its reader validates the entire program and converts bytecode to typed Rust instructions.
It checks operand layouts, types, forward branches, reachability, and register initialization on
every path. The runtime executes those validated instructions.

**Reference execution does not provide a durability receipt.** In a running database, its caller
must establish log durability before execution and protect the required history. Use an isolated
reference store for experiments, or the public database API for durable application work.

- `interpret` accepts program and argument bytes, a pinned storage view, a record sequence and
  explicit claims. It returns an `Outcome` without changing storage.
- `execute` accepts a bound `Transaction`. `execute_bytes` accepts its saved byte vectors directly,
  including for reference replay. Both interpret the program and install its final versions and
  canonical outcome in one storage batch.
- `execute_catalogue` creates, renames or drops tables. Creation uses the record sequence as the
  table ID. `execute_limits` replaces the policy for later records. These operations share the
  transaction sequence space and remain available even when the policy disables transactions.
- Executing APIs require consecutive records after the checkpoint. Rejected input does not install
  an outcome or consume that position. A semantic abort installs an outcome but no data versions, so
  the following record can proceed. Already resolved or checkpointed positions cannot be
  overwritten.

Reads use the catalogue, policy and MVCC state strictly below the transaction sequence. Private
writes take precedence, including tombstones. Bounded scans merge this overlay before selecting rows
in canonical key order. Register, range and overlay charges follow the logical quantities and
failure order in the design, rather than allocation sizes or physical version counts. Final effects
are sorted by table ID and canonical key, with one effect per written address.

`Outcome::Success` contains the result descriptor, decoded `Value` and final effects.
`Outcome::Aborted` contains the stable reason, instruction index, user code and resource detail.
Submission errors and storage failures are separate `Err` values, never semantic aborts.

The following example uses an **isolated reference store**, explicit resource limits and fixture
record digests. It does not create a durable transaction log:

```rust
# #[cfg(any(unix, windows))]
# fn main() -> Result<(), Box<dyn std::error::Error>> {
use blop_db::{storage, tx, vm};
use storage::{Genesis, LimitPolicy};
use vm::{CatalogueOperation, Outcome, Type, Value};

# let temporary = tempfile::tempdir()?;
# let path = temporary.path().join("reference");
// Resource IDs 1 through 17, in design appendix D.2 order.
let claims = LimitPolicy::new([
    1_048_576, 1024, 1024, 64, 1_048_576, 64, 0, 1024, 1024, 1024,
    1_048_576, 8_388_608, 1024, 8_388_608, 1024, 8_388_608, 1_048_576,
])?;
let mut store = storage::create(path, Genesis {
    database_id: [1; 16],
    initial_policy: claims.clone(),
}, [2; 16])?;

vm::execute_catalogue(&mut store, 1, [1; 32], &CatalogueOperation::Create {
    name: "counters".into(),
    key: Type::U64,
    value: Type::I64,
})?;
let transaction = tx! {
    tables { counters: u64 => i64 = 1 }
    insert(counters[7], 40);
    counters[7] += 2;
    return counters[7];
}?;
let outcome = vm::execute(&mut store, 2, [2; 32], &transaction, &claims)?;
assert!(matches!(outcome, Outcome::Success { value: Value::I64(42), .. }));
# Ok(())
# }
# #[cfg(not(any(unix, windows)))]
# fn main() {}
```

**Execution is not a durability receipt.** In a running database the caller must establish log
durability before executing, supply the digest of the complete canonical record, protect the prior
history and publish only a fully resolved checkpoint prefix. The reference APIs do not verify that
digest against a log, append log records, advance public visibility or orchestrate recovery.
Reopening storage restores checkpoint roots; replay must execute every subsequent durable record
against those roots, rather than reuse later cached outcomes.

The original byte-only reference execution APIs do not charge resource 7 because they have no
manifest. Use the preparation API below for complete C.4 admission. Full logged Transaction bodies
remain the database record layer's responsibility. The production scheduler uses prepared
interpretation and serial atomic installation rather than the serial `vm::execute` helper.

### Prepare access declarations

Call `vm::prepare_transaction(&view, sequence, &transaction, &claims, supplied_manifest)` to
validate a typed program and its access scopes. It returns a `PreparedTransaction` whose fields
callers cannot change. Preparation reads historical catalogue and policy metadata, but does not read
database rows.

Pass `None` to derive declarations, or `Some(&manifest)` to verify and retain supplied declarations.
A broader supplied manifest can use fewer entries than separately derived points and therefore fit a
smaller scope-count budget. Prepare again if an administrative barrier changes the catalogue or
policy before sequencing.

- `prepared.manifest()` exposes the verified `AccessManifest`; `tables()`, `sequence()` and
  `instruction_count()` expose preparation metadata.
- `vm::interpret_prepared(&view, &prepared)` reuses the typed program without decoding it again. It
  returns an outcome without installing effects. The caller must protect the prior view and
  establish durability and resolved dependencies, or use an isolated reference store.
- `AccessManifest::new` accepts `(Scope, AccessMode)` pairs and normalizes their order, aliases and
  per-mode table suppression. `entries()`, `reads()` and `writes()` expose the normalized
  declarations.
- `AccessManifest::decode` rejects noncanonical wire declarations instead of silently normalizing
  them. `encode` preserves their exact canonical encoding. Historical key-schema and coverage checks
  happen during preparation.
- `vm::overlap(&earlier_write, &later_read)` implements the section 8 address overlap rules,
  including absent keys. It does not introduce read/write or write/write scheduling dependencies on
  its own.

```rust
use blop_db::vm::{AccessManifest, AccessMode, Scope};

let manifest = AccessManifest::new([
    (Scope::Table(3), AccessMode::Write),
    (Scope::Key(3, 7_u64.to_be_bytes().to_vec()), AccessMode::Read),
])?;
assert_eq!(manifest.len(), 2);
assert_eq!(AccessManifest::decode(&manifest.encode()?)?, manifest);
# Ok::<(), blop_db::vm::Error>(())
```

Abstract evaluation reuses the VM's pure operations, without application callbacks. ARG and CONST
seed known values. Pure operations retain known results only when intrinsic computation and
destination bounds succeed. LOAD, EXISTS and scans produce Unknown, even after overlay writes or for
predictably empty scans. Joins retain only values that agree on every predecessor. Predictable
failures do not reject the transaction or prune later access declarations; execution still
determines the first semantic abort and applies the original runtime resource charges.

## Storage

Use `blop_db::storage` when implementing or testing the engine's physical storage. It implements the
page and metadata formats in design appendices F and G, key and MVCC encodings in appendix B, and
WAL framing in appendices E and I. Application writes should use `blop_db::database`.

- Append-only, copy-on-write B+ trees use 16 KiB checksummed pages and all five system-tree IDs.
- Point lookups and lazy ordered scans operate on immutable, pinned roots. Inserts, replacements and
  physical deletions copy changed paths, including splits and root collapse.
- Values through 1,024 bytes are inline. Larger values use validated overflow chains, up to the
  physical 128 MiB ceiling.
- `apply` installs a complete physical batch against the latest roots. Include a transaction's final
  state versions and its complete outcome together. The batch does not advance visibility or make
  anything durable.
- `view` pins the latest installed roots. `checkpoint_view` pins the published checkpoint roots.
  Neither is an externally admitted snapshot or proof of a resolved log prefix.
- `prepare_checkpoint` constructs separate roots that exclude logical versions above a selected
  sequence, without changing live roots. It conservatively retains all older history.
- `publish` flushes referenced files, writes an immutable manifest and atomically replaces
  `CURRENT`. It rejects stale metadata, decreasing frontiers, changed history anchors and stale
  cursor roots.
- `open` restores the selected checkpoint, validates reachable pages and committed log envelopes,
  and discovers and flushes later complete WAL groups. Its default policy discards malformed
  suffixes outside selected durable bounds. `open_with_recovery(path, TailRecovery::Strict)` selects
  strict tail validation. Corrupt selected data is always an error; recovery does not fall back to
  an older manifest.

### Create an isolated store

Creation requires a **new directory**, an explicit initial limit policy, a unique nonzero database
ID and a unique nonzero cursor namespace. The caller selects the identities outside transaction
code.

```rust
# #[cfg(any(unix, windows))]
# fn main() -> Result<(), Box<dyn std::error::Error>> {
use blop_db::storage::{self, Genesis, LimitPolicy, TreeId};

# let temporary = tempfile::tempdir()?;
# let path = temporary.path().join("database");
# let database_id = [1_u8; 16];
# let cursor_namespace = [2_u8; 16];
// Zero is a valid policy limit. Supply the intended 17 resource limits at creation.
let genesis = Genesis {
    database_id,
    initial_policy: LimitPolicy::new([0; 17])?,
};
let store = storage::create(&path, genesis, cursor_namespace)?;
let checkpoint = storage::checkpoint_view(&store);
let initial_policy = storage::get(&checkpoint, TreeId::Policy, &0_u64.to_be_bytes())?;
assert_eq!(initial_policy, Some(store.genesis().initial_policy.encode()));

drop(checkpoint);
drop(store);
let reopened = storage::open(&path)?;
assert_eq!(reopened.manifest().checkpoint_sequence, 0);
# Ok::<(), Box<dyn std::error::Error>>(())
# }
# #[cfg(not(any(unix, windows)))]
# fn main() {}
```

### Encode keys and publish state

`storage::encoding::Schema` validates non-Rows schema descriptors. `encode_key` and `decode_key`
convert between schema value bytes and canonical ordered keys. `storage::mvcc` provides validated
`StateKey` and `StateValue` framing, sequence-bounded point reads and logical range scans. A
tombstone stops lookup; it never falls through to an older value. Physical `Mutation::value = None`
instead removes an entry and is intended for storage maintenance, not a transaction DELETE.

The reference VM supplies schema-checked state versions and complete catalogue and outcome values.
Callers using physical storage directly remain responsible for those checks. The engine must also
interpret log bodies, select a contiguous resolved checkpoint prefix and protect retention claims.
`publish` takes a pinned checkpoint view and a clone of the **latest** `Store::manifest()`, with the
engine's frontier, log descriptor and next-ID updates. Storage manages page identity, roots, page
count and manifest generation. Newly durable log prefixes must extend the previously published
hash-chain anchor.

### Recover the database above the storage layer

When `durable_sequence > checkpoint_sequence`, `storage::open` leaves the durable suffix unexecuted.
The higher-level `database::open` reads and validates that suffix and passes every record to the
reference execution layer before accepting new submissions. The storage layer itself provides no
scheduler, logical log writer or recovery coordinator. The database layer provides cursor lifecycle
APIs, resolved and logical feeds, verified replica import, retention-aware GC, whole-file
compaction, log rotation and pinned backup with explicit attachment. Obsolete files stay retained
until durable selection and physical pin retirement.

Database recovery also validates retained logical checkpoint history before accepting work, even
when there is no replay suffix. It checks catalogue lifecycles and immutable schemas, historical row
liveness and encodings, complete outcome framing, required outcome coverage, and agreement with
exact retained versions and available canonical records. Optional outcomes below the history floor
may outlive superseded state versions, but cannot contradict versions that remain. Missing required
history is corruption, not an empty feed or permission to initialize replacement metadata.

### Platform support

The storage module builds on Unix and Windows. A small internal platform module handles positional
I/O, file and directory synchronization, and same-directory file replacement. Page formats and the
publication sequence are unchanged. An OS lock protects the directory until the store and all views
and scans have been dropped.

**Windows support is experimental and has not been runtime-tested.** No Windows machine was
available. The production libraries have been checked from Linux with
`cargo check --workspace --lib --target x86_64-pc-windows-gnu`. The current all-target cross-check
is blocked by the missing `x86_64-w64-mingw32-gcc` compiler needed by the bundled SQLite benchmark
dependency. A cross-check does not test linking, execution, filesystem behaviour or crash
durability. Tests have run on Linux only; other Unix platforms have not been runtime-tested either.
Do not rely on the Windows backend for important data until its filesystem and recovery behaviour
has been tested on Windows.

The Windows backend uses synchronous offset I/O, writable directory handles opened with
`FILE_FLAG_BACKUP_SEMANTICS`, and `MoveFileExW` with `MOVEFILE_REPLACE_EXISTING` and
`MOVEFILE_WRITE_THROUGH`. It does not delete the destination first or enable a cross-volume copy
fallback. Windows directory-flush support and permissions remain unverified. A filesystem or account
that cannot perform the required directory flushes will cause creation or publication to fail; the
implementation never treats those flushes as optional or substitutes a no-op.

Durable storage still requires the filesystem guarantees in design section G.3: atomic
same-directory replacement and durable file and directory synchronization. Write-through flags alone
are not proof of those guarantees. After a publication I/O error, the store rejects further
mutations with `NeedsRecovery`; drop its handles and reopen to establish which publication survived.

## Benchmarks

Run the KV comparison against redb and SQLite with
`cargo run --release --example kv_bench -- --dir /path/to/existing/directory --mode durable --clients 4 --workers 4`.
The durable workload uses independent one-key transactions and public snapshot reads. See
[BENCHMARKS.md](BENCHMARKS.md) for single-client controls, concurrency results, the separate
buffered reference-VM diagnostic and comparison limits. The harness uses temporary databases, not
existing application data.

## Development

Run these checks from the repository root:

```sh
cargo test --workspace
cargo +nightly fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
mdformat --wrap 100 --check README.md DESIGN.md CONFORMANCE.md BENCHMARKS.md
```

The [conformance guide](CONFORMANCE.md) maps the implementation to its tests, explains the fault
coverage, and gives Windows cross-check instructions. Run the relevant tests when changing a
feature, and update its documentation in the same change.

### Maintain plain language documentation

Apply ISO 24495-1:2023 when editing documentation, including Rust documentation comments:

1. Identify the reader and the task. The README helps application developers use the API; the design
   helps implementers preserve semantics and binary compatibility; the conformance guide helps
   contributors verify changes; the benchmark report helps readers reproduce and interpret results.
1. Put the main result, prerequisites, and required actions where readers can find them. Use task
   headings and present procedures in execution order. Keep reference details near their
   definitions.
1. Use consistent terms, explain unfamiliar abbreviations, and write direct sentences. Preserve
   exact API names, binary values, normative requirements, and measurement conditions.
1. Check examples, links, and rendered structure during revision. When practical, ask an intended
   reader to complete a task without extra guidance and record where they get stuck. Revise those
   passages.

Recheck documentation when APIs, formats, or measured implementations change, and before a release.
Use reader questions and task failures to identify what needs revision. Formatting and test success
support this review; they do not establish reader usability on their own.
