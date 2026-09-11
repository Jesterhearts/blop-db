# Design conformance

Use this guide to locate the implementation and tests for each part of the
[design specification](DESIGN.md). It is for contributors reviewing a change or checking which
behaviours have been tested. The design defines the required transaction semantics and version 1
storage bytes.

The tests provide evidence for specific behaviours and failure cases. They do not certify every
filesystem, hardware failure mode, or possible execution. Read the
[qualification limits](#qualification-limits) before applying their results to a deployment.

## Find a check

- [Run verification commands](#run-verification-commands).
- [Check transactions and access declarations](#transactions-and-access-declarations).
- [Check scheduling and resource limits](#scheduling-and-resource-limits).
- [Check storage and recovery](#storage-and-recovery).
- [Check snapshots, feeds, and replication](#snapshots-feeds-and-replication).
- [Check maintenance and backups](#maintenance-and-backups).
- [Check retries and derived consumers](#retries-and-derived-consumers).
- [Review implementation choices](#implementation-choices).

The [README glossary](README.md#terms-used-in-this-guide) defines the terms used here. In
particular, C is the checkpoint sequence, F is the visible frontier, and D is the durable log
frontier.

## Run verification commands

Run these commands from the repository root:

```sh
cargo test --workspace
cargo +nightly fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
mdformat --wrap 100 --check README.md DESIGN.md CONFORMANCE.md BENCHMARKS.md
tombi lint Cargo.toml crates/blop-db-macros/Cargo.toml rustfmt.toml
```

The workspace tests include the test-enabled benchmark and SQLite examples. Documentation examples
in the README are also crate documentation tests.

To type-check the Windows libraries from Linux, install the target and run the library check:

```sh
rustup target add x86_64-pc-windows-gnu
cargo check --workspace --lib --target x86_64-pc-windows-gnu
```

An all-target Windows check also needs a MinGW C compiler for bundled SQLite. Cross-compilation does
not test Windows execution or durability.

## Transactions and access declarations

### Compiler, types, and bytecode

Design references: sections 4 through 6 and appendices A through C.

Implementation: `crates/blop-db-macros`, `src/bind.rs`, `src/vm/program.rs`, and `src/vm/value.rs`.

Tests check:

- canonical encodings against fixed expected bytes, including the exact H.3 program and its smallest
  sufficient resource claims;
- all 48 ISA 1 opcodes, invalid types and control flow, and register initialization on every path;
- rejection of malformed bytecode, arguments, descriptors, manifests, and outcomes.

`tests/conformance.rs` includes 57,344 reproducible mutated inputs. This is a bounded regression
corpus, not a continuously running coverage-guided fuzzer.

### Read and write scopes

Design references: section 8 and C.4.

Implementation: `src/vm/access.rs`, `src/vm/access_analysis.rs`, and `src/database/record.rs`.

Tests check canonical normalization, aliases, empty encoded point keys, and separate read and write
modes. They also check joins where a value is known only if all incoming paths agree, predictable
runtime failures, historical schemas, broader supplied manifests, and rejection before sequence
allocation. Resource 7 uses the actual normalized entry count, including after replay.

### Execution, rollback, and logical limits

Design references: sections 10, 11, 18, and 19; D.2 and D.3.

Implementation: `src/vm/runtime.rs`, `src/vm/operations.rs`, `src/vm/database.rs`, and
`src/vm/outcome.rs`.

Tests run compiled programs through real storage and compare complete outcomes. They cover rollback,
reads of a transaction's own writes, historical schemas and policies, and scans merged with private
writes. They check the required order of resource failures and repeat execution from saved bytes.
Equivalent logical views must produce the same results despite different physical histories or later
invisible versions.

## Scheduling and resource limits

Design references: sections 7, 9, 12, and 22.

Implementation: `src/database/engine.rs`, `src/database/scheduler.rs`, `src/database/workers.rs`,
and `src/database/budget.rs`.

Tests pause real production workers at controlled points to check:

- independent progress and completion in a different order from sequencing;
- dependencies on every relevant earlier possible writer, including aborted writers and successful
  branches that omit writes;
- point, table-wide, and scan dependencies, including reads of resolved versions above F;
- execution-window bounds, queue and backlog limits, and count and byte admission waits;
- catalogue and policy barriers, including submissions queued before a metadata change;
- cursor publication while `C < F < D`;
- cancellation, worker panics, I/O errors, and sequential recovery with smaller process capacities.

Seeded mixed logs compare receipts, stored outcomes, catalogue and policy history, and final version
trees with serial reference execution. Tests also compare public snapshots at sampled visible
prefixes. Small logs enumerate every legal dependency schedule and compare the resulting outcomes
and prefix states. These checks cover bounded cases, not all possible programs.

### Shutdown and diagnostics

Design references: sections 13 and 25.

`EngineStatus`, cursor listings, and maintenance reports expose frontiers, reservations, backlog,
workers, the oldest unresolved record, barriers, retention floors, backup activity, and system
errors.

Lifecycle tests verify that close waits for workers and their dependents, and that dropping all
handles drains assigned and queued work. They check snapshot revocation before directory unlock,
repeated-close behaviour, and definite rejection of unread controls during shutdown.

A checkpoint-failure test blocks unpublished temporary output after durable logging and
installation. It then verifies F/C/D diagnostics and replay. Deferred-checkpoint tests cover
interval thresholds, interval 1, old snapshots, final checkpoint failures, and process exit after
successful receipts. Reopening must preserve exact outcomes, increments applied once, and cursor
baselines above C. Both feed kinds must remain continuous.

## Storage and recovery

### Physical storage

Design references: section 17 and appendices F and G.

Implementation: `src/storage/page.rs`, `src/storage/tree.rs`, `src/storage/mvcc.rs`, and
`src/storage/platform.rs`.

Tests check slotted pages, overflow boundaries, tree shape, unique page ownership, and randomized
tree edits against a reference model. They also check immutable roots, ordered point and range
reads, malformed and truncated objects, multi-version filtering, and native cross-process locking.

The directory lease explicitly unlocks when its last registered owner retires. A descriptor briefly
inherited while creating a subprocess therefore cannot extend the lock beyond close. Real views,
scans, workers, and backups continue to hold the lease until safe release.

### Checkpoint history

Design references: sections 14, 15, and 27; appendices D through G.

Implementation: `src/database/engine.rs`, `src/vm/history.rs`, `src/storage/store.rs`, and
`src/storage/metadata.rs`.

Tests verify that recovery replays every durable record after C, including records with cached
outcomes. They reject invalid logical checkpoint history even when physical checksums are valid.
They check exact agreement between outcomes and retained versions, retained record bodies, required
outcome coverage, and unsupported versions. Missing required history is an error.

### WAL publication and fault injection

Design reference: appendix I.

Implementation: `src/storage/wal.rs`, `src/storage/store/wal.rs`, and the `CURRENT` codec in
`src/storage/metadata.rs`.

Ordinary WAL groups use one file flush. Recovery discovers linked complete groups and flushes the
recovered suffix before replay. The engine, logical readers, backups, and maintenance use the same
segment framing. Canonical record and exchange encodings retain their own tests.

The fault matrix injects errors before and after file flushes, directory flushes, and renames for
new and extended segments. It also covers partial writes, incomplete metadata, and truncation of
committed prefixes. WAL-specific checks cover:

- errors and partial writes at every group-append I/O boundary;
- native child-process exit at each append boundary;
- truncation at every byte position in a group;
- complete damaged groups, forks, unsupported versions, and invalid group bounds;
- checkpoints inside a group and backups whose live D exceeds the selected manifest's D;
- cache bounds, pinned-prefix rejection, incremental anchors, and invalidation of root proofs;
- agreement between incremental and full checkpoint validation.

All fixtures use disposable directories. Full validation and reopen still reject corrupt pages after
runtime caches have been warmed.

## Snapshots, feeds, and replication

### Snapshots

Design reference: section 13. Implementation: `src/database/snapshot.rs`.

Tests preserve values and catalogue metadata at the captured sequence. They check point-read and
iterator revocation, in-flight read protection, and close with idle handles. An iterator must report
revocation as an error, including after it previously reached the end.

### Cursors and feeds

Design references: section 16, G.4, H.1, and H.2.

Implementation: `src/database/cursor.rs`, `src/database/feed.rs`, and `src/database/exchange.rs`.

Tests cover durable checkout, acknowledgement, release, restart, namespace checks, issued-ID checks,
and atomic snapshot-plus-tail capture. Feeds must preserve exact historical effects, complete
bounded records, and consecutive progress without implicitly acknowledging a cursor.

### Logical replication

Design references: section 16.2 and H.1. Implementation: `src/database/replica.rs`.

Tests compare every source record and outcome with isolated reference execution before any incoming
durable append. They compare typed state at each imported prefix and preserve canonical bytes across
different local segment layouts.

Cases include computed scopes, aborts, catalogue changes, policy changes, retained snapshots, and
cursor-protected garbage collection. Invalid chains with recomputed checksums and digests, wrong
anchors, and divergent outcomes must be rejected before publication.

Per-replica controls test shared admission budgets and deferred requests. Injected errors and
process exits cover interruption before validation, after comparison, during multi-record
publication, and before live replay. Recovery must select valid metadata and recover only an already
verified prefix.

## Maintenance and backups

Design references: section 15, F.6, and G.3 through G.5.

Implementation: `src/storage/maintenance.rs`, `src/storage/backup.rs`, and
`src/database/maintenance.rs`.

Tests retain baseline versions, tombstones, all above-frontier versions, catalogue history, and
policy history. They cover snapshots and durable cursors across collection and log rotation,
collection without compaction, exact feeds after source-log deletion, and recovery.

Compaction tests check direct copying of reachable nodes and rewriting of internal and overflow
references. Controlled workers verify that maintenance drains above-frontier installations, adopts
the new live roots, and allows later writes. File pins must defer obsolete-file deletion.

Backup tests copy an exact earlier `C < D` image while the source writes and compacts, then replay
its suffix. Attachment tests reject old namespace tokens despite numeric-ID reuse, validate cursor
rebinding, and preserve read-only roles. Close must join backup workers even if their callers
cancel. Publication-failure tests check old-or-new `CURRENT` selection and prevent reclamation after
an uncertain handover.

## Retries and derived consumers

Design references: sections 13.3 and 24; H.2.

Implementation and tests: `tests/conformance.rs` and `examples/derived_sqlite.rs`.

Business-request tests combine request-ID checks and business writes in one transaction. They cover
payload mismatches and retries after aborted attempts.

The SQLite example commits complete source effects and their watermark atomically, then acknowledges
the source cursor. Subprocess tests exit before commit, after commit but before acknowledgement, and
after acknowledgement. They reopen both databases and check for missing or duplicate records,
including large records. These tests cover software interruption and lost completion messages, not
every SQLite internal commit fault.

## Implementation choices

The implementation makes these choices within the design's required semantics.

### Durability and checkpoints

- Local admission groups up to 64 already queued transactions within existing budgets. It does not
  wait to fill a group. The complete group is durable before dispatch.
- Receipts require contiguous visibility. The default checkpoint interval is 64 newly visible
  records; interval 1 checkpoints before releasing each visible prefix's receipts.
- Checkpoints also precede drained administrative barriers, maintenance, and normal shutdown.
  Logical import publishes appends individually and checkpoints before completion.
- Asynchronous checkpoint writing and merging the effects of separate operations are not
  implemented.
- WAL append advances live D after one segment flush, plus a directory flush for a new segment.
  `CURRENT` advances when checkpoints or storage metadata are published.
- Recovery rejects complete malformed groups and discards only physically short terminal appends.
  Some full-length torn writes therefore require explicit repair. Later loss or truncation of an
  uncheckpointed suffix cannot be distinguished from an interrupted append.
- Backup constructs destination metadata for its pinned live D and selected C. It copies exact
  complete-group log prefixes and the cursor metadata captured with them.

### Caches and publication work

Live owners use bounded decoded-page and snapshot-table caches, plus reusable validation results for
immutable roots and log prefixes. Recovery and low-level storage perform full validation.
Unsupported transitions or exhausted indexes trigger full checks. Validation results update only
after successful publication, and a file handover discards them.

Publication need not flush unchanged prefixes already made durable. It also omits the initial
directory flush when there are no new referenced filenames. Manifest and `CURRENT` flushes, renames,
and directory flushes retain G.3 ordering. Reused roots keep their published page count until the
roots change, excluding reconstructible later pages.

External changes to immutable files while an owner is live are unsupported. Caches do not
continuously check for later disk corruption.

### Retention and capacity

- Maintenance and snapshot revocation are explicit. There is no automatic disk-pressure policy,
  age-based snapshot revocation, or cursor release. Close revokes remaining snapshots after draining
  work.
- Observe and explicitly release abandoned cursors. Protected history is never silently discarded.
- A directory lease pins all obsolete files, so deletion can be more conservative than per-file
  pinning. Run maintenance again after pins retire.
- Operational reservations bound admission, not total resident memory, disk history, public read
  results, or maintenance scratch. They do not change logged semantic limits.

### Replicas and consumers

Logical import copies the selected physical prefix for isolated comparison. This simplifies
isolation but adds work proportional to the copied database. Raw-segment transport is not
implemented.

Read-only replicas can change local retention metadata and compact storage. They cannot accept
application transaction submissions. A writable restore requires the caller to retire the previous
primary; this library does not fence a primary on another machine.

The SQLite example starts from a retained sequence-zero feed and rejects missing baseline history.
The API supports snapshot-plus-tail rebuilds, but the example does not implement one.

## Qualification limits

Runtime verification has run on Linux. Windows library cross-compilation checks types and
conditional code, but not linking, filesystem behaviour, or crash durability. Windows and other Unix
platforms need native runtime testing before their durability can be qualified.

Fault injection and process-exit tests do not simulate hardware power loss, lost device caches, or
filesystem write reordering. Durable operation depends on the filesystem guarantees in G.3: atomic
same-directory replacement and durable file and directory synchronization. An implementation must
fail if the platform cannot provide these guarantees.

Checksums and consistency checks do not authenticate external writers. After independent
reconstruction sources have been removed, a validator cannot prove every possible historical
omission from the checkpoint alone. Publication and reclamation must preserve complete history by
construction and reject detectable inconsistencies.
