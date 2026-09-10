# Design Conformance

This document maps the implemented engine to [DESIGN.md](DESIGN.md). It records implementation and
test coverage, not a certification of every filesystem, hardware failure mode or possible execution.
The design remains the authority for transaction semantics and version-1 storage bytes.

## Implementation Map

| Design area                                                              | Implementation                                                                                             | Main verification                                                                                                                                                                                                     |
| ------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Sections 4-6; appendices A-C: stable types, DSL, bytecode and validation | `crates/blop-db-macros`, `src/bind.rs`, `src/vm/program.rs`, `src/vm/value.rs`                             | Golden encodings, all 48 opcodes, invalid control flow and types, bounded seeded input mutation, the exact H.3 program and minimal claims.                                                                            |
| Section 8; C.4: verified point and table access scopes                   | `src/vm/access.rs`, `src/vm/access_analysis.rs`, `src/database/record.rs`                                  | Canonical normalization, independent read/write modes, known-value joins, aliases, predictable failures, broader supplied manifests and their recorded resource charges.                                              |
| Sections 7, 9, 12, 22: sequencing, dependencies and prefix visibility    | `src/database/engine.rs`, `src/database/scheduler.rs`, `src/database/workers.rs`, `src/database/budget.rs` | Gated production workers, all-prior-writer dependencies, inverted completion, aborts and omitted writes, window and byte pressure, barriers, seeded serial comparisons and small exhaustive schedule models.          |
| Sections 10-11, 18-19; D.2-D.3: execution, rollback, MVCC and limits     | `src/vm/runtime.rs`, `src/vm/operations.rs`, `src/vm/database.rs`, `src/vm/outcome.rs`                     | Complete outcome comparisons, rollback, logical resource precedence, overlay scans, identical logical views with different physical histories and later invisible versions.                                           |
| Sections 14-15, 27; D-G: recovery and authoritative history              | `src/database/engine.rs`, `src/vm/history.rs`, `src/storage/store.rs`, `src/storage/metadata.rs`           | No cached-outcome replay skip, malformed checkpoint history with valid physical checksums, exact outcome/version correspondence, retained body validation, unsupported versions and missing required outcomes.        |
| Section 13: external snapshots                                           | `src/database/snapshot.rs`                                                                                 | Stable values and catalogue at the captured sequence, explicit point/iterator revocation, in-flight read protection and close with idle handles.                                                                      |
| Section 16; G.4; H.1-H.2: cursors and resolved feeds                     | `src/database/cursor.rs`, `src/database/feed.rs`, `src/database/exchange.rs`                               | Durable checkout/acknowledgement/release, namespace and issued-ID checks, snapshot-plus-tail, restart, exact historical effects, complete bounded batches and continuation without implicit acknowledgement.          |
| Section 17; F-G: physical storage                                        | `src/storage/page.rs`, `src/storage/tree.rs`, `src/storage/mvcc.rs`, `src/storage/platform.rs`             | Slotted pages, overflow thresholds, tree shape and ownership validation, randomized tree edits, immutable roots, publication interruption and native cross-process locking.                                           |
| Section 15; F.6; G.3-G.5: collection, compaction and backup              | `src/storage/maintenance.rs`, `src/storage/backup.rs`, `src/database/maintenance.rs`                       | Above-frontier and baseline retention, snapshot-only and durable claims, GC without compaction, quiescent root handover, pinned obsolete files, exact concurrent backup prefixes and crash-safe attachment/rebinding. |
| Section 16.2; H.1: logical replication                                   | `src/database/replica.rs`                                                                                  | Full isolated reference comparison before any incoming durable append, unchanged canonical bytes, mixed catalogue/policy history, divergence, partial durable import, subprocess exits and ordinary recovery.         |
| Sections 13.3, 24; H.2: retry ownership and derived consumers            | `tests/conformance.rs`, `examples/derived_sqlite.rs`                                                       | Atomic request-ID/business writes, payload mismatch and aborted-attempt retries; SQLite effects and watermark in one commit before cursor acknowledgement, including subprocess restart and large records.            |
| Section 25: diagnostics                                                  | `EngineStatus`, cursor listings and maintenance reports                                                    | Frontiers, reservations, backlog, workers, oldest unresolved record, barriers, retention floors, backup activity and terminal system errors.                                                                          |

## Verification Commands

Version-1 WAL publication is specified in DESIGN appendix I and implemented in `src/storage/wal.rs`,
`src/storage/store/wal.rs` and the CURRENT codec. It has one file flush per ordinary WAL group,
linked-tail discovery and recovery flushing before replay. The engine, cursor/logical readers,
backup capture and maintenance share one framed segment format. Canonical record and exchange bytes
remain covered by their original tests.

```sh
cargo test --workspace
cargo +nightly fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
mdformat --wrap 100 --check README.md CONFORMANCE.md
tombi lint Cargo.toml
cargo check --workspace --lib --target x86_64-pc-windows-gnu
```

`tests/conformance.rs` exercises 57,344 seeded mutated inputs across bytecode, arguments,
descriptors, manifests and outcomes. This bounded regression corpus is not continuous
coverage-guided fuzzing.

The storage fault matrix covers before and after file flushes, directory flushes and renames for
both new and extended segment publication, plus partial writes, incomplete metadata and
committed-prefix truncation. Import and derived-consumer subprocess tests exit without cleanup at
selected protocol boundaries. All fixtures use disposable directories.

The WAL matrix additionally injects errors and partial writes before/after every group append I/O
boundary, exits native child processes at each append boundary, truncates a group at every byte
position, rejects complete damaged groups and forks, and verifies strict version and group bounds.
Recovery tests include a checkpoint inside a group and backups with live D beyond the selected
manifest.

The directory lease explicitly unlocks when its final registered owner retires. This prevents a file
descriptor briefly inherited during subprocess creation from extending the lock beyond close. Actual
views, scans, worker jobs and backups still retain the shared lease until safe release.

## Operating Choices

- Local transaction admission opportunistically groups up to 64 already queued requests within
  existing budgets. The complete group is durable before dispatch. Receipts require contiguous
  visibility, not a fresh materialized checkpoint. `EngineOptions::checkpoint_interval` defaults to
  64 newly visible records; 1 retains checkpoint-before-receipt behaviour. Checkpoints also precede
  drained administrative barriers, maintenance and normal shutdown. Logical import retains
  individual append publication and checkpoints before completion. Asynchronous checkpoint writing
  and semantic operation coalescing are not implemented.
- Live owners use bounded decoded-page and snapshot-table caches, plus incremental root and log
  validation proofs. Full recovery and low-level storage validation remain uncached. Unsupported
  transitions and bounded-index exhaustion fall back to full checks. Proofs are updated only after
  successful publication, and file handover resets them. External mutation of immutable files while
  an owner is live is unsupported; cached reads are not continuous disk scrubbing.
- G.3 synchronization skips unchanged prefixes already made durable by a previous publication and
  the initial directory flush when no new referenced filenames exist. Manifest and CURRENT file
  flushes, renames and directory flushes remain ordered. Runtime fault tests exercise every actual
  I/O boundary with old-or-new recovery checks. Reused runtime roots keep their published page
  count, excluding reconstructible appended pages until roots change. Group tests cover publication
  failure and replay; a subprocess exits after successful deferred-checkpoint receipts and above-C
  cursor publication, then recovery verifies outcomes and both feed kinds.
- Version-1 WAL append updates live D after one segment flush, plus a directory flush for a newly
  created segment; CURRENT advances on checkpoints and storage metadata publication. Creation and
  opening share one protocol. Full-sized malformed groups fail closed. Only physically short
  terminal appends are discarded, so some full-length torn writes require explicit repair. Loss or
  physical truncation of an uncheckpointed suffix caused by later damage cannot be distinguished
  from an interrupted append.
- Physical backup synthesizes destination metadata for its pinned live D and selected C. It copies
  exact complete-group log bounds and preserves all cursor metadata captured at that point.
- Maintenance is explicit. There is no automatic disk-pressure policy or automatic cursor release.
  Abandoned cursors must be observed and explicitly released; protected history is never silently
  discarded.
- Snapshot revocation is explicit, and close revokes all remaining snapshots after draining work.
  Automatic age-based revocation is not implemented.
- Directory leases conservatively pin all obsolete files. Maintenance may therefore defer more file
  deletion than a per-file pinning scheme; rerun it after pins retire.
- Logical import copies a complete selected physical prefix for isolated comparison. This favours a
  simple correctness boundary over replication throughput. Raw segment transport is an optional
  alternative and is not implemented.
- The derived SQLite example builds from a retained genesis feed. It refuses missing baseline
  history rather than silently creating an incomplete index. Applications can use the implemented
  snapshot-plus-tail API for snapshot rebuilds; the example does not implement such a rebuild.
- Operational byte reservations are conservative admission controls, not a cap on total RSS,
  retained disk history, public read results or maintenance scratch. They never alter logged
  semantic budgets.
- Read-only replicas may change local retention metadata and compact storage. They cannot accept
  application transaction submissions. A writable restore explicitly requires the caller to retire
  the previous primary; distributed fencing is outside this design.

## Qualification Limits

Runtime verification has run on Linux. Windows library cross-compilation checks types and
conditional code, not linking, filesystem behaviour or crash durability. Windows runtime
qualification and testing on other Unix platforms remain necessary before making durability claims
for those platforms. The Windows all-target check also requires a MinGW C compiler for the SQLite
benchmark/example dependency.

Fault injection and subprocess exit tests do not simulate hardware power loss, lost device caches or
filesystem write reordering. Durable operation still depends on the filesystem guarantees in G.3:
atomic same-directory replacement and durable file and directory synchronization. A platform that
cannot supply them must fail rather than substitute a no-op.

Checksums and internal consistency checks do not authenticate an external writer. Once independent
reconstruction sources have been removed, no validator can prove every possible historical omission
from the remaining checkpoint alone. The engine preserves completeness by construction during
publication and reclamation and rejects detectable inconsistencies rather than inventing history.
