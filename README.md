# blop-db

blop-db is an embedded database for Rust. Transactions are small programs written with `tx!`. The
database writes each program and its inputs to a durable log before executing it. Transactions can
run in parallel, with the same results as running them in log order.

The `blop_db::database` API provides async writes, snapshots, changefeeds, logical replication,
backups and manual compaction. Tables have typed keys and values. Transaction code can read rows,
check conditions and update several tables atomically.

## Getting started

Run the [balance-transfer example](examples/transactions.rs) from the repository root:

```sh
cargo run --example transactions -- /tmp/blop-example-db
```

Choose a database directory that does not exist yet, with an existing parent directory. The example
creates two accounts with balances of 100 and 50, transfers 25, then reopens the database and checks
that both balances are 75.

Here is a smaller example that creates a table, writes a counter and reads it through a snapshot:

```rust
use blop_db::{database, Limits, tx};
use blop_db::vm::{CatalogueOperation, Outcome, Type, Value};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("database");
    let db = database::create(&path, Default::default()).await?;
    let created = database::execute_catalogue(&db, CatalogueOperation::Create {
        name: "counters".into(),
        key: Type::U64,
        value: Type::I64,
    }).await?;
    let Outcome::Success { value: Value::U64(table), .. } = created.outcome else {
        return Err("table creation aborted".into());
    };

    let receipt = database::execute(&db, tx! {
        tables { counters: u64 => i64 = table }
        insert(counters[7], 40);
        counters[7] += 2;
        return counters[7];
    }?, Limits::default()).await?;

    match receipt.outcome {
        Outcome::Success { value, .. } => println!("Counter: {value:?}"),
        Outcome::Aborted(abort) => return Err(format!("transaction aborted: {abort:?}").into()),
    }

    let snapshot = database::snapshot(&db).await?;
    assert_eq!(database::get(&snapshot, table, &Value::U64(7))?, Some(Value::I64(42)));
    database::revoke(&snapshot);
    database::close(&db).await?;
    Ok(())
}
```

This example uses a temporary directory. For persistent storage, set `path` to your database
directory. `create` generates the database's identity and cursor namespace. `open(path).await` opens
an existing database and finishes recovery before returning.

The examples use Tokio's current-thread executor. The API futures can run on any executor; database
coordination, blocking publication I/O and transaction execution use dedicated threads.

## Writing transactions

`tx!` compiles transaction code to version-1 bytecode during Rust compilation. At runtime it copies
captured inputs, binds table IDs and returns `Result<Transaction, BuildError>`. Constructing a
transaction does not run it. Submit it with `database::execute`.

A transfer looks like this:

```rust
use blop_db::tx;

fn main() -> Result<(), blop_db::BuildError> {
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
            require(from != to, 2);
            require(balances[from] >= amount, 3);
            balances[from] -= amount;
            balances[to] += amount;
            return balances[from];
        }
    }?;
    Ok(())
}
```

The balances table must already exist with the declared schema. Missing keys, insufficient funds,
arithmetic overflow or any other transaction abort discard all of the transaction's writes.

### Captures, tables and results

- `captures` brings Rust values into the program. Initializers run once in declaration order and
  borrow their inputs; encoding copies the values. Later changes to the originals have no effect.
- Captures are immutable. Use `name` to read one, or `$name` to refer to it even when a local
  variable shadows the name.
- `tables` binds names and schemas to runtime `u64` table IDs. IDs are evaluated after the captures.
  Zero, `u64::MAX` and duplicate IDs are binding errors. Declare a table once and reuse its name.
- `-> type` declares the result type. It is optional when the compiler can infer the result from
  returns. Every return must have the same shape. A program that can reach the end must return Unit.

Both declaration sections are optional. When both are present, `captures` comes first. Braces around
the body are optional too.

Rust expressions belong in capture and table initializers. The transaction body cannot call Rust
functions or use loops, closures, recursion, casts or other macros. Branches only move forward, so
every program has a bounded instruction count.

### Types

| Type            | Rust capture               | Notes                                                       |
| --------------- | -------------------------- | ----------------------------------------------------------- |
| `()`            | `()`                       | Unit.                                                       |
| `bool`          | `bool`                     | Boolean.                                                    |
| `i64`, `u64`    | Matching Rust integer type | Checked 64-bit arithmetic; no implicit integer conversions. |
| `bytes<N>`      | `AsRef<[u8]>`              | At most N bytes.                                            |
| `string<N>`     | `AsRef<str>`               | At most N UTF-8 bytes, not characters.                      |
| `(T, U, ...)`   | Matching Rust tuple        | Use `(T,)` for one field.                                   |
| `tuple<>`       | `()`                       | Empty Tuple, distinct from Unit.                            |
| `rows<K, V, N>` | None                       | Up to N rows; only for registers and results.               |

Bounds must be integer literals. Values can nest up to 16 levels; tuples can have up to 256 fields.
Encoded values must fit 16 MiB, and encoded table keys must fit 1,024 bytes. Rows cannot be nested,
captured or used in table schemas. Binding returns `BuildError` if a capture exceeds its bound or
the complete arguments exceed 16 MiB.

Integer literals default to `i64` unless the context selects `u64`. String and byte-string literals
use their lengths as bounds. An annotation such as `let mut text: string<128> = "";` gives a local
more room. The VM checks bounds whenever it copies or produces a value.

### Statements and expressions

The syntax follows Rust's operator precedence and left-to-right evaluation order. Locals have
lexical scope, allow shadowing and require initializers. Use `let mut` for a local you intend to
change.

| Syntax                                               | Behaviour                                                                |
| ---------------------------------------------------- | ------------------------------------------------------------------------ |
| `let x = expression;`                                | Declare an immutable local.                                              |
| `let mut x: type = expression;`                      | Declare a mutable local; the annotation is optional.                     |
| `x = expression;`                                    | Assign to a mutable local.                                               |
| `table[key]` or `load(table[key])`                   | Read a value; abort if the key is missing.                               |
| `exists(table[key])`                                 | Check whether a key exists.                                              |
| `table[key] = value;` or `store(table[key], value);` | Write without reading the old value.                                     |
| `insert(table[key], value);`                         | Write, aborting if the key exists.                                       |
| `delete(table[key]);`                                | Delete a key.                                                            |
| `x += value;` or `table[key] -= value;`              | Read, compute and write.                                                 |
| `if condition { ... } else { ... }`                  | Branch; `else if` and an omitted `else` are supported.                   |
| `require(condition, code);`                          | Abort if false. The literal `u32` code is optional and defaults to zero. |
| `abort(code);`                                       | Abort unconditionally. `abort;` and `abort();` use code zero.            |
| `return expression;` or `return;`                    | Return a value or Unit.                                                  |

`if` is a statement, not an expression. Unreachable statements are compile errors. A compound table
assignment evaluates its key once and loads the old value before evaluating the right operand.

Arithmetic operators use checked integer arithmetic. `==`, `!=`, `<`, `<=`, `>` and `>=` compare
values. `&&` and `||` short-circuit: a skipped operand cannot read data or abort. `&`, `|`, `^` and
`!` operate on booleans or equal integer types. Shifts use a `u64` count. Rows support neither
comparison nor equality.

These intrinsics cover the remaining operations and named forms:

| Operation                | Intrinsics                                                                                       |
| ------------------------ | ------------------------------------------------------------------------------------------------ |
| Copy and arithmetic      | `copy`, `add_checked`, `sub_checked`, `mul_checked`, `div_checked`, `rem_checked`, `neg_checked` |
| Integer conversion       | `to_i64_checked`, `to_u64_checked`                                                               |
| Comparison               | `eq`, `lt`, `le`, `gt`, `ge`                                                                     |
| Eager Boolean operations | `bool_and`, `bool_or`, `bool_xor`, `bool_not`                                                    |
| Bitwise operations       | `bit_and`, `bit_or`, `bit_xor`, `bit_not`, `shl_wrap`, `shr`                                     |
| Bytes and strings        | `byte_len`, `concat`, `slice_bytes`, `utf8_bytes`, `parse_utf8`, `sha256`                        |
| Tuples                   | `(a, b)`, `tuple(a, b)`, `tuple()`, `value.0`, `field(value, 0)`                                 |
| Rows                     | `rows_len`, `rows_key`, `rows_value`                                                             |

`slice_bytes(bytes, start, length)` uses `u64` offsets and lengths. Row indices are also `u64`;
tuple field indices must be known at compile time. Invalid arithmetic, shifts, UTF-8 and bounds
produce transaction aborts.

### Bounded scans

`scan_bounded(table, lower, upper, flags, row_limit, byte_limit)` reads a key range. Either endpoint
can be `unbounded`. The last three arguments are unsigned integer literals:

- `flags`: bit 0 includes the lower endpoint; bit 1 includes the upper endpoint. Other bits are
  forbidden, and an unbounded endpoint must have its inclusion bit clear.
- `row_limit`: zero through 65,535.
- `byte_limit`: zero through 64 MiB.

The result's maximum encoded size must fit 16 MiB. Transaction resource claims also apply.

```rust
fn main() -> Result<(), blop_db::BuildError> {
    let transaction = blop_db::tx! {
        tables { balances: u64 => i64 = 3 }
        let rows = scan_bounded(balances, 10, 20, 1, 100, 4096);
        if rows_len(rows) == 0 {
            abort(4);
        }
        return (rows_key(rows, 0), rows_value(rows, 0));
    }?;
    Ok(())
}
```

This reads `[10, 20)` and returns the first key and value, or aborts with code 4 if the range is
empty.

### Compile-time checks

External values must be declared as captures, and loops are not supported:

```compile_fail
let external = 5_i64;
let _ = blop_db::tx! { return external; };
```

```compile_fail
let _ = blop_db::tx! { loop { require(true); } };
```

Capture types must match their Rust values. Tuple fields cannot be dropped, and signedness does not
change implicitly:

```compile_fail
let _ = blop_db::tx! { captures { pair: (i64,) = (1_i64, 2_i64) } return pair; };
```

```compile_fail
let value = 1_u64;
let _ = blop_db::tx! { captures { value: i64 = value } return value; };
```

`abort` and `unbounded` are reserved names. The compiler reports unsupported syntax at its source
location. Live table IDs and schemas are checked when the database prepares the transaction.

## Writes, results and retries

`database::execute(&db, transaction, claims).await` returns a `Receipt { sequence, outcome }` once
the transaction is durable and visible. Every earlier log record is also resolved at that point. A
checkpoint may still lag behind; reopening replays the durable records after it.

Check both the call's `Result` and the receipt's `Outcome`:

| Result                                | Meaning                                                                                           |
| ------------------------------------- | ------------------------------------------------------------------------------------------------- |
| `Ok(Receipt)` with `Outcome::Success` | The transaction committed. The outcome contains its returned value and final effects.             |
| `Ok(Receipt)` with `Outcome::Aborted` | The abort was recorded and consumed a sequence number. All of its business writes were discarded. |
| `Error::Rejected`                     | Validation failed before sequencing. Correct the request before retrying.                         |
| `Error::OperationalLimit`             | The request cannot fit the process's configured capacity. It was rejected before sequencing.      |
| `Error::Storage`                      | A startup or pre-append system operation failed. Inspect the error.                               |
| `Error::Uncertain`                    | The request may have committed. Close and reopen the database to recover.                         |
| `Error::Closed` on submission         | The writer was closed, so this request did not execute.                                           |

Dropping a future after enqueueing does not cancel the transaction. Retrying creates a new log
record. For request deduplication, check the request ID and store its result in the same `tx!`
program as the business writes. Check that a reused ID has the same payload. An abort rolls back the
request-ID write too.

A system failure stops the writer. Close and reopen it before submitting more work.

### Database operations

Clone a `Database` handle to share it between callers. Use these functions in `blop_db::database` to
manage the database:

- `create(path, options)` creates a database. `CreateOptions::default()` generates independent
  version-4 UUIDs for the database ID and cursor namespace and supplies default resource limits.
- `open(path)` restores the checkpoint and replays later durable records.
- `execute_catalogue(&db, operation)` creates, renames or drops a table. A successful create returns
  the table ID as `Value::U64`.
- `execute_limits(&db, limits)` changes the policy for later records. It remains available when the
  current policy prevents transaction submission.
- `execute_with_manifest(&db, transaction, claims, manifest)` validates supplied access declarations
  before sequencing. Broader declarations are allowed and kept as supplied.
- `close(&db)` stops submissions through every clone, drains accepted work, checkpoints and waits
  for the directory lock to be released. Dropping all handles starts the same shutdown
  asynchronously, but cannot report shutdown errors.

Await these operations. `status(&db)` is synchronous and returns the latest diagnostic sample,
including queue reservations, worker activity, recovery frontiers, retention floors and the last
system error. A status sample can lag during I/O; it is not a receipt.

If you supply your own `CreateOptions::database_id` or `cursor_namespace`, use unique nonzero
`Some([u8; 16])` values. `db.database_id()` and `db.cursor_namespace()` expose the selected IDs.
Ordinary reopening preserves both IDs and the stored resource policy.

### Transaction limits

`Limits` names 17 logical resources. The database policy sets their ceilings; each transaction's
claims state how much it may use. Claims must fit the current policy. Defaults are finite format
ceilings, which you can lower for your application:

```rust
use blop_db::{database, Limits, tx};
use database::CreateOptions;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let limits = Limits {
        instructions: 1024,
        writes: 100,
        overlay_bytes: 8 * 1024 * 1024,
        ..Limits::default()
    };
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("database");
    let db = database::create(&path, CreateOptions {
        limits,
        ..CreateOptions::default()
    }).await?;
    let receipt = database::execute(&db, tx! { return 42; }?, limits).await?;
    println!("Sequence {}: {:?}", receipt.sequence, receipt.outcome);
    database::close(&db).await?;
    Ok(())
}
```

### Process capacity

`EngineOptions` controls worker counts, queues and memory reservations. These settings belong to the
process and are separate from the logged resource policy. Set them with
`create_with_options(path, create_options, engine_options)` or
`open_with_options(path, engine_options)`.

| Field                    | Default                               | Purpose                                                            |
| ------------------------ | ------------------------------------- | ------------------------------------------------------------------ |
| `tail_recovery`          | `TailRecovery::DiscardInvalid`        | Policy for damaged WAL suffixes.                                   |
| `workers`                | Available CPUs clamped to 2 through 4 | Interpreter threads; accepts 1 through 256.                        |
| `execution_window`       | 64                                    | How far beyond the visible sequence work can be dispatched.        |
| `checkpoint_interval`    | 64                                    | Newly visible records between checkpoints; accepts 1 through 4096. |
| `submission_queue_count` | 64                                    | Reservations for unassigned submissions.                           |
| `submission_queue_bytes` | 64 MiB                                | Input bytes reserved for those submissions.                        |
| `assigned_backlog_count` | 64                                    | Assigned records waiting for visible receipts.                     |
| `assigned_backlog_bytes` | 64 MiB                                | Encoded log bytes in that backlog.                                 |
| `execution_bytes`        | 512 MiB                               | Lifetime reservations for assigned transactions.                   |
| `preparation_bytes`      | 512 MiB                               | Capacity for active validation or the prepared queue head.         |

When capacity is busy, callers wait before admission. Cancelling that wait releases its permits.
Once a transaction has a sequence, the engine keeps it until it finishes rather than dropping it to
make room. Reservations cover the transaction through installation and visibility. The estimates are
conservative, so a transaction can be rejected even if it would fit in practice.

If one request exceeds a budget, the call returns
`Error::OperationalLimit { resource, required, limit }`. Increase that budget or reduce the request.
Changing these settings does not change replay semantics for records already in the log.

These budgets do not cap total resident memory or disk usage. Read results, retained history,
caches, allocator overhead and maintenance scratch space are separate. Logical imports share the
queue budgets and run one at a time; they also need temporary disk space for a copy of the selected
physical database prefix.

## Execution and recovery

One coordinator thread validates submissions, writes the log and installs results. Worker threads
interpret prepared transactions against pinned storage views. A transaction waits for every earlier
possible writer that overlaps its reads. Independent transactions, including blind writes, can run
in parallel. Results become visible only when every earlier record has finished.

The engine tracks three sequence numbers:

- **D, durable:** the last record in the contiguous durable log.
- **F, visible:** the last record through which all work is durable and resolved.
- **C, checkpoint:** the sequence saved as the recovery starting point.

A receipt requires visibility through its sequence, not a new checkpoint. Recovery starts from C and
replays every record through D, including acknowledged transactions, aborts and no-write results.

The default checkpoint interval is 64 newly visible records. Set it to 1 to checkpoint before every
receipt. The interval counts records, not time or bytes; an idle database can keep a smaller suffix
uncheckpointed. Checkpoints also precede catalogue and policy barriers, maintenance and normal
shutdown. A failed checkpoint stops the writer. Successful receipts remain durable, while receipts
waiting on the failed publication are uncertain. Reopen to recover; `status(&db).last_error` retains
the error detail.

### Damaged log tails

The default `TailRecovery::DiscardInvalid` keeps complete valid WAL groups beyond the manifest's
selected durable bounds, then discards the suffix from the first malformed group. It never skips
damage to resume at a later group. `TailRecovery::Strict` instead rejects complete-sized malformed
groups without modifying the WAL:

```rust,no_run
use blop_db::database::{self, EngineOptions, TailRecovery};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = database::open_with_options("/path/to/database", EngineOptions {
        tail_recovery: TailRecovery::Strict,
        ..EngineOptions::default()
    }).await?;
    database::close(&db).await?;
    Ok(())
}
```

Both policies discard physically short terminal appends, including incomplete padding. Both reject
damage inside selected log bounds or checkpoint data, unsupported formats, broken sequence or
predecessor links, and complete segment forks or gaps. A complete successor after a damaged accepted
segment is also an error before trimming.

The default favours recovery from torn appends. Later corruption of an acknowledged suffix outside
selected bounds can look like an interrupted append and be discarded. Physical truncation of that
suffix is ambiguous under either policy. The
[WAL specification](DESIGN.md#appendix-i-wal-commit-groups) describes the framing and recovery
rules.

## Snapshots and changefeeds

`snapshot(&db).await` captures the visible sequence without adding a log record. `get`, `scan` and
`catalogue` read data and table metadata at that fixed sequence. Keys and range endpoints are typed
`vm::Value` values; scans yield `(Value, Value)` pairs in key order. Catalogue reads include dropped
tables.

Reads perform synchronous local I/O on the calling thread. Use your executor's blocking facility for
large reads. Unlike snapshot reads, a result-only `execute` still enters the transaction log.

Snapshot clones and iterators share one revocable claim. `revoke(&snapshot)` waits for in-flight
reads, then releases the view. Later reads return `SnapshotRevoked`. A revoked iterator returns that
error even if it previously returned `None`, so it is not a fused iterator. Closing the database
revokes all snapshots; idle snapshots and iterators do not keep the database open.

### Building a derived index

`snapshot_and_cursor` captures a snapshot and durably registers a feed cursor at the same sequence.
Build from the snapshot, then consume the records after its watermark:

```rust
use blop_db::database::{self, BatchLimits, CursorKind, CursorToken, FeedBatch, Watermark};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let db = database::create(directory.path().join("db"), Default::default()).await?;
    let (snapshot, cursor) = database::snapshot_and_cursor(
        &db, CursorKind::Resolved, "search-index-build",
    ).await?;
    let baseline = snapshot.watermark();
    let saved_token = cursor.encode();
    // The index and its baseline must become durable together, or a restart could skip data.
    // Commit the completed snapshot build and baseline.encode() in one index transaction.
    database::revoke(&snapshot);

    let cursor = CursorToken::decode(&saved_token)?;
    database::reopen_cursor(&db, &cursor).await?;
    let batch = database::read_feed(&db, &cursor, baseline, BatchLimits::default()).await?;
    let wire = batch.encode()?;
    assert_eq!(FeedBatch::decode(&wire)?, batch);
    let next = batch.watermark()?;
    let payload = next.to_hex();
    let recovered = Watermark::from_hex(&payload)?;
    // Acknowledgement permits history reclamation, so commit the records and payload first.
    database::acknowledge_cursor(&db, &cursor, recovered).await?;
    database::release_cursor(&db, &cursor).await?;
    database::close(&db).await?;
    Ok(())
}
```

A cursor token is 56 bytes. A watermark is 40 bytes, or 80 lowercase ASCII hex characters. Save the
token for restart, and commit each new watermark atomically with the corresponding derived data.
Only then acknowledge it to the source.

`read_feed` returns complete consecutive outcome records, including aborts, no-write successes,
catalogue changes and policy changes. It neither filters out sequences nor advances the cursor.
Effects contain encoded key and value bytes; use the baseline catalogue and subsequent catalogue
events to interpret them.

Batch limits include the header and checksum, up to 256 MiB. If the first record will not fit,
`BatchTooSmall { required }` reports the needed size. A zero record limit or a poll at the visible
frontier returns an empty batch.

### Resuming and releasing cursors

Use `reopen_cursor` with a saved token. Without one,
`checkout_cursor(&db, recovered_watermark, kind, label)` can register a cursor if the required
history still exists. Saving a watermark alone does not retain history.

Cursors survive snapshot revocation, handle loss and ordinary restart. If a snapshot build is
revoked, discard the incomplete build and release its cursor unless you still need it. Use
`list_cursors` to find abandoned registrations and `release_cursor` to remove them. Labels are
diagnostic and need not be unique. Tokens identify registrations; they are not credentials.

Acknowledgements can stay at the current position or advance through the visible frontier. Repeating
an acknowledgement or releasing an already released, previously issued ID succeeds. Reopening or
acknowledging a released ID returns `CursorReleased`. Cursor IDs are never reused.

Cursor operations consume no transaction sequence and do not delete history themselves.
`retention_status` reports the current history and log claim floors.

## Logical replication

To start a replica:

1. Keep a logical-feed or log-replica cursor on the source to retain the required log history.
1. Create a physical image with `backup`, then attach it with `AttachMode::ReadOnlyReplica`.
1. Read the recovered replica's `snapshot(&replica).await?.watermark()`.
1. Fetch the source's logical feed after that watermark with `read_logical_feed`. The source must
   have made that baseline visible first.
1. Encode the batch and call `import_logical(&replica, &encoded).await`.
1. Acknowledge the returned durable watermark to the source cursor.

Logical-feed and log-replica cursors protect original log availability from their baseline plus one.
Logical batches contain the exact recorded bytes and resolved outcomes. Local segment IDs and
boundaries can differ; source sequences, record bytes and SHA256 digests stay the same.

Import validates the whole batch and executes it in a private copy of the selected physical prefix.
Every generated outcome must match the supplied outcome byte-for-byte before anything is appended to
the replica. A malformed or divergent batch leaves the live prefix unchanged. Wire checksums alone
cannot establish this agreement.

Imports run one at a time and block new snapshot, feed, cursor, backup and maintenance requests
until completion. Existing snapshots keep their original views. Each batch currently copies the
selected physical prefix, so temporary disk space and copy time can be substantial. A crash may
leave a `blop-import-*` directory in the OS temporary directory; remove it only after its process
has stopped.

An I/O failure or crash can leave part of a verified batch durable. After an uncertain result or
cancellation, close and reopen the replica, inspect its recovered watermark and request the
remaining records. Reading or importing never acknowledges a source cursor automatically.

### SQLite example

The [SQLite example](examples/derived_sqlite.rs) maintains a mirror of live keys and values, a table
catalogue, a SHA256 value index and an outcome audit table:

```sh
cargo run --example derived_sqlite -- /path/to/source-db /path/to/index.sqlite
```

It starts from retained history at sequence zero and does not implement snapshot rebuilds. Run one
consumer process per index. Each SQLite transaction applies complete records and commits their
watermark before acknowledging the source cursor. The example uses rollback journalling and
`synchronous = FULL`; keep the SQLite file and its journals together.

On restart it resumes from SQLite's committed watermark. Missing history, a wrong source identity or
a cursor ahead of the saved watermark is an error. An uncertain SQLite commit requires reopening
SQLite and checking that watermark before retrying. The audit history is retained without
compaction.

## Maintenance

Run `maintain(&db, MaintenanceOptions::default()).await` to collect eligible history, retire whole
log segments, compact the five storage trees and seal the current log. Maintenance drains assigned
work and checkpoints first. New snapshot and cursor controls wait for it; queued, unassigned writes
resume afterwards. Cancelling the waiter does not cancel accepted maintenance.

`MaintenanceOptions` has three independent switches: `collect_history`, `compact` and `rotate_log`.
Enable only `rotate_log` to seal a segment without collecting history. Set all three to false to
validate the checkpoint and retry obsolete-file deletion. There is no background maintenance
scheduler or automatic history-retention policy.

Snapshots and durable cursor baselines determine how much history must remain. Collection retains
all newer versions and the latest version at or below the retention floor for each key, including
deletions. Version-1 catalogue and policy history remain. Logical and replica cursors also retain
source log coverage. Once history has been reclaimed, checking out an older cursor returns
`HistoryUnavailable`.

Physical deletion is conservative: any storage view, unrevoked snapshot, worker or backup pin can
keep all obsolete files in the directory. Revoke unused snapshots, let backups finish, then run
maintenance again. Collection without compaction can append copy-on-write pages rather than shrink
disk usage.

`MaintenanceReport` gives the selected floors, removed versions and outcomes, retired segments, page
counts, and deleted or deferred files and bytes. The latest successful report is also in
`status(&db)`.

Read-only replicas permit local maintenance. It does not change their canonical sequence or affect
the source's files and cursor registrations.

## Backup and attach

`backup(&db, new_directory).await` copies a pinned durable physical prefix while the source keeps
running. The destination must not exist. A failed backup can leave an incomplete directory, and a
later backup will not overwrite it. The returned `storage::Manifest` describes the copied prefix,
which may be behind the source by the time the copy finishes.

Up to four backup workers run per database; excess requests return `OperationalLimit` for
`backup_jobs`. Cancellation does not stop a worker or release its pins during I/O. Closing waits for
active backups. Destination I/O failures do not stop the source writer.

Backup images carry an `ATTACH_REQUIRED` marker and must be attached before use:

- `attach(path, AttachMode::ReadOnlyReplica).await` preserves the database identity and rejects
  local transaction, catalogue and policy writes. Snapshots, cursors, logical import, maintenance
  and backups remain available. Ordinary reopen preserves this role.
- `attach(path, AttachMode::RestorePrimarySourceRetired).await` permits writes. Retire the previous
  primary first: the library cannot fence another machine. Independent writable primaries must not
  share a database identity. Promoting a replica requires this same choice.
- `attach_with_options(path, mode, engine_options).await` also sets process capacity and recovery
  options.

Use `open` for normal recovery. Every explicit `attach` generates a fresh cursor namespace and
invalidates old tokens, even if the directory was already attached. An interrupted attach can be
retried. For a filesystem copy made outside the backup API, attach it explicitly; the library cannot
recognise arbitrary copies. Do not bypass the attachment or role markers.

Attach preserves copied cursor registrations and baselines. After checking which registration
belongs to a consumer and verifying its saved watermark, call
`rebind_cursor(&db, existing_numeric_id, watermark).await` to get a token in the new namespace.
Rebinding validates the protected interval and retained history. It does not advance the cursor or
verify the contents of your derived index.

## Engine APIs

`blop_db::vm` provides a sequential reference interpreter for all 48 version-1 opcodes. It validates
bytecode into typed instructions, then checks transaction behaviour against historical catalogue,
policy and row state. `interpret` returns an outcome without changing storage. `execute` and
`execute_bytes` also install the final versions and outcome as one batch.

These functions do not append a transaction log or issue durability receipts. Use an isolated
reference store for experiments. A caller integrating them into an engine must establish log
durability, supply the canonical record digest, protect history and publish a fully resolved
checkpoint prefix. The production scheduler uses prepared interpretation and serial installation.

`vm::prepare_transaction` validates the program and access manifest without reading database rows.
Pass `None` to derive scopes or `Some(&manifest)` to validate supplied ones. Preparation includes
reads and writes from both sides of every branch. Known keys produce point scopes; unknown keys and
scans produce table scopes. `vm::interpret_prepared` reuses the validated program.

`blop_db::storage` implements append-only, copy-on-write B+ trees with 16 KiB checksummed pages,
immutable pinned views, overflow values, manifests and WAL framing. Physical `apply` installs a
batch but does not make it durable or visible. `publish` flushes referenced files and selects a new
manifest through `CURRENT`. `storage::open` restores physical state; `database::open` also validates
logical history and replays the durable suffix.

The [design specification](DESIGN.md) defines the formats and required semantics. The
[conformance guide](CONFORMANCE.md) maps them to implementation tests and records the current
implementation choices.

## Terms used in this guide

| Term                                     | Meaning                                                                                 |
| ---------------------------------------- | --------------------------------------------------------------------------------------- |
| Sequence                                 | A record's permanent position in the database log.                                      |
| Prefix                                   | All records through a sequence, with no gaps.                                           |
| Durable frontier, D                      | The last sequence in the contiguous durable log.                                        |
| Visible frontier, F                      | The last sequence through which every record is durable and resolved.                   |
| Checkpoint, C                            | The sequence whose materialized state has been published for recovery.                  |
| Materialized state                       | Stored data and metadata computed by executing the log.                                 |
| Outcome                                  | A transaction's success result or deterministic abort result.                           |
| Resolved                                 | Finished with success or an abort, with the complete result installed.                  |
| Catalogue                                | Table IDs, names, schemas and live or dropped status.                                   |
| Canonical encoding                       | The stable byte representation required by a format.                                    |
| Access manifest                          | The keys or tables a transaction may read or write.                                     |
| Storage manifest                         | Published checkpoint roots, durable log bounds and retained-history metadata.           |
| Overlay                                  | A transaction's private pending writes, discarded on abort.                             |
| Multi-version concurrency control (MVCC) | Sequence-tagged versions that let readers select a historical state.                    |
| Tombstone                                | A deletion version that stops reads from returning an older value.                      |
| Cursor                                   | A durable registration that retains history for a feed consumer.                        |
| Watermark                                | A database identity and sequence identifying the source state a consumer has processed. |
| Pin                                      | A reference that keeps storage from being reclaimed.                                    |
| Copy-on-write (COW)                      | Writing new pages and roots instead of changing published pages.                        |
| Write-ahead log (WAL)                    | The durable record of work saved before execution.                                      |
| UUID                                     | A universally unique identifier.                                                        |

`1 KiB = 1,024 bytes` and `1 MiB = 1,048,576 bytes`.

## Platform support

Runtime tests have run on Linux. The libraries build on Unix and Windows, but Windows support is
experimental and has not been runtime-tested. Other Unix platforms have not been runtime-tested
either.

The Windows libraries have been cross-checked from Linux with
`cargo check --workspace --lib --target x86_64-pc-windows-gnu`. The all-target check is blocked by
the missing `x86_64-w64-mingw32-gcc` compiler needed for bundled SQLite. Cross-compilation does not
test linking, filesystem behaviour or crash recovery.

Durability requires atomic same-directory file replacement and durable file and directory
synchronization, as specified in design section G.3. Creation or publication fails if the platform
cannot provide the required flushes. Windows directory-flush behaviour still needs native testing.

## Benchmarks

The [benchmark report](BENCHMARKS.md) compares blop-db with redb and SQLite and includes workloads,
measurements and reproduction commands. To run the durable key-value workload:

```sh
cargo run --release --example kv_bench -- \
  --dir /path/to/existing/directory --mode durable --clients 4 --workers 4
```

The harness creates temporary databases in the supplied directory. It uses independent one-key
transactions and public snapshot reads.

## Development

```sh
cargo test --workspace
cargo +nightly fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
mdformat --wrap 100 --check README.md DESIGN.md CONFORMANCE.md BENCHMARKS.md
```

The Rust examples in this README are also crate documentation tests. See
[CONFORMANCE.md](CONFORMANCE.md) for fault-injection coverage and platform checks. When changing an
API or format, update its examples and documentation with the code.
