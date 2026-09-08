# blop-db

This repository contains a transaction compiler, a single-threaded reference VM, an engine-facing
storage layer and an async durable write API. The write API sequences transactions on a background
thread, publishes their log records before execution and returns receipts after checkpoint
publication.

`tx!` compiles a small deterministic transaction program to the ISA 1 bytecode specified in
[`DESIGN.md`](DESIGN.md), appendices A, B and C. Parsing, type checking, register allocation and
branch resolution happen during Rust compilation. Runtime binding snapshots the captures and
resolves table IDs. It does not evaluate the VM program.

## Async Writes

Use `blop_db::database` to create or open a database and submit bound `tx!` programs. Its free
functions accept a cloneable `Database` handle:

- `create(path, options).await` creates a new directory. `CreateOptions::default()` generates a UUID
  database ID and cursor namespace, and supplies default named limits. The parent directory must
  already exist.
- `open(path).await` restores the checkpoint and replays any later durable records before accepting
  work.
- `execute(&db, transaction, claims).await` returns a `Receipt { sequence, outcome }` after the
  transaction is durable, resolved and checkpointed.
- `execute_catalogue(&db, operation).await` creates, renames or drops tables. A successful create
  returns its table ID as `Value::U64` in the outcome.
- `execute_limits(&db, limits).await` changes the policy for subsequent records, even when the old
  policy prevents transaction submission.
- `close(&db).await` stops submissions from every clone, drains accepted requests and waits for the
  directory lock to be released. Dropping every handle also drains accepted work, but does not wait.

Both transaction claims and creation limits use `Limits`, a struct with semantic names for all 17
resources. Defaults are the generous, finite format ceilings. Override individual fields for your
application, and keep each transaction's claims within the database's current policy:

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

The API uses Tokio channels, but its futures can run on any executor. Blocking I/O, VM execution and
recovery run on one dedicated thread per database. The queue holds at most 64 waiting requests;
additional submissions await capacity. Execution is serial, with two durable publications per
record, not a parallel scheduler or a group-commit implementation.

The writer generates one conservative read/write table scope per declared table, including unused
declarations. Resource 7 (`manifest_scopes`) must allow that count in both the claims and the
policy. Recovery accepts this broad-table manifest profile; finer externally produced manifests are
not yet supported. The writer retains its log and historical versions without rotation or
reclamation.

`Ok(Receipt)` is a durability receipt, but its `Outcome` can be `Success` or `Aborted`. A semantic
abort discards all business writes, records the abort durably and consumes its sequence.
`Error::Rejected` means validation failed before sequencing; `Error::Storage` reports a startup or
pre-append system failure. `Error::Uncertain` means the request may have committed. After a system
failure, the writer stops; close and reopen it to recover before proceeding. `Error::Closed` on a
submission means that request did not execute.

Cancellation after enqueueing does not cancel a transaction. Never assume that a dropped future or
an uncertain result means no writes occurred. Retrying submits a new transaction; applications
needing deduplication must encode their request-ID checks and business writes in the same `tx!`
program.

Run the complete [balance-transfer example](examples/transactions.rs) against a **new** directory:

```sh
cargo run --example transactions -- /tmp/blop-example-db
```

The example generates database identities, creates a balances table, inserts two accounts, transfers
25 units atomically and reopens the database to verify both balances are 75. It uses Tokio's
current-thread executor and explicitly handles semantic aborts. It refuses to overwrite an existing
directory. A result-only verification transaction still enters the log; a public snapshot API is not
yet available.

## Transaction Construction

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

## Inputs and output

The macro returns `Result<Transaction, BuildError>`. `Transaction::program_bytes()` contains the
complete `BLOPVM01` container. `Transaction::argument_bytes()` contains the separate ISA 1 Arguments
encoding: a count followed by one length-prefixed value per capture. `into_parts()` returns both
owned byte vectors, program first.

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

## Types

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

Bounds are integer literals. Types obey the ISA limits: at most 16 nesting levels, 256 tuple fields,
65,535 rows, 16 MiB maximum encoded value size and 1,024 maximum canonical table-key bytes. Rows
cannot be nested, used in table schemas or supplied as captures. Runtime capture bounds and the 16
MiB total Arguments limit produce `BuildError` rather than panicking.

Ordinary integer literals default to I64; a typed context can select U64. Use `i64` or `u64`
suffixes when needed. Other numeric types, floating point and implicit integer conversions are not
supported. String and byte-string literals use their actual lengths as bounds. Local annotations can
declare larger or smaller bounds, such as `let mut text: string<128> = "";`. The VM checks
destination bounds when copying or producing a value; equal shapes need not have equal bounds.

## Statements

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

## Expressions

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

### Bounded scans

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

## Compile-time errors

Unknown names must be captured explicitly. Loops, arbitrary Rust calls, items, macros, attributes,
closures, recursion, casts and unsupported expressions are rejected at the source location. `abort`
and `unbounded` are reserved VM keywords and cannot name captures, tables or locals.

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

## Reference Execution

`blop_db::vm` implements all 48 ISA 1 opcodes as a single-threaded reference interpreter. Its reader
converts bytecode into typed Rust instruction enums and validates the entire program before
execution: operand layouts, types, forward control flow, reachability and definite register
initialization on every branch. The runtime never dispatches on raw opcode bytes.

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

Access manifests, their resource-7 count and full logged Transaction body validation are not part of
this layer. Other admission and runtime claims are checked against the historical policy. There is
no parallel scheduler or optimized execution path.

## Storage

`blop_db::storage` implements the physical storage formats in design appendices F and G, with
canonical key and MVCC encodings from appendix B. This is a low-level engine component, not a
client-side database write API.

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
- `open` restores only the selected checkpoint and validates reachable pages and committed log
  envelopes. Unpublished tails are discarded; corrupt committed data is an error, not a reason to
  fall back to an older manifest.

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

When `durable_sequence > checkpoint_sequence`, `storage::open` deliberately leaves the durable
suffix unexecuted. The higher-level `database::open` reads and validates that suffix and passes
every record to the reference execution layer before accepting new submissions. The storage layer
itself provides no scheduler, logical log writer or recovery coordinator. Cursor lifecycle APIs,
changefeeds, replication, GC and whole-file compaction remain unimplemented. Old manifests and
unreferenced pages are retained rather than reclaimed unsafely.

### Platforms

The storage module builds on Unix and Windows. A small internal platform module handles positional
I/O, file and directory synchronization, and same-directory file replacement. Page formats and the
publication sequence are unchanged. An OS lock protects the directory until the store and all views
and scans have been dropped.

**Windows support is experimental and has not been runtime-tested.** No Windows machine was
available. The workspace, including test targets, has been checked from Linux with
`cargo check --workspace --all-targets --target x86_64-pc-windows-gnu`. This checks compilation, not
linking, execution, filesystem behaviour or crash durability. Tests have run on Linux only; other
Unix platforms have not been runtime-tested either. Do not rely on the Windows backend for important
data until its filesystem and recovery behaviour has been tested on Windows.

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

Run the single-client KV comparison against redb and SQLite with
`cargo run --release --example kv_bench -- --dir /path/to/existing/directory`. See
[BENCHMARKS.md](BENCHMARKS.md) for separate buffered and durable measurements, reproducible
commands, local results and comparison limits. The harness uses temporary databases, not existing
application data.

## Development

Run `cargo test --workspace`, `cargo +nightly fmt --all -- --check` and
`cargo clippy --workspace --all-targets -- -D warnings`. Tests compare canonical byte encodings,
check all 48 ISA 1 opcodes and verify emitted control-flow graphs and definite register
initialization. VM tests execute compiled programs through actual storage, checking rollback,
read-your-writes, historical schemas and limits, scan merging, resource-limit precedence and
repeatable replay from saved bytes. Storage tests also cover binary conformance, malformed and
truncated objects, model-checked tree edits, retained roots, MVCC filtering and injected
interruptions at publication boundaries. These filesystem interruption tests do not simulate
hardware power loss. Async writer tests cover concurrent submissions, queue backpressure,
cancellation, shutdown, durable receipts, rejected requests, semantic rollback and post-checkpoint
recovery without applying increments twice.

To check the Windows code without running it, install the target with
`rustup target add x86_64-pc-windows-gnu`, then run
`cargo check --workspace --all-targets --target x86_64-pc-windows-gnu`. A native Windows test run is
still required before claiming tested Windows support.
