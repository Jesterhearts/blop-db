# Deterministic Transaction Program Database

_Design specification_

_A Rust-native embedded database built around stable transaction bytecode, a total-order log,
deterministic parallel execution, MVCC, and prefix visibility._

Version 1 uses self-committing WAL groups. Appendices E and I define log durability and recovery;
appendix G defines checkpoint and retention-metadata publication. This is one storage protocol,
shared by creation, opening, reads and writes.

## Executive summary

This database treats a transaction as a small deterministic program instead of a client-side
read/modify/write sequence. The application passes all external inputs as transaction arguments. The
transaction program may compute keys from database values, but its target tables and conservative
access scopes must be known before sequencing. Known keys use point scopes; computed keys and range
reads use whole-table scopes. The program cannot perform I/O, call functions, loop, recurse, read
the system clock, generate random values, or perform unbounded work.

The transaction DSL compiles to a stable, versioned bytecode. The database appends each bytecode
program and its arguments to a durable log. The same log records schema operations and semantic
limit changes using stable descriptions. Each record receives one monotonically increasing sequence
number. Sequence numbers define the canonical serial order. Only durably logged records may execute.

The database may execute transactions out of log order when it can prove that this does not change
their results. A scheduler derives data dependencies from each transaction's verified read and write
scopes. Independent work can run in parallel within a bounded distance of the visibility frontier.
The result must always be identical to sequential interpretation of the log.

Each transaction writes first to a private overlay. A successful transaction installs versioned
values tagged with its sequence number. An aborted transaction discards its overlay and produces no
versions. Later transactions never consume unresolved tentative state, so an abort does not require
cascading rollback.

External readers see only a contiguous prefix of the log. A visibility frontier records the greatest
sequence number through which every record is durable and resolved. MVCC lets the engine execute and
materialize later independent transactions before they become externally visible. This gives simple
snapshots, recovery, checkpoints, replication, and changefeed cursors at the cost of possible
visibility head-of-line blocking behind a slow earlier transaction.

Recovery restores a checkpoint and replays every subsequent durable record, including records with
cached outcomes. Garbage collection retains all versions above the frontier and any older history
needed by active retention claims. Snapshots are revocable; checked-out changefeed cursors durably
protect their unacknowledged history until advanced or explicitly released.

| **Area**             | **Core decision**                                                                        |
| -------------------- | ---------------------------------------------------------------------------------------- |
| Transaction model    | Deterministic programs, not precomputed writes.                                          |
| Program format       | Stable, versioned bytecode with permanently defined instruction semantics.               |
| Ordering             | One durable total-order transaction log.                                                 |
| Parallelism          | Verified key or table scopes; execute independent work within a bounded window.          |
| Rollback             | Private write overlay; abort by discarding it.                                           |
| Storage visibility   | MVCC versions tagged with transaction sequence numbers.                                  |
| External consistency | Readers see only a contiguous resolved log prefix.                                       |
| Recovery             | Replay every durable record after the checkpoint; never skip using cached outcomes.      |
| Schema and limits    | Stable descriptions in the log; administrative changes act as barriers.                  |
| Retention            | Preserve above-frontier versions and history protected by snapshots and durable cursors. |
| Derived systems      | Commit derived data and its source watermark atomically, then acknowledge the cursor.    |
| Retry ownership      | The caller manages deduplication and uncertain submission outcomes.                      |

## 1. Scope

This document defines the logical architecture, correctness model, and version 1 binary formats of
an embedded database for Rust applications. It defines transaction semantics, bytecode rules,
scheduling, MVCC, visibility, rollback, durability, recovery, and physical storage. Appendices A
through I are normative: they specify the bytes and validation rules needed to implement compatible
readers, writers, interpreters, and recovery tools without relying on Rust representations.

The design is for a single database instance with one canonical transaction log. It may use many
worker threads. Replication may copy and replay the canonical log, but distributed consensus and
multi-primary writes are outside this design.

### 1.1 Normative language

The word "must" states a required property. The word "may" states an optional implementation or API
choice. The word "should" states a recommendation. A compliant implementation must preserve the
stated observable semantics even when it uses a different internal representation.

The logical rules apply to every implementation. A version 1 format implementation must also use the
layouts in the appendices when persisting or exchanging those formats. In-memory structures, page
split choices, and scheduling need not be byte-for-byte identical. Version 1 is a new format, not a
compatibility claim about an existing implementation.

## 2. Design goals

- Make read/modify/write operations express application intent directly, such as "A += 1", without
  client-side compare-and-swap loops.
- Give every transaction one stable serialization position before it becomes externally visible.
- Execute independent transactions in parallel without changing the result defined by log order.
- Keep transaction execution deterministic and statically bounded.
- Support computed keys and bounded range reads through conservative table-wide scheduling scopes.
- Make transaction abort cheap and local. An abort must not require undoing shared state or rolling
  back dependent transactions.
- Represent every public snapshot with a single sequence number.
- Use the same sequence numbers for recovery, checkpoints, replication, changefeeds, and derived
  indexes.
- Keep the persisted transaction format independent of Rust ABI, application code, and compiler
  versions.
- Preserve schema and semantic limit history independently of application and process configuration.
- Make correctness testable against a simple sequential reference interpreter.

### 2.1 Non-goals

- The transaction language is not a general-purpose stored procedure language.
- The transaction VM does not perform network, file, clock, random, operating-system, or application
  callbacks.
- The core transaction VM does not select target tables from database values or access data outside
  its verified scopes.
- The core design does not require SQL, a query planner, joins, or an object-relational model.
- The core design does not require distributed consensus, multi-primary replication, or
  cross-machine transaction coordination.
- Compression, encryption, and physical format migration are not included in version 1.

## 3. Conceptual model

The database has the following logical pipeline:

```text
Application DSL
      |
      v
Versioned transaction bytecode
      |
      v
Durable log: programs + catalogue operations + limit changes
      |
      v
Dependency scheduler -> transaction VM workers
      |
      v
MVCC materialized state + catalogue + limits + visibility frontier
```

The log defines meaning. Parallel execution is an optimization. The storage engine materializes the
result. A correct implementation must produce the same externally visible data, catalogue, policy,
and outcomes as a sequential interpreter that executes every logged record in increasing sequence
order.

### 3.1 Core correctness rule

> Sequential-equivalence rule: For any durable transaction log, the database must expose exactly the
> state and outcomes that result from executing that log from left to right with the reference
> bytecode and administrative semantics.

This rule is the primary correctness oracle. The scheduler, MVCC implementation, caching, batching,
and physical page writes may change, but they must not change this result.

## 4. Data model

The core database is an ordered collection of tables. A table maps a canonical key encoding to a
canonical value encoding. Keys are ordered by their encoded bytes. The transaction VM works with
typed values whose on-disk representation is stable. The database enforces each table's key and
value schemas; type correctness must not depend on the current application's Rust definitions.

### 4.1 Stable types

ISA 1 supports Unit, Boolean, I64, U64, byte strings, UTF-8 strings, and positional tuples, with
bounded row sets as VM results. Appendix B defines their encodings and comparison rules. Additional
types may be added by later ISA versions without changing existing semantics.

| **Type property**  | **Requirement**                                                                                               |
| ------------------ | ------------------------------------------------------------------------------------------------------------- |
| Integer arithmetic | Each opcode states whether overflow aborts, saturates, or wraps. There is no implicit host-language behavior. |
| String comparison  | The ISA defines byte or Unicode comparison explicitly. Locale-dependent comparison is not implicit.           |
| Floating point     | Exclude from the core ISA unless bit-level arithmetic and comparison behavior is fully specified.             |
| Composite records  | A higher-level schema layer may map record fields to stable logical keys or to a stable record encoding.      |

### 4.2 Key addresses

Every target table must be resolvable before a transaction is sequenced. A key may be resolved from
constants and arguments before sequencing, or computed deterministically from database values during
execution. Computed keys must conform to the target table's key schema and the applicable size
limits.

```text
Point scopes:
    balances[$from]
    balances[$to]
    counters["global"]

Table-wide scopes for computed targets:
    let owner = items[$item].owner
    users[owner] += 1

    let key = sha256(load(A))
    table[key] = 1
```

The validator must cover every possible access with a known key or whole-table scope. A computed
read declares a read scope for the whole target table. A computed write declares a write scope for
that table, plus a read scope if it consumes the target's prior value or existence. Reads used to
compute the address also contribute their own scopes. Section 8 defines the dependency rules.

Table-wide scope is a conservative scheduling declaration, not an instruction to touch every key. It
includes absent keys that earlier transactions might create. Different address expressions that
produce the same canonical key must use the same overlay entry and stored version history.

### 4.3 Range access

Transaction range reads use a whole-table read scope. Their target table must be known before
sequencing; their endpoints may be constants, arguments, or computed values. Limits on logical rows
examined, result count, and bytes must be established before sequencing. Unbounded transaction scans
are prohibited.

A range reads the log-prior MVCC view merged with the transaction overlay, including its insertions
and deletions. It selects keys in canonical byte order and applies its result limit to that logical
view, not to physical entries before version filtering. Appendix C.5 defines endpoint inclusion,
ordering, empty-range results, and resource-limit failures for ISA 1, which has no predicate filter.

Table-wide dependencies account for earlier inserts and deletes even when no matching key currently
exists. Later versions are excluded by the transaction's sequence bound. External snapshot reads may
provide ordinary ordered scans without joining transaction scheduling, subject to snapshot
revocation.

### 4.4 Missing keys and creation

A point LOAD of a missing key must cause a deterministic semantic abort. An overlay or stored
tombstone means that the key is missing; a read must not fall through to an older value. There is no
optional result from a point LOAD. This rule applies equally to known and computed addresses.

Creation must be a defined operation rather than a load of an absent value. INSERT creates a value
only if the key is absent and aborts if it already exists. Unconditional STORE may create or replace
a value without inspecting the old value. An unconditional DELETE records a tombstone without
requiring an existing value. Existence-checked operations declare read access as well as write
access, even when their replacement value is constant. Every created or replacement value must
satisfy the table's schema.

External snapshot lookup may report absence without aborting anything. Empty-range behaviour is
defined by the range opcode rather than by the point-LOAD rule.

### 4.5 Table catalogue and schema history

The catalogue maps stable table identities to names, key encodings, value schemas, and lifecycle
state. Table identities must never be reused within a database history. Recreating a dropped table
with the same name creates a different identity; retained bytecode must not silently address it.

Every schema operation, including table creation, rename, and drop, must enter the canonical log in
a versioned administrative format with stable schema descriptions. User bytecode cannot modify the
catalogue directly. Rust bindings must be checked against the persisted schema rather than redefine
it when the database opens.

The initial design keeps key and value schemas immutable for a table identity. An incompatible
change requires a new table and an explicit migration using bounded transactions. Any later schema
evolution operations must have permanent, versioned semantics that preserve interpretation of
retained history.

Catalogue changes use the administrative barrier in section 7.3. A logical drop changes catalogue
state rather than deleting all rows in one unbounded transaction. Physical reclamation waits for
retention claims. Snapshots use the catalogue at their sequence, and changefeeds include lifecycle
events so derived systems can apply drops and other changes correctly.

Checkpoints must include the catalogue and all historical schema descriptions needed to interpret
retained versions, outcomes, and log records. The initial checkpoint defines the initial catalogue
and semantic limit policy without relying on later process configuration.

## 5. Transaction DSL

The DSL is an application-facing way to construct transaction programs. The DSL syntax is not part
of the durable format. Only the compiled bytecode, stable metadata, and transaction arguments define
persisted behavior.

### 5.1 Example

```rust
db.execute(tx! {
    require(balances[$from] >= $amount);
    balances[$from] -= $amount;
    balances[$to] += $amount;
    return balances[$from];
}, args! {
    from: from_id,
    to: to_id,
    amount: amount,
})?;
```

The program reads the current log-prior balance for the source account, checks a condition, updates
two known keys, and returns a value. The client does not read either balance before submission and
does not perform a compare-and-swap retry.

### 5.2 Allowed computation

- Load a value from a known or computed key within a declared table.
- Load a transaction argument or constant.
- Perform deterministic arithmetic, comparison, Boolean, bitwise, and bounded data operations
  defined by the ISA.
- Branch forward based on a deterministic condition.
- Insert, write, or delete a known or computed key within the verified access scopes.
- Read a bounded range through a table-wide read scope.
- Abort the transaction when a condition is not met.
- Return a bounded value to the caller.

### 5.3 Prohibited computation

- Loops, backward jumps, recursion, or any other unbounded control flow.
- Function calls, including calls into application Rust code.
- Network, file, device, environment, process, or other I/O.
- System clock reads, random number generation, thread identity, or other nondeterministic inputs.
- Target tables derived from database reads, or accesses outside the verified manifest.
- Unbounded allocation or data growth.
- Undefined or implementation-dependent arithmetic behavior.

### 5.4 Arguments

All information that comes from outside the database must be passed as an argument. This includes
timestamps, random identifiers, user input, request identifiers, and any value produced by
application code. Arguments are part of the durable transaction record.

### 5.5 Control flow

The bytecode may use conditional forward branches. It must not contain a backward branch. The
validator must reject control-flow graphs that contain a cycle. This gives each accepted program a
finite maximum instruction count.

### 5.6 Read-your-own-writes

Within one transaction, a read of a key after a write to that key must observe the value in the
transaction's private overlay. If the overlay does not contain the key, the read uses the latest
successful log-prior version for that key. An overlay deletion makes a point LOAD abort; it does not
allow fallback to a stored value. Range reads merge the overlay before selecting their results.

## 6. Versioned transaction bytecode

The persisted transaction program is a compact bytecode with a stable instruction set architecture,
or ISA. Each ISA version is permanent. Once an opcode is published, its type rules, arithmetic
behavior, error behavior, comparison behavior, and encoding must not change.

### 6.1 Log record envelope

The following is a logical sketch, not a binary layout. Appendices C, D, and E define the complete
transaction body, administrative bodies, and checksummed log envelope, respectively.

```text
LogRecord {
    sequence:        u64
    format_version:  u16
    body:            Transaction | CatalogueOperation | SetLimits
    checksum:        CRC-32C
}

Transaction {
    isa_version:     u16
    program_bytes:   bytes
    arguments:       typed values
    access_manifest: verified read/write scopes
    resource_claims: bounded sizes/counts
}
```

CatalogueOperation and SetLimits have stable, versioned encodings for their operation and arguments.
Catalogue operations carry stable table identities and schema descriptions where required. SetLimits
carries the new semantic limit policy. These records use the same sequence space, durability rules,
outcomes, and visibility frontier as transaction programs.

The access manifest may be generated by the compiler and independently verified by the database
validator. The database must never trust client-supplied metadata that could make dependency
analysis unsound.

### 6.2 Instruction groups

| **Group**           | **Examples**                           | **Purpose**                                                          |
| ------------------- | -------------------------------------- | -------------------------------------------------------------------- |
| Data                | LOAD, INSERT, STORE, DELETE            | Read and modify known or computed keys within verified scopes.       |
| Range               | SCAN_BOUNDED                           | Read a bounded logical range through a whole-table read scope.       |
| Arithmetic          | ADD_CHECKED, SUB_CHECKED               | Perform deterministic numeric operations.                            |
| Comparison          | EQ, LT, GE                             | Produce Boolean values.                                              |
| Boolean/bitwise     | AND, OR, XOR, NOT                      | Perform deterministic logic.                                         |
| Control             | JUMP_IF_FALSE_FORWARD, RETURN          | Provide bounded conditional execution.                               |
| Validation          | REQUIRE                                | Abort the transaction when a condition is false.                     |
| Structured mutation | SET_INSERT, MAP_INSERT, APPEND_BOUNDED | Optional typed operations with explicit bounds and stable semantics. |

This table describes instruction families, including possible extensions. Appendix C is the
exhaustive opcode allocation for ISA 1. Sets, maps, and structured mutation opcodes are not in ISA
1; bounded byte concatenation and tuple construction cover its composite operations.

### 6.3 Validation before sequencing

The database must validate a program before it receives a sequence number. Validation includes:

- The ISA version is supported.
- The bytecode decodes without ambiguity.
- All register and local-variable uses are valid.
- All control-flow targets are forward and in bounds.
- The control-flow graph has no cycle.
- Every opcode uses valid argument and value types.
- Every target table and its schema can be resolved before sequencing.
- Known addresses are canonicalized and aliases combine their read and write declarations.
- Computed keys have the target key type and enforceable size bounds.
- The access manifest conservatively covers every possible access on every branch, including
  computed addresses, ranges, existence checks, and address-producing reads.
- Bounds on instructions, logical accesses, ranges, arguments, values, locals, writes, and output
  satisfy the semantic policy applicable at the transaction's log position.

A validation failure is a submission error. It does not enter the transaction log and does not
consume a sequence number. Validation and sequencing must agree on the applicable catalogue and
policy. A validated but unsequenced submission must be revalidated if an administrative barrier
changes either one before it is sequenced. Runtime checks enforce bounds that depend on loaded or
computed values; they do not replace pre-sequencing validation of the program and its declared
bounds.

### 6.4 ISA compatibility

A database process must support every ISA and administrative format version needed to replay its
retained log, and every schema description needed to interpret retained data. A durable checkpoint
may allow older log segments to be removed. Removing old log segments may therefore reduce the set
of historical ISA versions that the active database must retain, subject to backup, audit, and
replication retention rules.

## 7. Total-order transaction log

The transaction log is the canonical ordered history of transaction programs, catalogue operations,
and semantic limit changes. A sequencer assigns each accepted record the next monotonically
increasing sequence number. Sequence order is the database's serial order; access scopes affect
execution dependencies, not that order.

### 7.1 Log properties

- Sequence numbers are unique and strictly increasing within one database history.
- A sequence number never changes after assignment.
- Once a transaction is durably sequenced, client cancellation or disconnection does not remove it
  from history.
- A semantic abort remains in the log and acts as an identity state transition.
- The log must detect torn, truncated, or corrupted records with length framing and checksums or
  equivalent protection.

### 7.2 Transaction states

| **State**         | **Meaning**                                                                                      |
| ----------------- | ------------------------------------------------------------------------------------------------ |
| Validated         | The program is valid but has not yet received a sequence number.                                 |
| Sequenced         | The program has a permanent log position.                                                        |
| Durably logged    | The complete log prefix through this record survives the selected durability failure model.      |
| Executing         | A VM worker evaluates a durable program with resolved dependencies and an execution-window slot. |
| Resolved: success | The complete overlay and deterministic result are installed and available to dependents.         |
| Resolved: abort   | Execution reached a deterministic abort condition and produced no database versions.             |
| Visible           | The visibility frontier has advanced through this sequence number.                               |

"Resolved" means that execution reached a terminal semantic result. A system failure such as
unavailable storage is not a semantic abort. It leaves the transaction unresolved until the system
retries or recovers.

A record must reach Durably logged before Executing. Neither a transaction nor an administrative
operation may execute from an unflushed suffix of the log. Administrative operations have their own
deterministic success or abort outcomes and occupy frontier positions in the same way as programs.
The isolated import-validation interpreter in H.1 is not transaction dispatch and cannot publish
results or install effects in the running database.

### 7.3 Administrative barriers

Catalogue operations and SetLimits records are sequencing and execution barriers in the initial
design. For an administrative record at sequence N, the sequencer must stop assigning later
positions until the record resolves. The record must be durable and the visibility frontier must
reach N - 1 before the operation executes. It then applies its metadata change atomically, or aborts
without changing that metadata, and advances the frontier through N before sequencing resumes.

The operation itself uses the preceding catalogue and policy. A successful change applies only to
later records. Submissions waiting across the barrier must be validated against the resulting
catalogue and policy before receiving a position. Recovery follows the same rule. This prevents
physical completion order from selecting which schema or limits a transaction uses.

## 8. Static access manifest

Before sequencing, the database verifies a conservative manifest covering every possible access. A
manifest has read and write scopes of these forms:

```text
Key(table_id, canonical_key)
Table(table_id)
```

A Table scope covers every possible key in the table, including absent keys. It must not be expanded
only into currently existing keys. Read and write scopes are separate: a table-wide write does not
automatically make a point read table-wide. The access modes below describe the required
declarations.

| **Access mode** | **Meaning**                                                                 |
| --------------- | --------------------------------------------------------------------------- |
| Read            | The program may consume log-prior values or existence state in the scope.   |
| Write-only      | The program may write without inspecting prior target values or existence.  |
| Read/write      | The program may inspect prior values or existence and may produce writes.   |
| Delete-only     | The program may delete without inspecting prior target values or existence. |

The manifest is conservative across branches. If one branch may read or write a key, the manifest
includes that access even when a particular execution does not take the branch.

Known keys use point scopes. Computed reads and range reads use whole-table read scopes. Computed
writes use whole-table write scopes. A computed operation that checks prior values or existence also
needs a whole-table read scope. Reads that compute an address must be included independently.

For `let owner = items[$item].owner; users[owner] += 1`, the read scopes are Key(items, $item) and
Table(users), and the write scope is Table(users). The broad scope restricts parallelism but the
transaction still touches only the actual computed user key. A program dominated by computed
read/write accesses may approach serial execution within the affected table.

### 8.1 Dependency rule

A later transaction depends on an earlier transaction when a possible write scope of the earlier
transaction overlaps a possible read scope of the later transaction. An already resolved predecessor
satisfies its dependency. The overlap rules are:

| **Earlier write** | **Later read**             | **Overlap**                                   |
| ----------------- | -------------------------- | --------------------------------------------- |
| Key(T, a)         | Key(T, b)                  | Only when a and b are the same canonical key. |
| Key(T, a)         | Table(T)                   | Always.                                       |
| Table(T)          | Key(T, b)                  | Always.                                       |
| Table(T)          | Table(T)                   | Always.                                       |
| Scope in T        | Scope in a different table | Never.                                        |

```text
Log:
    100  WRITE A
    101  READ/WRITE A
    102  WRITE B
    103  READ A

Required dependencies:
    100 -> 101 -> 103

Transaction 102 is independent.
```

An earlier read does not need to block a later write because MVCC can ensure that the earlier read
never observes a later sequence. Two write-only operations may also execute independently when each
can materialize its own sequence-tagged version without reading the other. This is more precise than
treating every overlapping write as an execution conflict.

These rules also apply to table scopes. A range reader waits for prior possible writers throughout
its table, including inserts into previously absent keys. Later blind writers need not wait for the
range reader because its MVCC view excludes their versions. All dependency edges still point from
lower to higher sequence numbers.

### 8.2 Aborted and non-writing predecessors

A later reader must wait until each relevant earlier possible writer reaches a terminal state. A
successful transaction may omit a declared write because its branch was not taken. Both that case
and an abort require considering older writers rather than assuming the latest declared writer
produced a version. The reader never consumes tentative overlay state.

```text
100: A = 1                   // delayed
101: if flag { A = 2 }        // succeeds with flag false
102: return A
```

Transaction 102 must still wait for 100 even after 101 resolves without writing A. The same rule
applies when 101 declared a table-wide write or aborted. Waiting only for the most recent declared
writer is insufficient when blind writers can execute independently.

## 9. Dependency scheduler

The scheduler converts log order and access manifests into executable dependencies. It may dispatch
a transaction only after its log prefix is durable, its required dependencies have resolved, and its
execution-window and byte-budget requirements are satisfied. Within those constraints, it does not
need to wait for unrelated earlier sequence numbers.

Dependency discovery and writer registration must process manifests in log order. A later
transaction must not finish dependency discovery while an earlier manifest is still unregistered.
Computed addresses do not introduce undeclared runtime locks: the verified table scopes already
cover their dependencies.

### 9.1 Example

```text
Log order:
    100: X += 1
    101: A += 1
    102: B = 7
    103: A *= 2
    104: C += 1

Data dependency:
    101 -> 103

Possible physical execution:
    worker 1: 100
    worker 2: 101 -> 103
    worker 3: 102
    worker 4: 104
```

The physical completion order may differ from log order. This is safe because sequence-tagged
versions and the visibility frontier preserve the canonical serial meaning.

### 9.2 Scheduling granularity

The simplest scheduler treats each transaction as one unit. A more aggressive implementation may
split one program into independent actions, but only if it preserves the transaction's private
overlay semantics, atomic result, and sequential-equivalence rule. Transaction-level scheduling is
sufficient for the logical design.

### 9.3 Bounded execution window

Let F be the visibility frontier, D the highest contiguous durable log position, and W a positive
configured execution-window size. A transaction at N may dispatch only when:

```text
F < N <= min(D, F + W)
```

Use overflow-safe sequence arithmetic. Enforce byte budgets for decoded programs, dependency state,
overlays, pending outcomes, and above-frontier versions as well as a transaction-count window.
Admission and resource reservations must preserve progress for the oldest unresolved record; later
work must not consume all capacity needed to resolve it.

Bound the validated submission queue and the assigned-but-not-visible log backlog separately. A
bounded execution window alone does not prevent an unbounded on-disk backlog. Apply backpressure
before sequencing; do not convert resource pressure into an abort of a sequenced transaction.

### 9.4 Semantic operation optimization

The VM may know that some mutations are associative or commutative, such as integer addition or
maximum. This information may support batching or reduced storage work. Such optimization is
optional. It must preserve every transaction's return value, abort result, version at each visible
sequence boundary, resolved changefeed effects, and final state. It must also preserve logical
resource-limit outcomes. The database must not reorder same-key operations merely because their
final aggregate value would match.

## 10. Transaction execution and rollback

A VM worker never changes shared visible state while it interprets a transaction. It reads stable
log-prior versions and records writes in a private transaction overlay.

### 10.1 Read algorithm

For a transaction with sequence N, a read of key K uses this order:

1. Establish that all required earlier writer dependencies have resolved. The scheduler normally
   guarantees this before dispatch, including dependencies covered by table-wide scopes.

1. Resolve and canonically encode K, whether supplied directly or computed, and enforce its type and
   size bounds.

1. If K is present in the overlay, use its value or tombstone. Otherwise, select the newest
   successful stored version of K whose sequence is less than N.

1. If there is no value, or the selected entry is a tombstone, abort the point LOAD. Otherwise
   enforce the applicable load limits before returning the value.

Never read a stored version whose sequence is greater than or equal to N. Range reads establish the
same readiness, then merge the sequence-bounded MVCC view and overlay as described in section 4.3.

### 10.2 Successful execution

When the program returns successfully, the database atomically installs the overlay as versions
tagged with the transaction sequence number and records the transaction's result and actual written
keys, including deletions. Only then may it mark the transaction resolved-success, wake dependents,
or advance the frontier. Readers and recovery must never observe a successful transaction with only
part of its overlay installed.

### 10.3 Semantic abort

A missing point-LOAD key, REQUIRE failure, checked arithmetic failure, failed existence check,
deterministic resource-limit failure, explicit ABORT instruction, or other defined runtime condition
resolves the transaction as aborted. The database discards the private overlay. The transaction
produces no database versions.

```text
Before TX 100:
    A = 100
    B = 980

TX 100 overlay:
    A = 50
    B = 1030
    REQUIRE B <= 1000   -> false

Result:
    TX 100 = ABORTED
    overlay discarded
    shared state unchanged
```

An aborted transaction still occupies its log position. For state evolution it is equivalent to an
identity operation. It therefore does not create a gap in the visibility frontier.

### 10.4 No cascading rollback

A transaction must not execute using unresolved tentative state from an earlier transaction. A
dependent transaction waits for the predecessor to resolve. If the predecessor aborts, the dependent
transaction reads the next earlier successful version. Because no dependent transaction consumes
tentative state, an abort never requires cascading rollback or re-execution.

### 10.5 Irrevocability

Once a successful transaction has resolved and its result is available to dependent transactions, it
is logically irrevocable. The database does not support removing it from history. A later
compensating transaction may reverse an application-level effect. Client disconnection after
sequencing does not cancel a transaction.

## 11. MVCC materialized state

The storage layer keeps multiple versions of a key. Each successful transaction version carries the
transaction sequence number that produced it. An implementation may store the sequence in the key,
in version metadata, or in another equivalent structure.

```text
A:
    @87   = 10
    @100  = 11
    @103  = 22

B:
    @91   = 5
    @101  = 6
```

A snapshot at sequence S reads the newest successful version whose sequence is less than or equal to
S. A tombstone at that position means absence. An aborted sequence has no version and is skipped
naturally. Catalogue state and schema interpretation must also correspond to S.

### 11.1 Internal and external reads

| **Read type**              | **Maximum visible sequence**                                                                    |
| -------------------------- | ----------------------------------------------------------------------------------------------- |
| External snapshot          | The snapshot sequence, normally the current visibility frontier.                                |
| Transaction execution at N | Log-prior versions below N, including successful versions above the public visibility frontier. |
| Transaction overlay read   | The transaction's own tentative writes take precedence over stored versions.                    |

This distinction lets execution run ahead of publication within its configured window. A later
transaction may use a successful relevant predecessor even while an unrelated earlier transaction
keeps the public frontier behind both of them.

## 12. Visibility frontier

The visibility frontier F is the greatest sequence number such that every log record with sequence
less than or equal to F is durable and has reached a terminal semantic state: success or abort.
Successful records have their complete data or metadata effects installed. Because dispatch requires
a durable log prefix, F must never exceed the durable frontier D. Ordinary external readers may not
observe data or catalogue state above F. Administrative records occupy frontier positions too.

### 12.1 Prefix visibility invariant

> Prefix visibility invariant: Every externally observable database state is exactly the state
> produced by one contiguous prefix of the transaction log.

This makes a snapshot identifier a single integer. It also makes the same integer useful as a
replication position, changefeed cursor, checkpoint boundary, and derived-index watermark.

### 12.2 Frontier advancement

```text
Completion state:
    100  done
    101  done
    102  running
    103  done
    104  done

visibility frontier = 101

When 102 resolves, the frontier may advance directly to 104.
```

An implementation may track a completion bitmap or ring buffer for the outstanding execution window.
Frontier advancement scans forward from the current frontier until it reaches the first unresolved
sequence.

### 12.3 Head-of-line blocking

The main cost of prefix visibility is visibility head-of-line blocking. A slow transaction at
sequence N can prevent later completed transactions from becoming externally visible even when they
are independent. Those later transactions may still execute and may serve as inputs to their own
log-later dependents.

The transaction language reduces this risk because accepted programs have bounded instructions,
logical accesses, ranges, writes, and values, with no external I/O, calls, or loops. Table-wide
access scopes do not authorize unbounded work. The execution window and byte budgets bound
accumulation behind a hole. These are work and storage-pressure controls, not hard wall-clock
latency guarantees: storage waits, system failures, and scheduling delays can still stall
publication.

## 13. External read and transaction API

External reads are snapshot reads. They do not join the transaction log because they do not change
state. A snapshot captures one sequence number. Its reads remain at that sequence until the snapshot
is released or explicitly revoked by the engine's retention policy.

### 13.1 Suggested Rust shape

```rust
let snapshot = db.snapshot()?;
let value = snapshot.get(BALANCES, account_id)?;
println!("snapshot sequence = {}", snapshot.sequence());

let result = db.execute(tx! {
    require(BALANCES[$from] >= $amount);
    BALANCES[$from] -= $amount;
    BALANCES[$to] += $amount;
    return BALANCES[$from];
}, args! {
    from: from_id,
    to: to_id,
    amount,
})?;
```

The exact Rust syntax is not normative. The important rule is that state-dependent writes run inside
bytecode. A client that reads a snapshot and later submits a write based on that value is
responsible for expressing any required precondition in the transaction program rather than relying
on the earlier snapshot still being current.

### 13.2 Submission and completion

The API may expose a two-step form for callers that need explicit control: submit a validated
program and receive its sequence receipt, then wait for the transaction to become visible and
receive its terminal result. A convenience execute call may perform both steps. The default
"committed" result must mean that the transaction is durable and the visibility frontier has
advanced through its sequence.

The API must distinguish definite rejection before sequencing from an uncertain submission outcome.
A receipt reported as durable requires a durable prefix through its sequence. Interruption before a
receipt reaches the caller does not prove rejection: the transaction may already be durable and may
execute. Cancellation is not a way to remove a sequenced record.

### 13.3 Caller-managed deduplication

The database does not automatically deduplicate submissions. Repeating a program and its arguments
creates a new transaction. The caller owns request identity, retry policy, result retention, and
deduplication. A snapshot check followed by an unguarded submission is not an atomic deduplication
protocol.

A caller may manage a request table and include an INSERT at a known request-ID key in the same
transaction as its business writes. INSERT aborts on an existing key, so a duplicate discards the
entire business overlay. The caller can read the stored result externally after the duplicate abort.
The request record should bind the identifier to the request content and result so identifier reuse
for a different request can be detected. Request records must remain for the caller's promised retry
interval.

This pattern provides at most one successful application, not automatic preservation of aborted
attempts. An abort discards the request record too. Applications that must remember expected
business rejections can represent them as successfully committed result records rather than VM
aborts.

### 13.4 Revocable snapshots

Every successful snapshot read must use the original sequence. After revocation, new reads fail with
an explicit snapshot-revoked error; they must not silently use a newer sequence. A range iterator
must report revocation as an error rather than ordinary end-of-results. In-flight reads must either
complete against their protected original view or fail explicitly.

Reclamation must wait until in-flight reads and borrowed storage views are safe, even after logical
revocation. Revoking a snapshot releases only its retention claim, not any durable cursor or other
claim protecting the same history. A bulk build whose snapshot is revoked must not publish its
partial output as a complete snapshot.

## 14. Durability and crash recovery

The durable transaction log is the authoritative description of state changes. The MVCC store is a
materialized representation that can be reconstructed from a durable checkpoint plus every later
durable record. Recovery includes catalogue operations and limit changes, not just bytecode
programs.

### 14.1 Durable sequencing

A record must not execute or be reported as durably sequenced until the complete log prefix through
its sequence survives a crash according to the selected durability policy. Group commit may batch
several records into one storage flush without changing their sequence order. Dependency
registration may precede the flush, but dispatch must wait for it. No snapshot, completion
notification, or changefeed may publish a record from an unflushed suffix.

### 14.2 Crash during execution

A crash may occur after bytecode is durable but before execution or materialization finishes.
Recovery replays every durable record after the checkpoint, including successful and aborted records
whose outcomes were previously known. Deterministic bytecode, fixed arguments, and the historical
catalogue and limit policy guarantee the same semantic result.

### 14.3 Crash during materialization

Physical storage must not expose a partially installed overlay as a successful transaction. A
copy-on-write root update, append-only version installation with an atomic completion record, or
another atomic publication method may enforce this rule. Incomplete materialization found after a
crash is discarded or overwritten by deterministic replay. Recovery must begin from the checkpoint
view with all later materialization excluded, rather than replay increments against a newer state
that already includes them.

### 14.4 Resolution metadata

The engine records successful and aborted outcomes, returned values or abort reasons, and references
to actual written keys and tombstones for changefeeds. Administrative outcomes also identify their
catalogue or policy effects. Records must be tied to the exact canonical log record and sequence.

A cached outcome must never cause recovery to skip a post-checkpoint record or mark it resolved
before replay installs its effects. Outcomes after the checkpoint are regenerated by replay;
incomplete or corrupt copies can be discarded when the durable log supplies their reconstruction.

Outcomes needed by a retained cursor at or below the checkpoint are retained history, not merely
disposable recovery caches. Before their source log can be removed, those outcomes, their referenced
MVCC versions, and required schema descriptions must be durably recoverable. Missing or corrupt
retained history may be reconstructed only from sufficient retained sources; otherwise the engine
must report corruption rather than silently omit feed records.

### 14.5 Recovery procedure

1. Open and verify the newest durable checkpoint and crash-safe retention metadata.

1. Restore its data, catalogue, semantic limit policy, retained historical versions and outcomes,
   and visibility frontier C. Restore durable cursor registrations before allowing reclamation.

1. Open the log immediately after C and verify sequence continuity, framing, and checksums. Recover
   the valid durable prefix according to the log protocol; distinguish an incomplete append tail
   from corruption of retained history.

1. Exclude all materialization after C from the initial recovered view. Do not seed scheduler
   completion state from cached post-checkpoint outcomes.

1. Replay every durable record after C, including previously successful or aborted transactions,
   catalogue operations, and limit changes. Use the normal scope dependencies, execution window, and
   administrative barriers, or the equivalent sequential interpreter.

1. Install replayed effects and regenerate outcomes. Advance the frontier only through the
   contiguous resolved prefix.

1. Reclaim incomplete unreferenced materialization left by the crash.

## 15. Checkpoints and retention

A checkpoint is a durable materialized database state at a visibility frontier F. It must represent
exactly the state produced by the log prefix through F. A checkpoint never includes a hole or a
version from a sequence greater than F in its logical recovery view. Shared physical storage may
contain later versions, but they must remain excluded until reconstructed by post-checkpoint replay.

The checkpoint includes the catalogue and semantic limit policy at F. It also retains or durably
references historical versions, tombstones, outcomes, and schema descriptions required by active
retention claims. A latest-values-only checkpoint cannot replace pinned changefeed history. Publish
the checkpoint only after all referenced data is durable, and only then consider log truncation.

### 15.1 Log truncation

After a checkpoint is durable, the database may remove log records at or below the checkpoint
sequence when no retained feature needs those records. Resolved-feed history may be served from
retained MVCC versions and outcomes without retaining the original bytecode. Logical feeds, log
replicas, audit history, and backups require their own log retention claims. Durable cursor checkout
and reclamation decisions must be coordinated so history cannot disappear between validation of a
cursor position and establishment of its claim.

### 15.2 MVCC garbage collection

The database must retain every version and tombstone above the visibility frontier F. These entries
may be needed by internal transactions that have not resolved, even when no public snapshot can yet
observe them. A later installed version is not sufficient reason to reclaim an earlier one.

A conservative retention floor G is the minimum of F and all sequence floors required by unrevoked
snapshots, checked-out cursors, checkpoint or recovery work, replicas, and history policies. Retain
every version above G and the newest version at or below G for each key, including tombstones. This
preserves the state at G and every later retained state. Equivalent physical retention mechanisms
must preserve the same views and feed events.

Retain corresponding outcomes and schema history for protected feed intervals. Snapshot revocation
or cursor advancement releases only that claim; other claims still apply. Reclamation must also
respect in-flight storage readers. Internal transaction reads must never be revoked like external
snapshots or converted to semantic aborts because required history was collected.

## 16. Changefeeds, derived indexes, and replication

The total-order log and visibility frontier provide a natural durable change stream. A consumer
stores one sequence watermark and requests visible records after that sequence. The stream includes
transaction outcomes and administrative events and never exposes a sequence above the frontier.

```text
Database frontier:         83_917
Tantivy index watermark:   83_901

Consumer resumes at:       83_902
Consumer catches up through 83_917
```

Because committed visibility is a contiguous prefix, a consumer watermark identifies one complete
source database state. A consumer's derived state corresponds to that watermark only when its own
data and watermark publication are atomic, as specified in section 24. It does not need a bitmap of
out-of-order source completions.

### 16.1 Changefeed contents

A resolved changefeed uses retained MVCC history and outcomes. An implementation may also expose
logical records for consumers that replay the VM:

- Logical form: transaction bytecode and arguments, or the versioned administrative operation,
  together with sequence and success/abort outcome.
- Resolved form: actual keys and values written or deleted by a successful transaction, or the
  resolved catalogue or policy event, together with sequence and outcome.

Logical replay is compact and preserves intent. Resolved changes are easier for consumers that do
not implement the bytecode VM. Both forms must preserve sequence order and transaction boundaries.
Aborts and successful no-write transactions still provide progress through their positions. A
consumer may ignore irrelevant administrative effects, but must still account for their sequences.

Outcome metadata must identify actual written keys, or an equivalent sequence index must locate
them. Values and deletions come from the exact sequence-tagged MVCC entries, not a lookup of the
current value. A conservative manifest alone does not identify writes made by a computed address or
a particular branch.

### 16.2 Replica model

A read replica may restore a checkpoint and replay later visible log records using the same ISA,
catalogue, and semantic limit history. Replica scheduling and physical resource settings may differ
without changing outcomes. The design assumes one canonical writer history. Choosing a writer
through distributed consensus is outside the database core.

### 16.3 Checked-out retention cursors

A checked-out cursor at C reserves a baseline at C and a feed starting at `C + 1`. An existing
consumer uses its last durably committed watermark as that baseline. A new snapshot rebuild reserves
the baseline before its derived state exists; checkout itself is not an acknowledgement that the
build has completed.

Checkout must atomically verify that the required history is available at a position no greater than
the current visibility frontier and establish a durable retention claim. It fails with an explicit
history-unavailable error if the requested history is already gone. A numeric position without a
checked-out cursor does not protect history.

The cursor protects the state at C and the subsequent MVCC versions, tombstones, outcomes, and
schema history needed by its resolved feed. Logical-feed and log-replica cursors also protect the
required original log records. Reading a batch does not move the retention position. Only an
explicit acknowledgement may advance it, monotonically and no further than the visible source
frontier, after the consumer has durably committed through that position.

Cursor registration, acknowledgement, and release are crash-safe retention metadata. Losing an
in-memory handle or restarting either process must not implicitly release a registration. A cursor
is not revoked by snapshot-retention policy; its claim ends only through advancement or explicit
release. Abandoned cursors can therefore retain unbounded history and must be observable and
administratively releasable. Storage pressure must cause backpressure or an explicit operator
action, not silent loss of protected history.

### 16.4 Snapshot plus tail

Provide an operation that obtains a snapshot at F and checks out its tail cursor at F as one
retention operation. Once it succeeds, the state at F and the following feed history cannot be
collected through that cursor's floor. The snapshot remains revocable, but revoking it does not
release the cursor.

A rebuild must finish successfully from the snapshot before publishing its derived state and
watermark F. It then consumes complete records from F + 1. If the snapshot is revoked, the consumer
must discard or restart the unpublished build and explicitly manage the cursor it no longer needs.

## 17. Physical storage engine

The logical transaction model does not require one physical tree design. The version 1 physical
format selects an uncompressed, copy-on-write B+ tree with 16 KiB pages. Appendix F defines its
pages and system trees; appendix G defines atomic publication and reclamation. This supports ordered
reads, snapshots, and direct key access without LSM compaction semantics.

The logical layout is:

```text
transaction.log
    ordered LogRecord entries: transactions, catalogue operations, limit changes

state tree
    (table_id, key, sequence) -> encoded value or tombstone

metadata
    current log tail
    durable log frontier
    visibility frontier
    checkpoint sequence
    catalogue and stable schema descriptions
    semantic limit policy
    retained history floor
    outcomes and per-sequence actual write references
    durable cursor registrations and acknowledged positions
    checksums / format versions
```

### 17.1 Physical and logical logging

The logical transaction and administrative log defines database history. Version 1 uses immutable
copy-on-write pages and an atomically replaced manifest pointer, not a physical WAL. An alternative
physical format must have its own version and must not replace the stable logical history needed for
replay and changefeeds.

### 17.2 Storage transactions

Installing one successful overlay may touch several keys. The storage layer must make the
installation recoverably atomic. This does not require all pages to be written in one device
operation. It requires a durable publication point that makes recovery choose either the complete
old state or the complete new set of sequence-tagged versions.

A successful transaction need not flush all materialized pages before returning if its durable log
record can reconstruct them. This does not relax complete in-memory installation before resolution,
checkpoint durability, or the durability of pinned history before its reconstruction sources are
removed.

## 18. Error model

| **Error class**        | **Behavior**                                                                                                                                                                      |
| ---------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Submission error       | Invalid bytecode or administrative record, unsupported format, bad schema or types, bad access manifest, or admission limits. Reject before sequencing.                           |
| Semantic abort         | Missing point-LOAD key, failed condition or existence check, checked overflow, or deterministic execution-limit failure. Keep the log entry, discard effects, resolve as aborted. |
| Transient system error | I/O failure, temporary allocation failure, or worker failure. Do not convert to semantic abort. Retry or recover.                                                                 |
| Corruption             | Checksum, format, or invariant failure. Stop unsafe progress and report database corruption.                                                                                      |
| Client cancellation    | Allowed only before sequencing. After sequencing, the transaction remains part of canonical history.                                                                              |
| Uncertain submission   | The caller cannot establish whether sequencing occurred. Do not imply rejection or retry automatically; deduplication belongs to the caller.                                      |
| Snapshot revoked       | Fail subsequent reads explicitly without changing the snapshot sequence; report revocation rather than normal iterator exhaustion.                                                |
| History unavailable    | Reject a cursor checkout whose required history has already been reclaimed. Never silently resume at a newer position.                                                            |

## 19. Static resource bounds

Every accepted transaction must have a finite, enforceable bound on logical VM work. The database
must validate declared bounds before sequencing and enforce runtime bounds on loaded values,
computed keys, range work, intermediate results, and writes. Conflict scope and actual work are
separate: a Table scope can describe one computed point access or a bounded range, but never grants
permission for unbounded execution.

| **Resource**          | **Example bound**                                                       |
| --------------------- | ----------------------------------------------------------------------- |
| Bytecode instructions | Maximum decoded instruction count and maximum control-flow path length. |
| Access manifest       | Maximum declared tables and point scopes.                               |
| Point accesses        | Maximum executed access operations and distinct keys actually touched.  |
| Computed keys         | Maximum encoded key bytes, with the target key type enforced.           |
| Range operations      | Maximum logical rows examined, returned rows, and bytes processed.      |
| Arguments             | Maximum encoded argument bytes and item count.                          |
| Locals/registers      | Fixed maximum count and bounded encoded size.                           |
| Value size            | Maximum stored or loaded value size supported by transaction opcodes.   |
| Return value          | Maximum encoded result size.                                            |
| Structured operations | Explicit maximum growth, element count, or output size.                 |
| Writes                | Maximum actual written keys and total overlay bytes.                    |

Runtime limit failures must have stable ISA-defined outcomes. A range work budget counts logical
rows in the transaction's MVCC-plus-overlay view, including rows examined but rejected by an
opcode's filter. It must not count physical pages, obsolete versions, or later invisible entries.
Otherwise garbage collection, storage layout, or replay could change whether the transaction aborts.

Physical I/O, allocation pressure, cache size, and elapsed time must not cause semantic resource
aborts. They may delay work or require system recovery. The execution window, reservations, and
backpressure in section 9.3 control aggregate resource use without changing record outcomes.

### 19.1 Logged semantic limit policy

Limits governing admission and deterministic execution are database policy stored in the initial
checkpoint and changed only by versioned SetLimits log records. A transaction uses the policy in
effect immediately before its sequence, in live execution, recovery, and replication. Resource
claims must be checked against that historical policy, not current process settings.

SetLimits uses the administrative barrier in section 7.3. It cannot retroactively change validation
or outcomes of earlier records. A successful change becomes effective for subsequent records;
checkpoints preserve the policy at their boundary. Limits on a loaded or produced value are enforced
under the transaction's applicable policy. Lowering a load limit can therefore make a later load of
an older large value abort, but must not reinterpret or invalidate the earlier stored value or its
transaction outcome.

Operational settings such as worker count, cache capacity, and execution-window size may differ
between processes because they affect scheduling rather than semantics. An environment unable to
support required historical semantics must report that system limitation rather than silently
substitute a smaller semantic budget.

## 20. Required correctness invariants

1. Determinism: the same valid checkpoint and retained log, including arguments, catalogue history,
   and semantic limit history, produce the same outcomes and logical state under the stable formats.

1. Total order: every sequenced record has one immutable position in canonical history.

1. Durable dispatch: no record executes before the complete log prefix through its sequence is
   durable; the visibility frontier never exceeds the durable frontier.

1. Scope coverage: every actual access is covered by the verified read or write scopes. Table
   wildcards include absent keys, and target tables are known before sequencing.

1. Sequential equivalence: parallel execution produces the same results as sequential log-order
   interpretation.

1. Atomicity: a successful transaction contributes all of its writes; an aborted transaction
   contributes none.

1. No tentative dependencies: a transaction never consumes unresolved overlay state from another
   transaction.

1. Log-prior reads: transaction N never reads a stored version produced by a sequence greater than
   or equal to N.

1. Prefix visibility: every external snapshot corresponds to a contiguous resolved prefix of the
   durable log, including the catalogue at that position, until explicitly revoked.

1. Durability: once a transaction is reported committed under the selected durability policy, crash
   recovery preserves its canonical outcome.

1. Stable replay: retained bytecode semantics never depend on the application binary, Rust ABI, host
   callbacks, wall clock, random state, or current operational resource settings. Every durable
   post-checkpoint record is replayed regardless of cached outcomes.

1. Historical metadata: schema and semantic policy changes have stable logged descriptions and
   affect only subsequent records. Checkpoints preserve the metadata needed for retained data.

1. Safe reclamation: all above-frontier versions are retained. Versions, tombstones, outcomes,
   schemas, and log segments are reclaimed only when no internal reader or retained feature needs
   them.

1. Cursor retention: a checked-out cursor protects its unacknowledged history across crashes until
   advanced or explicitly released; consuming a batch alone does not acknowledge it.

1. Derived publication: a derived index publishes its data and source watermark in one atomic
   durable commit before acknowledging that watermark to the source cursor.

## 21. Reference execution model

A simple single-threaded interpreter defines the expected result. Production parallel execution is
correct when it matches this reference model.

```text
state = checkpoint_state
catalogue = checkpoint_catalogue
limits = checkpoint_limits
outcomes = checkpoint_retained_outcomes
frontier = checkpoint_sequence

for record in durable_log_after_checkpoint in sequence_order:
    effects = empty

    if record.body is Transaction:
        tx = record.body
        overlay = empty
        result = interpret(
            tx.isa_version, tx.program_bytes, tx.arguments, state, catalogue, limits, overlay
        )

        if result is SUCCESS:
            state, effects = apply_atomically(state, overlay, sequence=record.sequence)
        else if result is ABORT:
            discard(overlay)
    else:
        catalogue, limits, result, effects = apply_administrative_atomically(
            record, catalogue, limits
        )

    outcomes[record.sequence] = (result, effects)
    frontier = record.sequence
```

This algorithm is intentionally simple. It is useful as a specification, test oracle, recovery
fallback, and debugging tool. An administrative abort returns unchanged metadata and no effects.
Effects identify actual written keys and tombstones or resolved administrative changes. System
errors stop or suspend progress; they are not semantic aborts. The checkpoint view excludes all
later materialization, and cached later outcomes never cause an iteration to be skipped.

## 22. Parallel execution model

A production scheduler may follow this procedure for transaction programs. Administrative records
instead follow the barrier protocol in section 7.3 and share the same durability and publication
requirements.

1. Apply admission backpressure, validate against the applicable catalogue and policy, and verify
   conservative point and table scopes with bounded resource claims.

1. Append the transaction to the total-order log and assign sequence N.

1. In log order, identify earlier possible writers whose scopes overlap N's read scopes.

1. Register N's possible write scopes before completing dependency discovery for later records.

1. Wait for the durable log frontier to reach N, all required predecessor writers to resolve, and
   execution-window and resource reservations to permit dispatch.

1. Execute N against sequence-bounded MVCC reads and a private overlay, resolving computed keys and
   bounded ranges within the declared scopes and historical semantic policy.

1. On abort, discard the overlay, record the deterministic outcome, and mark N resolved-abort.

1. On success, install the complete sequence-tagged overlay and outcome with actual write
   references, then mark N resolved-success.

1. Wake dependent transactions whose required predecessors are now resolved.

1. Advance the visibility frontier through the longest contiguous resolved prefix.

1. Publish newly visible records to waiting callers, snapshots, and changefeed consumers. Retain
   their effects and outcomes according to the frontier and other retention claims.

## 23. End-to-end example

Initial visible state at sequence 99:

```text
A = 10
B = 20
C = 30
frontier = 99
```

The log durably records:

```text
100: A += 1
101: B += 1
102: C += 1
103: A *= 2
```

The dependency scheduler derives:

```text
100 -> 103
101 independent
102 independent
```

Transactions 101 and 102 may finish first. The MVCC store can contain B@101 = 21 and C@102 = 31, but
ordinary readers still use frontier 99 and therefore see B = 20 and C = 30. Transaction 103 waits
for transaction 100 because it reads A.

When transaction 100 resolves successfully, it installs A@100 = 11. Transaction 103 can then execute
and install A@103 = 22. If all transactions 100 through 103 are now resolved, the visibility
frontier advances directly from 99 to 103. New snapshots see A = 22, B = 21, and C = 31.

If transaction 100 had aborted instead, it would install no A@100 version. Transaction 103 would
then read the newest earlier successful A version, A@99 = 10, and produce A@103 = 20. No rollback of
transaction 103 is needed because it did not execute until transaction 100 had reached a terminal
state.

## 24. Derived index example

A derived index must commit its data and source watermark atomically in the same durable index
commit. The watermark is not a separately written file or an independently updated database row.
Tantivy supports attaching it to the index commit through `PreparedCommit::set_payload`; the source
database sequence is distinct from Tantivy's own operation stamp.

Suppose the database frontier is 50,000 and the index's committed watermark is 49,920. The consumer
holds a durable cursor protecting that position and processes this pipeline:

```text
Read complete records 49,921 through 50,000 in sequence order
    -> apply resolved data and relevant catalogue effects to the index writer
    -> commit index changes and source watermark 50,000 together
    -> acknowledge 50,000 to the database retention cursor
```

Aborts, no-write transactions, and ignored administrative events still count toward progress. An
index commit must not split a source transaction's effects. Acknowledgement must never precede the
durable index commit.

| **Crash position**                                            | **Recovery action**                                                                               |
| ------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| Before the new index commit is published                      | Recover the old index and watermark; resume from 49,921.                                          |
| During commit, with an uncertain outcome                      | Recover either complete index commit and use its watermark to choose the resume position.         |
| After the durable index commit, before cursor acknowledgement | Recover watermark 50,000 from the index, advance the conservative cursor, and resume from 50,001. |
| After cursor acknowledgement                                  | Recover the index and watermark 50,000 and resume from 50,001.                                    |

If commit completion is uncertain, inspect the recovered index commit and its payload rather than
guessing whether to repeat staged operations. The index's committed watermark is the recovery
authority. An older source cursor only retains excess history. This protocol does not require a
transaction spanning both databases.

A full rebuild obtains a snapshot and durable tail cursor together at F, builds an unpublished index
from that snapshot, and atomically publishes the completed index generation with watermark F. It
then consumes from F + 1 through the same commit-and-acknowledge protocol. Snapshot revocation
invalidates an incomplete build; it must not be published or mistaken for successful iterator
exhaustion.

## 25. Observability and diagnostics

The database should expose enough internal state to explain latency and stalled visibility without
exposing unsafe internals.

| **Metric or diagnostic**       | **Purpose**                                                                     |
| ------------------------------ | ------------------------------------------------------------------------------- |
| Log tail                       | Highest assigned sequence number.                                               |
| Durable log frontier           | Highest contiguous log position eligible for execution.                         |
| Visibility frontier            | Highest externally visible sequence number.                                     |
| Execution window and budgets   | Configured lookahead and count/byte usage above the frontier.                   |
| Submission backlog             | Queued submissions, logged backlog bytes, and admission waits.                  |
| Resolved-above-frontier count  | Shows work completed but waiting behind a hole.                                 |
| Oldest unresolved sequence     | Identifies the transaction holding the visibility frontier.                     |
| Dependency wait time           | Measures time spent waiting for log-prior writers.                              |
| Table-wide scope frequency     | Explains conservative dependencies and table-level serialization.               |
| VM execution time              | Measures actual bytecode work.                                                  |
| Overlay bytes and key count    | Shows transaction materialization pressure.                                     |
| Checkpoint and retention floor | Shows how much history can be reclaimed.                                        |
| Changefeed consumer watermarks | Shows acknowledged positions, lag, and history retained by each durable cursor. |
| Snapshot revocations           | Explains failed readers and reclaimed snapshot claims.                          |
| Administrative barrier         | Shows pending schema or limit changes and their wait time.                      |
| Catalogue and policy position  | Identifies the active historical metadata for validation and replay.            |

## 26. Verification and testing strategy

The design lends itself to unusually strong differential testing because the sequential interpreter
is a compact reference model.

- Generate random valid logs containing bytecode, catalogue operations, and limit changes. Compare
  data, metadata, returned values, and abort outcomes with the sequential reference interpreter
  after every visible frontier.
- Randomize worker counts, execution delays, completion order, and dependency scheduling to search
  for hidden ordering assumptions.
- Inject semantic aborts at every valid instruction boundary and verify that overlays leave no
  partial state.
- Inject process crashes and torn writes around log append, materialization, resolution metadata,
  checkpoint publication, cursor metadata, and frontier persistence. Verify that unflushed records
  never execute or become visible.
- Replay the same retained log repeatedly and verify byte-for-byte stable logical state and
  transaction results.
- Recover from checkpoints with cached post-checkpoint success or abort outcomes and partial later
  materialization. Verify that every later record is replayed without duplicate application.
- Verify that unrevoked snapshots remain stable while newer transactions execute and become visible,
  and that revoked point reads and iterators fail explicitly without unsafe reclamation.
- Verify collection with a blocked internal reader needing an intermediate above-frontier version
  after a newer blind write has installed another version of the same key.
- Test predecessor aborts and successful branches that omit writes, including independent blind
  writers and table-wide write declarations.
- Test computed-key aliases, address-producing reads, read-your-own-writes, delete-then-load aborts,
  and existence-checked insertion for both point and table-wide scopes.
- Test empty and bounded ranges with earlier unresolved inserts or deletes, later invisible writes,
  and overlay changes affecting ordering and limits.
- Verify identical logical resource-limit outcomes with different physical version histories,
  storage layouts, worker counts, and cache settings.
- Test limit and catalogue barriers with submissions validated before a change but sequenced after
  it. Replay under different startup settings and verify the historical policy and schema are used.
- Verify execution-window and byte-budget backpressure while the oldest record is delayed, ensuring
  that later work cannot consume its progress resources.
- Verify durable cursor checkout racing reclamation, restart with an outstanding cursor, and
  checkpoint/log truncation while older MVCC feed history and schema descriptions remain pinned.
- Crash a derived consumer before and after its atomic data/watermark commit and cursor
  acknowledgement. Verify restart from the index watermark without lost or duplicate effects.
- Test uncertain submissions and caller-managed deduplication, including different payloads using
  the same request ID and retries after semantic aborts.
- Fuzz bytecode validation so malformed programs can never reach the scheduler or interpreter.
- Run the binary conformance cases in appendix H, including cross-endian decoding, page invariants,
  unsupported versions, resource accounting, and crash points in the publication protocol.
- Use model checking for small key sets to compare all legal parallel schedules with the canonical
  serial order.

## 27. Integrity and trust boundaries

Even in an embedded database, persisted bytes and transaction input must be treated as untrusted at
format boundaries. Corruption or malformed data must fail safely rather than change transaction
semantics.

- Validate bytecode and administrative records before sequencing and verify retained records when
  loading them. Replay validation must use the historical catalogue and semantic limit policy.
- Checksum or authenticate log records, checkpoints, and critical metadata.
- Use explicit lengths and maximum sizes before allocation.
- Treat the compiler-generated access manifest as an optimization hint until the database
  independently verifies coverage of every possible access, including table wildcards and existence
  checks. Malformed bytecode must not bypass scope, type, or runtime size enforcement.
- Keep stable encodings independent of Rust enum layout, pointer width, endianness, and compiler
  version.

## 28. Deliberately unspecified implementation choices

The following choices do not need to be fixed to define the database semantics or version 1 bytes.
An implementation may choose them independently while preserving this document's invariants.

- B+ tree split and merge heuristics, cache policy, and in-memory allocation.
- Timing of checkpoints, whole-file compaction, log rotation, and group commit.
- In-memory program deduplication. Version 1 log records always contain the complete program.
- Exact worker-pool and dependency-queue implementation.
- Whether same-key semantic operations are coalesced as a storage optimization.
- More precise internal dependency analysis, provided it proves the same ordering guarantees as the
  persisted point and table scopes. A new persisted scope kind requires a new format version.
- Default execution-window, byte-budget, backlog, snapshot-revocation, and history-retention
  settings, subject to the required progress and checked-out cursor guarantees.
- The process-locking primitive. Version 1 requires exclusive ownership of an open database
  directory; shared access across processes requires a separately specified coordination protocol.
- High-level document, migration, secondary-index, or query APIs above the required stable catalogue
  and schema-enforcement layer.

## 29. Design summary

The database replaces client-side optimistic read/modify/write loops with deterministic transaction
programs. The DSL is restricted so every program is bounded and its target tables and conservative
access scopes are known before sequencing. Individual keys may be computed from database state;
computed accesses and bounded ranges use whole-table scopes. The DSL lowers to stable bytecode that
is safe to persist and replay without application callbacks.

A durable log assigns one total order to programs, schema operations, and semantic limit changes.
Verified scope overlap lets the scheduler execute durably logged transactions concurrently within a
bounded window when their predecessors have resolved. Administrative barriers preserve historical
schema and policy interpretation. Each transaction uses a private overlay, so semantic abort is
implemented by discarding tentative writes rather than undoing shared state. Dependent transactions
wait for predecessor resolution, which prevents cascading rollback.

Successful writes become MVCC versions tagged with their transaction sequence. Internal execution
may use successful log-prior versions above the public frontier. External readers see only the
contiguous resolved prefix selected by the visibility frontier. This gives every public snapshot,
checkpoint, replica, changefeed consumer, and derived index one simple sequence-number boundary.

Recovery restores checkpoint data and metadata, then replays every subsequent durable record without
skipping cached outcomes. Retention preserves all above-frontier versions and the historical
versions, outcomes, and schemas needed by other claims. Snapshots are revocable, while durable
checked-out cursors protect unacknowledged history until advanced or explicitly released. A derived
index commits its data and source watermark atomically before acknowledging the cursor. Callers
manage request deduplication rather than relying on submission retries being automatically
idempotent.

The core implementation can therefore optimize aggressively while retaining a simple specification:
it must behave exactly like a single-threaded interpreter that runs the durable transaction log in
sequence order.

## Appendix A. Binary conventions and version registry

### A.1 Encoding rules

All offsets and lengths are in bytes. Layouts are packed in the listed order, with no implicit
alignment or padding. `u8`, `u16`, `u32`, and `u64` are unsigned integers of exactly that many bits.
They are little-endian unless explicitly marked `be16`, `be32`, or `be64`. I64 values use 64-bit
two's-complement representation. `bytes[n]` means exactly n bytes, not a pointer. ASCII magic
strings contain exactly the displayed characters, with no NUL terminator. Hexadecimal examples are
in wire order. `||` means byte concatenation.

The following productions are used throughout the appendices:

```text
Blob       = length: u32 || data: bytes[length]
Text       = Blob containing valid UTF-8
Vector<T>  = count: u32 || items: T[count]
TypedValue = type: Blob(TypeDesc) || value: Blob(ValueEncoding(type))
```

`Blob(T)` means a Blob whose entire data decodes as T. Every decoder must consume exactly its
enclosing length. Extra bytes, duplicate fields where uniqueness is required, nonzero reserved
bytes, unknown flags, unknown tags, and unsupported versions are errors. No variable-length
integers, native Rust enum encodings, implicit Unicode normalization, optional padding, or implicit
compression are used. A missing optional reference is encoded only by the sentinel specified for
that field, never by omitting bytes.

Before allocation or pointer arithmetic, validate lengths, counts, multiplication, addition,
nesting, and containing-file bounds using checked arithmetic. A decoder may stream large objects; it
must not turn physical memory pressure into a different semantic result. Invalid submitted bytes are
submission errors. Invalid authoritative persisted bytes are corruption. A supported outer format
containing an unsupported required inner version must report unsupported-format and stop; it must
not skip the object or interpret it as a semantic abort.

### A.2 Integrity and identities

`CRC-32C` is the Castagnoli CRC: polynomial `0x1edc6f41`, reflected polynomial `0x82f63b78`,
reflected input and output, initial register `0xffffffff`, and final XOR `0xffffffff`. The resulting
u32 is stored little-endian. A checksum exists only where a layout explicitly declares a checksum
field; nested values and programs have no implicit checksum trailer. For a declared final CRC field,
the default coverage is all preceding bytes. An explicitly stated coverage span overrides that
default, including the log record's header-and-body-only CRC. Where a layout specifies a
whole-object embedded checksum, replace its field with four zero bytes while computing the CRC. CRCs
detect accidental damage; they do not authenticate an untrusted writer.

`SHA256(x)` is SHA-256 as defined by FIPS 180-4, stored as the 32 digest bytes in their standard
order, without integer byte reversal. Hash chains and manifest digests use this function. A backup
or network transport needing authenticity must provide authentication outside version 1.

Every database has a nonzero 16-byte `database_id`, selected at creation outside the VM and
persisted in its genesis file. A sequence or cursor identifier is meaningful only with this
identity. Sequence 0 is the genesis state. Canonical records are numbered consecutively from 1
through `2^64 - 2`; `2^64 - 1` is reserved. Table IDs are the sequence numbers of their successful
creation records. File, segment, manifest, and cursor IDs are separate nonzero u64 namespaces local
to a physical database directory, also excluding `2^64 - 1`. Published IDs must not wrap or be
reused in that directory. Cursor tokens additionally carry a persistent local cursor namespace so
restored copies and replicas cannot alias each other's registrations; G.4 defines its lifecycle.
Exhaustion is a system limitation, not a logged transaction abort.

### A.3 Version 1 profile

| Object                                                   | Version and definition                     |
| -------------------------------------------------------- | ------------------------------------------ |
| Schema and value encoding                                | Schema 1, appendix B.                      |
| Transaction program                                      | ISA 1, appendix C.                         |
| Transaction body and manifest                            | Transaction 1, appendix C.                 |
| Administrative operations and limits                     | Administrative 1 and policy 1, appendix D. |
| Outcome                                                  | Outcome 1, appendix D.                     |
| Log segment and record                                   | Log 1, appendix E.                         |
| Page and physical system trees                           | Page 1 and storage 1, appendix F.          |
| Genesis, publication pointer, manifest, and cursor value | Metadata 1, appendix G.                    |
| Feed batch, cursor token, and consumer watermark         | Exchange 1, appendix H.                    |

These are independent version namespaces. Changing a physical format does not change ISA semantics.
Version 1 has no optional feature bits. A change to an existing layout, opcode, type meaning,
comparator, checksum, or required feature needs a new corresponding version. Readers must not guess
compatibility from an otherwise familiar magic string.

Hard format ceilings are independent of the logged policy. A program is at most 16 MiB, a log record
at most 64 MiB, a schema-encoded value at most 16 MiB, a physical leaf value or outcome at most 128
MiB, and an exchange batch at most 256 MiB. Here `1 MiB = 1,048,576 bytes`. Type descriptions are at
most 65,536 bytes, have nesting depth at most 16, and have at most 256 fields per tuple. The
outermost type has depth 1. All nested types count toward the depth limit. A type's declared maximum
encoding length, computed with checked arithmetic, must fit the relevant value ceiling.

## Appendix B. Types, schemas, and canonical keys

### B.1 Type descriptions

A `TypeDesc` starts with `schema_version: u16 = 1`, followed by one recursively encoded `TypeNode`.
Nested nodes do not repeat the version. The tag is a u8. A type's exact identity is its complete
canonical descriptor bytes. Its *shape* is the same descriptor with byte-length and row-count bounds
ignored. There are no names, field offsets, native alignment rules, or optional fields in a schema.

| Tag    | TypeNode operands after the tag                            | Value encoding                                             |
| ------ | ---------------------------------------------------------- | ---------------------------------------------------------- |
| `0x00` | Unit, no operands.                                         | Zero bytes.                                                |
| `0x01` | Boolean, no operands.                                      | One byte: false `00`, true `01`.                           |
| `0x02` | I64, no operands.                                          | Eight little-endian two's-complement bytes.                |
| `0x03` | U64, no operands.                                          | Eight little-endian bytes.                                 |
| `0x04` | Bytes: `max_bytes: u32`.                                   | Blob of at most max_bytes bytes.                           |
| `0x05` | String: `max_bytes: u32`.                                  | Text of at most max_bytes UTF-8 bytes.                     |
| `0x06` | Tuple: `field_count: u16`, then that many TypeNodes.       | Field value encodings concatenated in field order.         |
| `0x20` | Rows: `max_rows: u32`, `key: TypeNode`, `value: TypeNode`. | `count: u32`, then count pairs of key and value encodings. |

Tuple fields are positional, numbered from zero. Empty tuples and zero byte bounds are valid.
Strings must be well-formed UTF-8, excluding overlong encodings, surrogate code points, and code
points above U+10FFFF. There is no normalization; byte-distinct Unicode spellings remain distinct.
NUL is allowed in user strings. Unit and an empty tuple have different types despite both having
empty value encodings.

Rows is allowed only as a top-level register or result type. Its key and value nodes must be
ordinary non-Rows schema types, its maximum count is 65,535, and its maximum encoded size must fit
16 MiB. Rows cannot be nested in another type, used in table schemas, supplied as an argument, or
used as a constant. A row set contains strictly increasing, nonduplicate canonical keys. Its key and
value shapes identify how each row is decoded; it does not encode a table ID.

A table schema is `key: Blob(TypeDesc) || value: Blob(TypeDesc)`. Both descriptions exclude Rows.
The maximum key encoding in B.2 must be at most 1,024 bytes; the maximum value encoding must be at
most 16 MiB. These are schema ceilings, not defaults inferred from the application. The persisted
schema remains valid when a later policy lowers runtime limits.

### B.2 Order-preserving key encoding

User keys are compared by unsigned lexicographic comparison of their complete canonical key bytes; a
shorter proper prefix sorts first. Type tags and value-encoding length prefixes are not included in
a key. The table schema supplies the types.

| Type            | Key encoding                                                                        |
| --------------- | ----------------------------------------------------------------------------------- |
| Unit            | Empty bytes.                                                                        |
| Boolean         | The same single byte as its value encoding.                                         |
| U64             | Eight big-endian bytes.                                                             |
| I64             | Reinterpret as u64, XOR with `0x8000000000000000`, then encode big-endian.          |
| Bytes or String | Escape every `00` byte as `00 ff`, leave other bytes unchanged, and append `00 00`. |
| Tuple           | Concatenate the canonical key encodings of its fields, without a tuple header.      |

The byte-string encoding is called `Escape(x)` below. Its terminator makes variable-width fields
self-delimiting and preserves prefix ordering. A schema fixes tuple arity and all zero-width fields,
so key decoding is unambiguous. Maximum encoded sizes are 0, 1, or 8 for fixed-width types,
`2 * max_bytes + 2` for Bytes and String, and the sum of field maxima for Tuple. Decoding must
reject an invalid escape, missing terminator, invalid UTF-8, schema-bound violation, or trailing
bytes.

Logical comparison in the VM uses this same type order: ordinary signed or unsigned numeric order,
false before true, byte order for Bytes and String, and lexicographic field order for Tuple. Unit
values compare equal. Comparisons require equal shapes; no implicit numeric conversion or collation
is allowed. Rows supports neither equality nor ordering opcodes in ISA 1.

### B.3 Physical MVCC keys

The state tree uses this physical key, distinct from a user key:

```text
StateKey = table_id: be64 || Escape(canonical_user_key) || be64(~sequence)
```

`~sequence` is the bitwise complement in exactly 64 bits. Thus tables and user keys sort ascending,
while versions of one key sort newest first. The extra escaping frames the complete user key,
including fixed-width integer and tuple encodings. A state key is at most 2,066 bytes. Table IDs and
version sequences must be nonzero. A row version's sequence must be greater than its table ID,
because table creation itself installs no rows and only later transactions can address that table.

For snapshot S, seek to `table_id || Escape(key) || be64(~S)` and use the first entry with that
exact table/key prefix. For transaction N, use S = N - 1. An entry with another prefix is absence. A
tombstone is terminal absence, not permission to continue to an older version. Range iteration
groups physical entries by table/key before applying the same sequence bound and merging the
overlay. Physical version counts never consume logical range budgets.

## Appendix C. Transaction format and ISA 1

### C.1 Program container

The program has this 32-byte header, followed by the listed sections without gaps:

| Offset | Bytes | Field                                        |
| ------ | ----- | -------------------------------------------- |
| 0      | 8     | Magic `BLOPVM01`.                            |
| 8      | 2     | ISA version = 1.                             |
| 10     | 2     | Flags = 0.                                   |
| 12     | 4     | Total program length, including this header. |
| 16     | 2     | Register count.                              |
| 18     | 2     | Argument count.                              |
| 20     | 2     | Table count.                                 |
| 22     | 2     | Constant count.                              |
| 24     | 4     | Instruction count, from 1 through 65,535.    |
| 28     | 4     | Reserved = 0.                                |

```text
result_type:    Blob(TypeDesc)
tables:         u64[table_count]
argument_types: Blob(TypeDesc)[argument_count]
register_types: Blob(TypeDesc)[register_count]
constants:      TypedValue[constant_count]
instructions:   Instruction[instruction_count]
```

Table IDs are strictly increasing and must name live tables in the pre-sequence catalogue.
Registers, arguments, tables, and constants are zero-indexed. A register operand is a u16 from 0
through `register_count - 1`; `0xffff` is never a register and is reserved for absent range
endpoints. There are no implicit registers, stack, flags register, or host pointers. Each register
has one declared type for the entire program. All registers initially are uninitialized; ARG and
CONST explicitly initialize them. Sources are read before writing a destination, so a destination
may alias a source of compatible shape.

### C.2 Transaction body and arguments

Log kind 1 has this body:

```text
transaction_version: u16 = 1
reserved:            u16 = 0
program:             Blob(Program)
arguments:           Blob(Arguments)
access_manifest:     Blob(AccessManifest)
claims:              Budget

Arguments = count: u32 || values: Blob(ValueEncoding(argument_type))[count]
```

The argument count must equal the program's count. Each argument and constant must satisfy its
declared type and the applicable admission policy. `Budget` is the fixed vector in D.2. There are no
external program references: every record is independently decodable with its historical catalogue
and policy. The enclosing log record supplies identity and integrity.

### C.3 Instruction encoding and control flow

Every instruction is `opcode: u8 || flags: u8 = 0 || operand_bytes: u16 || operands`. The operand
length must exactly match the opcode's layout. Instructions are packed with no alignment. Branch
targets are zero-based instruction indices, not byte offsets, and must be strictly greater than the
branch's index and less than the instruction count. An implementation must decode and validate the
entire instruction array before interpreting any of it.

In the tables, `d`, `a`, `b`, `s`, `key`, `value`, `index`, `lo`, and `hi` are u16 register
operands; `t` is a u16 table index; `arg` and `constant` are u16 pool indices. Other widths are
explicit. All operands appear in the displayed order. Every unlisted opcode byte is invalid in ISA
1, including `0x00` and `0xff`. There are no extension instructions to skip.

| Opcode | Name                  | Operands                            | Semantics                                                               |
| ------ | --------------------- | ----------------------------------- | ----------------------------------------------------------------------- |
| `0x01` | CONST                 | `d, constant`                       | Copy the typed constant to d.                                           |
| `0x02` | ARG                   | `d, arg`                            | Copy the supplied argument to d.                                        |
| `0x03` | MOVE                  | `d, s`                              | Copy s to d.                                                            |
| `0x10` | ADD_CHECKED           | `d, a, b`                           | I64 or U64 addition; abort on overflow.                                 |
| `0x11` | SUB_CHECKED           | `d, a, b`                           | I64 or U64 subtraction; abort on overflow or unsigned underflow.        |
| `0x12` | MUL_CHECKED           | `d, a, b`                           | I64 or U64 multiplication; abort on overflow.                           |
| `0x13` | DIV_CHECKED           | `d, a, b`                           | Integer division; rules below.                                          |
| `0x14` | REM_CHECKED           | `d, a, b`                           | Integer remainder; rules below.                                         |
| `0x15` | NEG_CHECKED           | `d, a`                              | I64 negation; abort for the minimum I64.                                |
| `0x16` | TO_I64_CHECKED        | `d, a`                              | U64 to I64; abort above the maximum I64.                                |
| `0x17` | TO_U64_CHECKED        | `d, a`                              | I64 to U64; abort for a negative input.                                 |
| `0x20` | EQ                    | `d, a, b`                           | Boolean equality using B.2.                                             |
| `0x21` | LT                    | `d, a, b`                           | Boolean less-than using B.2.                                            |
| `0x22` | LE                    | `d, a, b`                           | Boolean less-than-or-equal.                                             |
| `0x23` | GT                    | `d, a, b`                           | Boolean greater-than.                                                   |
| `0x24` | GE                    | `d, a, b`                           | Boolean greater-than-or-equal.                                          |
| `0x28` | BOOL_AND              | `d, a, b`                           | Boolean conjunction; both sources are already evaluated.                |
| `0x29` | BOOL_OR               | `d, a, b`                           | Boolean disjunction.                                                    |
| `0x2a` | BOOL_XOR              | `d, a, b`                           | Boolean exclusive OR.                                                   |
| `0x2b` | BOOL_NOT              | `d, a`                              | Boolean negation.                                                       |
| `0x30` | BIT_AND               | `d, a, b`                           | Bitwise AND of equal integer types.                                     |
| `0x31` | BIT_OR                | `d, a, b`                           | Bitwise OR of equal integer types.                                      |
| `0x32` | BIT_XOR               | `d, a, b`                           | Bitwise XOR of equal integer types.                                     |
| `0x33` | BIT_NOT               | `d, a`                              | Complement all 64 bits of an integer.                                   |
| `0x34` | SHL_WRAP              | `d, a, b`                           | Shift integer a left by U64 b, discarding high bits.                    |
| `0x35` | SHR                   | `d, a, b`                           | Shift integer a right by U64 b; sign-extend I64, zero-fill U64.         |
| `0x40` | LOAD                  | `d, t, key`                         | Read the overlay/log-prior value; abort if missing.                     |
| `0x41` | EXISTS                | `d, t, key`                         | Boolean presence in the same view; tombstones are absent.               |
| `0x42` | INSERT                | `t, key, value`                     | Abort if present; otherwise write to the overlay.                       |
| `0x43` | STORE                 | `t, key, value`                     | Unconditionally create or replace the overlay value.                    |
| `0x44` | DELETE                | `t, key`                            | Unconditionally place a tombstone in the overlay.                       |
| `0x48` | SCAN_BOUNDED          | See C.5.                            | Produce a bounded ordered Rows value.                                   |
| `0x49` | ROWS_LEN              | `d, s`                              | Return the Rows count as U64.                                           |
| `0x4a` | ROWS_KEY              | `d, s, index`                       | Return the key at U64 index; abort if out of bounds.                    |
| `0x4b` | ROWS_VALUE            | `d, s, index`                       | Return the value at U64 index; abort if out of bounds.                  |
| `0x50` | BYTE_LEN              | `d, s`                              | Return Bytes or String byte length as U64, excluding its length prefix. |
| `0x51` | CONCAT                | `d, a, b`                           | Concatenate two Bytes or two String values of the same kind.            |
| `0x52` | SLICE_BYTES           | `d, s, index, length: u16`          | Slice Bytes using U64 start and length registers.                       |
| `0x53` | UTF8_BYTES            | `d, s`                              | Convert String to its unchanged UTF-8 Bytes.                            |
| `0x54` | PARSE_UTF8            | `d, s`                              | Convert Bytes to String; abort for invalid UTF-8.                       |
| `0x55` | SHA256                | `d, s`                              | Hash Bytes contents, excluding the length prefix; produce 32 Bytes.     |
| `0x58` | TUPLE                 | `d, count: u16, fields: u16[count]` | Construct a tuple from the listed registers in order.                   |
| `0x59` | FIELD                 | `d, s, field: u16`                  | Read the statically indexed field of a Tuple.                           |
| `0x60` | JUMP_FORWARD          | `target: u32`                       | Continue at target.                                                     |
| `0x61` | JUMP_IF_FALSE_FORWARD | `a, target: u32`                    | Branch if Boolean a is false; otherwise fall through.                   |
| `0x62` | REQUIRE               | `a, user_code: u32`                 | Abort with user_code if Boolean a is false.                             |
| `0x63` | ABORT                 | `user_code: u32`                    | Unconditionally abort.                                                  |
| `0x64` | RETURN                | `s`                                 | Successfully return s and install the final overlay.                    |

ADD, SUB, MUL, DIV, REM, BIT_AND, BIT_OR, and BIT_XOR require the same integer type in both sources
and destination. BIT_NOT preserves its source integer type. SHL_WRAP and SHR require the destination
to have a's integer type and b to be U64. DIV truncates toward zero for I64; REM satisfies
`a = quotient * b + remainder` with the remainder having the dividend's sign or being zero. Both
abort on division by zero, and both abort for `I64_MIN / -1` or `I64_MIN % -1`. Shifts abort for a
count of 64 or more, including counts that a host instruction would mask. Shift count zero is valid.
No other arithmetic wraps.

SLICE_BYTES requires `start <= byte_length` and `length <= byte_length - start`; it never clamps or
wraps indices. Empty slices at the end are valid. Strings have no direct slicing opcode; callers can
convert to bytes, slice, and explicitly validate UTF-8. TUPLE arity and FIELD indices are checked
statically. All value movement and construction requires equal source/destination shapes, or the
specific conversion described by the opcode. Destination bounds are enforced on the result. Table
keys and values must statically match the table's shapes. Key bounds are checked before target
access; new value bounds are checked before overlay mutation, after any INSERT presence check, as
specified in C.6.

### C.4 Verification and access derivation

Build the forward control-flow graph, including both edges of every conditional branch. Every
instruction must be reachable from instruction zero, and every path must terminate with RETURN or
ABORT, never fall off the array. Verify all instructions even when an argument makes a branch
predictable. At a join, a register is definitely initialized only if it is initialized on every
incoming path. Every source must be definitely initialized. RETURN must have the declared result
shape; ABORT may terminate a path for any result type. Program-local shape mismatches are submission
errors, not runtime type errors.

The verifier also propagates abstract register values in instruction order. ARG and CONST are known
values; a pure instruction with all known inputs produces a known result if it completes within its
declared bounds without a semantic failure. Otherwise its result is Unknown. LOAD and range
operations produce Unknown. EXISTS also produces Unknown, even when a preceding overlay write
appears to make its result obvious. At a join, retain a known value only if every predecessor gives
the same type and value; otherwise use Unknown. A predictable runtime failure is not executed during
validation and does not remove control-flow edges.

A key known by this analysis gives a point scope after canonical encoding. An Unknown key gives a
table scope. LOAD and EXISTS require read access, STORE and DELETE require write access, and INSERT
requires both. SCAN_BOUNDED always requires a table-wide read scope. Reads used to compute keys are
included separately. The database may accept a broader supplied scope but must independently prove
that every derived read and write scope is covered. This analysis is deliberately conservative;
compilers can keep independent known addresses in separate registers to retain point scopes.

```text
AccessManifest = Vector<Scope>
Scope = table_id: u64 || kind: u8 || mode: u8 || reserved: u16 = 0 || key: Blob
```

Kind 0 is Table and requires an empty key Blob. Kind 1 is Key and requires canonical user-key bytes
under the named schema; an empty key is valid for a zero-width key schema. Mode 1 is read, 2 is
write, and 3 is read/write. Scope entries sort by numeric table ID, then kind, then key byte order.
Duplicate `(table, kind, key)` entries must be combined by OR-ing modes. For each access mode, a
Table entry suppresses that mode on point entries in the same table; remove entries with no
remaining modes. These normalization rules are mandatory in persisted manifests. All scope tables
must occur in the program table array. A broader manifest may reduce parallelism but cannot affect
semantics.

### C.5 Bounded range semantics

SCAN_BOUNDED has exactly 22 operand bytes:

```text
d: u16 || t: u16 || lo: u16 || hi: u16
endpoint_flags: u8 || reserved: u8 = 0
row_limit: u32 || byte_limit: u64
```

`lo` or `hi` equal to `0xffff` means unbounded on that side, not an infinite scan budget. Flag bit 0
makes a present lower endpoint inclusive; bit 1 makes a present upper endpoint inclusive. A clear
bit makes that endpoint exclusive. An absent endpoint requires its inclusion bit to be zero. All
other bits are zero. Present endpoints have the table key shape and obey its bounds. The result
register is Rows with the table's key and value shapes.

The immediate row and byte limits must not exceed the transaction's corresponding claims. Select the
first at most row_limit present logical rows in ascending key order from the sequence-bounded view
merged with the overlay. Version filtering and overlay tombstones happen before row selection. There
is no predicate filter, reverse scan, implicit continuation, or extra lookahead row in ISA 1. An
inverted interval, an equal-endpoint interval with either side exclusive, or row_limit zero returns
zero rows. Equal inclusive endpoints can return one row.

For each selected row, charge one logical examined/returned row and the sum of canonical user-key
bytes and schema-encoded value bytes. Reaching row_limit stops successfully without probing whether
another logical row exists. Exceeding byte_limit aborts the whole transaction rather than returning
a shorter successful batch. Per-transaction aggregate range budgets, table and register bounds, and
load limits also apply. Even an overlay row consumes the same logical charges. Deletions, invisible
versions, skipped old versions, and physical pages consume none. An empty range returns an encoded
u32 zero count, not a missing-key abort.

### C.6 Runtime check order

Each instruction first charges its instruction visit. A point operation then canonicalizes and
checks the key, and charges its point access and distinct address before inspecting presence. Range
endpoints are checked before scanning; each selected row is checked and charged in key order. These
checks also apply to overlay hits. EXISTS and INSERT inspect only presence and do not load or charge
the old value. DELETE does not inspect the old value at all.

For each key, first enforce its table descriptor, then canonicalize it and enforce key_bytes. For a
scan, complete these checks on the lower endpoint before checking the upper endpoint, even when
row_limit is zero or the interval will be empty. Absent endpoints require no checks. Endpoints do
not count as distinct accessed addresses or point operations. Point access and distinct-address
charges follow the key check. For a selected row, check the key first, then its loaded value, then
the prospective distinct-address and range charges in resource-ID order.

Next perform the opcode's intrinsic computation or presence check, validate the produced value
against its destination or table descriptor, and check prospective resource usage before changing
the register or overlay. RETURN also checks the declared result descriptor and return budget. A
descriptor-bound failure is BOUND_EXCEEDED; policy/claim failures are RESOURCE_LIMIT. At a check
stage with multiple exceeded resource fields, report the lowest resource ID. SCAN_BOUNDED's local
byte limit is checked with aggregate range bytes and reports resource 14. A selected row is first
checked as a loaded key/value, then charged to the range budgets; the completed Rows value is then
checked against its destination bounds and register budgets.

The first failure in this execution order is the outcome. No later instruction runs. Arithmetic,
UTF-8, index, missing-key, and explicit failure codes are in D.3. Physical errors suspend execution
or require recovery; they never compete with semantic errors to choose an abort code.

## Appendix D. Administrative records, limits, and outcomes

### D.1 Catalogue operations

Log kind 2 has `administrative_version: u16 = 1 || operation: u8 || reserved: u8 = 0`, followed by
the operation operands:

| Operation | Name         | Operands                                                 | Successful effect                                                              |
| --------- | ------------ | -------------------------------------------------------- | ------------------------------------------------------------------------------ |
| `0x01`    | CREATE_TABLE | `name: Text, key: Blob(TypeDesc), value: Blob(TypeDesc)` | Create a live table whose ID is this record's sequence; return that ID as U64. |
| `0x02`    | RENAME_TABLE | `table_id: u64, name: Text`                              | Change the live table's name; return Unit.                                     |
| `0x03`    | DROP_TABLE   | `table_id: u64`                                          | Mark the live table dropped; return Unit.                                      |

Names contain 1 through 255 UTF-8 bytes, excluding NUL. They use exact byte equality without
normalization or case folding. No two live tables may share a name. CREATE with an occupied name
aborts NAME_IN_USE. RENAME first checks that the table is live, then checks name availability;
renaming a table to its current name succeeds. RENAME or DROP of an unknown or dropped identity
aborts TABLE_NOT_LIVE. DROP releases the name immediately at its visible sequence but does not reuse
the identity or discard retained versions. Malformed names or schemas are submission errors.
Well-formed operations with state-dependent failures are sequenced and abort deterministically.

The full catalogue version value stored after a successful operation is:

```text
catalogue_version: u16 = 1
state:             u8 = 1 (live) or 2 (dropped)
reserved:          u8 = 0
name:              Text
key_type:          Blob(TypeDesc)
value_type:        Blob(TypeDesc)
```

Rename and drop preserve the original descriptors; drop preserves the last name. The tree key
supplies the table ID and effective sequence. Version 1 retains all catalogue and policy versions,
even for dropped tables. This avoids losing schema interpretation when older outcomes are pinned; a
future metadata-GC policy may be added without changing these encodings.

### D.2 Semantic limits and claims

`Budget` is exactly 17 u64 fields, in ascending resource-ID order below. A `LimitPolicy` is
`policy_version: u16 = 1 || reserved: u16 = 0 || limits: Budget`. Log kind 3 contains exactly one
LimitPolicy and replaces the entire policy, not a partial patch. The initial policy is stored in the
genesis file and policy tree. Creation must receive an explicit valid policy; no process default is
consulted during replay. A well-formed SetLimits succeeds and returns Unit. It uses the barrier in
section 7.3 and does not validate old rows against the new limits.

Budget fields govern transaction programs, not administrative records. Catalogue operations and
SetLimits use their fixed format and schema ceilings instead of transaction claims. In particular, a
policy that prevents all transaction submissions does not prevent a later SetLimits from raising
those limits.

Each field is a nonnegative finite bound no greater than its hard ceiling. Zero is valid and can
disable a category of work. Every transaction claim must be no greater than its pre-sequence policy.
Statically known usage must fit its claims before sequencing. Runtime checks use the claim as the
effective limit; a looser current process setting or historical policy must not enlarge it.

| ID  | Field           | Hard ceiling | Exact quantity                                                                                             |
| --- | --------------- | ------------ | ---------------------------------------------------------------------------------------------------------- |
| 1   | program_bytes   | 16 MiB       | Entire Program encoding, including its header and constants.                                               |
| 2   | instructions    | 65,535       | Entire decoded instruction count at admission; visits during execution.                                    |
| 3   | registers       | 65,535       | Declared register count.                                                                                   |
| 4   | arguments       | 65,535       | Declared and supplied argument count.                                                                      |
| 5   | argument_bytes  | 16 MiB       | Entire Arguments encoding, including count and Blob prefixes.                                              |
| 6   | tables          | 65,535       | Program table count.                                                                                       |
| 7   | manifest_scopes | 16,384       | Normalized manifest entry count.                                                                           |
| 8   | point_accesses  | 65,535       | Executed LOAD, EXISTS, INSERT, STORE, and DELETE operations, including repeated accesses.                  |
| 9   | distinct_keys   | 65,535       | Distinct `(table_id, canonical_key)` addresses across point accesses and selected range rows.              |
| 10  | key_bytes       | 1,024        | Maximum canonical user-key length used by any access, including range endpoints and selected rows.         |
| 11  | value_bytes     | 16 MiB       | Maximum schema-encoded argument, constant, initialized register, loaded value, or new stored value length. |
| 12  | register_bytes  | 64 MiB       | Sum of encoded lengths in all currently initialized registers.                                             |
| 13  | range_rows      | 65,535       | Sum of selected logical rows over all executed scans.                                                      |
| 14  | range_bytes     | 64 MiB       | Sum of canonical key and schema-encoded value lengths for those rows.                                      |
| 15  | writes          | 65,535       | Distinct addresses currently in the final-write overlay, including tombstones.                             |
| 16  | overlay_bytes   | 64 MiB       | Sum over overlay entries of `8 + key_length + 1 + value_length`; tombstones have value_length zero.        |
| 17  | result_bytes    | 16 MiB       | Returned value encoding only, without its TypeDesc or Blob wrapper.                                        |

Registers are charged by logical encoded length even when the implementation shares their backing
memory. Overwriting a register replaces its old charge. Overwriting an overlay address replaces its
old charge; a tombstone remains an entry. Range keys join the same distinct-address set as point
keys. A Rows register includes its count and schema-encoded keys and values in value/register bytes;
range_bytes instead uses canonical key bytes, as stated above. No physical page framing, allocation
overhead, hashing implementation cost, or obsolete MVCC entries enter these quantities.

The maximum encoding implied by each program-declared type must satisfy appendix A, but it need not
fit a smaller transaction value_bytes claim: an actual loaded value can fail that claim. Supplied
arguments and constants are checked at admission. Table schema bounds are also checked at use.
Static validation checks all scan immediates against range claims and the program's syntactic
resource counts, but data-dependent register, key, and overlay usage is enforced at runtime. The
acyclic program and bounded individual values establish a finite worst-case work bound even when the
chosen claims cause an earlier deterministic abort.

### D.3 Outcome encoding and abort registry

There is one outcome for each resolved sequence. It has this encoding:

```text
outcome_version: u16 = 1
record_kind:     u8 = 1 (transaction), 2 (catalogue), or 3 (limits)
status:          u8 = 0 (success) or 1 (abort)
sequence:        u64
record_digest:   bytes[32]
reason:          u16
reserved:        u16 = 0
instruction:     u32
user_code:       u32
reserved:        u32 = 0
detail:          u64
returned:        Blob(TypedValue on success; empty on abort)
effects:         Vector<Effect>
```

`record_digest` is SHA256 of the complete canonical log record, including framing and CRC. The
outcome tree key must equal sequence. For success, reason, user_code, and detail are zero and
instruction is `0xffffffff`. A transaction returns its declared result type, re-encoded with those
exact descriptor bounds. Administrative results use the descriptors specified in D.1 and D.2. An
abort has no return bytes and zero effects. Transaction aborts identify the failing instruction;
administrative aborts use instruction `0xffffffff`. User code is nonzero or zero as supplied only
for REQUIRE_FAILED and EXPLICIT_ABORT, and is zero for every other reason.

| Reason   | Name                | Detail                                                        |
| -------- | ------------------- | ------------------------------------------------------------- |
| `0x0000` | SUCCESS             | Zero; legal only with success status.                         |
| `0x0001` | MISSING_KEY         | Zero.                                                         |
| `0x0002` | KEY_EXISTS          | Zero.                                                         |
| `0x0003` | REQUIRE_FAILED      | Zero; user_code carries the instruction's code.               |
| `0x0004` | EXPLICIT_ABORT      | Zero; user_code carries the instruction's code.               |
| `0x0005` | INTEGER_OVERFLOW    | Zero, including invalid checked integer conversion.           |
| `0x0006` | BOUND_EXCEEDED      | Zero; a type descriptor's length or count bound was exceeded. |
| `0x0007` | RESOURCE_LIMIT      | Resource ID 1 through 17.                                     |
| `0x0008` | DIVISION_BY_ZERO    | Zero.                                                         |
| `0x0009` | INVALID_SHIFT       | Zero.                                                         |
| `0x000a` | INDEX_OUT_OF_BOUNDS | Zero, including an invalid byte slice interval.               |
| `0x000b` | INVALID_UTF8        | Zero.                                                         |
| `0x0010` | NAME_IN_USE         | Zero.                                                         |
| `0x0011` | TABLE_NOT_LIVE      | Zero.                                                         |

All other reason numbers are invalid in outcome version 1. Reasons `0x0010` and `0x0011` are legal
only for catalogue records; the other nonzero reasons are legal only for transaction records.
SetLimits has only success outcomes. There is no generic host-error string, I/O abort, timeout
abort, or allocator-error abort in the durable outcome.

An Effect starts with a u8 tag and has the following operands, with no padding:

| Tag    | Effect operands                                                                    |
| ------ | ---------------------------------------------------------------------------------- |
| `0x01` | Put: `table_id: u64, key: Blob(canonical_key), value: Blob(schema_encoded_value)`. |
| `0x02` | Delete: `table_id: u64, key: Blob(canonical_key)`.                                 |
| `0x03` | Catalogue: `table_id: u64, catalogue_value: Blob(CatalogueVersion)`.               |
| `0x04` | Limits: `policy: Blob(LimitPolicy)`.                                               |

A successful transaction's effects contain exactly its final overlay entries, sorted by numeric
table ID then canonical key bytes, without duplicates. Multiple writes to one address produce one
effect. STORE of the same bytes as before still produces a Put, and DELETE of an absent key still
produces a Delete. The effects do not remove semantic writes based on equality with old values. A
successful catalogue or limits record has exactly one corresponding effect. No other combinations of
kind and effect tags are valid.

Version 1 stores values in outcomes, rather than physical page pointers. Each Put or Delete must
agree with the exact state version at the outcome's sequence. These self-contained effects let a
feed avoid looking up a newer value and remain valid across page compaction. Referenced schemas and
the MVCC baseline are still retained under the cursor rules. Outcomes at or below a checkpoint that
protect feed history are authoritative; post-checkpoint outcome caches never suppress replay.

## Appendix E. Canonical log framing

### E.1 Segment header

The log consists of files named `log-<segment_id>.bin`, where the ID is 20 zero-padded decimal
digits. A segment begins with this 96-byte header, followed by the WAL groups in I.2 without
alignment or padding. Neither a group nor a record can cross a segment boundary. Segment IDs
increase in append order but need not be consecutive after a crash. Rotation occurs at group
boundaries.

| Offset | Bytes | Field                                                  |
| ------ | ----- | ------------------------------------------------------ |
| 0      | 8     | Magic `BLOPLG01`.                                      |
| 8      | 2     | Log segment version = 1.                               |
| 10     | 2     | Header length = 96.                                    |
| 12     | 4     | Flags = 0.                                             |
| 16     | 16    | Database ID.                                           |
| 32     | 8     | Segment ID matching the filename.                      |
| 40     | 8     | Sequence of the first record in this segment.          |
| 48     | 32    | Digest of the record preceding the first record.       |
| 80     | 12    | Reserved = 0.                                          |
| 92     | 4     | CRC-32C of the complete header with this field zeroed. |

The first record in the database uses the genesis-file digest as its predecessor. Every subsequent
record uses the preceding record's digest, including across a segment boundary. A segment retained
after older log truncation keeps its original predecessor digest; it is not rewritten with a new
genesis anchor.

### E.2 Record envelope

| Offset           | Bytes       | Field                                                        |
| ---------------- | ----------- | ------------------------------------------------------------ |
| 0                | 4           | Magic `BLR1`.                                                |
| 4                | 2           | Header length = 64.                                          |
| 6                | 2           | Log record version = 1.                                      |
| 8                | 4           | Total record length = `72 + body_length`, at most 64 MiB.    |
| 12               | 4           | Body length.                                                 |
| 16               | 8           | Sequence number.                                             |
| 24               | 1           | Kind: 1 transaction, 2 catalogue, 3 SetLimits.               |
| 25               | 1           | Flags = 0.                                                   |
| 26               | 2           | Reserved = 0.                                                |
| 28               | 32          | Predecessor record digest, or genesis digest for sequence 1. |
| 60               | 4           | Reserved = 0.                                                |
| 64               | body_length | Body defined in C.2 or D.1/D.2.                              |
| 64 + body_length | 4           | CRC-32C of header and body only.                             |
| 68 + body_length | 4           | Repeated total record length.                                |

This canonical envelope is contained in a WAL group. Its own checksum validates a record, not a
commit boundary. A complete group is the local durability unit; its framing does not enter the
canonical record digest. A manifest records guaranteed complete-group prefixes, and recovery may
discover later complete groups using E.3 and I.4. Accepted recovered groups must be flushed before
execution.

Record CRC validation precedes interpretation. Validate both length copies, sequence continuity,
kind/body compatibility, and the hash chain. Do not search forward for the next magic string after
an invalid committed record. A mismatch inside a published segment prefix is corruption, even in the
last record. Missing bytes inside that prefix are also corruption, not a recoverable tail.

### E.3 Durability boundary and tail handling

The selected manifest establishes a lower bound on D, the durable frontier, and exact guaranteed
prefixes for its listed segments. The active segment can contain later complete groups; new linked
segments can also extend the durable chain without a manifest update. Existing durable prefixes are
immutable. Recovery discovers complete groups from the selected bounds, validates their framing and
canonical chain, trims only physically incomplete terminal appends, and establishes durability
before replay as specified in I.4. It never skips an invalid complete group to search for later
records.

Advance live D only after flushing a complete WAL group and, for a newly created segment, its
directory entry. No manifest-pointer update is needed for this append. Dispatch waits for that
durability step; receipts also wait for complete installation and contiguous visibility. A complete
valid group may survive a failed flush or a lost receipt and be recovered after reopening. That is
an uncertain submission. Checkpoints and cursor metadata use G.3 separately.

Removing records uses whole-segment deletion after publication of a manifest that no longer needs
those segments. The active segment may be rotated first. A retained segment can contain extra
records before the required retention floor; retaining extra history is safe. No bytecode outcome is
inferred from an append record's presence or from a materialized page.

## Appendix F. Binary B+ tree storage

### F.1 Page file and addressing

Version 1 stores all five system trees in one append-only page file named `pages-<file_id>.bin`,
with a 20-digit zero-padded decimal ID. Every page is exactly 16,384 bytes. Page P starts at byte
offset `P * 16384`; page IDs are file-local u64 integers. Page 0 is the file header and is never a
tree node. A zero root ID means an empty tree; a zero child ID is invalid. A manifest's page_count
includes page 0 and is at least 1 and at most `2^48`. References must be below that count, and
offset calculations must not overflow or exceed the actual file length.

Published pages are immutable. New nodes and overflow pages are appended, and old paths are replaced
by new paths through copy-on-write. There is no persisted free list, in-place page reuse, physical
undo log, prefix compression, encryption, or sibling-link repair in storage version 1. Unreachable
space is recovered by whole-file compaction or by removing an unpublished crash tail. These
restrictions keep the initial allocation and crash format small and unambiguous.

### F.2 Common page header

Every page, including page 0, has this 64-byte header:

| Offset | Bytes | Field                                                                        |
| ------ | ----- | ---------------------------------------------------------------------------- |
| 0      | 4     | Magic `BLP1`.                                                                |
| 4      | 2     | Page format version = 1.                                                     |
| 6      | 1     | Kind: 0 file header, 1 leaf, 2 internal, 3 overflow.                         |
| 7      | 1     | Level: leaf/overflow/file header = 0; internal = distance to leaves.         |
| 8      | 8     | This page ID, equal to its file offset divided by page size.                 |
| 16     | 4     | Tree ID, or zero for the file header.                                        |
| 20     | 2     | Cell count for a leaf/internal node; otherwise zero.                         |
| 22     | 2     | Reserved = 0.                                                                |
| 24     | 2     | Lower free-space boundary.                                                   |
| 26     | 2     | Upper free-space boundary.                                                   |
| 28     | 4     | Reserved = 0.                                                                |
| 32     | 8     | Leftmost child for an internal node; next page for overflow; otherwise zero. |
| 40     | 4     | Payload byte count for file-header/overflow pages; otherwise zero.           |
| 44     | 16    | Reserved = 0.                                                                |
| 60     | 4     | CRC-32C of all 16,384 bytes with this field zeroed.                          |

Page 0 has tree ID zero, lower = 64, upper = 16384, and payload byte count = 32. Bytes 64 through 95
are `magic: bytes[8] = BLOPST01 || database_id: bytes[16] || file_id: u64`; the remaining payload
bytes are zero. Only page 0 may have kind 0. Its identities must agree with the manifest and
filename.

Every other page has a tree ID from F.5. A child or overflow reference must remain within that tree
and file. Validate the CRC and header before following any offsets. Node depth is limited to 255,
and child levels must decrease by exactly one. Cycles, repeated ownership of a child in one tree
root, and child references to file-header or overflow pages are corruption.

### F.3 Slotted leaf and internal nodes

A node's slot array starts at byte 64. Each slot is `cell_offset: u16 || cell_length: u16`. Slots
appear in strictly increasing key order. The cells are packed backwards from the end of the page in
slot order: slot 0's cell ends at byte 16384, slot 1's cell ends at slot 0's start, and so on. There
are no holes, overlapping cells, duplicate keys, or deleted slots.

For n cells, `lower = 64 + 4 * n` and `upper = 16384 - sum(cell_lengths)`. Require
`64 <= lower <= upper <= 16384`; every byte in `[lower, upper)` is zero. Cell lengths must exactly
match the decoded content. The complete physical key is stored in each cell, with no prefix
shortening. All node keys use unsigned lexicographic byte order and have length at most 2,066.

Leaf cells have this encoding:

```text
key_length:    u16
storage:       u8 = 0 (inline) or 1 (overflow)
reserved:      u8 = 0
value_length:  u32
overflow_head: u64
key:           bytes[key_length]
inline_value:  bytes[value_length] only when storage = 0
```

Values of at most 1,024 bytes must be inline with overflow_head zero. Larger values, up to 128 MiB,
must be entirely in an overflow chain with nonzero overflow_head and no inline bytes. Zero-length
values are structurally legal, though each system tree still validates its own value format. The
storage byte is not an MVCC tombstone flag; tombstones are logical values in the state tree.

Internal cells have this encoding:

```text
key_length:  u16
reserved:    u16 = 0
right_child: u64
separator:   bytes[key_length]
```

An internal node with separators `K[0..n)` has n + 1 children. Its header supplies child 0 and cell
i supplies child i + 1. Separator K[i] is exactly the minimum full key in child i + 1's subtree. All
keys in child i are less than K[i], and all keys in child i + 1 are at least K[i]. Search chooses
child 0 for a key less than K[0], or the child after the greatest separator less than or equal to
the search key. In particular, equality descends to the right of its separator.

Every nonempty tree has at least one key. Every leaf has at least one cell, and every internal node,
including the root, has at least one separator and two children. All leaves beneath a root have the
same depth. Empty trees use root zero; a single-child root is collapsed to that child. Removing the
last key removes the root. There is no mandatory half-full occupancy rule: variable-sized cells and
the maximum key size make byte occupancy, rather than a fixed minimum entry count, the relevant
split heuristic. Writers must split or redistribute before a page would overflow and must remove
empty children and update separators after deletion. They may merge other underfilled nodes.

Range traversal uses an ancestor stack from one immutable root, not leaf next/previous pointers.
This avoids mixing old and new generations through stale sibling links after a copy-on-write split.
Different valid split choices may produce different page bytes for the same ordered contents.

### F.4 Overflow chains

An overflow page has no slots: cell_count = 0, lower = 64, upper = 16384, level = 0. Its data starts
at byte 64 and has payload byte count from 1 through 16,320. Bytes after the payload are zero. The
header link points to the next overflow page, or zero at the end.

Concatenate payloads in link order to obtain the complete leaf value. Every nonfinal page has 16,320
payload bytes; the final page has exactly the remaining byte count. Chain length must equal
`ceil(value_length / 16320)`. A short chain, excess data, a nonzero link after the final byte, wrong
page kind/tree ID, repeated page, or checksum failure is corruption. A chain belongs to one leaf
cell within a root and cannot be shared by different cells. Different immutable generations may
share the same unchanged cell and chain. No key or separator is stored in overflow pages.

### F.5 System tree registry

The manifest publishes roots for these trees in this order. Unknown tree IDs are invalid.

| ID  | Tree      | Key                                                       | Value                                                                                                            |
| --- | --------- | --------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| 1   | State     | StateKey from B.3.                                        | `tag: u8 = 0` alone for a tombstone; or `tag: u8 = 1` followed by `value: Blob(schema_encoded_value)` for a Put. |
| 2   | Catalogue | `table_id: be64` followed by `be64(~effective_sequence)`. | CatalogueVersion from D.1.                                                                                       |
| 3   | Policy    | `effective_sequence: be64`.                               | LimitPolicy from D.2.                                                                                            |
| 4   | Outcomes  | `sequence: be64`.                                         | Outcome from D.3.                                                                                                |
| 5   | Cursors   | `cursor_id: be64`.                                        | CursorValue from G.4.                                                                                            |

The state tree has at most one entry for `(table_id, key, sequence)`. An installed version is the
final Put or Delete from a successful transaction, never an intermediate overlay state. Stored
values must decode exactly under the table's immutable schema. Catalogue entries use the sequence of
the successful administrative operation; the first version of a table has table_id equal to
effective_sequence. The genesis catalogue and state trees are empty.

The policy tree has an initial entry at sequence 0 and one entry for each successful SetLimits. The
policy at S is the greatest key no greater than S. Catalogue lookup uses the same descending version
seek as state lookup; a dropped catalogue version stops lookup as not-live. Names are resolved by
live catalogue versions at the requested sequence; an in-memory name index is derived, not a
separately persisted authority.

A checkpoint at C with history floor G stores every state version with `G < sequence <= C` and the
newest version at or below G for each address if one exists, including tombstones. It stores every
outcome with `G < sequence <= C`, including aborts and no-write outcomes, and all catalogue/policy
versions through C. Additional older state versions and outcomes may be kept. None of these four
checkpoint roots may expose entries above C. The cursor root is current local metadata and is not
bounded by C; its positions may be above C when the durable log supplies recovery through them.

### F.6 Atomic installation and allocation

An in-memory installation publishes the new state root and its complete outcome together only after
all overlay entries are present. Parallel blind writers must merge their updates into the latest
installed root, using serialized root publication or compare-and-retry. Publishing a root built from
a stale base must not lose a different transaction's installed versions. The public frontier still
gates visibility even though an internal root may contain versions above it.

Durable publication uses a separate checkpoint view filtered at C, never the current unfiltered live
root. A checkpoint writer may build this view while execution continues, provided it pins its source
pages and required history. New page IDs append after the file's allocated tail; each page is
written completely before a manifest can reference it. Intermediate private pages are not committed
merely because they are present in the file.

Version 1 compaction uses a quiescent writer handover. Pause sequencing, finish all durably logged
work so F = D, and stop page installation and concurrent checkpoint writers. Build a checkpoint at C
= F = D using all current retention claims. Copy its four logical roots and the current cursor root
to a new file ID, rewriting every child and overflow reference, then flush and publish the new file
and roots through G.3. Adopt these roots as the live roots before resuming sequencing and
installation. Cursor metadata changes must serialize with the handover or wait until it finishes.
This drain ensures that neither visible post-checkpoint state nor installed above-frontier state is
lost when the live roots move to the new file.

Keep the old file pinned until every root that can reference it has retired, including idle snapshot
handles, in-flight readers, pending writers, checkpoint work, and backups. Absence from CURRENT
alone does not permit deletion while the process still holds such roots. Across a crash, only the
CURRENT-selected file is authoritative. Within a file, published page IDs are never recycled. This
version therefore needs no persistent allocator bitmap or per-page free-generation metadata.

## Appendix G. Database directory, checkpoints, and retention metadata

### G.1 Directory and genesis

An open version 1 database has one exclusive process owner. The implementation must hold an
operating-system lock preventing another process from opening it for reads or writes that could race
publication or reclamation. The lock mechanism has no portable persisted payload.

The directory contains `GENESIS`, `CURRENT`, the selected `manifest-<generation>.bin`, its selected
`pages-<file_id>.bin`, and its retained `log-<segment_id>.bin` files. Numeric filename components
are 20 zero-padded decimal digits. Old manifests, page files, and uncommitted temporary output may
also be present, but are not alternative authorities. Temporary filenames are implementation-local
and must not be interpreted as any of these published objects.

`GENESIS` is immutable and has this encoding:

```text
magic:          bytes[8] = BLOPGN01
version:        u16 = 1
flags:          u16 = 0
total_length:   u32
database_id:    bytes[16]
initial_policy: Blob(LimitPolicy)
crc32c:         u32
```

Its catalogue, state, and outcomes are empty; its policy is effective at sequence 0. The genesis
digest is SHA256 of this complete file. For checkpoint or durable frontier 0, the corresponding
last-record digest is this genesis digest. Genesis must be preserved even after original log
segments are removed. Replacing it creates a different history, not a configuration change.

Creation writes and flushes GENESIS, page 0, and the policy tree's initial entry in page file 1,
then publishes manifest generation 1 with C = D = G = 0, log_floor = 1, no segments, and an empty
cursor root. Initial next_cursor_id and next_segment_id are 1; next_page_file_id is 2. Select the
cursor namespace as described in G.4. Until a valid CURRENT is published, the directory is an
incomplete creation, not an empty database that may be silently initialized over existing files.

### G.2 Manifest and publication pointer

Each immutable manifest contains exactly:

```text
magic:                 bytes[8] = BLOPMF01
version:               u16 = 1
flags:                 u16 = 0
total_length:          u32
database_id:           bytes[16]
genesis_digest:        bytes[32]
cursor_namespace:      bytes[16]
generation:            u64
page_file_id:          u64
page_count:            u64
checkpoint_sequence:   u64
checkpoint_digest:     bytes[32]
durable_sequence:      u64
durable_digest:        bytes[32]
history_floor:         u64
log_floor:             u64
next_cursor_id:        u64
next_segment_id:       u64
next_page_file_id:      u64
state_root:            u64
catalogue_root:        u64
policy_root:           u64
outcomes_root:         u64
cursors_root:          u64
segments:              Vector<SegmentDescriptor>
crc32c:                u32

SegmentDescriptor =
    segment_id:         u64
    first_sequence:     u64
    last_sequence:      u64
    committed_bytes:    u64
    predecessor_digest: bytes[32]
    last_digest:        bytes[32]
```

A manifest is at most 16 MiB. It must satisfy
`0 <= history_floor <= checkpoint_sequence <= durable_sequence` and
`1 <= log_floor <= checkpoint_sequence + 1`. Segments are ordered by sequence, nonempty, and
together cover exactly `[log_floor, durable_sequence]` without gaps or overlap. If the vector is
empty, D = C and log_floor = D + 1. A descriptor's committed_bytes includes the 96-byte segment
header and ends at the complete group containing last_sequence, which must be that group's last
record. Segment headers, filenames, predecessor digests, and final digests must agree with the
descriptors. Adjacent descriptors must chain to each other. Segment IDs are strictly increasing.

The manifest's durable_sequence is a guaranteed lower bound on live D. Later complete groups are
discovered according to I.4. For N records, a descriptor needs at least
`96 + 72 * N + 168 * ceil(N / 64)` bytes and at most `96 + N * (64 MiB + 168)` bytes.

The chain at C must equal checkpoint_digest when C is in retained log coverage; when C is just
before log_floor, the first descriptor must use checkpoint_digest as its predecessor. The last
descriptor's digest must equal durable_digest. With no segments, durable_digest equals
checkpoint_digest. Published C, D, and the history/log floors never decrease. The initial policy
tree entry must exactly equal GENESIS. The roots, page_count, and logical tree contents must meet
F.5; policy_root is always nonzero.

Each published manifest advances its next-ID fields past every durable ID known at that publication,
even after objects are released or files are deleted. Recovery also advances the live segment
counter past discovered complete WAL segments. `2^64 - 1` in a next-ID field means exhausted; it is
not allocatable. Incomplete orphan output does not allocate durable identities; allocation skips its
filenames rather than overwriting them. Manifest generations strictly increase after each metadata
publication; an implementation may skip generations to avoid an orphan filename collision.

`CURRENT` is exactly 64 bytes:

| Offset | Bytes | Field                                  |
| ------ | ----- | -------------------------------------- |
| 0      | 8     | Magic `BLOPCU01`.                      |
| 8      | 2     | Metadata version = 1.                  |
| 10     | 2     | Reserved = 0.                          |
| 12     | 4     | Length = 64.                           |
| 16     | 8     | Selected manifest generation.          |
| 24     | 32    | SHA256 of that complete manifest file. |
| 56     | 4     | Reserved = 0.                          |
| 60     | 4     | CRC-32C of preceding 60 bytes.         |

The manifest filename is derived from the generation in CURRENT. Its own generation must agree.
CURRENT is the checkpoint and retention-metadata selection authority. A valid higher-generation
manifest left by an interrupted publication is not committed just because its filename sorts last.

### G.3 Flush and atomic publication protocol

Version 1 assumes a filesystem that provides atomic same-directory rename-over-existing, durable
file flushing, and durable directory flushing. After a crash, a flushed rename must select the new
file; a rename not yet flushed may select the complete old or complete new file, not torn mixed
contents. Hardware and filesystem failures that violate these guarantees are outside the crash
model. An implementation without these primitives must refuse durable mode or use a separately
versioned publication protocol, not assume that a sector-sized write is atomic.

Serialize all publication operations, including log group commits, checkpoints, cursor changes, and
compaction. Build each new manifest from the latest live durable metadata, preserving unrelated
updates. For example, a checkpoint built concurrently with checkout must incorporate the new cursor
claim or restart with a sufficiently conservative history floor.

The following steps publish checkpoints and storage metadata. Ordinary log appends use the one-flush
WAL procedure in I.3 and can advance D without this metadata publication.

1. Write new log bytes and new pages. Complete all referenced file contents, flush those files, and
   flush the directory for any new filenames. Never overwrite a published page or log prefix.
1. Write the new manifest to a unique temporary file, flush it, rename it to its final generation
   filename, and flush the directory. Its C, D, roots, digests, and retention claims must describe
   only complete flushed data. The final filename must not replace a published manifest.
1. Write a complete new CURRENT to a different temporary file and flush it. Atomically rename that
   file over CURRENT, then flush the directory. Only now report the metadata publication durable or
   acknowledge a cursor metadata operation.
1. Reclaim unreferenced old files or log segments only after the replacement pointer is durable and
   all in-flight users have released the old objects. Flush the directory after deletions when their
   durable removal matters to space accounting.

On an uncertain failure during publication, stop new dispatch and reclamation until recovery
establishes which CURRENT is selected. Never publish another manifest from a guessed base. A crash
before pointer replacement uses the old checkpoint and discovers its WAL suffix; a crash after
replacement may use the complete new checkpoint even before its caller saw success. Unselected
materialized pages are not recovery authority. New referenced data was flushed first in either case.
There is no fallback to an older checkpoint if a valid CURRENT points to missing or corrupt
authoritative data: that could lose acknowledged transactions or cursor registrations. Explicit
offline repair is distinct from normal recovery.

### G.4 Durable cursor metadata

The manifest's nonzero 16-byte cursor_namespace identifies this directory's local retention
registrations, independently of the canonical database_id. Select a fresh unique namespace outside
the VM at creation. Ordinary crash recovery preserves it. Every cursor token carries both
identities, and cursor APIs reject a namespace mismatch before looking up a numeric cursor ID.

Restoring an older backup or attaching a copied directory as a replica is an explicit operation, not
ordinary crash reopening. Before enabling cursor APIs, it must durably publish a fresh cursor
namespace through G.3. Copied registrations, baselines, and next_cursor_id are preserved, but their
old tokens are invalid. An explicit administrative rebind issues new tokens for those retained
registrations after the consumer selects the correct copied cursor ID and validates its watermark.
There is no automatic rebind by an untrusted old token or by a possibly nonunique label. This
prevents tokens issued after the backup, or in a different replica, from naming a new registration
that happens to reuse the same local counter value. A crashed restore retries the explicit attach
operation before allowing normal access; an extra namespace change is safe and does not release any
claim. Merely copying files does not complete attachment as an independently open directory.

The cursor tree value is:

```text
cursor_version: u16 = 1
kind:           u8 = 1 (resolved feed), 2 (logical feed), or 3 (log replica)
flags:          u8 = 0
baseline:       u64
label:          Text
```

The label is 0 through 255 UTF-8 bytes excluding NUL, for diagnostics, and need not be unique. The
tree key is a nonzero local cursor ID. Baseline is the checkout or last acknowledged sequence,
always at most D in the published manifest. It may exceed checkpoint C because recovery replays
through D before serving the cursor. A registration remains present until explicit release.

Checkout serializes with retention decisions, verifies `G <= baseline <= F` and availability of all
required state/outcomes, and, for kinds 2 and 3, verifies original log availability from baseline +
1 onward. It allocates next_cursor_id, adds the immutable cursor-tree entry, and publishes the
updated root and incremented ID through G.3 before returning its token. A snapshot-plus-tail
operation pins the selected view while performing this same publication. Empty-tail checkout at F is
valid even when there are no later records yet.

Acknowledgement requires an existing ID, the same feed kind, and
`old_baseline <= new_baseline <= F`. It replaces that cursor value and publishes durably before
returning. Repeating the same baseline is successful. Release removes the entry and publishes the
new root; repeating release of an already absent issued ID is successful, but must not release a
different registration. Cursor IDs are never reused within the local namespace, and tokens for
another database or cursor namespace are rejected. An ID at or above next_cursor_id has not been
issued and is an invalid token. Cursor operations are local retention metadata, not canonical log
records, and consume no transaction sequence numbers.

Every checkpoint history_floor must be no greater than the minimum baseline of current cursors and
all other protected floors in section 15.2. Every logical/replica cursor additionally requires
log_floor no greater than baseline + 1. Checkout cannot resurrect history below the published
floors. Advancing or releasing a cursor may allow a later checkpoint to raise those floors; it does
not itself delete history. Ephemeral snapshot claims are not persisted because process restart
invalidates their handles, but live readers still block unsafe reclamation.

### G.5 Recovery and backup rules

Recovery validates CURRENT, the selected manifest's digest and CRC, GENESIS and its digest, page 0,
the reachable checkpoint/cursor trees, and each selected log prefix, then discovers and flushes the
complete WAL suffix under I.4. Corruption of unreachable orphan pages does not invalidate a
checkpoint; corruption of a referenced page does. Restore cursor registrations before reclamation,
restore the four logical trees at C, and replay every record in `(C, D]` using the historical policy
and catalogue. A record that fails replay validation indicates corruption or an unsupported
implementation, not a newly invented abort. Set the recovered frontier to C initially and advance it
only as replay resolves the contiguous prefix.

The manifest does not store a later live visibility frontier or a completion bitmap. Recovery
reconstructs those from C and the durable log. Bytes beyond page_count pages are excluded; log bytes
beyond selected prefixes are classified by I.4. After validating the recovered view and excluding
active readers, incomplete tails and unused files may be removed. Retained outcomes at or below C
cannot be discarded as disposable caches. Post-C materialization is never a recovery starting point.

A physical backup is a database directory image, not an unspecified archive format. Capture live D
and its complete-group log bounds with the selected checkpoint roots, page count and retention
metadata. Synthesize a manifest for that pinned prefix, copy GENESIS, exactly page_count complete
pages and exactly those log prefixes, then write a matching CURRENT last using G.3 ordering in the
destination. Source files must remain pinned until copying finishes. Copying the live CURRENT and
then independently copying whatever filenames happen to exist is not a consistent backup. Restore
retains the backed-up cursor registrations; a restored consumer must resume from a watermark
protected by that backup, not assume it contains later source history. A read replica may explicitly
clear copied source-local cursor registrations by a new local manifest publication before applying
its own retention policy. Source and replica retain the same genesis identity, but must not become
independent writable primaries under it.

## Appendix H. Exchange formats and conformance cases

### H.1 Feed batches

A binary feed batch has the following layout. Transport setup, authentication, compression outside
the batch, and request/response APIs are not part of this embedded database format.

```text
magic:           bytes[8] = BLOPFE01
version:         u16 = 1
kind:            u8 = 1 (resolved) or 2 (logical)
flags:           u8 = 0
total_length:    u32
database_id:     bytes[16]
start_exclusive: u64
end_inclusive:   u64
record_count:    u32
reserved:        u32 = 0
records:         FeedRecord[record_count]
crc32c:          u32

Resolved FeedRecord = outcome: Blob(Outcome)
Logical FeedRecord  = log_record: Blob(LogRecord) || outcome: Blob(Outcome)
```

The fixed header is 56 bytes. The total length includes the header and final CRC and is at most 256
MiB. A batch never splits an outcome, a log record, or a transaction's effects. Select a smaller
record count when the next complete record would exceed the byte ceiling. A single maximum-sized
version 1 record and outcome fit in one batch.

Require `record_count = end_inclusive - start_exclusive`, using checked subtraction, and strictly
consecutive sequence numbers from start_exclusive + 1 through end_inclusive. Empty polling batches
have count zero and equal endpoints. All returned records must be no greater than the source's
visibility frontier captured for the batch. Aborts, no-write transactions, and administrative
records remain present; table-filtered feeds that silently omit sequence positions are not this
format. Consumers may filter only after accounting for complete source records.

Logical records must match their outcomes in sequence, kind, and SHA256 record digest, and chain to
each other. Import has two separate interpretation stages. First, pause local append and finish any
outstanding replay so local F = D. Pin that prefix and check the incoming first predecessor digest
against its last-record or checkpoint anchor. Validate the batch and run the sequential reference
interpreter in an isolated disposable copy of the prefix, comparing every generated outcome
byte-for-byte with the supplied outcome. This is import validation, not transaction dispatch: it
cannot install live versions, notify callers, advance a frontier, or produce a feed. A mismatch is
divergence, not permission to accept source effects instead of executing the VM.

Only after all comparisons succeed may import append those exact canonical record bytes and publish
their durable prefix through G.3. Then replay them through the normal live installation path and
advance visibility. Keep local append serialized from anchor validation through durable publication
so the validated base cannot change. A crash before publication discards validation work; a crash
after publication replays a prefix whose expected outcomes were already verified. The source
outcomes need no separate pending-verification journal. System errors during either stage stop
progress rather than becoming semantic aborts.

Resolved feeds use their complete effects without a VM and require the baseline catalogue plus
subsequent catalogue events to decode values. A resolved-only feed is not enough to reconstruct the
original canonical bytecode log.

Raw log replication may instead copy E.1 segments and E.2 records, but must restrict publication to
a source-visible prefix and validate the same database identity, anchors, and continuity. Local
segment packaging may differ; canonical record bytes and sequence numbers may not. Original-log
retention is required for both logical feed and raw-log replica cursors.

### H.2 Cursor tokens and consumer watermarks

A serialized local cursor token is exactly 56 bytes:

```text
magic:            bytes[8] = BLOPCT01
version:          u16 = 1
kind:             u8 = 1, 2, or 3, as in G.4
reserved:         u8 = 0
database_id:      bytes[16]
cursor_namespace: bytes[16]
cursor_id:        u64
crc32c:           u32
```

The token is an identity reference, not proof of registration, an authorization secret, or a
retention claim on its own. Reopen checks the current cursor tree. An absent released registration
reports cursor-released; it must not create a replacement at the same number or silently choose a
new baseline. Both identities are checked even when an integer cursor ID happens to exist locally.
Restored copies and replicas require explicit token rebinding under G.4.

A consumer's source watermark is exactly 40 bytes:

```text
magic:       bytes[8] = BLOPWM01
version:     u16 = 1
reserved:    u16 = 0
database_id: bytes[16]
sequence:    u64
crc32c:      u32
```

The consumer stores these bytes in the same atomic durable commit as its derived data. If an index
commit API accepts only text, encode the complete 40 bytes as exactly 80 lowercase hexadecimal ASCII
characters without a prefix, whitespace, or newline. Decode and check the watermark before
acknowledging its sequence to the source cursor. The source sequence is not an index library's
internal operation stamp. This format gives a concrete payload for the protocol in section 24; the
index's own segment and commit formats remain that library's responsibility.

### H.3 Primitive and bytecode vectors

Compatible implementations must produce the following primitive encodings:

| Input                                              | Expected bytes or digest                                           |
| -------------------------------------------------- | ------------------------------------------------------------------ |
| CRC-32C of ASCII `123456789`, stored little-endian | `83 92 06 e3`                                                      |
| SHA256 of empty bytes                              | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| TypeDesc for U64                                   | `01 00 03`                                                         |
| TypeDesc for String with maximum 8 bytes           | `01 00 05 08 00 00 00`                                             |
| Value encoding of U64 42                           | `2a 00 00 00 00 00 00 00`                                          |
| Value encoding of String `a`                       | `01 00 00 00 61`                                                   |
| Canonical key for I64 -1                           | `7f ff ff ff ff ff ff ff`                                          |
| Canonical key for I64 0                            | `80 00 00 00 00 00 00 00`                                          |
| Canonical key for U64 7                            | `00 00 00 00 00 00 00 07`                                          |
| Escape of bytes `41 00 42`                         | `41 00 ff 42 00 00`                                                |
| Canonical key for Tuple(U64 7, String `a`)         | `00 00 00 00 00 00 00 07 61 00 00`                                 |

This complete 79-byte program returns U64 42, has one U64 register, no arguments or tables, and one
constant. Whitespace below is for presentation only:

```text
42 4c 4f 50 56 4d 30 31 01 00 00 00 4f 00 00 00
01 00 00 00 00 00 01 00 02 00 00 00 00 00 00 00
03 00 00 00 01 00 03
03 00 00 00 01 00 03
03 00 00 00 01 00 03 08 00 00 00 2a 00 00 00 00 00 00 00
01 00 04 00 00 00 00 00
64 00 02 00 00 00
```

Its Arguments and AccessManifest encodings are each `00 00 00 00`. The smallest sufficient claims
vector, in resource-ID order, is:

```text
[79, 2, 1, 0, 4, 0, 0, 0, 0, 0, 8, 8, 0, 0, 0, 0, 8]
```

With those fields, its transaction body is 239 bytes and its complete log record is 311 bytes. The
record's two total-length fields are both `37 01 00 00`. The sequence, predecessor digest, and CRC
depend on its placement in the database history. Execution under a policy admitting these claims
must return U64 42, produce zero effects, and advance the frontier through its sequence.

### H.4 Tree and recovery conformance

For table 1, canonical String key `a`, and version 9, the full StateKey is 23 bytes:

```text
00 00 00 00 00 00 00 01
61 00 ff 00 ff 00 00
ff ff ff ff ff ff ff f6
```

A leaf containing only this key with a tombstone has a 40-byte cell: 16-byte leaf-cell header,
23-byte key, and one value byte `00`. Its single slot has offset 16344 and length 40, lower = 68,
and upper = 16344. With any valid nonzero page ID and tree ID 1, this is a valid page after filling
reserved/free bytes with zero and computing its CRC. It represents a tombstone at sequence 9, not an
empty Bytes value. The same address's version 10 sorts before version 9. A snapshot at 8 must not
observe either; a snapshot at 9 observes absence without falling through to an older Put.

In addition to section 26, an implementation must test these binary behaviours:

- Round-trip every supported type, bound-zero value, tuple, key escape, and integer extreme. Check
  that key-byte order agrees with the comparator and that physical version order is descending.
- Decode the fixed program above independently of native endianness. Reject unknown opcodes,
  incorrect operand lengths, non-forward or out-of-bounds instruction-index targets, uninitialized
  register uses at joins, reachable fall-through, unreachable instructions, and mismatched shapes.
- Check arithmetic overflow, I64 minimum divided/remaindered by -1, division by zero, shifts of
  0/63/64, invalid UTF-8, slice endpoints, explicit user abort codes, and deterministic failure
  order.
- Check INSERT at an existing key with an oversized replacement: KEY_EXISTS precedes the new-value
  bound failure. Check two invalid scan endpoints: finish all lower-endpoint checks before any
  upper-endpoint checks, including mixed descriptor-bound and key_bytes failures.
- Verify identical scan results and resource failures for the same logical view with different page
  shapes, obsolete versions, overlay histories, and later invisible writes. Check empty and
  equal-endpoint intervals and exact row/byte limits without an extra charged lookahead row.
- Verify leaf inline values of 1,024 bytes and overflow values of 1,025 bytes. Verify chains of
  exactly 16,320 and 16,321 bytes, key-size boundaries, separator equality routing, root collapse,
  split propagation, and range iteration across copy-on-write generations.
- Reject overlapping or misordered slots, nonzero free bytes, invalid levels, cross-tree children,
  cycles, bad overflow lengths, duplicate versions, incorrect catalogue schemas, and outcome/key
  sequence mismatches, even when an outer checksum has been recomputed.
- Truncate each fixed header and variable-length object at every byte boundary. Reject oversized
  counts before allocation, checked-arithmetic overflow, unknown flags/versions, extra trailing
  data, changed checksums, and broken record hash chains.
- Crash before and after every file flush, rename, and directory flush in G.3. Recovery must use
  exactly the old or new CURRENT-selected checkpoint, then validate and flush complete WAL groups
  beyond its log bounds. An unselected higher-generation manifest or partially installed overlay is
  never a recovery starting point.
- Corrupt or remove a record inside a published prefix and require corruption, not silent tail
  truncation. Physically incomplete terminal WAL appends must be discardable without changing the
  preceding complete prefix; complete malformed groups must fail as specified in I.4.
- Interleave cursor checkout, acknowledgement, release, checkpoint publication, and compaction.
  Verify that unrelated manifest updates are preserved and that protected history is never deleted
  using a stale retention root. Start compaction with C < F and installed versions above F, drain
  work, and verify the live-root handover while an idle old snapshot still pins the original file. A
  released cursor ID must not be reused after restart.
- Copy a pinned physical backup while the source continues writing; restore its exact checkpoint and
  replay its complete durable suffix. Verify that a wrong database ID, missing retained outcome, or
  mismatched source watermark cannot silently resume a feed from a different history.
- Restore an older backup and allocate a cursor whose numeric ID was issued later by the source.
  Verify that old source tokens cannot acknowledge or release the restored registration. Apply the
  same check to independently allocated replica-local cursors and explicit token rebinding.
- Crash logical import before comparison, between comparison and durable publication, and between
  durable publication and live replay. Never publish unverified outcomes or lose the normal
  durable-before-dispatch guarantee.

Conformance requires both byte-level validation and the sequential-equivalence tests. Checksums
alone cannot establish semantic validity, and matching final table contents alone cannot establish
compatible outcomes, historical snapshots, resource accounting, or retention behaviour.

## Appendix I. WAL commit groups

### I.1 Checkpoint selection and live durability

CURRENT uses the version-1 layout in G.2. WAL group and segment headers also use version 1; unknown
versions are rejected. There is one publication protocol and one log reader/writer format.

The selected manifest is authoritative for C, materialized roots, page-file prefix, cursor metadata,
retention floors and allocation counters. Its D and log descriptors are a durable lower bound, not
necessarily the latest durable frontier. The live owner maintains an expanded manifest-shaped value
with current D and exact complete-group log bounds. Normal WAL appends do not change CURRENT or its
selected manifest generation.

Segments use the 96-byte E.1 header and magic `BLOPLG01`. Canonical E.2 record bytes are wrapped in
local groups; group framing does not consume a sequence number and does not enter canonical record
hashes or logical replication bytes.

### I.2 Group framing

All integer fields below are little-endian. A group is a 112-byte header, consecutive complete E.2
records, and a 56-byte trailer. It contains 1 through 64 records. Total group length is
`168 + sum(canonical_record_lengths)`, from `168 + 72 * count` through `168 + 64 MiB * count`.
Arithmetic is checked before allocation or traversal. The first sequence is nonzero, and
`first + count` must fit u64, so no record uses the reserved u64 maximum.

| Header offset | Bytes | Field                                             |
| ------------: | ----: | ------------------------------------------------- |
|             0 |     8 | Magic `BLOPWG01`.                                 |
|             8 |     2 | Group version = 1.                                |
|            10 |     2 | Flags = 0.                                        |
|            12 |     4 | Header length = 112.                              |
|            16 |     8 | Total group length, including header and trailer. |
|            24 |     8 | First canonical sequence number.                  |
|            32 |     4 | Canonical record count.                           |
|            36 |     4 | Reserved = 0.                                     |
|            40 |    32 | Predecessor canonical record digest.              |
|            72 |    32 | Last canonical record digest in this group.       |
|           104 |     4 | Reserved = 0.                                     |
|           108 |     4 | CRC-32C of header bytes 0 through 107.            |

| Trailer offset | Bytes | Field                                                                              |
| -------------: | ----: | ---------------------------------------------------------------------------------- |
|              0 |     8 | Magic `BLOPGE01`.                                                                  |
|              8 |     8 | Repeated total group length.                                                       |
|             16 |    32 | SHA256 of the complete header, including its CRC, and every canonical record byte. |
|             48 |     4 | Reserved = 0.                                                                      |
|             52 |     4 | CRC-32C of trailer bytes 0 through 51.                                             |

Every canonical envelope must also pass its own length, CRC, version, kind, sequence and predecessor
checks. The group must contain exactly its declared record count and bytes and end at its declared
last digest. Selected segment endpoints must coincide with complete group endpoints. A checkpoint
sequence may lie inside a group; the whole group remains in the retained log. Descriptor byte counts
include framing, with the minimum and maximum sizes specified in G.2.

### I.3 Append and checkpoint ordering

Serialize append and metadata publication on the directory owner:

1. Validate the bounded group and reserve its canonical sequences in order.
1. Write its header, canonical records and trailer to the active segment without changing an
   existing committed prefix. On rotation, create a fresh noncolliding segment ID and write its
   complete version-1 segment header first.
1. Flush the segment file once. For a new segment, also flush its directory entry. Failure stops
   dispatch and further writes until recovery; a failed or cancelled submission remains uncertain.
1. Advance live D and the in-memory descriptor only after those operations succeed. Dispatch is now
   allowed. Receipts still wait for complete installation and contiguous visibility F.

The trailer shares the same file flush as the records. An ordinary append on an existing segment
requires one file flush and no rename, directory flush or manifest publication. A group is a local
durability unit, not a semantic transaction merge.

Checkpoints and cursor/retention/layout changes continue to use G.3. Their manifests incorporate the
latest live D and exact segment bounds. Files already flushed by WAL append need no repeated flush
for unchanged prefixes. Page roots are published only after their referenced contents are durable.
Retention and reclamation preserve every log record needed by C or any durable claim. Administrative
canonical records use WAL publication, then their existing checkpoint barrier.

### I.4 Recovery and tail classification

Under the exclusive directory lease:

1. Validate CURRENT, the selected manifest, GENESIS, checkpoint pages and every selected log prefix.
   Missing or corrupt bytes within selected prefixes are always errors.
1. Extend the last selected segment from its recorded boundary using complete groups.
1. Enumerate potential successor log files at or above the selected next_segment_id in increasing
   file-ID order. Validate their database/file identities and require version 1. Follow only a
   contiguous sequence/digest chain from the preceding accepted endpoint. Reject complete forks,
   gaps, foreign identities, unsupported versions and malformed headers. Never resynchronise at a
   later magic string after invalid data. Discovery retains at most 1,048,576 candidate IDs.
1. Discard a physically short terminal group: fewer than 112 remaining header bytes, or a valid
   header whose declared group extends beyond physical EOF. A physically short segment header, or a
   valid unlisted segment containing no complete group, is orphan output. A later complete successor
   after an incomplete accepted segment is an error. Complete-sized groups with invalid CRCs,
   hashes, framing or envelopes fail closed.
1. Only after validating the candidate chain, truncate incomplete accepted tails, flush recovered
   suffix files and flush newly discovered filenames. Readable bytes left in an OS cache after a
   process crash do not by themselves establish durability-before-execution.
1. Discard post-checkpoint materialization and replay every canonical record in `(C, D]`, including
   aborts and records whose receipts may already have succeeded. Publish the resulting checkpoint
   before serving public requests.

A complete valid group may be recovered although its writer never observed a successful flush or
sent a receipt. That is an uncertain submission. These rules do not claim that checksums prove a
past flush occurred. In particular, loss or physical truncation of an uncheckpointed suffix caused
by later damage is indistinguishable from an interrupted append. The selected manifest's existing
byte bounds remain strict; complete malformed suffix groups are never silently rolled back. A
full-length torn append that fails those checks is an explicit recovery error, not an automatically
discarded tail. This conservative distinction is part of the version-1 recovery contract.

### I.5 Backup and logical consumers

A backup captures the live durable descriptor set with the selected checkpoint roots and page
prefix. Verify that source CURRENT and its selected manifest still match the owner's selected
metadata, then synthesize a destination manifest and matching CURRENT for the frozen live D. Copy
exactly those pinned file prefixes and use G.3 ordering at the destination. The copied manifest can
therefore differ from the source's older selected manifest without observing tentative state or a
moving log tail. Attachment and namespace renewal follow G.4-G.5.

Logical feeds and imports transport the unchanged E.2 bytes and canonical outcomes, omitting local
group headers and trailers. Source and replica group boundaries may differ. Import still validates
the complete batch by isolated reference execution before the first local durable append, and can
recover an already verified shorter prefix after interruption. Maintenance validates retained
segments and reclaims whole segments only after replacement metadata is durable.

### I.6 Verification obligations

Test one-flush append counts, immutable CURRENT across WAL-only commits, exact group boundaries,
checksummed malformed frames, every physically short group prefix, segment forks and missing
predecessors, process exit and partial writes at append boundaries, recovery flushing before replay,
checkpoint sequences inside groups, unsupported versions, orphan filenames, and backups captured
while live D exceeds the selected manifest. Existing sequential-equivalence, retention, revocation
and replica outcome checks remain required. Fault injection and process exit are not simulations of
hardware power loss or arbitrary filesystem write reordering.
