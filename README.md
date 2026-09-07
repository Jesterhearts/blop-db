# blop-db

`tx!` compiles a small deterministic transaction program to the ISA 1 bytecode specified in
[`DESIGN.md`](DESIGN.md), appendices A, B and C. Parsing, type checking, register allocation and
branch resolution happen during Rust compilation. Runtime binding snapshots the captures and
resolves table IDs. It does not evaluate the VM program.

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

This repository does not yet implement the VM, database execution, access manifests or resource
claims. These two byte vectors are not the complete logged Transaction body. The future database
must independently validate bytecode, actual catalogue schemas, scopes and resource claims before
sequencing. In particular, the macro cannot check whether a runtime table ID names a live table.

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

## Development

Run `cargo test --workspace`, `cargo +nightly fmt --all -- --check` and
`cargo clippy --workspace --all-targets -- -D warnings`. Tests compare canonical byte encodings,
check all 48 ISA 1 opcodes and verify emitted control-flow graphs and definite register
initialization.
