//! Evaluate instructions in order and charge their logical resource use.

use std::collections::BTreeSet;
use std::ops::Bound;

use super::Abort;
use super::AbortReason;
use super::Effect;
use super::Error;
use super::Outcome;
use super::Result;
use super::Table;
use super::Type;
use super::Value;
use super::database;
use super::database::Overlay;
use super::operations;
use super::program::Instruction;
use super::program::Program;
use super::value::decode_value;
use crate::storage;
use crate::storage::LimitPolicy;
use crate::storage::View;
use crate::storage::encoding::Schema;

enum Failure {
    Abort {
        reason: AbortReason,
        user_code: u32,
        detail: u64,
    },
    Error(Error),
}

impl From<AbortReason> for Failure {
    fn from(reason: AbortReason) -> Self {
        Self::Abort {
            reason,
            user_code: 0,
            detail: 0,
        }
    }
}

impl From<Error> for Failure {
    fn from(error: Error) -> Self {
        Self::Error(error)
    }
}

impl From<storage::Error> for Failure {
    fn from(error: storage::Error) -> Self {
        Self::Error(error.into())
    }
}

type EvalResult<T> = std::result::Result<T, Failure>;
type Address = (u64, Vec<u8>);

#[derive(Default)]
struct Usage {
    instructions: u64,
    point_accesses: u64,
    addresses: BTreeSet<Address>,
    register_bytes: u64,
    range_rows: u64,
    range_bytes: u64,
    overlay_bytes: u64,
}

fn check(
    claims: &LimitPolicy,
    amounts: &[(usize, u64)],
) -> EvalResult<()> {
    for &(id, amount) in amounts {
        if amount > claims.values()[id - 1] {
            return Err(Failure::Abort {
                reason: AbortReason::ResourceLimit,
                user_code: 0,
                detail: id as u64,
            });
        }
    }
    Ok(())
}

fn bound(
    ty: &Type,
    value: &Value,
) -> EvalResult<()> {
    if !ty.accepts(value) {
        return Err(AbortReason::BoundExceeded.into());
    }
    Ok(())
}

fn register(
    registers: &[Option<Value>],
    index: usize,
) -> &Value {
    registers[index]
        .as_ref()
        .expect("reader verified definite initialization")
}

fn assign(
    registers: &mut [Option<Value>],
    types: &[Type],
    usage: &mut Usage,
    claims: &LimitPolicy,
    dst: usize,
    value: Value,
) -> EvalResult<()> {
    bound(&types[dst], &value)?;
    let size = value.encoded_len() as u64;
    let bytes = usage.register_bytes
        - registers[dst]
            .as_ref()
            .map_or(0, |v| v.encoded_len() as u64)
        + size;
    check(claims, &[(11, size), (12, bytes)])?;
    registers[dst] = Some(value);
    usage.register_bytes = bytes;
    Ok(())
}

fn canonical_key(
    table: &Table,
    value: &Value,
    claims: &LimitPolicy,
) -> EvalResult<Vec<u8>> {
    bound(&table.key, value)?;
    let schema = Schema::decode(&table.key.descriptor())?;
    let key = storage::encoding::encode_key(&schema, &value.encode())?;
    check(claims, &[(10, key.len() as u64)])?;
    Ok(key)
}

fn point(
    usage: &mut Usage,
    claims: &LimitPolicy,
    address: &Address,
) -> EvalResult<()> {
    let accesses = usage.point_accesses + 1;
    let distinct = usage.addresses.len() as u64 + u64::from(!usage.addresses.contains(address));
    check(claims, &[(8, accesses), (9, distinct)])?;
    usage.point_accesses = accesses;
    usage.addresses.insert(address.clone());
    Ok(())
}

fn loaded(
    ty: &Type,
    bytes: &[u8],
    claims: &LimitPolicy,
) -> EvalResult<Value> {
    let value = decode_value(ty, bytes).map_err(|error| match error {
        Error::Invalid(reason) => Error::Storage(storage::Error::Corrupt(reason)),
        error => error,
    })?;
    check(claims, &[(11, bytes.len() as u64)])?;
    Ok(value)
}

fn write(
    overlay: &mut Overlay,
    usage: &mut Usage,
    claims: &LimitPolicy,
    table: &Table,
    key: Vec<u8>,
    value: Option<&Value>,
) -> EvalResult<()> {
    if let Some(value) = value {
        bound(&table.value, value)?;
    }
    let size = value.map_or(0, |v| v.encoded_len() as u64);
    let address = (table.id, key);
    let old = overlay.get(&address);
    let writes = overlay.len() as u64 + u64::from(old.is_none());
    let old_size = old.map_or(0, |v| {
        9 + address.1.len() as u64 + v.as_ref().map_or(0, |v| v.len() as u64)
    });
    let bytes = usage.overlay_bytes - old_size + 9 + address.1.len() as u64 + size;
    check(claims, &[(11, size), (15, writes), (16, bytes)])?;
    overlay.insert(address, value.map(Value::encode));
    usage.overlay_bytes = bytes;
    Ok(())
}

fn scan(
    view: &View,
    prior: u64,
    overlay: &Overlay,
    usage: &mut Usage,
    claims: &LimitPolicy,
    table: &Table,
    lower: Option<&Value>,
    upper: Option<&Value>,
    flags: u8,
    row_limit: u32,
    byte_limit: u64,
) -> EvalResult<Value> {
    let lower = lower
        .map(|value| canonical_key(table, value, claims))
        .transpose()?;
    let upper = upper
        .map(|value| canonical_key(table, value, claims))
        .transpose()?;
    if row_limit == 0
        || lower
            .as_ref()
            .zip(upper.as_ref())
            .is_some_and(|(lo, hi)| lo > hi || (lo == hi && flags != 3))
    {
        return Ok(Value::Rows(Vec::new()));
    }
    let lo = lower.as_deref().map_or(Bound::Unbounded, |key| {
        if flags & 1 != 0 {
            Bound::Included(key)
        } else {
            Bound::Excluded(key)
        }
    });
    let hi = upper.as_deref().map_or(Bound::Unbounded, |key| {
        if flags & 2 != 0 {
            Bound::Included(key)
        } else {
            Bound::Excluded(key)
        }
    });
    let key_schema = Schema::decode(&table.key.descriptor())?;
    let entries = database::scan(view, table.id, prior, lo, hi, overlay)?;
    let mut rows = Vec::new();
    let mut local_bytes = 0;
    for entry in entries.take(row_limit as usize) {
        let (key, bytes) = entry?;
        let key_value =
            storage::encoding::decode_key(&key_schema, &key).map_err(|error| match error {
                storage::Error::InvalidInput(reason) => storage::Error::Corrupt(reason),
                error => error,
            })?;
        let key_value = decode_value(&table.key, &key_value).map_err(|error| match error {
            Error::Invalid(reason) => Error::Storage(storage::Error::Corrupt(reason)),
            error => error,
        })?;
        check(claims, &[(10, key.len() as u64)])?;
        let value = loaded(&table.value, &bytes, claims)?;
        let size = (key.len() + bytes.len()) as u64;
        let address = (table.id, key);
        let distinct =
            usage.addresses.len() as u64 + u64::from(!usage.addresses.contains(&address));
        let range_rows = usage.range_rows + 1;
        let range_bytes = usage.range_bytes + size;
        check(
            claims,
            &[(9, distinct), (13, range_rows), (14, range_bytes)],
        )?;
        if local_bytes + size > byte_limit {
            return Err(Failure::Abort {
                reason: AbortReason::ResourceLimit,
                user_code: 0,
                detail: 14,
            });
        }
        usage.addresses.insert(address);
        usage.range_rows = range_rows;
        usage.range_bytes = range_bytes;
        local_bytes += size;
        rows.push((key_value, value));
    }
    Ok(Value::Rows(rows))
}

enum Step {
    Next(usize),
    Return(Value),
}

fn step(
    view: &View,
    prior: u64,
    program: &Program,
    claims: &LimitPolicy,
    pc: usize,
    registers: &mut [Option<Value>],
    overlay: &mut Overlay,
    usage: &mut Usage,
) -> EvalResult<Step> {
    usage.instructions += 1;
    check(claims, &[(2, usage.instructions)])?;
    let (dst, value) = match &program.instructions[pc] {
        Instruction::Const { dst, constant } => (*dst, program.constants[*constant].clone()),
        Instruction::Arg { dst, argument } => (*dst, program.arguments[*argument].clone()),
        Instruction::Move { dst, src } => (*dst, register(registers, *src).clone()),
        Instruction::Binary {
            op,
            dst,
            left,
            right,
        } => (
            *dst,
            operations::binary(
                *op,
                register(registers, *left),
                register(registers, *right),
                &program.register_types[*dst],
            )?,
        ),
        Instruction::Unary { op, dst, src } => {
            (*dst, operations::unary(*op, register(registers, *src))?)
        }
        Instruction::Slice {
            dst,
            src,
            start,
            length,
        } => (
            *dst,
            operations::slice(
                register(registers, *src),
                register(registers, *start),
                register(registers, *length),
            )?,
        ),
        Instruction::Tuple { dst, fields } => {
            let Type::Tuple(types) = &program.register_types[*dst] else {
                unreachable!()
            };
            for (field, ty) in fields.iter().zip(types) {
                bound(ty, register(registers, *field))?;
            }
            (
                *dst,
                Value::Tuple(
                    fields
                        .iter()
                        .map(|field| register(registers, *field).clone())
                        .collect(),
                ),
            )
        }
        Instruction::Field { dst, src, field } => {
            let Value::Tuple(fields) = register(registers, *src) else {
                unreachable!()
            };
            (*dst, fields[*field].clone())
        }
        Instruction::Load { dst, table, key } | Instruction::Exists { dst, table, key } => {
            let table = &program.tables[*table];
            let key = canonical_key(table, register(registers, *key), claims)?;
            point(usage, claims, &(table.id, key.clone()))?;
            let bytes = database::get(view, table.id, &key, prior, overlay)?;
            let value = if matches!(program.instructions[pc], Instruction::Exists { .. }) {
                Value::Boolean(bytes.is_some())
            } else {
                loaded(&table.value, &bytes.ok_or(AbortReason::MissingKey)?, claims)?
            };
            (*dst, value)
        }
        Instruction::Insert { table, key, value } | Instruction::Store { table, key, value } => {
            let table = &program.tables[*table];
            let key = canonical_key(table, register(registers, *key), claims)?;
            point(usage, claims, &(table.id, key.clone()))?;
            if matches!(program.instructions[pc], Instruction::Insert { .. })
                && database::get(view, table.id, &key, prior, overlay)?.is_some()
            {
                return Err(AbortReason::KeyExists.into());
            }
            write(
                overlay,
                usage,
                claims,
                table,
                key,
                Some(register(registers, *value)),
            )?;
            return Ok(Step::Next(pc + 1));
        }
        Instruction::Delete { table, key } => {
            let table = &program.tables[*table];
            let key = canonical_key(table, register(registers, *key), claims)?;
            point(usage, claims, &(table.id, key.clone()))?;
            write(overlay, usage, claims, table, key, None)?;
            return Ok(Step::Next(pc + 1));
        }
        Instruction::Scan {
            dst,
            table,
            lower,
            upper,
            flags,
            row_limit,
            byte_limit,
        } => {
            let value = scan(
                view,
                prior,
                overlay,
                usage,
                claims,
                &program.tables[*table],
                lower.map(|r| register(registers, r)),
                upper.map(|r| register(registers, r)),
                *flags,
                *row_limit,
                *byte_limit,
            )?;
            (*dst, value)
        }
        Instruction::Jump { target } => return Ok(Step::Next(*target)),
        Instruction::JumpIfFalse { condition, target } => {
            return Ok(Step::Next(
                if register(registers, *condition) == &Value::Boolean(false) {
                    *target
                } else {
                    pc + 1
                },
            ));
        }
        Instruction::Require {
            condition,
            user_code,
        } => {
            if register(registers, *condition) == &Value::Boolean(false) {
                return Err(Failure::Abort {
                    reason: AbortReason::RequireFailed,
                    user_code: *user_code,
                    detail: 0,
                });
            }
            return Ok(Step::Next(pc + 1));
        }
        Instruction::Abort { user_code } => {
            return Err(Failure::Abort {
                reason: AbortReason::ExplicitAbort,
                user_code: *user_code,
                detail: 0,
            });
        }
        Instruction::Return { src } => {
            let value = register(registers, *src);
            bound(&program.result_type, value)?;
            check(claims, &[(17, value.encoded_len() as u64)])?;
            return Ok(Step::Return(value.clone()));
        }
    };
    assign(
        registers,
        &program.register_types,
        usage,
        claims,
        dst,
        value,
    )?;
    Ok(Step::Next(pc + 1))
}

pub(super) fn run(
    view: &View,
    prior: u64,
    program: &Program,
    claims: &LimitPolicy,
) -> Result<Outcome> {
    let mut registers = vec![None; program.register_types.len()];
    let mut overlay = Overlay::new();
    let mut usage = Usage::default();
    let mut pc = 0;
    loop {
        match step(
            view,
            prior,
            program,
            claims,
            pc,
            &mut registers,
            &mut overlay,
            &mut usage,
        ) {
            Ok(Step::Next(next)) => pc = next,
            Ok(Step::Return(value)) => {
                let effects = overlay
                    .into_iter()
                    .map(|((table, key), value)| match value {
                        Some(value) => Effect::Put { table, key, value },
                        None => Effect::Delete { table, key },
                    })
                    .collect();
                return Ok(Outcome::Success {
                    result_type: program.result_type.clone(),
                    value,
                    effects,
                });
            }
            Err(Failure::Abort {
                reason,
                user_code,
                detail,
            }) => {
                return Ok(Outcome::Aborted(Abort {
                    reason,
                    instruction: pc as u32,
                    user_code,
                    detail,
                }));
            }
            Err(Failure::Error(error)) => return Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Transaction;
    use crate::storage::Store;
    use crate::storage::TreeId;
    use crate::storage::mvcc;
    use crate::tx;
    use crate::vm;
    use crate::vm::CatalogueOperation;

    const CEILINGS: [u64; 17] = [
        16 * 1024 * 1024,
        65_535,
        65_535,
        65_535,
        16 * 1024 * 1024,
        65_535,
        16_384,
        65_535,
        65_535,
        1_024,
        16 * 1024 * 1024,
        64 * 1024 * 1024,
        65_535,
        64 * 1024 * 1024,
        65_535,
        64 * 1024 * 1024,
        16 * 1024 * 1024,
    ];

    fn claims(limits: &[(usize, u64)]) -> LimitPolicy {
        let mut values = CEILINGS;
        for &(id, limit) in limits {
            values[id - 1] = limit;
        }
        LimitPolicy::new(values).unwrap()
    }

    fn create_store() -> (tempfile::TempDir, Store) {
        let directory = tempfile::tempdir().unwrap();
        let store = storage::create(
            directory.path().join("database"),
            storage::Genesis {
                database_id: [1; 16],
                initial_policy: claims(&[]),
            },
            [2; 16],
        )
        .unwrap();
        (directory, store)
    }

    fn table_store(
        key: Type,
        value: Type,
    ) -> (tempfile::TempDir, Store) {
        let (directory, mut store) = create_store();
        let outcome = vm::execute_catalogue(
            &mut store,
            1,
            [1; 32],
            &CatalogueOperation::Create {
                name: "data".into(),
                key,
                value,
            },
        )
        .unwrap();
        assert!(matches!(
            outcome,
            Outcome::Success {
                value: Value::U64(1),
                ..
            }
        ));
        (directory, store)
    }

    fn execute(
        store: &mut Store,
        sequence: u64,
        transaction: &Transaction,
    ) -> Outcome {
        vm::execute(
            store,
            sequence,
            [sequence as u8; 32],
            transaction,
            &claims(&[]),
        )
        .unwrap()
    }

    fn interpret(
        view: &View,
        sequence: u64,
        transaction: &Transaction,
        policy: &LimitPolicy,
    ) -> Outcome {
        vm::interpret(
            view,
            sequence,
            transaction.program_bytes(),
            transaction.argument_bytes(),
            policy,
        )
        .unwrap()
    }

    fn assert_output(
        outcome: Outcome,
        expected: Value,
    ) -> Vec<Effect> {
        let Outcome::Success {
            result_type,
            value,
            effects,
        } = outcome
        else {
            panic!("expected success, got {outcome:?}");
        };
        assert!(
            result_type.accepts(&expected),
            "unexpected result descriptor: {result_type:?}"
        );
        assert_eq!(value, expected);
        effects
    }

    fn tree_entries(
        view: &View,
        tree: TreeId,
    ) -> Vec<storage::Entry> {
        storage::scan(view, tree, Bound::Unbounded, Bound::Unbounded)
            .unwrap()
            .collect::<storage::Result<_>>()
            .unwrap()
    }

    fn assert_abort(
        store: &mut Store,
        sequence: u64,
        transaction: &Transaction,
        policy: &LimitPolicy,
        reason: AbortReason,
        detail: u64,
    ) -> Abort {
        let before = storage::view(store);
        let outcome =
            vm::execute(store, sequence, [sequence as u8; 32], transaction, policy).unwrap();
        let Outcome::Aborted(abort) = outcome else {
            panic!("expected {reason:?}, got {outcome:?}");
        };
        assert_eq!(abort.reason, reason);
        assert_eq!(abort.detail, detail);
        if !matches!(
            reason,
            AbortReason::RequireFailed | AbortReason::ExplicitAbort
        ) {
            assert_eq!(abort.user_code, 0);
        }
        let count = u32::from_le_bytes(transaction.program_bytes()[24..28].try_into().unwrap());
        assert!(abort.instruction < count);
        let after = storage::view(store);
        for tree in [TreeId::State, TreeId::Catalogue, TreeId::Policy] {
            assert_eq!(tree_entries(&after, tree), tree_entries(&before, tree));
        }
        let mut expected = vec![1, 0, 1, 1];
        expected.extend(sequence.to_le_bytes());
        expected.extend([sequence as u8; 32]);
        expected.extend((reason as u16).to_le_bytes());
        expected.extend([0; 2]);
        expected.extend(abort.instruction.to_le_bytes());
        expected.extend(abort.user_code.to_le_bytes());
        expected.extend([0; 4]);
        expected.extend(detail.to_le_bytes());
        expected.extend([0; 8]);
        assert_eq!(
            storage::get(&after, TreeId::Outcomes, &sequence.to_be_bytes()).unwrap(),
            Some(expected)
        );
        assert_eq!(
            storage::get(&before, TreeId::Outcomes, &sequence.to_be_bytes()).unwrap(),
            None
        );
        abort
    }

    fn numbers() -> (tempfile::TempDir, Store) {
        let (directory, mut store) = table_store(Type::U64, Type::I64);
        let transaction = tx! {
            tables { data: u64 => i64 = 1 }
            data[3] = 30; data[1] = 10; data[4] = 40; data[2] = 20;
        }
        .unwrap();
        assert_eq!(
            assert_output(execute(&mut store, 2, &transaction), Value::Unit).len(),
            4
        );
        (directory, store)
    }

    fn rows(values: &[(u64, i64)]) -> Value {
        Value::Rows(
            values
                .iter()
                .map(|&(key, value)| (Value::U64(key), Value::I64(value)))
                .collect(),
        )
    }

    #[test]
    fn signed_arithmetic_bitwise_and_conversions_return_values() {
        let (_directory, mut store) = create_store();
        let transaction = tx! {
            captures { x: i64 = -7_i64, y: i64 = 3_i64, n: u64 = 1_u64 }
            return (copy(x), x + y, x - y, x * y, x / y, x % y, -x,
                x & y, x | y, x ^ y, !x, x << n, x >> n,
                to_i64_checked(n), to_u64_checked(y));
        }
        .unwrap();
        let mut expected: Vec<_> = [-7, -4, -10, -21, -2, -1, 7, 1, -5, -6, 6, -14, -4, 1]
            .into_iter()
            .map(Value::I64)
            .collect();
        expected.push(Value::U64(3));
        assert!(
            assert_output(execute(&mut store, 1, &transaction), Value::Tuple(expected)).is_empty()
        );
        assert!(tree_entries(&storage::view(&store), TreeId::State).is_empty());
    }

    #[test]
    fn unsigned_arithmetic_and_wrapping_shifts_return_values() {
        let (_directory, mut store) = create_store();
        let transaction = tx! {
            captures { x: u64 = 13_u64, y: u64 = 3_u64, maximum: u64 = u64::MAX }
            return (x + y, x - y, x * y, x / y, x % y,
                x & y, x | y, x ^ y, !x, x << y, x >> y,
                maximum << 1_u64, maximum >> 63_u64, maximum << 0_u64);
        }
        .unwrap();
        let expected = [
            16,
            10,
            39,
            4,
            1,
            1,
            15,
            14,
            !13_u64,
            104,
            1,
            u64::MAX - 1,
            1,
            u64::MAX,
        ]
        .into_iter()
        .map(Value::U64)
        .collect();
        assert!(
            assert_output(execute(&mut store, 1, &transaction), Value::Tuple(expected)).is_empty()
        );
    }

    #[test]
    fn comparisons_booleans_and_nested_tuples_preserve_logical_order() {
        let (_directory, mut store) = create_store();
        let transaction = tx! {
            captures { a: bool = true, b: bool = false }
            let pair = ("a\0", (-1_i64, 255_u64));
            return (2_i64 == 2_i64, -1_i64 < 0_i64, 2_u64 <= 2_u64,
                256_u64 > 255_u64, -1_i64 >= -1_i64, b < a,
                b"\0" < b"\0\0", "e\u{301}" < "\u{e9}",
                pair < ("a\0", (0_i64, 0_u64)),
                bool_and(a, b), bool_or(a, b), bool_xor(a, b), !a,
                pair.0, field(pair.1, 0), (), tuple());
        }
        .unwrap();
        let mut expected: Vec<_> = [
            true, true, true, true, true, true, true, true, true, false, true, true, false,
        ]
        .into_iter()
        .map(Value::Boolean)
        .collect();
        expected.extend([
            Value::String("a\0".into()),
            Value::I64(-1),
            Value::Unit,
            Value::Tuple(vec![]),
        ]);
        assert!(
            assert_output(execute(&mut store, 1, &transaction), Value::Tuple(expected)).is_empty()
        );
    }

    #[test]
    fn boolean_truth_tables_and_comparisons_distinguish_false_equal_and_true() {
        let (_directory, store) = create_store();
        let view = storage::view(&store);
        for (a, b) in [(false, false), (false, true), (true, false), (true, true)] {
            let transaction = tx! {
                captures { a: bool = a, b: bool = b }
                return (bool_and(a, b), bool_or(a, b), bool_xor(a, b), !a,
                    a == b, a < b, a <= b, a > b, a >= b);
            }
            .unwrap();
            let expected = [
                a & b,
                a | b,
                a ^ b,
                !a,
                a == b,
                !a & b,
                a <= b,
                a & !b,
                a >= b,
            ]
            .into_iter()
            .map(Value::Boolean)
            .collect();
            assert!(
                assert_output(
                    interpret(&view, 1, &transaction, &claims(&[])),
                    Value::Tuple(expected)
                )
                .is_empty()
            );
        }
        for (a, b) in [(i64::MIN, i64::MAX), (0, 0), (256, 255), (-1, -2)] {
            let transaction = tx! {
                captures { a: i64 = a, b: i64 = b }
                return (a == b, a < b, a <= b, a > b, a >= b);
            }
            .unwrap();
            let expected = [a == b, a < b, a <= b, a > b, a >= b]
                .into_iter()
                .map(Value::Boolean)
                .collect();
            assert!(
                assert_output(
                    interpret(&view, 1, &transaction, &claims(&[])),
                    Value::Tuple(expected)
                )
                .is_empty()
            );
        }
    }

    #[test]
    fn byte_string_slice_utf8_and_hash_instructions_return_exact_bytes() {
        let (_directory, mut store) = create_store();
        let transaction = tx! {
            let raw = b"a\0\xff";
            let text = "\u{e9}\0";
            return (byte_len(raw), byte_len(text), concat(raw, b"z"), concat(text, "!"),
                slice_bytes(raw, 1, 2), slice_bytes(raw, 3, 0), utf8_bytes(text),
                parse_utf8(b"\xc3\xa9\0"), sha256(b"abc"));
        }
        .unwrap();
        let expected = Value::Tuple(vec![
            Value::U64(3),
            Value::U64(3),
            Value::Bytes(b"a\0\xffz".to_vec()),
            Value::String("\u{e9}\0!".into()),
            Value::Bytes(vec![0, 0xff]),
            Value::Bytes(vec![]),
            Value::Bytes(vec![0xc3, 0xa9, 0]),
            Value::String("\u{e9}\0".into()),
            Value::Bytes(vec![
                0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae,
                0x22, 0x23, 0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61,
                0xf2, 0x00, 0x15, 0xad,
            ]),
        ]);
        assert!(assert_output(execute(&mut store, 1, &transaction), expected).is_empty());
    }

    #[test]
    fn intrinsic_failures_are_deterministic_aborts_not_rejections() {
        let (_directory, mut store) = create_store();
        let cases = [
            (
                tx! { return 9223372036854775807_i64 + 1_i64; }.unwrap(),
                AbortReason::IntegerOverflow,
            ),
            (
                tx! { return 0_u64 - 1_u64; }.unwrap(),
                AbortReason::IntegerOverflow,
            ),
            (
                tx! { return 18446744073709551615_u64 * 2_u64; }.unwrap(),
                AbortReason::IntegerOverflow,
            ),
            (
                tx! { return -9223372036854775808_i64 / -1_i64; }.unwrap(),
                AbortReason::IntegerOverflow,
            ),
            (
                tx! { return -9223372036854775808_i64 % -1_i64; }.unwrap(),
                AbortReason::IntegerOverflow,
            ),
            (
                tx! { let x = -9223372036854775808_i64; return -x; }.unwrap(),
                AbortReason::IntegerOverflow,
            ),
            (
                tx! { return to_i64_checked(18446744073709551615_u64); }.unwrap(),
                AbortReason::IntegerOverflow,
            ),
            (
                tx! { return to_u64_checked(-1_i64); }.unwrap(),
                AbortReason::IntegerOverflow,
            ),
            (
                tx! { return 1_i64 / 0_i64; }.unwrap(),
                AbortReason::DivisionByZero,
            ),
            (
                tx! { return 1_u64 % 0_u64; }.unwrap(),
                AbortReason::DivisionByZero,
            ),
            (
                tx! { return 1_u64 << 64_u64; }.unwrap(),
                AbortReason::InvalidShift,
            ),
            (
                tx! { return -1_i64 >> 18446744073709551615_u64; }.unwrap(),
                AbortReason::InvalidShift,
            ),
            (
                tx! { return slice_bytes(b"a", 2, 0); }.unwrap(),
                AbortReason::IndexOutOfBounds,
            ),
            (
                tx! { return slice_bytes(b"a", 1, 18446744073709551615_u64); }.unwrap(),
                AbortReason::IndexOutOfBounds,
            ),
            (
                tx! { return parse_utf8(b"\xc0\xaf"); }.unwrap(),
                AbortReason::InvalidUtf8,
            ),
        ];
        for (index, (transaction, reason)) in cases.iter().enumerate() {
            assert_abort(
                &mut store,
                index as u64 + 1,
                transaction,
                &claims(&[]),
                *reason,
                0,
            );
        }
    }

    #[test]
    fn branches_short_circuit_missing_loads_and_preserve_mutable_values() {
        let (_directory, store) = table_store(Type::U64, Type::I64);
        let view = storage::view(&store);
        for gate in [false, true] {
            let transaction = tx! {
                captures { gate: bool = gate }
                tables { data: u64 => i64 = 1 }
                let mut value = 1_i64;
                if gate { value = 7; } else { value = 9; }
                require(true, 99);
                return (value, false && data[999] > 0, true || data[999] > 0);
            }
            .unwrap();
            assert!(
                assert_output(
                    interpret(&view, 2, &transaction, &claims(&[(8, 0), (9, 0)])),
                    Value::Tuple(vec![
                        Value::I64(if gate { 7 } else { 9 }),
                        Value::Boolean(false),
                        Value::Boolean(true)
                    ])
                )
                .is_empty()
            );
            let transaction = tx! {
                captures { gate: bool = gate }
                tables { data: u64 => i64 = 1 }
                if gate { return true && data[999] > 0; }
                else { return false || data[999] > 0; }
            }
            .unwrap();
            assert!(matches!(
                interpret(&view, 2, &transaction, &claims(&[])),
                Outcome::Aborted(Abort {
                    reason: AbortReason::MissingKey,
                    ..
                })
            ));
        }
    }

    #[test]
    fn serial_transfer_and_failed_require_preserve_exact_mvcc_balances() {
        let (_directory, mut store) = table_store(Type::U64, Type::I64);
        let seed = tx! { tables { accounts: u64 => i64 = 1 } accounts[1] = 100; accounts[2] = 20; }
            .unwrap();
        execute(&mut store, 2, &seed);
        let pinned = storage::view(&store);
        for (sequence, amount, expected) in [(3, 30_i64, (70, 50)), (4, 10, (60, 60))] {
            let transaction = tx! {
                captures { amount: i64 = amount }
                tables { accounts: u64 => i64 = 1 }
                accounts[1] -= amount;
                accounts[2] += amount;
                require(accounts[1] >= 0, 0x12345678);
                return (accounts[1], accounts[2]);
            }
            .unwrap();
            let effects = assert_output(
                execute(&mut store, sequence, &transaction),
                Value::Tuple(vec![Value::I64(expected.0), Value::I64(expected.1)]),
            );
            assert_eq!(
                effects,
                vec![
                    Effect::Put {
                        table: 1,
                        key: 1_u64.to_be_bytes().to_vec(),
                        value: expected.0.to_le_bytes().to_vec()
                    },
                    Effect::Put {
                        table: 1,
                        key: 2_u64.to_be_bytes().to_vec(),
                        value: expected.1.to_le_bytes().to_vec()
                    },
                ]
            );
        }
        let rejected = tx! {
            tables { accounts: u64 => i64 = 1 }
            accounts[1] -= 61;
            accounts[2] += 61;
            require(accounts[1] >= 0, 0x12345678);
            abort(9);
        }
        .unwrap();
        let abort = assert_abort(
            &mut store,
            5,
            &rejected,
            &claims(&[]),
            AbortReason::RequireFailed,
            0,
        );
        assert_eq!(abort.user_code, 0x12345678);
        let current = storage::view(&store);
        for (view, sequence, balances) in [
            (&pinned, 5, [100_i64, 20]),
            (&current, 2, [100, 20]),
            (&current, 3, [70, 50]),
            (&current, 5, [60, 60]),
        ] {
            for (key, balance) in [1_u64, 2].into_iter().zip(balances) {
                assert_eq!(
                    mvcc::get(view, 1, &key.to_be_bytes(), sequence).unwrap(),
                    Some(balance.to_le_bytes().to_vec())
                );
            }
        }
        let explicit =
            tx! { tables { accounts: u64 => i64 = 1 } accounts[1] = 0; abort(0x87654321); }
                .unwrap();
        let abort = assert_abort(
            &mut store,
            6,
            &explicit,
            &claims(&[]),
            AbortReason::ExplicitAbort,
            0,
        );
        assert_eq!(abort.user_code, 0x87654321);
    }

    #[test]
    fn tuple_and_computed_key_aliases_share_one_final_write() {
        let (_directory, mut store) =
            table_store(Type::Tuple(vec![Type::Bytes(8), Type::I64]), Type::I64);
        let transaction = tx! {
            captures { prefix: bytes<8> = b"a\0", number: i64 = -2_i64 }
            tables { data: (bytes<8>, i64) => i64 = 1 }
            let key = (prefix, number);
            let alias = copy(key);
            insert(data[key], 10);
            let first = data[alias];
            data[(concat(b"a", b"\0"), -1_i64 - 1_i64)] = 20;
            let second = data[key];
            delete(data[alias]);
            let absent = exists(data[key]);
            insert(data[key], 30);
            return (first, second, absent, exists(data[alias]), data[alias]);
        }
        .unwrap();
        let policy = claims(&[(9, 1), (15, 1)]);
        let outcome = vm::execute(&mut store, 2, [2; 32], &transaction, &policy).unwrap();
        let mut key = vec![b'a', 0, 0xff, 0, 0];
        key.extend([0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe]);
        assert_eq!(
            assert_output(
                outcome,
                Value::Tuple(vec![
                    Value::I64(10),
                    Value::I64(20),
                    Value::Boolean(false),
                    Value::Boolean(true),
                    Value::I64(30)
                ])
            ),
            vec![Effect::Put {
                table: 1,
                key: key.clone(),
                value: 30_i64.to_le_bytes().to_vec()
            }]
        );
        let view = storage::view(&store);
        assert_eq!(
            mvcc::get(&view, 1, &key, 2).unwrap(),
            Some(30_i64.to_le_bytes().to_vec())
        );
        assert_eq!(tree_entries(&view, TreeId::State).len(), 1);
    }

    #[test]
    fn absent_delete_and_unchanged_store_still_install_final_versions() {
        let (_directory, mut store) = numbers();
        let transaction = tx! {
            tables { data: u64 => i64 = 1 }
            let missing = exists(data[9]);
            delete(data[9]);
            data[1] = 10;
            store(data[8], 80);
            return (missing, exists(data[9]), data[8]);
        }
        .unwrap();
        assert_eq!(
            assert_output(
                execute(&mut store, 3, &transaction),
                Value::Tuple(vec![
                    Value::Boolean(false),
                    Value::Boolean(false),
                    Value::I64(80)
                ])
            ),
            vec![
                Effect::Put {
                    table: 1,
                    key: 1_u64.to_be_bytes().to_vec(),
                    value: 10_i64.to_le_bytes().to_vec()
                },
                Effect::Put {
                    table: 1,
                    key: 8_u64.to_be_bytes().to_vec(),
                    value: 80_i64.to_le_bytes().to_vec()
                },
                Effect::Delete {
                    table: 1,
                    key: 9_u64.to_be_bytes().to_vec()
                },
            ]
        );
        let view = storage::view(&store);
        for (key, expected) in [
            (1, mvcc::StateValue::Put(10_i64.to_le_bytes().to_vec())),
            (8, mvcc::StateValue::Put(80_i64.to_le_bytes().to_vec())),
            (9, mvcc::StateValue::Delete),
        ] {
            let address = mvcc::StateKey::new(1, (key as u64).to_be_bytes().to_vec(), 3)
                .unwrap()
                .encode();
            assert_eq!(
                storage::get(&view, TreeId::State, &address).unwrap(),
                Some(expected.encode().unwrap())
            );
        }
        for (sequence, transaction, reason) in [
            (
                4,
                tx! { tables { data: u64 => i64 = 1 } data[8] = 99; return data[9]; }.unwrap(),
                AbortReason::MissingKey,
            ),
            (
                5,
                tx! { tables { data: u64 => i64 = 1 } data[8] = 99; insert(data[1], 1); }.unwrap(),
                AbortReason::KeyExists,
            ),
            (
                6,
                tx! { tables { data: u64 => i64 = 1 } insert(data[9], 9); insert(data[9], 10); }
                    .unwrap(),
                AbortReason::KeyExists,
            ),
            (
                7,
                tx! { tables { data: u64 => i64 = 1 } delete(data[1]); return data[1]; }.unwrap(),
                AbortReason::MissingKey,
            ),
        ] {
            assert_abort(&mut store, sequence, &transaction, &claims(&[]), reason, 0);
        }
    }

    #[test]
    fn interpret_uses_log_prior_even_when_view_contains_future_versions() {
        let (_directory, mut store) = numbers();
        let pinned = storage::view(&store);
        let future =
            tx! { tables { data: u64 => i64 = 1 } data[1] = 99; delete(data[2]); data[5] = 50; }
                .unwrap();
        execute(&mut store, 3, &future);
        let read = tx! {
            tables { data: u64 => i64 = 1 }
            return (data[1], exists(data[2]), exists(data[5]));
        }
        .unwrap();
        let current = storage::view(&store);
        for (view, sequence, expected) in [
            (&current, 3, (10, true, false)),
            (&pinned, 4, (10, true, false)),
            (&current, 4, (99, false, true)),
        ] {
            assert!(
                assert_output(
                    interpret(view, sequence, &read, &claims(&[])),
                    Value::Tuple(vec![
                        Value::I64(expected.0),
                        Value::Boolean(expected.1),
                        Value::Boolean(expected.2)
                    ])
                )
                .is_empty()
            );
        }
        let scan = tx! { tables { data: u64 => i64 = 1 } return scan_bounded(data, unbounded, unbounded, 0, 8, 128); }.unwrap();
        assert!(
            assert_output(
                interpret(&current, 3, &scan, &claims(&[])),
                rows(&[(1, 10), (2, 20), (3, 30), (4, 40)])
            )
            .is_empty()
        );
        assert!(
            assert_output(
                interpret(&current, 4, &scan, &claims(&[])),
                rows(&[(1, 99), (3, 30), (4, 40), (5, 50)])
            )
            .is_empty()
        );
        assert_eq!(tree_entries(&current, TreeId::State).len(), 7);
        assert_eq!(tree_entries(&current, TreeId::Outcomes).len(), 3);
    }

    #[test]
    fn scans_apply_ascending_endpoint_inclusion_and_empty_interval_rules() {
        let (_directory, store) = numbers();
        let view = storage::view(&store);
        let cases = [
            (tx! { tables { data: u64 => i64 = 1 } return scan_bounded(data, 1, 4, 0, 8, 128); }.unwrap(), vec![(2, 20), (3, 30)]),
            (tx! { tables { data: u64 => i64 = 1 } return scan_bounded(data, 1, 4, 1, 8, 128); }.unwrap(), vec![(1, 10), (2, 20), (3, 30)]),
            (tx! { tables { data: u64 => i64 = 1 } return scan_bounded(data, 1, 4, 2, 8, 128); }.unwrap(), vec![(2, 20), (3, 30), (4, 40)]),
            (tx! { tables { data: u64 => i64 = 1 } return scan_bounded(data, 1, 4, 3, 8, 128); }.unwrap(), vec![(1, 10), (2, 20), (3, 30), (4, 40)]),
            (tx! { tables { data: u64 => i64 = 1 } return scan_bounded(data, unbounded, 2, 2, 8, 128); }.unwrap(), vec![(1, 10), (2, 20)]),
            (tx! { tables { data: u64 => i64 = 1 } return scan_bounded(data, 3, unbounded, 1, 8, 128); }.unwrap(), vec![(3, 30), (4, 40)]),
            (tx! { tables { data: u64 => i64 = 1 } return scan_bounded(data, 2, 2, 3, 8, 128); }.unwrap(), vec![(2, 20)]),
            (tx! { tables { data: u64 => i64 = 1 } return scan_bounded(data, 2, 2, 0, 8, 128); }.unwrap(), vec![]),
            (tx! { tables { data: u64 => i64 = 1 } return scan_bounded(data, 2, 2, 1, 8, 128); }.unwrap(), vec![]),
            (tx! { tables { data: u64 => i64 = 1 } return scan_bounded(data, 2, 2, 2, 8, 128); }.unwrap(), vec![]),
            (tx! { tables { data: u64 => i64 = 1 } return scan_bounded(data, 4, 1, 3, 8, 128); }.unwrap(), vec![]),
            (tx! { tables { data: u64 => i64 = 1 } return scan_bounded(data, unbounded, unbounded, 0, 0, 0); }.unwrap(), vec![]),
        ];
        for (transaction, expected) in cases {
            assert!(
                assert_output(
                    interpret(&view, 3, &transaction, &claims(&[(8, 0)])),
                    rows(&expected)
                )
                .is_empty()
            );
        }
    }

    #[test]
    fn scans_check_lower_then_upper_keys_before_any_empty_result() {
        let (_directory, mut store) = table_store(Type::Bytes(1), Type::Unit);
        let cases = [
            (tx! { tables { data: bytes<8> => () = 1 } return scan_bounded(data, b"ab", b"\0", 3, 0, 0); }.unwrap(), AbortReason::BoundExceeded, 0),
            (tx! { tables { data: bytes<8> => () = 1 } return scan_bounded(data, b"\0", b"ab", 3, 0, 0); }.unwrap(), AbortReason::ResourceLimit, 10),
            (tx! { tables { data: bytes<8> => () = 1 } return scan_bounded(data, b"a", b"ab", 3, 0, 0); }.unwrap(), AbortReason::BoundExceeded, 0),
            (tx! { tables { data: bytes<8> => () = 1 } return scan_bounded(data, b"a", b"\0", 3, 0, 0); }.unwrap(), AbortReason::ResourceLimit, 10),
            (tx! { tables { data: bytes<8> => () = 1 } return scan_bounded(data, b"z", b"ab", 3, 1, 0); }.unwrap(), AbortReason::BoundExceeded, 0),
        ];
        for (index, (transaction, reason, detail)) in cases.iter().enumerate() {
            assert_abort(
                &mut store,
                index as u64 + 2,
                transaction,
                &claims(&[(10, 3), (8, 0), (9, 0)]),
                *reason,
                *detail,
            );
        }
        let empty = tx! { tables { data: bytes<1> => () = 1 } return scan_bounded(data, b"z", b"a", 3, 1, 0); }.unwrap();
        assert!(
            assert_output(
                interpret(
                    &storage::view(&store),
                    7,
                    &empty,
                    &claims(&[(8, 0), (9, 0)])
                ),
                Value::Rows(vec![])
            )
            .is_empty()
        );
    }

    #[test]
    fn scans_merge_overlay_and_mvcc_before_logical_row_limits() {
        let (_directory, mut store) = numbers();
        let update =
            tx! { tables { data: u64 => i64 = 1 } data[1] = 11; delete(data[2]); data[4] = 44; }
                .unwrap();
        execute(&mut store, 3, &update);
        let transaction = tx! {
            tables { data: u64 => i64 = 1 }
            delete(data[1]); insert(data[2], 22); data[3] = 33; data[0] = 0;
            return scan_bounded(data, unbounded, unbounded, 0, 3, 48);
        }
        .unwrap();
        let policy = claims(&[(8, 4), (9, 4), (13, 3), (14, 48), (15, 4)]);
        let outcome = vm::execute(&mut store, 4, [4; 32], &transaction, &policy).unwrap();
        assert_eq!(
            assert_output(outcome, rows(&[(0, 0), (2, 22), (3, 33)])),
            vec![
                Effect::Put {
                    table: 1,
                    key: 0_u64.to_be_bytes().to_vec(),
                    value: 0_i64.to_le_bytes().to_vec()
                },
                Effect::Delete {
                    table: 1,
                    key: 1_u64.to_be_bytes().to_vec()
                },
                Effect::Put {
                    table: 1,
                    key: 2_u64.to_be_bytes().to_vec(),
                    value: 22_i64.to_le_bytes().to_vec()
                },
                Effect::Put {
                    table: 1,
                    key: 3_u64.to_be_bytes().to_vec(),
                    value: 33_i64.to_le_bytes().to_vec()
                },
            ]
        );
        let view = storage::view(&store);
        assert_eq!(mvcc::get(&view, 1, &1_u64.to_be_bytes(), 4).unwrap(), None);
        assert_eq!(
            mvcc::get(&view, 1, &4_u64.to_be_bytes(), 4).unwrap(),
            Some(44_i64.to_le_bytes().to_vec())
        );
        let persisted = tx! { tables { data: u64 => i64 = 1 } return scan_bounded(data, unbounded, unbounded, 0, 2, 32); }.unwrap();
        assert!(
            assert_output(
                interpret(&view, 5, &persisted, &claims(&[(9, 2), (13, 2), (14, 32)])),
                rows(&[(0, 0), (2, 22)])
            )
            .is_empty()
        );
    }

    #[test]
    fn scan_row_limit_does_not_load_an_oversized_later_value() {
        let (_directory, mut store) = table_store(Type::U64, Type::Bytes(32));
        let seed = tx! { tables { data: u64 => bytes<32> = 1 } data[1] = b"a"; data[2] = b"0123456789abcdef"; }.unwrap();
        execute(&mut store, 2, &seed);
        let first = tx! { tables { data: u64 => bytes<32> = 1 } return scan_bounded(data, unbounded, unbounded, 0, 1, 13); }.unwrap();
        let policy = claims(&[(11, 17), (13, 1), (14, 13)]);
        assert!(
            assert_output(
                interpret(&storage::view(&store), 3, &first, &policy),
                Value::Rows(vec![(Value::U64(1), Value::Bytes(b"a".to_vec()))])
            )
            .is_empty()
        );
        let both = tx! { tables { data: u64 => bytes<32> = 1 } return scan_bounded(data, unbounded, unbounded, 0, 2, 64); }.unwrap();
        assert_abort(
            &mut store,
            3,
            &both,
            &claims(&[(11, 17), (9, 1)]),
            AbortReason::ResourceLimit,
            11,
        );
    }

    #[test]
    fn scan_byte_limit_aborts_instead_of_returning_a_shorter_batch() {
        let (_directory, mut store) = numbers();
        let fits = tx! { tables { data: u64 => i64 = 1 } return scan_bounded(data, unbounded, unbounded, 0, 2, 32); }.unwrap();
        assert!(
            assert_output(
                interpret(&storage::view(&store), 3, &fits, &claims(&[])),
                rows(&[(1, 10), (2, 20)])
            )
            .is_empty()
        );
        let short = tx! {
            tables { data: u64 => i64 = 1 }
            data[9] = 90;
            return scan_bounded(data, unbounded, unbounded, 0, 2, 31);
        }
        .unwrap();
        assert_abort(
            &mut store,
            3,
            &short,
            &claims(&[]),
            AbortReason::ResourceLimit,
            14,
        );
        let zero = tx! { tables { data: u64 => i64 = 1 } return scan_bounded(data, unbounded, unbounded, 0, 1, 0); }.unwrap();
        assert_abort(
            &mut store,
            4,
            &zero,
            &claims(&[]),
            AbortReason::ResourceLimit,
            14,
        );
    }

    #[test]
    fn rows_length_key_value_and_out_of_bounds_indices_use_selected_rows() {
        let (_directory, mut store) = numbers();
        let transaction = tx! {
            tables { data: u64 => i64 = 1 }
            let rows = scan_bounded(data, 2, 4, 3, 3, 48);
            let empty = scan_bounded(data, unbounded, unbounded, 0, 0, 0);
            return (rows_len(rows), rows_key(rows, 0), rows_value(rows, 2), rows_len(empty));
        }
        .unwrap();
        assert!(
            assert_output(
                execute(&mut store, 3, &transaction),
                Value::Tuple(vec![
                    Value::U64(3),
                    Value::U64(2),
                    Value::I64(40),
                    Value::U64(0)
                ])
            )
            .is_empty()
        );
        for (index, transaction) in [
            tx! { tables { data: u64 => i64 = 1 } let r = scan_bounded(data, unbounded, unbounded, 0, 1, 16); return rows_key(r, 1); }.unwrap(),
            tx! { tables { data: u64 => i64 = 1 } let r = scan_bounded(data, unbounded, unbounded, 0, 1, 16); return rows_value(r, 18446744073709551615_u64); }.unwrap(),
            tx! { tables { data: u64 => i64 = 1 } let r = scan_bounded(data, unbounded, unbounded, 0, 0, 0); return rows_key(r, 0); }.unwrap(),
        ].iter().enumerate() {
            assert_abort(&mut store, index as u64 + 4, transaction, &claims(&[]), AbortReason::IndexOutOfBounds, 0);
        }
    }

    #[test]
    fn point_limits_count_every_operation_but_deduplicate_addresses() {
        let (_directory, mut store) = table_store(Type::U64, Type::I64);
        let transaction = tx! {
            tables { data: u64 => i64 = 1 }
            let key = 7_u64;
            let value = 70_i64;
            insert(data[key], value);
            data[key]; exists(data[key]); store(data[key], value);
            delete(data[key]); exists(data[key]);
        }
        .unwrap();
        assert_abort(
            &mut store,
            2,
            &transaction,
            &claims(&[(8, 5), (9, 1)]),
            AbortReason::ResourceLimit,
            8,
        );
        let outcome = vm::execute(
            &mut store,
            3,
            [3; 32],
            &transaction,
            &claims(&[(8, 6), (9, 1), (15, 1)]),
        )
        .unwrap();
        assert_eq!(
            assert_output(outcome, Value::Unit),
            vec![Effect::Delete {
                table: 1,
                key: 7_u64.to_be_bytes().to_vec()
            }]
        );
        let distinct = tx! { tables { data: u64 => i64 = 1 } data[1] = 1; data[2] = 2; }.unwrap();
        assert_abort(
            &mut store,
            4,
            &distinct,
            &claims(&[(8, 1), (9, 1), (15, 1)]),
            AbortReason::ResourceLimit,
            8,
        );
        assert_abort(
            &mut store,
            5,
            &distinct,
            &claims(&[(9, 1), (15, 1)]),
            AbortReason::ResourceLimit,
            9,
        );
    }

    #[test]
    fn point_key_bounds_precede_canonical_key_and_access_limits() {
        let (_directory, mut store) = table_store(Type::Bytes(1), Type::Unit);
        let canonical =
            tx! { tables { data: bytes<8> => () = 1 } data[b"a"] = (); data[b"\0"] = (); }.unwrap();
        assert_abort(
            &mut store,
            2,
            &canonical,
            &claims(&[(10, 3), (8, 1)]),
            AbortReason::ResourceLimit,
            10,
        );
        let descriptor =
            tx! { tables { data: bytes<8> => () = 1 } data[b"a"] = (); data[b"\0\0"] = (); }
                .unwrap();
        assert_abort(
            &mut store,
            3,
            &descriptor,
            &claims(&[(10, 3), (8, 1)]),
            AbortReason::BoundExceeded,
            0,
        );
        let outcome = vm::execute(&mut store, 4, [4; 32], &canonical, &claims(&[(10, 4)])).unwrap();
        assert_eq!(
            assert_output(outcome, Value::Unit),
            vec![
                Effect::Put {
                    table: 1,
                    key: vec![0, 0xff, 0, 0],
                    value: vec![]
                },
                Effect::Put {
                    table: 1,
                    key: vec![b'a', 0, 0],
                    value: vec![]
                },
            ]
        );
    }

    #[test]
    fn value_and_register_limits_check_actual_generated_sizes_in_id_order() {
        let (_directory, mut store) = create_store();
        let transaction = tx! { captures { a: bytes<4> = b"abcd" } return concat(a, a); }.unwrap();
        assert_abort(
            &mut store,
            1,
            &transaction,
            &claims(&[(11, 8), (12, 8)]),
            AbortReason::ResourceLimit,
            11,
        );
        assert_abort(
            &mut store,
            2,
            &transaction,
            &claims(&[(11, 12), (12, 19)]),
            AbortReason::ResourceLimit,
            12,
        );
        let outcome = vm::execute(
            &mut store,
            3,
            [3; 32],
            &transaction,
            &claims(&[(11, 12), (12, 20), (17, 12)]),
        )
        .unwrap();
        assert!(assert_output(outcome, Value::Bytes(b"abcdabcd".to_vec())).is_empty());
    }

    #[test]
    fn register_overwrites_replace_charge_and_dead_locals_remain_charged() {
        let (_directory, mut store) = create_store();
        let transaction = tx! {
            captures { large: bytes<8> = b"abcdefgh", small: bytes<8> = b"" }
            let mut value = large;
            value = small;
            copy(large);
            return value;
        }
        .unwrap();
        // Captures use 16 bytes. The mutable register shrinks from 12 to 4,
        // leaving room for the 12-byte copy at an exact peak of 32.
        assert_abort(
            &mut store,
            1,
            &transaction,
            &claims(&[(12, 31)]),
            AbortReason::ResourceLimit,
            12,
        );
        let outcome =
            vm::execute(&mut store, 2, [2; 32], &transaction, &claims(&[(12, 32)])).unwrap();
        assert!(assert_output(outcome, Value::Bytes(vec![])).is_empty());
        let scoped = tx! {
            captures { value: bytes<4> = b"abcd" }
            { let local = value; }
            return copy(value);
        }
        .unwrap();
        assert_abort(
            &mut store,
            3,
            &scoped,
            &claims(&[(12, 23)]),
            AbortReason::ResourceLimit,
            12,
        );
        let outcome = vm::execute(&mut store, 4, [4; 32], &scoped, &claims(&[(12, 24)])).unwrap();
        assert!(assert_output(outcome, Value::Bytes(b"abcd".to_vec())).is_empty());
    }

    #[test]
    fn exists_insert_and_delete_do_not_charge_the_old_value() {
        let (_directory, mut store) = table_store(Type::U64, Type::Bytes(32));
        let seed =
            tx! { tables { data: u64 => bytes<32> = 1 } data[1] = b"0123456789abcdef"; }.unwrap();
        execute(&mut store, 2, &seed);
        let policy = claims(&[(11, 8)]);
        let exists = tx! { tables { data: u64 => bytes<32> = 1 } return exists(data[1]); }.unwrap();
        assert!(
            assert_output(
                vm::execute(&mut store, 3, [3; 32], &exists, &policy).unwrap(),
                Value::Boolean(true)
            )
            .is_empty()
        );
        let load =
            tx! { tables { data: u64 => bytes<32> = 1 } data[2] = b"a"; return data[1]; }.unwrap();
        assert_abort(
            &mut store,
            4,
            &load,
            &policy,
            AbortReason::ResourceLimit,
            11,
        );
        let insert = tx! { tables { data: u64 => bytes<32> = 1 } insert(data[1], b"a"); }.unwrap();
        assert_abort(&mut store, 5, &insert, &policy, AbortReason::KeyExists, 0);
        let delete =
            tx! { tables { data: u64 => bytes<32> = 1 } delete(data[1]); return exists(data[1]); }
                .unwrap();
        assert_eq!(
            assert_output(
                vm::execute(&mut store, 6, [6; 32], &delete, &policy).unwrap(),
                Value::Boolean(false)
            ),
            vec![Effect::Delete {
                table: 1,
                key: 1_u64.to_be_bytes().to_vec()
            }]
        );
    }

    #[test]
    fn insert_checks_presence_before_new_value_table_bounds_and_write_limits() {
        let (_directory, mut store) = table_store(Type::U64, Type::Bytes(1));
        let seed = tx! { tables { data: u64 => bytes<8> = 1 } data[1] = b"a"; }.unwrap();
        execute(&mut store, 2, &seed);
        let present = tx! { tables { data: u64 => bytes<8> = 1 } insert(data[1], b"ab"); }.unwrap();
        assert_abort(
            &mut store,
            3,
            &present,
            &claims(&[(15, 0), (16, 0)]),
            AbortReason::KeyExists,
            0,
        );
        let absent = tx! { tables { data: u64 => bytes<8> = 1 } insert(data[2], b"ab"); }.unwrap();
        assert_abort(
            &mut store,
            4,
            &absent,
            &claims(&[(15, 0), (16, 0)]),
            AbortReason::BoundExceeded,
            0,
        );
        let overlay =
            tx! { tables { data: u64 => bytes<8> = 1 } data[2] = b"a"; insert(data[2], b"ab"); }
                .unwrap();
        assert_abort(
            &mut store,
            5,
            &overlay,
            &claims(&[]),
            AbortReason::KeyExists,
            0,
        );
    }

    #[test]
    fn repeated_scans_accumulate_rows_and_bytes_but_share_point_addresses() {
        let (_directory, mut store) = numbers();
        let transaction = tx! {
            tables { data: u64 => i64 = 1 }
            data[1] = 11;
            scan_bounded(data, unbounded, unbounded, 0, 1, 16);
            return scan_bounded(data, unbounded, unbounded, 0, 1, 16);
        }
        .unwrap();
        assert_abort(
            &mut store,
            3,
            &transaction,
            &claims(&[(13, 1), (14, 16)]),
            AbortReason::ResourceLimit,
            13,
        );
        assert_abort(
            &mut store,
            4,
            &transaction,
            &claims(&[(13, 2), (14, 31)]),
            AbortReason::ResourceLimit,
            14,
        );
        let policy = claims(&[(8, 1), (9, 1), (13, 2), (14, 32)]);
        let outcome = vm::execute(&mut store, 5, [5; 32], &transaction, &policy).unwrap();
        assert_eq!(assert_output(outcome, rows(&[(1, 11)])).len(), 1);
        let distinct = tx! {
            tables { data: u64 => i64 = 1 }
            scan_bounded(data, 1, 1, 3, 1, 16);
            return scan_bounded(data, 2, 2, 3, 1, 16);
        }
        .unwrap();
        assert_abort(
            &mut store,
            6,
            &distinct,
            &claims(&[(9, 1), (13, 1), (14, 16)]),
            AbortReason::ResourceLimit,
            9,
        );
    }

    #[test]
    fn overlay_overwrites_replace_bytes_and_tombstones_keep_write_charges() {
        let (_directory, mut store) = table_store(Type::U64, Type::Bytes(8));
        let transaction = tx! {
            tables { data: u64 => bytes<8> = 1 }
            data[1] = b"abcdefgh";
            data[1] = b"";
            delete(data[1]);
            data[2] = b"a";
        }
        .unwrap();
        // The first Put uses 29 bytes, then 21, then a 17-byte tombstone.
        // The second address adds 22 bytes for a final total of 39.
        assert_abort(
            &mut store,
            2,
            &transaction,
            &claims(&[(15, 1), (16, 38)]),
            AbortReason::ResourceLimit,
            15,
        );
        assert_abort(
            &mut store,
            3,
            &transaction,
            &claims(&[(15, 2), (16, 38)]),
            AbortReason::ResourceLimit,
            16,
        );
        let outcome = vm::execute(
            &mut store,
            4,
            [4; 32],
            &transaction,
            &claims(&[(15, 2), (16, 39)]),
        )
        .unwrap();
        assert_eq!(
            assert_output(outcome, Value::Unit),
            vec![
                Effect::Delete {
                    table: 1,
                    key: 1_u64.to_be_bytes().to_vec()
                },
                Effect::Put {
                    table: 1,
                    key: 2_u64.to_be_bytes().to_vec(),
                    value: vec![1, 0, 0, 0, b'a']
                },
            ]
        );
    }

    #[test]
    fn result_budget_counts_only_encoding_and_rolls_back_prior_writes() {
        let (_directory, mut store) = numbers();
        let transaction =
            tx! { tables { data: u64 => i64 = 1 } data[1] = 99; return b"abc"; }.unwrap();
        assert_abort(
            &mut store,
            3,
            &transaction,
            &claims(&[(17, 6)]),
            AbortReason::ResourceLimit,
            17,
        );
        let outcome =
            vm::execute(&mut store, 4, [4; 32], &transaction, &claims(&[(17, 7)])).unwrap();
        assert_eq!(
            assert_output(outcome, Value::Bytes(b"abc".to_vec())).len(),
            1
        );
        let unit = tx! { return (); }.unwrap();
        assert!(
            assert_output(
                vm::execute(&mut store, 5, [5; 32], &unit, &claims(&[(17, 0)])).unwrap(),
                Value::Unit
            )
            .is_empty()
        );
        let empty_rows = tx! { tables { data: u64 => i64 = 1 } return scan_bounded(data, unbounded, unbounded, 0, 0, 0); }.unwrap();
        assert_abort(
            &mut store,
            6,
            &empty_rows,
            &claims(&[(17, 3)]),
            AbortReason::ResourceLimit,
            17,
        );
        assert!(
            assert_output(
                vm::execute(&mut store, 7, [7; 32], &empty_rows, &claims(&[(17, 4)])).unwrap(),
                Value::Rows(vec![])
            )
            .is_empty()
        );
    }

    #[test]
    fn scans_order_signed_keys_numerically_across_encoding_boundaries() {
        let (_directory, mut store) = table_store(Type::I64, Type::U64);
        let transaction = tx! {
            tables { data: i64 => u64 = 1 }
            data[256] = 5; data[-1] = 2; data[255] = 4;
            data[0] = 3; data[-9223372036854775808_i64] = 1;
            return scan_bounded(data, unbounded, unbounded, 0, 5, 80);
        }
        .unwrap();
        let expected = Value::Rows(
            [i64::MIN, -1, 0, 255, 256]
                .into_iter()
                .zip(1..=5)
                .map(|(key, value)| (Value::I64(key), Value::U64(value)))
                .collect(),
        );
        assert_eq!(
            assert_output(execute(&mut store, 2, &transaction), expected).len(),
            5
        );
    }

    #[test]
    fn scan_key_value_range_and_rows_register_charges_are_distinct_stages() {
        let (_directory, mut store) = table_store(Type::Bytes(1), Type::I64);
        let seed = tx! { tables { data: bytes<1> => i64 = 1 } data[b"\0"] = 7; }.unwrap();
        execute(&mut store, 2, &seed);
        let transaction = tx! { tables { data: bytes<1> => i64 = 1 } return scan_bounded(data, unbounded, unbounded, 0, 1, 12); }.unwrap();
        for (sequence, limits, detail) in [
            (3, vec![(10, 3), (11, 7), (9, 0)], 10),
            (4, vec![(10, 4), (11, 7), (9, 0)], 11),
            (5, vec![(11, 16), (12, 16)], 11),
            (6, vec![(11, 17), (12, 16)], 12),
        ] {
            assert_abort(
                &mut store,
                sequence,
                &transaction,
                &claims(&limits),
                AbortReason::ResourceLimit,
                detail,
            );
        }
        // A NUL key occupies four canonical bytes, five schema bytes. Rows
        // adds a four-byte count to the schema key and eight-byte value.
        let policy = claims(&[
            (8, 0),
            (9, 1),
            (10, 4),
            (11, 17),
            (12, 17),
            (13, 1),
            (14, 12),
            (17, 17),
        ]);
        assert!(
            assert_output(
                vm::execute(&mut store, 7, [7; 32], &transaction, &policy).unwrap(),
                Value::Rows(vec![(Value::Bytes(vec![0]), Value::I64(7))])
            )
            .is_empty()
        );
    }

    #[test]
    fn distinct_addresses_include_table_identity_and_effects_sort_by_table() {
        let (_directory, mut store) = table_store(Type::U64, Type::I64);
        vm::execute_catalogue(
            &mut store,
            2,
            [2; 32],
            &CatalogueOperation::Create {
                name: "second".into(),
                key: Type::U64,
                value: Type::I64,
            },
        )
        .unwrap();
        let transaction = tx! {
            tables { second: u64 => i64 = 2, first: u64 => i64 = 1 }
            second[1] = 20; first[1] = 10;
            return (first[1], second[1]);
        }
        .unwrap();
        assert_abort(
            &mut store,
            3,
            &transaction,
            &claims(&[(9, 1)]),
            AbortReason::ResourceLimit,
            9,
        );
        let outcome =
            vm::execute(&mut store, 4, [4; 32], &transaction, &claims(&[(9, 2)])).unwrap();
        assert_eq!(
            assert_output(outcome, Value::Tuple(vec![Value::I64(10), Value::I64(20)])),
            vec![
                Effect::Put {
                    table: 1,
                    key: 1_u64.to_be_bytes().to_vec(),
                    value: 10_i64.to_le_bytes().to_vec()
                },
                Effect::Put {
                    table: 2,
                    key: 1_u64.to_be_bytes().to_vec(),
                    value: 20_i64.to_le_bytes().to_vec()
                },
            ]
        );
    }

    #[test]
    fn byte_and_tuple_destination_bounds_abort_at_use_not_admission() {
        let (_directory, mut store) = create_store();
        let cases = [
            tx! { let narrow: bytes<1> = b"ab"; return narrow; }.unwrap(),
            tx! { captures { value: bytes<8> = b"ab" } let narrow: bytes<1> = value; return narrow; }.unwrap(),
            tx! { let value = b"ab"; let narrow: bytes<1> = copy(value); return narrow; }.unwrap(),
            tx! { let narrow: bytes<1> = concat(b"a", b"b"); return narrow; }.unwrap(),
            tx! { let narrow: string<1> = concat("a", "b"); return narrow; }.unwrap(),
            tx! { let narrow: bytes<1> = slice_bytes(b"ab", 0, 2); return narrow; }.unwrap(),
            tx! { let narrow: bytes<1> = utf8_bytes("\u{e9}"); return narrow; }.unwrap(),
            tx! { let narrow: string<1> = parse_utf8(b"\xc3\xa9"); return narrow; }.unwrap(),
            tx! { let narrow: bytes<31> = sha256(b"abc"); return narrow; }.unwrap(),
            tx! { let narrow: (bytes<1>, i64) = (b"ab", 1_i64); return narrow; }.unwrap(),
            tx! { let pair = (b"ab", 1_i64); let narrow: bytes<1> = pair.0; return narrow; }.unwrap(),
        ];
        for (index, transaction) in cases.iter().enumerate() {
            assert_abort(
                &mut store,
                index as u64 + 1,
                transaction,
                &claims(&[]),
                AbortReason::BoundExceeded,
                0,
            );
        }
        let exact = tx! {
            let raw: bytes<2> = utf8_bytes("\u{e9}");
            let text: string<2> = parse_utf8(raw);
            let pair: (bytes<2>, string<2>) = (raw, text);
            return pair;
        }
        .unwrap();
        assert!(
            assert_output(
                execute(&mut store, 12, &exact),
                Value::Tuple(vec![
                    Value::Bytes(vec![0xc3, 0xa9]),
                    Value::String("\u{e9}".into())
                ])
            )
            .is_empty()
        );
    }

    #[test]
    fn loaded_and_rows_destination_bounds_discard_pending_writes() {
        let (_directory, mut store) = table_store(Type::Bytes(4), Type::Bytes(4));
        let seed = tx! { tables { data: bytes<4> => bytes<4> = 1 } data[b"aa"] = b"bb"; data[b"cc"] = b"dd"; }.unwrap();
        execute(&mut store, 2, &seed);
        let cases = [
            tx! { tables { data: bytes<4> => bytes<1> = 1 } delete(data[b"cc"]); return data[b"aa"]; }.unwrap(),
            tx! { tables { data: bytes<4> => bytes<4> = 1 } data[b"cc"] = b"e"; let r: rows<bytes<4>, bytes<4>, 1> = scan_bounded(data, unbounded, unbounded, 0, 2, 32); return r; }.unwrap(),
            tx! { tables { data: bytes<1> => bytes<4> = 1 } delete(data[b"c"]); return scan_bounded(data, unbounded, unbounded, 0, 2, 32); }.unwrap(),
            tx! { tables { data: bytes<4> => bytes<1> = 1 } delete(data[b"c"]); return scan_bounded(data, unbounded, unbounded, 0, 2, 32); }.unwrap(),
            tx! { tables { data: bytes<4> => bytes<4> = 1 } delete(data[b"cc"]); let r = scan_bounded(data, unbounded, unbounded, 0, 1, 16); let key: bytes<1> = rows_key(r, 0); return key; }.unwrap(),
            tx! { tables { data: bytes<4> => bytes<4> = 1 } delete(data[b"cc"]); let r = scan_bounded(data, unbounded, unbounded, 0, 1, 16); let value: bytes<1> = rows_value(r, 0); return value; }.unwrap(),
        ];
        for (index, transaction) in cases.iter().enumerate() {
            assert_abort(
                &mut store,
                index as u64 + 3,
                transaction,
                &claims(&[]),
                AbortReason::BoundExceeded,
                0,
            );
        }
    }

    #[test]
    fn declared_result_bounds_precede_result_budget_and_preserve_descriptor() {
        let (_directory, mut store) = numbers();
        let narrow = tx! {
            tables { data: u64 => i64 = 1 }
            -> bytes<1> { data[1] = 99; return b"ab"; }
        }
        .unwrap();
        let abort = assert_abort(
            &mut store,
            3,
            &narrow,
            &claims(&[(17, 0)]),
            AbortReason::BoundExceeded,
            0,
        );
        let count = u32::from_le_bytes(narrow.program_bytes()[24..28].try_into().unwrap());
        assert_eq!(abort.instruction, count - 1);
        let rows_result = tx! {
            tables { data: u64 => i64 = 1 }
            -> rows<u64, i64, 1> { data[1] = 99; return scan_bounded(data, unbounded, unbounded, 0, 2, 32); }
        }.unwrap();
        assert_abort(
            &mut store,
            4,
            &rows_result,
            &claims(&[]),
            AbortReason::BoundExceeded,
            0,
        );
        let wide = tx! { -> bytes<32> { return b"ab"; } }.unwrap();
        assert_eq!(
            execute(&mut store, 5, &wide),
            Outcome::Success {
                result_type: Type::Bytes(32),
                value: Value::Bytes(b"ab".to_vec()),
                effects: vec![],
            }
        );
    }

    #[test]
    fn intrinsic_failures_after_writes_leave_no_state_versions() {
        let (_directory, mut store) = numbers();
        for (index, (transaction, reason)) in [
            (tx! { tables { data: u64 => i64 = 1 } data[1] = 99; return 1_u64 - 2_u64; }.unwrap(), AbortReason::IntegerOverflow),
            (tx! { tables { data: u64 => i64 = 1 } delete(data[1]); return 1_i64 / 0_i64; }.unwrap(), AbortReason::DivisionByZero),
            (tx! { tables { data: u64 => i64 = 1 } data[5] = 50; return 1_u64 << 64_u64; }.unwrap(), AbortReason::InvalidShift),
            (tx! { tables { data: u64 => i64 = 1 } data[1] = 99; return parse_utf8(b"\xff"); }.unwrap(), AbortReason::InvalidUtf8),
            (tx! { tables { data: u64 => i64 = 1 } data[1] = 99; return slice_bytes(b"a", 2, 0); }.unwrap(), AbortReason::IndexOutOfBounds),
        ].iter().enumerate() {
            assert_abort(&mut store, index as u64 + 3, transaction, &claims(&[]), *reason, 0);
        }
    }

    #[test]
    fn generated_value_and_register_budget_failures_discard_prior_writes() {
        let (_directory, mut store) = numbers();
        let generated = tx! {
            tables { data: u64 => i64 = 1 }
            data[1] = 99;
            return concat(b"abcd", b"abcd");
        }
        .unwrap();
        assert_abort(
            &mut store,
            3,
            &generated,
            &claims(&[(11, 8)]),
            AbortReason::ResourceLimit,
            11,
        );
        let registers = tx! {
            tables { data: u64 => i64 = 1 }
            data[1] = 99;
            return b"a";
        }
        .unwrap();
        let abort = assert_abort(
            &mut store,
            4,
            &registers,
            &claims(&[(12, 16)]),
            AbortReason::ResourceLimit,
            12,
        );
        assert_eq!(abort.instruction, 3);
    }

    #[test]
    fn malformed_program_tail_and_arguments_are_rejected_before_any_effect() {
        let (_directory, store) = numbers();
        let transaction = tx! {
            captures { gate: bool = false }
            tables { data: u64 => i64 = 1 }
            data[1] = 99;
            if gate { return data[2]; } else { return data[3]; }
        }
        .unwrap();
        let view = storage::view(&store);
        let before_state = tree_entries(&view, TreeId::State);
        let before_outcomes = tree_entries(&view, TreeId::Outcomes);
        let (valid, arguments) = transaction.into_parts();
        let mut unknown = valid.clone();
        let last = unknown.len() - 6;
        unknown[last] = 0xff;
        let mut bad_source = valid.clone();
        let end = bad_source.len();
        bad_source[end - 2..].copy_from_slice(&u16::MAX.to_le_bytes());
        let mut truncated = valid.clone();
        truncated.pop();
        let mut trailing = valid.clone();
        trailing.push(0);
        let mut bad_argument = arguments.clone();
        *bad_argument.last_mut().unwrap() = 2;
        for (program, args) in [
            (unknown, arguments.clone()),
            (bad_source, arguments.clone()),
            (truncated, arguments.clone()),
            (trailing, arguments.clone()),
            (valid, bad_argument),
        ] {
            assert!(matches!(
                vm::interpret(&view, 3, &program, &args, &claims(&[])),
                Err(Error::Invalid(_))
            ));
            let after = storage::view(&store);
            assert_eq!(tree_entries(&after, TreeId::State), before_state);
            assert_eq!(tree_entries(&after, TreeId::Outcomes), before_outcomes);
        }
    }
}
