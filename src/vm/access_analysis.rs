//! Derive C.4 access scopes by propagating known values through forward
//! branches.
//!
//! Analysis does not read rows or call application code.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::rc::Rc;

use super::AccessManifest;
use super::AccessMode;
use super::Result;
use super::Scope;
use super::Type;
use super::Value;
use super::operations;
use super::program::Instruction;
use super::program::Program;
use crate::storage::encoding;
use crate::storage::encoding::Schema;

// Missing entries are Unknown. Each register has one immutable declared type,
// so equal values for the same register also have equal types at every join.
// Borrow pool values and share MOVE/branch copies rather than duplicating large
// payloads during validation, before runtime register budgets can be enforced.
type Known<'a> = BTreeMap<usize, Rc<Cow<'a, Value>>>;

fn join<'a>(
    current: &mut Known<'a>,
    incoming: &Known<'a>,
) {
    current.retain(|register, value| incoming.get(register) == Some(value));
}

fn evaluate<'a>(
    instruction: &Instruction,
    program: &'a Program,
    known: &Known<'a>,
) -> Option<Rc<Cow<'a, Value>>> {
    let value = match instruction {
        Instruction::Const { constant, .. } => {
            return Some(Rc::new(Cow::Borrowed(&program.constants[*constant])));
        }
        Instruction::Arg { argument, .. } => {
            return Some(Rc::new(Cow::Borrowed(&program.arguments[*argument])));
        }
        Instruction::Move { src, .. } => return known.get(src).cloned(),
        Instruction::Binary {
            op,
            dst,
            left,
            right,
        } => operations::binary(
            *op,
            known.get(left)?,
            known.get(right)?,
            &program.register_types[*dst],
        )
        .ok()?,
        Instruction::Unary { op, src, .. } => operations::unary(*op, known.get(src)?).ok()?,
        Instruction::Slice {
            src, start, length, ..
        } => operations::slice(known.get(src)?, known.get(start)?, known.get(length)?).ok()?,
        Instruction::Tuple { dst, fields } => {
            let Type::Tuple(types) = &program.register_types[*dst] else {
                unreachable!()
            };
            // Match the runtime's field checks before allocating the tuple:
            // source bounds may be much larger than destination field bounds.
            for (field, ty) in fields.iter().zip(types) {
                if !ty.accepts(known.get(field)?) {
                    return None;
                }
            }
            Value::Tuple(
                fields
                    .iter()
                    .map(|field| known[field].as_ref().as_ref().clone())
                    .collect(),
            )
        }
        Instruction::Field { src, field, .. } => {
            let Value::Tuple(fields) = known.get(src)?.as_ref().as_ref() else {
                unreachable!()
            };
            fields[*field].clone()
        }
        _ => return None,
    };
    Some(Rc::new(Cow::Owned(value)))
}

/// Derive scopes from a program already checked for types and control flow.
///
/// Reachability and register initialization must also be verified first.
/// Derivation does not charge resource 7 because a broader supplied manifest
/// may need fewer normalized entries.
pub(super) fn derive(program: &Program) -> Result<AccessManifest> {
    let schemas = program
        .tables
        .iter()
        .map(|table| Schema::decode(&table.key.descriptor()))
        .collect::<crate::storage::Result<Vec<_>>>()?;
    let mut entries = Vec::new();
    let mut pending: Vec<Option<Known<'_>>> = vec![None; program.instructions.len()];
    let mut fallthrough = Some(Known::new());
    for (index, instruction) in program.instructions.iter().enumerate() {
        let mut known = match (fallthrough.take(), pending[index].take()) {
            (Some(mut current), Some(incoming)) => {
                join(&mut current, &incoming);
                current
            }
            (Some(current), None) | (None, Some(current)) => current,
            (None, None) => unreachable!("verified control-flow reachability"),
        };
        let access = match instruction {
            Instruction::Load { table, key, .. } | Instruction::Exists { table, key, .. } => {
                Some((*table, Some(*key), AccessMode::Read))
            }
            Instruction::Insert { table, key, .. } => {
                Some((*table, Some(*key), AccessMode::ReadWrite))
            }
            Instruction::Store { table, key, .. } | Instruction::Delete { table, key } => {
                Some((*table, Some(*key), AccessMode::Write))
            }
            Instruction::Scan { table, .. } => Some((*table, None, AccessMode::Read)),
            _ => None,
        };
        if let Some((table, key, mode)) = access {
            let id = program.tables[table].id;
            let key = key
                .and_then(|register| known.get(&register))
                .and_then(|value| encoding::encode_key(&schemas[table], &value.encode()).ok());
            // A known key that fails its table descriptor still cannot prune
            // the access or later edges. No valid point can represent it.
            entries.push((
                key.map_or(Scope::Table(id), |key| Scope::Key(id, key)),
                mode,
            ));
        }
        let destination = match instruction {
            Instruction::Const { dst, .. }
            | Instruction::Arg { dst, .. }
            | Instruction::Move { dst, .. }
            | Instruction::Binary { dst, .. }
            | Instruction::Unary { dst, .. }
            | Instruction::Slice { dst, .. }
            | Instruction::Tuple { dst, .. }
            | Instruction::Field { dst, .. }
            | Instruction::Load { dst, .. }
            | Instruction::Exists { dst, .. }
            | Instruction::Scan { dst, .. } => Some(*dst),
            _ => None,
        };
        if let Some(dst) = destination {
            let value = evaluate(instruction, program, &known)
                .filter(|value| program.register_types[dst].accepts(value));
            if let Some(value) = value {
                known.insert(dst, value);
            } else {
                known.remove(&dst);
            }
        }
        match instruction {
            Instruction::Jump { target } | Instruction::JumpIfFalse { target, .. } => {
                // Predictable conditions and failures never remove CFG edges.
                if matches!(instruction, Instruction::JumpIfFalse { .. }) {
                    fallthrough = Some(known.clone());
                }
                if let Some(incoming) = &mut pending[*target] {
                    join(incoming, &known);
                } else {
                    pending[*target] = Some(known);
                }
            }
            Instruction::Return { .. } | Instruction::Abort { .. } => {}
            _ => fallthrough = Some(known),
        }
    }
    AccessManifest::new(entries)
}

#[cfg(test)]
mod tests {
    use super::AccessMode::Read;
    use super::AccessMode::ReadWrite;
    use super::AccessMode::Write;
    use super::Scope::Key;
    use super::Scope::Table as WholeTable;
    use super::*;
    use crate::Transaction;
    use crate::tx;
    use crate::vm::Table;
    use crate::vm::Type;
    use crate::vm::program;

    fn table(
        id: u64,
        key: Type,
        value: Type,
    ) -> Table {
        Table { id, key, value }
    }

    fn scopes(
        transaction: Transaction,
        tables: &[Table],
    ) -> AccessManifest {
        let program = program::decode(
            transaction.program_bytes(),
            transaction.argument_bytes(),
            tables,
            &crate::Limits::default().try_into().unwrap(),
        )
        .unwrap();
        derive(&program).unwrap()
    }

    fn key(
        table: u64,
        value: u64,
    ) -> Scope {
        Key(table, value.to_be_bytes().to_vec())
    }

    #[test]
    fn arguments_constants_arithmetic_and_aliases_produce_normalized_points() {
        let transaction = tx! {
            captures { base: u64 = 40_u64 }
            tables { unused: u64 => u64 = 2, data: u64 => u64 = 1 }
            data[base + 2] = 7;
            delete(data[42]);
            insert(data[42], 1);
            return data[copy(base) + 2];
        }
        .unwrap();
        assert_eq!(
            scopes(
                transaction,
                &[
                    table(1, Type::U64, Type::U64),
                    table(2, Type::U64, Type::U64),
                ]
            )
            .entries(),
            [(key(1, 42), ReadWrite)]
        );
    }

    #[test]
    fn loads_used_as_addresses_and_existence_checks_have_independent_read_scopes() {
        let transaction = tx! {
            tables { items: u64 => u64 = 1, users: u64 => u64 = 2, flags: bool => u64 = 3 }
            let owner = items[7];
            users[owner] += 1;
            flags[exists(items[8])] = 42;
        }
        .unwrap();
        let manifest = scopes(
            transaction,
            &[
                table(1, Type::U64, Type::U64),
                table(2, Type::U64, Type::U64),
                table(3, Type::Boolean, Type::U64),
            ],
        );
        assert_eq!(
            manifest.entries(),
            [
                (key(1, 7), Read),
                (key(1, 8), Read),
                (WholeTable(2), ReadWrite),
                (WholeTable(3), Write),
            ]
        );
    }

    #[test]
    fn whole_table_writes_do_not_broaden_point_reads_or_the_reverse() {
        let transaction = tx! {
            tables { data: u64 => u64 = 1 }
            let target = data[7];
            data[target] = 42;
            delete(data[target]);
        }
        .unwrap();
        assert_eq!(
            scopes(transaction, &[table(1, Type::U64, Type::U64)]).entries(),
            [(WholeTable(1), Write), (key(1, 7), Read),]
        );
        let transaction = tx! {
            tables { data: u64 => u64 = 1 }
            data[7] = 42;
            return scan_bounded(data, unbounded, unbounded, 0, 0, 0);
        }
        .unwrap();
        assert_eq!(
            scopes(transaction, &[table(1, Type::U64, Type::U64)]).entries(),
            [(WholeTable(1), Read), (key(1, 7), Write),]
        );
    }

    #[test]
    fn overlay_loads_existence_and_predictably_empty_scans_remain_unknown() {
        let transaction = tx! {
            tables { data: u64 => u64 = 1, flags: bool => bool = 2 }
            data[7] = 42;
            data[data[7]] = 1;
            flags[false] = true;
            delete(flags[exists(flags[false])]);
        }
        .unwrap();
        assert_eq!(
            scopes(
                transaction,
                &[
                    table(1, Type::U64, Type::U64),
                    table(2, Type::Boolean, Type::Boolean),
                ]
            )
            .entries(),
            [
                (WholeTable(1), Write),
                (key(1, 7), Read),
                (WholeTable(2), Write),
                (Key(2, vec![0]), Read),
            ]
        );
        let transaction = tx! {
            tables { data: u64 => u64 = 1 }
            let rows = scan_bounded(data, 10, 0, 3, 0, 0);
            data[rows_len(rows)] = 42;
            data[rows_key(rows, 0)] = 42;
            data[rows_value(rows, 0)] = 42;
        }
        .unwrap();
        assert_eq!(
            scopes(transaction, &[table(1, Type::U64, Type::U64)]).entries(),
            [(WholeTable(1), ReadWrite),]
        );
    }

    #[test]
    fn both_conditional_edges_contribute_even_for_known_conditions_and_short_circuits() {
        let transaction = tx! {
            tables { data: u64 => bool = 1 }
            if true { data[1] = true; } else { delete(data[2]); }
            require(false);
            let ignored = false && data[3];
            return true || data[4];
        }
        .unwrap();
        assert_eq!(
            scopes(transaction, &[table(1, Type::U64, Type::Boolean)]).entries(),
            [
                (key(1, 1), Write),
                (key(1, 2), Write),
                (key(1, 3), Read),
                (key(1, 4), Read),
            ]
        );
    }

    #[test]
    fn cfg_joins_retain_only_values_equal_on_every_predecessor() {
        for other in [7_u64, 8] {
            let transaction = tx! {
                captures { other: u64 = other }
                tables { data: u64 => u64 = 1 }
                let mut target: u64 = 0;
                if false { target = 7; } else { target = other; }
                data[target] = 42;
            }
            .unwrap();
            let target = if other == 7 { key(1, 7) } else { WholeTable(1) };
            assert_eq!(
                scopes(transaction, &[table(1, Type::U64, Type::U64)]).entries(),
                [(target, Write)]
            );
        }
        let transaction = tx! {
            tables { data: u64 => u64 = 1 }
            let mut target: u64 = 7;
            if true { target = data[0]; }
            data[target] = 42;
        }
        .unwrap();
        assert_eq!(
            scopes(transaction, &[table(1, Type::U64, Type::U64)]).entries(),
            [(WholeTable(1), Write), (key(1, 0), Read),]
        );
        let transaction = tx! {
            tables { data: u64 => u64 = 1 }
            let mut target: u64 = 7;
            if true { target = 1 / 0; }
            data[target] = 42;
        }
        .unwrap();
        assert_eq!(
            scopes(transaction, &[table(1, Type::U64, Type::U64)]).entries(),
            [(WholeTable(1), Write)]
        );
    }

    #[test]
    fn intrinsic_failures_yield_unknown_without_stopping_later_accesses() {
        for transaction in [
            tx! { tables { data: u64 => u64 = 1 } delete(data[1_u64 / 0_u64]); }.unwrap(),
            tx! { tables { data: u64 => u64 = 1 } delete(data[0_u64 - 1_u64]); }.unwrap(),
            tx! { tables { data: u64 => u64 = 1 } delete(data[1_u64 << 64_u64]); }.unwrap(),
            tx! { tables { data: u64 => u64 = 1 } delete(data[to_u64_checked(-1)]); }.unwrap(),
        ] {
            assert_eq!(
                scopes(transaction, &[table(1, Type::U64, Type::U64)]).entries(),
                [(WholeTable(1), Write)]
            );
        }
        let transaction = tx! {
            tables { data: u64 => u64 = 1, later: u64 => u64 = 2 }
            let failed = 1_u64 / 0_u64;
            delete(data[failed]);
            if true { later[7] = 42; } else { later[8] = 42; }
            require(false);
            return later[9];
        }
        .unwrap();
        assert_eq!(
            scopes(
                transaction,
                &[
                    table(1, Type::U64, Type::U64),
                    table(2, Type::U64, Type::U64),
                ]
            )
            .entries(),
            [
                (WholeTable(1), Write),
                (key(2, 7), Write),
                (key(2, 8), Write),
                (key(2, 9), Read),
            ]
        );
    }

    #[test]
    fn pure_byte_tuple_boolean_and_integer_operations_retain_known_keys() {
        let transaction = tx! {
            captures { text: string<8> = "abc" }
            tables { data: bytes<32> => u64 = 1 }
            let bytes = slice_bytes(concat(utf8_bytes(text), b"d"), 1, 2);
            delete(data[sha256(utf8_bytes(parse_utf8(bytes)))]);
        }
        .unwrap();
        use sha2::Digest;
        assert_eq!(
            scopes(transaction, &[table(1, Type::Bytes(32), Type::U64)]).entries(),
            [(
                Key(
                    1,
                    encoding::encode_key(
                        &Schema::decode(&Type::Bytes(32).descriptor()).unwrap(),
                        &Value::Bytes(sha2::Sha256::digest(b"bc").to_vec()).encode()
                    )
                    .unwrap()
                ),
                Write
            ),]
        );
        let transaction = tx! {
            tables { data: (u64, bytes<4>) => u64 = 1 }
            let pair = (byte_len(b"abc"), b"a\0");
            delete(data[(pair.0, copy(pair.1))]);
        }
        .unwrap();
        let mut bytes = 3_u64.to_be_bytes().to_vec();
        bytes.extend_from_slice(&[b'a', 0, 255, 0, 0]);
        assert_eq!(
            scopes(
                transaction,
                &[table(
                    1,
                    Type::Tuple(vec![Type::U64, Type::Bytes(4)]),
                    Type::U64
                )]
            )
            .entries(),
            [(Key(1, bytes), Write),]
        );
        let transaction = tx! {
            tables { data: bool => u64 = 1 }
            let number = ((to_u64_checked(-(-9)) / 2) * 3 + 1) % 5;
            let bits = ((number << 2) | 2) ^ 1;
            delete(data[bool_not(bool_and((bits & 7) == 7, to_i64_checked(bits >> 1) >= 0))]);
        }
        .unwrap();
        assert_eq!(
            scopes(transaction, &[table(1, Type::Boolean, Type::U64)]).entries(),
            [(Key(1, vec![0]), Write),]
        );
    }

    #[test]
    fn descriptor_and_utf8_or_slice_failures_produce_unknown_keys() {
        for transaction in [
            tx! { tables { data: bytes<32> => u64 = 1 } let key: bytes<1> = b"ab"; delete(data[key]); }.unwrap(),
            tx! { tables { data: bytes<32> => u64 = 1 } let key: bytes<1> = concat(b"a", b"b"); delete(data[key]); }.unwrap(),
            tx! { tables { data: bytes<32> => u64 = 1 } let key: bytes<31> = sha256(b"a"); delete(data[key]); }.unwrap(),
            tx! { tables { data: bytes<32> => u64 = 1 } let key = slice_bytes(b"a", 2, 0); delete(data[key]); }.unwrap(),
            tx! { tables { data: bytes<32> => u64 = 1 } let key: bytes<1> = slice_bytes(b"abc", 0, 2); delete(data[key]); }.unwrap(),
            tx! { tables { data: bytes<32> => u64 = 1 } let pair: (bytes<1>,) = (b"ab",); delete(data[pair.0]); }.unwrap(),
            tx! { tables { data: bytes<32> => u64 = 1 } delete(data[utf8_bytes(parse_utf8(b"\xff"))]); }.unwrap(),
        ] {
            assert_eq!(scopes(transaction, &[table(1, Type::Bytes(32), Type::U64)]).entries(), [(WholeTable(1), Write)]);
        }
        let transaction =
            tx! { tables { data: bytes<1> => u64 = 1 } delete(data[b"ab"]); }.unwrap();
        assert_eq!(
            scopes(transaction, &[table(1, Type::Bytes(1), Type::U64)]).entries(),
            [(WholeTable(1), Write)]
        );
    }

    #[test]
    fn zero_width_keys_are_points_and_source_destination_aliases_use_old_values() {
        let transaction = tx! { tables { data: () => u64 = 1 } insert(data[()], 42); }.unwrap();
        assert_eq!(
            scopes(transaction, &[table(1, Type::Unit, Type::U64)]).entries(),
            [(Key(1, vec![]), ReadWrite)]
        );
        let transaction = tx! { tables { data: u64 => u64 = 1 } let mut key: u64 = 7; key += 1; delete(data[key]); }.unwrap();
        assert_eq!(
            scopes(transaction, &[table(1, Type::U64, Type::U64)]).entries(),
            [(key(1, 8), Write)]
        );
    }
}
