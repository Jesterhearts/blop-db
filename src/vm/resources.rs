//! Estimate memory reservations without changing logged VM resource limits.

use super::AccessManifest;
use super::Type;
use super::program::Instruction;
use super::program::Program;
use crate::storage::LimitPolicy;

/// Estimate decoding and register-analysis scratch space before decoding.
///
/// Table descriptors come from storage, so reserve their maximum format sizes.
/// Leave invalid or truncated header rejection to the VM validator.
pub(super) fn decoding(transaction: &crate::Transaction) -> u64 {
    let bytes = transaction.program_bytes();
    let base = (bytes.len() as u64 + transaction.argument_bytes().len() as u64)
        .saturating_mul(128)
        .saturating_add(65536);
    if bytes.len() < 32 {
        return base;
    }
    let registers = u64::from(u16::from_le_bytes(bytes[16..18].try_into().unwrap()));
    let tables = u64::from(u16::from_le_bytes(bytes[20..22].try_into().unwrap()));
    let instructions = u64::from(u32::from_le_bytes(bytes[24..28].try_into().unwrap()));
    base.saturating_add(tables * 8 * 1024 * 1024)
        .saturating_add(instructions.saturating_mul(registers).saturating_mul(2))
}

pub(super) fn analysis(
    program: &Program,
    decoding: u64,
) -> u64 {
    let largest = program
        .register_types
        .iter()
        .map(value_bytes)
        .max()
        .unwrap_or(64);
    // Every forward branch can retain a map of all known registers. Rc shares
    // its payload, but each instruction may create one new bounded known value.
    decoding
        .saturating_add((program.instructions.len() as u64).saturating_mul(
            (program.register_types.len() as u64 * 128).saturating_add(largest.saturating_mul(2)),
        ))
        .saturating_add(largest.saturating_mul(8))
}

// Include container capacity and allocator/node overhead, not just encoded
// bytes. In particular, a tuple of Unit values has a nonzero memory cost.
fn value_bytes(ty: &Type) -> u64 {
    64_u64.saturating_add(match ty {
        Type::Bytes(n) | Type::String(n) => 2 * u64::from(*n),
        Type::Tuple(fields) => fields.iter().map(value_bytes).sum(),
        Type::Rows {
            max_rows,
            key,
            value,
        } => u64::from(*max_rows)
            .saturating_mul(value_bytes(key).saturating_add(value_bytes(value)))
            .saturating_mul(2),
        _ => 0,
    })
}

pub(super) fn reservation(
    program: &Program,
    claims: &LimitPolicy,
    manifest: &AccessManifest,
) -> u64 {
    let limits = claims.values();
    let registers: u64 = program.register_types.iter().map(value_bytes).sum();
    let largest = program
        .register_types
        .iter()
        .chain(program.tables.iter().flat_map(|t| [&t.key, &t.value]))
        .map(value_bytes)
        .max()
        .unwrap_or(64);
    let points = program
        .instructions
        .iter()
        .filter(|instruction| {
            matches!(
                instruction,
                Instruction::Load { .. }
                    | Instruction::Exists { .. }
                    | Instruction::Insert { .. }
                    | Instruction::Store { .. }
                    | Instruction::Delete { .. }
            )
        })
        .count() as u64;
    let writes = program
        .instructions
        .iter()
        .filter(|instruction| {
            matches!(
                instruction,
                Instruction::Insert { .. } | Instruction::Store { .. } | Instruction::Delete { .. }
            )
        })
        .count() as u64;
    let rows: u64 = program
        .instructions
        .iter()
        .filter_map(|instruction| match instruction {
            Instruction::Scan { row_limit, .. } => Some(u64::from(*row_limit)),
            _ => None,
        })
        .sum();
    let scan_scratch: u64 = program
        .instructions
        .iter()
        .filter_map(|instruction| match instruction {
            Instruction::Scan {
                table, row_limit, ..
            } => {
                let table = &program.tables[*table];
                Some(
                    u64::from(*row_limit)
                        .saturating_mul(
                            value_bytes(&table.key).saturating_add(value_bytes(&table.value)),
                        )
                        .saturating_mul(2),
                )
            }
            _ => None,
        })
        .max()
        .unwrap_or(0);
    let key = program
        .tables
        .iter()
        .map(|t| t.key.max_value_bytes() as u64 * 2 + 2)
        .max()
        .unwrap_or(0)
        .min(1024);
    let value = program
        .tables
        .iter()
        .map(|t| t.value.max_value_bytes() as u64)
        .max()
        .unwrap_or(0);
    let overlay = writes
        .min(limits[14])
        .saturating_mul(key + value + 128)
        .min(limits[15].saturating_add(writes.min(limits[14]) * 128));
    let addresses = (points + rows).min(limits[8]).saturating_mul(key + 128);
    let scopes: u64 = manifest
        .entries()
        .iter()
        .map(|(scope, _)| match scope {
            super::Scope::Table(_) => 128,
            super::Scope::Key(_, key) => 128 + key.len() as u64 * 2,
        })
        .sum();
    // A decoded node is bounded by its descriptor/operand encoding, including
    // zero-width values whose shape is present in the program. Pool values can
    // repeat a shape, so count their actual nested representation separately.
    let pools: u64 = program
        .arguments
        .iter()
        .chain(&program.constants)
        .map(pool_bytes)
        .sum();
    let types: u64 = program
        .register_types
        .iter()
        .chain([&program.result_type])
        .chain(program.tables.iter().flat_map(|t| [&t.key, &t.value]))
        .map(|ty| ty.descriptor().len() as u64 * 64)
        .sum();
    let instructions: u64 = program
        .instructions
        .iter()
        .map(|instruction| {
            128 + match instruction {
                Instruction::Tuple { fields, .. } => fields.len() as u64 * 8,
                _ => 0,
            }
        })
        .sum();
    [
        4096,
        types,
        instructions,
        pools,
        scopes * 3,
        registers * 2,
        // Scratch includes loaded values before claim checks, scan construction,
        // clones and encoded result/outcome buffers during atomic installation.
        largest.saturating_mul(8),
        value_bytes(&program.result_type).saturating_mul(4),
        addresses,
        overlay.saturating_mul(8),
        scan_scratch.saturating_mul(2),
    ]
    .into_iter()
    .fold(0_u64, u64::saturating_add)
}

fn pool_bytes(value: &super::Value) -> u64 {
    use super::Value;
    64 + match value {
        Value::Bytes(bytes) => bytes.len() as u64 * 2,
        Value::String(text) => text.len() as u64 * 2,
        Value::Tuple(fields) => fields.iter().map(pool_bytes).sum(),
        Value::Rows(rows) => rows
            .iter()
            .map(|(key, value)| pool_bytes(key) + pool_bytes(value))
            .sum(),
        _ => 0,
    }
}
