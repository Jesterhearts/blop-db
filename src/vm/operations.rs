//! Compute pure value transforms after the reader has verified operand shapes.

use sha2::Digest;

use super::AbortReason;
use super::Type;
use super::Value;
use super::program::BinaryOp;
use super::program::UnaryOp;

type Result<T> = std::result::Result<T, AbortReason>;

pub(super) fn binary(
    op: BinaryOp,
    left: &Value,
    right: &Value,
    destination: &Type,
) -> Result<Value> {
    use BinaryOp::*;
    if matches!(op, Eq | Lt | Le | Gt | Ge) {
        return Ok(Value::Boolean(match op {
            Eq => left == right,
            Lt => left < right,
            Le => left <= right,
            Gt => left > right,
            Ge => left >= right,
            _ => unreachable!(),
        }));
    }
    if matches!(op, RowsKey | RowsValue) {
        let (Value::Rows(rows), Value::U64(index)) = (left, right) else {
            unreachable!()
        };
        let row = usize::try_from(*index)
            .ok()
            .and_then(|index| rows.get(index))
            .ok_or(AbortReason::IndexOutOfBounds)?;
        return Ok(if op == RowsKey {
            row.0.clone()
        } else {
            row.1.clone()
        });
    }
    if matches!(op, Shl | Shr) {
        let Value::U64(count) = right else {
            unreachable!()
        };
        if *count >= 64 {
            return Err(AbortReason::InvalidShift);
        }
        return Ok(match left {
            Value::I64(number) => Value::I64(if op == Shl {
                number.wrapping_shl(*count as u32)
            } else {
                number >> count
            }),
            Value::U64(number) => Value::U64(if op == Shl {
                number.wrapping_shl(*count as u32)
            } else {
                number >> count
            }),
            _ => unreachable!(),
        });
    }
    match (left, right) {
        (Value::I64(a), Value::I64(b)) => {
            if matches!(op, Div | Rem) && *b == 0 {
                return Err(AbortReason::DivisionByZero);
            }
            let result = match op {
                Add => a.checked_add(*b),
                Sub => a.checked_sub(*b),
                Mul => a.checked_mul(*b),
                Div => a.checked_div(*b),
                Rem => a.checked_rem(*b),
                BitAnd => Some(a & b),
                BitOr => Some(a | b),
                BitXor => Some(a ^ b),
                _ => unreachable!(),
            };
            result.map(Value::I64).ok_or(AbortReason::IntegerOverflow)
        }
        (Value::U64(a), Value::U64(b)) => {
            if matches!(op, Div | Rem) && *b == 0 {
                return Err(AbortReason::DivisionByZero);
            }
            let result = match op {
                Add => a.checked_add(*b),
                Sub => a.checked_sub(*b),
                Mul => a.checked_mul(*b),
                Div => a.checked_div(*b),
                Rem => a.checked_rem(*b),
                BitAnd => Some(a & b),
                BitOr => Some(a | b),
                BitXor => Some(a ^ b),
                _ => unreachable!(),
            };
            result.map(Value::U64).ok_or(AbortReason::IntegerOverflow)
        }
        (Value::Boolean(a), Value::Boolean(b)) => Ok(Value::Boolean(match op {
            BoolAnd => a & b,
            BoolOr => a | b,
            BoolXor => a ^ b,
            _ => unreachable!(),
        })),
        (Value::Bytes(a), Value::Bytes(b)) if op == Concat => {
            let Type::Bytes(bound) = destination else {
                unreachable!()
            };
            if a.len() + b.len() > *bound as usize {
                return Err(AbortReason::BoundExceeded);
            }
            Ok(Value::Bytes([a.as_slice(), b.as_slice()].concat()))
        }
        (Value::String(a), Value::String(b)) if op == Concat => {
            let Type::String(bound) = destination else {
                unreachable!()
            };
            if a.len() + b.len() > *bound as usize {
                return Err(AbortReason::BoundExceeded);
            }
            Ok(Value::String([a.as_str(), b.as_str()].concat()))
        }
        _ => unreachable!("the reader validates operand shapes"),
    }
}

pub(super) fn unary(
    op: UnaryOp,
    source: &Value,
) -> Result<Value> {
    use UnaryOp::*;
    Ok(match (op, source) {
        (Neg, Value::I64(number)) => {
            Value::I64(number.checked_neg().ok_or(AbortReason::IntegerOverflow)?)
        }
        (ToI64, Value::U64(number)) => {
            Value::I64(i64::try_from(*number).map_err(|_| AbortReason::IntegerOverflow)?)
        }
        (ToU64, Value::I64(number)) => {
            Value::U64(u64::try_from(*number).map_err(|_| AbortReason::IntegerOverflow)?)
        }
        (BoolNot, Value::Boolean(value)) => Value::Boolean(!value),
        (BitNot, Value::I64(value)) => Value::I64(!value),
        (BitNot, Value::U64(value)) => Value::U64(!value),
        (RowsLen, Value::Rows(rows)) => Value::U64(rows.len() as u64),
        (ByteLen, Value::Bytes(bytes)) => Value::U64(bytes.len() as u64),
        (ByteLen, Value::String(text)) => Value::U64(text.len() as u64),
        (Utf8Bytes, Value::String(text)) => Value::Bytes(text.as_bytes().to_vec()),
        (ParseUtf8, Value::Bytes(bytes)) => Value::String(
            std::str::from_utf8(bytes)
                .map_err(|_| AbortReason::InvalidUtf8)?
                .to_owned(),
        ),
        (Sha256, Value::Bytes(bytes)) => Value::Bytes(sha2::Sha256::digest(bytes).to_vec()),
        _ => unreachable!("the reader validates operand shapes"),
    })
}

pub(super) fn slice(
    source: &Value,
    start: &Value,
    length: &Value,
) -> Result<Value> {
    let (Value::Bytes(bytes), Value::U64(start), Value::U64(length)) = (source, start, length)
    else {
        unreachable!()
    };
    if *start > bytes.len() as u64 || *length > bytes.len() as u64 - start {
        return Err(AbortReason::IndexOutOfBounds);
    }
    Ok(Value::Bytes(
        bytes[*start as usize..(*start + length) as usize].to_vec(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arithmetic_extremes_and_shift_rules() {
        for op in [BinaryOp::Div, BinaryOp::Rem] {
            assert_eq!(
                binary(op, &Value::I64(i64::MIN), &Value::I64(-1), &Type::I64),
                Err(AbortReason::IntegerOverflow)
            );
            assert_eq!(
                binary(op, &Value::U64(1), &Value::U64(0), &Type::U64),
                Err(AbortReason::DivisionByZero)
            );
        }
        assert_eq!(
            binary(BinaryOp::Div, &Value::I64(-7), &Value::I64(3), &Type::I64),
            Ok(Value::I64(-2))
        );
        assert_eq!(
            binary(BinaryOp::Rem, &Value::I64(-7), &Value::I64(3), &Type::I64),
            Ok(Value::I64(-1))
        );
        assert_eq!(
            binary(BinaryOp::Sub, &Value::U64(0), &Value::U64(1), &Type::U64),
            Err(AbortReason::IntegerOverflow)
        );
        assert_eq!(
            binary(
                BinaryOp::Shl,
                &Value::U64(u64::MAX),
                &Value::U64(1),
                &Type::U64
            ),
            Ok(Value::U64(u64::MAX - 1))
        );
        assert_eq!(
            binary(BinaryOp::Shr, &Value::I64(-2), &Value::U64(1), &Type::I64),
            Ok(Value::I64(-1))
        );
        for count in [64, 65, u64::MAX] {
            assert_eq!(
                binary(
                    BinaryOp::Shl,
                    &Value::U64(1),
                    &Value::U64(count),
                    &Type::U64
                ),
                Err(AbortReason::InvalidShift)
            );
        }
        assert_eq!(
            unary(UnaryOp::Neg, &Value::I64(i64::MIN)),
            Err(AbortReason::IntegerOverflow)
        );
        assert_eq!(
            unary(UnaryOp::ToI64, &Value::U64(u64::MAX)),
            Err(AbortReason::IntegerOverflow)
        );
        assert_eq!(
            unary(UnaryOp::ToU64, &Value::I64(-1)),
            Err(AbortReason::IntegerOverflow)
        );
    }

    #[test]
    fn slices_utf8_and_hash_vectors() {
        let bytes = Value::Bytes(b"abc".to_vec());
        assert_eq!(
            slice(&bytes, &Value::U64(3), &Value::U64(0)),
            Ok(Value::Bytes(vec![]))
        );
        assert_eq!(
            slice(&bytes, &Value::U64(u64::MAX), &Value::U64(1)),
            Err(AbortReason::IndexOutOfBounds)
        );
        assert_eq!(
            slice(&bytes, &Value::U64(1), &Value::U64(u64::MAX)),
            Err(AbortReason::IndexOutOfBounds)
        );
        assert_eq!(
            unary(UnaryOp::ParseUtf8, &Value::Bytes(vec![0xff])),
            Err(AbortReason::InvalidUtf8)
        );
        assert_eq!(
            unary(UnaryOp::Sha256, &bytes),
            Ok(Value::Bytes(vec![
                0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae,
                0x22, 0x23, 0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61,
                0xf2, 0x00, 0x15, 0xad,
            ]))
        );
    }
}
