//! Encode and decode complete outcomes using design section D.3.
//!
//! History validation separately checks schemas and agreement with exact
//! sequence-tagged versions.

use super::Abort;
use super::AbortReason;
use super::Effect;
use super::Error;
use super::Outcome;
use super::Result;
use super::Type;
use super::Value;
use super::database::decode_catalogue;
use super::database::input_error;
use super::database::persisted;
use super::database::valid_id;
use super::value::decode_value;
use crate::storage;
use crate::storage::LimitPolicy;
use crate::storage::TreeId;
use crate::storage::View;

const MAX_OUTCOME_BYTES: usize = 128 * 1024 * 1024;

/// A resolved outcome identified by its canonical log record and sequence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutcomeRecord {
    pub sequence: u64,
    pub record_digest: [u8; 32],
    pub record_kind: u8,
    pub outcome: Outcome,
}

fn take<'a>(
    input: &mut &'a [u8],
    length: usize,
) -> Result<&'a [u8]> {
    let (bytes, rest) = input
        .split_at_checked(length)
        .ok_or(Error::Invalid("truncated outcome"))?;
    *input = rest;
    Ok(bytes)
}

fn read_blob<'a>(input: &mut &'a [u8]) -> Result<&'a [u8]> {
    let length = u32::from_le_bytes(take(input, 4)?.try_into().unwrap()) as usize;
    take(input, length)
}

fn blob(
    output: &mut Vec<u8>,
    bytes: &[u8],
) -> Result<()> {
    let length =
        u32::try_from(bytes.len()).map_err(|_| Error::Invalid("outcome blob exceeds u32"))?;
    output.extend_from_slice(&length.to_le_bytes());
    output.extend_from_slice(bytes);
    Ok(())
}

pub(super) fn effect_address(effect: &Effect) -> (u64, &[u8]) {
    match effect {
        Effect::Put { table, key, .. } | Effect::Delete { table, key } => (*table, key),
        Effect::Catalogue { table, .. } => (*table, &[]),
        Effect::Limits { .. } => (0, &[]),
    }
}

fn abort_reason(reason: u16) -> Result<AbortReason> {
    Ok(match reason {
        1 => AbortReason::MissingKey,
        2 => AbortReason::KeyExists,
        3 => AbortReason::RequireFailed,
        4 => AbortReason::ExplicitAbort,
        5 => AbortReason::IntegerOverflow,
        6 => AbortReason::BoundExceeded,
        7 => AbortReason::ResourceLimit,
        8 => AbortReason::DivisionByZero,
        9 => AbortReason::InvalidShift,
        10 => AbortReason::IndexOutOfBounds,
        11 => AbortReason::InvalidUtf8,
        16 => AbortReason::NameInUse,
        17 => AbortReason::TableNotLive,
        _ => return Err(Error::Invalid("unknown outcome abort reason")),
    })
}

fn validate_abort(
    kind: u8,
    abort: &Abort,
) -> Result<()> {
    let reason = abort.reason as u16;
    let legal = match kind {
        1 => (1..=11).contains(&reason) && abort.instruction < 65_535,
        2 => (16..=17).contains(&reason) && abort.instruction == u32::MAX,
        _ => false,
    };
    if !legal
        || (!matches!(reason, 3 | 4) && abort.user_code != 0)
        || (reason == 7 && !(1..=17).contains(&abort.detail))
        || (reason != 7 && abort.detail != 0)
    {
        return Err(Error::Invalid("invalid abort fields for record kind"));
    }
    Ok(())
}

fn validate_effect(
    sequence: u64,
    kind: u8,
    effect: &Effect,
) -> Result<()> {
    match effect {
        Effect::Put { table, key, value } if kind == 1 => {
            storage::mvcc::StateKey::new(*table, key.clone(), sequence).map_err(input_error)?;
            if value.len() > 16 * 1024 * 1024 {
                return Err(Error::Invalid("outcome Put exceeds 16 MiB"));
            }
        }
        Effect::Delete { table, key } if kind == 1 => {
            storage::mvcc::StateKey::new(*table, key.clone(), sequence).map_err(input_error)?;
        }
        Effect::Catalogue { table, value } if kind == 2 => {
            let version = decode_catalogue(*table, value)?;
            if *table > sequence || (*table == sequence && !version.live) {
                return Err(Error::Invalid("invalid catalogue effect creation sequence"));
            }
        }
        Effect::Limits { .. } if kind == 3 => {}
        _ => return Err(Error::Invalid("effect does not match record kind")),
    }
    Ok(())
}

fn validate_success(
    sequence: u64,
    kind: u8,
    ty: &Type,
    value: &Value,
    effects: &[Effect],
) -> Result<()> {
    if effects.len() > 65_535 || (kind != 1 && effects.len() != 1) {
        return Err(Error::Invalid("invalid outcome effect count"));
    }
    let expected = match (kind, effects.first()) {
        (2, Some(Effect::Catalogue { table, .. })) if *table == sequence => {
            Some((Type::U64, Value::U64(sequence)))
        }
        (2 | 3, _) => Some((Type::Unit, Value::Unit)),
        _ => None,
    };
    if expected.is_some_and(|(expected_type, expected_value)| {
        *ty != expected_type || *value != expected_value
    }) {
        return Err(Error::Invalid("invalid administrative return value"));
    }
    let mut overlay_bytes = 0_u64;
    for effect in effects {
        validate_effect(sequence, kind, effect)?;
        if let Effect::Put { key, value, .. } = effect {
            overlay_bytes += 9 + key.len() as u64 + value.len() as u64;
        } else if let Effect::Delete { key, .. } = effect {
            overlay_bytes += 9 + key.len() as u64;
        }
    }
    if overlay_bytes > 64 * 1024 * 1024 {
        return Err(Error::Invalid("outcome overlay exceeds 64 MiB"));
    }
    Ok(())
}

/// Decode exactly one outcome and reject incomplete or trailing data.
///
/// Readers of authoritative history must classify invalid encoding as
/// corruption while preserving unsupported-version errors.
pub fn decode_outcome(bytes: &[u8]) -> Result<OutcomeRecord> {
    if bytes.len() > MAX_OUTCOME_BYTES {
        return Err(Error::Invalid("outcome exceeds 128 MiB"));
    }
    let mut input = bytes;
    let version = u16::from_le_bytes(take(&mut input, 2)?.try_into().unwrap());
    if version != 1 {
        return Err(Error::Unsupported {
            format: "outcome",
            version,
        });
    }
    let record_kind = take(&mut input, 1)?[0];
    if !(1..=3).contains(&record_kind) {
        return Err(Error::Invalid("invalid outcome record kind"));
    }
    let status = take(&mut input, 1)?[0];
    let sequence = u64::from_le_bytes(take(&mut input, 8)?.try_into().unwrap());
    valid_id(sequence)?;
    let record_digest = take(&mut input, 32)?.try_into().unwrap();
    let reason = u16::from_le_bytes(take(&mut input, 2)?.try_into().unwrap());
    if take(&mut input, 2)? != [0; 2] {
        return Err(Error::Invalid("nonzero outcome reserved field"));
    }
    let instruction = u32::from_le_bytes(take(&mut input, 4)?.try_into().unwrap());
    let user_code = u32::from_le_bytes(take(&mut input, 4)?.try_into().unwrap());
    if take(&mut input, 4)? != [0; 4] {
        return Err(Error::Invalid("nonzero outcome reserved field"));
    }
    let detail = u64::from_le_bytes(take(&mut input, 8)?.try_into().unwrap());
    let returned = read_blob(&mut input)?;
    let count = u32::from_le_bytes(take(&mut input, 4)?.try_into().unwrap()) as usize;
    let outcome = match status {
        0 => {
            if reason != 0 || instruction != u32::MAX || user_code != 0 || detail != 0 {
                return Err(Error::Invalid("invalid success outcome fields"));
            }
            // The smallest effect is a Delete with an empty Unit key (13
            // bytes).
            if count > 65_535 || count > input.len() / 13 || (record_kind != 1 && count != 1) {
                return Err(Error::Invalid("invalid outcome effect count"));
            }
            let mut returned = returned;
            let result_type = Type::decode(read_blob(&mut returned)?)?;
            let value = decode_value(&result_type, read_blob(&mut returned)?)?;
            if !returned.is_empty() {
                return Err(Error::Invalid("trailing outcome TypedValue bytes"));
            }
            let mut effects = Vec::with_capacity(count);
            for _ in 0..count {
                let tag = take(&mut input, 1)?[0];
                let effect = match (record_kind, tag) {
                    (1, 1 | 2) => {
                        let table = u64::from_le_bytes(take(&mut input, 8)?.try_into().unwrap());
                        valid_id(table)?;
                        let key = read_blob(&mut input)?;
                        if table >= sequence || key.len() > 1_024 {
                            return Err(Error::Invalid("invalid outcome state address"));
                        }
                        if tag == 1 {
                            let value = read_blob(&mut input)?;
                            if value.len() > 16 * 1024 * 1024 {
                                return Err(Error::Invalid("outcome Put exceeds 16 MiB"));
                            }
                            Effect::Put {
                                table,
                                key: key.to_vec(),
                                value: value.to_vec(),
                            }
                        } else {
                            Effect::Delete {
                                table,
                                key: key.to_vec(),
                            }
                        }
                    }
                    (2, 3) => {
                        let table = u64::from_le_bytes(take(&mut input, 8)?.try_into().unwrap());
                        let value = read_blob(&mut input)?;
                        decode_catalogue(table, value)?;
                        Effect::Catalogue {
                            table,
                            value: value.to_vec(),
                        }
                    }
                    (3, 4) => Effect::Limits {
                        policy: LimitPolicy::decode(read_blob(&mut input)?).map_err(input_error)?,
                    },
                    _ => return Err(Error::Invalid("effect does not match record kind")),
                };
                if effects
                    .last()
                    .is_some_and(|previous| effect_address(previous) >= effect_address(&effect))
                {
                    return Err(Error::Invalid("outcome effects are not strictly ordered"));
                }
                effects.push(effect);
            }
            validate_success(sequence, record_kind, &result_type, &value, &effects)?;
            Outcome::Success {
                result_type,
                value,
                effects,
            }
        }
        1 => {
            if !returned.is_empty() || count != 0 {
                return Err(Error::Invalid(
                    "aborted outcome has return bytes or effects",
                ));
            }
            let abort = Abort {
                reason: abort_reason(reason)?,
                instruction,
                user_code,
                detail,
            };
            validate_abort(record_kind, &abort)?;
            Outcome::Aborted(abort)
        }
        _ => return Err(Error::Invalid("invalid outcome status")),
    };
    if !input.is_empty() {
        return Err(Error::Invalid("trailing outcome bytes"));
    }
    Ok(OutcomeRecord {
        sequence,
        record_digest,
        record_kind,
        outcome,
    })
}

fn descriptor(ty: &Type) -> Result<Vec<u8>> {
    // Bound public enum construction before the recursive, infallible encoder.
    let mut pending = vec![(ty, 1)];
    let mut size = 2;
    while let Some((ty, depth)) = pending.pop() {
        if depth > 16 {
            return Err(Error::Invalid("type nesting exceeds depth 16"));
        }
        size += match ty {
            Type::Tuple(fields) => {
                if fields.len() > 256 {
                    return Err(Error::Invalid("tuple exceeds 256 fields"));
                }
                pending.extend(fields.iter().map(|field| (field, depth + 1)));
                3
            }
            Type::Rows { key, value, .. } => {
                pending.push((key, depth + 1));
                pending.push((value, depth + 1));
                5
            }
            Type::Bytes(_) | Type::String(_) => 5,
            _ => 1,
        };
        if size > 65_536 {
            return Err(Error::Invalid("type descriptor exceeds 65,536 bytes"));
        }
    }
    let bytes = ty.descriptor();
    Type::decode(&bytes)?;
    Ok(bytes)
}

/// Encode a validated D.3 outcome with effects in canonical order.
///
/// Duplicate addresses are errors. Resolve repeated writes before encoding
/// this final outcome rather than relying on last-write-wins handling here.
pub fn encode_outcome(
    sequence: u64,
    digest: [u8; 32],
    kind: u8,
    outcome: &Outcome,
) -> Result<Vec<u8>> {
    valid_id(sequence)?;
    if !(1..=3).contains(&kind) {
        return Err(Error::Invalid("invalid outcome record kind"));
    }
    let mut bytes = vec![1, 0, kind, u8::from(matches!(outcome, Outcome::Aborted(_)))];
    bytes.extend_from_slice(&sequence.to_le_bytes());
    bytes.extend_from_slice(&digest);
    match outcome {
        Outcome::Aborted(abort) => {
            validate_abort(kind, abort)?;
            bytes.extend_from_slice(&(abort.reason as u16).to_le_bytes());
            bytes.extend_from_slice(&[0; 2]);
            bytes.extend_from_slice(&abort.instruction.to_le_bytes());
            bytes.extend_from_slice(&abort.user_code.to_le_bytes());
            bytes.extend_from_slice(&[0; 4]);
            bytes.extend_from_slice(&abort.detail.to_le_bytes());
            bytes.extend_from_slice(&[0; 8]);
        }
        Outcome::Success {
            result_type,
            value,
            effects,
        } => {
            let descriptor = descriptor(result_type)?;
            if !result_type.accepts(value) {
                return Err(Error::Invalid("outcome value does not match result type"));
            }
            validate_success(sequence, kind, result_type, value, effects)?;
            bytes.extend_from_slice(&[0; 4]);
            bytes.extend_from_slice(&u32::MAX.to_le_bytes());
            bytes.extend_from_slice(&[0; 16]);
            let mut returned = Vec::new();
            blob(&mut returned, &descriptor)?;
            blob(&mut returned, &value.encode())?;
            blob(&mut bytes, &returned)?;
            bytes.extend_from_slice(&(effects.len() as u32).to_le_bytes());
            let mut effects: Vec<_> = effects.iter().collect();
            effects.sort_unstable_by(|a, b| effect_address(a).cmp(&effect_address(b)));
            if effects
                .windows(2)
                .any(|pair| effect_address(pair[0]) == effect_address(pair[1]))
            {
                return Err(Error::Invalid("duplicate final effect address"));
            }
            for effect in effects {
                match effect {
                    Effect::Put { table, key, value } => {
                        bytes.push(1);
                        bytes.extend_from_slice(&table.to_le_bytes());
                        blob(&mut bytes, key)?;
                        blob(&mut bytes, value)?;
                    }
                    Effect::Delete { table, key } => {
                        bytes.push(2);
                        bytes.extend_from_slice(&table.to_le_bytes());
                        blob(&mut bytes, key)?;
                    }
                    Effect::Catalogue { table, value } => {
                        bytes.push(3);
                        bytes.extend_from_slice(&table.to_le_bytes());
                        blob(&mut bytes, value)?;
                    }
                    Effect::Limits { policy } => {
                        bytes.push(4);
                        blob(&mut bytes, &policy.encode())?;
                    }
                }
            }
        }
    }
    if bytes.len() > MAX_OUTCOME_BYTES {
        return Err(Error::Invalid("outcome exceeds 128 MiB"));
    }
    Ok(bytes)
}

/// Read an authoritative outcome and check that its sequence matches the tree
/// key.
///
/// Preserve unsupported-version errors when classifying invalid history.
pub fn read_outcome(
    view: &View,
    sequence: u64,
) -> Result<Option<OutcomeRecord>> {
    valid_id(sequence)?;
    let Some(bytes) = storage::get(view, TreeId::Outcomes, &sequence.to_be_bytes())? else {
        return Ok(None);
    };
    let record = decode_outcome(&bytes).map_err(persisted)?;
    if record.sequence != sequence {
        return Err(persisted(Error::Invalid(
            "outcome sequence differs from tree key",
        )));
    }
    Ok(Some(record))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vm::CatalogueVersion;
    use crate::vm::Table;
    use crate::vm::encode_catalogue;

    fn success(effects: Vec<Effect>) -> Outcome {
        Outcome::Success {
            result_type: Type::Unit,
            value: Value::Unit,
            effects,
        }
    }

    fn cases() -> Vec<(u8, Outcome)> {
        let catalogue = CatalogueVersion {
            live: true,
            name: "flags".into(),
            table: Table {
                id: 1,
                key: Type::Boolean,
                value: Type::Boolean,
            },
        };
        let mut cases = vec![
            (1, success(vec![])),
            (
                1,
                Outcome::Success {
                    result_type: Type::Rows {
                        max_rows: 3,
                        key: Box::new(Type::I64),
                        value: Box::new(Type::Bytes(8)),
                    },
                    value: Value::Rows(vec![
                        (Value::I64(-1), Value::Bytes(vec![])),
                        (Value::I64(1), Value::Bytes(vec![1])),
                    ]),
                    effects: vec![
                        Effect::Delete {
                            table: 1,
                            key: vec![0],
                        },
                        Effect::Put {
                            table: 1,
                            key: vec![1],
                            value: vec![1],
                        },
                    ],
                },
            ),
            (
                2,
                success(vec![Effect::Catalogue {
                    table: 1,
                    value: encode_catalogue(&catalogue).unwrap(),
                }]),
            ),
            (
                3,
                success(vec![Effect::Limits {
                    policy: LimitPolicy::new([0; 17]).unwrap(),
                }]),
            ),
        ];
        for reason in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 16, 17] {
            let kind = if reason < 16 { 1 } else { 2 };
            cases.push((
                kind,
                Outcome::Aborted(Abort {
                    reason: abort_reason(reason).unwrap(),
                    instruction: if kind == 1 { 0 } else { u32::MAX },
                    user_code: if matches!(reason, 3 | 4) { 42 } else { 0 },
                    detail: if reason == 7 { 17 } else { 0 },
                }),
            ));
        }
        cases
    }

    #[test]
    fn complete_codec_round_trips_results_administration_and_all_abort_reasons() {
        for (kind, outcome) in cases() {
            let bytes = encode_outcome(2, [3; 32], kind, &outcome).unwrap();
            let record = decode_outcome(&bytes).unwrap();
            assert_eq!(
                record,
                OutcomeRecord {
                    sequence: 2,
                    record_digest: [3; 32],
                    record_kind: kind,
                    outcome
                }
            );
            assert_eq!(
                encode_outcome(
                    record.sequence,
                    record.record_digest,
                    record.record_kind,
                    &record.outcome
                )
                .unwrap(),
                bytes
            );
        }
    }

    #[test]
    fn codec_rejects_every_truncation_trailing_bytes_and_unsupported_outer_version() {
        for (kind, outcome) in cases() {
            let bytes = encode_outcome(2, [3; 32], kind, &outcome).unwrap();
            for length in 0..bytes.len() {
                assert!(
                    decode_outcome(&bytes[..length]).is_err(),
                    "kind {kind}, length {length}"
                );
            }
            let mut trailing = bytes.clone();
            trailing.push(0);
            assert!(decode_outcome(&trailing).is_err());
            let mut unsupported = bytes;
            unsupported[0] = 2;
            assert!(matches!(
                decode_outcome(&unsupported),
                Err(Error::Unsupported {
                    format: "outcome",
                    version: 2
                })
            ));
        }
    }

    #[test]
    fn codec_validates_success_fields_typed_values_and_effect_order() {
        let outcome = success(vec![
            Effect::Put {
                table: 1,
                key: vec![1],
                value: vec![1],
            },
            Effect::Delete {
                table: 1,
                key: vec![0],
            },
        ]);
        let bytes = encode_outcome(2, [3; 32], 1, &outcome).unwrap();
        let decoded = decode_outcome(&bytes).unwrap();
        let Outcome::Success { effects, .. } = decoded.outcome else {
            unreachable!()
        };
        assert!(matches!(&effects[0], Effect::Delete { key, .. } if key == &[0]));
        for (offset, byte) in [
            (2, 0),
            (3, 2),
            (4, 0),
            (44, 1),
            (46, 1),
            (48, 0),
            (52, 1),
            (56, 1),
            (60, 1),
            (68, 255),
            (83, 1),
        ] {
            let mut invalid = bytes.clone();
            invalid[offset] = byte;
            assert!(decode_outcome(&invalid).is_err(), "offset {offset}");
        }
        // Unit TypedValue occupies 11 bytes; the effect vector begins at 83.
        let mut invalid = bytes.clone();
        invalid[83..87].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode_outcome(&invalid).is_err());
        let mut reversed = bytes[..87].to_vec();
        reversed.extend_from_slice(&bytes[101..]);
        reversed.extend_from_slice(&bytes[87..101]);
        assert!(decode_outcome(&reversed).is_err());
        let mut duplicate = success(effects);
        let Outcome::Success { effects, .. } = &mut duplicate else {
            unreachable!()
        };
        effects.push(effects[0].clone());
        assert!(encode_outcome(2, [3; 32], 1, &duplicate).is_err());
        for (ty, value) in [
            (Type::Boolean, Value::Unit),
            (Type::Tuple(vec![Type::Unit; 65_536]), Value::Unit),
        ] {
            assert!(
                encode_outcome(
                    2,
                    [3; 32],
                    1,
                    &Outcome::Success {
                        result_type: ty,
                        value,
                        effects: vec![]
                    }
                )
                .is_err()
            );
        }
    }

    #[test]
    fn codec_rejects_illegal_abort_fields_and_preserves_nested_versions() {
        for (kind, outcome) in cases() {
            let Outcome::Aborted(abort) = outcome else {
                continue;
            };
            for (offset, byte) in [(44, 255), (46, 1), (56, 1), (68, 1), (72, 1)] {
                let mut bytes =
                    encode_outcome(2, [3; 32], kind, &Outcome::Aborted(abort.clone())).unwrap();
                bytes[offset] = byte;
                assert!(decode_outcome(&bytes).is_err());
            }
            assert!(encode_outcome(2, [3; 32], 3, &Outcome::Aborted(abort)).is_err());
        }
        let mut bytes = encode_outcome(2, [3; 32], 1, &success(vec![])).unwrap();
        bytes[76] = 2;
        assert!(matches!(
            decode_outcome(&bytes),
            Err(Error::Unsupported {
                format: "schema",
                version: 2
            })
        ));
        let mut bytes = encode_outcome(
            2,
            [3; 32],
            3,
            &success(vec![Effect::Limits {
                policy: LimitPolicy::new([0; 17]).unwrap(),
            }]),
        )
        .unwrap();
        bytes[92] = 2;
        assert!(matches!(
            decode_outcome(&bytes),
            Err(Error::Unsupported {
                format: "limit policy",
                version: 2
            })
        ));
    }
}
