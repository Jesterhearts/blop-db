//! ISA 1 type descriptors and schema-encoded values.

use super::Error;
use super::Result;

const MAX_DESCRIPTOR_BYTES: usize = 65_536;
const MAX_VALUE_BYTES: usize = 16 * 1024 * 1024;
const MAX_ROWS: u32 = 65_535;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Type {
    Unit,
    Boolean,
    I64,
    U64,
    Bytes(u32),
    String(u32),
    Tuple(Vec<Type>),
    Rows {
        max_rows: u32,
        key: Box<Type>,
        value: Box<Type>,
    },
}

/// Compare values only after validating that they have the same non-Rows shape.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum Value {
    Unit,
    Boolean(bool),
    I64(i64),
    U64(u64),
    Bytes(Vec<u8>),
    String(String),
    Tuple(Vec<Value>),
    Rows(Vec<(Value, Value)>),
}

impl Type {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_DESCRIPTOR_BYTES {
            return Err(Error::Invalid("type descriptor exceeds 65,536 bytes"));
        }
        let mut input = bytes;
        let version = u16::from_le_bytes(take(&mut input, 2)?.try_into().unwrap());
        if version != 1 {
            return Err(Error::Unsupported {
                format: "schema",
                version,
            });
        }
        let (ty, _) = decode_type(&mut input, 1)?;
        if !input.is_empty() {
            return Err(Error::Invalid("trailing type descriptor bytes"));
        }
        Ok(ty)
    }

    pub fn descriptor(&self) -> Vec<u8> {
        let mut bytes = 1_u16.to_le_bytes().to_vec();
        encode_type(self, &mut bytes);
        bytes
    }

    pub fn same_shape(
        &self,
        other: &Self,
    ) -> bool {
        match (self, other) {
            (Self::Unit, Self::Unit)
            | (Self::Boolean, Self::Boolean)
            | (Self::I64, Self::I64)
            | (Self::U64, Self::U64)
            | (Self::Bytes(_), Self::Bytes(_))
            | (Self::String(_), Self::String(_)) => true,
            (Self::Tuple(left), Self::Tuple(right)) => {
                left.len() == right.len() && left.iter().zip(right).all(|(a, b)| a.same_shape(b))
            }
            (
                Self::Rows {
                    key: a, value: b, ..
                },
                Self::Rows {
                    key: c, value: d, ..
                },
            ) => a.same_shape(c) && b.same_shape(d),
            _ => false,
        }
    }

    /// Check actual values, including row order, against a validated
    /// descriptor.
    pub fn accepts(
        &self,
        value: &Value,
    ) -> bool {
        match (self, value) {
            (Self::Unit, Value::Unit)
            | (Self::Boolean, Value::Boolean(_))
            | (Self::I64, Value::I64(_))
            | (Self::U64, Value::U64(_)) => true,
            (Self::Bytes(bound), Value::Bytes(bytes)) => bytes.len() <= *bound as usize,
            (Self::String(bound), Value::String(text)) => text.len() <= *bound as usize,
            (Self::Tuple(types), Value::Tuple(values)) => {
                types.len() == values.len()
                    && types
                        .iter()
                        .zip(values)
                        .all(|(ty, value)| ty.accepts(value))
            }
            (
                Self::Rows {
                    max_rows,
                    key,
                    value,
                },
                Value::Rows(rows),
            ) => {
                rows.len() <= *max_rows as usize
                    && rows.len() <= MAX_ROWS as usize
                    && rows.iter().all(|(k, v)| key.accepts(k) && value.accepts(v))
                    && rows.windows(2).all(|pair| pair[0].0 < pair[1].0)
            }
            _ => false,
        }
    }

    /// Maximum encoded size for a type validated by `Type::decode`.
    pub fn max_value_bytes(&self) -> usize {
        match self {
            Self::Unit => 0,
            Self::Boolean => 1,
            Self::I64 | Self::U64 => 8,
            Self::Bytes(bound) | Self::String(bound) => 4 + *bound as usize,
            Self::Tuple(fields) => fields.iter().map(Self::max_value_bytes).sum(),
            Self::Rows {
                max_rows,
                key,
                value,
            } => 4 + *max_rows as usize * (key.max_value_bytes() + value.max_value_bytes()),
        }
    }
}

impl Value {
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.encoded_len());
        encode_value(self, &mut bytes);
        bytes
    }

    pub fn encoded_len(&self) -> usize {
        match self {
            Self::Unit => 0,
            Self::Boolean(_) => 1,
            Self::I64(_) | Self::U64(_) => 8,
            Self::Bytes(bytes) => 4 + bytes.len(),
            Self::String(text) => 4 + text.len(),
            Self::Tuple(fields) => fields.iter().map(Self::encoded_len).sum(),
            Self::Rows(rows) => {
                4 + rows
                    .iter()
                    .map(|(key, value)| key.encoded_len() + value.encoded_len())
                    .sum::<usize>()
            }
        }
    }
}

fn take<'a>(
    input: &mut &'a [u8],
    length: usize,
) -> Result<&'a [u8]> {
    if length > input.len() {
        return Err(Error::Invalid("truncated type descriptor or value"));
    }
    let (bytes, rest) = input.split_at(length);
    *input = rest;
    Ok(bytes)
}

fn decode_type(
    input: &mut &[u8],
    depth: usize,
) -> Result<(Type, usize)> {
    if depth > 16 {
        return Err(Error::Invalid("type nesting exceeds depth 16"));
    }
    let tag = take(input, 1)?[0];
    let (ty, max_bytes) = match tag {
        0x00 => (Type::Unit, 0),
        0x01 => (Type::Boolean, 1),
        0x02 => (Type::I64, 8),
        0x03 => (Type::U64, 8),
        0x04 | 0x05 => {
            let bound = u32::from_le_bytes(take(input, 4)?.try_into().unwrap());
            let size = (bound as usize)
                .checked_add(4)
                .ok_or(Error::Invalid("type value size overflow"))?;
            let ty = if tag == 0x04 {
                Type::Bytes(bound)
            } else {
                Type::String(bound)
            };
            (ty, size)
        }
        0x06 => {
            let count = u16::from_le_bytes(take(input, 2)?.try_into().unwrap()) as usize;
            if count > 256 {
                return Err(Error::Invalid("tuple exceeds 256 fields"));
            }
            if count > input.len() {
                return Err(Error::Invalid("truncated tuple descriptor"));
            }
            let mut fields = Vec::with_capacity(count);
            let mut size = 0_usize;
            for _ in 0..count {
                let (field, field_size) = decode_type(input, depth + 1)?;
                size = size
                    .checked_add(field_size)
                    .filter(|&size| size <= MAX_VALUE_BYTES)
                    .ok_or(Error::Invalid("type value size exceeds 16 MiB"))?;
                fields.push(field);
            }
            (Type::Tuple(fields), size)
        }
        0x20 => {
            if depth != 1 {
                return Err(Error::Invalid("Rows is only allowed at the top level"));
            }
            let max_rows = u32::from_le_bytes(take(input, 4)?.try_into().unwrap());
            if max_rows > MAX_ROWS {
                return Err(Error::Invalid("Rows exceeds 65,535 rows"));
            }
            let (key, key_size) = decode_type(input, depth + 1)?;
            let (value, value_size) = decode_type(input, depth + 1)?;
            let size = key_size
                .checked_add(value_size)
                .and_then(|size| size.checked_mul(max_rows as usize))
                .and_then(|size| size.checked_add(4))
                .ok_or(Error::Invalid("Rows value size overflow"))?;
            (
                Type::Rows {
                    max_rows,
                    key: Box::new(key),
                    value: Box::new(value),
                },
                size,
            )
        }
        _ => return Err(Error::Invalid("unknown type tag")),
    };
    if max_bytes > MAX_VALUE_BYTES {
        return Err(Error::Invalid("type value size exceeds 16 MiB"));
    }
    Ok((ty, max_bytes))
}

fn encode_type(
    ty: &Type,
    bytes: &mut Vec<u8>,
) {
    match ty {
        Type::Unit => bytes.push(0x00),
        Type::Boolean => bytes.push(0x01),
        Type::I64 => bytes.push(0x02),
        Type::U64 => bytes.push(0x03),
        Type::Bytes(bound) | Type::String(bound) => {
            bytes.push(if matches!(ty, Type::Bytes(_)) {
                0x04
            } else {
                0x05
            });
            bytes.extend_from_slice(&bound.to_le_bytes());
        }
        Type::Tuple(fields) => {
            bytes.push(0x06);
            let count = u16::try_from(fields.len()).expect("tuple field count exceeds u16");
            bytes.extend_from_slice(&count.to_le_bytes());
            for field in fields {
                encode_type(field, bytes);
            }
        }
        Type::Rows {
            max_rows,
            key,
            value,
        } => {
            bytes.push(0x20);
            bytes.extend_from_slice(&max_rows.to_le_bytes());
            encode_type(key, bytes);
            encode_type(value, bytes);
        }
    }
}

fn encode_value(
    value: &Value,
    bytes: &mut Vec<u8>,
) {
    match value {
        Value::Unit => {}
        Value::Boolean(value) => bytes.push(u8::from(*value)),
        Value::I64(value) => bytes.extend_from_slice(&value.to_le_bytes()),
        Value::U64(value) => bytes.extend_from_slice(&value.to_le_bytes()),
        Value::Bytes(_) | Value::String(_) => {
            let data = match value {
                Value::Bytes(data) => data.as_slice(),
                Value::String(text) => text.as_bytes(),
                _ => unreachable!(),
            };
            let length = u32::try_from(data.len()).expect("value byte length exceeds u32");
            bytes.extend_from_slice(&length.to_le_bytes());
            bytes.extend_from_slice(data);
        }
        Value::Tuple(fields) => {
            for field in fields {
                encode_value(field, bytes);
            }
        }
        Value::Rows(rows) => {
            let count = u32::try_from(rows.len()).expect("row count exceeds u32");
            bytes.extend_from_slice(&count.to_le_bytes());
            for (key, value) in rows {
                encode_value(key, bytes);
                encode_value(value, bytes);
            }
        }
    }
}

fn min_value_bytes(ty: &Type) -> usize {
    match ty {
        Type::Unit => 0,
        Type::Boolean => 1,
        Type::I64 | Type::U64 => 8,
        Type::Bytes(_) | Type::String(_) | Type::Rows { .. } => 4,
        Type::Tuple(fields) => fields.iter().map(min_value_bytes).sum(),
    }
}

/// Decode exactly one value under a type validated by `Type::decode`.
pub(super) fn decode_value(
    ty: &Type,
    bytes: &[u8],
) -> Result<Value> {
    if bytes.len() > MAX_VALUE_BYTES || bytes.len() > ty.max_value_bytes() {
        return Err(Error::Invalid("encoded value exceeds type bounds"));
    }
    let mut input = bytes;
    let value = decode_value_node(ty, &mut input)?;
    if !input.is_empty() {
        return Err(Error::Invalid("trailing value bytes"));
    }
    Ok(value)
}

fn decode_value_node(
    ty: &Type,
    input: &mut &[u8],
) -> Result<Value> {
    if min_value_bytes(ty) > input.len() {
        return Err(Error::Invalid("truncated value"));
    }
    match ty {
        Type::Unit => Ok(Value::Unit),
        Type::Boolean => match take(input, 1)?[0] {
            0 => Ok(Value::Boolean(false)),
            1 => Ok(Value::Boolean(true)),
            _ => Err(Error::Invalid("noncanonical Boolean")),
        },
        Type::I64 => Ok(Value::I64(i64::from_le_bytes(
            take(input, 8)?.try_into().unwrap(),
        ))),
        Type::U64 => Ok(Value::U64(u64::from_le_bytes(
            take(input, 8)?.try_into().unwrap(),
        ))),
        Type::Bytes(bound) | Type::String(bound) => {
            let length = u32::from_le_bytes(take(input, 4)?.try_into().unwrap());
            if length > *bound {
                return Err(Error::Invalid("value exceeds byte bound"));
            }
            let data = take(input, length as usize)?;
            if matches!(ty, Type::Bytes(_)) {
                Ok(Value::Bytes(data.to_vec()))
            } else {
                let text =
                    std::str::from_utf8(data).map_err(|_| Error::Invalid("invalid UTF-8"))?;
                Ok(Value::String(text.to_owned()))
            }
        }
        Type::Tuple(fields) => {
            let mut values = Vec::with_capacity(fields.len());
            for field in fields {
                values.push(decode_value_node(field, input)?);
            }
            Ok(Value::Tuple(values))
        }
        Type::Rows {
            max_rows,
            key,
            value,
        } => {
            let count = u32::from_le_bytes(take(input, 4)?.try_into().unwrap());
            if count > *max_rows || count > MAX_ROWS {
                return Err(Error::Invalid("value exceeds row bound"));
            }
            let minimum = min_value_bytes(key)
                .checked_add(min_value_bytes(value))
                .and_then(|size| size.checked_mul(count as usize))
                .ok_or(Error::Invalid("Rows value size overflow"))?;
            if minimum > input.len() {
                return Err(Error::Invalid("truncated Rows value"));
            }
            let mut rows: Vec<(Value, Value)> = Vec::with_capacity(count as usize);
            for _ in 0..count {
                let k = decode_value_node(key, input)?;
                if rows.last().is_some_and(|(previous, _)| previous >= &k) {
                    return Err(Error::Invalid("Rows keys are not strictly increasing"));
                }
                let v = decode_value_node(value, input)?;
                rows.push((k, v));
            }
            Ok(Value::Rows(rows))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(
        ty: Type,
        value: Value,
    ) {
        let descriptor = ty.descriptor();
        assert_eq!(Type::decode(&descriptor).unwrap(), ty);
        assert!(ty.accepts(&value));
        let bytes = value.encode();
        assert_eq!(value.encoded_len(), bytes.len());
        assert!(bytes.len() <= ty.max_value_bytes());
        assert_eq!(decode_value(&ty, &bytes).unwrap(), value);
        for length in 0..descriptor.len() {
            assert!(Type::decode(&descriptor[..length]).is_err());
        }
        for length in 0..bytes.len() {
            assert!(decode_value(&ty, &bytes[..length]).is_err());
        }
        let mut trailing = descriptor;
        trailing.push(0);
        assert!(Type::decode(&trailing).is_err());
        let mut trailing = bytes;
        trailing.push(0);
        assert!(decode_value(&ty, &trailing).is_err());
    }

    fn rows_type(
        max_rows: u32,
        key: Type,
        value: Type,
    ) -> Type {
        Type::Rows {
            max_rows,
            key: Box::new(key),
            value: Box::new(value),
        }
    }

    #[test]
    fn scalar_and_tuple_round_trips() {
        round_trip(Type::Unit, Value::Unit);
        round_trip(Type::Boolean, Value::Boolean(false));
        round_trip(Type::Boolean, Value::Boolean(true));
        for value in [i64::MIN, -1, 0, 1, i64::MAX] {
            round_trip(Type::I64, Value::I64(value));
            assert_eq!(Value::I64(value).encode(), value.to_le_bytes());
        }
        for value in [0, 1, 255, 256, u64::MAX] {
            round_trip(Type::U64, Value::U64(value));
            assert_eq!(Value::U64(value).encode(), value.to_le_bytes());
        }
        round_trip(Type::Bytes(0), Value::Bytes(vec![]));
        round_trip(Type::Bytes(4), Value::Bytes(vec![0, 0xff, 2, 0]));
        round_trip(Type::String(0), Value::String(String::new()));
        round_trip(Type::String(8), Value::String("\0e\u{301}\u{1f600}".into()));
        round_trip(
            Type::Tuple(vec![
                Type::Unit,
                Type::I64,
                Type::Tuple(vec![Type::String(4), Type::Boolean]),
            ]),
            Value::Tuple(vec![
                Value::Unit,
                Value::I64(-7),
                Value::Tuple(vec![Value::String("hi\0".into()), Value::Boolean(true)]),
            ]),
        );
        assert_eq!(Type::Bytes(0x0102).descriptor(), [1, 0, 4, 2, 1, 0, 0]);
        assert_eq!(Value::Bytes(vec![0, 0xff]).encode(), [2, 0, 0, 0, 0, 0xff]);
        assert_eq!(Value::Boolean(false).encode(), [0]);
        assert_eq!(Value::Boolean(true).encode(), [1]);
    }

    #[test]
    fn unit_and_empty_tuple_remain_distinct() {
        let empty = Type::Tuple(vec![]);
        round_trip(empty.clone(), Value::Tuple(vec![]));
        assert_eq!(Type::Unit.descriptor(), [1, 0, 0]);
        assert_eq!(empty.descriptor(), [1, 0, 6, 0, 0]);
        assert_eq!(Value::Unit.encode(), Value::Tuple(vec![]).encode());
        assert!(!Type::Unit.same_shape(&empty));
        assert!(!Type::Unit.accepts(&Value::Tuple(vec![])));
        assert!(!empty.accepts(&Value::Unit));
    }

    #[test]
    fn shapes_ignore_only_bounds() {
        let left = rows_type(
            1,
            Type::Bytes(1),
            Type::Tuple(vec![Type::String(4), Type::U64]),
        );
        let right = rows_type(
            4,
            Type::Bytes(100),
            Type::Tuple(vec![Type::String(0), Type::U64]),
        );
        assert_ne!(left, right);
        assert!(left.same_shape(&right));
        assert!(right.same_shape(&left));
        assert!(!left.same_shape(&rows_type(
            1,
            Type::String(1),
            Type::Tuple(vec![Type::String(4), Type::U64])
        )));
        assert!(!Type::I64.same_shape(&Type::U64));
        assert!(!Type::Bytes(4).same_shape(&Type::String(4)));
        assert!(!Type::Tuple(vec![Type::Unit]).same_shape(&Type::Tuple(vec![])));
    }

    #[test]
    fn descriptor_rejects_unknown_tags_versions_and_nested_rows() {
        assert!(matches!(
            Type::decode(&[2, 0, 0]),
            Err(Error::Unsupported {
                format: "schema",
                version: 2
            })
        ));
        for tag in [0x07, 0x1f, 0x21, 0xff] {
            assert!(Type::decode(&[1, 0, tag]).is_err());
        }
        assert!(Type::decode(&[1, 0, 6, 0, 1]).is_err());
        let rows = rows_type(1, Type::Unit, Type::Unit);
        for ty in [
            Type::Tuple(vec![rows.clone()]),
            rows_type(1, rows.clone(), Type::Unit),
            rows_type(1, Type::Unit, rows),
        ] {
            assert!(Type::decode(&ty.descriptor()).is_err());
        }
    }

    #[test]
    fn descriptor_enforces_depth_arity_and_length() {
        let mut deep = Type::Unit;
        for _ in 1..16 {
            deep = Type::Tuple(vec![deep]);
        }
        assert_eq!(Type::decode(&deep.descriptor()).unwrap(), deep);
        assert!(Type::decode(&Type::Tuple(vec![deep.clone()]).descriptor()).is_err());
        assert!(Type::decode(&rows_type(0, deep, Type::Unit).descriptor()).is_err());

        let wide = Type::Tuple(vec![Type::Unit; 256]);
        assert_eq!(Type::decode(&wide.descriptor()).unwrap(), wide);
        assert!(Type::decode(&Type::Tuple(vec![Type::Unit; 257]).descriptor()).is_err());

        let mut fields = vec![wide; 253];
        fields.push(Type::Tuple(vec![Type::Unit]));
        let maximum = Type::Tuple(fields);
        let mut descriptor = maximum.descriptor();
        assert_eq!(descriptor.len(), MAX_DESCRIPTOR_BYTES);
        assert_eq!(Type::decode(&descriptor).unwrap(), maximum);
        descriptor[MAX_DESCRIPTOR_BYTES - 3] = 2;
        descriptor.push(0);
        assert!(Type::decode(&descriptor).is_err());
    }

    #[test]
    fn descriptor_enforces_maximum_encoded_sizes() {
        let bound = (MAX_VALUE_BYTES - 4) as u32;
        for ty in [Type::Bytes(bound), Type::String(bound)] {
            assert_eq!(
                Type::decode(&ty.descriptor()).unwrap().max_value_bytes(),
                MAX_VALUE_BYTES
            );
        }
        let maximum_rows = rows_type(MAX_ROWS, Type::Unit, Type::Unit);
        assert_eq!(
            Type::decode(&maximum_rows.descriptor())
                .unwrap()
                .max_value_bytes(),
            4
        );
        let maximum = rows_type(1, Type::Bytes(bound - 4), Type::Unit);
        assert_eq!(
            Type::decode(&maximum.descriptor())
                .unwrap()
                .max_value_bytes(),
            MAX_VALUE_BYTES
        );
        for ty in [
            Type::Bytes(bound + 1),
            Type::String(u32::MAX),
            Type::Tuple(vec![Type::Bytes(bound), Type::Boolean]),
            rows_type(1, Type::Unit, Type::Bytes(bound)),
            rows_type(MAX_ROWS, Type::U64, Type::Bytes(1024)),
            rows_type(0, Type::Bytes(u32::MAX), Type::Unit),
            rows_type(MAX_ROWS + 1, Type::Unit, Type::Unit),
            rows_type(u32::MAX, Type::Unit, Type::Unit),
        ] {
            assert!(Type::decode(&ty.descriptor()).is_err(), "{ty:?}");
        }
    }

    #[test]
    fn values_require_canonical_boolean_utf8_and_exact_lengths() {
        for byte in [2, 0x7f, 0xff] {
            assert!(decode_value(&Type::Boolean, &[byte]).is_err());
        }
        for invalid in [
            vec![0xc0, 0xaf],
            vec![0xed, 0xa0, 0x80],
            vec![0xf4, 0x90, 0x80, 0x80],
            vec![0xff],
            vec![0xc2],
        ] {
            assert!(decode_value(&Type::String(4), &Value::Bytes(invalid).encode()).is_err());
        }
        for ty in [Type::Bytes(4), Type::String(4)] {
            assert!(decode_value(&ty, &[5, 0, 0, 0]).is_err());
            assert!(decode_value(&ty, &[4, 0, 0, 0, 1]).is_err());
            assert!(decode_value(&ty, &u32::MAX.to_le_bytes()).is_err());
            assert!(decode_value(&ty, &[0, 0, 0, 0, 1]).is_err());
        }
        assert_ne!(
            Value::String("\u{e9}".into()),
            Value::String("e\u{301}".into())
        );
        assert!(decode_value(&Type::Unit, &[0]).is_err());
    }

    #[test]
    fn accepts_checks_actual_bounds_and_shapes() {
        assert!(Type::Bytes(0).accepts(&Value::Bytes(vec![])));
        assert!(!Type::Bytes(0).accepts(&Value::Bytes(vec![0])));
        assert!(!Type::Bytes(1).accepts(&Value::String("a".into())));
        assert!(Type::String(2).accepts(&Value::String("\u{e9}".into())));
        assert!(!Type::String(1).accepts(&Value::String("\u{e9}".into())));
        assert!(!Type::I64.accepts(&Value::U64(1)));
        let ty = Type::Tuple(vec![Type::Bytes(1), Type::Boolean]);
        assert!(!ty.accepts(&Value::Tuple(vec![Value::Bytes(vec![])])));
        assert!(!ty.accepts(&Value::Tuple(vec![
            Value::Bytes(vec![1, 2]),
            Value::Boolean(false)
        ])));
        assert!(!ty.accepts(&Value::Tuple(vec![Value::Bytes(vec![]), Value::U64(0)])));
        assert!(
            decode_value(
                &ty,
                &Value::Tuple(vec![Value::Bytes(vec![1, 2]), Value::Boolean(false)]).encode()
            )
            .is_err()
        );
    }

    #[test]
    fn rows_round_trip_in_logical_key_order() {
        let ty = rows_type(4, Type::I64, Type::String(4));
        round_trip(ty.clone(), Value::Rows(vec![]));
        round_trip(
            ty,
            Value::Rows(vec![
                (Value::I64(i64::MIN), Value::String("min".into())),
                (Value::I64(-1), Value::String("neg".into())),
                (Value::I64(0), Value::String(String::new())),
                (Value::I64(i64::MAX), Value::String("max".into())),
            ]),
        );
        round_trip(
            rows_type(2, Type::Bytes(1024), Type::Unit),
            Value::Rows(vec![
                (Value::Bytes(vec![0; 1024]), Value::Unit),
                (Value::Bytes(vec![1]), Value::Unit),
            ]),
        );
        round_trip(
            rows_type(1, Type::Unit, Type::Tuple(vec![])),
            Value::Rows(vec![(Value::Unit, Value::Tuple(vec![]))]),
        );
        round_trip(rows_type(0, Type::Unit, Type::Unit), Value::Rows(vec![]));
        assert_eq!(Value::Rows(vec![]).encode(), [0, 0, 0, 0]);
    }

    #[test]
    fn rows_reject_duplicates_descending_keys_and_mismatched_shapes() {
        let ty = rows_type(2, Type::I64, Type::Unit);
        for keys in [[0, 0], [1, 0]] {
            let value = Value::Rows(
                keys.into_iter()
                    .map(|k| (Value::I64(k), Value::Unit))
                    .collect(),
            );
            assert!(!ty.accepts(&value));
            assert!(decode_value(&ty, &value.encode()).is_err());
        }
        assert!(!ty.accepts(&Value::Rows(vec![(Value::U64(0), Value::Unit)])));
        assert!(!ty.accepts(&Value::Rows(vec![(Value::I64(0), Value::Boolean(false))])));
        assert!(!ty.accepts(&Value::Rows(vec![
            (Value::I64(0), Value::Unit),
            (Value::U64(1), Value::Unit)
        ])));
        let units = rows_type(2, Type::Unit, Type::Unit);
        assert!(decode_value(&units, &[2, 0, 0, 0]).is_err());
        let bounded = rows_type(1, Type::Bytes(0), Type::String(0));
        for value in [
            Value::Rows(vec![(Value::Bytes(vec![0]), Value::String(String::new()))]),
            Value::Rows(vec![(Value::Bytes(vec![]), Value::String("a".into()))]),
        ] {
            assert!(!bounded.accepts(&value));
            assert!(decode_value(&bounded, &value.encode()).is_err());
        }
    }

    #[test]
    fn row_counts_are_checked_before_allocation() {
        let ty = rows_type(MAX_ROWS, Type::U64, Type::Unit);
        for count in [1, MAX_ROWS, MAX_ROWS + 1, u32::MAX] {
            assert!(decode_value(&ty, &count.to_le_bytes()).is_err());
        }
        let rows = Value::Rows(vec![
            (Value::U64(0), Value::Unit),
            (Value::U64(1), Value::Unit),
        ]);
        let bounded = rows_type(1, Type::U64, Type::Unit);
        assert!(!bounded.accepts(&rows));
        assert!(decode_value(&bounded, &rows.encode()).is_err());
        let value = Value::Rows(
            (0..MAX_ROWS)
                .map(|k| (Value::U64(u64::from(k)), Value::Unit))
                .collect(),
        );
        assert!(ty.accepts(&value));
        assert_eq!(decode_value(&ty, &value.encode()).unwrap(), value);
    }

    #[test]
    fn value_order_matches_canonical_key_order_for_equal_shapes() {
        let signed = [i64::MIN, -256, -1, 0, 1, 255, 256, i64::MAX].map(Value::I64);
        assert!(signed.windows(2).all(|pair| pair[0] < pair[1]));
        let unsigned = [0, 1, 255, 256, u64::MAX].map(Value::U64);
        assert!(unsigned.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(Value::Boolean(false) < Value::Boolean(true));
        let bytes =
            [vec![], vec![0], vec![0, 0], vec![0, 1], vec![1], vec![0xff]].map(Value::Bytes);
        assert!(bytes.windows(2).all(|pair| pair[0] < pair[1]));
        let text = ["", "\0", "\0a", "a", "\u{e9}", "\u{1f600}"].map(|s| Value::String(s.into()));
        assert!(text.windows(2).all(|pair| pair[0] < pair[1]));
        let tuples = [
            Value::Tuple(vec![Value::Bytes(vec![]), Value::I64(i64::MAX)]),
            Value::Tuple(vec![Value::Bytes(vec![0]), Value::I64(-1)]),
            Value::Tuple(vec![Value::Bytes(vec![0]), Value::I64(0)]),
        ];
        assert!(tuples.windows(2).all(|pair| pair[0] < pair[1]));
    }
}
