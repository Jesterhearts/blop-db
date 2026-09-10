//! Validated storage schemas and binary transforms between values and canonical
//! keys.

use super::Error;
use super::Result;

pub(crate) const MAX_KEY_BYTES: usize = 1_024;
pub(crate) const MAX_VALUE_BYTES: usize = 16 * 1024 * 1024;
const MAX_DESCRIPTOR_BYTES: usize = 65_536;

#[derive(Clone, Debug, Eq, PartialEq)]
enum TypeNode {
    Unit,
    Boolean,
    I64,
    U64,
    Bytes(usize),
    String(usize),
    Tuple(Vec<TypeNode>),
}

/// A canonical, non-Rows storage schema whose maximum value fits 16 MiB.
///
/// Value schemas may have a maximum key size above 1,024 bytes. Table schema
/// validation and the key transforms must reject such schemas for keys.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Schema {
    descriptor: Vec<u8>,
    node: TypeNode,
    max_value_bytes: usize,
    max_key_bytes: usize,
}

impl Schema {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_DESCRIPTOR_BYTES {
            return Err(Error::InvalidInput(
                "schema descriptor exceeds 65,536 bytes",
            ));
        }
        let mut input = bytes;
        let version = u16::from_le_bytes(take(&mut input, 2)?.try_into().unwrap());
        if version != 1 {
            return Err(Error::Unsupported {
                format: "schema",
                version,
            });
        }
        let (node, max_value_bytes, max_key_bytes) = decode_node(&mut input, 1)?;
        if !input.is_empty() {
            return Err(Error::InvalidInput("trailing schema descriptor bytes"));
        }
        Ok(Self {
            descriptor: bytes.to_vec(),
            node,
            max_value_bytes,
            max_key_bytes,
        })
    }

    pub fn bytes(&self) -> &[u8] {
        &self.descriptor
    }

    pub fn max_value_bytes(&self) -> usize {
        self.max_value_bytes
    }

    pub fn max_key_bytes(&self) -> usize {
        self.max_key_bytes
    }
}

fn take<'a>(
    input: &mut &'a [u8],
    length: usize,
) -> Result<&'a [u8]> {
    if length > input.len() {
        return Err(Error::InvalidInput("truncated schema, value, or key"));
    }
    let (bytes, rest) = input.split_at(length);
    *input = rest;
    Ok(bytes)
}

fn decode_node(
    input: &mut &[u8],
    depth: usize,
) -> Result<(TypeNode, usize, usize)> {
    if depth > 16 {
        return Err(Error::InvalidInput("schema nesting exceeds depth 16"));
    }
    let tag = take(input, 1)?[0];
    let (node, value_bytes, key_bytes) = match tag {
        0x00 => (TypeNode::Unit, 0, 0),
        0x01 => (TypeNode::Boolean, 1, 1),
        0x02 => (TypeNode::I64, 8, 8),
        0x03 => (TypeNode::U64, 8, 8),
        0x04 | 0x05 => {
            let bound = u32::from_le_bytes(take(input, 4)?.try_into().unwrap()) as usize;
            let value_bytes = bound
                .checked_add(4)
                .ok_or(Error::InvalidInput("schema value size overflow"))?;
            let key_bytes = bound
                .checked_mul(2)
                .and_then(|bytes| bytes.checked_add(2))
                .ok_or(Error::InvalidInput("schema key size overflow"))?;
            let node = if tag == 0x04 {
                TypeNode::Bytes(bound)
            } else {
                TypeNode::String(bound)
            };
            (node, value_bytes, key_bytes)
        }
        0x06 => {
            let count = u16::from_le_bytes(take(input, 2)?.try_into().unwrap()) as usize;
            if count > 256 {
                return Err(Error::InvalidInput("tuple exceeds 256 fields"));
            }
            if count > input.len() {
                return Err(Error::InvalidInput("truncated tuple descriptor"));
            }
            let mut fields = Vec::with_capacity(count);
            let mut value_bytes = 0_usize;
            let mut key_bytes = 0_usize;
            for _ in 0..count {
                let (field, value_size, key_size) = decode_node(input, depth + 1)?;
                value_bytes = value_bytes
                    .checked_add(value_size)
                    .filter(|&bytes| bytes <= MAX_VALUE_BYTES)
                    .ok_or(Error::InvalidInput("schema value size exceeds 16 MiB"))?;
                key_bytes = key_bytes
                    .checked_add(key_size)
                    .ok_or(Error::InvalidInput("schema key size overflow"))?;
                fields.push(field);
            }
            (TypeNode::Tuple(fields), value_bytes, key_bytes)
        }
        0x20 => return Err(Error::InvalidInput("Rows is not a storage schema type")),
        _ => return Err(Error::InvalidInput("unknown schema type tag")),
    };
    if value_bytes > MAX_VALUE_BYTES {
        return Err(Error::InvalidInput("schema value size exceeds 16 MiB"));
    }
    Ok((node, value_bytes, key_bytes))
}

/// Validate one complete schema-encoded value without allocating its decoded
/// form.
pub fn validate_value(
    schema: &Schema,
    value: &[u8],
) -> Result<()> {
    if value.len() > schema.max_value_bytes {
        return Err(Error::InvalidInput("value exceeds schema maximum size"));
    }
    let mut input = value;
    read_value(&schema.node, &mut input, None)?;
    if !input.is_empty() {
        return Err(Error::InvalidInput("trailing value bytes"));
    }
    Ok(())
}

/// Convert a complete schema-encoded value to an order-preserving canonical
/// key.
pub fn encode_key(
    schema: &Schema,
    value: &[u8],
) -> Result<Vec<u8>> {
    if schema.max_key_bytes > MAX_KEY_BYTES {
        return Err(Error::InvalidInput("schema key size exceeds 1,024 bytes"));
    }
    if value.len() > schema.max_value_bytes {
        return Err(Error::InvalidInput("value exceeds schema maximum size"));
    }
    let mut input = value;
    let mut key = Vec::with_capacity(schema.max_key_bytes);
    read_value(&schema.node, &mut input, Some(&mut key))?;
    if !input.is_empty() {
        return Err(Error::InvalidInput("trailing value bytes"));
    }
    Ok(key)
}

fn read_value(
    node: &TypeNode,
    input: &mut &[u8],
    mut key: Option<&mut Vec<u8>>,
) -> Result<()> {
    match node {
        TypeNode::Unit => {}
        TypeNode::Boolean => {
            let byte = take(input, 1)?[0];
            if byte > 1 {
                return Err(Error::InvalidInput("invalid Boolean value"));
            }
            if let Some(key) = key {
                key.push(byte);
            }
        }
        TypeNode::I64 | TypeNode::U64 => {
            let mut number = u64::from_le_bytes(take(input, 8)?.try_into().unwrap());
            if matches!(node, TypeNode::I64) {
                number ^= 1 << 63;
            }
            if let Some(key) = key {
                key.extend_from_slice(&number.to_be_bytes());
            }
        }
        TypeNode::Bytes(bound) | TypeNode::String(bound) => {
            let length = u32::from_le_bytes(take(input, 4)?.try_into().unwrap()) as usize;
            if length > *bound {
                return Err(Error::InvalidInput("byte length exceeds schema bound"));
            }
            let bytes = take(input, length)?;
            if matches!(node, TypeNode::String(_)) && std::str::from_utf8(bytes).is_err() {
                return Err(Error::InvalidInput("invalid UTF-8 string"));
            }
            if let Some(key) = key {
                escape_into(bytes, key);
            }
        }
        TypeNode::Tuple(fields) => {
            for field in fields {
                read_value(field, input, key.as_deref_mut())?;
            }
        }
    }
    Ok(())
}

/// Decode one complete canonical key into its canonical schema value encoding.
pub fn decode_key(
    schema: &Schema,
    key: &[u8],
) -> Result<Vec<u8>> {
    if schema.max_key_bytes > MAX_KEY_BYTES {
        return Err(Error::InvalidInput("schema key size exceeds 1,024 bytes"));
    }
    if key.len() > schema.max_key_bytes {
        return Err(Error::InvalidInput("key exceeds schema maximum size"));
    }
    let mut input = key;
    let mut value = Vec::with_capacity(schema.max_value_bytes);
    read_key(&schema.node, &mut input, &mut value)?;
    if !input.is_empty() {
        return Err(Error::InvalidInput("trailing key bytes"));
    }
    Ok(value)
}

fn read_key(
    node: &TypeNode,
    input: &mut &[u8],
    value: &mut Vec<u8>,
) -> Result<()> {
    match node {
        TypeNode::Unit => {}
        TypeNode::Boolean => {
            let byte = take(input, 1)?[0];
            if byte > 1 {
                return Err(Error::InvalidInput("invalid Boolean key"));
            }
            value.push(byte);
        }
        TypeNode::I64 | TypeNode::U64 => {
            let mut number = u64::from_be_bytes(take(input, 8)?.try_into().unwrap());
            if matches!(node, TypeNode::I64) {
                number ^= 1 << 63;
            }
            value.extend_from_slice(&number.to_le_bytes());
        }
        TypeNode::Bytes(bound) | TypeNode::String(bound) => {
            let (bytes, consumed) = unescape(input)?;
            if bytes.len() > *bound {
                return Err(Error::InvalidInput("byte length exceeds schema bound"));
            }
            if matches!(node, TypeNode::String(_)) && std::str::from_utf8(&bytes).is_err() {
                return Err(Error::InvalidInput("invalid UTF-8 string"));
            }
            *input = &input[consumed..];
            value.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            value.extend_from_slice(&bytes);
        }
        TypeNode::Tuple(fields) => {
            for field in fields {
                read_key(field, input, value)?;
            }
        }
    }
    Ok(())
}

pub(crate) fn escape_into(
    bytes: &[u8],
    encoded: &mut Vec<u8>,
) {
    encoded.reserve(2 * bytes.len() + 2);
    for &byte in bytes {
        encoded.push(byte);
        if byte == 0 {
            encoded.push(0xff);
        }
    }
    encoded.extend_from_slice(&[0, 0]);
}

/// Decode one Escape frame. Callers bound the enclosing key before decoding.
pub(crate) fn unescape(encoded: &[u8]) -> Result<(Vec<u8>, usize)> {
    let mut bytes = Vec::new();
    let mut input = encoded;
    loop {
        let byte = take(&mut input, 1)?[0];
        if byte != 0 {
            bytes.push(byte);
            continue;
        }
        match take(&mut input, 1)?[0] {
            0 => return Ok((bytes, encoded.len() - input.len())),
            0xff => bytes.push(0),
            _ => return Err(Error::InvalidInput("invalid key escape")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn escape(bytes: &[u8]) -> Vec<u8> {
        let mut encoded = Vec::new();
        escape_into(bytes, &mut encoded);
        encoded
    }

    fn schema(node: &[u8]) -> Schema {
        Schema::decode(&[&[1, 0], node].concat()).unwrap()
    }

    fn blob(bytes: &[u8]) -> Vec<u8> {
        [&(bytes.len() as u32).to_le_bytes(), bytes].concat()
    }

    fn bytes_schema(
        tag: u8,
        bound: u32,
    ) -> Schema {
        schema(&[&[tag], bound.to_le_bytes().as_slice()].concat())
    }

    fn round_trip(
        schema: &Schema,
        value: &[u8],
        expected: &[u8],
    ) {
        validate_value(schema, value).unwrap();
        assert_eq!(encode_key(schema, value).unwrap(), expected);
        assert_eq!(decode_key(schema, expected).unwrap(), value);
    }

    #[test]
    fn primitive_vectors_and_integer_extremes() {
        let unsigned = schema(&[3]);
        assert_eq!(unsigned.bytes(), &[1, 0, 3]);
        assert_eq!(unsigned.max_value_bytes(), 8);
        assert_eq!(unsigned.max_key_bytes(), 8);
        let values = [0_u64, 1, 7, 42, 1 << 63, u64::MAX];
        let mut keys = Vec::new();
        for value in values {
            round_trip(&unsigned, &value.to_le_bytes(), &value.to_be_bytes());
            keys.push(encode_key(&unsigned, &value.to_le_bytes()).unwrap());
        }
        assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));

        let signed = schema(&[2]);
        let values = [i64::MIN, i64::MIN + 1, -1, 0, 1, i64::MAX];
        let mut keys = Vec::new();
        for value in values {
            let expected = ((value as u64) ^ (1 << 63)).to_be_bytes();
            round_trip(&signed, &value.to_le_bytes(), &expected);
            keys.push(expected);
        }
        assert_eq!(keys[2], [0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);
        assert_eq!(keys[3], [0x80, 0, 0, 0, 0, 0, 0, 0]);
        assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
        for value in [0, 1] {
            round_trip(&schema(&[1]), &[value], &[value]);
        }
    }

    #[test]
    fn zero_width_and_zero_bound_types_remain_distinct() {
        let unit = schema(&[0]);
        let tuple = schema(&[6, 0, 0]);
        assert_ne!(unit, tuple);
        for schema in [&unit, &tuple] {
            round_trip(schema, &[], &[]);
            assert_eq!(schema.max_value_bytes(), 0);
            assert_eq!(schema.max_key_bytes(), 0);
        }
        for tag in [4, 5] {
            let schema = bytes_schema(tag, 0);
            round_trip(&schema, &[0; 4], &[0; 2]);
            assert_eq!(schema.max_value_bytes(), 4);
            assert_eq!(schema.max_key_bytes(), 2);
            assert!(validate_value(&schema, &blob(b"x")).is_err());
            assert!(decode_key(&schema, &escape(b"x")).is_err());
        }
    }

    #[test]
    fn escape_round_trips_all_bytes_and_preserves_prefix_order() {
        assert_eq!(escape(b"A\0B"), [0x41, 0, 0xff, 0x42, 0, 0]);
        let all: Vec<_> = (0..=255).collect();
        let mut framed = escape(&all);
        let consumed = framed.len();
        framed.extend_from_slice(b"suffix");
        assert_eq!(unescape(&framed).unwrap(), (all.clone(), consumed));
        let schema = bytes_schema(4, 256);
        round_trip(&schema, &blob(&all), &escape(&all));
        let values: &[&[u8]] = &[b"", b"\0", b"\0\0", b"\0a", b"a", b"a\0", b"aa", b"\xff"];
        let keys: Vec<_> = values.iter().map(|value| escape(value)).collect();
        assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
        for bad in [&b""[..], b"a", b"\0", b"\0\xff", b"\0\x01", b"\0\xfe"] {
            assert!(matches!(unescape(bad), Err(Error::InvalidInput(_))));
        }
    }

    #[test]
    fn utf8_and_nested_tuple_vectors() {
        let string = bytes_schema(5, 8);
        assert_eq!(string.bytes(), &[1, 0, 5, 8, 0, 0, 0]);
        round_trip(&string, &[1, 0, 0, 0, b'a'], b"a\0\0");
        for text in ["", "\0", "\u{e9}", "e\u{301}", "\u{10ffff}"] {
            round_trip(&string, &blob(text.as_bytes()), &escape(text.as_bytes()));
        }
        assert_ne!(
            encode_key(&string, &blob("\u{e9}".as_bytes())).unwrap(),
            encode_key(&string, &blob("e\u{301}".as_bytes())).unwrap()
        );
        let tuple = schema(&[6, 2, 0, 3, 5, 8, 0, 0, 0]);
        let value = [&7_u64.to_le_bytes()[..], &blob(b"a")].concat();
        let key = [0, 0, 0, 0, 0, 0, 0, 7, b'a', 0, 0];
        round_trip(&tuple, &value, &key);

        let nested = schema(&[6, 3, 0, 0, 6, 2, 0, 3, 5, 8, 0, 0, 0, 1]);
        round_trip(
            &nested,
            &[value, vec![1]].concat(),
            &[&key[..], &[1]].concat(),
        );
        let pairs = [
            (0_u64, b"z".as_slice()),
            (1, b""),
            (1, b"\0"),
            (1, b"a"),
            (2, b""),
        ];
        let keys: Vec<_> = pairs
            .into_iter()
            .map(|(number, text)| {
                encode_key(&tuple, &[&number.to_le_bytes()[..], &blob(text)].concat()).unwrap()
            })
            .collect();
        assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn schemas_enforce_depth_arity_and_descriptor_bounds() {
        let mut nested = vec![1, 0];
        for _ in 1..16 {
            nested.extend_from_slice(&[6, 1, 0]);
        }
        nested.push(0);
        let deepest = Schema::decode(&nested).unwrap();
        round_trip(&deepest, &[], &[]);
        nested.splice(2..2, [6, 1, 0]);
        assert!(Schema::decode(&nested).is_err());

        let mut wide = vec![1, 0, 6, 0, 1];
        wide.extend_from_slice(&[0; 256]);
        Schema::decode(&wide).unwrap();
        wide[3] = 1;
        wide.push(0);
        assert!(Schema::decode(&wide).is_err());

        // Exactly 65,536 descriptor bytes, with zero-width fields throughout.
        let mut maximum = vec![1, 0, 6, 254, 0];
        for _ in 0..253 {
            maximum.extend_from_slice(&[6, 0, 1]);
            maximum.extend_from_slice(&[0; 256]);
        }
        maximum.extend_from_slice(&[6, 1, 0, 0]);
        assert_eq!(maximum.len(), MAX_DESCRIPTOR_BYTES);
        round_trip(&Schema::decode(&maximum).unwrap(), &[], &[]);
        maximum[MAX_DESCRIPTOR_BYTES - 3] = 2;
        maximum.push(0);
        assert!(Schema::decode(&maximum).is_err());
    }

    #[test]
    fn schema_maxima_distinguish_key_and_value_limits() {
        let maximum = bytes_schema(4, (MAX_VALUE_BYTES - 4) as u32);
        assert_eq!(maximum.max_value_bytes(), MAX_VALUE_BYTES);
        assert_eq!(maximum.max_key_bytes(), 2 * (MAX_VALUE_BYTES - 4) + 2);
        let value = blob(&vec![0; MAX_VALUE_BYTES - 4]);
        validate_value(&maximum, &value).unwrap();
        assert!(validate_value(&maximum, &vec![0; MAX_VALUE_BYTES + 1]).is_err());
        assert!(encode_key(&maximum, &blob(b"")).is_err());
        assert!(decode_key(&maximum, &[0, 0]).is_err());
        for bound in [(MAX_VALUE_BYTES - 3) as u32, u32::MAX] {
            let bytes = [&[1, 0, 4][..], &bound.to_le_bytes()].concat();
            assert!(Schema::decode(&bytes).is_err());
        }
        let tuple = [&[1, 0, 6, 2, 0][..], &maximum.bytes()[2..], &[1]].concat();
        assert!(Schema::decode(&tuple).is_err());

        let key = bytes_schema(4, 511);
        assert_eq!(key.max_key_bytes(), MAX_KEY_BYTES);
        round_trip(&key, &blob(&[0; 511]), &escape(&[0; 511]));
        let too_wide = bytes_schema(4, 512);
        validate_value(&too_wide, &blob(b"a")).unwrap();
        assert!(encode_key(&too_wide, &blob(b"a")).is_err());
        assert!(decode_key(&too_wide, &escape(b"a")).is_err());
    }

    #[test]
    fn malformed_descriptors_values_and_keys_are_invalid_input() {
        for version in [0_u16, 2, 257, u16::MAX] {
            let bytes = [version.to_le_bytes().as_slice(), &[0]].concat();
            assert!(
                matches!(Schema::decode(&bytes), Err(Error::Unsupported { format: "schema", version: actual }) if actual == version)
            );
        }
        for bad in [
            &b""[..],
            &[1],
            &[1, 0],
            &[1, 0, 0xff],
            &[1, 0, 0, 0],
            &[1, 0, 0x20],
            &[1, 0, 6, 1, 0, 0x20],
            &[1, 0, 4, 0],
            &[1, 0, 6, 1, 0],
        ] {
            assert!(matches!(Schema::decode(bad), Err(Error::InvalidInput(_))));
        }
        let tuple = schema(&[6, 3, 0, 1, 3, 5, 8, 0, 0, 0]);
        let value = [&[1][..], &42_u64.to_le_bytes(), &blob(b"a\0b")].concat();
        let key = encode_key(&tuple, &value).unwrap();
        for end in 0..value.len() {
            assert!(validate_value(&tuple, &value[..end]).is_err());
            assert!(encode_key(&tuple, &value[..end]).is_err());
        }
        for end in 0..key.len() {
            assert!(decode_key(&tuple, &key[..end]).is_err());
        }
        assert!(validate_value(&tuple, &[value, vec![0]].concat()).is_err());
        assert!(decode_key(&tuple, &[key, vec![0]].concat()).is_err());
        for bad in [&[2][..], &[255], &[], &[0, 0]] {
            assert!(validate_value(&schema(&[1]), bad).is_err());
            assert!(encode_key(&schema(&[1]), bad).is_err());
            assert!(decode_key(&schema(&[1]), bad).is_err());
        }
        let string = bytes_schema(5, 8);
        for bad in [
            &b"\xc0\x80"[..],
            b"\xed\xa0\x80",
            b"\xf4\x90\x80\x80",
            b"\x80",
            b"\xc2",
        ] {
            assert!(validate_value(&string, &blob(bad)).is_err());
            assert!(encode_key(&string, &blob(bad)).is_err());
            assert!(decode_key(&string, &escape(bad)).is_err());
        }
        let bounded = bytes_schema(4, 1);
        assert!(decode_key(&bounded, b"ab\0\0").is_err());
        assert!(validate_value(&bounded, &[0xff; 4]).is_err());
        for bad in [&b"\0\x01"[..], b"a\0", b"\0\xff\0", b"\0\0a"] {
            assert!(decode_key(&string, bad).is_err());
        }
    }
}
