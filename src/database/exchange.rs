//! Canonical H.1/H.2 exchange framing, independent of transport and VM replay.

use sha2::Digest;
use sha2::Sha256;

use super::Error;
use super::Result;
use crate::vm::OutcomeRecord;
use crate::vm::{
    self,
};

pub const MAX_BATCH_BYTES: usize = 256 * 1024 * 1024;
pub const EMPTY_BATCH_BYTES: usize = 60;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum CursorKind {
    Resolved = 1,
    Logical = 2,
    LogReplica = 3,
}

impl CursorKind {
    pub(super) fn decode(byte: u8) -> Result<Self> {
        match byte {
            1 => Ok(Self::Resolved),
            2 => Ok(Self::Logical),
            3 => Ok(Self::LogReplica),
            _ => Err(Error::InvalidFormat("invalid cursor kind")),
        }
    }
}

/// An identity reference, not an authorization secret or proof of registration.
/// Dropping it does not release the durable claim. Decode then use
/// `reopen_cursor` to check the current registration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CursorToken {
    pub(super) database_id: [u8; 16],
    pub(super) cursor_namespace: [u8; 16],
    pub(super) cursor_id: u64,
    pub(super) kind: CursorKind,
}

impl CursorToken {
    pub fn database_id(&self) -> [u8; 16] {
        self.database_id
    }

    pub fn cursor_namespace(&self) -> [u8; 16] {
        self.cursor_namespace
    }

    pub fn cursor_id(&self) -> u64 {
        self.cursor_id
    }

    pub fn kind(&self) -> CursorKind {
        self.kind
    }

    pub fn encode(&self) -> [u8; 56] {
        let mut bytes = [0; 56];
        bytes[..8].copy_from_slice(b"BLOPCT01");
        bytes[8..10].copy_from_slice(&1_u16.to_le_bytes());
        bytes[10] = self.kind as u8;
        bytes[12..28].copy_from_slice(&self.database_id);
        bytes[28..44].copy_from_slice(&self.cursor_namespace);
        bytes[44..52].copy_from_slice(&self.cursor_id.to_le_bytes());
        let crc = crc32c::crc32c(&bytes[..52]);
        bytes[52..].copy_from_slice(&crc.to_le_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let parse = || {
            check_frame(bytes, 56, 56, b"BLOPCT01")?;
            let kind = CursorKind::decode(bytes[10])?;
            let database_id = bytes[12..28].try_into().unwrap();
            let cursor_namespace = bytes[28..44].try_into().unwrap();
            let cursor_id = u64::from_le_bytes(bytes[44..52].try_into().unwrap());
            if bytes[11] != 0
                || database_id == [0; 16]
                || cursor_namespace == [0; 16]
                || cursor_id == 0
                || cursor_id == u64::MAX
            {
                return Err(Error::InvalidFormat("invalid cursor token fields"));
            }
            Ok(Self {
                database_id,
                cursor_namespace,
                cursor_id,
                kind,
            })
        };
        parse().map_err(|error| match error {
            Error::InvalidFormat(reason) => Error::InvalidToken(reason),
            other => other,
        })
    }
}

/// Store this payload in the same atomic durable commit as derived data, then
/// acknowledge it. Sequence zero denotes the empty database state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Watermark {
    database_id: [u8; 16],
    sequence: u64,
}

impl Watermark {
    pub fn new(
        database_id: [u8; 16],
        sequence: u64,
    ) -> Result<Self> {
        if database_id == [0; 16] || sequence == u64::MAX {
            return Err(Error::InvalidFormat(
                "invalid watermark identity or sequence",
            ));
        }
        Ok(Self {
            database_id,
            sequence,
        })
    }

    pub fn database_id(&self) -> [u8; 16] {
        self.database_id
    }

    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    pub fn encode(&self) -> [u8; 40] {
        let mut bytes = [0; 40];
        bytes[..8].copy_from_slice(b"BLOPWM01");
        bytes[8..10].copy_from_slice(&1_u16.to_le_bytes());
        bytes[12..28].copy_from_slice(&self.database_id);
        bytes[28..36].copy_from_slice(&self.sequence.to_le_bytes());
        let crc = crc32c::crc32c(&bytes[..36]);
        bytes[36..].copy_from_slice(&crc.to_le_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        check_frame(bytes, 40, 40, b"BLOPWM01")?;
        if bytes[10..12] != [0; 2] {
            return Err(Error::InvalidFormat("nonzero watermark reserved field"));
        }
        Self::new(
            bytes[12..28].try_into().unwrap(),
            u64::from_le_bytes(bytes[28..36].try_into().unwrap()),
        )
    }

    pub fn to_hex(&self) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut text = String::with_capacity(80);
        for byte in self.encode() {
            text.push(HEX[(byte >> 4) as usize] as char);
            text.push(HEX[(byte & 15) as usize] as char);
        }
        text
    }

    pub fn from_hex(text: &str) -> Result<Self> {
        if text.len() != 80 {
            return Err(Error::InvalidFormat(
                "watermark text must be 80 lowercase hex bytes",
            ));
        }
        let mut bytes = [0; 40];
        for (pair, byte) in text.as_bytes().as_chunks::<2>().0.iter().zip(&mut bytes) {
            for digit in pair {
                let nibble = match digit {
                    b'0'..=b'9' => digit - b'0',
                    b'a'..=b'f' => digit - b'a' + 10,
                    _ => return Err(Error::InvalidFormat("noncanonical watermark hex")),
                };
                *byte = (*byte << 4) | nibble;
            }
        }
        Self::decode(&bytes)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalRecord {
    pub log_record: Vec<u8>,
    pub outcome: OutcomeRecord,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FeedRecords {
    Resolved(Vec<OutcomeRecord>),
    Logical(Vec<LogicalRecord>),
}

/// Complete, consecutive source records. Public construction is checked by
/// `encode`; `decode` validates framing, CRCs, counts, sequences and hash
/// chains. Logical bodies still require historical VM validation before any
/// import.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FeedBatch {
    pub database_id: [u8; 16],
    pub start_exclusive: u64,
    pub end_inclusive: u64,
    pub records: FeedRecords,
}

impl FeedBatch {
    pub fn watermark(&self) -> Result<Watermark> {
        Watermark::new(self.database_id, self.end_inclusive)
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        let (kind, count) = match &self.records {
            FeedRecords::Resolved(records) => (1, records.len()),
            FeedRecords::Logical(records) => (2, records.len()),
        };
        validate_interval(
            self.database_id,
            self.start_exclusive,
            self.end_inclusive,
            count,
        )?;
        let mut bytes = vec![0; 56];
        bytes[..8].copy_from_slice(b"BLOPFE01");
        bytes[8..10].copy_from_slice(&1_u16.to_le_bytes());
        bytes[10] = kind;
        bytes[16..32].copy_from_slice(&self.database_id);
        bytes[32..40].copy_from_slice(&self.start_exclusive.to_le_bytes());
        bytes[40..48].copy_from_slice(&self.end_inclusive.to_le_bytes());
        bytes[48..52].copy_from_slice(&(count as u32).to_le_bytes());
        let mut predecessor = None;
        for index in 0..count {
            let outcome = match &self.records {
                FeedRecords::Resolved(records) => &records[index],
                FeedRecords::Logical(records) => {
                    let record = &records[index];
                    predecessor = Some(validate_log(
                        &record.log_record,
                        &record.outcome,
                        predecessor,
                    )?);
                    append_blob(&mut bytes, &record.log_record)?;
                    &record.outcome
                }
            };
            if outcome.sequence != self.start_exclusive + index as u64 + 1 {
                return Err(Error::InvalidFormat("nonconsecutive feed outcome"));
            }
            append_blob(&mut bytes, &encode_outcome(outcome)?)?;
        }
        let length = bytes.len() + 4;
        bytes[12..16].copy_from_slice(&(length as u32).to_le_bytes());
        bytes.extend_from_slice(&crc32c::crc32c(&bytes).to_le_bytes());
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        check_frame(bytes, EMPTY_BATCH_BYTES, MAX_BATCH_BYTES, b"BLOPFE01")?;
        let kind = bytes[10];
        if !(1..=2).contains(&kind)
            || bytes[11] != 0
            || bytes[52..56] != [0; 4]
            || u32::from_le_bytes(bytes[12..16].try_into().unwrap()) as usize != bytes.len()
        {
            return Err(Error::InvalidFormat("invalid feed header"));
        }
        let database_id = bytes[16..32].try_into().unwrap();
        let start_exclusive = u64::from_le_bytes(bytes[32..40].try_into().unwrap());
        let end_inclusive = u64::from_le_bytes(bytes[40..48].try_into().unwrap());
        let count = u32::from_le_bytes(bytes[48..52].try_into().unwrap()) as usize;
        validate_interval(database_id, start_exclusive, end_inclusive, count)?;
        let mut input = &bytes[56..bytes.len() - 4];
        // The smallest D.3 outcome is 76 bytes; each logical envelope adds 72.
        let minimum = if kind == 1 { 80 } else { 156 };
        if count > input.len() / minimum {
            return Err(Error::InvalidFormat("feed count exceeds available records"));
        }
        let mut resolved = Vec::new();
        let mut logical = Vec::new();
        let mut predecessor = None;
        for index in 0..count {
            let log = if kind == 2 {
                Some(read_blob(&mut input)?)
            } else {
                None
            };
            let outcome = vm::decode_outcome(read_blob(&mut input)?).map_err(codec_error)?;
            if outcome.sequence != start_exclusive + index as u64 + 1 {
                return Err(Error::InvalidFormat("nonconsecutive feed outcome"));
            }
            if let Some(log) = log {
                predecessor = Some(validate_log(log, &outcome, predecessor)?);
                logical.push(LogicalRecord {
                    log_record: log.to_vec(),
                    outcome,
                });
            } else {
                resolved.push(outcome);
            }
        }
        if !input.is_empty() {
            return Err(Error::InvalidFormat("trailing feed bytes"));
        }
        Ok(Self {
            database_id,
            start_exclusive,
            end_inclusive,
            records: if kind == 1 {
                FeedRecords::Resolved(resolved)
            } else {
                FeedRecords::Logical(logical)
            },
        })
    }
}

fn validate_interval(
    id: [u8; 16],
    start: u64,
    end: u64,
    count: usize,
) -> Result<()> {
    if id == [0; 16]
        || end == u64::MAX
        || end.checked_sub(start) != Some(count as u64)
        || count > (MAX_BATCH_BYTES - EMPTY_BATCH_BYTES) / 80
    {
        return Err(Error::InvalidFormat(
            "invalid feed identity, endpoints or count",
        ));
    }
    Ok(())
}

fn check_frame(
    bytes: &[u8],
    min: usize,
    max: usize,
    magic: &[u8; 8],
) -> Result<()> {
    if !(min..=max).contains(&bytes.len()) {
        return Err(Error::InvalidFormat("invalid exchange length"));
    }
    let split = bytes.len() - 4;
    if crc32c::crc32c(&bytes[..split]) != u32::from_le_bytes(bytes[split..].try_into().unwrap()) {
        return Err(Error::InvalidFormat("exchange CRC mismatch"));
    }
    if &bytes[..8] != magic || bytes[8..10] != 1_u16.to_le_bytes() {
        return Err(Error::InvalidFormat(
            "invalid exchange magic or unsupported version",
        ));
    }
    Ok(())
}

pub(super) fn read_blob<'a>(input: &mut &'a [u8]) -> Result<&'a [u8]> {
    let (length, rest) = input
        .split_at_checked(4)
        .ok_or(Error::InvalidFormat("truncated blob length"))?;
    let length = u32::from_le_bytes(length.try_into().unwrap()) as usize;
    let (bytes, rest) = rest
        .split_at_checked(length)
        .ok_or(Error::InvalidFormat("truncated blob"))?;
    *input = rest;
    Ok(bytes)
}

fn append_blob(
    output: &mut Vec<u8>,
    blob: &[u8],
) -> Result<()> {
    if blob.len() > MAX_BATCH_BYTES - 8 || output.len() > MAX_BATCH_BYTES - 8 - blob.len() {
        return Err(Error::InvalidFormat("feed batch exceeds 256 MiB"));
    }
    output.extend_from_slice(&(blob.len() as u32).to_le_bytes());
    output.extend_from_slice(blob);
    Ok(())
}

pub(super) fn codec_error(error: vm::Error) -> Error {
    match error {
        vm::Error::Invalid(reason) => Error::InvalidFormat(reason),
        other => Error::Read(other),
    }
}

pub(super) fn encode_outcome(record: &OutcomeRecord) -> Result<Vec<u8>> {
    vm::encode_outcome(
        record.sequence,
        record.record_digest,
        record.record_kind,
        &record.outcome,
    )
    .map_err(codec_error)
}

fn validate_log(
    bytes: &[u8],
    outcome: &OutcomeRecord,
    predecessor: Option<[u8; 32]>,
) -> Result<[u8; 32]> {
    if !(72..=64 * 1024 * 1024).contains(&bytes.len()) {
        return Err(Error::InvalidFormat("invalid logical record length"));
    }
    let length = bytes.len();
    if crc32c::crc32c(&bytes[..length - 8])
        != u32::from_le_bytes(bytes[length - 8..length - 4].try_into().unwrap())
    {
        return Err(Error::InvalidFormat("logical record CRC mismatch"));
    }
    if &bytes[..4] != b"BLR1"
        || bytes[4..6] != 64_u16.to_le_bytes()
        || bytes[6..8] != 1_u16.to_le_bytes()
        || u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize != length
        || u32::from_le_bytes(bytes[length - 4..].try_into().unwrap()) as usize != length
        || u32::from_le_bytes(bytes[12..16].try_into().unwrap()) as usize != length - 72
        || u64::from_le_bytes(bytes[16..24].try_into().unwrap()) != outcome.sequence
        || bytes[24] != outcome.record_kind
        || !(1..=3).contains(&bytes[24])
        || bytes[25..28] != [0; 3]
        || bytes[60..64] != [0; 4]
        || predecessor.is_some_and(|digest| bytes[28..60] != digest)
    {
        return Err(Error::InvalidFormat(
            "invalid logical envelope or hash chain",
        ));
    }
    let digest: [u8; 32] = Sha256::digest(bytes).into();
    if digest != outcome.record_digest {
        return Err(Error::InvalidFormat(
            "logical record outcome digest mismatch",
        ));
    }
    Ok(digest)
}
