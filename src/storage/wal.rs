//! Read version 1 segments and frame complete WAL commit groups.
//!
//! Group layout is local; it does not change canonical records or their
//! digests.

use std::fs::File;
use std::io::BufReader;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Take;

use sha2::Digest;
use sha2::Sha256;

use super::Error;
use super::Manifest;
use super::Result;
use super::SegmentDescriptor;
use super::metadata;

pub(super) const SEGMENT_BYTES: u64 = 96;
pub(super) const HEADER_BYTES: usize = 112;
pub(super) const TRAILER_BYTES: usize = 56;
pub(super) const GROUP_OVERHEAD: u64 = (HEADER_BYTES + TRAILER_BYTES) as u64;
pub(super) const MAX_RECORD_BYTES: u64 = 64 * 1024 * 1024;
pub(super) const MAX_GROUP_RECORDS: usize = 64;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct GroupHeader {
    pub bytes: u64,
    pub first: u64,
    pub count: u32,
    pub predecessor: [u8; 32],
    pub last_digest: [u8; 32],
}

fn valid_group(header: &GroupHeader) -> bool {
    (1..=MAX_GROUP_RECORDS as u32).contains(&header.count)
        && header.first != 0
        && header.first.checked_add(u64::from(header.count)).is_some()
        && header.bytes >= GROUP_OVERHEAD + u64::from(header.count) * 72
        && header.bytes <= GROUP_OVERHEAD + u64::from(header.count) * MAX_RECORD_BYTES
}

impl GroupHeader {
    pub(super) fn encode(&self) -> Result<[u8; HEADER_BYTES]> {
        if !valid_group(self) {
            return Err(Error::InvalidInput("invalid WAL group bounds"));
        }
        let mut bytes = [0; HEADER_BYTES];
        bytes[..8].copy_from_slice(b"BLOPWG01");
        bytes[8..10].copy_from_slice(&1_u16.to_le_bytes());
        bytes[12..16].copy_from_slice(&(HEADER_BYTES as u32).to_le_bytes());
        bytes[16..24].copy_from_slice(&self.bytes.to_le_bytes());
        bytes[24..32].copy_from_slice(&self.first.to_le_bytes());
        bytes[32..36].copy_from_slice(&self.count.to_le_bytes());
        bytes[40..72].copy_from_slice(&self.predecessor);
        bytes[72..104].copy_from_slice(&self.last_digest);
        let crc = crc32c::crc32c(&bytes[..108]);
        bytes[108..].copy_from_slice(&crc.to_le_bytes());
        Ok(bytes)
    }

    pub(super) fn decode(bytes: &[u8; HEADER_BYTES]) -> Result<Self> {
        if crc32c::crc32c(&bytes[..108]) != u32_at(bytes, 108) {
            return Err(Error::Corrupt("WAL group header CRC mismatch"));
        }
        if &bytes[..8] != b"BLOPWG01" {
            return Err(Error::Corrupt("invalid WAL group magic"));
        }
        let version = u16::from_le_bytes(bytes[8..10].try_into().unwrap());
        if version != 1 {
            return Err(Error::Unsupported {
                format: "WAL group",
                version,
            });
        }
        if bytes[10..12] != [0; 2]
            || u32_at(bytes, 12) != HEADER_BYTES as u32
            || bytes[36..40] != [0; 4]
            || bytes[104..108] != [0; 4]
        {
            return Err(Error::Corrupt("invalid WAL group header fields"));
        }
        let header = Self {
            bytes: u64_at(bytes, 16),
            first: u64_at(bytes, 24),
            count: u32_at(bytes, 32),
            predecessor: bytes[40..72].try_into().unwrap(),
            last_digest: bytes[72..104].try_into().unwrap(),
        };
        if !valid_group(&header) {
            return Err(Error::Corrupt("invalid WAL group bounds"));
        }
        Ok(header)
    }
}

pub(super) fn trailer(
    length: u64,
    digest: [u8; 32],
) -> [u8; TRAILER_BYTES] {
    let mut bytes = [0; TRAILER_BYTES];
    bytes[..8].copy_from_slice(b"BLOPGE01");
    bytes[8..16].copy_from_slice(&length.to_le_bytes());
    bytes[16..48].copy_from_slice(&digest);
    let crc = crc32c::crc32c(&bytes[..52]);
    bytes[52..].copy_from_slice(&crc.to_le_bytes());
    bytes
}

pub(super) fn segment_header(
    database: [u8; 16],
    segment: &SegmentDescriptor,
) -> [u8; 96] {
    let mut bytes = [0; 96];
    bytes[..8].copy_from_slice(b"BLOPLG01");
    bytes[8..10].copy_from_slice(&1_u16.to_le_bytes());
    bytes[10..12].copy_from_slice(&96_u16.to_le_bytes());
    bytes[16..32].copy_from_slice(&database);
    bytes[32..40].copy_from_slice(&segment.segment_id.to_le_bytes());
    bytes[40..48].copy_from_slice(&segment.first_sequence.to_le_bytes());
    bytes[48..80].copy_from_slice(&segment.predecessor_digest);
    let crc = crc32c::crc32c(&bytes);
    bytes[92..].copy_from_slice(&crc.to_le_bytes());
    bytes
}

pub(crate) struct Record {
    pub sequence: u64,
    pub digest: [u8; 32],
    pub bytes: Vec<u8>,
}

struct Group {
    header: GroupHeader,
    remaining: u64,
    records: u32,
    hash: Sha256,
}

/// Read records only within the specified physical prefix.
///
/// Before execution, the caller must establish durability for each record's
/// group. Full-prefix validation consumes the entire iterator before publishing
/// or replaying the prefix.
pub(crate) struct Records<R> {
    reader: BufReader<Take<R>>,
    remaining: u64,
    sequence: u64,
    predecessor: [u8; 32],
    last: u64,
    last_digest: [u8; 32],
    group: Option<Group>,
    done: bool,
}

impl<R: Read + Seek> Records<R> {
    pub(crate) fn new(
        file: R,
        database: [u8; 16],
        segment: &SegmentDescriptor,
    ) -> Result<Self> {
        Self::suffix(
            file,
            database,
            segment,
            SEGMENT_BYTES,
            segment.first_sequence,
            segment.predecessor_digest,
        )
    }

    pub(super) fn suffix(
        mut file: R,
        database: [u8; 16],
        segment: &SegmentDescriptor,
        offset: u64,
        first: u64,
        predecessor: [u8; 32],
    ) -> Result<Self> {
        metadata::validate_descriptor(segment).map_err(Error::Corrupt)?;
        file.seek(SeekFrom::Start(0))?;
        let mut header = [0; 96];
        metadata::read_committed(&mut file, &mut header)?;
        metadata::validate_segment_header(&header, &database, segment)?;
        if offset < SEGMENT_BYTES
            || offset > segment.committed_bytes
            || first < segment.first_sequence
            || first > segment.last_sequence
        {
            return Err(Error::Corrupt("invalid log reader prefix"));
        }
        file.seek(SeekFrom::Start(offset))?;
        let remaining = segment.committed_bytes - offset;
        Ok(Self {
            reader: BufReader::new(file.take(remaining)),
            remaining,
            sequence: first,
            predecessor,
            last: segment.last_sequence,
            last_digest: segment.last_digest,
            group: None,
            done: false,
        })
    }
}

impl<R: Read> Iterator for Records<R> {
    type Item = Result<Record>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let result = next_record(self);
        if result.is_err() || self.sequence > self.last {
            self.done = true;
        }
        Some(result)
    }
}

impl<R: Read> std::iter::FusedIterator for Records<R> {}

fn next_record<R: Read>(records: &mut Records<R>) -> Result<Record> {
    if records.group.is_none() {
        if records.remaining < GROUP_OVERHEAD {
            return Err(Error::Corrupt("truncated committed WAL group"));
        }
        let mut bytes = [0; HEADER_BYTES];
        metadata::read_committed(&mut records.reader, &mut bytes)?;
        let header = GroupHeader::decode(&bytes)?;
        if header.bytes > records.remaining
            || header.first != records.sequence
            || header.predecessor != records.predecessor
            || header.first + u64::from(header.count) - 1 > records.last
        {
            return Err(Error::Corrupt("WAL group does not match committed prefix"));
        }
        records.remaining -= HEADER_BYTES as u64;
        let mut hash = Sha256::new();
        hash.update(bytes);
        records.group = Some(Group {
            remaining: header.bytes - GROUP_OVERHEAD,
            records: header.count,
            header,
            hash,
        });
    }
    let remaining = records
        .group
        .as_ref()
        .expect("group header precedes its records")
        .remaining;
    let record = read_canonical(
        &mut records.reader,
        remaining,
        records.sequence,
        records.predecessor,
    )?;
    records.remaining -= record.bytes.len() as u64;
    records.sequence += 1;
    records.predecessor = record.digest;
    let group = records
        .group
        .as_mut()
        .expect("group header precedes its records");
    group.hash.update(&record.bytes);
    group.remaining -= record.bytes.len() as u64;
    group.records -= 1;
    if group.records == 0 {
        let group = records.group.take().unwrap();
        if group.remaining != 0 || record.digest != group.header.last_digest {
            return Err(Error::Corrupt("WAL group endpoint mismatch"));
        }
        let mut bytes = [0; TRAILER_BYTES];
        metadata::read_committed(&mut records.reader, &mut bytes)?;
        if bytes != trailer(group.header.bytes, group.hash.finalize().into()) {
            return Err(Error::Corrupt("WAL group trailer or digest mismatch"));
        }
        records.remaining -= TRAILER_BYTES as u64;
    }
    if records.sequence > records.last {
        if records.remaining != 0 || records.group.is_some() {
            return Err(Error::Corrupt("excess bytes inside committed log prefix"));
        }
        if record.digest != records.last_digest {
            return Err(Error::Corrupt("log final digest mismatch"));
        }
    }
    Ok(record)
}

pub(crate) fn read_canonical(
    reader: &mut impl Read,
    remaining: u64,
    sequence: u64,
    predecessor: [u8; 32],
) -> Result<Record> {
    if remaining < 72 {
        return Err(Error::Corrupt("record crosses committed log prefix"));
    }
    let mut header = [0; 64];
    metadata::read_committed(reader, &mut header)?;
    let length = u64::from(u32_at(&header, 8));
    if !(72..=MAX_RECORD_BYTES).contains(&length) || length > remaining {
        return Err(Error::Corrupt("invalid log record length"));
    }
    let mut bytes = vec![0; length as usize];
    bytes[..64].copy_from_slice(&header);
    metadata::read_committed(reader, &mut bytes[64..])?;
    let (_, digest) =
        metadata::validate_record(&mut bytes.as_slice(), length, sequence, predecessor)?;
    Ok(Record {
        sequence,
        digest,
        bytes,
    })
}

pub(super) fn validate_segment(
    file: File,
    manifest: &Manifest,
    segment: &SegmentDescriptor,
) -> Result<()> {
    for record in Records::new(file, manifest.database_id, segment)? {
        let record = record?;
        if record.sequence == manifest.checkpoint_sequence
            && record.digest != manifest.checkpoint_digest
        {
            return Err(Error::Corrupt("log checkpoint digest mismatch"));
        }
    }
    Ok(())
}

pub(super) fn u64_at(
    bytes: &[u8],
    offset: usize,
) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

fn u32_at(
    bytes: &[u8],
    offset: usize,
) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) fn frame(
        first: u64,
        predecessor: [u8; 32],
        records: &[&[u8]],
    ) -> Vec<u8> {
        let length = GROUP_OVERHEAD
            + records
                .iter()
                .map(|record| record.len() as u64)
                .sum::<u64>();
        let header = GroupHeader {
            bytes: length,
            first,
            count: records.len() as u32,
            predecessor,
            last_digest: Sha256::digest(records.last().unwrap()).into(),
        }
        .encode()
        .unwrap();
        let mut bytes = header.to_vec();
        for record in records {
            bytes.extend_from_slice(record);
        }
        bytes.extend_from_slice(&trailer(length, Sha256::digest(&bytes).into()));
        bytes
    }

    pub(crate) fn single(record: &[u8]) -> Vec<u8> {
        frame(
            u64_at(record, 16),
            record[28..60].try_into().unwrap(),
            &[record],
        )
    }

    pub(crate) fn record(
        sequence: u64,
        predecessor: [u8; 32],
    ) -> Vec<u8> {
        let body = super::super::LimitPolicy::try_from(crate::Limits::default())
            .unwrap()
            .encode();
        let length = 72 + body.len();
        let mut bytes = vec![0; 64];
        bytes[..4].copy_from_slice(b"BLR1");
        bytes[4..6].copy_from_slice(&64_u16.to_le_bytes());
        bytes[6..8].copy_from_slice(&1_u16.to_le_bytes());
        bytes[8..12].copy_from_slice(&(length as u32).to_le_bytes());
        bytes[12..16].copy_from_slice(&(body.len() as u32).to_le_bytes());
        bytes[16..24].copy_from_slice(&sequence.to_le_bytes());
        bytes[24] = 3;
        bytes[28..60].copy_from_slice(&predecessor);
        bytes.extend_from_slice(&body);
        bytes.extend_from_slice(&crc32c::crc32c(&bytes).to_le_bytes());
        bytes.extend_from_slice(&(length as u32).to_le_bytes());
        bytes
    }

    #[test]
    fn group_header_bounds_versions_crc_and_reserved_fields_are_checked() {
        let header = GroupHeader {
            bytes: GROUP_OVERHEAD + 72,
            first: u64::MAX - 1,
            count: 1,
            predecessor: [1; 32],
            last_digest: [2; 32],
        };
        let original = header.encode().unwrap();
        assert_eq!(GroupHeader::decode(&original).unwrap(), header);
        assert_eq!(&original[..8], b"BLOPWG01");
        for index in 0..HEADER_BYTES {
            let mut bytes = original;
            bytes[index] ^= 1;
            assert!(GroupHeader::decode(&bytes).is_err());
        }
        for (offset, value) in [(10, 1_u32), (12, 0), (32, 0), (32, 65), (36, 1), (104, 1)] {
            let mut bytes = original;
            bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
            let crc = crc32c::crc32c(&bytes[..108]);
            bytes[108..].copy_from_slice(&crc.to_le_bytes());
            assert!(GroupHeader::decode(&bytes).is_err(), "offset {offset}");
        }
        for (first, count) in [(0, 1), (u64::MAX, 1), (u64::MAX - 1, 2), (1, 0), (1, 65)] {
            assert!(
                GroupHeader {
                    first,
                    count,
                    ..header.clone()
                }
                .encode()
                .is_err()
            );
        }
        let mut future = original;
        future[8..10].copy_from_slice(&3_u16.to_le_bytes());
        let crc = crc32c::crc32c(&future[..108]);
        future[108..].copy_from_slice(&crc.to_le_bytes());
        assert!(matches!(
            GroupHeader::decode(&future),
            Err(Error::Unsupported { version: 3, .. })
        ));
    }
}
