//! Version 1 metadata codecs and read-only validation of published log prefixes.
//!
//! These checks do not select a manifest, validate page contents, compare successive
//! publications, or interpret log bodies. Those checks belong to the store and engine.

use std::{
    fs::File,
    io::{self, Read},
    path::Path,
};

use sha2::{Digest, Sha256};

use super::{Error, Result};

const POLICY_LENGTH: usize = 140;
const GENESIS_LENGTH: usize = 180;
const MANIFEST_BASE_LENGTH: usize = 272;
const MAX_MANIFEST_LENGTH: usize = 16 * 1024 * 1024;
const DESCRIPTOR_LENGTH: usize = 96;
const SEGMENT_HEADER_LENGTH: u64 = 96;
const MAX_RECORD_LENGTH: u64 = 64 * 1024 * 1024;
const LIMIT_CEILINGS: [u64; 17] = [
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

/// Validated limits in ascending resource-ID order. Zero disables a resource.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LimitPolicy([u64; 17]);

impl LimitPolicy {
    pub fn new(values: [u64; 17]) -> Result<Self> {
        validate_limits(&values).map_err(Error::InvalidInput)?;
        Ok(Self(values))
    }

    pub fn values(&self) -> &[u64; 17] {
        &self.0
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(POLICY_LENGTH);
        bytes.extend_from_slice(&1_u16.to_le_bytes());
        bytes.extend_from_slice(&0_u16.to_le_bytes());
        for value in self.0 {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != POLICY_LENGTH {
            return Err(Error::Corrupt("invalid limit policy length"));
        }
        check_version(u16_at(bytes, 0), "limit policy")?;
        if u16_at(bytes, 2) != 0 {
            return Err(Error::Corrupt("nonzero limit policy reserved field"));
        }
        let values = std::array::from_fn(|index| u64_at(bytes, 4 + index * 8));
        validate_limits(&values).map_err(Error::Corrupt)?;
        Ok(Self(values))
    }
}

fn validate_limits(values: &[u64; 17]) -> std::result::Result<(), &'static str> {
    if values
        .iter()
        .zip(LIMIT_CEILINGS)
        .any(|(&value, ceiling)| value > ceiling)
    {
        return Err("limit policy exceeds a hard ceiling");
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Genesis {
    pub database_id: [u8; 16],
    pub initial_policy: LimitPolicy,
}

impl Genesis {
    pub fn encode(&self) -> Result<Vec<u8>> {
        if self.database_id == [0; 16] {
            return Err(Error::InvalidInput("zero database ID"));
        }
        let mut bytes = metadata_prefix(b"BLOPGN01", GENESIS_LENGTH);
        bytes.extend_from_slice(&self.database_id);
        bytes.extend_from_slice(&(POLICY_LENGTH as u32).to_le_bytes());
        bytes.extend_from_slice(&self.initial_policy.encode());
        append_crc(&mut bytes);
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        check_metadata(
            bytes,
            b"BLOPGN01",
            "genesis",
            GENESIS_LENGTH,
            GENESIS_LENGTH,
        )?;
        if u32_at(bytes, 32) != POLICY_LENGTH as u32 {
            return Err(Error::Corrupt("invalid genesis policy blob length"));
        }
        let database_id = array_at(bytes, 16);
        if database_id == [0; 16] {
            return Err(Error::Corrupt("zero database ID"));
        }
        Ok(Self {
            database_id,
            initial_policy: LimitPolicy::decode(&bytes[36..176])?,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentDescriptor {
    pub segment_id: u64,
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub committed_bytes: u64,
    pub predecessor_digest: [u8; 32],
    pub last_digest: [u8; 32],
}

impl SegmentDescriptor {
    pub fn encode(&self) -> Result<Vec<u8>> {
        validate_descriptor(self).map_err(Error::InvalidInput)?;
        let mut bytes = Vec::with_capacity(DESCRIPTOR_LENGTH);
        append_descriptor(&mut bytes, self);
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != DESCRIPTOR_LENGTH {
            return Err(Error::Corrupt("invalid segment descriptor length"));
        }
        let descriptor = Self {
            segment_id: u64_at(bytes, 0),
            first_sequence: u64_at(bytes, 8),
            last_sequence: u64_at(bytes, 16),
            committed_bytes: u64_at(bytes, 24),
            predecessor_digest: array_at(bytes, 32),
            last_digest: array_at(bytes, 64),
        };
        validate_descriptor(&descriptor).map_err(Error::Corrupt)?;
        Ok(descriptor)
    }
}

fn validate_descriptor(segment: &SegmentDescriptor) -> std::result::Result<(), &'static str> {
    if !valid_id(segment.segment_id) {
        return Err("invalid segment ID");
    }
    if !valid_id(segment.first_sequence)
        || !valid_id(segment.last_sequence)
        || segment.first_sequence > segment.last_sequence
    {
        return Err("invalid segment sequence range");
    }
    let records = u128::from(segment.last_sequence - segment.first_sequence + 1);
    let Some(payload) = segment.committed_bytes.checked_sub(SEGMENT_HEADER_LENGTH) else {
        return Err("segment prefix is shorter than its header");
    };
    if u128::from(payload) < records * 72
        || u128::from(payload) > records * u128::from(MAX_RECORD_LENGTH)
    {
        return Err("segment prefix length cannot contain its record range");
    }
    Ok(())
}

fn append_descriptor(bytes: &mut Vec<u8>, segment: &SegmentDescriptor) {
    for value in [
        segment.segment_id,
        segment.first_sequence,
        segment.last_sequence,
        segment.committed_bytes,
    ] {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes.extend_from_slice(&segment.predecessor_digest);
    bytes.extend_from_slice(&segment.last_digest);
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Manifest {
    pub database_id: [u8; 16],
    pub genesis_digest: [u8; 32],
    pub cursor_namespace: [u8; 16],
    pub generation: u64,
    pub page_file_id: u64,
    pub page_count: u64,
    pub checkpoint_sequence: u64,
    pub checkpoint_digest: [u8; 32],
    pub durable_sequence: u64,
    pub durable_digest: [u8; 32],
    pub history_floor: u64,
    pub log_floor: u64,
    pub next_cursor_id: u64,
    pub next_segment_id: u64,
    pub next_page_file_id: u64,
    pub roots: [u64; 5],
    pub segments: Vec<SegmentDescriptor>,
}

impl Manifest {
    pub fn encode(&self) -> Result<Vec<u8>> {
        validate_manifest(self).map_err(Error::InvalidInput)?;
        let length = MANIFEST_BASE_LENGTH + self.segments.len() * DESCRIPTOR_LENGTH;
        let mut bytes = metadata_prefix(b"BLOPMF01", length);
        bytes.extend_from_slice(&self.database_id);
        bytes.extend_from_slice(&self.genesis_digest);
        bytes.extend_from_slice(&self.cursor_namespace);
        for value in [
            self.generation,
            self.page_file_id,
            self.page_count,
            self.checkpoint_sequence,
        ] {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        bytes.extend_from_slice(&self.checkpoint_digest);
        bytes.extend_from_slice(&self.durable_sequence.to_le_bytes());
        bytes.extend_from_slice(&self.durable_digest);
        for value in [
            self.history_floor,
            self.log_floor,
            self.next_cursor_id,
            self.next_segment_id,
            self.next_page_file_id,
        ] {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        for root in self.roots {
            bytes.extend_from_slice(&root.to_le_bytes());
        }
        bytes.extend_from_slice(&(self.segments.len() as u32).to_le_bytes());
        for segment in &self.segments {
            append_descriptor(&mut bytes, segment);
        }
        append_crc(&mut bytes);
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        check_metadata(
            bytes,
            b"BLOPMF01",
            "manifest",
            MANIFEST_BASE_LENGTH,
            MAX_MANIFEST_LENGTH,
        )?;
        let count = u32_at(bytes, 264) as usize;
        let length = count
            .checked_mul(DESCRIPTOR_LENGTH)
            .and_then(|length| length.checked_add(MANIFEST_BASE_LENGTH));
        if length != Some(bytes.len()) {
            return Err(Error::Corrupt("invalid manifest segment vector length"));
        }
        // The complete vector fits the checked enclosing length before allocation.
        let (descriptors, _) = bytes[268..bytes.len() - 4].as_chunks::<DESCRIPTOR_LENGTH>();
        let mut segments = Vec::with_capacity(count);
        for bytes in descriptors {
            segments.push(SegmentDescriptor::decode(bytes)?);
        }
        let manifest = Self {
            database_id: array_at(bytes, 16),
            genesis_digest: array_at(bytes, 32),
            cursor_namespace: array_at(bytes, 64),
            generation: u64_at(bytes, 80),
            page_file_id: u64_at(bytes, 88),
            page_count: u64_at(bytes, 96),
            checkpoint_sequence: u64_at(bytes, 104),
            checkpoint_digest: array_at(bytes, 112),
            durable_sequence: u64_at(bytes, 144),
            durable_digest: array_at(bytes, 152),
            history_floor: u64_at(bytes, 184),
            log_floor: u64_at(bytes, 192),
            next_cursor_id: u64_at(bytes, 200),
            next_segment_id: u64_at(bytes, 208),
            next_page_file_id: u64_at(bytes, 216),
            roots: std::array::from_fn(|index| u64_at(bytes, 224 + index * 8)),
            segments,
        };
        validate_manifest(&manifest).map_err(Error::Corrupt)?;
        Ok(manifest)
    }
}

fn validate_manifest(manifest: &Manifest) -> std::result::Result<(), &'static str> {
    if manifest.segments.len() > (MAX_MANIFEST_LENGTH - MANIFEST_BASE_LENGTH) / DESCRIPTOR_LENGTH {
        return Err("manifest exceeds the hard size ceiling");
    }
    if manifest.database_id == [0; 16] || manifest.cursor_namespace == [0; 16] {
        return Err("zero database ID or cursor namespace");
    }
    if !valid_id(manifest.generation) || !valid_id(manifest.page_file_id) {
        return Err("invalid manifest generation or page file ID");
    }
    if manifest.next_cursor_id == 0
        || manifest.next_segment_id == 0
        || manifest.next_page_file_id <= manifest.page_file_id
    {
        return Err("invalid next ID");
    }
    if manifest.page_count == 0 || manifest.page_count > 1 << 48 {
        return Err("invalid page count");
    }
    if manifest.roots[2] == 0
        || manifest
            .roots
            .iter()
            .any(|&root| root >= manifest.page_count)
    {
        return Err("invalid tree root");
    }
    for (index, &root) in manifest.roots.iter().enumerate() {
        if root != 0 && manifest.roots[..index].contains(&root) {
            return Err("different system trees share a root");
        }
    }
    if manifest.durable_sequence == u64::MAX
        || manifest.history_floor > manifest.checkpoint_sequence
        || manifest.checkpoint_sequence > manifest.durable_sequence
    {
        return Err("invalid manifest sequence frontiers");
    }
    if manifest.log_floor == 0 || manifest.log_floor > manifest.checkpoint_sequence + 1 {
        return Err("invalid log floor");
    }
    if (manifest.checkpoint_sequence == 0 && manifest.checkpoint_digest != manifest.genesis_digest)
        || (manifest.durable_sequence == 0 && manifest.durable_digest != manifest.genesis_digest)
        || (manifest.checkpoint_sequence == manifest.durable_sequence
            && manifest.checkpoint_digest != manifest.durable_digest)
    {
        return Err("inconsistent frontier digests");
    }
    if manifest.segments.is_empty() {
        if manifest.durable_sequence != manifest.checkpoint_sequence
            || manifest.log_floor != manifest.durable_sequence + 1
        {
            return Err("empty log does not match manifest frontiers");
        }
        return Ok(());
    }

    let mut next_sequence = manifest.log_floor;
    let mut previous_id = 0;
    let mut predecessor = if manifest.log_floor == 1 {
        Some(manifest.genesis_digest)
    } else if manifest.log_floor == manifest.checkpoint_sequence + 1 {
        Some(manifest.checkpoint_digest)
    } else {
        None
    };
    for segment in &manifest.segments {
        validate_descriptor(segment)?;
        if segment.segment_id <= previous_id || segment.segment_id >= manifest.next_segment_id {
            return Err("segment IDs are not increasing below the next ID");
        }
        if segment.first_sequence != next_sequence {
            return Err("segment sequence gap or overlap");
        }
        if predecessor.is_some_and(|digest| digest != segment.predecessor_digest) {
            return Err("segment predecessor digest mismatch");
        }
        if (segment.first_sequence == manifest.checkpoint_sequence + 1
            && segment.predecessor_digest != manifest.checkpoint_digest)
            || (segment.last_sequence == manifest.checkpoint_sequence
                && segment.last_digest != manifest.checkpoint_digest)
        {
            return Err("segment checkpoint digest mismatch");
        }
        previous_id = segment.segment_id;
        next_sequence = segment.last_sequence + 1;
        predecessor = Some(segment.last_digest);
    }
    if next_sequence != manifest.durable_sequence + 1
        || predecessor != Some(manifest.durable_digest)
    {
        return Err("segments do not end at the durable frontier");
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Current {
    pub generation: u64,
    pub digest: [u8; 32],
}

impl Current {
    pub fn encode(&self) -> Result<[u8; 64]> {
        if !valid_id(self.generation) {
            return Err(Error::InvalidInput("invalid current manifest generation"));
        }
        let mut bytes = [0; 64];
        bytes[..8].copy_from_slice(b"BLOPCU01");
        bytes[8..10].copy_from_slice(&1_u16.to_le_bytes());
        bytes[12..16].copy_from_slice(&64_u32.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.generation.to_le_bytes());
        bytes[24..56].copy_from_slice(&self.digest);
        let crc = crc32c::crc32c(&bytes[..60]);
        bytes[60..].copy_from_slice(&crc.to_le_bytes());
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        check_metadata(bytes, b"BLOPCU01", "current", 64, 64)?;
        if u32_at(bytes, 56) != 0 {
            return Err(Error::Corrupt("nonzero current reserved field"));
        }
        let generation = u64_at(bytes, 16);
        if !valid_id(generation) {
            return Err(Error::Corrupt("invalid current manifest generation"));
        }
        Ok(Self {
            generation,
            digest: array_at(bytes, 24),
        })
    }
}

fn valid_id(id: u64) -> bool {
    id != 0 && id != u64::MAX
}

fn check_version(version: u16, format: &'static str) -> Result<()> {
    if version != 1 {
        return Err(Error::Unsupported { format, version });
    }
    Ok(())
}

fn metadata_prefix(magic: &[u8; 8], length: usize) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(length);
    bytes.extend_from_slice(magic);
    bytes.extend_from_slice(&1_u16.to_le_bytes());
    bytes.extend_from_slice(&0_u16.to_le_bytes());
    bytes.extend_from_slice(&(length as u32).to_le_bytes());
    bytes
}

fn append_crc(bytes: &mut Vec<u8>) {
    let crc = crc32c::crc32c(bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());
}

fn check_metadata(
    bytes: &[u8],
    magic: &[u8; 8],
    format: &'static str,
    minimum: usize,
    maximum: usize,
) -> Result<()> {
    if !(minimum..=maximum).contains(&bytes.len()) {
        return Err(Error::Corrupt("invalid metadata length"));
    }
    if crc32c::crc32c(&bytes[..bytes.len() - 4]) != u32_at(bytes, bytes.len() - 4) {
        return Err(Error::Corrupt("metadata CRC mismatch"));
    }
    if &bytes[..8] != magic {
        return Err(Error::Corrupt("invalid metadata magic"));
    }
    check_version(u16_at(bytes, 8), format)?;
    if u16_at(bytes, 10) != 0 {
        return Err(Error::Corrupt("nonzero metadata flags"));
    }
    if u32_at(bytes, 12) as usize != bytes.len() {
        return Err(Error::Corrupt("metadata length mismatch"));
    }
    Ok(())
}

// All callers check the enclosing fixed layout before reading fields at fixed offsets.
fn array_at<const N: usize>(bytes: &[u8], offset: usize) -> [u8; N] {
    bytes[offset..offset + N]
        .try_into()
        .expect("validated field bounds")
}

fn u16_at(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(array_at(bytes, offset))
}

fn u32_at(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(array_at(bytes, offset))
}

fn u64_at(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(array_at(bytes, offset))
}

/// Validate only manifest-authoritative log bytes without changing the directory.
///
/// Bodies remain opaque, except for the fixed SetLimits body length. This checks
/// framing and integrity, not transaction, catalogue, or policy body semantics.
pub fn validate_logs(directory: &Path, manifest: &Manifest) -> Result<()> {
    validate_manifest(manifest).map_err(Error::Corrupt)?;
    for segment in &manifest.segments {
        let path = directory.join(format!("log-{:020}.bin", segment.segment_id));
        let file = File::open(path).map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                Error::Corrupt("missing authoritative log segment")
            } else {
                Error::Io(error)
            }
        })?;
        // A bounded reader must not even prefetch bytes from an uncommitted tail.
        let mut reader = file.take(segment.committed_bytes);
        let mut header = [0; SEGMENT_HEADER_LENGTH as usize];
        read_committed(&mut reader, &mut header)?;
        validate_segment_header(&header, &manifest.database_id, segment)?;
        let mut remaining = segment.committed_bytes - SEGMENT_HEADER_LENGTH;
        let mut predecessor = segment.predecessor_digest;
        for sequence in segment.first_sequence..=segment.last_sequence {
            let (length, digest) = validate_record(&mut reader, remaining, sequence, predecessor)?;
            if sequence == manifest.checkpoint_sequence && digest != manifest.checkpoint_digest {
                return Err(Error::Corrupt("log checkpoint digest mismatch"));
            }
            remaining -= length;
            predecessor = digest;
        }
        if remaining != 0 {
            return Err(Error::Corrupt("excess bytes inside committed log prefix"));
        }
        if predecessor != segment.last_digest {
            return Err(Error::Corrupt("log final digest mismatch"));
        }
    }
    Ok(())
}

fn read_committed(reader: &mut impl Read, bytes: &mut [u8]) -> Result<()> {
    reader.read_exact(bytes).map_err(|error| {
        if error.kind() == io::ErrorKind::UnexpectedEof {
            Error::Corrupt("truncated committed log prefix")
        } else {
            Error::Io(error)
        }
    })
}

fn validate_segment_header(
    header: &[u8; 96],
    database_id: &[u8; 16],
    segment: &SegmentDescriptor,
) -> Result<()> {
    let crc = crc32c::crc32c_append(crc32c::crc32c(&header[..92]), &[0; 4]);
    if crc != u32_at(header, 92) {
        return Err(Error::Corrupt("log segment header CRC mismatch"));
    }
    if &header[..8] != b"BLOPLG01" {
        return Err(Error::Corrupt("invalid log segment magic"));
    }
    check_version(u16_at(header, 8), "log segment")?;
    if u16_at(header, 10) != 96 || u32_at(header, 12) != 0 || header[80..92] != [0; 12] {
        return Err(Error::Corrupt("invalid log segment header fields"));
    }
    if &header[16..32] != database_id
        || u64_at(header, 32) != segment.segment_id
        || u64_at(header, 40) != segment.first_sequence
        || header[48..80] != segment.predecessor_digest
    {
        return Err(Error::Corrupt("log segment header disagrees with manifest"));
    }
    Ok(())
}

fn validate_record(
    reader: &mut impl Read,
    remaining: u64,
    sequence: u64,
    predecessor: [u8; 32],
) -> Result<(u64, [u8; 32])> {
    if remaining < 72 {
        return Err(Error::Corrupt("record crosses committed log prefix"));
    }
    let mut header = [0; 64];
    read_committed(reader, &mut header)?;
    let length = u64::from(u32_at(&header, 8));
    let body_length = u64::from(u32_at(&header, 12));
    if length != 72 + body_length || length > MAX_RECORD_LENGTH || length > remaining {
        return Err(Error::Corrupt("invalid log record length"));
    }

    let mut crc = crc32c::crc32c(&header);
    let mut hash = Sha256::new();
    hash.update(header);
    let mut buffer = [0; 8192];
    let mut unread = body_length;
    while unread != 0 {
        let count = unread.min(buffer.len() as u64) as usize;
        let chunk = &mut buffer[..count];
        read_committed(reader, chunk)?;
        crc = crc32c::crc32c_append(crc, chunk);
        hash.update(chunk);
        unread -= count as u64;
    }
    let mut trailer = [0; 8];
    read_committed(reader, &mut trailer)?;
    if crc != u32_at(&trailer, 0) {
        return Err(Error::Corrupt("log record CRC mismatch"));
    }
    if u64::from(u32_at(&trailer, 4)) != length {
        return Err(Error::Corrupt("log record repeated length mismatch"));
    }
    if &header[..4] != b"BLR1" || u16_at(&header, 4) != 64 {
        return Err(Error::Corrupt("invalid log record header"));
    }
    check_version(u16_at(&header, 6), "log record")?;
    if !(1..=3).contains(&header[24]) || header[25..28] != [0; 3] || header[60..64] != [0; 4] {
        return Err(Error::Corrupt(
            "invalid log record kind, flags, or reserved fields",
        ));
    }
    if header[24] == 3 && body_length != POLICY_LENGTH as u64 {
        return Err(Error::Corrupt("invalid SetLimits body length"));
    }
    if u64_at(&header, 16) != sequence || header[28..60] != predecessor {
        return Err(Error::Corrupt(
            "log record sequence or predecessor mismatch",
        ));
    }
    hash.update(trailer);
    Ok((length, hash.finalize().into()))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    fn hash(bytes: &[u8]) -> [u8; 32] {
        Sha256::digest(bytes).into()
    }

    fn put_u64(bytes: &mut [u8], offset: usize, value: u64) {
        bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
    }

    fn rewrite_crc(bytes: &mut [u8]) {
        let end = bytes.len() - 4;
        let crc = crc32c::crc32c(&bytes[..end]);
        bytes[end..].copy_from_slice(&crc.to_le_bytes());
    }

    fn rewrite_record_crc(record: &mut [u8]) {
        let end = record.len() - 8;
        let crc = crc32c::crc32c(&record[..end]);
        record[end..end + 4].copy_from_slice(&crc.to_le_bytes());
    }

    fn rewrite_segment_crc(bytes: &mut [u8]) {
        bytes[92..96].fill(0);
        let crc = crc32c::crc32c(&bytes[..96]);
        bytes[92..96].copy_from_slice(&crc.to_le_bytes());
    }

    fn genesis() -> Genesis {
        Genesis {
            database_id: [0x11; 16],
            initial_policy: LimitPolicy::new([0; 17]).unwrap(),
        }
    }

    fn initial_manifest() -> Manifest {
        let genesis = genesis();
        let genesis_digest = hash(&genesis.encode().unwrap());
        Manifest {
            database_id: genesis.database_id,
            genesis_digest,
            cursor_namespace: [0x22; 16],
            generation: 1,
            page_file_id: 1,
            page_count: 2,
            checkpoint_sequence: 0,
            checkpoint_digest: genesis_digest,
            durable_sequence: 0,
            durable_digest: genesis_digest,
            history_floor: 0,
            log_floor: 1,
            next_cursor_id: 1,
            next_segment_id: 1,
            next_page_file_id: 2,
            roots: [0, 0, 1, 0, 0],
            segments: Vec::new(),
        }
    }

    fn segmented_manifest() -> Manifest {
        Manifest {
            genesis_digest: [0x11; 32],
            generation: 7,
            page_file_id: 8,
            page_count: 6,
            checkpoint_sequence: 2,
            checkpoint_digest: [0x22; 32],
            durable_sequence: 4,
            durable_digest: [0x44; 32],
            history_floor: 1,
            next_cursor_id: 9,
            next_segment_id: 22,
            next_page_file_id: 10,
            roots: [1, 2, 3, 4, 5],
            segments: vec![
                SegmentDescriptor {
                    segment_id: 11,
                    first_sequence: 1,
                    last_sequence: 2,
                    committed_bytes: 240,
                    predecessor_digest: [0x11; 32],
                    last_digest: [0x22; 32],
                },
                SegmentDescriptor {
                    segment_id: 21,
                    first_sequence: 3,
                    last_sequence: 4,
                    committed_bytes: 240,
                    predecessor_digest: [0x22; 32],
                    last_digest: [0x44; 32],
                },
            ],
            ..initial_manifest()
        }
    }

    #[test]
    fn integrity_primitives_match_standard_vectors() {
        assert_eq!(crc32c::crc32c(b"123456789"), 0xe306_9283);
        assert_eq!(crc32c::crc32c(b""), 0);
        for split in 0..=9 {
            assert_eq!(
                crc32c::crc32c_append(
                    crc32c::crc32c(&b"123456789"[..split]),
                    &b"123456789"[split..]
                ),
                0xe306_9283
            );
        }
        assert_eq!(
            hash(b"abc"),
            [
                0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae,
                0x22, 0x23, 0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61,
                0xf2, 0x00, 0x15, 0xad,
            ]
        );
    }

    #[test]
    fn policy_layout_roundtrip_and_all_ceilings() {
        for values in [
            [0; 17],
            LIMIT_CEILINGS,
            std::array::from_fn(|index| index as u64 + 1),
        ] {
            let policy = LimitPolicy::new(values).unwrap();
            let bytes = policy.encode();
            assert_eq!(policy.values(), &values);
            assert_eq!(bytes.len(), 140);
            assert_eq!(&bytes[..4], &[1, 0, 0, 0]);
            for (index, value) in values.iter().enumerate() {
                assert_eq!(&bytes[4 + index * 8..12 + index * 8], &value.to_le_bytes());
            }
            assert_eq!(LimitPolicy::decode(&bytes).unwrap(), policy);
        }
        for index in 0..17 {
            let mut values = LIMIT_CEILINGS;
            values[index] += 1;
            assert!(matches!(
                LimitPolicy::new(values),
                Err(Error::InvalidInput(_))
            ));
            let mut bytes = LimitPolicy::new(LIMIT_CEILINGS).unwrap().encode();
            put_u64(&mut bytes, 4 + index * 8, values[index]);
            assert!(matches!(
                LimitPolicy::decode(&bytes),
                Err(Error::Corrupt(_))
            ));
        }
    }

    #[test]
    fn genesis_golden_bytes_and_complete_file_digest() {
        let mut expected = [0; 180];
        expected[..16].copy_from_slice(b"BLOPGN01\x01\x00\x00\x00\xb4\x00\x00\x00");
        expected[16..32].fill(0x11);
        expected[32..36].copy_from_slice(&140_u32.to_le_bytes());
        expected[36] = 1;
        expected[176..].copy_from_slice(&0xdd86_e588_u32.to_le_bytes());
        assert_eq!(genesis().encode().unwrap(), expected);
        assert_eq!(Genesis::decode(&expected).unwrap(), genesis());
        assert_eq!(
            hash(&expected),
            [
                0xbd, 0x49, 0xb1, 0xbe, 0x42, 0x84, 0x18, 0x27, 0x6d, 0x36, 0x96, 0xbf, 0x09, 0xf2,
                0xc0, 0x49, 0x2f, 0x66, 0x43, 0x44, 0x0a, 0x1a, 0xa8, 0xba, 0x47, 0x13, 0x9d, 0xf0,
                0x35, 0x44, 0x21, 0x5d,
            ]
        );
    }

    #[test]
    fn current_golden_bytes() {
        let current = Current {
            generation: 0x0807_0605_0403_0201,
            digest: std::array::from_fn(|i| i as u8),
        };
        let mut expected = [0; 64];
        expected[..16].copy_from_slice(b"BLOPCU01\x01\x00\x00\x00\x40\x00\x00\x00");
        expected[16..24].copy_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);
        expected[24..56].copy_from_slice(&current.digest);
        expected[60..].copy_from_slice(&0x2afb_73f9_u32.to_le_bytes());
        assert_eq!(current.encode().unwrap(), expected);
        assert_eq!(Current::decode(&expected).unwrap(), current);
    }

    #[test]
    fn manifest_and_descriptor_golden_offsets() {
        let manifest = segmented_manifest();
        let bytes = manifest.encode().unwrap();
        assert_eq!(bytes.len(), 464);
        assert_eq!(&bytes[..16], b"BLOPMF01\x01\x00\x00\x00\xd0\x01\x00\x00");
        assert_eq!(&bytes[16..32], &[0x11; 16]);
        assert_eq!(&bytes[32..64], &[0x11; 32]);
        assert_eq!(&bytes[64..80], &[0x22; 16]);
        assert_eq!(&bytes[112..144], &[0x22; 32]);
        assert_eq!(&bytes[152..184], &[0x44; 32]);
        for (offset, value) in [
            (80, 7_u64),
            (88, 8),
            (96, 6),
            (104, 2),
            (144, 4),
            (184, 1),
            (192, 1),
            (200, 9),
            (208, 22),
            (216, 10),
            (224, 1),
            (232, 2),
            (240, 3),
            (248, 4),
            (256, 5),
        ] {
            assert_eq!(
                &bytes[offset..offset + 8],
                &value.to_le_bytes(),
                "offset {offset}"
            );
        }
        assert_eq!(&bytes[264..268], &2_u32.to_le_bytes());
        let mut descriptor = [0; 96];
        put_u64(&mut descriptor, 0, 11);
        put_u64(&mut descriptor, 8, 1);
        put_u64(&mut descriptor, 16, 2);
        put_u64(&mut descriptor, 24, 240);
        descriptor[32..64].fill(0x11);
        descriptor[64..96].fill(0x22);
        assert_eq!(manifest.segments[0].encode().unwrap(), descriptor);
        assert_eq!(&bytes[268..364], &descriptor);
        assert_eq!(
            SegmentDescriptor::decode(&descriptor).unwrap(),
            manifest.segments[0]
        );
        assert_eq!(&bytes[364..460], manifest.segments[1].encode().unwrap());
        assert_eq!(u32_at(&bytes, 460), crc32c::crc32c(&bytes[..460]));
        assert_eq!(Manifest::decode(&bytes).unwrap(), manifest);
        let initial = initial_manifest();
        assert_eq!(initial.encode().unwrap().len(), 272);
        assert_eq!(
            Manifest::decode(&initial.encode().unwrap()).unwrap(),
            initial
        );
    }

    #[test]
    fn codecs_reject_every_truncation_and_trailing_bytes() {
        let policy = genesis().initial_policy.encode();
        let genesis = genesis().encode().unwrap();
        let manifest = segmented_manifest().encode().unwrap();
        let descriptor = segmented_manifest().segments[0].encode().unwrap();
        let current = Current {
            generation: 7,
            digest: hash(&manifest),
        }
        .encode()
        .unwrap();
        for end in 0..policy.len() {
            assert!(
                LimitPolicy::decode(&policy[..end]).is_err(),
                "policy length {end}"
            );
        }
        for end in 0..genesis.len() {
            assert!(
                Genesis::decode(&genesis[..end]).is_err(),
                "genesis length {end}"
            );
        }
        for end in 0..manifest.len() {
            assert!(
                Manifest::decode(&manifest[..end]).is_err(),
                "manifest length {end}"
            );
        }
        for end in 0..descriptor.len() {
            assert!(
                SegmentDescriptor::decode(&descriptor[..end]).is_err(),
                "descriptor length {end}"
            );
        }
        for end in 0..current.len() {
            assert!(
                Current::decode(&current[..end]).is_err(),
                "current length {end}"
            );
        }
        assert!(LimitPolicy::decode(&[policy, vec![0]].concat()).is_err());
        assert!(Genesis::decode(&[genesis, vec![0]].concat()).is_err());
        assert!(Manifest::decode(&[manifest, vec![0]].concat()).is_err());
        assert!(SegmentDescriptor::decode(&[descriptor, vec![0]].concat()).is_err());
        assert!(Current::decode(&[current.to_vec(), vec![0]].concat()).is_err());
    }

    #[test]
    fn metadata_checks_crc_magic_flags_lengths_and_versions() {
        type Decode = fn(&[u8]) -> Result<()>;
        let decoders: [Decode; 3] = [
            |bytes| Genesis::decode(bytes).map(|_| ()),
            |bytes| Manifest::decode(bytes).map(|_| ()),
            |bytes| Current::decode(bytes).map(|_| ()),
        ];
        let encodings = [
            genesis().encode().unwrap(),
            segmented_manifest().encode().unwrap(),
            Current {
                generation: 1,
                digest: [0; 32],
            }
            .encode()
            .unwrap()
            .to_vec(),
        ];
        for (decode, original) in decoders.into_iter().zip(encodings) {
            for index in 0..original.len() {
                let mut bytes = original.clone();
                bytes[index] ^= 1;
                assert!(
                    matches!(decode(&bytes), Err(Error::Corrupt(_))),
                    "CRC byte {index}"
                );
            }
            for index in (0..8).chain(10..16) {
                let mut bytes = original.clone();
                bytes[index] ^= 1;
                rewrite_crc(&mut bytes);
                assert!(
                    matches!(decode(&bytes), Err(Error::Corrupt(_))),
                    "header byte {index}"
                );
            }
            let mut bytes = original;
            bytes[8..10].copy_from_slice(&2_u16.to_le_bytes());
            rewrite_crc(&mut bytes);
            assert!(matches!(
                decode(&bytes),
                Err(Error::Unsupported { version: 2, .. })
            ));
        }
    }

    #[test]
    fn genesis_and_policy_reject_invalid_nested_fields() {
        for length in [0_u32, 139, 141, u32::MAX] {
            let mut bytes = genesis().encode().unwrap();
            bytes[32..36].copy_from_slice(&length.to_le_bytes());
            rewrite_crc(&mut bytes);
            assert!(matches!(Genesis::decode(&bytes), Err(Error::Corrupt(_))));
        }
        let mut invalid = genesis();
        invalid.database_id = [0; 16];
        assert!(matches!(invalid.encode(), Err(Error::InvalidInput(_))));
        let mut bytes = genesis().encode().unwrap();
        bytes[16..32].fill(0);
        rewrite_crc(&mut bytes);
        assert!(matches!(Genesis::decode(&bytes), Err(Error::Corrupt(_))));
        for index in [38, 39] {
            let mut bytes = genesis().encode().unwrap();
            bytes[index] = 1;
            rewrite_crc(&mut bytes);
            assert!(matches!(Genesis::decode(&bytes), Err(Error::Corrupt(_))));
        }
        let mut bytes = genesis().encode().unwrap();
        bytes[36] = 2;
        rewrite_crc(&mut bytes);
        assert!(matches!(
            Genesis::decode(&bytes),
            Err(Error::Unsupported {
                format: "limit policy",
                version: 2
            })
        ));
    }

    #[test]
    fn current_rejects_reserved_bytes_and_invalid_ids() {
        for generation in [0, u64::MAX] {
            assert!(matches!(
                Current {
                    generation,
                    digest: [0; 32]
                }
                .encode(),
                Err(Error::InvalidInput(_))
            ));
            let mut bytes = Current {
                generation: 1,
                digest: [0; 32],
            }
            .encode()
            .unwrap();
            put_u64(&mut bytes, 16, generation);
            rewrite_crc(&mut bytes);
            assert!(matches!(Current::decode(&bytes), Err(Error::Corrupt(_))));
        }
        for index in 56..60 {
            let mut bytes = Current {
                generation: 1,
                digest: [0; 32],
            }
            .encode()
            .unwrap();
            bytes[index] = 1;
            rewrite_crc(&mut bytes);
            assert!(matches!(Current::decode(&bytes), Err(Error::Corrupt(_))));
        }
    }

    #[test]
    fn manifest_rejects_invalid_ids_frontiers_roots_and_continuity() {
        for (offset, value) in [
            (80, 0),
            (80, u64::MAX),
            (88, 0),
            (88, u64::MAX),
            (96, 0),
            (96, (1 << 48) + 1),
            (96, 5),
            (104, 0),
            (104, 5),
            (104, u64::MAX),
            (144, 1),
            (144, u64::MAX),
            (184, 3),
            (192, 0),
            (192, 2),
            (192, 4),
            (200, 0),
            (208, 0),
            (208, 21),
            (216, 8),
            (224, 6),
            (240, 0),
            (248, 1),
            (268, 0),
            (268, u64::MAX),
            (276, 0),
            (276, 2),
            (284, 0),
            (284, 3),
            (292, 95),
            (292, 96),
            (292, 239),
            (292, 96 + 2 * MAX_RECORD_LENGTH + 1),
            (364, 11),
            (372, 2),
            (372, 4),
            (380, 3),
            (380, 5),
        ] {
            let mut bytes = segmented_manifest().encode().unwrap();
            put_u64(&mut bytes, offset, value);
            rewrite_crc(&mut bytes);
            assert!(
                matches!(Manifest::decode(&bytes), Err(Error::Corrupt(_))),
                "offset {offset}, value {value}"
            );
        }
        for (start, end) in [
            (16, 32),
            (64, 80),
            (112, 144),
            (152, 184),
            (300, 332),
            (332, 364),
            (396, 428),
            (428, 460),
        ] {
            let mut bytes = segmented_manifest().encode().unwrap();
            bytes[start..end].fill(0);
            rewrite_crc(&mut bytes);
            assert!(
                matches!(Manifest::decode(&bytes), Err(Error::Corrupt(_))),
                "field at {start}"
            );
        }
    }

    #[test]
    fn manifest_count_and_size_are_bounded_before_allocation() {
        for count in [0_u32, 1, 3, u32::MAX] {
            let mut bytes = segmented_manifest().encode().unwrap();
            bytes[264..268].copy_from_slice(&count.to_le_bytes());
            rewrite_crc(&mut bytes);
            assert!(matches!(Manifest::decode(&bytes), Err(Error::Corrupt(_))));
        }
        assert!(matches!(
            Manifest::decode(&vec![0; MAX_MANIFEST_LENGTH + 1]),
            Err(Error::Corrupt(_))
        ));
        let mut manifest = segmented_manifest();
        manifest.segments = vec![
            manifest.segments[0];
            (MAX_MANIFEST_LENGTH - MANIFEST_BASE_LENGTH) / DESCRIPTOR_LENGTH
                + 1
        ];
        assert!(matches!(manifest.encode(), Err(Error::InvalidInput(_))));
    }

    #[test]
    fn encoders_validate_public_fields() {
        let mutations: [fn(&mut Manifest); 12] = [
            |m| m.database_id.fill(0),
            |m| m.cursor_namespace.fill(0),
            |m| m.generation = 0,
            |m| m.page_file_id = u64::MAX,
            |m| m.page_count = 0,
            |m| m.roots[2] = 0,
            |m| m.next_cursor_id = 0,
            |m| m.next_segment_id = 21,
            |m| m.history_floor = 3,
            |m| m.checkpoint_digest.fill(0),
            |m| m.segments[1].first_sequence = 4,
            |m| m.segments[1].predecessor_digest.fill(0),
        ];
        for mutate in mutations {
            let mut manifest = segmented_manifest();
            mutate(&mut manifest);
            assert!(matches!(manifest.encode(), Err(Error::InvalidInput(_))));
        }
        for value in [0, u64::MAX] {
            let mut descriptor = segmented_manifest().segments[0];
            descriptor.segment_id = value;
            assert!(matches!(descriptor.encode(), Err(Error::InvalidInput(_))));
            descriptor = segmented_manifest().segments[0];
            descriptor.first_sequence = value;
            assert!(matches!(descriptor.encode(), Err(Error::InvalidInput(_))));
        }
    }

    #[test]
    fn genesis_frontiers_empty_logs_and_exhaustion_sentinels() {
        let mut manifest = initial_manifest();
        manifest.checkpoint_digest[0] ^= 1;
        assert!(manifest.encode().is_err());
        manifest = initial_manifest();
        manifest.durable_digest[0] ^= 1;
        assert!(manifest.encode().is_err());
        manifest = initial_manifest();
        manifest.durable_sequence = 1;
        assert!(manifest.encode().is_err());
        manifest = initial_manifest();
        manifest.checkpoint_sequence = 1;
        manifest.durable_sequence = 1;
        assert!(manifest.encode().is_err());
        manifest.log_floor = 2;
        assert!(manifest.encode().is_ok());

        manifest.checkpoint_sequence = u64::MAX - 1;
        manifest.durable_sequence = u64::MAX - 1;
        manifest.log_floor = u64::MAX;
        manifest.generation = u64::MAX - 1;
        manifest.page_file_id = u64::MAX - 1;
        manifest.next_page_file_id = u64::MAX;
        manifest.next_cursor_id = u64::MAX;
        manifest.next_segment_id = u64::MAX;
        manifest.page_count = 1 << 48;
        manifest.roots[2] = (1 << 48) - 1;
        assert_eq!(
            Manifest::decode(&manifest.encode().unwrap()).unwrap(),
            manifest
        );

        manifest.checkpoint_sequence = u64::MAX - 2;
        manifest.log_floor = u64::MAX - 1;
        manifest.segments.push(SegmentDescriptor {
            segment_id: u64::MAX - 1,
            first_sequence: u64::MAX - 1,
            last_sequence: u64::MAX - 1,
            committed_bytes: 168,
            predecessor_digest: manifest.checkpoint_digest,
            last_digest: manifest.durable_digest,
        });
        assert_eq!(
            Manifest::decode(&manifest.encode().unwrap()).unwrap(),
            manifest
        );
        manifest.segments[0].first_sequence = 1;
        manifest.segments[0].committed_bytes = u64::MAX;
        assert!(manifest.segments[0].encode().is_err());
    }

    fn record(sequence: u64, kind: u8, predecessor: [u8; 32], body: &[u8]) -> Vec<u8> {
        let length = (72 + body.len()) as u32;
        let mut bytes = vec![0; 64];
        bytes[..8].copy_from_slice(b"BLR1\x40\x00\x01\x00");
        bytes[8..12].copy_from_slice(&length.to_le_bytes());
        bytes[12..16].copy_from_slice(&(body.len() as u32).to_le_bytes());
        put_u64(&mut bytes, 16, sequence);
        bytes[24] = kind;
        bytes[28..60].copy_from_slice(&predecessor);
        bytes.extend_from_slice(body);
        append_crc(&mut bytes);
        bytes.extend_from_slice(&length.to_le_bytes());
        bytes
    }

    fn segment(
        database_id: [u8; 16],
        id: u64,
        records: &[Vec<u8>],
    ) -> (SegmentDescriptor, Vec<u8>) {
        let mut bytes = vec![0; 96];
        bytes[..12].copy_from_slice(b"BLOPLG01\x01\x00\x60\x00");
        bytes[16..32].copy_from_slice(&database_id);
        put_u64(&mut bytes, 32, id);
        let first_sequence = u64_at(&records[0], 16);
        put_u64(&mut bytes, 40, first_sequence);
        let predecessor_digest = array_at(&records[0], 28);
        bytes[48..80].copy_from_slice(&predecessor_digest);
        rewrite_segment_crc(&mut bytes);
        for record in records {
            bytes.extend_from_slice(record);
        }
        let last_record = records.last().unwrap();
        let descriptor = SegmentDescriptor {
            segment_id: id,
            first_sequence,
            last_sequence: u64_at(last_record, 16),
            committed_bytes: bytes.len() as u64,
            predecessor_digest,
            last_digest: hash(last_record),
        };
        (descriptor, bytes)
    }

    fn single_log(kind: u8, body: &[u8]) -> (tempfile::TempDir, Manifest, Vec<u8>) {
        let directory = tempfile::tempdir().unwrap();
        let mut manifest = initial_manifest();
        let record = record(1, kind, manifest.genesis_digest, body);
        let (descriptor, bytes) = segment(manifest.database_id, 1, &[record]);
        manifest.durable_sequence = 1;
        manifest.durable_digest = descriptor.last_digest;
        manifest.next_segment_id = 2;
        manifest.segments.push(descriptor);
        fs::write(
            directory.path().join("log-00000000000000000001.bin"),
            &bytes,
        )
        .unwrap();
        (directory, manifest, bytes)
    }

    fn logs() -> (tempfile::TempDir, Manifest, [Vec<u8>; 2]) {
        let directory = tempfile::tempdir().unwrap();
        let mut manifest = initial_manifest();
        let r1 = record(1, 1, manifest.genesis_digest, b"opaque transaction");
        let r2 = record(2, 2, hash(&r1), b"opaque catalogue");
        let r3 = record(
            3,
            3,
            hash(&r2),
            &LimitPolicy::new([0; 17]).unwrap().encode(),
        );
        let r4 = record(4, 1, hash(&r3), &vec![0x59; 16_387]);
        manifest.checkpoint_sequence = 3;
        manifest.checkpoint_digest = hash(&r3);
        manifest.durable_sequence = 4;
        manifest.durable_digest = hash(&r4);
        manifest.next_segment_id = 10;
        let (first, first_bytes) = segment(manifest.database_id, 2, &[r1, r2]);
        let (second, second_bytes) = segment(manifest.database_id, 9, &[r3, r4]);
        manifest.segments = vec![first, second];
        fs::write(
            directory.path().join("log-00000000000000000002.bin"),
            &first_bytes,
        )
        .unwrap();
        fs::write(
            directory.path().join("log-00000000000000000009.bin"),
            &second_bytes,
        )
        .unwrap();
        (directory, manifest, [first_bytes, second_bytes])
    }

    #[test]
    fn log_validation_streams_bodies_and_checks_retained_checkpoint_anchors() {
        let (directory, mut manifest, _) = logs();
        validate_logs(directory.path(), &manifest).unwrap();
        manifest.checkpoint_digest[0] ^= 1;
        assert!(matches!(
            validate_logs(directory.path(), &manifest),
            Err(Error::Corrupt("log checkpoint digest mismatch"))
        ));
        manifest.checkpoint_sequence = 2;
        manifest.checkpoint_digest = manifest.segments[0].last_digest;
        validate_logs(directory.path(), &manifest).unwrap();
        manifest.segments.remove(0);
        manifest.log_floor = 3;
        validate_logs(directory.path(), &manifest).unwrap();
        manifest.checkpoint_digest[0] ^= 1;
        assert!(matches!(
            validate_logs(directory.path(), &manifest),
            Err(Error::Corrupt(_))
        ));
    }

    #[test]
    fn every_missing_committed_byte_is_corruption() {
        let (directory, manifest, bytes) = single_log(1, b"abc");
        let path = directory.path().join("log-00000000000000000001.bin");
        for end in 0..bytes.len() {
            fs::write(&path, &bytes[..end]).unwrap();
            assert!(
                matches!(
                    validate_logs(directory.path(), &manifest),
                    Err(Error::Corrupt(_))
                ),
                "prefix length {end}"
            );
        }
        fs::remove_file(&path).unwrap();
        assert!(matches!(
            validate_logs(directory.path(), &manifest),
            Err(Error::Corrupt("missing authoritative log segment"))
        ));
    }

    #[test]
    fn checksummed_and_corrupt_uncommitted_tails_are_ignored_without_mutation() {
        let (directory, manifest, mut bytes) = single_log(1, b"abc");
        let path = directory.path().join("log-00000000000000000001.bin");
        bytes.extend_from_slice(&record(
            2,
            1,
            manifest.durable_digest,
            b"valid uncommitted record",
        ));
        fs::write(&path, &bytes).unwrap();
        fs::write(
            directory.path().join("log-00000000000000000002.bin"),
            b"corrupt orphan",
        )
        .unwrap();
        validate_logs(directory.path(), &manifest).unwrap();
        assert_eq!(fs::read(&path).unwrap(), bytes);
        bytes[manifest.segments[0].committed_bytes as usize] ^= 1;
        bytes.extend_from_slice(b"incomplete corrupt tail");
        fs::write(&path, &bytes).unwrap();
        validate_logs(directory.path(), &manifest).unwrap();
        assert_eq!(fs::read(&path).unwrap(), bytes);
        bytes[160] ^= 1;
        fs::write(&path, &bytes).unwrap();
        assert!(matches!(
            validate_logs(directory.path(), &manifest),
            Err(Error::Corrupt("log record CRC mismatch"))
        ));
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }

    #[test]
    fn every_segment_header_field_is_validated_even_with_a_valid_crc() {
        let (directory, manifest, original) = single_log(1, b"abc");
        let path = directory.path().join("log-00000000000000000001.bin");
        for index in 0..96 {
            let mut bytes = original.clone();
            bytes[index] ^= 1;
            if index < 92 {
                rewrite_segment_crc(&mut bytes);
            }
            fs::write(&path, bytes).unwrap();
            assert!(
                validate_logs(directory.path(), &manifest).is_err(),
                "header byte {index}"
            );
        }
    }

    #[test]
    fn every_record_header_field_is_validated_even_with_valid_crc_and_final_digest() {
        let (directory, mut manifest, original) = single_log(1, b"abc");
        let path = directory.path().join("log-00000000000000000001.bin");
        for index in 0..64 {
            let mut bytes = original.clone();
            bytes[96 + index] ^= 0x80;
            rewrite_record_crc(&mut bytes[96..]);
            manifest.durable_digest = hash(&bytes[96..]);
            manifest.segments[0].last_digest = manifest.durable_digest;
            fs::write(&path, bytes).unwrap();
            assert!(
                validate_logs(directory.path(), &manifest).is_err(),
                "record header byte {index}"
            );
        }
    }

    #[test]
    fn record_crc_precedes_version_interpretation_and_lengths_do_not_wrap() {
        let (directory, mut manifest, original) = single_log(1, b"abc");
        let path = directory.path().join("log-00000000000000000001.bin");
        let mut bytes = original.clone();
        bytes[102] = 2;
        fs::write(&path, &bytes).unwrap();
        assert!(matches!(
            validate_logs(directory.path(), &manifest),
            Err(Error::Corrupt("log record CRC mismatch"))
        ));
        rewrite_record_crc(&mut bytes[96..]);
        fs::write(&path, &bytes).unwrap();
        assert!(matches!(
            validate_logs(directory.path(), &manifest),
            Err(Error::Unsupported {
                format: "log record",
                version: 2
            })
        ));
        for (length, body_length) in [
            (71_u32, 0_u32),
            (71, u32::MAX),
            (MAX_RECORD_LENGTH as u32 + 1, MAX_RECORD_LENGTH as u32 - 71),
        ] {
            let mut bytes = original.clone();
            bytes[104..108].copy_from_slice(&length.to_le_bytes());
            bytes[108..112].copy_from_slice(&body_length.to_le_bytes());
            rewrite_record_crc(&mut bytes[96..]);
            fs::write(&path, bytes).unwrap();
            assert!(matches!(
                validate_logs(directory.path(), &manifest),
                Err(Error::Corrupt("invalid log record length"))
            ));
        }
        let mut bytes = original;
        let end = bytes.len();
        bytes[end - 4] ^= 1;
        manifest.durable_digest = hash(&bytes[96..]);
        manifest.segments[0].last_digest = manifest.durable_digest;
        fs::write(&path, bytes).unwrap();
        assert!(matches!(
            validate_logs(directory.path(), &manifest),
            Err(Error::Corrupt("log record repeated length mismatch"))
        ));
    }

    #[test]
    fn log_prefix_must_end_exactly_at_descriptor_sequence_and_digest() {
        let (directory, mut manifest, mut bytes) = single_log(1, b"abc");
        let path = directory.path().join("log-00000000000000000001.bin");
        manifest.durable_digest[0] ^= 1;
        manifest.segments[0].last_digest = manifest.durable_digest;
        assert!(matches!(
            validate_logs(directory.path(), &manifest),
            Err(Error::Corrupt("log final digest mismatch"))
        ));
        manifest.durable_digest = hash(&bytes[96..]);
        manifest.segments[0].last_digest = manifest.durable_digest;
        bytes.push(0);
        manifest.segments[0].committed_bytes += 1;
        fs::write(&path, bytes).unwrap();
        assert!(matches!(
            validate_logs(directory.path(), &manifest),
            Err(Error::Corrupt("excess bytes inside committed log prefix"))
        ));
        manifest.segments[0].committed_bytes -= 2;
        assert!(matches!(
            validate_logs(directory.path(), &manifest),
            Err(Error::Corrupt("invalid log record length"))
        ));
    }

    #[test]
    fn set_limits_has_fixed_framing_but_bodies_are_not_semantically_decoded() {
        let (directory, manifest, _) = single_log(3, &[0xff; 140]);
        validate_logs(directory.path(), &manifest).unwrap();
        let (directory, manifest, _) = single_log(3, &[0; 139]);
        assert!(matches!(
            validate_logs(directory.path(), &manifest),
            Err(Error::Corrupt("invalid SetLimits body length"))
        ));
        let directory = tempfile::tempdir().unwrap();
        fs::write(
            directory.path().join("log-00000000000000000001.bin"),
            b"orphan",
        )
        .unwrap();
        validate_logs(directory.path(), &initial_manifest()).unwrap();
    }
}
