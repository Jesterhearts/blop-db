//! Reuse log-envelope validation results within one live storage owner.
//!
//! Recovery and public low-level validation still read complete log prefixes.
//! Only the exclusive owner may reuse its earlier validation. External changes
//! to those bytes invalidate the proof.

use std::fs::File;
use std::io::Read;
use std::io::Seek;
use std::io::{
    self,
};
use std::path::Path;

use super::Error;
use super::Manifest;
use super::Result;
use super::SegmentDescriptor;
use super::metadata;
use super::metadata::SEGMENT_HEADER_LENGTH;

const MAX_PENDING_ANCHORS: u64 = 4096;

/// Extend a live-owner proof after publishing a complete WAL group.
///
/// The owner must have checked the canonical records before append and flushed
/// the complete group without replacing a file or generation.
pub(super) fn committed(
    proof: &LogValidation,
    previous: &Manifest,
    next: &Manifest,
    digests: &[[u8; 32]],
) -> Option<LogValidation> {
    if &proof.manifest != previous
        || next.durable_sequence - next.checkpoint_sequence > MAX_PENDING_ANCHORS
    {
        return None;
    }
    let mut proof = proof.clone();
    proof.pending.extend_from_slice(digests);
    proof.manifest = next.clone();
    Some(proof)
}

/// Validated immutable prefixes and the digests needed by later checkpoints.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct LogValidation {
    manifest: Manifest,
    // Dense sequence range (checkpoint, durable], bounded independently of the
    // amount of retained history. The manifest itself has a format size limit.
    pending: Vec<[u8; 32]>,
}

impl LogValidation {
    /// Seed a live-owner proof after full recovery or full publication
    /// validation.
    ///
    /// First validate the manifest's logs, checkpoint, and previous publication
    /// anchors. This method checks metadata only; it cannot prove that the
    /// directory contains the claimed bytes.
    pub(super) fn at_checkpoint(manifest: &Manifest) -> Option<Self> {
        if manifest.checkpoint_sequence != manifest.durable_sequence
            || metadata::validate_manifest(manifest).is_err()
        {
            return None;
        }
        Some(Self {
            manifest: manifest.clone(),
            pending: Vec::new(),
        })
    }
}

/// Validate a candidate's new envelopes and both publication anchors.
///
/// `previous` must be the publication held by the same live owner that created
/// `proof`. `next` must have its final, incremented generation. Store-level
/// transition and checkpoint validation remain the caller's responsibility.
///
/// On `Ok(None)`, run full log and anchor validation. Then a fresh proof may be
/// created only when checkpoint C equals durable frontier D. Replace the old
/// proof with a returned proof only after publication succeeds.
pub(super) fn validate_extension(
    directory: &Path,
    previous: &Manifest,
    next: &Manifest,
    proof: &LogValidation,
) -> Result<Option<LogValidation>> {
    metadata::validate_manifest(next).map_err(Error::Corrupt)?;
    if &proof.manifest != previous
        || previous.generation.checked_add(1) != Some(next.generation)
        || next.database_id != previous.database_id
        || next.genesis_digest != previous.genesis_digest
        || next.cursor_namespace != previous.cursor_namespace
        || next.page_file_id != previous.page_file_id
        || next.next_page_file_id != previous.next_page_file_id
        || next.checkpoint_sequence < previous.checkpoint_sequence
        || next.durable_sequence < previous.durable_sequence
        || next.history_floor < previous.history_floor
        || next.log_floor != previous.log_floor
        || next.next_cursor_id < previous.next_cursor_id
        || next.next_segment_id < previous.next_segment_id
    {
        return Ok(None);
    }
    if next.durable_sequence == previous.durable_sequence
        && next.durable_digest != previous.durable_digest
    {
        return Err(Error::Corrupt("publication changes the durable anchor"));
    }
    if next.checkpoint_sequence <= previous.durable_sequence {
        let digest = if next.checkpoint_sequence == previous.checkpoint_sequence {
            previous.checkpoint_digest
        } else {
            proof.pending[(next.checkpoint_sequence - previous.checkpoint_sequence - 1) as usize]
        };
        if next.checkpoint_digest != digest {
            return Err(Error::Corrupt("log checkpoint digest mismatch"));
        }
    }

    let old_count = previous.segments.len();
    if next.segments.len() < old_count || next.segments.len() > old_count + 1 {
        return Ok(None);
    }
    let prefix_count = old_count.saturating_sub(1);
    if next.segments[..prefix_count] != previous.segments[..prefix_count] {
        return Ok(None);
    }
    if let Some(old) = previous.segments.last() {
        let tail = &next.segments[old_count - 1];
        if tail != old
            && (tail.segment_id != old.segment_id
                || tail.first_sequence != old.first_sequence
                || tail.predecessor_digest != old.predecessor_digest
                || tail.last_sequence <= old.last_sequence
                || tail.committed_bytes <= old.committed_bytes)
        {
            return Ok(None);
        }
    }
    if next
        .segments
        .get(old_count)
        .is_some_and(|segment| segment.segment_id < previous.next_segment_id)
    {
        return Ok(None);
    }
    let pending_count = next.durable_sequence - next.checkpoint_sequence;
    if pending_count > MAX_PENDING_ANCHORS {
        return Ok(None);
    }
    let pruned =
        next.checkpoint_sequence.min(previous.durable_sequence) - previous.checkpoint_sequence;
    let mut pending = Vec::with_capacity(pending_count as usize);
    pending.extend_from_slice(&proof.pending[pruned as usize..]);

    let mut sequence = previous.durable_sequence + 1;
    let mut predecessor = previous.durable_digest;
    for (index, segment) in next.segments.iter().enumerate().skip(prefix_count) {
        let old = previous.segments.get(index);
        if old == Some(segment) {
            continue;
        }
        let offset = if let Some(old) = old {
            old.committed_bytes
        } else {
            if segment.first_sequence != sequence || segment.predecessor_digest != predecessor {
                return Err(Error::Corrupt(
                    "new segment does not extend the durable anchor",
                ));
            }
            SEGMENT_HEADER_LENGTH
        };
        let path = directory.join(format!("log-{:020}.bin", segment.segment_id));
        let file = File::open(path).map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                Error::Corrupt("missing authoritative log segment")
            } else {
                Error::Io(error)
            }
        })?;
        validate_segment_extension(
            file,
            next,
            segment,
            offset,
            sequence,
            predecessor,
            &mut pending,
        )?;
        sequence = segment.last_sequence + 1;
        predecessor = segment.last_digest;
    }
    if sequence != next.durable_sequence + 1 || predecessor != next.durable_digest {
        return Err(Error::Corrupt(
            "log extension does not reach the durable frontier",
        ));
    }
    Ok(Some(LogValidation {
        manifest: next.clone(),
        pending,
    }))
}

fn validate_segment_extension(
    file: impl Read + Seek,
    manifest: &Manifest,
    segment: &SegmentDescriptor,
    offset: u64,
    first_sequence: u64,
    predecessor: [u8; 32],
    pending: &mut Vec<[u8; 32]>,
) -> Result<()> {
    for record in super::wal::Records::suffix(
        file,
        manifest.database_id,
        segment,
        offset,
        first_sequence,
        predecessor,
    )? {
        let record = record?;
        if record.sequence == manifest.checkpoint_sequence
            && record.digest != manifest.checkpoint_digest
        {
            return Err(Error::Corrupt("log checkpoint digest mismatch"));
        }
        if record.sequence > manifest.checkpoint_sequence {
            pending.push(record.digest);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::fs::OpenOptions;
    use std::io::SeekFrom;
    use std::io::Write;
    use std::ops::Range;

    use sha2::Digest;
    use sha2::Sha256;

    use super::*;
    use crate::storage::Genesis;
    use crate::storage::LimitPolicy;

    fn hash(bytes: &[u8]) -> [u8; 32] {
        Sha256::digest(bytes).into()
    }

    fn initial_manifest() -> Manifest {
        let genesis = Genesis {
            database_id: [0x11; 16],
            initial_policy: LimitPolicy::new([0; 17]).unwrap(),
        };
        let digest = hash(&genesis.encode().unwrap());
        Manifest {
            database_id: genesis.database_id,
            genesis_digest: digest,
            cursor_namespace: [0x22; 16],
            generation: 1,
            page_file_id: 1,
            page_count: 2,
            checkpoint_sequence: 0,
            checkpoint_digest: digest,
            durable_sequence: 0,
            durable_digest: digest,
            history_floor: 0,
            log_floor: 1,
            next_cursor_id: 1,
            next_segment_id: 1,
            next_page_file_id: 2,
            roots: [0, 0, 1, 0, 0],
            segments: Vec::new(),
        }
    }

    fn record(
        sequence: u64,
        predecessor: [u8; 32],
    ) -> Vec<u8> {
        let body = b"opaque transaction body";
        let length = (72 + body.len()) as u32;
        let mut bytes = vec![0; 64];
        bytes[..8].copy_from_slice(b"BLR1\x40\x00\x01\x00");
        bytes[8..12].copy_from_slice(&length.to_le_bytes());
        bytes[12..16].copy_from_slice(&(body.len() as u32).to_le_bytes());
        bytes[16..24].copy_from_slice(&sequence.to_le_bytes());
        bytes[24] = 1;
        bytes[28..60].copy_from_slice(&predecessor);
        bytes.extend_from_slice(body);
        let crc = crc32c::crc32c(&bytes);
        bytes.extend_from_slice(&crc.to_le_bytes());
        bytes.extend_from_slice(&length.to_le_bytes());
        bytes
    }

    fn extend(
        directory: &Path,
        previous: &Manifest,
        count: u64,
        rotate: bool,
    ) -> (Manifest, Vec<[u8; 32]>) {
        assert!(count > 0);
        let mut next = previous.clone();
        next.generation += 1;
        if rotate || next.segments.is_empty() {
            let id = next.next_segment_id;
            let mut header = [0; 96];
            header[..12].copy_from_slice(b"BLOPLG01\x01\x00\x60\x00");
            header[16..32].copy_from_slice(&next.database_id);
            header[32..40].copy_from_slice(&id.to_le_bytes());
            header[40..48].copy_from_slice(&(previous.durable_sequence + 1).to_le_bytes());
            header[48..80].copy_from_slice(&previous.durable_digest);
            let crc = crc32c::crc32c(&header);
            header[92..96].copy_from_slice(&crc.to_le_bytes());
            fs::write(directory.join(format!("log-{id:020}.bin")), header).unwrap();
            next.segments.push(SegmentDescriptor {
                segment_id: id,
                first_sequence: previous.durable_sequence + 1,
                last_sequence: previous.durable_sequence,
                committed_bytes: 96,
                predecessor_digest: previous.durable_digest,
                last_digest: previous.durable_digest,
            });
            next.next_segment_id += 1;
        }
        let tail = next.segments.last_mut().unwrap();
        let mut file = OpenOptions::new()
            .write(true)
            .open(directory.join(format!("log-{:020}.bin", tail.segment_id)))
            .unwrap();
        file.set_len(tail.committed_bytes).unwrap();
        file.seek(SeekFrom::Start(tail.committed_bytes)).unwrap();
        let mut digests = Vec::new();
        for sequence in previous.durable_sequence + 1..=previous.durable_sequence + count {
            let bytes = record(sequence, tail.last_digest);
            let frame = crate::storage::wal::tests::single(&bytes);
            file.write_all(&frame).unwrap();
            tail.last_sequence = sequence;
            tail.last_digest = hash(&bytes);
            tail.committed_bytes += frame.len() as u64;
            digests.push(tail.last_digest);
        }
        next.durable_sequence = tail.last_sequence;
        next.durable_digest = tail.last_digest;
        next.encode().unwrap();
        (next, digests)
    }

    fn checkpoint(manifest: &mut Manifest) {
        manifest.checkpoint_sequence = manifest.durable_sequence;
        manifest.checkpoint_digest = manifest.durable_digest;
    }

    fn seed(
        directory: &Path,
        manifest: &Manifest,
    ) -> LogValidation {
        metadata::validate_logs(directory, manifest).unwrap();
        LogValidation::at_checkpoint(manifest).unwrap()
    }

    fn checkpointed_log(directory: &Path) -> (Manifest, LogValidation) {
        let (mut manifest, _) = extend(directory, &initial_manifest(), 1, false);
        checkpoint(&mut manifest);
        let proof = seed(directory, &manifest);
        (manifest, proof)
    }

    #[test]
    fn initialization_requires_valid_checkpointed_metadata() {
        let directory = tempfile::tempdir().unwrap();
        let initial = initial_manifest();
        assert!(seed(directory.path(), &initial).pending.is_empty());
        let (mut manifest, _) = extend(directory.path(), &initial, 3, false);
        metadata::validate_logs(directory.path(), &manifest).unwrap();
        assert!(LogValidation::at_checkpoint(&manifest).is_none());
        checkpoint(&mut manifest);
        assert!(seed(directory.path(), &manifest).pending.is_empty());
        manifest.checkpoint_digest[0] ^= 1;
        assert!(LogValidation::at_checkpoint(&manifest).is_none());
        checkpoint(&mut manifest);
        manifest.roots[2] = 0;
        assert!(LogValidation::at_checkpoint(&manifest).is_none());
    }

    #[test]
    fn unchanged_segments_and_cached_checkpoints_require_no_io() {
        let directory = tempfile::tempdir().unwrap();
        let (previous, proof) = checkpointed_log(directory.path());
        let (durable, digests) = extend(directory.path(), &previous, 3, false);
        let durable_proof = validate_extension(directory.path(), &previous, &durable, &proof)
            .unwrap()
            .unwrap();
        assert_eq!(durable_proof.pending, digests);
        assert!(proof.pending.is_empty());
        fs::remove_file(directory.path().join("log-00000000000000000001.bin")).unwrap();

        let mut next = durable.clone();
        next.generation += 1;
        let same = validate_extension(directory.path(), &durable, &next, &durable_proof)
            .unwrap()
            .unwrap();
        assert_eq!(same.pending, digests);
        next.checkpoint_sequence = 3;
        next.checkpoint_digest = digests[1];
        let checkpoint_proof =
            validate_extension(directory.path(), &durable, &next, &durable_proof)
                .unwrap()
                .unwrap();
        assert_eq!(checkpoint_proof.pending, digests[2..]);
        let mut wrong = next.clone();
        wrong.checkpoint_digest[0] ^= 1;
        wrong.encode().unwrap();
        assert!(matches!(
            validate_extension(directory.path(), &durable, &wrong, &durable_proof),
            Err(Error::Corrupt("log checkpoint digest mismatch"))
        ));
        let mut drained = next.clone();
        drained.generation += 1;
        checkpoint(&mut drained);
        assert!(
            validate_extension(directory.path(), &next, &drained, &checkpoint_proof)
                .unwrap()
                .unwrap()
                .pending
                .is_empty()
        );
        assert!(metadata::validate_logs(directory.path(), &drained).is_err());
    }

    #[test]
    fn a_checkpoint_inside_new_records_is_verified_before_pruning() {
        let directory = tempfile::tempdir().unwrap();
        let (previous, proof) = checkpointed_log(directory.path());
        let (mut next, digests) = extend(directory.path(), &previous, 3, false);
        next.checkpoint_sequence = 3;
        next.checkpoint_digest = digests[1];
        metadata::validate_logs(directory.path(), &next).unwrap();
        let mut wrong = next.clone();
        wrong.checkpoint_digest[0] ^= 1;
        wrong.encode().unwrap();
        assert!(matches!(
            validate_extension(directory.path(), &previous, &wrong, &proof),
            Err(Error::Corrupt("log checkpoint digest mismatch"))
        ));
        let next_proof = validate_extension(directory.path(), &previous, &next, &proof)
            .unwrap()
            .unwrap();
        assert_eq!(next_proof.pending, digests[2..]);
        assert_eq!(proof, seed(directory.path(), &previous));

        let (mut later, digests) = extend(directory.path(), &next, 3, false);
        later.checkpoint_sequence = 5;
        later.checkpoint_digest = digests[0];
        let later_proof = validate_extension(directory.path(), &next, &later, &next_proof)
            .unwrap()
            .unwrap();
        assert_eq!(later_proof.pending, digests[1..]);
    }

    #[test]
    fn rotation_reads_only_the_new_segment() {
        let directory = tempfile::tempdir().unwrap();
        let (previous, proof) = checkpointed_log(directory.path());
        let (next, digests) = extend(directory.path(), &previous, 3, true);
        metadata::validate_logs(directory.path(), &next).unwrap();
        fs::remove_file(directory.path().join("log-00000000000000000001.bin")).unwrap();
        let next_proof = validate_extension(directory.path(), &previous, &next, &proof)
            .unwrap()
            .unwrap();
        assert_eq!(next_proof.pending, digests);
    }

    #[test]
    fn one_publication_can_extend_the_old_tail_and_rotate_once() {
        let directory = tempfile::tempdir().unwrap();
        let (previous, proof) = checkpointed_log(directory.path());
        let (extended, mut digests) = extend(directory.path(), &previous, 2, false);
        let (mut next, rotated_digests) = extend(directory.path(), &extended, 2, true);
        next.generation = previous.generation + 1;
        digests.extend(rotated_digests);
        metadata::validate_logs(directory.path(), &next).unwrap();
        let next_proof = validate_extension(directory.path(), &previous, &next, &proof)
            .unwrap()
            .unwrap();
        assert_eq!(next_proof.pending, digests);
    }

    #[test]
    fn new_segment_must_extend_the_previous_durable_anchor_even_when_checkpoint_advances() {
        let directory = tempfile::tempdir().unwrap();
        let (mut previous, _) = checkpointed_log(directory.path());
        previous.segments.clear();
        previous.log_floor = previous.durable_sequence + 1;
        let proof = seed(directory.path(), &previous);
        let mut fabricated = previous.clone();
        fabricated.durable_digest = [0x55; 32];
        checkpoint(&mut fabricated);
        let (mut next, digests) = extend(directory.path(), &fabricated, 3, true);
        next.checkpoint_sequence = 3;
        next.checkpoint_digest = digests[1];
        metadata::validate_logs(directory.path(), &next).unwrap();
        assert!(matches!(
            validate_extension(directory.path(), &previous, &next, &proof),
            Err(Error::Corrupt(
                "new segment does not extend the durable anchor"
            ))
        ));
    }

    #[test]
    fn every_missing_or_corrupt_suffix_byte_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let (previous, proof) = checkpointed_log(directory.path());
        let (next, _) = extend(directory.path(), &previous, 1, false);
        let path = directory.path().join("log-00000000000000000001.bin");
        let bytes = fs::read(&path).unwrap();
        let offset = previous.segments[0].committed_bytes as usize;
        for index in offset..bytes.len() {
            fs::write(&path, &bytes[..index]).unwrap();
            assert!(
                matches!(
                    validate_extension(directory.path(), &previous, &next, &proof),
                    Err(Error::Corrupt(_))
                ),
                "truncation at {index}"
            );
            let mut corrupt = bytes.clone();
            corrupt[index] ^= 0x80;
            fs::write(&path, corrupt).unwrap();
            assert!(
                matches!(
                    validate_extension(directory.path(), &previous, &next, &proof),
                    Err(Error::Corrupt(_))
                ),
                "corruption at {index}"
            );
        }
        fs::remove_file(&path).unwrap();
        assert!(matches!(
            validate_extension(directory.path(), &previous, &next, &proof),
            Err(Error::Corrupt("missing authoritative log segment"))
        ));
        fs::write(&path, bytes).unwrap();
        assert!(
            validate_extension(directory.path(), &previous, &next, &proof)
                .unwrap()
                .is_some()
        );
        assert_eq!(proof, seed(directory.path(), &previous));
    }

    #[test]
    fn checksummed_suffix_headers_cannot_forge_framing_sequence_or_predecessor() {
        let directory = tempfile::tempdir().unwrap();
        let (previous, proof) = checkpointed_log(directory.path());
        let (next, _) = extend(directory.path(), &previous, 1, false);
        let path = directory.path().join("log-00000000000000000001.bin");
        let original = fs::read(&path).unwrap();
        let offset = previous.segments[0].committed_bytes as usize;
        for index in 0..64 {
            let mut record = original[offset + crate::storage::wal::HEADER_BYTES
                ..original.len() - crate::storage::wal::TRAILER_BYTES]
                .to_vec();
            record[index] ^= 0x80;
            let end = record.len() - 8;
            let crc = crc32c::crc32c(&record[..end]);
            record[end..end + 4].copy_from_slice(&crc.to_le_bytes());
            let mut bytes = original[..offset].to_vec();
            bytes.extend_from_slice(&crate::storage::wal::tests::frame(
                previous.durable_sequence + 1,
                previous.durable_digest,
                &[&record],
            ));
            let mut forged = next.clone();
            forged.durable_digest = hash(&record);
            forged.segments[0].last_digest = forged.durable_digest;
            forged.encode().unwrap();
            fs::write(&path, bytes).unwrap();
            assert!(
                validate_extension(directory.path(), &previous, &forged, &proof).is_err(),
                "checksummed header byte {index}"
            );
        }
    }

    #[test]
    fn touched_header_final_digest_and_committed_end_are_verified() {
        let directory = tempfile::tempdir().unwrap();
        let (previous, proof) = checkpointed_log(directory.path());
        let (next, _) = extend(directory.path(), &previous, 1, false);
        let path = directory.path().join("log-00000000000000000001.bin");
        let original = fs::read(&path).unwrap();
        for index in 0..96 {
            let mut bytes = original.clone();
            bytes[index] ^= 0x80;
            if index < 92 {
                bytes[92..96].fill(0);
                let crc = crc32c::crc32c(&bytes[..96]);
                bytes[92..96].copy_from_slice(&crc.to_le_bytes());
            }
            fs::write(&path, bytes).unwrap();
            assert!(
                validate_extension(directory.path(), &previous, &next, &proof).is_err(),
                "segment header byte {index}"
            );
        }
        fs::write(&path, &original).unwrap();
        let mut wrong_digest = next.clone();
        wrong_digest.durable_digest[0] ^= 1;
        wrong_digest.segments[0].last_digest = wrong_digest.durable_digest;
        wrong_digest.encode().unwrap();
        assert!(matches!(
            validate_extension(directory.path(), &previous, &wrong_digest, &proof),
            Err(Error::Corrupt("log final digest mismatch"))
        ));

        let mut excess = next.clone();
        excess.segments[0].committed_bytes += 1;
        let mut bytes = original.clone();
        bytes.push(0);
        fs::write(&path, &bytes).unwrap();
        excess.encode().unwrap();
        assert!(matches!(
            validate_extension(directory.path(), &previous, &excess, &proof),
            Err(Error::Corrupt("excess bytes inside committed log prefix"))
        ));
        assert!(
            validate_extension(directory.path(), &previous, &next, &proof)
                .unwrap()
                .is_some()
        );
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }

    struct CountingFile {
        file: File,
        position: u64,
        reads: Vec<Range<u64>>,
    }

    impl Read for CountingFile {
        fn read(
            &mut self,
            bytes: &mut [u8],
        ) -> io::Result<usize> {
            let count = self.file.read(bytes)?;
            let end = self.position + count as u64;
            self.reads.push(self.position..end);
            self.position = end;
            Ok(count)
        }
    }

    impl Seek for CountingFile {
        fn seek(
            &mut self,
            from: SeekFrom,
        ) -> io::Result<u64> {
            self.position = self.file.seek(from)?;
            Ok(self.position)
        }
    }

    #[test]
    fn suffix_reader_reads_only_the_header_and_new_committed_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let (mut previous, _) = extend(directory.path(), &initial_manifest(), 512, false);
        checkpoint(&mut previous);
        let proof = seed(directory.path(), &previous);
        let (next, digests) = extend(directory.path(), &previous, 3, false);
        let path = directory.path().join("log-00000000000000000001.bin");
        let mut bytes = fs::read(&path).unwrap();
        let old_end = previous.segments[0].committed_bytes;
        let new_end = next.segments[0].committed_bytes;
        bytes.extend_from_slice(&[0xff; 16384]);
        fs::write(&path, &bytes).unwrap();
        let mut reader = CountingFile {
            file: File::open(&path).unwrap(),
            position: 0,
            reads: Vec::new(),
        };
        let mut pending = Vec::new();
        validate_segment_extension(
            &mut reader,
            &next,
            &next.segments[0],
            old_end,
            previous.durable_sequence + 1,
            previous.durable_digest,
            &mut pending,
        )
        .unwrap();
        assert_eq!(pending, digests);
        assert_eq!(reader.reads[0], 0..96);
        assert!(
            reader.reads[1..]
                .iter()
                .all(|range| range.start >= old_end && range.end <= new_end)
        );
        assert_eq!(
            reader
                .reads
                .iter()
                .map(|range| range.end - range.start)
                .sum::<u64>(),
            96 + new_end - old_end
        );

        // Deliberately violate the owner's immutable-prefix contract to detect
        // accidental full-prefix reads through the public incremental entry.
        bytes[100] ^= 1;
        fs::write(&path, &bytes).unwrap();
        assert!(
            validate_extension(directory.path(), &previous, &next, &proof)
                .unwrap()
                .is_some()
        );
        assert!(metadata::validate_logs(directory.path(), &next).is_err());
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }

    #[test]
    fn anchor_capacity_is_bounded_and_pruning_allows_further_progress() {
        let directory = tempfile::tempdir().unwrap();
        let initial = initial_manifest();
        let initial_proof = seed(directory.path(), &initial);
        let (previous, digests) = extend(directory.path(), &initial, MAX_PENDING_ANCHORS, false);
        let proof = validate_extension(directory.path(), &initial, &previous, &initial_proof)
            .unwrap()
            .unwrap();
        assert_eq!(proof.pending.len(), MAX_PENDING_ANCHORS as usize);
        assert_eq!(proof.pending.capacity(), MAX_PENDING_ANCHORS as usize);
        let (mut next, suffix) = extend(directory.path(), &previous, 1, false);
        assert!(
            validate_extension(directory.path(), &previous, &next, &proof)
                .unwrap()
                .is_none()
        );
        metadata::validate_logs(directory.path(), &next).unwrap();
        assert!(LogValidation::at_checkpoint(&next).is_none());

        next.checkpoint_sequence = 1;
        next.checkpoint_digest = digests[0];
        let advanced = validate_extension(directory.path(), &previous, &next, &proof)
            .unwrap()
            .unwrap();
        assert_eq!(advanced.pending.len(), MAX_PENDING_ANCHORS as usize);
        assert_eq!(
            &advanced.pending[..advanced.pending.len() - 1],
            &digests[1..]
        );
        assert_eq!(advanced.pending.last(), suffix.last());
        assert_eq!(proof.pending, digests);

        checkpoint(&mut next);
        let drained = validate_extension(directory.path(), &previous, &next, &proof)
            .unwrap()
            .unwrap();
        assert!(drained.pending.is_empty());
        assert_eq!(drained, seed(directory.path(), &next));
        let single_publication = Manifest {
            generation: initial.generation + 1,
            ..next
        };
        assert!(
            validate_extension(
                directory.path(),
                &initial,
                &single_publication,
                &initial_proof
            )
            .unwrap()
            .unwrap()
            .pending
            .is_empty()
        );
    }

    #[test]
    fn retired_replaced_unknown_or_multiple_new_segments_require_full_validation() {
        let directory = tempfile::tempdir().unwrap();
        let (first, _) = extend(directory.path(), &initial_manifest(), 2, false);
        let (mut previous, _) = extend(directory.path(), &first, 2, true);
        checkpoint(&mut previous);
        let proof = seed(directory.path(), &previous);
        let mut next = previous.clone();
        next.generation += 1;
        let mut retired = next.clone();
        retired.segments.remove(0);
        retired.log_floor = retired.segments[0].first_sequence;
        let mut cleared = next.clone();
        cleared.segments.clear();
        cleared.log_floor = cleared.durable_sequence + 1;
        let mut replaced = next.clone();
        replaced.segments[1].segment_id = replaced.next_segment_id;
        replaced.next_segment_id += 1;
        let mut altered_prefix = next.clone();
        altered_prefix.segments[0].committed_bytes += 1;
        let (one, _) = extend(directory.path(), &previous, 1, true);
        let (mut two, _) = extend(directory.path(), &one, 1, true);
        two.generation = next.generation;
        for candidate in [retired, cleared, replaced, altered_prefix, two] {
            candidate.encode().unwrap();
            assert!(
                validate_extension(directory.path(), &previous, &candidate, &proof)
                    .unwrap()
                    .is_none()
            );
        }

        previous.next_segment_id = 10;
        let proof = seed(directory.path(), &previous);
        let mut reused = one;
        reused.next_segment_id = 10;
        reused.encode().unwrap();
        assert!(
            validate_extension(directory.path(), &previous, &reused, &proof)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn proof_is_bound_to_the_complete_previous_manifest_and_final_generation() {
        let directory = tempfile::tempdir().unwrap();
        let (previous, proof) = checkpointed_log(directory.path());
        let mut next = previous.clone();
        next.generation += 1;
        let mutations: [fn(&mut Manifest); 7] = [
            |m| m.generation += 1,
            |m| m.database_id[0] ^= 1,
            |m| m.cursor_namespace[0] ^= 1,
            |m| m.page_count += 1,
            |m| m.segments[0].committed_bytes += 1,
            |m| m.checkpoint_sequence = 0,
            |m| m.durable_digest[0] ^= 1,
        ];
        for mutate in mutations {
            let mut unrelated = previous.clone();
            mutate(&mut unrelated);
            assert!(
                validate_extension(directory.path(), &unrelated, &next, &proof)
                    .unwrap()
                    .is_none()
            );
        }
        assert!(
            validate_extension(directory.path(), &previous, &previous, &proof)
                .unwrap()
                .is_none()
        );
        next.roots[2] = 0;
        assert!(matches!(
            validate_extension(directory.path(), &previous, &next, &proof),
            Err(Error::Corrupt(_))
        ));
    }
}
