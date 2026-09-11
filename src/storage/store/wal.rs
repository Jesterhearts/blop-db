//! Publish and recover WAL groups while holding the exclusive directory lease.

use std::fs::File;
use std::fs::OpenOptions;
use std::fs::{
    self,
};
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::Path;

use sha2::Digest;
use sha2::Sha256;

use super::Store;
use super::writable;
use crate::storage::Error;
use crate::storage::Manifest;
use crate::storage::Result;
use crate::storage::SegmentDescriptor;
use crate::storage::log_validation;
use crate::storage::maintenance;
use crate::storage::metadata;
use crate::storage::platform;
use crate::storage::wal;

pub(crate) fn append_wal<'a>(
    store: &mut Store,
    records: impl IntoIterator<Item = (u64, &'a [u8])>,
) -> Result<[u8; 32]> {
    writable(store)?;
    let result = append_group(store, records);
    if result.is_err() {
        store.poisoned = true;
    }
    result
}

fn append_group<'a>(
    store: &mut Store,
    records: impl IntoIterator<Item = (u64, &'a [u8])>,
) -> Result<[u8; 32]> {
    let mut group = Vec::new();
    let mut digests = Vec::new();
    let mut next = store.manifest.clone();
    let mut length = wal::GROUP_OVERHEAD;
    for (sequence, bytes) in records {
        if group.len() == wal::MAX_GROUP_RECORDS
            || sequence == u64::MAX
            || next.durable_sequence.checked_add(1) != Some(sequence)
        {
            return Err(Error::InvalidInput("invalid WAL group sequence or count"));
        }
        let mut input = bytes;
        let (consumed, digest) = metadata::validate_record(
            &mut input,
            bytes.len() as u64,
            sequence,
            next.durable_digest,
        )?;
        if consumed != bytes.len() as u64 {
            return Err(Error::InvalidInput("trailing canonical record bytes"));
        }
        length = length.checked_add(consumed).ok_or(Error::Exhausted)?;
        group.push(bytes);
        digests.push(digest);
        next.durable_sequence = sequence;
        next.durable_digest = digest;
    }
    if group.is_empty() {
        return Err(Error::InvalidInput("empty WAL group"));
    }
    let header = wal::GroupHeader {
        bytes: length,
        first: store.manifest.durable_sequence + 1,
        count: group.len() as u32,
        predecessor: store.manifest.durable_digest,
        last_digest: next.durable_digest,
    }
    .encode()?;
    let new_file = store.rotate_next || next.segments.is_empty();
    let file = if new_file {
        let (file, id) = maintenance::allocate(store, "log", next.next_segment_id)?;
        next.next_segment_id = id + 1;
        let segment = SegmentDescriptor {
            segment_id: id,
            first_sequence: store.manifest.durable_sequence + 1,
            last_sequence: next.durable_sequence,
            committed_bytes: wal::SEGMENT_BYTES,
            predecessor_digest: store.manifest.durable_digest,
            last_digest: next.durable_digest,
        };
        platform::write_all_at(&file, &wal::segment_header(next.database_id, &segment), 0)?;
        next.segments.push(segment);
        file
    } else {
        let segment = next.segments.last().unwrap();
        let file = OpenOptions::new().read(true).write(true).open(
            store
                .directory
                .join(format!("log-{:020}.bin", segment.segment_id)),
        )?;
        if file.metadata()?.len() != segment.committed_bytes {
            return Err(Error::Corrupt(
                "active WAL length differs from durable prefix",
            ));
        }
        file
    };
    let segment = next.segments.last_mut().unwrap();
    let mut offset = segment.committed_bytes;
    segment.committed_bytes = offset.checked_add(length).ok_or(Error::Exhausted)?;
    segment.last_sequence = next.durable_sequence;
    segment.last_digest = next.durable_digest;
    metadata::validate_manifest(&next).map_err(Error::InvalidInput)?;
    let mut hash = Sha256::new();
    hash.update(header);
    platform::write_all_at(&file, &header, offset)?;
    offset += header.len() as u64;
    for bytes in group {
        platform::write_all_at(&file, bytes, offset)?;
        hash.update(bytes);
        offset += bytes.len() as u64;
    }
    platform::write_all_at(&file, &wal::trailer(length, hash.finalize().into()), offset)?;
    platform::sync_file(&file)?;
    if new_file {
        platform::sync_directory(&platform::open_directory(&store.directory)?)?;
    }
    if let Some(runtime) = &mut store.runtime {
        runtime.logs = runtime
            .logs
            .as_ref()
            .and_then(|proof| log_validation::committed(proof, &store.manifest, &next, &digests));
    }
    let digest = next.durable_digest;
    store.manifest = next;
    store.rotate_next = false;
    Ok(digest)
}

/// Discover linked WAL segments after the selected publication's log bounds.
///
/// Reject complete malformed groups as corruption. Discard only physically
/// short terminal headers, bodies, or trailers. Do not search past damaged data
/// for a later valid group.
pub(super) fn recover_tail(
    directory: &Path,
    selected: &Manifest,
) -> Result<Manifest> {
    let mut next = selected.clone();
    let mut incomplete = false;
    let mut flush = Vec::new();
    if let Some(segment) = next.segments.last().copied() {
        let (extended, partial) = scan_tail(
            directory,
            selected.database_id,
            segment,
            segment.committed_bytes,
            segment.last_sequence + 1,
            segment.last_digest,
        )?;
        incomplete = partial;
        if let Some(extended) = extended {
            next.durable_sequence = extended.last_sequence;
            next.durable_digest = extended.last_digest;
            *next.segments.last_mut().unwrap() = extended;
            flush.push(extended);
        } else if partial {
            flush.push(segment);
        }
    }
    let candidates = segment_ids(directory, selected.next_segment_id)?;
    let mut new_files = false;
    for id in candidates {
        let path = directory.join(format!("log-{id:020}.bin"));
        let mut file = File::open(&path)?;
        if file.metadata()?.len() < wal::SEGMENT_BYTES {
            continue;
        }
        let mut header = [0; 96];
        metadata::read_committed(&mut file, &mut header)?;
        let segment = SegmentDescriptor {
            segment_id: id,
            first_sequence: wal::u64_at(&header, 40),
            last_sequence: 0,
            committed_bytes: wal::SEGMENT_BYTES,
            predecessor_digest: header[48..80].try_into().unwrap(),
            last_digest: [0; 32],
        };
        metadata::validate_segment_header(&header, &next.database_id, &segment)?;
        let (found, partial) = scan_tail(
            directory,
            next.database_id,
            segment,
            wal::SEGMENT_BYTES,
            segment.first_sequence,
            segment.predecessor_digest,
        )?;
        let Some(found) = found else {
            continue;
        };
        if incomplete
            || found.first_sequence != next.durable_sequence + 1
            || found.predecessor_digest != next.durable_digest
        {
            return Err(Error::Corrupt("unlinked, forked or gapped WAL successor"));
        }
        next.durable_sequence = found.last_sequence;
        next.durable_digest = found.last_digest;
        next.next_segment_id = found.segment_id + 1;
        next.segments.push(found);
        flush.push(found);
        new_files = true;
        incomplete = partial;
    }
    metadata::validate_manifest(&next).map_err(Error::Corrupt)?;
    // A process crash can leave complete records solely in the OS cache. Make
    // every accepted suffix durable before the engine is allowed to replay it.
    for segment in flush {
        let file = OpenOptions::new()
            .write(true)
            .open(directory.join(format!("log-{:020}.bin", segment.segment_id)))?;
        file.set_len(segment.committed_bytes)?;
        platform::sync_file(&file)?;
    }
    if new_files {
        platform::sync_directory(&platform::open_directory(directory)?)?;
    }
    Ok(next)
}

fn segment_ids(
    directory: &Path,
    floor: u64,
) -> Result<Vec<u64>> {
    let mut ids = Vec::new();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(id) = name
            .strip_prefix("log-")
            .and_then(|name| name.strip_suffix(".bin"))
        else {
            continue;
        };
        if id.len() != 20 || !id.as_bytes().iter().all(u8::is_ascii_digit) {
            continue;
        }
        let Ok(id) = id.parse::<u64>() else {
            continue;
        };
        if id >= floor && id != u64::MAX {
            if ids.len() == 1_048_576 {
                return Err(Error::Exhausted);
            }
            ids.push(id);
        }
    }
    ids.sort_unstable();
    Ok(ids)
}

fn scan_tail(
    directory: &Path,
    database: [u8; 16],
    mut segment: SegmentDescriptor,
    mut offset: u64,
    mut sequence: u64,
    mut predecessor: [u8; 32],
) -> Result<(Option<SegmentDescriptor>, bool)> {
    let mut file = File::open(directory.join(format!("log-{:020}.bin", segment.segment_id)))?;
    let length = file.metadata()?.len();
    if length < offset {
        return Err(Error::Corrupt("truncated selected WAL prefix"));
    }
    let mut found = false;
    while offset < length {
        if length - offset < wal::HEADER_BYTES as u64 {
            return Ok((found.then_some(segment), true));
        }
        file.seek(SeekFrom::Start(offset))?;
        let mut bytes = [0; wal::HEADER_BYTES];
        file.read_exact(&mut bytes)?;
        let header = wal::GroupHeader::decode(&bytes)?;
        if header.first != sequence || header.predecessor != predecessor {
            return Err(Error::Corrupt(
                "WAL suffix sequence or predecessor mismatch",
            ));
        }
        if header.bytes > length - offset {
            return Ok((found.then_some(segment), true));
        }
        segment.committed_bytes = offset + header.bytes;
        segment.last_sequence = header.first + u64::from(header.count) - 1;
        segment.last_digest = header.last_digest;
        for record in
            wal::Records::suffix(&mut file, database, &segment, offset, sequence, predecessor)?
        {
            record?;
        }
        offset = segment.committed_bytes;
        sequence = segment.last_sequence + 1;
        predecessor = segment.last_digest;
        found = true;
    }
    Ok((found.then_some(segment), false))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Genesis;
    use crate::storage::platform::faults::Event;
    use crate::storage::platform::faults::Failure;
    use crate::storage::platform::faults::Guard;
    use crate::storage::platform::faults::Operation;
    use crate::storage::platform::faults::Phase;
    use crate::storage::wal::tests::record;
    use crate::storage::{
        self,
    };

    fn fixture() -> (tempfile::TempDir, Store) {
        let directory = tempfile::tempdir().unwrap();
        let mut store = storage::create(
            directory.path().join("db"),
            Genesis {
                database_id: [1; 16],
                initial_policy: crate::Limits::default().try_into().unwrap(),
            },
            [2; 16],
        )
        .unwrap();
        storage::enable_runtime_cache(&mut store);
        (directory, store)
    }

    fn append_one(store: &mut Store) -> Vec<u8> {
        let sequence = store.manifest().durable_sequence + 1;
        let bytes = record(sequence, store.manifest().durable_digest);
        append_wal(store, [(sequence, bytes.as_slice())]).unwrap();
        bytes
    }

    fn pair(store: &Store) -> [Vec<u8>; 2] {
        let first = record(
            store.manifest().durable_sequence + 1,
            store.manifest().durable_digest,
        );
        let second = record(
            store.manifest().durable_sequence + 2,
            Sha256::digest(&first).into(),
        );
        [first, second]
    }

    fn append_pair(store: &mut Store) -> Result<[u8; 32]> {
        let first = store.manifest().durable_sequence + 1;
        let records = pair(store);
        append_wal(
            store,
            records
                .iter()
                .enumerate()
                .map(|(i, bytes)| (first + i as u64, bytes.as_slice())),
        )
    }

    #[test]
    fn crash_child() {
        let Ok(path) = std::env::var("BLOP_WAL_CRASH_PATH") else {
            return;
        };
        let boundary = std::env::var("BLOP_WAL_CRASH_BOUNDARY")
            .unwrap()
            .parse()
            .unwrap();
        let mut store = storage::open(path).unwrap();
        let _guard = Guard::new(Some((boundary, Failure::Exit)));
        append_pair(&mut store).unwrap();
        panic!("WAL append did not reach crash boundary");
    }

    #[test]
    fn process_exit_at_every_append_boundary_preserves_the_acknowledged_prefix() {
        let (_directory, mut store) = fixture();
        append_one(&mut store);
        store.rotate_next = true;
        let guard = Guard::new(None);
        append_pair(&mut store).unwrap();
        let trace = guard.trace();
        drop(guard);
        for boundary in 0..trace.len() {
            let (directory, mut store) = fixture();
            append_one(&mut store);
            drop(store);
            let path = directory.path().join("db");
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "storage::store::wal::tests::crash_child",
                    "--nocapture",
                ])
                .env("BLOP_WAL_CRASH_PATH", &path)
                .env("BLOP_WAL_CRASH_BOUNDARY", boundary.to_string())
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(77));
            let recovered = storage::open(&path).unwrap();
            assert!(matches!(recovered.manifest().durable_sequence, 1 | 3));
            assert_eq!(recovered.manifest().checkpoint_sequence, 0);
        }
    }

    #[test]
    fn one_flush_commits_a_group_without_advancing_current_and_recovery_flushes_it_before_replay() {
        let (directory, mut store) = fixture();
        let selected = fs::read(store.directory().join("CURRENT")).unwrap();
        let guard = Guard::new(None);
        let first = append_one(&mut store);
        let initial_trace = guard.trace();
        drop(guard);
        assert_eq!(
            initial_trace
                .iter()
                .filter(|event| **event == Event(Operation::SyncFile, Phase::Before))
                .count(),
            1
        );
        assert_eq!(
            initial_trace
                .iter()
                .filter(|event| **event == Event(Operation::SyncDirectory, Phase::Before))
                .count(),
            1
        );
        let records = pair(&store);
        let guard = Guard::new(None);
        append_pair(&mut store).unwrap();
        let trace = guard.trace();
        drop(guard);
        assert_eq!(
            trace
                .iter()
                .filter(|event| **event == Event(Operation::SyncFile, Phase::Before))
                .count(),
            1
        );
        assert!(!trace.iter().any(|event| matches!(
            event,
            Event(Operation::SyncDirectory | Operation::Rename(_), _)
        )));
        assert_eq!(store.manifest().durable_sequence, 3);
        assert_eq!(store.selected_manifest().durable_sequence, 0);
        assert_eq!(
            fs::read(store.directory().join("CURRENT")).unwrap(),
            selected
        );
        drop(store);
        let guard = Guard::new(None);
        let reopened = storage::open(directory.path().join("db")).unwrap();
        assert!(
            guard
                .trace()
                .contains(&Event(Operation::SyncFile, Phase::After))
        );
        drop(guard);
        assert_eq!(reopened.manifest().durable_sequence, 3);
        assert_eq!(reopened.manifest().checkpoint_sequence, 0);
        let segment = &reopened.manifest().segments[0];
        let values = wal::Records::new(
            File::open(reopened.directory().join("log-00000000000000000001.bin")).unwrap(),
            [1; 16],
            segment,
        )
        .unwrap()
        .map(|record| record.unwrap().bytes)
        .collect::<Vec<_>>();
        assert_eq!(values, vec![first, records[0].clone(), records[1].clone()]);
    }

    #[test]
    fn maximum_group_keeps_canonical_records_and_oversized_groups_write_nothing() {
        for count in [64, 65] {
            let (directory, mut store) = fixture();
            let mut previous = store.manifest().durable_digest;
            let mut records = Vec::new();
            for sequence in 1..=count {
                let bytes = record(sequence, previous);
                previous = Sha256::digest(&bytes).into();
                records.push(bytes);
            }
            let result = append_wal(
                &mut store,
                records
                    .iter()
                    .enumerate()
                    .map(|(i, bytes)| (i as u64 + 1, bytes.as_slice())),
            );
            if count == 65 {
                assert!(matches!(result, Err(Error::InvalidInput(_))));
                assert!(
                    !store
                        .directory()
                        .join("log-00000000000000000001.bin")
                        .exists()
                );
                continue;
            }
            assert_eq!(result.unwrap(), previous);
            drop(store);
            let store = storage::open(directory.path().join("db")).unwrap();
            assert_eq!(store.manifest().durable_sequence, 64);
            let actual = wal::Records::new(
                File::open(store.directory().join("log-00000000000000000001.bin")).unwrap(),
                [1; 16],
                &store.manifest().segments[0],
            )
            .unwrap()
            .map(|record| record.unwrap().bytes)
            .collect::<Vec<_>>();
            assert_eq!(actual, records);
        }
    }

    #[test]
    fn every_append_write_and_flush_failure_recovers_whole_groups_and_poisoned_owner_cannot_retry()
    {
        for rotate in [false, true] {
            let (_directory, mut store) = fixture();
            append_one(&mut store);
            store.rotate_next = rotate;
            let guard = Guard::new(None);
            append_pair(&mut store).unwrap();
            let trace = guard.trace();
            drop(guard);
            for (index, event) in trace.iter().enumerate() {
                for partial in [false, true] {
                    if partial && !matches!(event, Event(Operation::Write { .. }, Phase::Before)) {
                        continue;
                    }
                    let (directory, mut store) = fixture();
                    append_one(&mut store);
                    store.rotate_next = rotate;
                    let guard = Guard::new(Some((
                        index,
                        if partial {
                            Failure::PartialWrite
                        } else {
                            Failure::Error
                        },
                    )));
                    assert!(
                        append_pair(&mut store).is_err(),
                        "rotate={rotate} event={event:?}"
                    );
                    drop(guard);
                    assert!(matches!(append_pair(&mut store), Err(Error::NeedsRecovery)));
                    drop(store);
                    let recovered = storage::open(directory.path().join("db")).unwrap();
                    assert!(matches!(recovered.manifest().durable_sequence, 1 | 3));
                    assert_eq!(recovered.manifest().checkpoint_sequence, 0);
                }
            }
        }
    }

    #[test]
    fn every_physically_short_tail_is_discarded_but_complete_damaged_groups_are_errors() {
        let (directory, mut store) = fixture();
        append_one(&mut store);
        let boundary = store.manifest().segments[0].committed_bytes as usize;
        append_pair(&mut store).unwrap();
        let path = store.directory().join("log-00000000000000000001.bin");
        let full = fs::read(&path).unwrap();
        drop(store);
        for end in boundary..full.len() {
            fs::write(&path, &full[..end]).unwrap();
            let reopened = storage::open(directory.path().join("db")).unwrap();
            assert_eq!(
                reopened.manifest().durable_sequence,
                1,
                "tail length {}",
                end - boundary
            );
            assert_eq!(fs::metadata(&path).unwrap().len(), boundary as u64);
        }
        for index in [
            boundary,
            boundary + 16,
            boundary + 108,
            boundary + wal::HEADER_BYTES + 64,
            full.len() - wal::TRAILER_BYTES,
            full.len() - 1,
        ] {
            let mut bad = full.clone();
            bad[index] ^= 1;
            fs::write(&path, bad).unwrap();
            assert!(matches!(
                storage::open(directory.path().join("db")),
                Err(Error::Corrupt(_))
            ));
        }
    }

    #[test]
    fn discovery_rejects_missing_predecessors_and_forks_but_ignores_a_short_orphan() {
        let (directory, mut store) = fixture();
        append_one(&mut store);
        store.rotate_next = true;
        append_one(&mut store);
        let path = store.directory().to_owned();
        let first = fs::read(path.join("log-00000000000000000001.bin")).unwrap();
        let mut second = fs::read(path.join("log-00000000000000000002.bin")).unwrap();
        drop(store);
        let mut changed_version = first.clone();
        changed_version[8..10].copy_from_slice(&2_u16.to_le_bytes());
        fs::write(path.join("log-00000000000000000001.bin"), &changed_version).unwrap();
        assert!(matches!(storage::open(&path), Err(Error::Corrupt(_))));
        changed_version[92..96].fill(0);
        let crc = crc32c::crc32c(&changed_version[..96]);
        changed_version[92..96].copy_from_slice(&crc.to_le_bytes());
        fs::write(path.join("log-00000000000000000001.bin"), &changed_version).unwrap();
        assert!(matches!(
            storage::open(&path),
            Err(Error::Unsupported { version: 2, .. })
        ));
        fs::remove_file(path.join("log-00000000000000000001.bin")).unwrap();
        assert!(matches!(storage::open(&path), Err(Error::Corrupt(_))));
        fs::write(path.join("log-00000000000000000001.bin"), first).unwrap();
        second[32..40].copy_from_slice(&3_u64.to_le_bytes());
        second[92..96].fill(0);
        let crc = crc32c::crc32c(&second[..96]);
        second[92..96].copy_from_slice(&crc.to_le_bytes());
        fs::write(path.join("log-00000000000000000003.bin"), &second).unwrap();
        assert!(matches!(storage::open(&path), Err(Error::Corrupt(_))));
        fs::write(path.join("log-00000000000000000003.bin"), &second[..110]).unwrap();
        let mut reopened = storage::open(&path).unwrap();
        assert_eq!(reopened.manifest().durable_sequence, 2);
        append_one(&mut reopened);
        assert_eq!(reopened.manifest().segments.last().unwrap().segment_id, 4);
        drop(reopened);
        assert_eq!(
            storage::open(directory.path().join("db"))
                .unwrap()
                .manifest()
                .durable_sequence,
            3
        );
    }

    #[tokio::test]
    async fn checkpoint_inside_a_group_replays_only_the_remaining_records() {
        let (directory, mut store) = fixture();
        append_pair(&mut store).unwrap();
        let policy = crate::Limits::default().try_into().unwrap();
        let first = record(1, Sha256::digest(store.genesis().encode().unwrap()).into());
        let digest = Sha256::digest(first).into();
        crate::vm::execute_limits(&mut store, 1, digest, &policy).unwrap();
        let mut manifest = store.manifest().clone();
        manifest.checkpoint_sequence = 1;
        manifest.checkpoint_digest = digest;
        let view = storage::view(&store);
        storage::publish(&mut store, &view, manifest).unwrap();
        drop(view);
        drop(store);
        let database = crate::database::open(directory.path().join("db"))
            .await
            .unwrap();
        assert_eq!(
            crate::database::snapshot(&database)
                .await
                .unwrap()
                .sequence(),
            2
        );
        crate::database::close(&database).await.unwrap();
    }
}
