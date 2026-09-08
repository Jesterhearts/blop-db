//! Serial durability-before-execution and checkpoint-based log replay.

use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::BufReader;
use std::io::Read;
use std::io::Write;

use sha2::Digest;
use sha2::Sha256;

use super::Error;
use super::Receipt;
use super::Result;
use super::record;
use crate::storage;
use crate::vm;

const SEGMENT_HEADER_LENGTH: usize = 96;
const RECORD_HEADER_LENGTH: usize = 64;
const RECORD_OVERHEAD: usize = 72;
const MAX_RECORD_LENGTH: usize = 64 * 1024 * 1024;

pub(super) fn commit(
    store: &mut storage::Store,
    command: &record::Command,
) -> Result<Receipt> {
    let manifest = store.manifest();
    if manifest.checkpoint_sequence != manifest.durable_sequence {
        return Err(Error::Storage(storage::Error::NeedsRecovery));
    }
    let sequence = manifest
        .durable_sequence
        .checked_add(1)
        .filter(|&sequence| sequence != u64::MAX)
        .ok_or(Error::Storage(storage::Error::Exhausted))?;
    let command = record::prepare(&storage::view(store), sequence, command).map_err(rejection)?;
    let (kind, body) = record::encode(&command).map_err(rejection)?;
    let bytes = envelope(sequence, manifest.durable_digest, kind, &body).map_err(rejection)?;
    let uncertain = |source| Error::Uncertain {
        sequence: Some(sequence),
        source: Some(source),
    };
    let digest =
        append(store, sequence, &bytes).map_err(|error| uncertain(vm::Error::Storage(error)))?;
    let outcome = record::execute(store, sequence, digest, &command).map_err(uncertain)?;
    publish_checkpoint(store).map_err(|error| uncertain(vm::Error::Storage(error)))?;
    Ok(Receipt { sequence, outcome })
}

fn rejection(error: vm::Error) -> Error {
    match error {
        vm::Error::Storage(error) => Error::Storage(error),
        error => Error::Rejected(error),
    }
}

fn envelope(
    sequence: u64,
    predecessor: [u8; 32],
    kind: u8,
    body: &[u8],
) -> vm::Result<Vec<u8>> {
    let length = body
        .len()
        .checked_add(RECORD_OVERHEAD)
        .filter(|&length| length <= MAX_RECORD_LENGTH)
        .ok_or(vm::Error::Invalid("log record exceeds 64 MiB"))?;
    if sequence == 0 || sequence == u64::MAX || !(1..=3).contains(&kind) {
        return Err(vm::Error::Invalid("invalid log record sequence or kind"));
    }
    let mut bytes = Vec::with_capacity(length);
    bytes.resize(RECORD_HEADER_LENGTH, 0);
    bytes[..4].copy_from_slice(b"BLR1");
    bytes[4..6].copy_from_slice(&(RECORD_HEADER_LENGTH as u16).to_le_bytes());
    bytes[6..8].copy_from_slice(&1_u16.to_le_bytes());
    bytes[8..12].copy_from_slice(&(length as u32).to_le_bytes());
    bytes[12..16].copy_from_slice(&(body.len() as u32).to_le_bytes());
    bytes[16..24].copy_from_slice(&sequence.to_le_bytes());
    bytes[24] = kind;
    bytes[28..60].copy_from_slice(&predecessor);
    bytes.extend_from_slice(body);
    bytes.extend_from_slice(&crc32c::crc32c(&bytes).to_le_bytes());
    bytes.extend_from_slice(&(length as u32).to_le_bytes());
    Ok(bytes)
}

fn segment_header(
    database_id: [u8; 16],
    segment: &storage::SegmentDescriptor,
) -> [u8; SEGMENT_HEADER_LENGTH] {
    let mut bytes = [0; SEGMENT_HEADER_LENGTH];
    bytes[..8].copy_from_slice(b"BLOPLG01");
    bytes[8..10].copy_from_slice(&1_u16.to_le_bytes());
    bytes[10..12].copy_from_slice(&(SEGMENT_HEADER_LENGTH as u16).to_le_bytes());
    bytes[16..32].copy_from_slice(&database_id);
    bytes[32..40].copy_from_slice(&segment.segment_id.to_le_bytes());
    bytes[40..48].copy_from_slice(&segment.first_sequence.to_le_bytes());
    bytes[48..80].copy_from_slice(&segment.predecessor_digest);
    let crc = crc32c::crc32c(&bytes);
    bytes[92..96].copy_from_slice(&crc.to_le_bytes());
    bytes
}

fn append(
    store: &mut storage::Store,
    sequence: u64,
    bytes: &[u8],
) -> storage::Result<[u8; 32]> {
    let mut manifest = store.manifest().clone();
    let digest = Sha256::digest(bytes).into();
    if let Some(segment) = manifest.segments.last_mut() {
        let committed_bytes = segment
            .committed_bytes
            .checked_add(bytes.len() as u64)
            .ok_or(storage::Error::Exhausted)?;
        let mut file = OpenOptions::new().append(true).open(
            store
                .directory()
                .join(format!("log-{:020}.bin", segment.segment_id)),
        )?;
        match file.metadata()?.len().cmp(&segment.committed_bytes) {
            std::cmp::Ordering::Less => {
                return Err(storage::Error::Corrupt("truncated active log segment"));
            }
            std::cmp::Ordering::Greater => return Err(storage::Error::NeedsRecovery),
            std::cmp::Ordering::Equal => {}
        }
        file.write_all(bytes)?;
        segment.last_sequence = sequence;
        segment.last_digest = digest;
        segment.committed_bytes = committed_bytes;
    } else {
        let (mut file, segment_id) = loop {
            let segment_id = manifest.next_segment_id;
            manifest.next_segment_id =
                segment_id.checked_add(1).ok_or(storage::Error::Exhausted)?;
            match OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(store.directory().join(format!("log-{segment_id:020}.bin")))
            {
                Ok(file) => break (file, segment_id),
                // An unlisted crash file does not allocate an identity, but its
                // contents must not be overwritten while finding a free name.
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error.into()),
            }
        };
        let segment = storage::SegmentDescriptor {
            segment_id,
            first_sequence: sequence,
            last_sequence: sequence,
            committed_bytes: (SEGMENT_HEADER_LENGTH + bytes.len()) as u64,
            predecessor_digest: manifest.durable_digest,
            last_digest: digest,
        };
        file.write_all(&segment_header(manifest.database_id, &segment))?;
        file.write_all(bytes)?;
        manifest.segments.push(segment);
    }
    manifest.durable_sequence = sequence;
    manifest.durable_digest = digest;
    let checkpoint = storage::checkpoint_view(store);
    // Publication flushes log files and directory entries before selecting D.
    storage::publish(store, &checkpoint, manifest)?;
    Ok(digest)
}

fn publish_checkpoint(store: &mut storage::Store) -> storage::Result<()> {
    let mut manifest = store.manifest().clone();
    manifest.checkpoint_sequence = manifest.durable_sequence;
    manifest.checkpoint_digest = manifest.durable_digest;
    let checkpoint = storage::view(store);
    storage::publish(store, &checkpoint, manifest)
}

/// Replay from freshly opened checkpoint roots, never from cached outcomes or
/// a store left partially materialized by a failed recovery attempt.
pub(super) fn recover(store: &mut storage::Store) -> storage::Result<()> {
    let manifest = store.manifest().clone();
    let checkpoint = storage::checkpoint_view(store);
    let history =
        vm::validate_history(&checkpoint, &manifest, store.genesis()).map_err(replay_error)?;
    let mut resolved = manifest.checkpoint_sequence;
    for segment in &manifest.segments {
        let file = File::open(
            store
                .directory()
                .join(format!("log-{:020}.bin", segment.segment_id)),
        )
        .map_err(authoritative_io)?;
        let mut reader = BufReader::new(file.take(segment.committed_bytes));
        let mut header = [0; SEGMENT_HEADER_LENGTH];
        reader.read_exact(&mut header).map_err(authoritative_io)?;
        if header != segment_header(manifest.database_id, segment) {
            return Err(storage::Error::Corrupt(
                "log segment header disagrees with manifest",
            ));
        }
        let mut remaining = segment
            .committed_bytes
            .checked_sub(SEGMENT_HEADER_LENGTH as u64)
            .ok_or(storage::Error::Corrupt(
                "invalid committed log prefix length",
            ))?;
        let mut predecessor = segment.predecessor_digest;
        for sequence in segment.first_sequence..=segment.last_sequence {
            let (bytes, digest) = read_record(&mut reader, remaining, sequence, predecessor)?;
            remaining -= bytes.len() as u64;
            if sequence == manifest.checkpoint_sequence && digest != manifest.checkpoint_digest {
                return Err(storage::Error::Corrupt("log checkpoint digest mismatch"));
            }
            let command = record::decode(bytes[24], &bytes[RECORD_HEADER_LENGTH..bytes.len() - 8])
                .map_err(replay_error)?;
            if sequence > manifest.checkpoint_sequence {
                if sequence != resolved + 1
                    || (resolved == manifest.checkpoint_sequence
                        && predecessor != manifest.checkpoint_digest)
                {
                    return Err(storage::Error::Corrupt("noncontiguous recovery suffix"));
                }
                record::validate(&storage::view(store), sequence, &command)
                    .map_err(replay_error)?;
                record::execute(store, sequence, digest, &command).map_err(replay_error)?;
                resolved = sequence;
            } else {
                validate_checkpoint_record(
                    &checkpoint,
                    &history,
                    sequence,
                    digest,
                    bytes[24],
                    &command,
                )
                .map_err(replay_error)?;
            }
            predecessor = digest;
        }
        if remaining != 0 || predecessor != segment.last_digest {
            return Err(storage::Error::Corrupt(
                "committed log prefix endpoint mismatch",
            ));
        }
    }
    if resolved != manifest.durable_sequence {
        return Err(storage::Error::Corrupt("incomplete recovery suffix"));
    }
    if manifest.checkpoint_sequence != manifest.durable_sequence {
        publish_checkpoint(store)?;
    }
    Ok(())
}

fn validate_checkpoint_record(
    view: &storage::View,
    history: &vm::History,
    sequence: u64,
    digest: [u8; 32],
    kind: u8,
    command: &record::Command,
) -> vm::Result<()> {
    record::validate(view, sequence, command)?;
    let outcome = vm::read_outcome(view, sequence)?;
    if outcome
        .as_ref()
        .is_some_and(|outcome| outcome.record_kind != kind || outcome.record_digest != digest)
        || history
            .version_kind(sequence)
            .is_some_and(|version_kind| version_kind != kind)
    {
        return Err(vm::Error::Invalid(
            "retained history disagrees with source record kind or digest",
        ));
    }
    match command {
        record::Command::Transaction {
            transaction,
            claims,
            ..
        } => {
            if let Some(outcome) = &outcome {
                vm::validate_transaction_outcome(view, sequence, transaction, claims, outcome)?;
            }
            Ok(())
        }
        record::Command::Catalogue(operation) => {
            vm::validate_catalogue_record(view, history, sequence, operation, outcome.as_ref())
        }
        record::Command::Limits(policy) => {
            vm::validate_limits_record(view, history, sequence, policy, outcome.as_ref())
        }
    }
}

fn authoritative_io(error: io::Error) -> storage::Error {
    match error.kind() {
        io::ErrorKind::UnexpectedEof | io::ErrorKind::NotFound => {
            storage::Error::Corrupt("missing bytes in authoritative log")
        }
        _ => storage::Error::Io(error),
    }
}

fn replay_error(error: vm::Error) -> storage::Error {
    match error {
        vm::Error::Invalid(reason) => storage::Error::Corrupt(reason),
        vm::Error::Unsupported { format, version } => {
            storage::Error::Unsupported { format, version }
        }
        vm::Error::Storage(storage::Error::InvalidInput(reason)) => storage::Error::Corrupt(reason),
        vm::Error::Storage(error) => error,
    }
}

fn read_record(
    reader: &mut impl Read,
    remaining: u64,
    sequence: u64,
    predecessor: [u8; 32],
) -> storage::Result<(Vec<u8>, [u8; 32])> {
    if remaining < RECORD_OVERHEAD as u64 {
        return Err(storage::Error::Corrupt(
            "record crosses committed log prefix",
        ));
    }
    let mut header = [0; RECORD_HEADER_LENGTH];
    reader.read_exact(&mut header).map_err(authoritative_io)?;
    let length = u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;
    let body_length = u32::from_le_bytes(header[12..16].try_into().unwrap()) as usize;
    if !(RECORD_OVERHEAD..=MAX_RECORD_LENGTH).contains(&length)
        || length as u64 > remaining
        || body_length != length - RECORD_OVERHEAD
    {
        return Err(storage::Error::Corrupt("invalid log record length"));
    }
    let mut bytes = vec![0; length];
    bytes[..RECORD_HEADER_LENGTH].copy_from_slice(&header);
    reader
        .read_exact(&mut bytes[RECORD_HEADER_LENGTH..])
        .map_err(authoritative_io)?;
    let crc = u32::from_le_bytes(bytes[length - 8..length - 4].try_into().unwrap());
    let repeated_length = u32::from_le_bytes(bytes[length - 4..].try_into().unwrap()) as usize;
    if crc32c::crc32c(&bytes[..length - 8]) != crc || repeated_length != length {
        return Err(storage::Error::Corrupt("invalid log record trailer"));
    }
    if &header[..4] != b"BLR1"
        || u16::from_le_bytes(header[4..6].try_into().unwrap()) != RECORD_HEADER_LENGTH as u16
    {
        return Err(storage::Error::Corrupt("invalid log record header"));
    }
    let version = u16::from_le_bytes(header[6..8].try_into().unwrap());
    if version != 1 {
        return Err(storage::Error::Unsupported {
            format: "log record",
            version,
        });
    }
    if !(1..=3).contains(&header[24])
        || header[25..28] != [0; 3]
        || header[60..64] != [0; 4]
        || u64::from_le_bytes(header[16..24].try_into().unwrap()) != sequence
        || header[28..60] != predecessor
    {
        return Err(storage::Error::Corrupt(
            "invalid log record fields or hash chain",
        ));
    }
    let digest = Sha256::digest(&bytes).into();
    Ok((bytes, digest))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::ops::Bound;

    use super::*;
    use crate::Transaction;
    use crate::storage::LimitPolicy;
    use crate::storage::TreeId;
    use crate::tx;
    use crate::vm::CatalogueOperation;
    use crate::vm::Outcome;
    use crate::vm::Type;
    use crate::vm::Value;

    fn policy() -> LimitPolicy {
        LimitPolicy::new([
            1_048_576, 1024, 1024, 64, 1_048_576, 64, 64, 1024, 1024, 1024, 1_048_576, 8_388_608,
            1024, 8_388_608, 1024, 8_388_608, 1_048_576,
        ])
        .unwrap()
    }

    fn create() -> (tempfile::TempDir, storage::Store) {
        let directory = tempfile::tempdir().unwrap();
        let store = storage::create(
            directory.path().join("db"),
            storage::Genesis {
                database_id: [1; 16],
                initial_policy: policy(),
            },
            [2; 16],
        )
        .unwrap();
        (directory, store)
    }

    fn transaction(transaction: Transaction) -> record::Command {
        let manifest = vm::AccessManifest::new(
            vm::transaction_tables(transaction.program_bytes())
                .unwrap()
                .into_iter()
                .map(|id| (vm::Scope::Table(id), vm::AccessMode::ReadWrite)),
        )
        .unwrap();
        record::Command::Transaction {
            transaction,
            claims: policy(),
            manifest: Some(manifest),
        }
    }

    #[test]
    fn appendix_h3_program_body_record_and_minimal_claims() {
        let bytes = "42 4c 4f 50 56 4d 30 31 01 00 00 00 4f 00 00 00
            01 00 00 00 00 00 01 00 02 00 00 00 00 00 00 00
            03 00 00 00 01 00 03
            03 00 00 00 01 00 03
            03 00 00 00 01 00 03 08 00 00 00 2a 00 00 00 00 00 00 00
            01 00 04 00 00 00 00 00
            64 00 02 00 00 00"
            .split_ascii_whitespace()
            .map(|byte| u8::from_str_radix(byte, 16).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(bytes.len(), 79);
        let command = record::Command::Transaction {
            transaction: Transaction::from_parts(bytes, vec![0; 4]),
            claims: LimitPolicy::new([79, 2, 1, 0, 4, 0, 0, 0, 0, 0, 8, 8, 0, 0, 0, 0, 8]).unwrap(),
            manifest: Some(vm::AccessManifest::default()),
        };
        let (_directory, mut store) = create();
        let (kind, body) = record::encode(&command).unwrap();
        assert_eq!(body.len(), 239);
        let record = envelope(1, store.manifest().durable_digest, kind, &body).unwrap();
        assert_eq!(record.len(), 311);
        assert_eq!(&record[8..12], &[0x37, 1, 0, 0]);
        assert_eq!(&record[307..], &[0x37, 1, 0, 0]);
        let receipt = commit(&mut store, &command).unwrap();
        assert_eq!(receipt.sequence, 1);
        assert_eq!(
            receipt.outcome,
            Outcome::Success {
                result_type: Type::U64,
                value: Value::U64(42),
                effects: vec![],
            }
        );
        assert_eq!(store.manifest().checkpoint_sequence, 1);
        let path = store.directory().to_owned();
        drop(store);
        recover(&mut storage::open(path).unwrap()).unwrap();
    }

    fn entries(
        store: &storage::Store,
        tree: TreeId,
    ) -> Vec<storage::Entry> {
        storage::scan(
            &storage::view(store),
            tree,
            Bound::Unbounded,
            Bound::Unbounded,
        )
        .unwrap()
        .collect::<storage::Result<_>>()
        .unwrap()
    }

    fn append_command(
        store: &mut storage::Store,
        command: &record::Command,
    ) -> [u8; 32] {
        let sequence = store.manifest().durable_sequence + 1;
        let (kind, body) = record::encode(command).unwrap();
        let bytes = envelope(sequence, store.manifest().durable_digest, kind, &body).unwrap();
        append(store, sequence, &bytes).unwrap()
    }

    #[test]
    fn writer_logs_derived_modes_without_unnecessarily_broadening_reads() {
        let (_directory, mut store) = create();
        commit(
            &mut store,
            &record::Command::Catalogue(CatalogueOperation::Create {
                name: "data".into(),
                key: Type::U64,
                value: Type::U64,
            }),
        )
        .unwrap();
        let command = record::Command::Transaction {
            transaction: tx! { tables { data: u64 => u64 = 1 } data[7] = 42; data[data[7]] = 1; }
                .unwrap(),
            claims: policy(),
            manifest: None,
        };
        commit(&mut store, &command).unwrap();
        let log = fs::read(store.directory().join("log-00000000000000000001.bin")).unwrap();
        let length = u32::from_le_bytes(log[log.len() - 4..].try_into().unwrap()) as usize;
        let body = &log[log.len() - length + RECORD_HEADER_LENGTH..log.len() - 8];
        let decoded = record::decode(1, body).unwrap();
        assert_eq!(record::encode(&decoded).unwrap().1, body);
        let record::Command::Transaction { manifest, .. } = decoded else {
            unreachable!()
        };
        assert_eq!(
            manifest.unwrap().entries(),
            [
                (vm::Scope::Table(1), vm::AccessMode::Write),
                (
                    vm::Scope::Key(1, 7_u64.to_be_bytes().to_vec()),
                    vm::AccessMode::Read
                ),
            ]
        );
        reopen_recovery(store).unwrap();
    }

    #[test]
    fn point_manifests_replay_durably_with_original_counts_and_historical_policy() {
        let (_directory, mut store) = create();
        commit(
            &mut store,
            &record::Command::Catalogue(CatalogueOperation::Create {
                name: "counter".into(),
                key: Type::U64,
                value: Type::U64,
            }),
        )
        .unwrap();
        let supplied = vm::AccessManifest::new([
            (
                vm::Scope::Key(1, 7_u64.to_be_bytes().to_vec()),
                vm::AccessMode::ReadWrite,
            ),
            (
                vm::Scope::Key(1, 8_u64.to_be_bytes().to_vec()),
                vm::AccessMode::Read,
            ),
        ])
        .unwrap();
        let mut values = *policy().values();
        values[6] = 2;
        let claims = LimitPolicy::new(values).unwrap();
        for transaction in [
            tx! { tables { counter: u64 => u64 = 1 } insert(counter[7], 40); }.unwrap(),
            tx! { tables { counter: u64 => u64 = 1 } counter[7] += 2; return counter[7]; }.unwrap(),
        ] {
            let command = record::Command::Transaction {
                transaction,
                claims: claims.clone(),
                manifest: Some(supplied.clone()),
            };
            record::validate(
                &storage::view(&store),
                store.manifest().durable_sequence + 1,
                &command,
            )
            .unwrap();
            append_command(&mut store, &command);
        }
        values[6] = 1;
        append_command(
            &mut store,
            &record::Command::Limits(LimitPolicy::new(values).unwrap()),
        );
        let path = store.directory().to_owned();
        let original = fs::read(path.join("log-00000000000000000001.bin")).unwrap();
        assert!(entries(&store, TreeId::State).is_empty());
        drop(store);
        let mut store = storage::open(&path).unwrap();
        recover(&mut store).unwrap();
        let outcome = vm::read_outcome(&storage::view(&store), 3)
            .unwrap()
            .unwrap();
        assert!(matches!(
            outcome.outcome,
            Outcome::Success {
                value: Value::U64(42),
                ..
            }
        ));
        assert_eq!(store.manifest().checkpoint_sequence, 4);
        assert_eq!(
            fs::read(path.join("log-00000000000000000001.bin")).unwrap(),
            original
        );
        // Also validate the retained point manifests after they are
        // checkpointed.
        reopen_recovery(store).unwrap();
    }

    #[test]
    fn invalid_manifests_do_not_append_consume_sequences_or_install_outcomes() {
        let (_directory, mut store) = create();
        commit(
            &mut store,
            &record::Command::Catalogue(CatalogueOperation::Create {
                name: "data".into(),
                key: Type::Boolean,
                value: Type::U64,
            }),
        )
        .unwrap();
        let before = store.manifest().clone();
        let path = store.directory().join("log-00000000000000000001.bin");
        let log = fs::read(&path).unwrap();
        let outcomes = entries(&store, TreeId::Outcomes);
        for scopes in [
            vec![],
            vec![(vm::Scope::Key(1, vec![0]), vm::AccessMode::Write)],
            vec![(vm::Scope::Key(1, vec![1]), vm::AccessMode::ReadWrite)],
            vec![(vm::Scope::Key(1, vec![2]), vm::AccessMode::ReadWrite)],
            vec![(vm::Scope::Table(2), vm::AccessMode::ReadWrite)],
        ] {
            let command = record::Command::Transaction {
                transaction: tx! { tables { data: bool => u64 = 1 } insert(data[false], 42); }
                    .unwrap(),
                claims: policy(),
                manifest: Some(vm::AccessManifest::new(scopes).unwrap()),
            };
            assert!(matches!(
                commit(&mut store, &command),
                Err(Error::Rejected(_))
            ));
            assert_eq!(store.manifest(), &before);
            assert_eq!(fs::read(&path).unwrap(), log);
            assert_eq!(entries(&store, TreeId::Outcomes), outcomes);
        }
        assert_eq!(
            commit(&mut store, &transaction(tx! { return 42; }.unwrap()))
                .unwrap()
                .sequence,
            2
        );
    }

    #[test]
    fn insufficient_or_schema_invalid_durable_manifests_are_corruption() {
        for checkpointed in [false, true] {
            for scope in [
                vm::Scope::Key(1, vec![0]),
                vm::Scope::Key(1, vec![2]),
                vm::Scope::Table(2),
            ] {
                let (_directory, mut store) = create();
                commit(
                    &mut store,
                    &record::Command::Catalogue(CatalogueOperation::Create {
                        name: "data".into(),
                        key: Type::Boolean,
                        value: Type::U64,
                    }),
                )
                .unwrap();
                let command = record::Command::Transaction {
                    transaction: tx! { tables { data: bool => u64 = 1 } insert(data[false], 42); }
                        .unwrap(),
                    claims: policy(),
                    manifest: Some(
                        vm::AccessManifest::new([(scope, vm::AccessMode::Write)]).unwrap(),
                    ),
                };
                let digest = append_command(&mut store, &command);
                if checkpointed {
                    let outcome = Outcome::Success {
                        result_type: Type::Unit,
                        value: Value::Unit,
                        effects: vec![],
                    };
                    checkpoint_changes(
                        &mut store,
                        &[change(
                            TreeId::Outcomes,
                            2_u64.to_be_bytes().to_vec(),
                            Some(vm::encode_outcome(2, digest, 1, &outcome).unwrap()),
                        )],
                    );
                }
                assert!(matches!(
                    reopen_recovery(store),
                    Err(storage::Error::Corrupt(_))
                ));
            }
        }
    }

    #[test]
    fn commit_persists_roots_outcomes_and_complete_record_digests() {
        let (_directory, mut store) = create();
        let commands = [
            record::Command::Catalogue(CatalogueOperation::Create {
                name: "counter".into(),
                key: Type::U64,
                value: Type::I64,
            }),
            transaction(
                tx! { tables {counter:u64=>i64=1} counter[7] = 40; return counter[7]; }.unwrap(),
            ),
            record::Command::Limits(policy()),
        ];
        let path = store.directory().to_owned();
        for (index, command) in commands.iter().enumerate() {
            let receipt = commit(&mut store, command).unwrap();
            assert_eq!(receipt.sequence, index as u64 + 1);
            assert!(matches!(receipt.outcome, Outcome::Success { .. }));
            assert_eq!(store.manifest().checkpoint_sequence, receipt.sequence);
            assert_eq!(store.manifest().durable_sequence, receipt.sequence);
            let outcome = storage::get(
                &storage::view(&store),
                TreeId::Outcomes,
                &receipt.sequence.to_be_bytes(),
            )
            .unwrap()
            .unwrap();
            assert_eq!(&outcome[12..44], &store.manifest().durable_digest);
        }
        assert_eq!(store.manifest().segments.len(), 1);
        let trees = [
            TreeId::State,
            TreeId::Catalogue,
            TreeId::Policy,
            TreeId::Outcomes,
        ];
        let expected = trees.map(|tree| entries(&store, tree));
        let manifest = store.manifest().clone();
        drop(store);
        let mut store = storage::open(path).unwrap();
        recover(&mut store).unwrap();
        assert_eq!(store.manifest(), &manifest);
        assert_eq!(trees.map(|tree| entries(&store, tree)), expected);
    }

    #[test]
    fn durable_unexecuted_catalogue_state_and_policy_replay_in_order() {
        let (_directory, mut store) = create();
        append_command(
            &mut store,
            &record::Command::Catalogue(CatalogueOperation::Create {
                name: "counter".into(),
                key: Type::U64,
                value: Type::I64,
            }),
        );
        append_command(
            &mut store,
            &transaction(tx! { tables {counter:u64=>i64=1} counter[7] = 40; }.unwrap()),
        );
        append_command(
            &mut store,
            &transaction(tx! { tables {counter:u64=>i64=1} counter[7] += 2; }.unwrap()),
        );
        append_command(&mut store, &record::Command::Limits(policy()));
        assert!(entries(&store, TreeId::State).is_empty());
        assert!(entries(&store, TreeId::Catalogue).is_empty());
        assert!(entries(&store, TreeId::Outcomes).is_empty());
        let path = store.directory().to_owned();
        drop(store);
        let mut store = storage::open(path).unwrap();
        recover(&mut store).unwrap();
        assert_eq!(store.manifest().checkpoint_sequence, 4);
        assert_eq!(entries(&store, TreeId::Outcomes).len(), 4);
        assert_eq!(entries(&store, TreeId::Policy).len(), 2);
        let receipt = commit(
            &mut store,
            &transaction(tx! { tables {counter:u64=>i64=1} return counter[7]; }.unwrap()),
        )
        .unwrap();
        assert!(matches!(
            receipt.outcome,
            Outcome::Success {
                value: Value::I64(42),
                ..
            }
        ));
    }

    #[test]
    fn post_checkpoint_materialization_is_discarded_and_recovered_increment_runs_once() {
        let (_directory, mut store) = create();
        let create_table = record::Command::Catalogue(CatalogueOperation::Create {
            name: "counter".into(),
            key: Type::U64,
            value: Type::I64,
        });
        commit(&mut store, &create_table).unwrap();
        commit(
            &mut store,
            &transaction(tx! { tables {counter:u64=>i64=1} counter[7] = 40; }.unwrap()),
        )
        .unwrap();
        let increment = transaction(
            tx! { tables {counter:u64=>i64=1} counter[7] += 2; return counter[7]; }.unwrap(),
        );
        let digest = append_command(&mut store, &increment);
        assert_eq!(entries(&store, TreeId::Outcomes).len(), 2);
        assert_eq!(store.manifest().checkpoint_sequence, 2);
        assert_eq!(store.manifest().durable_sequence, 3);
        // Pages and outcomes written after C are deliberately not a replay
        // base.
        record::execute(&mut store, 3, digest, &increment).unwrap();
        let expected = entries(&store, TreeId::State);
        let path = store.directory().to_owned();
        drop(store);
        let mut store = storage::open(&path).unwrap();
        assert_eq!(entries(&store, TreeId::Outcomes).len(), 2);
        recover(&mut store).unwrap();
        assert_eq!(entries(&store, TreeId::State), expected);
        assert_eq!(entries(&store, TreeId::Outcomes).len(), 3);
        let manifest = store.manifest().clone();
        drop(store);
        let mut store = storage::open(path).unwrap();
        recover(&mut store).unwrap();
        assert_eq!(store.manifest(), &manifest);
        assert_eq!(entries(&store, TreeId::State), expected);
        let receipt = commit(
            &mut store,
            &transaction(tx! { tables {counter:u64=>i64=1} return counter[7]; }.unwrap()),
        )
        .unwrap();
        assert!(matches!(
            receipt.outcome,
            Outcome::Success {
                value: Value::I64(42),
                ..
            }
        ));
    }

    #[test]
    fn replay_resolves_aborts_and_later_records() {
        let (_directory, mut store) = create();
        append_command(&mut store, &transaction(tx! { abort(19); }.unwrap()));
        append_command(&mut store, &transaction(tx! { return 42; }.unwrap()));
        assert!(entries(&store, TreeId::Outcomes).is_empty());
        let path = store.directory().to_owned();
        drop(store);
        let mut store = storage::open(path).unwrap();
        recover(&mut store).unwrap();
        assert_eq!(store.manifest().checkpoint_sequence, 2);
        assert_eq!(entries(&store, TreeId::Outcomes).len(), 2);
        let outcomes = entries(&store, TreeId::Outcomes);
        assert_eq!(&outcomes[0].1[..4], &[1, 0, 1, 1]);
        assert_eq!(&outcomes[1].1[..4], &[1, 0, 1, 0]);
        assert!(entries(&store, TreeId::State).is_empty());
    }

    #[test]
    fn malformed_authoritative_body_never_publishes_a_partial_checkpoint() {
        let (_directory, mut store) = create();
        append_command(&mut store, &transaction(tx! { return 7; }.unwrap()));
        let bytes = envelope(2, store.manifest().durable_digest, 1, &[0]).unwrap();
        append(&mut store, 2, &bytes).unwrap();
        let path = store.directory().to_owned();
        drop(store);
        let mut store = storage::open(&path).unwrap();
        let manifest = store.manifest().clone();
        assert!(matches!(
            recover(&mut store),
            Err(storage::Error::Corrupt(_))
        ));
        assert_eq!(store.manifest(), &manifest);
        assert_eq!(entries(&store, TreeId::Outcomes).len(), 1);
        drop(store);
        let mut store = storage::open(path).unwrap();
        assert!(entries(&store, TreeId::Outcomes).is_empty());
        assert!(matches!(
            recover(&mut store),
            Err(storage::Error::Corrupt(_))
        ));
        assert_eq!(store.manifest(), &manifest);
    }

    #[test]
    fn recovery_validates_claims_against_replayed_policy() {
        let (_directory, mut store) = create();
        append_command(
            &mut store,
            &record::Command::Limits(LimitPolicy::new([0; 17]).unwrap()),
        );
        append_command(&mut store, &transaction(tx! { return 42; }.unwrap()));
        let path = store.directory().to_owned();
        drop(store);
        let mut store = storage::open(path).unwrap();
        assert!(matches!(
            recover(&mut store),
            Err(storage::Error::Corrupt(_))
        ));
        assert_eq!(store.manifest().checkpoint_sequence, 0);
        assert_eq!(store.manifest().durable_sequence, 2);
    }

    #[test]
    fn unpublished_valid_tail_is_ignored_and_append_reuses_only_its_position() {
        let (_directory, mut store) = create();
        commit(&mut store, &transaction(tx! { return 1; }.unwrap())).unwrap();
        let segment = store.manifest().segments[0];
        let path = store.directory().to_owned();
        let log = path.join(format!("log-{:020}.bin", segment.segment_id));
        let prefix = fs::read(&log).unwrap();
        let (kind, body) = record::encode(&transaction(tx! { return 999; }.unwrap())).unwrap();
        let bytes = envelope(2, segment.last_digest, kind, &body).unwrap();
        let mut file = OpenOptions::new().append(true).open(&log).unwrap();
        file.write_all(&bytes).unwrap();
        file.sync_all().unwrap();
        drop(file);
        drop(store);
        let mut store = storage::open(path).unwrap();
        recover(&mut store).unwrap();
        assert_eq!(fs::read(&log).unwrap(), prefix);
        assert_eq!(entries(&store, TreeId::Outcomes).len(), 1);
        let receipt = commit(&mut store, &transaction(tx! { return 2; }.unwrap())).unwrap();
        assert_eq!(receipt.sequence, 2);
        assert!(matches!(
            receipt.outcome,
            Outcome::Success {
                value: Value::I64(2),
                ..
            }
        ));
        assert_eq!(&fs::read(log).unwrap()[..prefix.len()], prefix);
    }

    #[test]
    fn initial_segment_creation_skips_unlisted_orphans_without_overwriting_them() {
        let (_directory, store) = create();
        let path = store.directory().to_owned();
        let orphan = path.join("log-00000000000000000001.bin");
        fs::write(&orphan, b"unpublished crash output").unwrap();
        drop(store);
        let mut store = storage::open(&path).unwrap();
        recover(&mut store).unwrap();
        commit(&mut store, &transaction(tx! { return 42; }.unwrap())).unwrap();
        assert_eq!(store.manifest().segments[0].segment_id, 2);
        assert_eq!(store.manifest().next_segment_id, 3);
        assert_eq!(fs::read(&orphan).unwrap(), b"unpublished crash output");
        drop(store);
        let mut store = storage::open(path).unwrap();
        recover(&mut store).unwrap();
        assert_eq!(store.manifest().durable_sequence, 1);
    }

    #[test]
    fn rejected_input_does_not_append_or_consume_a_sequence() {
        let (_directory, mut store) = create();
        commit(
            &mut store,
            &record::Command::Limits(LimitPolicy::new([0; 17]).unwrap()),
        )
        .unwrap();
        let before = store.manifest().clone();
        let log = store
            .directory()
            .join(format!("log-{:020}.bin", before.segments[0].segment_id));
        let bytes = fs::read(&log).unwrap();
        assert!(matches!(
            commit(&mut store, &transaction(tx! { return 42; }.unwrap())),
            Err(Error::Rejected(_))
        ));
        assert_eq!(store.manifest(), &before);
        assert_eq!(fs::read(log).unwrap(), bytes);
        assert_eq!(
            commit(&mut store, &record::Command::Limits(policy()))
                .unwrap()
                .sequence,
            2
        );
    }

    #[test]
    fn append_failure_is_uncertain_and_never_executes() {
        let (_directory, mut store) = create();
        commit(&mut store, &transaction(tx! { return 1; }.unwrap())).unwrap();
        let log = store.directory().join(format!(
            "log-{:020}.bin",
            store.manifest().segments[0].segment_id
        ));
        let before = entries(&store, TreeId::Outcomes);
        fs::remove_file(log).unwrap();
        assert!(matches!(
            commit(&mut store, &transaction(tx! { return 2; }.unwrap())),
            Err(Error::Uncertain {
                sequence: Some(2),
                source: Some(vm::Error::Storage(_))
            })
        ));
        assert_eq!(entries(&store, TreeId::Outcomes), before);
        assert_eq!(store.manifest().durable_sequence, 1);
    }

    #[test]
    fn framing_rejects_truncation_bad_lengths_and_checksums() {
        let original = envelope(1, [7; 32], 1, &[1, 2, 3]).unwrap();
        let (bytes, digest) =
            read_record(&mut original.as_slice(), original.len() as u64, 1, [7; 32]).unwrap();
        assert_eq!(bytes, original);
        assert_eq!(digest, <[u8; 32]>::from(Sha256::digest(&original)));
        for length in 0..original.len() {
            assert!(matches!(
                read_record(&mut &original[..length], original.len() as u64, 1, [7; 32]),
                Err(storage::Error::Corrupt(_))
            ));
        }
        for index in [8, 12, 24, 28, 64, original.len() - 8, original.len() - 1] {
            let mut bytes = original.clone();
            bytes[index] ^= 1;
            assert!(matches!(
                read_record(&mut bytes.as_slice(), bytes.len() as u64, 1, [7; 32]),
                Err(storage::Error::Corrupt(_))
            ));
        }
        let mut header = [0; RECORD_HEADER_LENGTH];
        header[8..12].copy_from_slice(&((MAX_RECORD_LENGTH + 1) as u32).to_le_bytes());
        header[12..16]
            .copy_from_slice(&((MAX_RECORD_LENGTH + 1 - RECORD_OVERHEAD) as u32).to_le_bytes());
        assert!(matches!(
            read_record(&mut header.as_slice(), u64::MAX, 1, [7; 32]),
            Err(storage::Error::Corrupt(_))
        ));
    }

    fn retained_history() -> (tempfile::TempDir, storage::Store) {
        let (directory, mut store) = create();
        for command in [
            record::Command::Catalogue(CatalogueOperation::Create {
                name: "flags".into(), key: Type::Boolean, value: Type::Boolean,
            }),
            transaction(tx! { tables { flags: bool => bool = 1 } flags[false] = true; flags[true] = false; return true; }.unwrap()),
            transaction(tx! { tables { flags: bool => bool = 1 } flags[false] = false; }.unwrap()),
            record::Command::Catalogue(CatalogueOperation::Rename { table: 1, name: "renamed".into() }),
            record::Command::Limits(policy()),
            transaction(tx! { abort(19); }.unwrap()),
        ] {
            commit(&mut store, &command).unwrap();
        }
        assert_eq!(store.manifest().checkpoint_sequence, 6);
        assert_eq!(store.manifest().durable_sequence, 6);
        (directory, store)
    }

    fn checkpoint_changes(
        store: &mut storage::Store,
        changes: &[storage::Mutation],
    ) {
        storage::apply(store, changes).unwrap();
        // This deliberately uses only the physical publisher: checksums and
        // framing are valid, while the logical database contents may not be.
        publish_checkpoint(store).unwrap();
    }

    fn change(
        tree: TreeId,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
    ) -> storage::Mutation {
        storage::Mutation { tree, key, value }
    }

    fn state_key(
        key: u8,
        sequence: u64,
    ) -> Vec<u8> {
        storage::mvcc::StateKey::new(1, vec![key], sequence)
            .unwrap()
            .encode()
    }

    fn stored(
        store: &storage::Store,
        tree: TreeId,
        key: &[u8],
    ) -> Vec<u8> {
        storage::get(&storage::view(store), tree, key)
            .unwrap()
            .unwrap()
    }

    fn reopen_recovery(store: storage::Store) -> storage::Result<()> {
        let path = store.directory().to_owned();
        drop(store);
        let mut store = storage::open(path)?;
        let before = store.manifest().clone();
        let result = recover(&mut store);
        assert_eq!(store.manifest(), &before);
        result
    }

    #[tokio::test]
    async fn public_open_rejects_missing_retained_outcomes_even_when_c_equals_d() {
        for sequence in [1_u64, 3, 6] {
            let (_directory, mut store) = retained_history();
            checkpoint_changes(
                &mut store,
                &[change(
                    TreeId::Outcomes,
                    sequence.to_be_bytes().to_vec(),
                    None,
                )],
            );
            let path = store.directory().to_owned();
            drop(store);
            assert!(matches!(
                crate::database::open(path).await,
                Err(Error::Storage(storage::Error::Corrupt(_)))
            ));
        }
    }

    #[test]
    fn recovery_rejects_checksum_valid_malformed_outcomes_and_old_catalogue_values() {
        for case in 0..8 {
            let (_directory, mut store) = retained_history();
            let mutation = match case {
                0 => change(
                    TreeId::Outcomes,
                    2_u64.to_be_bytes().to_vec(),
                    Some(vec![1]),
                ),
                1 | 2 => {
                    let mut bytes = stored(&store, TreeId::Outcomes, &2_u64.to_be_bytes());
                    bytes[if case == 1 { 4 } else { 46 }] ^= 1;
                    change(TreeId::Outcomes, 2_u64.to_be_bytes().to_vec(), Some(bytes))
                }
                3 | 4 => {
                    let key = vm::catalogue_key(1, 1);
                    let mut bytes = stored(&store, TreeId::Catalogue, &key);
                    if case == 3 {
                        bytes[3] = 1;
                    } else {
                        bytes[8] = 0;
                    }
                    change(TreeId::Catalogue, key, Some(bytes))
                }
                5 => {
                    let key = vm::catalogue_key(1, 4);
                    let mut catalogue =
                        vm::decode_catalogue(1, &stored(&store, TreeId::Catalogue, &key)).unwrap();
                    catalogue.table.value = Type::Unit;
                    change(
                        TreeId::Catalogue,
                        key,
                        Some(vm::encode_catalogue(&catalogue).unwrap()),
                    )
                }
                6 => change(
                    TreeId::State,
                    state_key(0, 2),
                    Some(storage::mvcc::StateValue::Put(vec![2]).encode().unwrap()),
                ),
                7 => change(
                    TreeId::State,
                    state_key(2, 2),
                    Some(storage::mvcc::StateValue::Delete.encode().unwrap()),
                ),
                _ => unreachable!(),
            };
            checkpoint_changes(&mut store, &[mutation]);
            assert!(
                matches!(reopen_recovery(store), Err(storage::Error::Corrupt(_))),
                "case {case}"
            );
        }
    }

    #[test]
    fn recovery_checks_exact_effects_not_latest_values_and_rejects_extra_versions() {
        for case in 0..6 {
            let (_directory, mut store) = retained_history();
            let mutation = match case {
                0 => change(TreeId::State, state_key(0, 2), None),
                1 => change(
                    TreeId::State,
                    state_key(0, 2),
                    Some(storage::mvcc::StateValue::Put(vec![0]).encode().unwrap()),
                ),
                2 => change(
                    TreeId::State,
                    state_key(0, 2),
                    Some(storage::mvcc::StateValue::Delete.encode().unwrap()),
                ),
                3 => change(
                    TreeId::State,
                    state_key(1, 3),
                    Some(storage::mvcc::StateValue::Delete.encode().unwrap()),
                ),
                4 => change(
                    TreeId::State,
                    state_key(1, 6),
                    Some(storage::mvcc::StateValue::Delete.encode().unwrap()),
                ),
                5 => {
                    let mut outcome = vm::read_outcome(&storage::view(&store), 2)
                        .unwrap()
                        .unwrap();
                    let Outcome::Success { effects, .. } = &mut outcome.outcome else {
                        unreachable!()
                    };
                    let vm::Effect::Put { value, .. } = &mut effects[0] else {
                        unreachable!()
                    };
                    value[0] ^= 1;
                    change(
                        TreeId::Outcomes,
                        2_u64.to_be_bytes().to_vec(),
                        Some(
                            vm::encode_outcome(2, outcome.record_digest, 1, &outcome.outcome)
                                .unwrap(),
                        ),
                    )
                }
                _ => unreachable!(),
            };
            checkpoint_changes(&mut store, &[mutation]);
            assert!(
                matches!(reopen_recovery(store), Err(storage::Error::Corrupt(_))),
                "case {case}"
            );
        }
    }

    #[test]
    fn retained_log_checks_digest_result_descriptor_and_abort_instruction_fields() {
        for case in 0..4 {
            let (_directory, mut store) = retained_history();
            let sequence = if case < 2 { 2 } else { 6 };
            let mut outcome = vm::read_outcome(&storage::view(&store), sequence)
                .unwrap()
                .unwrap();
            match case {
                0 => outcome.record_digest[0] ^= 1,
                1 => {
                    let Outcome::Success {
                        result_type, value, ..
                    } = &mut outcome.outcome
                    else {
                        unreachable!()
                    };
                    *result_type = Type::Unit;
                    *value = Value::Unit;
                }
                2 | 3 => {
                    let Outcome::Aborted(abort) = &mut outcome.outcome else {
                        unreachable!()
                    };
                    if case == 2 {
                        abort.instruction = 1;
                    } else {
                        abort.user_code = 20;
                    }
                }
                _ => unreachable!(),
            }
            let bytes =
                vm::encode_outcome(sequence, outcome.record_digest, 1, &outcome.outcome).unwrap();
            checkpoint_changes(
                &mut store,
                &[change(
                    TreeId::Outcomes,
                    sequence.to_be_bytes().to_vec(),
                    Some(bytes),
                )],
            );
            // The standalone logical checkpoint is well-formed; source records
            // expose the discrepancy without executing their old writes again.
            vm::validate_history(
                &storage::checkpoint_view(&store),
                store.manifest(),
                store.genesis(),
            )
            .unwrap();
            assert!(
                matches!(reopen_recovery(store), Err(storage::Error::Corrupt(_))),
                "case {case}"
            );
        }
    }

    #[test]
    fn checkpointed_log_bodies_are_validated_even_without_a_replay_suffix() {
        for body in [vec![0], vec![1, 0, 1, 0], vec![2, 0]] {
            let (_directory, mut store) = create();
            let bytes = envelope(1, store.manifest().durable_digest, 1, &body).unwrap();
            let digest = append(&mut store, 1, &bytes).unwrap();
            let outcome = Outcome::Success {
                result_type: Type::Unit,
                value: Value::Unit,
                effects: vec![],
            };
            checkpoint_changes(
                &mut store,
                &[change(
                    TreeId::Outcomes,
                    1_u64.to_be_bytes().to_vec(),
                    Some(vm::encode_outcome(1, digest, 1, &outcome).unwrap()),
                )],
            );
            let result = reopen_recovery(store);
            if body == [2, 0] {
                assert!(matches!(
                    result,
                    Err(storage::Error::Unsupported {
                        format: "transaction",
                        version: 2
                    })
                ));
            } else {
                assert!(matches!(result, Err(storage::Error::Corrupt(_))));
            }
        }
    }

    #[test]
    fn recovery_preserves_unsupported_outcomes_catalogue_and_historical_schemas() {
        for case in 0..3 {
            let (_directory, mut store) = retained_history();
            let (tree, key, offset) = if case == 0 {
                (TreeId::Outcomes, 2_u64.to_be_bytes().to_vec(), 0)
            } else {
                // The old name is five bytes; its first schema starts at 17.
                (
                    TreeId::Catalogue,
                    vm::catalogue_key(1, 1),
                    if case == 1 { 0 } else { 17 },
                )
            };
            let mut bytes = stored(&store, tree, &key);
            bytes[offset] = 2;
            checkpoint_changes(&mut store, &[change(tree, key, Some(bytes))]);
            assert!(
                matches!(
                    reopen_recovery(store),
                    Err(storage::Error::Unsupported { version: 2, .. })
                ),
                "case {case}"
            );
        }
    }

    fn reclaim_history(
        store: &mut storage::Store,
        floor: u64,
        remove_log: bool,
    ) {
        let mut changes: Vec<_> = (1..=floor)
            .map(|sequence| change(TreeId::Outcomes, sequence.to_be_bytes().to_vec(), None))
            .collect();
        if floor >= 3 {
            // false@3 is the baseline, but true@2 must survive without outcome
            // 2.
            changes.push(change(TreeId::State, state_key(0, 2), None));
        }
        storage::apply(store, &changes).unwrap();
        let mut manifest = store.manifest().clone();
        manifest.history_floor = floor;
        if remove_log {
            manifest.log_floor = manifest.checkpoint_sequence + 1;
            manifest.segments.clear();
        }
        let view = storage::view(store);
        storage::publish(store, &view, manifest).unwrap();
    }

    #[test]
    fn valid_dropped_tables_and_history_floors_recover_with_or_without_source_log() {
        for remove_log in [false, true] {
            for floor in [0, 3, 11] {
                let (_directory, mut store) = retained_history();
                commit(
                    &mut store,
                    &record::Command::Catalogue(CatalogueOperation::Drop { table: 1 }),
                )
                .unwrap();
                let receipt = commit(
                    &mut store,
                    &record::Command::Catalogue(CatalogueOperation::Create {
                        name: "renamed".into(),
                        key: Type::U64,
                        value: Type::I64,
                    }),
                )
                .unwrap();
                assert_eq!(receipt.sequence, 8);
                for operation in [
                    CatalogueOperation::Create {
                        name: "renamed".into(),
                        key: Type::U64,
                        value: Type::I64,
                    },
                    CatalogueOperation::Drop { table: 1 },
                ] {
                    assert!(matches!(
                        commit(&mut store, &record::Command::Catalogue(operation))
                            .unwrap()
                            .outcome,
                        Outcome::Aborted(_)
                    ));
                }
                commit(
                    &mut store,
                    &record::Command::Limits(LimitPolicy::new([0; 17]).unwrap()),
                )
                .unwrap();
                reclaim_history(&mut store, floor, remove_log);
                reopen_recovery(store).unwrap();
            }
        }
    }

    #[test]
    fn retained_sources_require_all_catalogue_and_policy_versions_after_outcome_gc() {
        for tree in [TreeId::Catalogue, TreeId::Policy] {
            let (_directory, mut store) = retained_history();
            reclaim_history(&mut store, 6, false);
            let key = if tree == TreeId::Catalogue {
                vm::catalogue_key(1, 4)
            } else {
                5_u64.to_be_bytes().to_vec()
            };
            checkpoint_changes(&mut store, &[change(tree, key, None)]);
            vm::validate_history(
                &storage::checkpoint_view(&store),
                store.manifest(),
                store.genesis(),
            )
            .unwrap();
            assert!(matches!(
                reopen_recovery(store),
                Err(storage::Error::Corrupt(_))
            ));
        }
    }

    #[test]
    fn optional_old_outcomes_allow_reclaimed_versions_but_not_contradictions() {
        for case in 0..3 {
            let (_directory, mut store) = retained_history();
            let mut changes = vec![
                change(TreeId::State, state_key(0, 2), None),
                change(TreeId::Outcomes, 1_u64.to_be_bytes().to_vec(), None),
            ];
            if case != 0 {
                changes.push(change(
                    TreeId::State,
                    state_key(1, if case == 1 { 2 } else { 3 }),
                    Some(storage::mvcc::StateValue::Put(vec![1]).encode().unwrap()),
                ));
            }
            storage::apply(&mut store, &changes).unwrap();
            let mut manifest = store.manifest().clone();
            manifest.history_floor = 3;
            let checkpoint = storage::view(&store);
            storage::publish(&mut store, &checkpoint, manifest).unwrap();
            drop(checkpoint);
            let result = reopen_recovery(store);
            if case == 0 {
                result.unwrap();
            } else {
                assert!(matches!(result, Err(storage::Error::Corrupt(_))));
            }
        }
    }

    #[test]
    fn baseline_versions_still_require_historical_liveness_without_outcomes_or_log() {
        let (_directory, mut store) = retained_history();
        commit(
            &mut store,
            &record::Command::Catalogue(CatalogueOperation::Drop { table: 1 }),
        )
        .unwrap();
        commit(&mut store, &transaction(tx! { return 1; }.unwrap())).unwrap();
        reclaim_history(&mut store, 8, true);
        checkpoint_changes(
            &mut store,
            &[change(
                TreeId::State,
                state_key(1, 8),
                Some(storage::mvcc::StateValue::Put(vec![1]).encode().unwrap()),
            )],
        );
        assert!(matches!(
            reopen_recovery(store),
            Err(storage::Error::Corrupt(_))
        ));
    }

    #[test]
    fn entire_catalogue_history_is_validated_without_outcomes_or_source_log() {
        for case in 0..4 {
            let (_directory, mut store) = retained_history();
            let operations = if case == 3 {
                vec![
                    CatalogueOperation::Create {
                        name: "flags".into(),
                        key: Type::Boolean,
                        value: Type::Boolean,
                    },
                    CatalogueOperation::Rename {
                        table: 7,
                        name: "other".into(),
                    },
                ]
            } else {
                vec![
                    CatalogueOperation::Drop { table: 1 },
                    CatalogueOperation::Drop { table: 1 },
                ]
            };
            for operation in operations {
                commit(&mut store, &record::Command::Catalogue(operation)).unwrap();
            }
            reclaim_history(&mut store, 8, true);
            let mutation = if case == 0 {
                change(TreeId::Catalogue, vm::catalogue_key(1, 1), None)
            } else {
                let (id, sequence) = if case == 3 { (7, 7) } else { (1, 7) };
                let mut catalogue = vm::decode_catalogue(
                    id,
                    &stored(&store, TreeId::Catalogue, &vm::catalogue_key(id, sequence)),
                )
                .unwrap();
                let effective = if case == 2 {
                    catalogue.live = true;
                    8
                } else {
                    catalogue.name = if case == 3 {
                        "renamed"
                    } else {
                        "changed-drop-name"
                    }
                    .into();
                    sequence
                };
                change(
                    TreeId::Catalogue,
                    vm::catalogue_key(id, effective),
                    Some(vm::encode_catalogue(&catalogue).unwrap()),
                )
            };
            checkpoint_changes(&mut store, &[mutation]);
            assert!(
                matches!(reopen_recovery(store), Err(storage::Error::Corrupt(_))),
                "case {case}"
            );
        }
    }
}
