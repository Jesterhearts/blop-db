//! Single-threaded ISA 1 reference execution over the storage layer.
//!
//! The pipeline is bytes -> validated typed instructions -> private overlay ->
//! outcome -> atomic storage batch. No scheduler or parallel execution is used.
//!
//! This is an engine-facing reference implementation, not a durable submission
//! API. Callers must protect history and either use an isolated reference store
//! or establish log durability before execution. These functions do not append
//! logs, publish checkpoints, advance public visibility, or perform recovery.
//! Recovery must restore checkpoint roots before replaying every later record.
//! Access manifests and complete logged transaction bodies are not handled
//! here.

mod database;
mod operations;
mod program;
mod runtime;
mod value;

use std::fmt;
use std::ops::Bound;

pub use database::CatalogueOperation;
pub(crate) use database::validate_catalogue;
pub use value::Type;
pub use value::Value;

use crate::Transaction;
use crate::storage;
use crate::storage::LimitPolicy;
use crate::storage::Store;
use crate::storage::TreeId;
use crate::storage::View;

/// A rejection or system failure, distinct from a deterministic VM abort.
#[derive(Debug)]
pub enum Error {
    Invalid(&'static str),
    Unsupported { format: &'static str, version: u16 },
    Storage(storage::Error),
}

impl fmt::Display for Error {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match self {
            Self::Invalid(reason) => write!(f, "invalid VM input: {reason}"),
            Self::Unsupported { format, version } => {
                write!(f, "unsupported {format} version {version}")
            }
            Self::Storage(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Storage(error) => Some(error),
            _ => None,
        }
    }
}

impl From<storage::Error> for Error {
    fn from(error: storage::Error) -> Self {
        Self::Storage(error)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// Permanent reason numbers from outcome format 1.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u16)]
pub enum AbortReason {
    MissingKey = 1,
    KeyExists = 2,
    RequireFailed = 3,
    ExplicitAbort = 4,
    IntegerOverflow = 5,
    BoundExceeded = 6,
    ResourceLimit = 7,
    DivisionByZero = 8,
    InvalidShift = 9,
    IndexOutOfBounds = 10,
    InvalidUtf8 = 11,
    NameInUse = 16,
    TableNotLive = 17,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Abort {
    pub reason: AbortReason,
    /// Zero-based instruction index, or `u32::MAX` for administration.
    pub instruction: u32,
    pub user_code: u32,
    /// The resource ID for `ResourceLimit`; zero for other reasons.
    pub detail: u64,
}

/// Final logical effects. A deletion is an MVCC tombstone, not physical
/// removal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Effect {
    Put {
        table: u64,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        table: u64,
        key: Vec<u8>,
    },
    Catalogue {
        table: u64,
        value: Vec<u8>,
    },
    Limits {
        policy: LimitPolicy,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Outcome {
    Success {
        result_type: Type,
        value: Value,
        effects: Vec<Effect>,
    },
    Aborted(Abort),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Table {
    id: u64,
    key: Type,
    value: Type,
}

/// Validate and interpret bytes without changing storage.
///
/// Reads use versions strictly below `sequence`, merged with private writes.
/// The caller supplies a protected, fully resolved prior view. Catalogue
/// schemas and policy are read at that same sequence bound. Both sides of every
/// branch are validated before any instruction runs. Semantic failures return
/// `Ok(Outcome::Aborted(..))`; rejected input and storage failures return
/// `Err`.
pub fn interpret(
    view: &View,
    sequence: u64,
    program: &[u8],
    arguments: &[u8],
    claims: &LimitPolicy,
) -> Result<Outcome> {
    let (prior, program) = prepare_transaction(view, sequence, program, arguments, claims)?;
    runtime::run(view, prior, &program, claims)
}

/// Validate admission without reading rows, running instructions, or installing
/// an outcome. Manifest coverage and its scope claim belong to the record
/// layer.
pub(crate) fn validate_transaction(
    view: &View,
    sequence: u64,
    transaction: &Transaction,
    claims: &LimitPolicy,
) -> Result<()> {
    prepare_transaction(
        view,
        sequence,
        transaction.program_bytes(),
        transaction.argument_bytes(),
        claims,
    )?;
    Ok(())
}

pub(crate) fn transaction_tables(program: &[u8]) -> Result<Vec<u64>> {
    program::table_ids(program)
}

fn prepare_transaction(
    view: &View,
    sequence: u64,
    program: &[u8],
    arguments: &[u8],
    claims: &LimitPolicy,
) -> Result<(u64, program::Program)> {
    let prior = prior_sequence(sequence)?;
    let policy = database::policy(view, prior)?;
    if claims
        .values()
        .iter()
        .zip(policy.values())
        .any(|(claim, limit)| claim > limit)
    {
        return Err(Error::Invalid(
            "transaction claims exceed historical policy",
        ));
    }
    let tables = program::table_ids(program)?
        .into_iter()
        .map(|id| database::table(view, id, prior))
        .collect::<Result<Vec<_>>>()?;
    let program = program::decode(program, arguments, &tables, claims)?;
    Ok((prior, program))
}

/// Interpret one transaction and atomically install its final versions and
/// canonical outcome. Aborts install only an outcome.
///
/// Records must be supplied in consecutive order after the checkpoint. The
/// digest must identify the complete canonical log record, including framing
/// and CRC. In an isolated reference store it may instead be a fixture
/// identity. This call is not a durability receipt and does not write or verify
/// that log.
pub fn execute(
    store: &mut Store,
    sequence: u64,
    record_digest: [u8; 32],
    transaction: &Transaction,
    claims: &LimitPolicy,
) -> Result<Outcome> {
    execute_bytes(
        store,
        sequence,
        record_digest,
        transaction.program_bytes(),
        transaction.argument_bytes(),
        claims,
    )
}

/// The byte-oriented form of [`execute`], for saved programs and reference
/// replay without reconstructing a Rust macro invocation.
///
/// Input validation errors remain `Error::Invalid`. A log reader must classify
/// invalid authoritative records as corruption, verify their enclosing record
/// and manifest, and supply the original claims before calling this function.
pub fn execute_bytes(
    store: &mut Store,
    sequence: u64,
    record_digest: [u8; 32],
    program: &[u8],
    arguments: &[u8],
    claims: &LimitPolicy,
) -> Result<Outcome> {
    let view = next_view(store, sequence)?;
    let outcome = interpret(&view, sequence, program, arguments, claims)?;
    database::install(store, sequence, record_digest, 1, &outcome)?;
    Ok(outcome)
}

/// Apply a reference catalogue record in the same serial sequence space.
/// Durability and recovery obligations are the same as for [`execute`].
pub fn execute_catalogue(
    store: &mut Store,
    sequence: u64,
    record_digest: [u8; 32],
    operation: &CatalogueOperation,
) -> Result<Outcome> {
    let view = next_view(store, sequence)?;
    let outcome = database::catalogue(&view, sequence, operation)?;
    database::install(store, sequence, record_digest, 2, &outcome)?;
    Ok(outcome)
}

/// Replace the reference policy for later records, even if the old policy
/// prevents all transaction submissions.
pub fn execute_limits(
    store: &mut Store,
    sequence: u64,
    record_digest: [u8; 32],
    policy: &LimitPolicy,
) -> Result<Outcome> {
    next_view(store, sequence)?;
    let outcome = database::limits(policy);
    database::install(store, sequence, record_digest, 3, &outcome)?;
    Ok(outcome)
}

fn prior_sequence(sequence: u64) -> Result<u64> {
    if sequence == 0 || sequence == u64::MAX {
        return Err(Error::Invalid("reserved record sequence"));
    }
    Ok(sequence - 1)
}

fn next_view(
    store: &Store,
    sequence: u64,
) -> Result<View> {
    let prior = prior_sequence(sequence)?;
    let checkpoint = store.manifest().checkpoint_sequence;
    if sequence <= checkpoint {
        return Err(Error::Invalid("record is already checkpointed"));
    }
    let view = storage::view(store);
    if prior != checkpoint && storage::get(&view, TreeId::Outcomes, &prior.to_be_bytes())?.is_none()
    {
        return Err(Error::Invalid(
            "reference execution requires the preceding outcome",
        ));
    }
    if storage::scan(
        &view,
        TreeId::Outcomes,
        Bound::Included(&sequence.to_be_bytes()),
        Bound::Unbounded,
    )?
    .next()
    .transpose()?
    .is_some()
    {
        return Err(Error::Invalid(
            "reference execution cannot replace resolved history",
        ));
    }
    Ok(view)
}

#[cfg(test)]
mod tests {
    use sha2::Digest;
    use sha2::Sha256;

    use super::*;
    use crate::tx;

    fn policy() -> LimitPolicy {
        LimitPolicy::new([
            1_048_576, 1024, 1024, 64, 1_048_576, 64, 0, 1024, 1024, 1024, 1_048_576, 8_388_608,
            1024, 8_388_608, 1024, 8_388_608, 1_048_576,
        ])
        .unwrap()
    }

    fn create() -> (tempfile::TempDir, Store) {
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

    fn entries(
        view: &View,
        tree: TreeId,
    ) -> Vec<storage::Entry> {
        storage::scan(view, tree, Bound::Unbounded, Bound::Unbounded)
            .unwrap()
            .collect::<storage::Result<_>>()
            .unwrap()
    }

    #[test]
    fn execution_rejects_reserved_gapped_and_resolved_sequences_without_mutation() {
        let (_directory, mut store) = create();
        let transaction = tx! { return 42; }.unwrap();
        for sequence in [0, 2, u64::MAX] {
            assert!(matches!(
                execute(&mut store, sequence, [1; 32], &transaction, &policy()),
                Err(Error::Invalid(_))
            ));
            assert!(entries(&storage::view(&store), TreeId::Outcomes).is_empty());
        }
        let aborted = tx! { abort(42); }.unwrap();
        assert!(matches!(
            execute(&mut store, 1, [1; 32], &aborted, &policy()).unwrap(),
            Outcome::Aborted(_)
        ));
        assert!(matches!(
            execute(&mut store, 2, [2; 32], &transaction, &policy()).unwrap(),
            Outcome::Success {
                value: Value::I64(42),
                ..
            }
        ));
        let before = entries(&storage::view(&store), TreeId::Outcomes);
        for sequence in [1, 2, 4] {
            assert!(matches!(
                execute(&mut store, sequence, [3; 32], &transaction, &policy()),
                Err(Error::Invalid(_))
            ));
        }
        assert_eq!(entries(&storage::view(&store), TreeId::Outcomes), before);
        storage::apply(
            &mut store,
            &[
                storage::Mutation {
                    tree: TreeId::Outcomes,
                    key: 2_u64.to_be_bytes().to_vec(),
                    value: None,
                },
                storage::Mutation {
                    tree: TreeId::Outcomes,
                    key: 3_u64.to_be_bytes().to_vec(),
                    value: Some(before[1].1.clone()),
                },
            ],
        )
        .unwrap();
        assert!(matches!(
            execute(&mut store, 2, [2; 32], &transaction, &policy()),
            Err(Error::Invalid(_))
        ));
    }

    #[test]
    fn policies_apply_at_their_log_position_and_rejection_does_not_consume_a_sequence() {
        let (_directory, mut store) = create();
        let transaction = tx! { return 42; }.unwrap();
        let old_view = storage::view(&store);
        let zero = LimitPolicy::new([0; 17]).unwrap();
        execute_limits(&mut store, 1, [1; 32], &zero).unwrap();
        assert!(matches!(
            execute(&mut store, 2, [2; 32], &transaction, &policy()),
            Err(Error::Invalid(_))
        ));
        assert!(
            storage::get(
                &storage::view(&store),
                TreeId::Outcomes,
                &2_u64.to_be_bytes()
            )
            .unwrap()
            .is_none()
        );
        assert!(
            interpret(
                &old_view,
                1,
                transaction.program_bytes(),
                transaction.argument_bytes(),
                &policy()
            )
            .is_ok()
        );
        assert!(
            interpret(
                &storage::view(&store),
                1,
                transaction.program_bytes(),
                transaction.argument_bytes(),
                &policy()
            )
            .is_ok()
        );
        execute_limits(&mut store, 2, [2; 32], &policy()).unwrap();
        execute(&mut store, 3, [3; 32], &transaction, &policy()).unwrap();
        assert!(matches!(
            interpret(
                &storage::view(&store),
                2,
                transaction.program_bytes(),
                transaction.argument_bytes(),
                &policy()
            ),
            Err(Error::Invalid(_))
        ));
        assert_eq!(entries(&storage::view(&store), TreeId::Policy).len(), 3);
        let outcome = storage::get(
            &storage::view(&store),
            TreeId::Outcomes,
            &2_u64.to_be_bytes(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(&outcome[..4], &[1, 0, 3, 0]);
    }

    #[test]
    fn replaying_saved_bytes_after_reopen_reconstructs_identical_versions_and_outcomes() {
        let (_directory, mut store) = create();
        let create_table = CatalogueOperation::Create {
            name: "counter".into(),
            key: Type::U64,
            value: Type::I64,
        };
        let programs = [
            tx! { tables {counter:u64=>i64=1} counter[7] = 40; }
                .unwrap()
                .into_parts(),
            tx! { tables {counter:u64=>i64=1} counter[7] += 2; return counter[7]; }
                .unwrap()
                .into_parts(),
            tx! { tables {counter:u64=>i64=1} counter[7] = 100; abort(5); }
                .unwrap()
                .into_parts(),
            tx! { tables {counter:u64=>i64=1} return counter[7]; }
                .unwrap()
                .into_parts(),
        ];
        let mut expected = Vec::new();
        execute_catalogue(&mut store, 1, [1; 32], &create_table).unwrap();
        for (index, (program, arguments)) in programs.iter().enumerate() {
            let sequence = index as u64 + 2;
            expected.push(
                execute_bytes(
                    &mut store,
                    sequence,
                    [sequence as u8; 32],
                    program,
                    arguments,
                    &policy(),
                )
                .unwrap(),
            );
        }
        let trees = [
            TreeId::State,
            TreeId::Catalogue,
            TreeId::Policy,
            TreeId::Outcomes,
        ];
        let before = trees.map(|tree| entries(&storage::view(&store), tree));
        let path = store.directory().to_owned();
        drop(store);
        let mut store = storage::open(path).unwrap();
        assert!(entries(&storage::view(&store), TreeId::Outcomes).is_empty());
        execute_catalogue(&mut store, 1, [1; 32], &create_table).unwrap();
        for (index, (program, arguments)) in programs.iter().enumerate() {
            let sequence = index as u64 + 2;
            let outcome = execute_bytes(
                &mut store,
                sequence,
                [sequence as u8; 32],
                program,
                arguments,
                &policy(),
            )
            .unwrap();
            assert_eq!(outcome, expected[index]);
        }
        assert_eq!(
            trees.map(|tree| entries(&storage::view(&store), tree)),
            before
        );
    }

    #[test]
    fn a_published_checkpoint_cannot_be_reexecuted_but_its_successor_can() {
        let (_directory, mut store) = create();
        let policy = policy();
        let mut record = vec![0; 64];
        record[..4].copy_from_slice(b"BLR1");
        record[4..6].copy_from_slice(&64_u16.to_le_bytes());
        record[6..8].copy_from_slice(&1_u16.to_le_bytes());
        record[8..12].copy_from_slice(&212_u32.to_le_bytes());
        record[12..16].copy_from_slice(&140_u32.to_le_bytes());
        record[16..24].copy_from_slice(&1_u64.to_le_bytes());
        record[24] = 3;
        record[28..60].copy_from_slice(&store.manifest().genesis_digest);
        record.extend_from_slice(&policy.encode());
        record.extend_from_slice(&crc32c::crc32c(&record).to_le_bytes());
        record.extend_from_slice(&212_u32.to_le_bytes());
        let digest = Sha256::digest(&record).into();
        let mut log = vec![0; 96];
        log[..8].copy_from_slice(b"BLOPLG01");
        log[8..10].copy_from_slice(&1_u16.to_le_bytes());
        log[10..12].copy_from_slice(&96_u16.to_le_bytes());
        log[16..32].copy_from_slice(&store.manifest().database_id);
        log[32..40].copy_from_slice(&1_u64.to_le_bytes());
        log[40..48].copy_from_slice(&1_u64.to_le_bytes());
        log[48..80].copy_from_slice(&store.manifest().genesis_digest);
        let crc = crc32c::crc32c(&log);
        log[92..96].copy_from_slice(&crc.to_le_bytes());
        log.extend_from_slice(&record);
        std::fs::write(store.directory().join("log-00000000000000000001.bin"), &log).unwrap();
        let mut manifest = store.manifest().clone();
        manifest.durable_sequence = 1;
        manifest.durable_digest = digest;
        manifest.next_segment_id = 2;
        manifest.segments.push(storage::SegmentDescriptor {
            segment_id: 1,
            first_sequence: 1,
            last_sequence: 1,
            committed_bytes: log.len() as u64,
            predecessor_digest: manifest.genesis_digest,
            last_digest: digest,
        });
        let checkpoint = storage::checkpoint_view(&store);
        storage::publish(&mut store, &checkpoint, manifest).unwrap();
        drop(checkpoint);
        execute_limits(&mut store, 1, digest, &policy).unwrap();
        let checkpoint = storage::prepare_checkpoint(&mut store, 1).unwrap();
        let mut manifest = store.manifest().clone();
        manifest.checkpoint_sequence = 1;
        manifest.checkpoint_digest = digest;
        storage::publish(&mut store, &checkpoint, manifest).unwrap();
        drop(checkpoint);
        let path = store.directory().to_owned();
        drop(store);
        let mut store = storage::open(path).unwrap();
        assert!(matches!(
            execute_limits(&mut store, 1, digest, &policy),
            Err(Error::Invalid(_))
        ));
        assert!(matches!(
            database::install(&mut store, 1, digest, 3, &database::limits(&policy)),
            Err(Error::Invalid(_))
        ));
        execute(&mut store, 2, [2; 32], &tx! {return 42;}.unwrap(), &policy).unwrap();
    }
}
