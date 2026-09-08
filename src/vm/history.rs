//! Logical checkpoint validation above the physical store. No pre-checkpoint
//! effects are replayed.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::ops::Bound;

use super::AbortReason;
use super::CatalogueOperation;
use super::CatalogueVersion;
use super::Effect;
use super::Error;
use super::Outcome;
use super::OutcomeRecord;
use super::Result;
use super::database;
use super::database::persisted;
use super::program::BinaryOp;
use super::program::Instruction;
use super::program::UnaryOp;
use crate::Transaction;
use crate::storage;
use crate::storage::Genesis;
use crate::storage::LimitPolicy;
use crate::storage::Manifest;
use crate::storage::TreeId;
use crate::storage::View;
use crate::storage::encoding;
use crate::storage::encoding::Schema;
use crate::storage::mvcc::StateKey;
use crate::storage::mvcc::StateValue;

/// Verified per-sequence version counts, used to cross-check retained source
/// records. This index contains metadata only; values and outcomes remain in
/// the pinned view.
#[derive(Debug)]
pub struct History {
    versions: BTreeMap<u64, (u8, usize)>,
    floor: u64,
}

impl History {
    /// The record kind that produced retained versions, or None for a sequence
    /// with no retained versions. Absence does not imply an abort.
    pub fn version_kind(
        &self,
        sequence: u64,
    ) -> Option<u8> {
        self.versions.get(&sequence).map(|&(kind, _)| kind)
    }
}

struct TableHistory {
    latest: CatalogueVersion,
    key: Schema,
    value: Schema,
    dropped: Option<u64>,
}

fn version(
    history: &mut History,
    sequence: u64,
    kind: u8,
    checkpoint: u64,
) -> Result<()> {
    database::valid_id(sequence)?;
    if sequence > checkpoint {
        return Err(Error::Invalid(
            "checkpoint contains a post-checkpoint version",
        ));
    }
    let (previous_kind, count) = history.versions.entry(sequence).or_insert((kind, 0));
    if *previous_kind != kind || (kind != 1 && *count != 0) {
        return Err(Error::Invalid(
            "incompatible versions at one record sequence",
        ));
    }
    *count = count
        .checked_add(1)
        .ok_or(Error::Invalid("too many retained versions"))?;
    Ok(())
}

fn system_sequence(
    key: &[u8],
    checkpoint: u64,
    genesis: bool,
) -> Result<u64> {
    let bytes: [u8; 8] = key
        .try_into()
        .map_err(|_| Error::Invalid("invalid system key length"))?;
    let sequence = u64::from_be_bytes(bytes);
    if sequence == u64::MAX || sequence > checkpoint || (!genesis && sequence == 0) {
        return Err(Error::Invalid("invalid checkpoint system sequence"));
    }
    Ok(sequence)
}

fn catalogue_history(
    view: &View,
    checkpoint: u64,
    history: &mut History,
) -> Result<BTreeMap<u64, TableHistory>> {
    let mut events = BTreeMap::new();
    for entry in storage::scan(view, TreeId::Catalogue, Bound::Unbounded, Bound::Unbounded)? {
        let (key, value) = entry?;
        let (id, sequence) = database::decode_catalogue_key(&key)?;
        let metadata = database::decode_catalogue(id, &value)?;
        version(history, sequence, 2, checkpoint)?;
        events.insert(sequence, metadata);
    }
    let mut tables: BTreeMap<u64, TableHistory> = BTreeMap::new();
    let mut names = BTreeMap::new();
    for (sequence, metadata) in events {
        let id = metadata.table.id;
        if let Some(previous) = tables.get_mut(&id) {
            if !previous.latest.live
                || previous.latest.table != metadata.table
                || (!metadata.live && previous.latest.name != metadata.name)
            {
                return Err(Error::Invalid(
                    "invalid catalogue lifecycle or changed immutable schema",
                ));
            }
            names.remove(&previous.latest.name);
            previous.latest = metadata.clone();
            if !metadata.live {
                previous.dropped = Some(sequence);
            }
        } else {
            if sequence != id || !metadata.live {
                return Err(Error::Invalid("missing live catalogue creation version"));
            }
            tables.insert(
                id,
                TableHistory {
                    key: Schema::decode(&metadata.table.key.descriptor())?,
                    value: Schema::decode(&metadata.table.value.descriptor())?,
                    latest: metadata.clone(),
                    dropped: None,
                },
            );
        }
        if metadata.live && names.insert(metadata.name, id).is_some() {
            return Err(Error::Invalid("duplicate live catalogue name"));
        }
    }
    Ok(tables)
}

fn live_table(
    tables: &BTreeMap<u64, TableHistory>,
    id: u64,
    sequence: u64,
) -> Result<&TableHistory> {
    let table = tables
        .get(&id)
        .ok_or(Error::Invalid("state references an unknown table"))?;
    if sequence <= id || table.dropped.is_some_and(|drop| sequence >= drop) {
        return Err(Error::Invalid(
            "state version was written outside table lifetime",
        ));
    }
    Ok(table)
}

fn exact_effect(
    view: &View,
    sequence: u64,
    effect: &Effect,
    floor: u64,
) -> Result<bool> {
    let (tree, key, value) = match effect {
        Effect::Put { table, key, value } => (
            TreeId::State,
            StateKey::new(*table, key.clone(), sequence)?.encode(),
            StateValue::Put(value.clone()).encode()?,
        ),
        Effect::Delete { table, key } => (
            TreeId::State,
            StateKey::new(*table, key.clone(), sequence)?.encode(),
            StateValue::Delete.encode()?,
        ),
        Effect::Catalogue { table, value } => (
            TreeId::Catalogue,
            database::catalogue_key(*table, sequence),
            value.clone(),
        ),
        Effect::Limits { policy } => (
            TreeId::Policy,
            sequence.to_be_bytes().to_vec(),
            policy.encode(),
        ),
    };
    let stored = storage::get(view, tree, &key)?;
    if stored.is_none() && tree == TreeId::State && sequence <= floor {
        return Ok(false);
    }
    if stored.as_deref() != Some(value.as_slice()) {
        return Err(Error::Invalid(
            "outcome effect differs from exact retained version",
        ));
    }
    Ok(true)
}

fn exact_outcome(
    view: &View,
    history: &History,
    sequence: u64,
    kind: u8,
    outcome: &Outcome,
) -> Result<()> {
    let effects = match outcome {
        Outcome::Success { effects, .. } => effects.as_slice(),
        Outcome::Aborted(_) => &[],
    };
    let (version_kind, count) = history
        .versions
        .get(&sequence)
        .copied()
        .unwrap_or((kind, 0));
    if version_kind != kind || count > effects.len() {
        return Err(Error::Invalid(
            "outcome does not describe every retained version at its sequence",
        ));
    }
    let mut matched = 0;
    for effect in effects {
        matched += usize::from(exact_effect(view, sequence, effect, history.floor)?);
    }
    if matched != count {
        return Err(Error::Invalid("outcome omits a retained version"));
    }
    Ok(())
}

fn transaction_limits(
    outcome: &Outcome,
    policy: &LimitPolicy,
) -> Result<()> {
    let Outcome::Success { value, effects, .. } = outcome else {
        return Ok(());
    };
    let limits = policy.values();
    if value.encoded_len() as u64 > limits[16]
        || value.encoded_len() as u64 > limits[10]
        || effects.len() as u64 > limits[14]
        || effects.len() as u64 > limits[8]
        || effects.len() as u64 > limits[7]
    {
        return Err(Error::Invalid(
            "outcome exceeds historical transaction bounds",
        ));
    }
    let mut bytes = 0_u64;
    for effect in effects {
        let (key, value_len) = match effect {
            Effect::Put { key, value, .. } => (key, value.len() as u64),
            Effect::Delete { key, .. } => (key, 0),
            _ => return Err(Error::Invalid("non-state transaction effect")),
        };
        if key.len() as u64 > limits[9] || value_len > limits[10] {
            return Err(Error::Invalid(
                "outcome effect exceeds historical transaction bounds",
            ));
        }
        bytes += 9 + key.len() as u64 + value_len;
    }
    if bytes > limits[15] {
        return Err(Error::Invalid("outcome exceeds historical overlay bound"));
    }
    Ok(())
}

/// Validate all retained logical history before accepting work. The physical
/// store remains usable for page fixtures that are not complete databases.
/// Additional older state/outcomes are valid; only (G, C] requires every
/// outcome.
///
/// The caller supplies physically verified roots and their matching manifest
/// and genesis. Engine recovery separately validates retained log bodies
/// against this history; no transaction is executed by this check.
pub fn validate_history(
    view: &View,
    manifest: &Manifest,
    genesis: &Genesis,
) -> Result<History> {
    validate(view, manifest, genesis).map_err(persisted)
}

fn validate(
    view: &View,
    manifest: &Manifest,
    genesis: &Genesis,
) -> Result<History> {
    let checkpoint = manifest.checkpoint_sequence;
    let floor = manifest.history_floor;
    if floor > checkpoint || checkpoint == u64::MAX {
        return Err(Error::Invalid("invalid logical checkpoint bounds"));
    }
    let mut history = History {
        versions: BTreeMap::new(),
        floor,
    };
    let tables = catalogue_history(view, checkpoint, &mut history)?;
    let mut policies = BTreeMap::new();
    for entry in storage::scan(view, TreeId::Policy, Bound::Unbounded, Bound::Unbounded)? {
        let (key, value) = entry?;
        let sequence = system_sequence(&key, checkpoint, true)?;
        let policy = LimitPolicy::decode(&value)?;
        if sequence != 0 {
            version(&mut history, sequence, 3, checkpoint)?;
        }
        policies.insert(sequence, policy);
    }
    if policies.get(&0) != Some(&genesis.initial_policy) {
        return Err(Error::Invalid("initial policy differs from genesis"));
    }
    for entry in storage::scan(view, TreeId::State, Bound::Unbounded, Bound::Unbounded)? {
        let (key, value) = entry?;
        let key = StateKey::decode(&key)?;
        let value = StateValue::decode(&value)?;
        version(&mut history, key.sequence(), 1, checkpoint)?;
        let table = live_table(&tables, key.table_id(), key.sequence())?;
        encoding::decode_key(&table.key, key.key())?;
        if let StateValue::Put(value) = value {
            encoding::validate_value(&table.value, &value)?;
        }
    }
    let mut next = floor + 1;
    for entry in storage::scan(view, TreeId::Outcomes, Bound::Unbounded, Bound::Unbounded)? {
        let (key, bytes) = entry?;
        let sequence = system_sequence(&key, checkpoint, false)?;
        let record = super::decode_outcome(&bytes)?;
        if record.sequence != sequence {
            return Err(Error::Invalid("outcome sequence differs from tree key"));
        }
        if sequence == checkpoint && record.record_digest != manifest.checkpoint_digest {
            return Err(Error::Invalid("outcome checkpoint digest mismatch"));
        }
        if sequence > floor {
            if sequence != next {
                return Err(Error::Invalid("missing retained outcome"));
            }
            next += 1;
        }
        if record.record_kind == 1 {
            let policy = policies.range(..sequence).next_back().unwrap().1;
            transaction_limits(&record.outcome, policy)?;
            if let Outcome::Success { effects, .. } = &record.outcome {
                for effect in effects {
                    let (id, key) = super::outcome::effect_address(effect);
                    let table = live_table(&tables, id, sequence)?;
                    encoding::decode_key(&table.key, key)?;
                    if let Effect::Put { value, .. } = effect {
                        encoding::validate_value(&table.value, value)?;
                    }
                }
            }
        }
        exact_outcome(
            view,
            &history,
            sequence,
            record.record_kind,
            &record.outcome,
        )?;
    }
    if next != checkpoint + 1 {
        return Err(Error::Invalid("missing retained outcome"));
    }
    Ok(history)
}

/// Check a retained catalogue source, including when its old outcome was
/// reclaimed.
pub(crate) fn validate_catalogue_record(
    view: &View,
    history: &History,
    sequence: u64,
    operation: &CatalogueOperation,
    stored: Option<&OutcomeRecord>,
) -> Result<()> {
    let expected = database::catalogue(view, sequence, operation)?;
    if stored.is_some_and(|record| record.outcome != expected) {
        return Err(Error::Invalid(
            "catalogue outcome disagrees with retained log",
        ));
    }
    exact_outcome(view, history, sequence, 2, &expected)
}

pub(crate) fn validate_limits_record(
    view: &View,
    history: &History,
    sequence: u64,
    policy: &LimitPolicy,
    stored: Option<&OutcomeRecord>,
) -> Result<()> {
    let expected = database::limits(policy);
    if stored.is_some_and(|record| record.outcome != expected) {
        return Err(Error::Invalid("limits outcome disagrees with retained log"));
    }
    exact_outcome(view, history, sequence, 3, &expected)
}

/// Check what the source program proves without reading rows or rerunning pre-C
/// effects.
pub(crate) fn validate_transaction_outcome(
    view: &View,
    sequence: u64,
    transaction: &Transaction,
    claims: &LimitPolicy,
    record: &OutcomeRecord,
) -> Result<()> {
    let (_, program) = super::prepare_transaction(
        view,
        sequence,
        transaction.program_bytes(),
        transaction.argument_bytes(),
        claims,
    )?;
    transaction_limits(&record.outcome, claims)?;
    match &record.outcome {
        Outcome::Success {
            result_type,
            effects,
            ..
        } => {
            if *result_type != program.result_type {
                return Err(Error::Invalid(
                    "outcome result descriptor differs from source program",
                ));
            }
            let mut writes = BTreeSet::new();
            for instruction in &program.instructions {
                let (table, delete) = match instruction {
                    Instruction::Store { table, .. } | Instruction::Insert { table, .. } => {
                        (*table, false)
                    }
                    Instruction::Delete { table, .. } => (*table, true),
                    _ => continue,
                };
                writes.insert((program.tables[table].id, delete));
            }
            for effect in effects {
                let (table, _) = super::outcome::effect_address(effect);
                if !writes.contains(&(table, matches!(effect, Effect::Delete { .. }))) {
                    return Err(Error::Invalid(
                        "outcome effect has no source write instruction",
                    ));
                }
            }
        }
        Outcome::Aborted(abort) => {
            let instruction =
                program
                    .instructions
                    .get(abort.instruction as usize)
                    .ok_or(Error::Invalid(
                        "outcome abort instruction is outside source program",
                    ))?;
            let legal = match abort.reason {
                AbortReason::RequireFailed => {
                    matches!(instruction, Instruction::Require { user_code, .. } if *user_code == abort.user_code)
                }
                AbortReason::ExplicitAbort => {
                    matches!(instruction, Instruction::Abort { user_code } if *user_code == abort.user_code)
                }
                AbortReason::MissingKey => matches!(instruction, Instruction::Load { .. }),
                AbortReason::KeyExists => matches!(instruction, Instruction::Insert { .. }),
                AbortReason::DivisionByZero => matches!(
                    instruction,
                    Instruction::Binary {
                        op: BinaryOp::Div | BinaryOp::Rem,
                        ..
                    }
                ),
                AbortReason::InvalidShift => matches!(
                    instruction,
                    Instruction::Binary {
                        op: BinaryOp::Shl | BinaryOp::Shr,
                        ..
                    }
                ),
                AbortReason::InvalidUtf8 => matches!(
                    instruction,
                    Instruction::Unary {
                        op: UnaryOp::ParseUtf8,
                        ..
                    }
                ),
                AbortReason::IndexOutOfBounds => matches!(
                    instruction,
                    Instruction::Slice { .. }
                        | Instruction::Binary {
                            op: BinaryOp::RowsKey | BinaryOp::RowsValue,
                            ..
                        }
                ),
                AbortReason::IntegerOverflow => matches!(
                    instruction,
                    Instruction::Binary {
                        op: BinaryOp::Add
                            | BinaryOp::Sub
                            | BinaryOp::Mul
                            | BinaryOp::Div
                            | BinaryOp::Rem,
                        ..
                    } | Instruction::Unary {
                        op: UnaryOp::Neg | UnaryOp::ToI64 | UnaryOp::ToU64,
                        ..
                    }
                ),
                AbortReason::BoundExceeded | AbortReason::ResourceLimit => true,
                _ => false,
            };
            if !legal {
                return Err(Error::Invalid(
                    "outcome abort fields disagree with source instruction",
                ));
            }
        }
    }
    Ok(())
}
