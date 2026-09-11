//! Read resolved feed effects from their exact sequence-tagged versions.
//!
//! A lookup of the latest value cannot substitute for a historical effect.

use super::BatchLimits;
use super::CursorToken;
use super::Database;
use super::Error;
use super::FeedBatch;
use super::FeedRecords;
use super::Result;
use super::Watermark;
use super::cursor;
use super::exchange;
use crate::storage::TreeId;
use crate::storage::View;
use crate::storage::encoding;
use crate::storage::mvcc;
use crate::storage::{
    self,
};
use crate::vm::Effect;
use crate::vm::Outcome;
use crate::vm::OutcomeRecord;
use crate::vm::{
    self,
};

/// Read complete resolved records after a watermark without advancing the
/// cursor.
///
/// `after` must lie within the protected interval and at or below the visible
/// frontier. A zero record limit returns an empty poll. If the first complete
/// record cannot fit, return `BatchTooSmall` so the caller can raise its limit.
pub async fn read_feed(
    database: &Database,
    token: &CursorToken,
    after: Watermark,
    limits: BatchLimits,
) -> Result<FeedBatch> {
    match cursor::request(
        database,
        cursor::Operation::Feed {
            token: *token,
            after,
            limits,
        },
    )
    .await?
    {
        cursor::Response::Batch(batch) => Ok(batch),
        _ => unreachable!("writer reply type"),
    }
}

pub(super) fn batch(
    store: &storage::Store,
    token: &CursorToken,
    after: Watermark,
    limits: BatchLimits,
    frontier: u64,
) -> Result<FeedBatch> {
    let info = cursor::lookup(store, token)?.ok_or(Error::CursorReleased)?;
    if token.kind() != super::CursorKind::Resolved {
        return Err(Error::InvalidToken("resolved feed requires kind 1"));
    }
    cursor::check_watermark(store, after)?;
    if after.sequence() < info.baseline || after.sequence() > frontier {
        return Err(Error::HistoryUnavailable);
    }
    if !(exchange::EMPTY_BATCH_BYTES..=exchange::MAX_BATCH_BYTES).contains(&limits.max_bytes) {
        return Err(Error::InvalidInput(
            "batch byte limit must be 60 through 256 MiB",
        ));
    }
    let view = storage::view(store);
    let mut records = Vec::new();
    let mut bytes = exchange::EMPTY_BATCH_BYTES;
    let mut end = after.sequence();
    for sequence in after.sequence() + 1..=frontier {
        if records.len() >= limits.max_records {
            break;
        }
        let record = read_record(&view, sequence).map_err(|error| match error {
            Error::HistoryUnavailable => Error::Storage(storage::Error::Corrupt(
                "missing history protected by a registered cursor",
            )),
            other => other,
        })?;
        let size = exchange::encode_outcome(&record)?.len() + 4;
        if size > limits.max_bytes - bytes {
            if records.is_empty() {
                return Err(Error::BatchTooSmall {
                    required: exchange::EMPTY_BATCH_BYTES + size,
                });
            }
            break;
        }
        bytes += size;
        records.push(record);
        end = sequence;
    }
    Ok(FeedBatch {
        database_id: store.genesis().database_id,
        start_exclusive: after.sequence(),
        end_inclusive: end,
        records: FeedRecords::Resolved(records),
    })
}

pub(super) fn read_record(
    view: &View,
    sequence: u64,
) -> Result<OutcomeRecord> {
    let record = vm::read_outcome(view, sequence)
        .map_err(Error::Read)?
        .ok_or(Error::HistoryUnavailable)?;
    if let Outcome::Success { effects, .. } = &record.outcome {
        for effect in effects {
            let (tree, key, expected) = match effect {
                Effect::Put { table, key, value } => (
                    TreeId::State,
                    mvcc::StateKey::new(*table, key.clone(), sequence)
                        .map_err(Error::Storage)?
                        .encode(),
                    mvcc::StateValue::Put(value.clone())
                        .encode()
                        .map_err(Error::Storage)?,
                ),
                Effect::Delete { table, key } => (
                    TreeId::State,
                    mvcc::StateKey::new(*table, key.clone(), sequence)
                        .map_err(Error::Storage)?
                        .encode(),
                    mvcc::StateValue::Delete.encode().map_err(Error::Storage)?,
                ),
                Effect::Catalogue { table, value } => (
                    TreeId::Catalogue,
                    vm::catalogue_key(*table, sequence),
                    value.clone(),
                ),
                Effect::Limits { policy } => (
                    TreeId::Policy,
                    sequence.to_be_bytes().to_vec(),
                    policy.encode(),
                ),
            };
            let actual = storage::get(view, tree, &key)
                .map_err(Error::Storage)?
                .ok_or(Error::HistoryUnavailable)?;
            if actual != expected {
                return Err(Error::Storage(storage::Error::Corrupt(
                    "feed outcome differs from exact version",
                )));
            }
            if let Effect::Put { table, key, .. } | Effect::Delete { table, key } = effect {
                let table = vm::catalogue_version(view, *table, sequence)
                    .map_err(Error::Read)?
                    .filter(|table| table.live)
                    .ok_or(Error::HistoryUnavailable)?
                    .table;
                let schema =
                    encoding::Schema::decode(&table.key.descriptor()).map_err(Error::Storage)?;
                encoding::decode_key(&schema, key)
                    .map_err(|error| super::persisted_read(error.into()))?;
                if let Effect::Put { value, .. } = effect {
                    vm::decode_value(&table.value, value).map_err(super::persisted_read)?;
                }
            }
        }
    }
    Ok(record)
}
