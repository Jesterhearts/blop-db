//! Verify logical feed batches before making them durable on a replica.
//!
//! Decode H.1 bytes, compare isolated reference execution, publish the verified
//! bytes, then replay them into live state.

use std::fs::File;

use tokio::sync::oneshot;
use tokio::sync::watch;

use super::BatchLimits;
use super::CursorKind;
use super::CursorToken;
use super::Database;
use super::EngineOptions;
use super::Error;
use super::FeedBatch;
use super::FeedRecords;
use super::LogicalRecord;
use super::Request;
use super::Result;
use super::Watermark;
use super::budget;
use super::cursor;
use super::engine;
use super::exchange;
use super::feed;
use super::record;
use crate::storage;
use crate::vm;

/// Read canonical log bytes and outcomes through the captured visible frontier.
///
/// Use a logical-feed or log-replica cursor, kinds 2 and 3. Reading does not
/// acknowledge that cursor. Batch limits include framing and preserve complete
/// records and outcomes.
pub async fn read_logical_feed(
    database: &Database,
    token: &CursorToken,
    after: Watermark,
    limits: BatchLimits,
) -> Result<FeedBatch> {
    match cursor::request(
        database,
        cursor::Operation::LogicalFeed {
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
    if !matches!(token.kind(), CursorKind::Logical | CursorKind::LogReplica) {
        return Err(Error::InvalidToken("logical feed requires kind 2 or 3"));
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
    let manifest = store.manifest();
    let view = storage::view(store);
    let mut records = Vec::new();
    let mut size = exchange::EMPTY_BATCH_BYTES;
    let mut end = after.sequence();
    let mut byte_limited = false;
    let corrupt = |reason| Error::Storage(storage::Error::Corrupt(reason));
    'segments: for segment in manifest
        .segments
        .iter()
        .filter(|s| s.last_sequence > after.sequence())
    {
        if end == frontier || records.len() == limits.max_records {
            break;
        }
        if segment.first_sequence > end + 1 {
            return Err(corrupt("gap in cursor-protected log"));
        }
        let file = File::open(
            store
                .directory()
                .join(format!("log-{:020}.bin", segment.segment_id)),
        )
        .map_err(|e| Error::Storage(e.into()))?;
        let mut reader = storage::wal::Records::new(file, manifest.database_id, segment)
            .map_err(Error::Storage)?;
        for sequence in segment.first_sequence..=segment.last_sequence {
            if sequence > frontier || records.len() == limits.max_records {
                break 'segments;
            }
            let storage::wal::Record {
                bytes: log_record,
                digest,
                ..
            } = reader.next().unwrap().map_err(Error::Storage)?;
            if sequence == manifest.checkpoint_sequence && digest != manifest.checkpoint_digest {
                return Err(corrupt("logical feed checkpoint anchor mismatch"));
            }
            if sequence <= after.sequence() {
                continue;
            }
            let outcome = feed::read_record(&view, sequence).map_err(|e| match e {
                Error::HistoryUnavailable => corrupt("missing cursor-protected outcome"),
                other => other,
            })?;
            if outcome.record_digest != digest || outcome.record_kind != log_record[24] {
                return Err(corrupt("logical feed outcome differs from source record"));
            }
            let required = log_record.len() + exchange::encode_outcome(&outcome)?.len() + 8;
            if required > limits.max_bytes - size {
                if records.is_empty() {
                    return Err(Error::BatchTooSmall {
                        required: exchange::EMPTY_BATCH_BYTES + required,
                    });
                }
                byte_limited = true;
                break 'segments;
            }
            size += required;
            records.push(LogicalRecord {
                log_record,
                outcome,
            });
            end = sequence;
        }
    }
    if end < frontier && records.len() < limits.max_records && !byte_limited {
        return Err(corrupt("missing cursor-protected log"));
    }
    Ok(FeedBatch {
        database_id: manifest.database_id,
        start_exclusive: after.sequence(),
        end_inclusive: end,
        records: FeedRecords::Logical(records),
    })
}

struct ParsedRecord {
    bytes: Vec<u8>,
    command: record::Command,
    sequence: u64,
    digest: [u8; 32],
    outcome: Vec<u8>,
}

/// Parsed import data whose bytes and outcomes cannot be replaced by
/// publication.
///
/// Private immutable fields preserve the exact data that the importer
/// validated.
pub(super) struct ParsedBatch {
    start: u64,
    watermark: Watermark,
    records: Vec<ParsedRecord>,
    bytes: u64,
    log_bytes: u64,
}

fn parse(encoded: &[u8]) -> Result<ParsedBatch> {
    let batch = FeedBatch::decode(encoded)?;
    let watermark = batch.watermark()?;
    let FeedRecords::Logical(logical) = batch.records else {
        return Err(Error::InvalidFormat("import requires a logical batch"));
    };
    let mut input = &encoded[56..encoded.len() - 4];
    let mut records = Vec::with_capacity(logical.len());
    let mut bytes = 512_u64;
    let mut log_bytes = 0;
    for record in logical {
        exchange::read_blob(&mut input)?;
        let outcome = exchange::read_blob(&mut input)?.to_vec();
        let command = record::decode(
            record.log_record[24],
            &record.log_record[64..record.log_record.len() - 8],
        )
        .map_err(input_error)?;
        // Charge owned representations and comparison/codec scratch as well as
        // wire bytes. Prefix disk space and storage traversal are separate.
        bytes = bytes
            .saturating_add(budget::input_bytes(&command)? * 3)
            .saturating_add((record.log_record.len() + outcome.len()) as u64 * 4);
        log_bytes += record.log_record.len() as u64;
        records.push(ParsedRecord {
            bytes: record.log_record,
            command,
            sequence: record.outcome.sequence,
            digest: record.outcome.record_digest,
            outcome,
        });
    }
    Ok(ParsedBatch {
        start: batch.start_exclusive,
        watermark,
        records,
        bytes,
        log_bytes,
    })
}

/// Import a logical batch into an attached read-only replica.
///
/// Parse the complete bounded batch before enqueueing. Before publishing any
/// incoming bytes locally, reproduce every outcome in a disposable copy of the
/// replica's selected prefix and compare it with the supplied outcome.
///
/// One import may be parsing, queued or running per database. It reserves the
/// aggregate record count and bytes from the shared submission budgets until
/// completion. Cancellation after enqueueing does not cancel import. On an
/// uncertain result, close and reopen, inspect the recovered snapshot
/// watermark, then request the remaining source suffix. Never blindly retry the
/// old batch.
pub async fn import_logical(
    database: &Database,
    encoded: &[u8],
) -> Result<Watermark> {
    if !database.read_only {
        return Err(Error::InvalidInput(
            "logical import requires a read-only replica",
        ));
    }
    if !(exchange::EMPTY_BATCH_BYTES..=exchange::MAX_BATCH_BYTES).contains(&encoded.len()) {
        return Err(Error::InvalidFormat("invalid exchange length"));
    }
    let slot = database
        .queue
        .import
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| Error::Closed)?;
    let batch = parse(encoded)?;
    if batch.watermark.database_id() != database.database_id() {
        return Err(Error::InvalidInput(
            "logical import database identity mismatch",
        ));
    }
    let permit =
        budget::reserve_batch(&database.queue, batch.records.len().max(1), batch.bytes).await?;
    let (reply, result) = oneshot::channel();
    database
        .sender
        .send(Request::Import {
            batch,
            reply,
            permit,
            slot,
        })
        .await
        .map_err(|_| Error::Closed)?;
    result.await.map_err(|_| Error::Uncertain {
        sequence: None,
        source: None,
    })?
}

fn capacity(
    resource: &'static str,
    required: u64,
    limit: u64,
) -> Result<()> {
    if required > limit {
        return Err(Error::OperationalLimit {
            resource,
            required,
            limit,
        });
    }
    Ok(())
}

fn input_error(error: vm::Error) -> Error {
    match error {
        vm::Error::Invalid(reason) | vm::Error::Storage(storage::Error::InvalidInput(reason)) => {
            Error::InvalidFormat(reason)
        }
        vm::Error::Unsupported { .. } => Error::Rejected(error),
        other => Error::Read(other),
    }
}

pub(super) fn install(
    store: &mut storage::Store,
    batch: &ParsedBatch,
    options: &EngineOptions,
    status: &watch::Sender<super::EngineStatus>,
    #[cfg(test)] hooks: &super::workers::test_support::Hooks,
) -> Result<Watermark> {
    let manifest = store.manifest();
    if manifest.checkpoint_sequence != manifest.durable_sequence {
        return Err(Error::Storage(storage::Error::NeedsRecovery));
    }
    if batch.watermark.database_id() != manifest.database_id
        || batch.start != manifest.durable_sequence
    {
        return Err(Error::InvalidInput(
            "logical import identity or baseline mismatch",
        ));
    }
    if batch
        .records
        .first()
        .is_some_and(|r| r.bytes[28..60] != manifest.durable_digest)
    {
        return Err(Error::InvalidInput(
            "logical import predecessor anchor mismatch",
        ));
    }
    capacity(
        "assigned_backlog_count",
        batch.records.len() as u64,
        options.assigned_backlog_count as u64,
    )?;
    capacity(
        "assigned_backlog_bytes",
        batch.log_bytes,
        options.assigned_backlog_bytes,
    )?;
    capacity("preparation_bytes", batch.bytes, options.preparation_bytes)?;
    capacity("execution_bytes", batch.bytes, options.execution_bytes)?;
    if batch.records.is_empty() {
        return Ok(batch.watermark);
    }
    #[cfg(test)]
    test_point(hooks, 0)?;
    // The private directory owns real copies, not writable aliases of live
    // files. RAII removes it on unwind/error. Crash orphans in the OS temporary
    // directory are never discovered or selected by database recovery.
    let temporary = tempfile::Builder::new()
        .prefix("blop-import-")
        .tempdir()
        .map_err(|e| Error::Storage(e.into()))?;
    let path = temporary.path().join("prefix");
    let image = storage::backup::capture(store).map_err(Error::Storage)?;
    storage::backup::copy(image, &path).map_err(Error::Storage)?;
    let mut isolated = storage::backup::attach(
        &path,
        super::random_uuid().map_err(Error::Storage)?,
        false,
        storage::TailRecovery::Strict,
    )
    .map_err(Error::Storage)?;
    engine::recover(&mut isolated).map_err(Error::Storage)?;
    #[cfg(test)]
    test_point(hooks, 2)?;
    for record in &batch.records {
        if let record::Command::Transaction {
            transaction,
            claims,
            manifest,
        } = &record.command
        {
            let prepared = vm::prepare_transaction_bounded(
                &storage::view(&isolated),
                record.sequence,
                transaction,
                claims,
                manifest.as_ref(),
                options.preparation_bytes - batch.bytes,
            )
            .map_err(input_error)?;
            let vm::Preparation::Ready(prepared) = prepared else {
                let vm::Preparation::Capacity(required) = prepared else {
                    unreachable!()
                };
                return Err(Error::OperationalLimit {
                    resource: "preparation_bytes",
                    required: batch.bytes.saturating_add(required),
                    limit: options.preparation_bytes,
                });
            };
            for (resource, limit) in [
                ("execution_bytes", options.execution_bytes),
                ("preparation_bytes", options.preparation_bytes),
            ] {
                capacity(
                    resource,
                    batch.bytes.saturating_add(prepared.reservation_bytes()),
                    limit,
                )?;
            }
        }
        let outcome = record::execute(
            &mut isolated,
            record.sequence,
            record.digest,
            &record.command,
        )
        .map_err(input_error)?;
        let actual = vm::encode_outcome(record.sequence, record.digest, record.bytes[24], &outcome)
            .map_err(Error::Read)?;
        if actual != record.outcome {
            return Err(Error::InvalidInput("logical import outcome divergence"));
        }
    }
    drop(isolated);
    temporary.close().map_err(|e| Error::Storage(e.into()))?;
    #[cfg(test)]
    test_point(hooks, 1)?;
    status.send_modify(|s| {
        s.assigned_count = batch.records.len();
        s.assigned_bytes = batch.log_bytes;
        s.reserved_execution_bytes = batch.bytes;
    });
    for record in &batch.records {
        engine::append(store, record.sequence, &record.bytes).map_err(|e| Error::Uncertain {
            sequence: Some(record.sequence),
            source: Some(e.into()),
        })?;
        #[cfg(test)]
        test_point(hooks, record.sequence + 2)?;
    }
    // This is the normal durable recovery path, not installation of the
    // supplied effects. It revalidates canonical bodies and executes the
    // retained suffix.
    engine::recover(store).map_err(|e| Error::Uncertain {
        sequence: Some(batch.watermark.sequence()),
        source: Some(e.into()),
    })?;
    Ok(batch.watermark)
}

#[cfg(test)]
fn test_point(
    hooks: &super::workers::test_support::Hooks,
    phase: u64,
) -> Result<()> {
    // Read-only replicas never dispatch transaction workers.
    super::workers::test_support::before(hooks, phase).map_err(|source| Error::Uncertain {
        sequence: None,
        source: Some(source),
    })
}

#[cfg(test)]
mod tests;
