//! Serial, crash-safe G.4 retention metadata. These operations never enter the
//! log.

use std::fs::File;
use std::io::BufReader;
use std::io::Read;
use std::ops::Bound;

use super::BatchLimits;
use super::CursorKind;
use super::CursorToken;
use super::Database;
use super::Error;
use super::FeedBatch;
use super::Request;
use super::Result;
use super::Snapshot;
use super::Watermark;
use super::engine;
use super::feed;
use super::snapshot;
use crate::storage::Mutation;
use crate::storage::Store;
use crate::storage::TreeId;
use crate::storage::{
    self,
};
use crate::vm;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CursorInfo {
    pub token: CursorToken,
    pub baseline: u64,
    /// Nonunique diagnostic label, never used to select a registration.
    pub label: String,
}

/// Conservative current retention claims, not a promise that older history
/// exists.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetentionFloors {
    pub history: u64,
    pub log: u64,
}

pub(super) enum Operation {
    Checkout {
        baseline: Watermark,
        kind: CursorKind,
        label: String,
    },
    SnapshotTail {
        kind: CursorKind,
        label: String,
    },
    Reopen(CursorToken),
    Ack {
        token: CursorToken,
        watermark: Watermark,
    },
    Release(CursorToken),
    List,
    Floors,
    Feed {
        token: CursorToken,
        after: Watermark,
        limits: BatchLimits,
    },
}

pub(super) enum Response {
    Token(CursorToken),
    SnapshotTail(Snapshot, CursorToken),
    Info(CursorInfo),
    List(Vec<CursorInfo>),
    Batch(FeedBatch),
    Unit,
    Floors(RetentionFloors),
}

pub(super) async fn request(
    database: &Database,
    operation: Operation,
) -> Result<Response> {
    let (reply, result) = tokio::sync::oneshot::channel();
    database
        .sender
        .send(Request::Retention { operation, reply })
        .await
        .map_err(|_| Error::Closed)?;
    result.await.map_err(|_| Error::Uncertain {
        sequence: None,
        source: None,
    })?
}

/// Durably reserve a baseline and its subsequent feed. Cancellation after
/// enqueue may leave a registration; list diagnostics to find abandoned claims.
/// Labels may repeat. Dropping the returned token does not release history.
pub async fn checkout_cursor(
    database: &Database,
    baseline: Watermark,
    kind: CursorKind,
    label: impl Into<String>,
) -> Result<CursorToken> {
    match request(
        database,
        Operation::Checkout {
            baseline,
            kind,
            label: label.into(),
        },
    )
    .await?
    {
        Response::Token(token) => Ok(token),
        _ => unreachable!("writer reply type"),
    }
}

/// Atomically capture F and durably reserve a cursor at F. Revoking the
/// snapshot does not release the cursor. A revoked build must not be published
/// as complete.
pub async fn snapshot_and_cursor(
    database: &Database,
    kind: CursorKind,
    label: impl Into<String>,
) -> Result<(Snapshot, CursorToken)> {
    match request(
        database,
        Operation::SnapshotTail {
            kind,
            label: label.into(),
        },
    )
    .await?
    {
        Response::SnapshotTail(snapshot, token) => Ok((snapshot, token)),
        _ => unreachable!("writer reply type"),
    }
}

/// Validate both identities, issuance, presence and kind against durable
/// metadata.
pub async fn reopen_cursor(
    database: &Database,
    token: &CursorToken,
) -> Result<CursorInfo> {
    match request(database, Operation::Reopen(*token)).await? {
        Response::Info(info) => Ok(info),
        _ => unreachable!("writer reply type"),
    }
}

/// Advance only after committing derived data and this watermark atomically.
/// Equal acknowledgement is successful; backwards or future positions fail.
pub async fn acknowledge_cursor(
    database: &Database,
    token: &CursorToken,
    watermark: Watermark,
) -> Result<()> {
    request(
        database,
        Operation::Ack {
            token: *token,
            watermark,
        },
    )
    .await
    .map(|_| ())
}

/// Explicit consumer or administrative release by token, never by label.
/// Repeated release of an absent, previously issued ID succeeds. Reopening or
/// acknowledging that released ID reports `CursorReleased`.
pub async fn release_cursor(
    database: &Database,
    token: &CursorToken,
) -> Result<()> {
    request(database, Operation::Release(*token))
        .await
        .map(|_| ())
}

/// List durable claims, including claims whose in-memory handles were dropped.
pub async fn list_cursors(database: &Database) -> Result<Vec<CursorInfo>> {
    match request(database, Operation::List).await? {
        Response::List(info) => Ok(info),
        _ => unreachable!("writer reply type"),
    }
}

pub async fn retention_status(database: &Database) -> Result<RetentionFloors> {
    match request(database, Operation::Floors).await? {
        Response::Floors(floors) => Ok(floors),
        _ => unreachable!("writer reply type"),
    }
}

pub(super) fn check_watermark(
    store: &Store,
    watermark: Watermark,
) -> Result<()> {
    if watermark.database_id() != store.genesis().database_id {
        return Err(Error::InvalidToken("watermark database identity mismatch"));
    }
    Ok(())
}

fn token(
    store: &Store,
    id: u64,
    kind: CursorKind,
) -> CursorToken {
    CursorToken {
        database_id: store.genesis().database_id,
        cursor_namespace: store.manifest().cursor_namespace,
        cursor_id: id,
        kind,
    }
}

fn decode(
    store: &Store,
    id: u64,
    bytes: &[u8],
) -> Result<CursorInfo> {
    let invalid = || Error::Storage(storage::Error::Corrupt("invalid durable cursor value"));
    if !(16..=271).contains(&bytes.len()) || bytes[..2] != 1_u16.to_le_bytes() || bytes[3] != 0 {
        return Err(invalid());
    }
    let kind = CursorKind::decode(bytes[2]).map_err(|_| invalid())?;
    let baseline = u64::from_le_bytes(bytes[4..12].try_into().unwrap());
    let length = u32::from_le_bytes(bytes[12..16].try_into().unwrap()) as usize;
    if length != bytes.len() - 16
        || bytes[16..].contains(&0)
        || id == 0
        || id >= store.manifest().next_cursor_id
        || baseline < store.manifest().history_floor
        || baseline > store.manifest().checkpoint_sequence
        || (kind != CursorKind::Resolved && store.manifest().log_floor > baseline + 1)
    {
        return Err(invalid());
    }
    let label = std::str::from_utf8(&bytes[16..])
        .map_err(|_| invalid())?
        .to_owned();
    Ok(CursorInfo {
        token: token(store, id, kind),
        baseline,
        label,
    })
}

pub(super) fn lookup(
    store: &Store,
    token: &CursorToken,
) -> Result<Option<CursorInfo>> {
    // Namespace checks precede even the issued-ID test, not just tree lookup.
    if token.database_id != store.genesis().database_id
        || token.cursor_namespace != store.manifest().cursor_namespace
    {
        return Err(Error::InvalidToken("cursor database or namespace mismatch"));
    }
    if token.cursor_id == 0 || token.cursor_id >= store.manifest().next_cursor_id {
        return Err(Error::InvalidToken("cursor ID was not issued"));
    }
    let Some(bytes) = storage::get(
        &storage::view(store),
        TreeId::Cursors,
        &token.cursor_id.to_be_bytes(),
    )
    .map_err(Error::Storage)?
    else {
        return Ok(None);
    };
    let info = decode(store, token.cursor_id, &bytes)?;
    if token.kind != info.token.kind {
        return Err(Error::InvalidToken("cursor kind mismatch"));
    }
    Ok(Some(info))
}

fn list(store: &Store) -> Result<Vec<CursorInfo>> {
    let mut result = Vec::new();
    for entry in storage::scan(
        &storage::view(store),
        TreeId::Cursors,
        Bound::Unbounded,
        Bound::Unbounded,
    )
    .map_err(Error::Storage)?
    {
        let (key, bytes) = entry.map_err(Error::Storage)?;
        let id = u64::from_be_bytes(
            key.as_slice()
                .try_into()
                .map_err(|_| Error::Storage(storage::Error::Corrupt("invalid cursor key")))?,
        );
        result.push(decode(store, id, &bytes)?);
    }
    Ok(result)
}

fn encode(info: &CursorInfo) -> Vec<u8> {
    let mut bytes = vec![1, 0, info.token.kind as u8, 0];
    bytes.extend_from_slice(&info.baseline.to_le_bytes());
    bytes.extend_from_slice(&(info.label.len() as u32).to_le_bytes());
    bytes.extend_from_slice(info.label.as_bytes());
    bytes
}

fn publish(
    store: &mut Store,
    id: u64,
    value: Option<Vec<u8>>,
    next_id: u64,
) -> Result<()> {
    let uncertain = |source| Error::Uncertain {
        sequence: None,
        source: Some(vm::Error::Storage(source)),
    };
    storage::apply(
        store,
        &[Mutation {
            tree: TreeId::Cursors,
            key: id.to_be_bytes().to_vec(),
            value,
        }],
    )
    .map_err(uncertain)?;
    let mut manifest = store.manifest().clone();
    manifest.next_cursor_id = next_id;
    // The serial writer has C = F = D; this view includes the new cursor root.
    let checkpoint = storage::view(store);
    storage::publish(store, &checkpoint, manifest).map_err(uncertain)
}

fn checkout(
    store: &mut Store,
    baseline: Watermark,
    kind: CursorKind,
    label: String,
) -> Result<CursorToken> {
    check_watermark(store, baseline)?;
    if label.len() > 255 || label.as_bytes().contains(&0) {
        return Err(Error::InvalidInput(
            "cursor label exceeds 255 UTF-8 bytes or contains NUL",
        ));
    }
    let id = store.manifest().next_cursor_id;
    let next_id = id.checked_add(1).ok_or(Error::CursorIdExhausted)?;
    available(store, baseline.sequence(), kind)?;
    let info = CursorInfo {
        token: token(store, id, kind),
        baseline: baseline.sequence(),
        label,
    };
    publish(store, id, Some(encode(&info)), next_id)?;
    Ok(info.token)
}

fn available(
    store: &Store,
    baseline: u64,
    kind: CursorKind,
) -> Result<()> {
    let manifest = store.manifest();
    if baseline < manifest.history_floor || baseline > manifest.checkpoint_sequence {
        return Err(Error::HistoryUnavailable);
    }
    let view = storage::view(store);
    // Check actual outcomes, exact versions and schemas, not just numeric G.
    for sequence in baseline + 1..=manifest.checkpoint_sequence {
        feed::read_record(&view, sequence)?;
    }
    vm::validate_history(&view, manifest, store.genesis()).map_err(Error::Read)?;
    if kind != CursorKind::Resolved {
        log_available(store, baseline)?;
    }
    Ok(())
}

fn log_available(
    store: &Store,
    baseline: u64,
) -> Result<()> {
    let manifest = store.manifest();
    if manifest.log_floor > baseline + 1 {
        return Err(Error::HistoryUnavailable);
    }
    let mut next = baseline + 1;
    for segment in manifest
        .segments
        .iter()
        .filter(|segment| segment.last_sequence > baseline)
    {
        if segment.first_sequence > next {
            return Err(Error::HistoryUnavailable);
        }
        let file = File::open(
            store
                .directory()
                .join(format!("log-{:020}.bin", segment.segment_id)),
        )
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                Error::HistoryUnavailable
            } else {
                Error::Storage(error.into())
            }
        })?;
        if file
            .metadata()
            .map_err(|error| Error::Storage(error.into()))?
            .len()
            < segment.committed_bytes
        {
            return Err(Error::HistoryUnavailable);
        }
        let mut reader = BufReader::new(file.take(segment.committed_bytes));
        let mut header = [0; 96];
        reader
            .read_exact(&mut header)
            .map_err(|error| Error::Storage(error.into()))?;
        if header != engine::segment_header(manifest.database_id, segment) {
            return Err(Error::Storage(storage::Error::Corrupt(
                "cursor log header mismatch",
            )));
        }
        let mut remaining = segment.committed_bytes - 96;
        let mut predecessor = segment.predecessor_digest;
        for sequence in segment.first_sequence..=segment.last_sequence {
            let (bytes, digest) =
                engine::read_record(&mut reader, remaining, sequence, predecessor)
                    .map_err(Error::Storage)?;
            remaining -= bytes.len() as u64;
            predecessor = digest;
        }
        if remaining != 0 || predecessor != segment.last_digest {
            return Err(Error::Storage(storage::Error::Corrupt(
                "cursor log trailer mismatch",
            )));
        }
        next = segment.last_sequence + 1;
    }
    if next != manifest.checkpoint_sequence + 1 {
        return Err(Error::HistoryUnavailable);
    }
    Ok(())
}

/// Floors for later reclamation. Logical/replica baselines pin log from N + 1;
/// checkpoint recovery independently requires log from C + 1. Does not collect.
pub(crate) fn retention_floors(
    store: &Store,
    registry: &mut snapshot::Registry,
) -> Result<(u64, u64)> {
    let frontier = store.manifest().checkpoint_sequence;
    let mut history = snapshot::snapshot_floor(registry, frontier);
    let mut log = frontier + 1;
    for info in list(store)? {
        history = history.min(info.baseline);
        if info.token.kind != CursorKind::Resolved {
            log = log.min(info.baseline + 1);
        }
    }
    Ok((history, log))
}

pub(super) fn handle(
    store: &mut Store,
    registry: &mut snapshot::Registry,
    operation: Operation,
) -> Result<Response> {
    match operation {
        Operation::Checkout {
            baseline,
            kind,
            label,
        } => checkout(store, baseline, kind, label).map(Response::Token),
        Operation::SnapshotTail { kind, label } => {
            let snapshot = snapshot::capture(registry, store);
            let token = checkout(store, snapshot.watermark(), kind, label)?;
            Ok(Response::SnapshotTail(snapshot, token))
        }
        Operation::Reopen(token) => lookup(store, &token)?
            .ok_or(Error::CursorReleased)
            .map(Response::Info),
        Operation::Ack { token, watermark } => {
            let mut info = lookup(store, &token)?.ok_or(Error::CursorReleased)?;
            check_watermark(store, watermark)?;
            if watermark.sequence() < info.baseline
                || watermark.sequence() > store.manifest().checkpoint_sequence
            {
                return Err(Error::InvalidInput(
                    "cursor acknowledgement is backwards or above frontier",
                ));
            }
            if watermark.sequence() != info.baseline {
                info.baseline = watermark.sequence();
                publish(
                    store,
                    token.cursor_id,
                    Some(encode(&info)),
                    store.manifest().next_cursor_id,
                )?;
            }
            Ok(Response::Unit)
        }
        Operation::Release(token) => {
            if lookup(store, &token)?.is_some() {
                publish(
                    store,
                    token.cursor_id,
                    None,
                    store.manifest().next_cursor_id,
                )?;
            }
            Ok(Response::Unit)
        }
        Operation::List => list(store).map(Response::List),
        Operation::Floors => {
            let (history, log) = retention_floors(store, registry)?;
            Ok(Response::Floors(RetentionFloors { history, log }))
        }
        Operation::Feed {
            token,
            after,
            limits,
        } => feed::batch(store, &token, after, limits).map(Response::Batch),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database as db;
    use crate::storage::mvcc::StateKey;
    use crate::tx;
    use crate::vm::CatalogueOperation;
    use crate::vm::Type;

    #[tokio::test]
    async fn checkout_checks_actual_outcomes_versions_and_schema_not_just_floor() {
        for missing in [TreeId::Outcomes, TreeId::State, TreeId::Catalogue] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("db");
            let database = db::create(&path, db::CreateOptions::default())
                .await
                .unwrap();
            db::execute_catalogue(
                &database,
                CatalogueOperation::Create {
                    name: "items".into(),
                    key: Type::U64,
                    value: Type::I64,
                },
            )
            .await
            .unwrap();
            db::execute(
                &database,
                tx! { tables { items: u64 => i64 = 1 } items[7] = 42; }.unwrap(),
                db::Limits::default(),
            )
            .await
            .unwrap();
            db::close(&database).await.unwrap();
            let mut store = storage::open(&path).unwrap();
            let key = match missing {
                TreeId::Outcomes => 2_u64.to_be_bytes().to_vec(),
                TreeId::State => StateKey::new(1, 7_u64.to_be_bytes().to_vec(), 2)
                    .unwrap()
                    .encode(),
                TreeId::Catalogue => vm::catalogue_key(1, 1),
                _ => unreachable!(),
            };
            storage::apply(
                &mut store,
                &[Mutation {
                    tree: missing,
                    key,
                    value: None,
                }],
            )
            .unwrap();
            assert_eq!(store.manifest().history_floor, 0);
            let baseline = Watermark::new(store.genesis().database_id, 1).unwrap();
            assert!(matches!(
                checkout(&mut store, baseline, CursorKind::Resolved, "missing".into()),
                Err(Error::HistoryUnavailable)
            ));
            assert_eq!(store.manifest().next_cursor_id, 1);
            assert!(list(&store).unwrap().is_empty());
        }
    }
}
