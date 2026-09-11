//! Read fixed-sequence public views that can be explicitly revoked.
//!
//! Every public view holds a registered retention claim.

use std::ops::Bound;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::RwLockReadGuard;
use std::sync::Weak;

use super::Control;
use super::Database;
use super::Error;
use super::Result;
use super::Watermark;
use crate::storage::TreeId;
use crate::storage::View;
use crate::storage::encoding;
use crate::storage::mvcc;
use crate::storage::{
    self,
};
use crate::vm::CatalogueVersion;
use crate::vm::Table;
use crate::vm::Value;
use crate::vm::{
    self,
};

pub(crate) struct Claim {
    sequence: u64,
    view: RwLock<Option<View>>,
    // One validated historical table only; schema sizes are format-bounded.
    table: RwLock<Option<Arc<ReadTable>>>,
}

struct ReadTable {
    table: Table,
    key_schema: encoding::Schema,
}

enum TableRead<'a> {
    Cached(RwLockReadGuard<'a, Option<Arc<ReadTable>>>),
    Loaded(Arc<ReadTable>),
}

impl TableRead<'_> {
    fn table(&self) -> &Arc<ReadTable> {
        match self {
            Self::Cached(guard) => guard.as_ref().expect("a cached table is present"),
            Self::Loaded(table) => table,
        }
    }
}

/// A fixed-sequence view whose clones share one revocable retention claim.
///
/// Reads perform synchronous local I/O. Dropping the last clone releases the
/// claim, which is not persisted across restart.
#[derive(Clone)]
pub struct Snapshot {
    claim: Arc<Claim>,
    watermark: Watermark,
}

impl Snapshot {
    pub fn sequence(&self) -> u64 {
        self.claim.sequence
    }

    pub fn watermark(&self) -> Watermark {
        self.watermark
    }

    pub fn is_revoked(&self) -> bool {
        self.claim
            .view
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .is_none()
    }
}

impl std::fmt::Debug for Snapshot {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("Snapshot")
            .field("watermark", &self.watermark)
            .field("revoked", &self.is_revoked())
            .finish()
    }
}

/// Weak snapshot registrations used to select retention floors.
///
/// The writer allocates registrations and selects floors. A read guard protects
/// both the physical view and its retention claim.
#[derive(Default)]
pub(crate) struct Registry {
    claims: Vec<Weak<Claim>>,
}

impl Drop for Registry {
    fn drop(&mut self) {
        revoke_all(self);
    }
}

pub(crate) fn capture(
    registry: &mut Registry,
    store: &storage::Store,
    sequence: u64,
) -> Snapshot {
    registry.claims.retain(|claim| claim.strong_count() != 0);
    let claim = Arc::new(Claim {
        sequence,
        view: RwLock::new(Some(storage::view(store))),
        table: RwLock::new(None),
    });
    registry.claims.push(Arc::downgrade(&claim));
    Snapshot {
        claim,
        watermark: Watermark::new(store.genesis().database_id, sequence)
            .expect("validated frontier"),
    }
}

pub(crate) fn snapshot_floor(
    registry: &mut Registry,
    frontier: u64,
) -> u64 {
    let mut floor = frontier;
    registry.claims.retain(|weak| {
        let Some(claim) = weak.upgrade() else {
            return false;
        };
        let view = claim.view.read().unwrap_or_else(|error| error.into_inner());
        if view.is_some() {
            floor = floor.min(claim.sequence);
        }
        view.is_some()
    });
    floor
}

pub(crate) fn revoke_all(registry: &mut Registry) {
    for claim in registry.claims.drain(..).filter_map(|weak| weak.upgrade()) {
        revoke_claim(&claim);
    }
}

/// Capture the current durable visible prefix without consuming a sequence.
pub async fn snapshot(database: &Database) -> Result<Snapshot> {
    let (reply, result) = tokio::sync::oneshot::channel();
    database
        .control
        .send(Control::Snapshot { reply })
        .await
        .map_err(|_| Error::Closed)?;
    result.await.map_err(|_| Error::Closed)?
}

/// Revoke every clone and iterator of this snapshot.
///
/// Wait for active reads, but not idle handles. Return true only if this call
/// released the view. Durable tail cursors remain registered independently.
pub fn revoke(snapshot: &Snapshot) -> bool {
    revoke_claim(&snapshot.claim)
}

fn revoke_claim(claim: &Claim) -> bool {
    let mut view = claim
        .view
        .write()
        .unwrap_or_else(|error| error.into_inner());
    let revoked = view.take().is_some();
    claim
        .table
        .write()
        .unwrap_or_else(|error| error.into_inner())
        .take();
    revoked
}

fn live_table<'a>(
    view: &View,
    id: u64,
    sequence: u64,
    cached: &'a RwLock<Option<Arc<ReadTable>>>,
) -> Result<TableRead<'a>> {
    let guard = cached.read().unwrap_or_else(|error| error.into_inner());
    if guard.as_ref().is_some_and(|table| table.table.id == id) {
        return Ok(TableRead::Cached(guard));
    }
    drop(guard);
    let table = vm::catalogue_version(view, id, sequence)
        .map_err(Error::Read)?
        .filter(|version| version.live)
        .map(|version| version.table)
        .ok_or(Error::InvalidInput(
            "table is unknown or dropped at snapshot sequence",
        ))?;
    let key_schema = encoding::Schema::decode(&table.key.descriptor()).map_err(Error::Storage)?;
    let table = Arc::new(ReadTable { table, key_schema });
    *cached.write().unwrap_or_else(|error| error.into_inner()) = Some(table.clone());
    Ok(TableRead::Loaded(table))
}

fn key_bytes(
    table: &ReadTable,
    key: &Value,
) -> Result<Vec<u8>> {
    // Check against the stored, validated type before recursive value encoding.
    if !table.table.key.accepts(key) {
        return Err(Error::InvalidInput(
            "key does not match captured table schema",
        ));
    }
    let schema = &table.key_schema;
    match key {
        Value::Unit => encoding::encode_key(schema, &[]),
        Value::Boolean(value) => encoding::encode_key(schema, &[u8::from(*value)]),
        Value::U64(value) => encoding::encode_key(schema, &value.to_le_bytes()),
        Value::I64(value) => encoding::encode_key(schema, &value.to_le_bytes()),
        _ => encoding::encode_key(schema, &key.encode()),
    }
    .map_err(Error::Storage)
}

/// Look up a typed key using the table schema at the snapshot's sequence.
///
/// Return a typed value if the key exists. The captured catalogue, rather than
/// the current table or a caller-supplied schema, determines interpretation.
pub fn get(
    snapshot: &Snapshot,
    table: u64,
    key: &Value,
) -> Result<Option<Value>> {
    let guard = snapshot
        .claim
        .view
        .read()
        .unwrap_or_else(|error| error.into_inner());
    let view = guard.as_ref().ok_or(Error::SnapshotRevoked)?;
    let table = live_table(view, table, snapshot.sequence(), &snapshot.claim.table)?;
    let table = table.table();
    let key = key_bytes(table, key)?;
    mvcc::read_value(view, table.table.id, &key, snapshot.sequence())
        .map_err(Error::Storage)?
        .map(|bytes| {
            vm::decode_value(&table.table.value, bytes.as_bytes()).map_err(super::persisted_read)
        })
        .transpose()
}

/// Return complete metadata, including dropped tables, at the captured
/// sequence.
pub fn catalogue(snapshot: &Snapshot) -> Result<Vec<CatalogueVersion>> {
    let guard = snapshot
        .claim
        .view
        .read()
        .unwrap_or_else(|error| error.into_inner());
    let view = guard.as_ref().ok_or(Error::SnapshotRevoked)?;
    let mut result = Vec::new();
    let mut selected = None;
    for entry in storage::scan(view, TreeId::Catalogue, Bound::Unbounded, Bound::Unbounded)
        .map_err(Error::Storage)?
    {
        let (key, value) = entry.map_err(Error::Storage)?;
        let (id, sequence) = vm::decode_catalogue_key(&key).map_err(Error::Read)?;
        if sequence > snapshot.sequence() || selected == Some(id) {
            continue;
        }
        selected = Some(id);
        result.push(vm::decode_catalogue(id, &value).map_err(super::persisted_read)?);
    }
    Ok(result)
}

/// An ordered typed iterator that checks revocation on every call.
///
/// It holds no additional permanent storage view. After revocation, it returns
/// `Some(Err(SnapshotRevoked))`, even if it previously returned `None`. It is
/// therefore not a fused iterator.
pub struct SnapshotScan {
    snapshot: Snapshot,
    table: Arc<ReadTable>,
    lower: Bound<Vec<u8>>,
    upper: Bound<Vec<u8>>,
    done: bool,
}

/// Scan keys between typed endpoints at the snapshot's sequence.
///
/// Endpoints may be inclusive, exclusive, or `Unbounded` at a table edge. They
/// must match the captured key schema. Equal endpoints select a key only when
/// both are inclusive. Reversed endpoints are rejected.
pub fn scan(
    snapshot: &Snapshot,
    table: u64,
    lower: Bound<&Value>,
    upper: Bound<&Value>,
) -> Result<SnapshotScan> {
    let guard = snapshot
        .claim
        .view
        .read()
        .unwrap_or_else(|error| error.into_inner());
    let view = guard.as_ref().ok_or(Error::SnapshotRevoked)?;
    let table = live_table(view, table, snapshot.sequence(), &snapshot.claim.table)?;
    let table = table.table().clone();
    let bound = |bound| match bound {
        Bound::Unbounded => Ok(Bound::Unbounded),
        Bound::Included(key) => key_bytes(&table, key).map(Bound::Included),
        Bound::Excluded(key) => key_bytes(&table, key).map(Bound::Excluded),
    };
    let lower = bound(lower)?;
    let upper = bound(upper)?;
    let mut done = false;
    if let (Bound::Included(a) | Bound::Excluded(a), Bound::Included(b) | Bound::Excluded(b)) =
        (&lower, &upper)
    {
        if a > b {
            return Err(Error::InvalidInput("reversed scan endpoints"));
        }
        done = a == b
            && (!matches!(lower, Bound::Included(_)) || !matches!(upper, Bound::Included(_)));
    }
    Ok(SnapshotScan {
        snapshot: snapshot.clone(),
        table,
        lower,
        upper,
        done,
    })
}

impl Iterator for SnapshotScan {
    type Item = Result<(Value, Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        let guard = self
            .snapshot
            .claim
            .view
            .read()
            .unwrap_or_else(|error| error.into_inner());
        let Some(view) = guard.as_ref() else {
            return Some(Err(Error::SnapshotRevoked));
        };
        if self.done {
            return None;
        }
        let read = || -> Result<Option<(Vec<u8>, Value, Value)>> {
            let mut rows = mvcc::scan(
                view,
                self.table.table.id,
                self.snapshot.sequence(),
                self.lower.as_ref().map(Vec::as_slice),
                self.upper.as_ref().map(Vec::as_slice),
            )
            .map_err(Error::Storage)?;
            let Some((key, value)) = rows.next().transpose().map_err(Error::Storage)? else {
                return Ok(None);
            };
            let decoded = encoding::decode_key(&self.table.key_schema, &key)
                .map_err(|error| super::persisted_read(error.into()))?;
            let decoded =
                vm::decode_value(&self.table.table.key, &decoded).map_err(super::persisted_read)?;
            let value =
                vm::decode_value(&self.table.table.value, &value).map_err(super::persisted_read)?;
            Ok(Some((key, decoded, value)))
        };
        match read() {
            Ok(Some((key, decoded, value))) => {
                self.lower = Bound::Excluded(key);
                Some(Ok((decoded, value)))
            }
            Ok(None) => {
                self.done = true;
                None
            }
            Err(error) => {
                self.done = true;
                Some(Err(error))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::Duration;

    use super::*;
    use crate::database as db;
    use crate::tx;
    use crate::vm::CatalogueOperation;
    use crate::vm::Type;

    #[tokio::test]
    async fn cached_tables_are_snapshot_local_bounded_and_cleared_on_close() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("db");
        let database = db::create(&path, db::CreateOptions::default())
            .await
            .unwrap();
        let empty = snapshot(&database).await.unwrap();
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
            tx! { tables { items: u64 => i64 = 1 } items[7] = 10; }.unwrap(),
            db::Limits::default(),
        )
        .await
        .unwrap();
        db::execute_catalogue(
            &database,
            CatalogueOperation::Create {
                name: "other".into(),
                key: Type::Bytes(4),
                value: Type::String(8),
            },
        )
        .await
        .unwrap();
        let captured = snapshot(&database).await.unwrap();
        let other = catalogue(&captured)
            .unwrap()
            .into_iter()
            .find(|entry| entry.name == "other")
            .unwrap()
            .table
            .id;
        assert!(matches!(
            get(&empty, 1, &Value::U64(7)),
            Err(Error::InvalidInput(_))
        ));
        assert!(empty.claim.table.read().unwrap().is_none());
        assert_eq!(
            get(&captured, 1, &Value::U64(7)).unwrap(),
            Some(Value::I64(10))
        );
        let warm = captured.claim.table.read().unwrap().clone().unwrap();
        assert_eq!(
            get(&captured.clone(), 1, &Value::U64(7)).unwrap(),
            Some(Value::I64(10))
        );
        assert!(Arc::ptr_eq(
            &warm,
            captured.claim.table.read().unwrap().as_ref().unwrap()
        ));
        assert!(get(&captured, 0, &Value::U64(7)).is_err());
        assert!(matches!(
            get(&captured, 1, &Value::I64(7)),
            Err(Error::InvalidInput(_))
        ));
        assert!(Arc::ptr_eq(
            &warm,
            captured.claim.table.read().unwrap().as_ref().unwrap()
        ));

        let mut rows = scan(&captured, 1, Bound::Unbounded, Bound::Unbounded).unwrap();
        assert!(Arc::ptr_eq(&warm, &rows.table));
        assert_eq!(
            rows.next().unwrap().unwrap(),
            (Value::U64(7), Value::I64(10))
        );
        assert_eq!(get(&captured, other, &Value::Bytes(vec![7])).unwrap(), None);
        assert_eq!(
            captured
                .claim
                .table
                .read()
                .unwrap()
                .as_ref()
                .unwrap()
                .table
                .id,
            other
        );
        assert!(rows.next().is_none());
        std::thread::scope(|scope| {
            for _ in 0..4 {
                let captured = &captured;
                scope.spawn(move || {
                    for _ in 0..64 {
                        assert_eq!(
                            get(captured, 1, &Value::U64(7)).unwrap(),
                            Some(Value::I64(10))
                        );
                        assert_eq!(get(captured, other, &Value::Bytes(vec![7])).unwrap(), None);
                        assert!(get(captured, other, &Value::Bytes(vec![7; 5])).is_err());
                    }
                });
            }
        });
        db::execute_catalogue(&database, CatalogueOperation::Drop { table: 1 })
            .await
            .unwrap();
        let dropped = snapshot(&database).await.unwrap();
        assert!(matches!(
            get(&dropped, 1, &Value::U64(7)),
            Err(Error::InvalidInput(_))
        ));
        assert!(dropped.claim.table.read().unwrap().is_none());
        assert_eq!(
            get(&captured, 1, &Value::U64(7)).unwrap(),
            Some(Value::I64(10))
        );
        db::close(&database).await.unwrap();
        for snapshot in [&empty, &captured, &dropped] {
            assert!(snapshot.claim.table.read().unwrap().is_none());
            assert!(matches!(
                get(snapshot, 1, &Value::U64(7)),
                Err(Error::SnapshotRevoked)
            ));
        }
        assert!(matches!(rows.next(), Some(Err(Error::SnapshotRevoked))));
        let reopened = db::open(&path).await.unwrap();
        let current = snapshot(&reopened).await.unwrap();
        assert!(matches!(
            get(&current, 1, &Value::U64(7)),
            Err(Error::InvalidInput(_))
        ));
        assert_eq!(get(&current, other, &Value::Bytes(vec![7])).unwrap(), None);
        db::close(&reopened).await.unwrap();
    }

    #[tokio::test]
    async fn revocation_waits_for_in_flight_original_view_but_not_idle_clones() {
        let directory = tempfile::tempdir().unwrap();
        let database = db::create(directory.path().join("db"), db::CreateOptions::default())
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
            tx! { tables { items: u64 => i64 = 1 } items[7] = 10; }.unwrap(),
            db::Limits::default(),
        )
        .await
        .unwrap();
        let snapshot = snapshot(&database).await.unwrap();
        assert_eq!(
            get(&snapshot, 1, &Value::U64(7)).unwrap(),
            Some(Value::I64(10))
        );
        db::execute(
            &database,
            tx! { tables { items: u64 => i64 = 1 } items[7] = 20; }.unwrap(),
            db::Limits::default(),
        )
        .await
        .unwrap();
        {
            let clone = snapshot.clone();
            let guard = snapshot.claim.view.read().unwrap();
            let (started, wait_started) = mpsc::channel();
            let (completed, wait_completed) = mpsc::channel();
            let worker = std::thread::spawn(move || {
                started.send(()).unwrap();
                completed.send(revoke(&clone)).unwrap();
            });
            wait_started.recv().unwrap();
            assert!(matches!(
                wait_completed.recv_timeout(Duration::from_millis(20)),
                Err(mpsc::RecvTimeoutError::Timeout)
            ));
            assert_eq!(
                mvcc::get(
                    guard.as_ref().unwrap(),
                    1,
                    &7_u64.to_be_bytes(),
                    snapshot.sequence()
                )
                .unwrap(),
                Some(10_i64.to_le_bytes().to_vec())
            );
            drop(guard);
            assert!(wait_completed.recv_timeout(Duration::from_secs(5)).unwrap());
            worker.join().unwrap();
        }
        assert!(matches!(
            get(&snapshot, 1, &Value::U64(7)),
            Err(Error::SnapshotRevoked)
        ));
        assert!(snapshot.claim.table.read().unwrap().is_none());
        db::close(&database).await.unwrap();
        let reopened = db::open(directory.path().join("db")).await.unwrap();
        db::close(&reopened).await.unwrap();
    }
}
