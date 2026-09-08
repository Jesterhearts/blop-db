//! Revocable public views. No public handle owns an unregistered storage lease.

use std::ops::Bound;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::Weak;

use super::Database;
use super::Error;
use super::Request;
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
}

/// Clones share one revocable claim at the original sequence. Reads perform
/// synchronous local I/O. Dropping the last clone releases this ephemeral
/// claim.
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

/// Weak registrations for phase-5 retention. All allocation and floor selection
/// occurs on the writer. A read guard protects both its view and retention
/// claim.
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
) -> Snapshot {
    registry.claims.retain(|claim| claim.strong_count() != 0);
    let sequence = store.manifest().checkpoint_sequence;
    let claim = Arc::new(Claim {
        sequence,
        view: RwLock::new(Some(storage::view(store))),
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
        claim
            .view
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .take();
    }
}

/// Capture the current durable visible prefix without consuming a sequence.
pub async fn snapshot(database: &Database) -> Result<Snapshot> {
    let (reply, result) = tokio::sync::oneshot::channel();
    database
        .sender
        .send(Request::Snapshot { reply })
        .await
        .map_err(|_| Error::Closed)?;
    result.await.map_err(|_| Error::Closed)?
}

/// Revoke all clones and iterators of this snapshot. Waits for in-flight reads,
/// not for idle handles. Returns true only when this call released the view.
/// Durable tail cursors are independent and remain registered.
pub fn revoke(snapshot: &Snapshot) -> bool {
    snapshot
        .claim
        .view
        .write()
        .unwrap_or_else(|error| error.into_inner())
        .take()
        .is_some()
}

fn live_table(
    view: &View,
    id: u64,
    sequence: u64,
) -> Result<Table> {
    vm::catalogue_version(view, id, sequence)
        .map_err(Error::Read)?
        .filter(|version| version.live)
        .map(|version| version.table)
        .ok_or(Error::InvalidInput(
            "table is unknown or dropped at snapshot sequence",
        ))
}

fn key_bytes(
    table: &Table,
    key: &Value,
) -> Result<Vec<u8>> {
    // Check against the stored, validated type before recursive value encoding.
    if !table.key.accepts(key) {
        return Err(Error::InvalidInput(
            "key does not match captured table schema",
        ));
    }
    let schema = encoding::Schema::decode(&table.key.descriptor()).map_err(Error::Storage)?;
    encoding::encode_key(&schema, &key.encode()).map_err(Error::Storage)
}

/// Read a typed key and return a typed value under the actual schema at the
/// captured sequence. This is not a current-table or caller-declared schema
/// read.
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
    let table = live_table(view, table, snapshot.sequence())?;
    let key = key_bytes(&table, key)?;
    mvcc::get(view, table.id, &key, snapshot.sequence())
        .map_err(Error::Storage)?
        .map(|bytes| vm::decode_value(&table.value, &bytes).map_err(super::persisted_read))
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

/// An ordered typed scan. It holds no extra permanent storage view. Every call
/// checks revocation, even after ordinary exhaustion, and returns
/// `Some(Err(SnapshotRevoked))` if revoked. It is deliberately not fused.
pub struct SnapshotScan {
    snapshot: Snapshot,
    table: Table,
    lower: Bound<Vec<u8>>,
    upper: Bound<Vec<u8>>,
    done: bool,
}

/// Scan using typed inclusive/exclusive endpoints; `Unbounded` selects a table
/// edge. Endpoints must match the table's captured key schema. Equal endpoints
/// select one key only if both are inclusive. Reversed endpoints are rejected.
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
    let table = live_table(view, table, snapshot.sequence())?;
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
                self.table.id,
                self.snapshot.sequence(),
                self.lower.as_ref().map(Vec::as_slice),
                self.upper.as_ref().map(Vec::as_slice),
            )
            .map_err(Error::Storage)?;
            let Some((key, value)) = rows.next().transpose().map_err(Error::Storage)? else {
                return Ok(None);
            };
            let schema =
                encoding::Schema::decode(&self.table.key.descriptor()).map_err(Error::Storage)?;
            let decoded = encoding::decode_key(&schema, &key)
                .map_err(|error| super::persisted_read(error.into()))?;
            let decoded =
                vm::decode_value(&self.table.key, &decoded).map_err(super::persisted_read)?;
            let value =
                vm::decode_value(&self.table.value, &value).map_err(super::persisted_read)?;
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
        db::close(&database).await.unwrap();
        let reopened = db::open(directory.path().join("db")).await.unwrap();
        db::close(&reopened).await.unwrap();
    }
}
