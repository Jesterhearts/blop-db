//! Local maintenance controls, separate from canonical sequence assignment.

use std::fs::File;
use std::io::BufReader;
use std::io::Read;
use std::path::Path;
use std::path::PathBuf;
use std::thread;

pub use storage::maintenance::Reclaimed;
use tokio::sync::oneshot;

use super::Control;
use super::Database;
use super::Error;
use super::Result;
use super::RetentionFloors;
use super::cursor;
use super::engine;
use super::record;
use super::snapshot;
use crate::storage;
use crate::vm;

/// Attachment does not create a new canonical history. A writable restore is
/// permitted only when the caller has retired the source primary. This local
/// library cannot fence a primary running on another machine.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttachMode {
    RestorePrimarySourceRetired,
    /// Reject canonical submissions, not local storage writes. Cursor changes,
    /// GC and compaction may apply this replica's own retention policy.
    ReadOnlyReplica,
}

/// Explicitly attach a physical copy with a fresh random cursor namespace.
/// Registrations and counters survive unchanged; old tokens do not. Normal
/// reopen preserves both the attached namespace and the read-only role.
///
/// Every call renews the namespace, even on an unmarked or already attached
/// directory. `ATTACH_REQUIRED` is not a precondition. Use `open` for normal
/// crash recovery; the caller must select the actual copied directory, or the
/// intended restore path after retiring the source primary.
pub async fn attach(
    path: impl AsRef<Path>,
    mode: AttachMode,
) -> Result<Database> {
    attach_with_options(path, mode, super::EngineOptions::default()).await
}

/// Explicit attachment with local engine options. Like `attach`, every call
/// renews the namespace, whether or not the directory has an attachment marker.
pub async fn attach_with_options(
    path: impl AsRef<Path>,
    mode: AttachMode,
    options: super::EngineOptions,
) -> Result<Database> {
    super::start(path.as_ref().to_owned(), None, options, Some(mode)).await
}

/// Manual maintenance. Default collects eligible history, compacts all five
/// trees and seals the current log. All-false only validates and reclaims
/// files.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MaintenanceOptions {
    pub collect_history: bool,
    pub compact: bool,
    /// The next canonical record starts a fresh nonempty segment. Reopening
    /// also starts a fresh segment, so crashes cannot undo this boundary.
    pub rotate_log: bool,
}

impl Default for MaintenanceOptions {
    fn default() -> Self {
        Self {
            collect_history: true,
            compact: true,
            rotate_log: true,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaintenanceReport {
    pub frontier: u64,
    pub generation: u64,
    pub previous_floors: RetentionFloors,
    pub floors: RetentionFloors,
    pub versions_removed: u64,
    pub outcomes_removed: u64,
    pub segments_retired: u64,
    pub previous_page_file: u64,
    pub page_file: u64,
    pub previous_pages: u64,
    pub pages: u64,
    pub rotation_pending: bool,
    pub reclaimed: Reclaimed,
}

/// Pause assignment, drain durable work, publish and adopt a validated
/// candidate, then reclaim whole files whose directory leases have retired.
/// Cancellation after enqueue does not cancel maintenance. No canonical
/// sequence is consumed.
/// Read-only replicas also permit local GC and compaction: these change
/// retained physical history, not the canonical sequence or its resolved state.
pub async fn maintain(
    database: &Database,
    options: MaintenanceOptions,
) -> Result<MaintenanceReport> {
    let (reply, result) = oneshot::channel();
    database
        .control
        .send(Control::Maintenance { options, reply })
        .await
        .map_err(|_| Error::Closed)?;
    result.await.map_err(|_| Error::Uncertain {
        sequence: None,
        source: None,
    })?
}

/// Copy one pinned published C,D image on a blocking thread while the source
/// continues executing. Destination must not exist. Close waits for accepted
/// copies, even if their waiters were cancelled. The result is the exact copied
/// manifest, not the source's later frontier. Output requires explicit
/// `attach`.
pub async fn backup(
    database: &Database,
    destination: impl AsRef<Path>,
) -> Result<storage::Manifest> {
    let (reply, result) = oneshot::channel();
    database
        .control
        .send(Control::Backup {
            destination: destination.as_ref().to_owned(),
            reply,
        })
        .await
        .map_err(|_| Error::Closed)?;
    result.await.map_err(|_| Error::Closed)?
}

/// Validate correspondence against ALL still-present source records before
/// retiring any segment. History completeness is preserved by candidate
/// construction, not inferred merely from a checksum or latest values.
fn validate(
    store: &storage::Store,
    view: &storage::View,
    manifest: &storage::Manifest,
) -> Result<()> {
    let history =
        vm::validate_history(view, manifest, store.genesis()).map_err(super::persisted_read)?;
    for segment in &store.manifest().segments {
        let file = File::open(
            store
                .directory()
                .join(format!("log-{:020}.bin", segment.segment_id)),
        )
        .map_err(|e| Error::Storage(e.into()))?;
        let mut reader = BufReader::new(file.take(segment.committed_bytes));
        let mut header = [0; 96];
        reader
            .read_exact(&mut header)
            .map_err(|e| Error::Storage(e.into()))?;
        if header != engine::segment_header(manifest.database_id, segment) {
            return Err(Error::Storage(storage::Error::Corrupt(
                "maintenance log header mismatch",
            )));
        }
        let mut remaining = segment.committed_bytes - 96;
        let mut predecessor = segment.predecessor_digest;
        for sequence in segment.first_sequence..=segment.last_sequence {
            let (bytes, digest) =
                engine::read_record(&mut reader, remaining, sequence, predecessor)
                    .map_err(Error::Storage)?;
            remaining -= bytes.len() as u64;
            let command = record::decode(bytes[24], &bytes[64..bytes.len() - 8])
                .map_err(super::persisted_read)?;
            engine::validate_checkpoint_record(
                view, &history, sequence, digest, bytes[24], &command,
            )
            .map_err(super::persisted_read)?;
            predecessor = digest;
        }
        if remaining != 0 || predecessor != segment.last_digest {
            return Err(Error::Storage(storage::Error::Corrupt(
                "maintenance log endpoint mismatch",
            )));
        }
    }
    Ok(())
}

pub(super) fn run(
    store: &mut storage::Store,
    registry: &mut snapshot::Registry,
    frontier: u64,
    options: MaintenanceOptions,
) -> Result<MaintenanceReport> {
    let previous = store.manifest().clone();
    if frontier != previous.durable_sequence || previous.checkpoint_sequence > frontier {
        return Err(Error::Storage(storage::Error::InvalidInput(
            "maintenance sequencer is not drained",
        )));
    }
    // Do not assume normal per-frontier checkpointing made C equal to F.
    let checkpoint = storage::prepare_checkpoint(store, frontier).map_err(Error::Storage)?;
    let mut manifest = previous.clone();
    manifest.checkpoint_sequence = frontier;
    manifest.checkpoint_digest = manifest.durable_digest;
    validate(store, &checkpoint, &manifest)?;
    storage::publish(store, &checkpoint, manifest).map_err(Error::Storage)?;
    drop(checkpoint);
    let (history, log) = cursor::retention_floors(store, registry, frontier)?;
    let candidate = storage::maintenance::prepare(
        store,
        history,
        log,
        options.collect_history,
        options.compact,
    )
    .map_err(Error::Storage)?;
    validate(store, &candidate.view, &candidate.manifest)?;
    let mut report = MaintenanceReport {
        frontier,
        generation: 0,
        previous_floors: RetentionFloors {
            history: previous.history_floor,
            log: previous.log_floor,
        },
        floors: RetentionFloors {
            history: candidate.manifest.history_floor,
            log: candidate.manifest.log_floor,
        },
        versions_removed: candidate.versions_removed,
        outcomes_removed: candidate.outcomes_removed,
        segments_retired: (previous.segments.len() - candidate.manifest.segments.len()) as u64,
        previous_page_file: previous.page_file_id,
        page_file: candidate.manifest.page_file_id,
        previous_pages: previous.page_count,
        pages: candidate.manifest.page_count,
        rotation_pending: false,
        reclaimed: Reclaimed::default(),
    };
    storage::maintenance::install(store, candidate).map_err(Error::Storage)?;
    store.rotate_next |= options.rotate_log;
    report.rotation_pending = store.rotate_next;
    report.generation = store.manifest().generation;
    report.reclaimed = storage::maintenance::reclaim(store).map_err(Error::Storage)?;
    Ok(report)
}

/// Join on every exit, including coordinator unwind. No public idle backup
/// handle holds a pin or can keep the directory locked after close completes.
#[derive(Default)]
pub(super) struct Backups {
    jobs: Vec<thread::JoinHandle<()>>,
}

impl Backups {
    pub(super) fn active(&self) -> usize {
        self.jobs.iter().filter(|job| !job.is_finished()).count()
    }
}

impl Drop for Backups {
    fn drop(&mut self) {
        for job in self.jobs.drain(..) {
            let _ = job.join();
        }
    }
}

pub(super) fn start_backup(
    jobs: &mut Backups,
    store: &storage::Store,
    destination: PathBuf,
    reply: oneshot::Sender<Result<storage::Manifest>>,
    #[cfg(test)] hooks: std::sync::Arc<super::workers::test_support::Hooks>,
) {
    jobs.jobs.retain(|job| !job.is_finished());
    if jobs.active() >= 4 {
        let _ = reply.send(Err(Error::OperationalLimit {
            resource: "backup_jobs",
            required: 5,
            limit: 4,
        }));
        return;
    }
    let image = match storage::backup::capture(store) {
        Ok(image) => image,
        Err(error) => {
            let _ = reply.send(Err(Error::Storage(error)));
            return;
        }
    };
    // Keep the sender outside the closure until spawn succeeds, so a failed
    // spawn still reports a definite error rather than an unexplained close.
    let reply = std::sync::Arc::new(std::sync::Mutex::new(Some(reply)));
    let worker_reply = reply.clone();
    match thread::Builder::new()
        .name("blop-db-backup".into())
        .spawn(move || {
            // Sequence zero is reserved and is the per-database backup test
            // gate.
            #[cfg(test)]
            super::workers::test_support::before(&hooks, 0).unwrap();
            let result = storage::backup::copy(image, &destination).map_err(Error::Storage);
            if let Some(reply) = worker_reply.lock().unwrap().take() {
                let _ = reply.send(result);
            }
        }) {
        Ok(job) => jobs.jobs.push(job),
        Err(error) => {
            let _ = reply
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .send(Err(Error::Storage(error.into())));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::future::Future;
    use std::ops::Bound::Unbounded;
    use std::pin::pin;
    use std::sync::Arc;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;
    use std::time::Duration;
    use std::time::Instant;

    use super::*;
    use crate::database as db;
    use crate::database::workers::test_support::Action;
    use crate::database::workers::test_support::Gate;
    use crate::storage::TreeId;
    use crate::storage::mvcc::StateKey;
    use crate::tx;
    use crate::vm::CatalogueOperation;
    use crate::vm::Type;
    use crate::vm::Value;

    fn only_rotate() -> MaintenanceOptions {
        MaintenanceOptions {
            collect_history: false,
            compact: false,
            rotate_log: true,
        }
    }

    async fn fixture() -> (tempfile::TempDir, Database) {
        let directory = tempfile::tempdir().unwrap();
        let database = db::create(directory.path().join("db"), db::CreateOptions::default())
            .await
            .unwrap();
        db::execute_catalogue(
            &database,
            CatalogueOperation::Create {
                name: "data".into(),
                key: Type::U64,
                value: Type::U64,
            },
        )
        .await
        .unwrap();
        db::execute(
            &database,
            tx! { tables { data: u64 => u64 = 1 } data[1] = 10; data[2] = 20; }.unwrap(),
            db::Limits::default(),
        )
        .await
        .unwrap();
        (directory, database)
    }

    async fn enqueue(
        database: &Database,
        transaction: crate::Transaction,
    ) -> oneshot::Receiver<Result<super::super::Receipt>> {
        let command = record::Command::Transaction {
            transaction,
            claims: db::Limits::default().try_into().unwrap(),
            manifest: None,
        };
        let permit = super::super::budget::reserve(&database.queue, &command)
            .await
            .unwrap();
        let (reply, result) = oneshot::channel();
        database
            .sender
            .send(super::super::Request::Execute {
                command: Box::new(command),
                reply,
                permit,
            })
            .await
            .unwrap();
        result
    }

    async fn wait(
        database: &Database,
        predicate: impl Fn(&db::EngineStatus) -> bool,
    ) {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let status = db::status(database);
            if predicate(&status) {
                return;
            }
            assert!(Instant::now() < deadline, "status timeout: {status:?}");
            tokio::task::yield_now().await;
        }
    }

    fn entries(
        store: &storage::Store,
        tree: TreeId,
    ) -> Vec<storage::Entry> {
        storage::scan(&storage::view(store), tree, Unbounded, Unbounded)
            .unwrap()
            .collect::<storage::Result<_>>()
            .unwrap()
    }

    fn published(path: &Path) -> storage::Manifest {
        let current = storage::Current::decode(&fs::read(path.join("CURRENT")).unwrap()).unwrap();
        let manifest = storage::Manifest::decode(
            &fs::read(path.join(format!("manifest-{:020}.bin", current.generation))).unwrap(),
        )
        .unwrap();
        assert_eq!(manifest.generation, current.generation);
        manifest
    }

    #[tokio::test]
    async fn snapshot_only_gc_without_compaction_publishes_floor_and_survives_reopen_and_handover()
    {
        let (directory, database) = fixture().await;
        let path = directory.path().join("db");
        db::execute(
            &database,
            tx! { tables { data: u64 => u64 = 1 } data[1] = 11; }.unwrap(),
            db::Limits::default(),
        )
        .await
        .unwrap();
        let old = db::snapshot(&database).await.unwrap();
        let baseline = old.watermark();
        assert_eq!(baseline.sequence(), 3);
        for transaction in [
            tx! { tables { data: u64 => u64 = 1 } data[1] = 12; }.unwrap(),
            tx! { tables { data: u64 => u64 = 1 } delete(data[1]); }.unwrap(),
            tx! { tables { data: u64 => u64 = 1 } data[2] = 21; }.unwrap(),
        ] {
            db::execute(&database, transaction, db::Limits::default())
                .await
                .unwrap();
        }
        assert!(db::list_cursors(&database).await.unwrap().is_empty());
        let before = published(&path);
        let old_manifest = path.join(format!("manifest-{:020}.bin", before.generation));
        let old_log = path.join(format!("log-{:020}.bin", before.segments[0].segment_id));
        let gc = MaintenanceOptions {
            collect_history: true,
            compact: false,
            rotate_log: false,
        };
        let report = maintain(&database, gc).await.unwrap();
        let retained = published(&path);
        // Reading the old view alone would still pass if GC ignored its claim.
        assert_eq!(retained.generation, report.generation);
        assert_eq!(
            (
                retained.history_floor,
                retained.checkpoint_sequence,
                retained.durable_sequence
            ),
            (3, 6, 6)
        );
        assert_eq!(report.floors.history, 3);
        assert_eq!(retained.page_file_id, before.page_file_id);
        assert_eq!(retained.next_page_file_id, before.next_page_file_id);
        assert_eq!((report.versions_removed, report.outcomes_removed), (1, 3));
        assert_eq!(report.reclaimed.files, 0);
        assert!(report.reclaimed.deferred_files > 0);
        assert!(old_manifest.exists() && old_log.exists());
        assert_eq!(
            db::get(&old, 1, &Value::U64(1)).unwrap(),
            Some(Value::U64(11))
        );
        assert_eq!(
            db::get(&old, 1, &Value::U64(2)).unwrap(),
            Some(Value::U64(20))
        );

        assert!(db::revoke(&old));
        let report = maintain(&database, gc).await.unwrap();
        let collected = published(&path);
        assert_eq!(collected.history_floor, 6);
        assert_eq!(report.floors.history, 6);
        assert_eq!(collected.page_file_id, before.page_file_id);
        assert_eq!((report.versions_removed, report.outcomes_removed), (3, 3));
        assert!(report.reclaimed.files > 0);
        assert_eq!(report.reclaimed.deferred_files, 0);
        assert!(!old_manifest.exists() && !old_log.exists());
        assert!(matches!(
            db::checkout_cursor(&database, baseline, db::CursorKind::Resolved, "expired").await,
            Err(Error::HistoryUnavailable)
        ));
        assert!(db::list_cursors(&database).await.unwrap().is_empty());
        assert_eq!(published(&path).next_cursor_id, 1);
        db::close(&database).await.unwrap();

        let store = storage::open(&path).unwrap();
        assert_eq!(store.manifest().history_floor, 6);
        let state = entries(&store, TreeId::State);
        assert_eq!(state.len(), 2);
        assert_eq!(StateKey::decode(&state[0].0).unwrap().sequence(), 5);
        assert_eq!(state[0].1, vec![0]);
        assert!(entries(&store, TreeId::Outcomes).is_empty());
        drop(store);
        let database = db::open(&path).await.unwrap();
        let latest = db::snapshot(&database).await.unwrap();
        assert_eq!(latest.sequence(), 6);
        assert_eq!(db::get(&latest, 1, &Value::U64(1)).unwrap(), None);
        assert_eq!(
            db::get(&latest, 1, &Value::U64(2)).unwrap(),
            Some(Value::U64(21))
        );
        let report = maintain(
            &database,
            MaintenanceOptions {
                collect_history: false,
                compact: true,
                rotate_log: false,
            },
        )
        .await
        .unwrap();
        assert_ne!(report.page_file, collected.page_file_id);
        assert_eq!(report.floors.history, 6);
        let old_pages = path.join(format!("pages-{:020}.bin", collected.page_file_id));
        let active_pages = path.join(format!("pages-{:020}.bin", report.page_file));
        assert!(old_pages.exists() && active_pages.exists());
        assert!(report.reclaimed.deferred_files > 0);
        assert_eq!(
            db::execute(
                &database,
                tx! { tables { data: u64 => u64 = 1 } data[1] = 42; data[2] += 1; }.unwrap(),
                db::Limits::default()
            )
            .await
            .unwrap()
            .sequence,
            7
        );
        db::close(&database).await.unwrap();
        assert!(latest.is_revoked());
        assert!(old_pages.exists());

        let database = db::open(&path).await.unwrap();
        let cleanup = maintain(
            &database,
            MaintenanceOptions {
                collect_history: false,
                compact: false,
                rotate_log: false,
            },
        )
        .await
        .unwrap();
        assert!(cleanup.reclaimed.files > 0);
        assert_eq!(cleanup.reclaimed.deferred_files, 0);
        assert!(!old_pages.exists() && active_pages.exists());
        let latest = db::snapshot(&database).await.unwrap();
        assert_eq!(latest.sequence(), 7);
        assert_eq!(
            db::get(&latest, 1, &Value::U64(1)).unwrap(),
            Some(Value::U64(42))
        );
        assert_eq!(
            db::get(&latest, 1, &Value::U64(2)).unwrap(),
            Some(Value::U64(22))
        );
        db::close(&database).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn queued_checkouts_wait_for_maintenance_and_preserve_only_available_claims() {
        let (directory, database) = fixture().await;
        let path = directory.path().join("db");
        let oldest = db::snapshot(&database).await.unwrap();
        db::execute(
            &database,
            tx! { tables { data: u64 => u64 = 1 } data[1] = 11; }.unwrap(),
            db::Limits::default(),
        )
        .await
        .unwrap();
        let gate = Arc::new(Gate::default());
        database
            .hooks
            .actions
            .lock()
            .unwrap()
            .insert(4, Action::Gate(gate.clone()));
        let first = enqueue(
            &database,
            tx! { tables { data: u64 => u64 = 1 } data[1] += 1; }.unwrap(),
        )
        .await;
        let later = enqueue(
            &database,
            tx! { tables { data: u64 => u64 = 1 } data[2] = 99; }.unwrap(),
        )
        .await;
        wait(&database, |s| {
            s.visibility_frontier == 3 && s.durable_frontier == 5 && s.resolved_above_frontier == 1
        })
        .await;
        let (reply, maintained) = oneshot::channel();
        database
            .control
            .send(Control::Maintenance {
                options: MaintenanceOptions::default(),
                reply,
            })
            .await
            .unwrap();
        wait(&database, |s| s.maintenance_pending).await;
        let mut unavailable = pin!(db::checkout_cursor(
            &database,
            db::Watermark::new(database.database_id(), 1).unwrap(),
            db::CursorKind::Resolved,
            "too old"
        ));
        let mut available = pin!(db::checkout_cursor(
            &database,
            db::Watermark::new(database.database_id(), 3).unwrap(),
            db::CursorKind::Resolved,
            "current F"
        ));
        assert!(matches!(
            unavailable
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
        assert!(matches!(
            available
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
        let before = published(&path);
        assert_eq!(before.next_cursor_id, 1);
        assert_eq!(before.roots[TreeId::Cursors.index()], 0);
        // This second handover must include the successful preceding checkout.
        let (reply, second_maintenance) = oneshot::channel();
        database
            .control
            .send(Control::Maintenance {
                options: MaintenanceOptions::default(),
                reply,
            })
            .await
            .unwrap();
        gate.release();
        assert_eq!(first.await.unwrap().unwrap().sequence, 4);
        assert_eq!(later.await.unwrap().unwrap().sequence, 5);
        let report = maintained.await.unwrap().unwrap();
        assert_eq!((report.frontier, report.floors.history), (5, 2));
        assert!(matches!(unavailable.await, Err(Error::HistoryUnavailable)));
        let token = available.await.unwrap();
        assert_eq!(token.cursor_id(), 1);
        assert_eq!(second_maintenance.await.unwrap().unwrap().floors.history, 2);
        let cursors = db::list_cursors(&database).await.unwrap();
        assert_eq!(cursors.len(), 1);
        assert_eq!(cursors[0].token, token);
        assert_eq!(cursors[0].baseline, 3);
        assert_eq!(cursors[0].label, "current F");
        assert_eq!(published(&path).next_cursor_id, 2);
        db::revoke(&oldest);
        db::execute(
            &database,
            tx! { tables { data: u64 => u64 = 1 } data[2] += 1; }.unwrap(),
            db::Limits::default(),
        )
        .await
        .unwrap();
        let report = maintain(&database, MaintenanceOptions::default())
            .await
            .unwrap();
        assert_eq!((report.frontier, published(&path).history_floor), (6, 3));
        db::close(&database).await.unwrap();
        let database = db::open(&path).await.unwrap();
        let info = db::reopen_cursor(&database, &token).await.unwrap();
        assert_eq!(info.baseline, 3);
        let batch = db::read_feed(
            &database,
            &token,
            db::Watermark::new(database.database_id(), 3).unwrap(),
            db::BatchLimits::default(),
        )
        .await
        .unwrap();
        let db::FeedRecords::Resolved(records) = batch.records else {
            panic!("expected resolved records")
        };
        assert_eq!(
            records
                .iter()
                .map(|record| record.sequence)
                .collect::<Vec<_>>(),
            [4, 5, 6]
        );
        db::close(&database).await.unwrap();
    }

    #[tokio::test]
    async fn unmarked_copy_attachment_renews_namespace_and_read_only_replica_can_collect_and_compact()
     {
        let (directory, database) = fixture().await;
        let source = directory.path().join("db");
        let destination = directory.path().join("external-copy");
        let baseline = db::Watermark::new(database.database_id(), 2).unwrap();
        let source_cursor = db::checkout_cursor(
            &database,
            baseline,
            db::CursorKind::Resolved,
            "source claim",
        )
        .await
        .unwrap();
        for transaction in [
            tx! { tables { data: u64 => u64 = 1 } data[1] = 11; }.unwrap(),
            tx! { tables { data: u64 => u64 = 1 } data[1] = 12; }.unwrap(),
        ] {
            db::execute(&database, transaction, db::Limits::default())
                .await
                .unwrap();
        }
        db::close(&database).await.unwrap();
        let original = published(&source);
        // An explicit offline filesystem copy, not the marker-producing backup
        // API.
        fs::create_dir(&destination).unwrap();
        for entry in fs::read_dir(&source).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_file() && entry.file_name() != "LOCK" {
                fs::copy(entry.path(), destination.join(entry.file_name())).unwrap();
            }
        }
        assert!(!destination.join("ATTACH_REQUIRED").exists());
        let replica = attach(&destination, AttachMode::ReadOnlyReplica)
            .await
            .unwrap();
        assert_ne!(replica.cursor_namespace(), original.cursor_namespace);
        assert!(replica.is_read_only());
        let copied_cursor = db::rebind_cursor(&replica, source_cursor.cursor_id(), baseline)
            .await
            .unwrap();
        assert!(matches!(
            db::reopen_cursor(&replica, &source_cursor).await,
            Err(Error::InvalidToken(_))
        ));
        db::release_cursor(&replica, &copied_cursor).await.unwrap();
        let before = published(&destination);
        let report = maintain(&replica, MaintenanceOptions::default())
            .await
            .unwrap();
        let after = published(&destination);
        assert_eq!(report.floors.history, 4);
        assert_eq!((report.versions_removed, report.outcomes_removed), (2, 4));
        assert_ne!(after.page_file_id, before.page_file_id);
        assert_eq!(
            (
                after.database_id,
                after.genesis_digest,
                after.cursor_namespace
            ),
            (
                before.database_id,
                before.genesis_digest,
                before.cursor_namespace
            )
        );
        assert_eq!(
            (
                after.checkpoint_sequence,
                after.checkpoint_digest,
                after.durable_sequence,
                after.durable_digest
            ),
            (
                before.checkpoint_sequence,
                before.checkpoint_digest,
                before.durable_sequence,
                before.durable_digest
            )
        );
        assert_eq!(after.next_cursor_id, before.next_cursor_id);
        assert!(report.reclaimed.files > 0);
        assert_eq!(published(&source), original);
        assert_eq!(
            fs::read(destination.join("GENESIS")).unwrap(),
            fs::read(source.join("GENESIS")).unwrap()
        );
        assert!(matches!(
            db::execute(&replica, tx! { return 42; }.unwrap(), db::Limits::default()).await,
            Err(Error::ReadOnly)
        ));
        db::close(&replica).await.unwrap();

        let replica = db::open(&destination).await.unwrap();
        assert_eq!(replica.cursor_namespace(), after.cursor_namespace);
        assert!(replica.is_read_only());
        let latest = db::snapshot(&replica).await.unwrap();
        assert_eq!(latest.sequence(), 4);
        assert_eq!(
            db::get(&latest, 1, &Value::U64(1)).unwrap(),
            Some(Value::U64(12))
        );
        assert_eq!(
            db::get(&latest, 1, &Value::U64(2)).unwrap(),
            Some(Value::U64(20))
        );
        db::close(&replica).await.unwrap();
        assert!(!destination.join("ATTACH_REQUIRED").exists());
        let reattached = attach(&destination, AttachMode::ReadOnlyReplica)
            .await
            .unwrap();
        assert_ne!(reattached.cursor_namespace(), after.cursor_namespace);
        assert_eq!(published(&destination).history_floor, 4);
        assert_eq!(
            published(&destination).next_cursor_id,
            original.next_cursor_id
        );
        assert!(reattached.is_read_only());
        db::close(&reattached).await.unwrap();
        let source = db::open(&source).await.unwrap();
        assert_eq!(source.cursor_namespace(), original.cursor_namespace);
        assert_eq!(
            db::reopen_cursor(&source, &source_cursor)
                .await
                .unwrap()
                .baseline,
            2
        );
        db::close(&source).await.unwrap();
    }

    #[tokio::test]
    async fn rotation_without_append_survives_close_and_reopen_with_a_fresh_chained_segment() {
        let (directory, database) = fixture().await;
        let path = directory.path().join("db");
        let before = published(&path);
        let original_log = path.join(format!("log-{:020}.bin", before.segments[0].segment_id));
        let original_bytes = fs::read(&original_log).unwrap();
        let report = maintain(&database, only_rotate()).await.unwrap();
        assert!(report.rotation_pending);
        let rotated = published(&path);
        assert_eq!(rotated.segments, before.segments);
        assert_eq!(rotated.next_segment_id, before.next_segment_id);
        assert_eq!(rotated.durable_sequence, 2);
        db::close(&database).await.unwrap();
        let database = db::open(&path).await.unwrap();
        assert_eq!(
            db::execute(
                &database,
                tx! { tables { data: u64 => u64 = 1 } data[1] += 1; }.unwrap(),
                db::Limits::default()
            )
            .await
            .unwrap()
            .sequence,
            3
        );
        let appended = published(&path);
        assert_eq!(appended.segments.len(), 2);
        assert_eq!(appended.segments[0], before.segments[0]);
        assert_eq!(fs::read(&original_log).unwrap(), original_bytes);
        let segment = &appended.segments[1];
        assert_eq!(segment.segment_id, before.next_segment_id);
        assert_eq!(appended.next_segment_id, segment.segment_id + 1);
        assert_eq!((segment.first_sequence, segment.last_sequence), (3, 3));
        assert_eq!(segment.predecessor_digest, before.durable_digest);
        let bytes = fs::read(path.join(format!("log-{:020}.bin", segment.segment_id))).unwrap();
        assert_eq!(
            &bytes[..96],
            &engine::segment_header(appended.database_id, segment)
        );
        let (_, digest) = engine::read_record(
            &mut &bytes[96..],
            segment.committed_bytes - 96,
            3,
            before.durable_digest,
        )
        .unwrap();
        assert_eq!(digest, segment.last_digest);
        assert_eq!(digest, appended.durable_digest);
        db::close(&database).await.unwrap();
        let database = db::open(&path).await.unwrap();
        let snapshot = db::snapshot(&database).await.unwrap();
        assert_eq!(snapshot.sequence(), 3);
        assert_eq!(
            db::get(&snapshot, 1, &Value::U64(1)).unwrap(),
            Some(Value::U64(11))
        );
        db::close(&database).await.unwrap();
    }

    #[tokio::test]
    async fn claims_protect_exact_feeds_until_ack_and_release_and_metadata_is_never_pruned() {
        let (directory, database) = fixture().await;
        let path = directory.path().join("db");
        let (old, resolved) = db::snapshot_and_cursor(&database, db::CursorKind::Resolved, "pin")
            .await
            .unwrap();
        let baseline = old.watermark();
        let logical = db::checkout_cursor(&database, baseline, db::CursorKind::Logical, "pin")
            .await
            .unwrap();
        maintain(&database, only_rotate()).await.unwrap();
        db::execute(
            &database,
            tx! { tables { data: u64 => u64 = 1 } data[1] = 11; }.unwrap(),
            db::Limits::default(),
        )
        .await
        .unwrap();
        db::execute(
            &database,
            tx! { tables { data: u64 => u64 = 1 } delete(data[1]); }.unwrap(),
            db::Limits::default(),
        )
        .await
        .unwrap();
        db::execute_catalogue(
            &database,
            CatalogueOperation::Rename {
                table: 1,
                name: "renamed".into(),
            },
        )
        .await
        .unwrap();
        db::execute_limits(&database, db::Limits::default())
            .await
            .unwrap();
        db::execute(
            &database,
            tx! { require(false); }.unwrap(),
            db::Limits::default(),
        )
        .await
        .unwrap();
        let before = db::read_feed(&database, &resolved, baseline, db::BatchLimits::default())
            .await
            .unwrap();
        let original_log = fs::read(path.join("log-00000000000000000002.bin")).unwrap();
        let report = maintain(&database, MaintenanceOptions::default())
            .await
            .unwrap();
        assert_eq!(report.frontier, 7);
        assert_eq!(report.floors, RetentionFloors { history: 2, log: 3 });
        assert_eq!(report.segments_retired, 1);
        assert_eq!(report.reclaimed.files, 0);
        assert!(report.reclaimed.deferred_files > 0);
        assert_eq!(
            fs::read(path.join("log-00000000000000000002.bin")).unwrap(),
            original_log
        );
        assert_eq!(
            db::get(&old, 1, &Value::U64(1)).unwrap(),
            Some(Value::U64(10))
        );
        assert_eq!(
            db::read_feed(&database, &resolved, baseline, db::BatchLimits::default())
                .await
                .unwrap(),
            before
        );
        let latest = db::Watermark::new(database.database_id(), 7).unwrap();
        db::acknowledge_cursor(&database, &logical, latest)
            .await
            .unwrap();
        db::release_cursor(&database, &logical).await.unwrap();
        db::revoke(&old);
        let report = maintain(&database, MaintenanceOptions::default())
            .await
            .unwrap();
        assert_eq!(report.floors, RetentionFloors { history: 2, log: 8 });
        assert!(report.reclaimed.files > 0);
        assert!(!path.join("log-00000000000000000001.bin").exists());
        assert!(!path.join("log-00000000000000000002.bin").exists());
        assert_eq!(
            db::read_feed(&database, &resolved, baseline, db::BatchLimits::default())
                .await
                .unwrap(),
            before
        );
        db::acknowledge_cursor(&database, &resolved, latest)
            .await
            .unwrap();
        let report = maintain(&database, MaintenanceOptions::default())
            .await
            .unwrap();
        assert_eq!(report.floors.history, 7);
        assert!(report.versions_removed >= 2);
        assert!(matches!(
            db::checkout_cursor(&database, baseline, db::CursorKind::Resolved, "too old").await,
            Err(Error::HistoryUnavailable)
        ));
        db::release_cursor(&database, &resolved).await.unwrap();
        db::execute(
            &database,
            tx! { tables { data: u64 => u64 = 1 } data[2] += 1; }.unwrap(),
            db::Limits::default(),
        )
        .await
        .unwrap();
        let report = maintain(&database, MaintenanceOptions::default())
            .await
            .unwrap();
        assert_eq!(report.floors.history, 8);
        db::close(&database).await.unwrap();
        let store = storage::open(&path).unwrap();
        assert_eq!(entries(&store, TreeId::Catalogue).len(), 2);
        assert_eq!(entries(&store, TreeId::Policy).len(), 2);
        assert!(entries(&store, TreeId::Outcomes).is_empty());
        let state = entries(&store, TreeId::State);
        assert_eq!(state.len(), 2);
        assert_eq!(StateKey::decode(&state[0].0).unwrap().sequence(), 4);
        assert_eq!(state[0].1, vec![0]);
        drop(store);
        let database = db::open(path).await.unwrap();
        let latest = db::snapshot(&database).await.unwrap();
        assert_eq!(db::get(&latest, 1, &Value::U64(1)).unwrap(), None);
        assert_eq!(
            db::get(&latest, 1, &Value::U64(2)).unwrap(),
            Some(Value::U64(21))
        );
        db::close(&database).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn queued_compaction_drains_future_installs_adopts_roots_and_resumes_assignment() {
        let (directory, database) = fixture().await;
        let old = db::snapshot(&database).await.unwrap();
        let gate = Arc::new(Gate::default());
        database
            .hooks
            .actions
            .lock()
            .unwrap()
            .insert(3, Action::Gate(gate.clone()));
        let first = enqueue(
            &database,
            tx! { tables { data: u64 => u64 = 1 } data[1] += 1; }.unwrap(),
        )
        .await;
        let future = enqueue(
            &database,
            tx! { tables { data: u64 => u64 = 1 } data[2] = 99; }.unwrap(),
        )
        .await;
        wait(&database, |s| {
            s.durable_frontier == 4 && s.resolved_above_frontier == 1
        })
        .await;
        let (reply, mut result) = oneshot::channel();
        database
            .control
            .send(Control::Maintenance {
                options: MaintenanceOptions::default(),
                reply,
            })
            .await
            .unwrap();
        wait(&database, |s| s.maintenance_pending).await;
        let after = enqueue(
            &database,
            tx! { tables { data: u64 => u64 = 1 } data[2] += 1; return data[2]; }.unwrap(),
        )
        .await;
        assert!(matches!(
            result.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert_eq!(db::status(&database).durable_frontier, 4);
        gate.release();
        assert_eq!(first.await.unwrap().unwrap().sequence, 3);
        assert_eq!(future.await.unwrap().unwrap().sequence, 4);
        let report = result.await.unwrap().unwrap();
        assert_eq!(report.frontier, 4);
        assert_ne!(report.page_file, report.previous_page_file);
        assert!(
            directory
                .path()
                .join("db/pages-00000000000000000001.bin")
                .exists()
        );
        let receipt = after.await.unwrap().unwrap();
        assert_eq!(receipt.sequence, 5);
        assert!(matches!(
            receipt.outcome,
            vm::Outcome::Success {
                value: Value::U64(100),
                ..
            }
        ));
        assert_eq!(
            db::get(&old, 1, &Value::U64(2)).unwrap(),
            Some(Value::U64(20))
        );
        db::revoke(&old);
        maintain(
            &database,
            MaintenanceOptions {
                collect_history: false,
                compact: false,
                rotate_log: false,
            },
        )
        .await
        .unwrap();
        assert!(
            !directory
                .path()
                .join("db/pages-00000000000000000001.bin")
                .exists()
        );
        db::close(&database).await.unwrap();
        let database = db::open(directory.path().join("db")).await.unwrap();
        let snapshot = db::snapshot(&database).await.unwrap();
        assert_eq!(
            db::get(&snapshot, 1, &Value::U64(1)).unwrap(),
            Some(Value::U64(11))
        );
        assert_eq!(
            db::get(&snapshot, 1, &Value::U64(2)).unwrap(),
            Some(Value::U64(100))
        );
        db::close(&database).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_backup_copies_exact_earlier_checkpoint_and_suffix_then_rebinds_fresh_namespace()
     {
        let (directory, database) = fixture().await;
        let source = directory.path().join("db");
        let destination = directory.path().join("backup");
        let baseline = db::Watermark::new(database.database_id(), 2).unwrap();
        let cursor =
            db::checkout_cursor(&database, baseline, db::CursorKind::Resolved, "same label")
                .await
                .unwrap();
        let worker = Arc::new(Gate::default());
        let copier = Arc::new(Gate::default());
        database.hooks.actions.lock().unwrap().extend([
            (3, Action::Gate(worker.clone())),
            (0, Action::Gate(copier.clone())),
        ]);
        let first = enqueue(
            &database,
            tx! { tables { data: u64 => u64 = 1 } data[1] += 1; }.unwrap(),
        )
        .await;
        let later = enqueue(
            &database,
            tx! { tables { data: u64 => u64 = 1 } data[2] = 77; }.unwrap(),
        )
        .await;
        wait(&database, |s| {
            s.durable_frontier == 4 && s.resolved_above_frontier == 1
        })
        .await;
        let current = storage::Current::decode(&fs::read(source.join("CURRENT")).unwrap()).unwrap();
        let original = storage::Manifest::decode(
            &fs::read(source.join(format!("manifest-{:020}.bin", current.generation))).unwrap(),
        )
        .unwrap();
        let original_pages =
            fs::read(source.join(format!("pages-{:020}.bin", original.page_file_id))).unwrap();
        let original_log = fs::read(source.join("log-00000000000000000001.bin")).unwrap();
        let (reply, copied) = oneshot::channel();
        database
            .control
            .send(Control::Backup {
                destination: destination.clone(),
                reply,
            })
            .await
            .unwrap();
        wait(&database, |s| s.backup_jobs == 1).await;
        worker.release();
        first.await.unwrap().unwrap();
        later.await.unwrap().unwrap();
        let source_later =
            db::checkout_cursor(&database, baseline, db::CursorKind::Resolved, "same label")
                .await
                .unwrap();
        db::execute(
            &database,
            tx! { tables { data: u64 => u64 = 1 } data[2] += 1; }.unwrap(),
            db::Limits::default(),
        )
        .await
        .unwrap();
        let report = maintain(&database, MaintenanceOptions::default())
            .await
            .unwrap();
        assert_eq!(report.frontier, 5);
        assert_eq!(report.reclaimed.files, 0);
        assert!(source.join("pages-00000000000000000001.bin").exists());
        copier.release();
        let copied = copied.await.unwrap().unwrap();
        assert_eq!(copied, original);
        assert_eq!(
            (copied.checkpoint_sequence, copied.durable_sequence),
            (2, 4)
        );
        assert_eq!(
            fs::read(destination.join("CURRENT")).unwrap(),
            current.encode().unwrap()
        );
        assert_eq!(
            fs::read(destination.join("GENESIS")).unwrap(),
            fs::read(source.join("GENESIS")).unwrap()
        );
        assert_eq!(
            fs::read(destination.join("pages-00000000000000000001.bin")).unwrap(),
            original_pages[..copied.page_count as usize * 16_384]
        );
        assert_eq!(
            fs::read(destination.join("log-00000000000000000001.bin")).unwrap(),
            original_log[..copied.segments[0].committed_bytes as usize]
        );
        assert!(matches!(
            db::open(&destination).await,
            Err(Error::Storage(storage::Error::AttachRequired))
        ));
        // Read-only attachment can coexist with the source primary.
        let replica = attach(&destination, AttachMode::ReadOnlyReplica)
            .await
            .unwrap();
        assert!(replica.is_read_only());
        assert_eq!(replica.database_id(), database.database_id());
        assert_ne!(replica.cursor_namespace(), database.cursor_namespace());
        assert!(matches!(
            db::execute(&replica, tx! { return 1; }.unwrap(), db::Limits::default()).await,
            Err(Error::ReadOnly)
        ));
        assert!(matches!(
            db::execute_catalogue(&replica, CatalogueOperation::Drop { table: 1 }).await,
            Err(Error::ReadOnly)
        ));
        assert!(matches!(
            db::execute_limits(&replica, db::Limits::default()).await,
            Err(Error::ReadOnly)
        ));
        assert!(matches!(
            db::reopen_cursor(&replica, &cursor).await,
            Err(Error::InvalidToken(_))
        ));
        let rebound = db::rebind_cursor(&replica, cursor.cursor_id(), baseline)
            .await
            .unwrap();
        assert_eq!(
            db::reopen_cursor(&replica, &rebound)
                .await
                .unwrap()
                .baseline,
            2
        );
        let reused =
            db::checkout_cursor(&replica, baseline, db::CursorKind::Resolved, "same label")
                .await
                .unwrap();
        assert_eq!(reused.cursor_id(), source_later.cursor_id());
        assert!(matches!(
            db::acknowledge_cursor(&replica, &source_later, baseline).await,
            Err(Error::InvalidToken(_))
        ));
        assert!(matches!(
            db::release_cursor(&replica, &source_later).await,
            Err(Error::InvalidToken(_))
        ));
        for watermark in [
            db::Watermark::new([9; 16], 2).unwrap(),
            db::Watermark::new(replica.database_id(), 1).unwrap(),
            db::Watermark::new(replica.database_id(), 5).unwrap(),
        ] {
            assert!(
                db::rebind_cursor(&replica, cursor.cursor_id(), watermark)
                    .await
                    .is_err()
            );
        }
        let snapshot = db::snapshot(&replica).await.unwrap();
        assert_eq!(snapshot.sequence(), 4);
        assert_eq!(
            db::get(&snapshot, 1, &Value::U64(1)).unwrap(),
            Some(Value::U64(11))
        );
        assert_eq!(
            db::get(&snapshot, 1, &Value::U64(2)).unwrap(),
            Some(Value::U64(77))
        );
        let namespace = replica.cursor_namespace();
        db::close(&replica).await.unwrap();
        let replica = db::open(&destination).await.unwrap();
        assert!(replica.is_read_only());
        assert_eq!(replica.cursor_namespace(), namespace);
        db::reopen_cursor(&replica, &rebound).await.unwrap();
        db::close(&replica).await.unwrap();
        maintain(&database, MaintenanceOptions::default())
            .await
            .unwrap();
        assert!(!source.join("pages-00000000000000000001.bin").exists());
        db::close(&database).await.unwrap();
        // The caller has now retired the source and explicitly chooses
        // promotion.
        let restored = attach(&destination, AttachMode::RestorePrimarySourceRetired)
            .await
            .unwrap();
        assert!(!restored.is_read_only());
        assert_ne!(restored.cursor_namespace(), namespace);
        assert!(matches!(
            db::release_cursor(&restored, &rebound).await,
            Err(Error::InvalidToken(_))
        ));
        assert_eq!(db::list_cursors(&restored).await.unwrap().len(), 2);
        assert_eq!(
            db::execute(
                &restored,
                tx! { return 42; }.unwrap(),
                db::Limits::default()
            )
            .await
            .unwrap()
            .sequence,
            5
        );
        db::close(&restored).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_backup_waiter_cannot_release_pins_and_close_joins_copy() {
        let (directory, database) = fixture().await;
        let gate = Arc::new(Gate::default());
        database
            .hooks
            .actions
            .lock()
            .unwrap()
            .insert(0, Action::Gate(gate.clone()));
        let destination = directory.path().join("backup");
        let mut future = Box::pin(backup(&database, &destination));
        assert!(matches!(
            future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
        wait(&database, |s| s.backup_jobs == 1).await;
        drop(future);
        let mut closing = pin!(db::close(&database));
        assert!(matches!(
            closing
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
        wait(&database, |s| s.closed).await;
        assert!(matches!(
            storage::open(directory.path().join("db")),
            Err(storage::Error::Locked)
        ));
        assert!(matches!(
            closing
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
        gate.release();
        closing.await.unwrap();
        drop(storage::open(directory.path().join("db")).unwrap());
        assert!(destination.join("CURRENT").exists());
        let restored = attach(&destination, AttachMode::RestorePrimarySourceRetired)
            .await
            .unwrap();
        assert_eq!(db::snapshot(&restored).await.unwrap().sequence(), 2);
        assert!(backup(&restored, &destination).await.is_err());
        // Destination errors do not poison the source coordinator.
        assert_eq!(db::snapshot(&restored).await.unwrap().sequence(), 2);
        db::close(&restored).await.unwrap();
    }

    #[test]
    fn maintenance_builds_checkpoint_when_c_is_below_the_drained_frontier() {
        let directory = tempfile::tempdir().unwrap();
        let mut store = storage::create(
            directory.path().join("db"),
            storage::Genesis {
                database_id: [1; 16],
                initial_policy: db::Limits::default().try_into().unwrap(),
            },
            [2; 16],
        )
        .unwrap();
        let command = record::Command::Catalogue(CatalogueOperation::Create {
            name: "data".into(),
            key: Type::U64,
            value: Type::U64,
        });
        let (kind, body) = record::encode(&command).unwrap();
        let bytes = engine::envelope(1, store.manifest().durable_digest, kind, &body).unwrap();
        let digest = engine::append(&mut store, 1, &bytes).unwrap();
        record::execute(&mut store, 1, digest, &command).unwrap();
        assert_eq!(store.manifest().checkpoint_sequence, 0);
        let report = run(
            &mut store,
            &mut snapshot::Registry::default(),
            1,
            MaintenanceOptions::default(),
        )
        .unwrap();
        assert_eq!(report.frontier, 1);
        assert_eq!(store.manifest().checkpoint_sequence, 1);
        assert_eq!(entries(&store, TreeId::Catalogue).len(), 1);
        let path = store.directory().to_owned();
        drop(store);
        engine::recover(&mut storage::open(path).unwrap()).unwrap();
    }
}
