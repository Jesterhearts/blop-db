//! Async, durable transaction submission through a serial background writer.
//!
//! [`create`] and [`open`] return a cloneable handle. Submission I/O,
//! validation, execution and recovery run on its dedicated thread, not the
//! caller's executor. Snapshot reads use synchronous caller-thread I/O.
//! The futures use Tokio channels but need no Tokio
//! runtime. Up to 64 requests can wait in the writer queue; further submissions
//! await capacity. Each receipt follows log durability, execution and
//! checkpoint publication, including when its outcome is a semantic abort.
//!
//! Dropping a submission future after enqueueing does not cancel the request.
//! An interrupted call may have committed. Retrying is a new transaction; put
//! any required request-ID deduplication inside the transaction itself.
//! [`close`] drains accepted requests and releases the directory lock for all
//! handle clones. Dropping every handle also drains the queue, but does not
//! wait for the writer to finish. Snapshots are revocable; close revokes all
//! claims and drains in-flight reads without waiting for idle handles or scans.
//! Durable cursors survive close and are released only by explicit request.

pub(crate) mod cursor;
mod engine;
mod exchange;
mod feed;
mod record;
pub(crate) mod snapshot;

use std::fmt;
use std::path::Path;
use std::path::PathBuf;
use std::thread;

pub use cursor::CursorInfo;
pub use cursor::RetentionFloors;
pub use cursor::acknowledge_cursor;
pub use cursor::checkout_cursor;
pub use cursor::list_cursors;
pub use cursor::release_cursor;
pub use cursor::reopen_cursor;
pub use cursor::retention_status;
pub use cursor::snapshot_and_cursor;
pub use exchange::CursorKind;
pub use exchange::CursorToken;
pub use exchange::EMPTY_BATCH_BYTES;
pub use exchange::FeedBatch;
pub use exchange::FeedRecords;
pub use exchange::LogicalRecord;
pub use exchange::MAX_BATCH_BYTES;
pub use exchange::Watermark;
pub use feed::read_feed;
pub use snapshot::Snapshot;
pub use snapshot::SnapshotScan;
pub use snapshot::catalogue;
pub use snapshot::get;
pub use snapshot::revoke;
pub use snapshot::scan;
pub use snapshot::snapshot;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;

use self::record::Command;
pub use crate::Limits;
use crate::Transaction;
use crate::storage;
use crate::storage::Genesis;
use crate::storage::LimitPolicy;
use crate::vm;
use crate::vm::CatalogueOperation;
use crate::vm::Outcome;

/// A cloneable submission handle to one exclusively owned database directory.
#[derive(Clone, Debug)]
pub struct Database {
    sender: mpsc::Sender<Request>,
    stopped: watch::Receiver<()>,
    database_id: [u8; 16],
    cursor_namespace: [u8; 16],
}

impl Database {
    /// The persistent database identity, unchanged by reopening.
    pub fn database_id(&self) -> [u8; 16] {
        self.database_id
    }

    /// The persistent namespace used for cursor identities.
    pub fn cursor_namespace(&self) -> [u8; 16] {
        self.cursor_namespace
    }
}

/// Database initialization settings. Omitted identities are independently
/// generated UUIDs (version 4). Explicit identities must be unique and nonzero.
/// Defaults use the finite format ceilings; applications can lower individual
/// named limits. All resolved settings are persisted, never regenerated on
/// open.
#[derive(Clone, Debug, Default)]
pub struct CreateOptions {
    pub limits: Limits,
    pub database_id: Option<[u8; 16]>,
    pub cursor_namespace: Option<[u8; 16]>,
}

/// A durable, checkpointed result. An aborted outcome contains no data writes
/// but still occupies this sequence and is durably recorded.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Receipt {
    pub sequence: u64,
    pub outcome: Outcome,
}

/// Limits include the complete H.1 header and CRC. Records are never split.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BatchLimits {
    pub max_records: usize,
    pub max_bytes: usize,
}

impl Default for BatchLimits {
    fn default() -> Self {
        Self {
            max_records: 1024,
            max_bytes: MAX_BATCH_BYTES,
        }
    }
}

/// Submission and system errors, separate from deterministic VM aborts.
#[derive(Debug)]
pub enum Error {
    SnapshotRevoked,
    HistoryUnavailable,
    CursorReleased,
    CursorIdExhausted,
    InvalidToken(&'static str),
    InvalidFormat(&'static str),
    InvalidInput(&'static str),
    BatchTooSmall {
        required: usize,
    },
    /// Read or exchange failure, never a semantic transaction abort.
    Read(vm::Error),
    /// Validation rejected the request before sequencing. Nothing was written.
    Rejected(vm::Error),
    /// A system failure during startup or before this request was appended.
    Storage(storage::Error),
    /// The request may be durable, even if its receipt was lost. The writer
    /// stops accepting work; close its handles and reopen before proceeding.
    Uncertain {
        sequence: Option<u64>,
        source: Option<vm::Error>,
    },
    /// The writer is closed. This request was not executed.
    Closed,
}

impl fmt::Display for Error {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match self {
            Self::SnapshotRevoked => f.write_str("snapshot has been revoked"),
            Self::HistoryUnavailable => f.write_str("required history is unavailable"),
            Self::CursorReleased => f.write_str("cursor has been released"),
            Self::CursorIdExhausted => f.write_str("cursor ID space is exhausted"),
            Self::InvalidToken(reason) => write!(f, "invalid cursor token or watermark: {reason}"),
            Self::InvalidFormat(reason) => write!(f, "invalid exchange format: {reason}"),
            Self::InvalidInput(reason) => write!(f, "invalid database input: {reason}"),
            Self::BatchTooSmall { required } => write!(
                f,
                "batch requires at least {required} bytes for its first complete record"
            ),
            Self::Read(error) => error.fmt(f),
            Self::Rejected(error) => write!(f, "transaction rejected: {error}"),
            Self::Storage(error) => error.fmt(f),
            Self::Uncertain { sequence, source } => {
                f.write_str("submission outcome is uncertain")?;
                if let Some(sequence) = sequence {
                    write!(f, " at sequence {sequence}")?;
                }
                if let Some(source) = source {
                    write!(f, ": {source}")?;
                }
                Ok(())
            }
            Self::Closed => f.write_str("database writer is closed"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Rejected(error) | Self::Read(error) => Some(error),
            Self::Storage(error) => Some(error),
            Self::Uncertain {
                source: Some(error),
                ..
            } => Some(error),
            _ => None,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

fn persisted_read(error: vm::Error) -> Error {
    match error {
        vm::Error::Invalid(reason) | vm::Error::Storage(storage::Error::InvalidInput(reason)) => {
            Error::Storage(storage::Error::Corrupt(reason))
        }
        other => Error::Read(other),
    }
}

enum Request {
    Snapshot {
        reply: oneshot::Sender<Result<Snapshot>>,
    },
    Retention {
        operation: cursor::Operation,
        reply: oneshot::Sender<Result<cursor::Response>>,
    },
    Execute {
        command: Box<Command>,
        reply: oneshot::Sender<Result<Receipt>>,
    },
    Close,
}

/// Create a database in a new directory. The parent directory must exist.
/// `CreateOptions::default()` generates UUIDs and uses default named limits.
/// A failed or cancelled creation may leave a directory; it is never silently
/// replaced or treated as a new empty database.
pub async fn create(
    path: impl AsRef<Path>,
    options: CreateOptions,
) -> Result<Database> {
    start(path.as_ref().to_owned(), Some(options)).await
}

/// Open an existing database and replay its durable post-checkpoint records
/// before accepting submissions. Corruption is an error, not a reason to fall
/// back to an older checkpoint. Recovery validates full C.4 access manifests
/// against the historical catalogue and policy, including older broad
/// manifests.
pub async fn open(path: impl AsRef<Path>) -> Result<Database> {
    start(path.as_ref().to_owned(), None).await
}

async fn start(
    path: PathBuf,
    options: Option<CreateOptions>,
) -> Result<Database> {
    let (sender, receiver) = mpsc::channel(64);
    let (ready, initialized) = oneshot::channel();
    let (stopped, completion) = watch::channel(());
    thread::Builder::new()
        .name("blop-db-writer".into())
        .spawn(move || {
            // Closing this channel signals that all storage handles have gone,
            // including when initialization or the writer fails.
            let _stopped = stopped;
            let store = match options {
                Some(options) => initialize(path, options),
                None => storage::open(path).and_then(|mut store| {
                    engine::recover(&mut store)?;
                    Ok(store)
                }),
            };
            match store {
                Ok(store) => {
                    let identities = (
                        store.genesis().database_id,
                        store.manifest().cursor_namespace,
                    );
                    if ready.send(Ok(identities)).is_ok() {
                        run_writer(store, receiver);
                    }
                }
                Err(error) => {
                    let _ = ready.send(Err(Error::Storage(error)));
                }
            }
        })
        .map_err(|error| Error::Storage(storage::Error::Io(error)))?;
    let (database_id, cursor_namespace) = initialized.await.map_err(|_| Error::Closed)??;
    Ok(Database {
        sender,
        stopped: completion,
        database_id,
        cursor_namespace,
    })
}

fn initialize(
    path: PathBuf,
    options: CreateOptions,
) -> storage::Result<storage::Store> {
    let initial_policy = options.limits.try_into()?;
    let database_id = options.database_id.map_or_else(random_uuid, Ok)?;
    let cursor_namespace = options.cursor_namespace.map_or_else(random_uuid, Ok)?;
    storage::create(
        path,
        Genesis {
            database_id,
            initial_policy,
        },
        cursor_namespace,
    )
}

fn random_uuid() -> storage::Result<[u8; 16]> {
    let mut bytes = [0; 16];
    getrandom::fill(&mut bytes)
        .map_err(|error| storage::Error::Io(std::io::Error::other(error)))?;
    // RFC 9562 version 4 and variant bits; the other 122 bits remain random.
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Ok(bytes)
}

/// Submit a bound transaction and wait for its durable, visible outcome.
/// Claims are checked against the policy at its assigned sequence. The writer
/// derives normalized C.4 scopes; resource 7 charges their actual entry count.
/// Concurrent callers are serialized in queue order.
///
/// Cancellation before enqueueing submits nothing. After enqueueing, the
/// transaction proceeds even if this future is dropped. An `Uncertain` error
/// is not a definite rejection and must not be retried blindly.
pub async fn execute(
    database: &Database,
    transaction: Transaction,
    claims: Limits,
) -> Result<Receipt> {
    let claims = validate_limits(claims)?;
    submit(
        database,
        Command::Transaction {
            transaction,
            claims,
            manifest: None,
        },
    )
    .await
}

/// Submit explicit normalized C.4 declarations. The writer independently proves
/// coverage under the historical schemas and policy before sequencing. Broader
/// declarations are allowed and retained exactly, including their resource-7
/// count. `AccessManifest::decode` accepts canonical wire manifests; `new`
/// normalizes application-constructed scopes before submission.
pub async fn execute_with_manifest(
    database: &Database,
    transaction: Transaction,
    claims: Limits,
    manifest: vm::AccessManifest,
) -> Result<Receipt> {
    submit(
        database,
        Command::Transaction {
            transaction,
            claims: validate_limits(claims)?,
            manifest: Some(manifest),
        },
    )
    .await
}

/// Create, rename or drop a table in the same durable sequence space as writes.
/// A successful create returns the new table ID as `Value::U64` in the outcome.
/// Name conflicts and missing tables are semantic aborts, not submission
/// errors.
pub async fn execute_catalogue(
    database: &Database,
    operation: CatalogueOperation,
) -> Result<Receipt> {
    submit(database, Command::Catalogue(operation)).await
}

/// Durably replace the policy for later transactions. This remains available
/// when the current policy prevents every transaction submission.
pub async fn execute_limits(
    database: &Database,
    limits: Limits,
) -> Result<Receipt> {
    submit(database, Command::Limits(validate_limits(limits)?)).await
}

fn validate_limits(limits: Limits) -> Result<LimitPolicy> {
    limits
        .try_into()
        .map_err(|_| Error::Rejected(vm::Error::Invalid("resource limit exceeds format ceiling")))
}

async fn submit(
    database: &Database,
    command: Command,
) -> Result<Receipt> {
    let (reply, result) = oneshot::channel();
    database
        .sender
        .send(Request::Execute {
            command: Box::new(command),
            reply,
        })
        .await
        .map_err(|_| Error::Closed)?;
    result.await.map_err(|_| Error::Uncertain {
        sequence: None,
        source: None,
    })?
}

/// Stop accepting new submissions from all clones, drain already accepted
/// requests, and wait until the directory lock has been released. Receipts for
/// individual transactions still report their own results. A repeated close
/// after shutdown returns `Error::Closed`. Even that result waits for the
/// writer to release its storage handles, so the directory can be reopened.
/// All snapshots and their iterators are revoked before this returns. In-flight
/// reads finish at their original view; idle handles do not delay close. Cursor
/// registrations remain durable and can be reopened using their saved tokens.
pub async fn close(database: &Database) -> Result<()> {
    let result = database
        .sender
        .send(Request::Close)
        .await
        .map_err(|_| Error::Closed);
    let mut stopped = database.stopped.clone();
    let _ = stopped.changed().await;
    result
}

fn run_writer(
    mut store: storage::Store,
    mut receiver: mpsc::Receiver<Request>,
) {
    let mut failed = false;
    let mut registry = snapshot::Registry::default();
    while let Some(request) = receiver.blocking_recv() {
        match request {
            Request::Snapshot { reply } => {
                let result = if failed {
                    Err(Error::Closed)
                } else {
                    Ok(snapshot::capture(&mut registry, &store))
                };
                let _ = reply.send(result);
            }
            Request::Retention { operation, reply } => {
                let result = if failed {
                    Err(Error::Closed)
                } else {
                    cursor::handle(&mut store, &mut registry, operation)
                };
                if result.as_ref().is_err_and(|error| {
                    matches!(
                        error,
                        Error::Storage(_) | Error::Uncertain { .. } | Error::Read(_)
                    )
                }) {
                    failed = true;
                    receiver.close();
                }
                let _ = reply.send(result);
            }
            Request::Execute { command, reply } => {
                let result = if failed {
                    Err(Error::Closed)
                } else {
                    engine::commit(&mut store, &command)
                };
                if result
                    .as_ref()
                    .is_err_and(|error| !matches!(error, Error::Rejected(_)))
                {
                    failed = true;
                    receiver.close();
                }
                let _ = reply.send(result);
            }
            Request::Close => {
                receiver.close();
            }
        }
    }
    snapshot::revoke_all(&mut registry);
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::pin;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;

    use super::*;
    use crate::tx;
    use crate::vm::AbortReason;
    use crate::vm::Type;
    use crate::vm::Value;

    fn policy() -> Limits {
        Limits::default()
    }

    async fn fixture() -> (tempfile::TempDir, Database) {
        let directory = tempfile::tempdir().unwrap();
        let database = create(
            directory.path().join("db"),
            CreateOptions {
                limits: policy(),
                ..CreateOptions::default()
            },
        )
        .await
        .unwrap();
        (directory, database)
    }

    async fn table(database: &Database) -> u64 {
        let receipt = execute_catalogue(
            database,
            CatalogueOperation::Create {
                name: "balances".into(),
                key: Type::U64,
                value: Type::I64,
            },
        )
        .await
        .unwrap();
        match receipt.outcome {
            Outcome::Success {
                value: Value::U64(id),
                ..
            } => id,
            outcome => panic!("unexpected table outcome: {outcome:?}"),
        }
    }

    fn value(receipt: Receipt) -> Value {
        match receipt.outcome {
            Outcome::Success { value, .. } => value,
            outcome => panic!("unexpected outcome: {outcome:?}"),
        }
    }

    #[tokio::test]
    async fn writes_and_aborts_are_atomic_and_survive_reopen() {
        let (directory, database) = fixture().await;
        let id = table(&database).await;
        let receipt = execute(
            &database,
            tx! {
                tables { balances: u64 => i64 = id }
                insert(balances[10], 100);
                insert(balances[20], 50);
            }
            .unwrap(),
            policy(),
        )
        .await
        .unwrap();
        assert_eq!(receipt.sequence, 2);
        let receipt = execute(
            &database,
            tx! {
                tables { balances: u64 => i64 = id }
                balances[10] -= 25;
                balances[20] += 25;
                return (balances[10], balances[20]);
            }
            .unwrap(),
            policy(),
        )
        .await
        .unwrap();
        assert_eq!(
            value(receipt),
            Value::Tuple(vec![Value::I64(75), Value::I64(75)])
        );
        let receipt = execute(
            &database,
            tx! {
                tables { balances: u64 => i64 = id }
                balances[10] = 999;
                require(false, 7);
            }
            .unwrap(),
            policy(),
        )
        .await
        .unwrap();
        assert_eq!(receipt.sequence, 4);
        assert!(matches!(
            receipt.outcome,
            Outcome::Aborted(vm::Abort {
                reason: AbortReason::RequireFailed,
                user_code: 7,
                ..
            })
        ));
        close(&database).await.unwrap();
        let store = storage::open(directory.path().join("db")).unwrap();
        assert_eq!(store.manifest().checkpoint_sequence, 4);
        assert_eq!(store.manifest().durable_sequence, 4);
        drop(store);
        let database = open(directory.path().join("db")).await.unwrap();
        let receipt = execute(
            &database,
            tx! {
                tables { balances: u64 => i64 = id }
                return (balances[10], balances[20]);
            }
            .unwrap(),
            policy(),
        )
        .await
        .unwrap();
        assert_eq!(receipt.sequence, 5);
        assert_eq!(
            value(receipt),
            Value::Tuple(vec![Value::I64(75), Value::I64(75)])
        );
        close(&database).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_handles_do_not_lose_updates() {
        let (_directory, database) = fixture().await;
        let id = table(&database).await;
        execute(
            &database,
            tx! {
                tables { balances: u64 => i64 = id }
                balances[1] = 0;
            }
            .unwrap(),
            policy(),
        )
        .await
        .unwrap();
        let mut tasks = Vec::new();
        for _ in 0..80 {
            let database = database.clone();
            tasks.push(tokio::spawn(async move {
                execute(
                    &database,
                    tx! {
                        tables { balances: u64 => i64 = id }
                        balances[1] += 1;
                        return balances[1];
                    }
                    .unwrap(),
                    policy(),
                )
                .await
                .unwrap()
            }));
        }
        let mut receipts = Vec::new();
        for task in tasks {
            receipts.push(task.await.unwrap());
        }
        receipts.sort_by_key(|receipt| receipt.sequence);
        for (index, receipt) in receipts.into_iter().enumerate() {
            assert_eq!(receipt.sequence, index as u64 + 3);
            assert_eq!(value(receipt), Value::I64(index as i64 + 1));
        }
        close(&database).await.unwrap();
    }

    #[tokio::test]
    async fn rejection_does_not_consume_sequence_and_limits_can_be_restored() {
        let (_directory, database) = fixture().await;
        assert!(matches!(
            execute_catalogue(
                &database,
                CatalogueOperation::Create {
                    name: "".into(),
                    key: Type::U64,
                    value: Type::I64,
                }
            )
            .await,
            Err(Error::Rejected(_))
        ));
        assert_eq!(table(&database).await, 1);
        let duplicate = execute_catalogue(
            &database,
            CatalogueOperation::Create {
                name: "balances".into(),
                key: Type::U64,
                value: Type::I64,
            },
        )
        .await
        .unwrap();
        assert_eq!(duplicate.sequence, 2);
        assert!(matches!(
            duplicate.outcome,
            Outcome::Aborted(vm::Abort {
                reason: AbortReason::NameInUse,
                ..
            })
        ));
        execute_limits(
            &database,
            Limits {
                program_bytes: 0,
                ..policy()
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            execute(&database, tx! { return 42; }.unwrap(), policy()).await,
            Err(Error::Rejected(_))
        ));
        assert_eq!(
            execute_limits(&database, policy()).await.unwrap().sequence,
            4
        );
        assert_eq!(
            execute(&database, tx! { return 42; }.unwrap(), policy())
                .await
                .unwrap()
                .sequence,
            5
        );
        close(&database).await.unwrap();
    }

    #[tokio::test]
    async fn supplied_manifests_are_verified_before_sequencing_and_survive_open() {
        let (directory, database) = fixture().await;
        let id = table(&database).await;
        let transaction =
            tx! { tables { balances: u64 => i64 = id } insert(balances[7], 42); }.unwrap();
        let scope = vm::Scope::Key(id, 7_u64.to_be_bytes().to_vec());
        for manifest in [
            vm::AccessManifest::default(),
            vm::AccessManifest::new([(scope.clone(), vm::AccessMode::Write)]).unwrap(),
        ] {
            assert!(matches!(
                execute_with_manifest(&database, transaction.clone(), policy(), manifest).await,
                Err(Error::Rejected(_))
            ));
        }
        let manifest = vm::AccessManifest::new([(scope, vm::AccessMode::ReadWrite)]).unwrap();
        let claims = Limits {
            manifest_scopes: 1,
            ..policy()
        };
        assert_eq!(
            execute_with_manifest(&database, transaction, claims, manifest)
                .await
                .unwrap()
                .sequence,
            2
        );
        close(&database).await.unwrap();
        let database = open(directory.path().join("db")).await.unwrap();
        let receipt = execute(
            &database,
            tx! { tables { balances: u64 => i64 = id } return balances[7]; }.unwrap(),
            claims,
        )
        .await
        .unwrap();
        assert_eq!(receipt.sequence, 3);
        assert_eq!(value(receipt), Value::I64(42));
        close(&database).await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_waiter_still_commits_and_close_drains_accepted_work() {
        let (directory, database) = fixture().await;
        let (reply, result) = oneshot::channel();
        database
            .sender
            .send(Request::Execute {
                command: Box::new(Command::Transaction {
                    transaction: tx! { return 42; }.unwrap(),
                    claims: policy().try_into().unwrap(),
                    manifest: None,
                }),
                reply,
            })
            .await
            .unwrap();
        drop(result);
        let clone = database.clone();
        close(&database).await.unwrap();
        assert!(matches!(
            execute(&clone, tx! { return 7; }.unwrap(), policy()).await,
            Err(Error::Closed)
        ));
        let store = storage::open(directory.path().join("db")).unwrap();
        assert_eq!(store.manifest().checkpoint_sequence, 1);
    }

    #[test]
    fn full_queue_yields_and_cancellation_before_enqueue_submits_nothing() {
        let (sender, mut receiver) = mpsc::channel(1);
        let database = Database {
            sender,
            stopped: watch::channel(()).1,
            database_id: [1; 16],
            cursor_namespace: [2; 16],
        };
        database.sender.try_send(Request::Close).unwrap();
        {
            let mut future = pin!(execute(&database, tx! { return 42; }.unwrap(), policy()));
            assert!(matches!(
                future
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop())),
                Poll::Pending
            ));
        }
        assert!(matches!(receiver.try_recv(), Ok(Request::Close)));
        assert!(matches!(
            receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn worker_loss_after_enqueue_is_uncertain_not_rejected() {
        let (sender, mut receiver) = mpsc::channel(1);
        let database = Database {
            sender,
            stopped: watch::channel(()).1,
            database_id: [1; 16],
            cursor_namespace: [2; 16],
        };
        let mut future = pin!(execute(&database, tx! { return 42; }.unwrap(), policy()));
        assert!(matches!(
            future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
        drop(receiver.recv().await.unwrap());
        assert!(matches!(
            future.await,
            Err(Error::Uncertain {
                sequence: None,
                source: None
            })
        ));
    }

    #[tokio::test]
    async fn startup_errors_are_reported_without_replacing_existing_databases() {
        let (directory, database) = fixture().await;
        let path = directory.path().join("db");
        assert!(matches!(
            open(&path).await,
            Err(Error::Storage(storage::Error::Locked))
        ));
        assert!(matches!(
            create(&path, CreateOptions::default()).await,
            Err(Error::Storage(storage::Error::Io(_)))
        ));
        assert!(matches!(
            open(directory.path().join("missing")).await,
            Err(Error::Storage(_))
        ));
        close(&database).await.unwrap();
        let database = open(path).await.unwrap();
        assert_eq!(
            execute(&database, tx! { return 42; }.unwrap(), policy())
                .await
                .unwrap()
                .sequence,
            1
        );
        close(&database).await.unwrap();
    }

    #[tokio::test]
    async fn generated_uuids_are_distinct_and_persisted_with_named_limits() {
        let directory = tempfile::tempdir().unwrap();
        let limits = Limits {
            writes: 123,
            overlay_bytes: 4096,
            ..Limits::default()
        };
        let path = directory.path().join("db");
        let database = create(
            &path,
            CreateOptions {
                limits,
                ..CreateOptions::default()
            },
        )
        .await
        .unwrap();
        let identities = [database.database_id(), database.cursor_namespace()];
        for id in identities {
            assert_eq!(id[6] >> 4, 4);
            assert_eq!(id[8] >> 6, 2);
        }
        assert_ne!(identities[0], identities[1]);
        close(&database).await.unwrap();
        let store = storage::open(&path).unwrap();
        assert_eq!(Limits::from(&store.genesis().initial_policy), limits);
        drop(store);
        let database = open(&path).await.unwrap();
        assert_eq!(
            [database.database_id(), database.cursor_namespace()],
            identities
        );
        close(&database).await.unwrap();
        let other = create(directory.path().join("other"), CreateOptions::default())
            .await
            .unwrap();
        assert!(!identities.contains(&other.database_id()));
        assert!(!identities.contains(&other.cursor_namespace()));
        close(&other).await.unwrap();
    }

    #[tokio::test]
    async fn explicit_identities_are_preserved_and_invalid_settings_do_not_create_a_directory() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("db");
        for options in [
            CreateOptions {
                database_id: Some([0; 16]),
                ..CreateOptions::default()
            },
            CreateOptions {
                cursor_namespace: Some([0; 16]),
                ..CreateOptions::default()
            },
            CreateOptions {
                limits: Limits {
                    writes: u64::MAX,
                    ..Limits::default()
                },
                ..CreateOptions::default()
            },
        ] {
            assert!(matches!(
                create(&path, options).await,
                Err(Error::Storage(storage::Error::InvalidInput(_)))
            ));
            assert!(!path.exists());
        }
        let database = create(
            &path,
            CreateOptions {
                database_id: Some([1; 16]),
                cursor_namespace: Some([2; 16]),
                ..CreateOptions::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(database.database_id(), [1; 16]);
        assert_eq!(database.cursor_namespace(), [2; 16]);
        close(&database).await.unwrap();
        let database = open(path).await.unwrap();
        assert_eq!(database.database_id(), [1; 16]);
        assert_eq!(database.cursor_namespace(), [2; 16]);
        close(&database).await.unwrap();
    }

    #[tokio::test]
    async fn invalid_named_claims_and_policies_are_rejected_without_consuming_a_sequence() {
        let (_directory, database) = fixture().await;
        let invalid = Limits {
            instructions: u64::MAX,
            ..Limits::default()
        };
        assert!(matches!(
            execute(&database, tx! { return 42; }.unwrap(), invalid).await,
            Err(Error::Rejected(_))
        ));
        assert!(matches!(
            execute_limits(&database, invalid).await,
            Err(Error::Rejected(_))
        ));
        let receipt = execute(&database, tx! { return 42; }.unwrap(), Limits::default())
            .await
            .unwrap();
        assert_eq!(receipt.sequence, 1);
        close(&database).await.unwrap();
    }

    #[tokio::test]
    async fn uncertain_append_stops_submissions_and_close_waits_for_recovery_access() {
        use std::io::Write;

        let (directory, database) = fixture().await;
        execute(&database, tx! { return 1; }.unwrap(), policy())
            .await
            .unwrap();
        let path = directory.path().join("db");
        let mut log = std::fs::OpenOptions::new()
            .append(true)
            .open(path.join("log-00000000000000000001.bin"))
            .unwrap();
        log.write_all(b"unpublished tail").unwrap();
        drop(log);
        assert!(matches!(
            execute(&database, tx! { return 2; }.unwrap(), policy()).await,
            Err(Error::Uncertain {
                sequence: Some(2),
                ..
            })
        ));
        assert!(matches!(
            execute(&database, tx! { return 3; }.unwrap(), policy()).await,
            Err(Error::Closed)
        ));
        assert!(matches!(close(&database).await, Err(Error::Closed)));
        let database = open(&path).await.unwrap();
        let receipt = execute(&database, tx! { return 4; }.unwrap(), policy())
            .await
            .unwrap();
        assert_eq!(receipt.sequence, 2);
        assert_eq!(value(receipt), Value::I64(4));
        close(&database).await.unwrap();
    }
}
