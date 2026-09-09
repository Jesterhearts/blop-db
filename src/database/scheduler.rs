//! Durable registration -> dependency-ready interpretation -> serial
//! installation -> prefix checkpoint/receipts. No worker publishes roots or
//! tentative state.

use std::collections::VecDeque;
use std::future::Future;
use std::future::poll_fn;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::thread;

use sha2::Digest;
use sha2::Sha256;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;

use super::Control;
use super::EngineOptions;
use super::Error;
use super::Receipt;
use super::Request;
use super::Result;
use super::RetentionFloors;
use super::budget;
use super::cursor;
use super::engine;
use super::maintenance;
use super::record;
use super::replica;
use super::snapshot;
use super::workers;
use crate::storage;
use crate::vm;

/// Last coordinator sample. Available without waiting for workers or storage
/// I/O. Byte fields report reserved capacity, not measured allocator or disk
/// usage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EngineStatus {
    pub options: EngineOptions,
    pub log_tail: u64,
    pub durable_frontier: u64,
    pub visibility_frontier: u64,
    pub checkpoint: u64,
    pub queued_count: usize,
    pub queued_bytes: u64,
    pub assigned_count: usize,
    pub assigned_bytes: u64,
    pub reserved_execution_bytes: u64,
    pub prepared_head_bytes: u64,
    pub oldest_unresolved: Option<u64>,
    pub resolved_above_frontier: usize,
    pub active_workers: usize,
    pub dependency_waiting: usize,
    pub administrative_barrier: Option<u64>,
    pub maintenance_pending: bool,
    pub backup_jobs: usize,
    pub last_maintenance: Option<super::MaintenanceReport>,
    pub retention: RetentionFloors,
    pub poisoned: bool,
    pub closed: bool,
    pub last_error: Option<String>,
}

pub(super) fn initial_status(options: EngineOptions) -> EngineStatus {
    EngineStatus {
        options,
        log_tail: 0,
        durable_frontier: 0,
        visibility_frontier: 0,
        checkpoint: 0,
        queued_count: 0,
        queued_bytes: 0,
        assigned_count: 0,
        assigned_bytes: 0,
        reserved_execution_bytes: 0,
        prepared_head_bytes: 0,
        oldest_unresolved: None,
        resolved_above_frontier: 0,
        active_workers: 0,
        dependency_waiting: 0,
        administrative_barrier: None,
        maintenance_pending: false,
        backup_jobs: 0,
        last_maintenance: None,
        retention: RetentionFloors { history: 0, log: 1 },
        poisoned: false,
        closed: false,
        last_error: None,
    }
}

type Reply = oneshot::Sender<Result<Receipt>>;

struct Prepared {
    transaction: vm::PreparedTransaction,
    bytes: Vec<u8>,
    reservation: u64,
}

struct Head {
    command: Box<record::Command>,
    prepared: Option<Prepared>,
    reply: Reply,
    _permit: budget::Permit,
}

struct Entry {
    sequence: u64,
    digest: [u8; 32],
    writes: Vec<vm::Scope>,
    dependencies: Vec<u64>,
    prepared: Option<vm::PreparedTransaction>,
    outcome: Option<vm::Outcome>,
    reply: Reply,
    bytes: u64,
    reservation: u64,
}

struct Schedule {
    frontier: u64,
    entries: VecDeque<Entry>,
    assigned_bytes: u64,
    reserved_bytes: u64,
}

fn next_sequence(durable: u64) -> Result<u64> {
    durable
        .checked_add(1)
        .filter(|&n| n != u64::MAX)
        .ok_or(Error::Storage(storage::Error::Exhausted))
}

fn prepare(
    store: &storage::Store,
    command: &mut record::Command,
    options: &EngineOptions,
    previous_sequence: u64,
    predecessor: [u8; 32],
) -> Result<Option<Prepared>> {
    let record::Command::Transaction {
        transaction,
        claims,
        manifest,
    } = command
    else {
        let (_, body) = record::encode(command).map_err(engine::rejection)?;
        let required = body.len() as u64 + 72;
        if required > options.assigned_backlog_bytes {
            return Err(Error::OperationalLimit {
                resource: "assigned_backlog_bytes",
                required,
                limit: options.assigned_backlog_bytes,
            });
        }
        return Ok(None);
    };
    let sequence = next_sequence(previous_sequence)?;
    let prepared = match vm::prepare_transaction_bounded(
        &storage::view(store),
        sequence,
        transaction,
        claims,
        manifest.as_ref(),
        options.preparation_bytes,
    )
    .map_err(engine::rejection)?
    {
        vm::Preparation::Ready(prepared) => *prepared,
        vm::Preparation::Capacity(required) => {
            return Err(Error::OperationalLimit {
                resource: "preparation_bytes",
                required,
                limit: options.preparation_bytes,
            });
        }
    };
    *manifest = Some(prepared.manifest().clone());
    let reservation = prepared
        .reservation_bytes()
        .saturating_add(options.assigned_backlog_count as u64 * 16)
        .saturating_add(budget::input_bytes(command)?.saturating_mul(3));
    for (resource, limit) in [
        ("execution_bytes", options.execution_bytes),
        ("preparation_bytes", options.preparation_bytes),
    ] {
        if reservation > limit {
            return Err(Error::OperationalLimit {
                resource,
                required: reservation,
                limit,
            });
        }
    }
    let (kind, body) = record::encode(command).map_err(engine::rejection)?;
    let bytes = engine::envelope(sequence, predecessor, kind, &body).map_err(engine::rejection)?;
    if bytes.len() as u64 > options.assigned_backlog_bytes {
        return Err(Error::OperationalLimit {
            resource: "assigned_backlog_bytes",
            required: bytes.len() as u64,
            limit: options.assigned_backlog_bytes,
        });
    }
    Ok(Some(Prepared {
        transaction: prepared,
        bytes,
        reservation,
    }))
}

fn overlaps(
    writes: &[vm::Scope],
    reads: &vm::AccessManifest,
) -> bool {
    reads
        .reads()
        .any(|read| writes.iter().any(|write| vm::overlap(write, read)))
}

fn register(
    store: &mut storage::Store,
    schedule: &mut Schedule,
    head: &mut Option<Head>,
    receiver: &mut mpsc::Receiver<Request>,
    control: &mpsc::Receiver<Control>,
    pool: &workers::Pool,
    options: &EngineOptions,
    status: &watch::Sender<EngineStatus>,
    barrier: &mut Option<Request>,
) -> Result<()> {
    let mut records = Vec::new();
    // Bound both accepted and rejected work, and never wait to fill a group.
    for _ in 0..=receiver.len().min(63) {
        if head.is_none() {
            if !control.is_empty()
                || !pool.completed.is_empty()
                || pool.poisoned.load(Ordering::Acquire)
            {
                break;
            }
            let Ok(request) = receiver.try_recv() else {
                break;
            };
            let Request::Execute {
                mut command,
                reply,
                permit,
            } = request
            else {
                *barrier = Some(request);
                break;
            };
            if !matches!(command.as_ref(), record::Command::Transaction { .. }) {
                *barrier = Some(Request::Execute {
                    command,
                    reply,
                    permit,
                });
                break;
            }
            let previous = schedule.entries.back().unwrap();
            status.send_modify(|s| s.prepared_head_bytes = options.preparation_bytes);
            match prepare(
                store,
                &mut command,
                options,
                previous.sequence,
                previous.digest,
            ) {
                Ok(prepared) => {
                    *head = Some(Head {
                        command,
                        prepared,
                        reply,
                        _permit: permit,
                    })
                }
                Err(error) => {
                    let fatal =
                        !matches!(error, Error::Rejected(_) | Error::OperationalLimit { .. });
                    let message = error.to_string();
                    let _ = reply.send(Err(error));
                    if fatal {
                        return Err(Error::Uncertain {
                            sequence: None,
                            source: Some(workers::failure(message)),
                        });
                    }
                    continue;
                }
            }
        }
        let prepared = head.as_ref().unwrap().prepared.as_ref().unwrap();
        if schedule.entries.len() == options.assigned_backlog_count
            || prepared.bytes.len() as u64
                > options.assigned_backlog_bytes - schedule.assigned_bytes
            || prepared.reservation > options.execution_bytes - schedule.reserved_bytes
        {
            break;
        }
        let Head {
            prepared,
            reply,
            _permit,
            ..
        } = head.take().unwrap();
        let Prepared {
            transaction,
            bytes,
            reservation,
        } = prepared.unwrap();
        let sequence = transaction.sequence();
        let digest = Sha256::digest(&bytes).into();
        // Every unresolved possible writer matters: a newer writer can abort.
        let dependencies = schedule
            .entries
            .iter()
            .filter(|entry| {
                entry.outcome.is_none() && overlaps(&entry.writes, transaction.manifest())
            })
            .map(|entry| entry.sequence)
            .collect();
        let writes = transaction.manifest().writes().cloned().collect();
        schedule.assigned_bytes += bytes.len() as u64;
        schedule.reserved_bytes += reservation;
        schedule.entries.push_back(Entry {
            sequence,
            digest,
            writes,
            dependencies,
            prepared: Some(transaction),
            outcome: None,
            reply,
            bytes: bytes.len() as u64,
            reservation,
        });
        // Selected preparations use assigned reservations; queue permits remain
        // held until the entire group is durable. Only `head` uses scratch.
        records.push((sequence, bytes, _permit));
    }
    engine::append_batch(
        store,
        records.iter().map(|(n, bytes, _)| (*n, bytes.as_slice())),
    )
    .map_err(|error| Error::Uncertain {
        sequence: records.first().map(|(n, _, _)| *n),
        source: Some(error.into()),
    })?;
    Ok(())
}

fn in_window(
    sequence: u64,
    frontier: u64,
    durable: u64,
    window: u64,
) -> bool {
    sequence > frontier && sequence <= durable && sequence - frontier <= window
}

fn dispatch(
    store: &storage::Store,
    schedule: &mut Schedule,
    pool: &mut workers::Pool,
    window: u64,
) -> vm::Result<bool> {
    let mut progress = false;
    for worker in 0..pool.busy.len() {
        if pool.busy[worker] {
            continue;
        }
        let Some(entry) = schedule.entries.iter_mut().find(|entry| {
            entry.prepared.is_some()
                && entry.dependencies.is_empty()
                && in_window(
                    entry.sequence,
                    schedule.frontier,
                    store.manifest().durable_sequence,
                    window,
                )
        }) else {
            break;
        };
        // Capture after readiness, not at admission: this root includes every
        // installed predecessor, while MVCC excludes all log-later versions.
        let job = workers::Job {
            prepared: entry.prepared.take().unwrap(),
            view: storage::view(store),
        };
        workers::dispatch(pool, worker, job)?;
        progress = true;
    }
    Ok(progress)
}

fn complete(
    store: &mut storage::Store,
    schedule: &mut Schedule,
    pool: &mut workers::Pool,
    mut completion: workers::Completion,
) -> vm::Result<()> {
    let queued = pool.completed.len();
    for index in 0..=queued {
        pool.busy[completion.worker] = false;
        let outcome = completion.outcome?;
        if pool.poisoned.load(Ordering::Acquire) {
            return Err(workers::failure("worker pool requires recovery"));
        }
        let entry = schedule
            .entries
            .iter_mut()
            .find(|entry| entry.sequence == completion.sequence)
            .expect("completion belongs to assigned record");
        vm::install(store, entry.sequence, entry.digest, 1, &outcome)?;
        entry.outcome = Some(outcome);
        entry.writes.clear();
        for entry in &mut schedule.entries {
            entry
                .dependencies
                .retain(|&sequence| sequence != completion.sequence);
        }
        if index == queued {
            break;
        }
        let Ok(next) = pool.completed.try_recv() else {
            break;
        };
        completion = next;
    }
    let mut digest = None;
    let previous_frontier = schedule.frontier;
    for entry in schedule
        .entries
        .iter()
        .filter(|entry| entry.sequence > previous_frontier)
    {
        if entry.outcome.is_none() {
            break;
        }
        schedule.frontier = entry.sequence;
        digest = Some(entry.digest);
    }
    if let Some(digest) = digest {
        let mut manifest = store.manifest().clone();
        manifest.checkpoint_sequence = schedule.frontier;
        manifest.checkpoint_digest = digest;
        let checkpoint = if schedule.frontier == manifest.durable_sequence {
            storage::view(store)
        } else {
            storage::prepare_checkpoint(store, schedule.frontier)?
        };
        storage::publish(store, &checkpoint, manifest)?;
        while schedule
            .entries
            .front()
            .is_some_and(|entry| entry.sequence <= schedule.frontier)
        {
            let entry = schedule.entries.pop_front().unwrap();
            schedule.assigned_bytes -= entry.bytes;
            schedule.reserved_bytes -= entry.reservation;
            let _ = entry.reply.send(Ok(Receipt {
                sequence: entry.sequence,
                outcome: entry.outcome.unwrap(),
            }));
        }
    }
    Ok(())
}

fn sample(
    store: &storage::Store,
    schedule: &Schedule,
    pool: &workers::Pool,
    queue: &budget::Queue,
    head: Option<&Head>,
    registry: &mut snapshot::Registry,
    status: &watch::Sender<EngineStatus>,
) -> Result<()> {
    let (history, log) = cursor::retention_floors(store, registry, schedule.frontier)?;
    status.send_modify(|status| {
        status.log_tail = store.manifest().durable_sequence;
        status.durable_frontier = store.manifest().durable_sequence;
        status.visibility_frontier = schedule.frontier;
        status.checkpoint = store.manifest().checkpoint_sequence;
        status.queued_count = queue.max_count - queue.count.available_permits();
        status.queued_bytes = (queue.max_bytes - queue.bytes.available_permits()) as u64;
        status.assigned_count = schedule.entries.len();
        status.assigned_bytes = schedule.assigned_bytes;
        status.reserved_execution_bytes = schedule.reserved_bytes;
        status.prepared_head_bytes = head
            .and_then(|h| h.prepared.as_ref())
            .map_or(0, |p| p.reservation);
        status.oldest_unresolved = schedule
            .entries
            .iter()
            .find(|e| e.outcome.is_none())
            .map(|e| e.sequence);
        status.resolved_above_frontier = schedule
            .entries
            .iter()
            .filter(|e| e.sequence > schedule.frontier && e.outcome.is_some())
            .count();
        status.active_workers = pool.busy.iter().filter(|&&busy| busy).count();
        status.dependency_waiting = schedule
            .entries
            .iter()
            .filter(|e| !e.dependencies.is_empty())
            .count();
        status.administrative_barrier = head
            .filter(|h| h.prepared.is_none())
            .and_then(|_| next_sequence(store.manifest().durable_sequence).ok());
        status.retention = RetentionFloors { history, log };
    });
    Ok(())
}

enum Event {
    Request(Option<Request>),
    Control(Option<Control>),
    Complete(Option<workers::Completion>),
}

struct Notify(thread::Thread);
impl Wake for Notify {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

fn wait(
    pool: &mut workers::Pool,
    receiver: &mut mpsc::Receiver<Request>,
    control: &mut mpsc::Receiver<Control>,
    requests: bool,
    controls: bool,
    prefer_requests: bool,
) -> Event {
    let waker = Waker::from(Arc::new(Notify(thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(poll_fn(|cx| {
        if let Poll::Ready(completion) = pool.completed.poll_recv(cx) {
            return Poll::Ready(Event::Complete(completion));
        }
        if prefer_requests
            && requests
            && let Poll::Ready(request) = receiver.poll_recv(cx)
        {
            return Poll::Ready(Event::Request(request));
        }
        if controls && let Poll::Ready(request) = control.poll_recv(cx) {
            return Poll::Ready(Event::Control(request));
        }
        if requests && let Poll::Ready(request) = receiver.poll_recv(cx) {
            return Poll::Ready(Event::Request(request));
        }
        Poll::Pending
    }));
    loop {
        if let Poll::Ready(event) = future.as_mut().poll(&mut context) {
            return event;
        }
        thread::park();
    }
}

pub(super) fn run(
    mut store: storage::Store,
    mut receiver: mpsc::Receiver<Request>,
    mut control: mpsc::Receiver<Control>,
    queue: budget::Queue,
    options: EngineOptions,
    status: watch::Sender<EngineStatus>,
    mut pool: workers::Pool,
) {
    storage::enable_runtime_cache(&mut store);
    let mut registry = snapshot::Registry::default();
    let mut schedule = Schedule {
        frontier: store.manifest().checkpoint_sequence,
        entries: VecDeque::new(),
        assigned_bytes: 0,
        reserved_bytes: 0,
    };
    let mut head: Option<Head> = None;
    let mut pending_request = None;
    let mut requests = true;
    let mut controls = true;
    let mut prefer_requests = false;
    let mut failed = false;
    let mut last_error = None;
    let mut pending_maintenance: Option<(
        super::MaintenanceOptions,
        oneshot::Sender<Result<super::MaintenanceReport>>,
    )> = None;
    let mut backups = maintenance::Backups::default();
    loop {
        if pool.poisoned.load(Ordering::Acquire) {
            failed = true;
            break;
        }
        // Observe reported faults before admitting or dispatching more work.
        if let Ok(completion) = pool.completed.try_recv() {
            if let Err(error) = complete(&mut store, &mut schedule, &mut pool, completion) {
                last_error = Some(error.to_string());
                failed = true;
                break;
            }
            continue;
        }
        if let Err(error) = sample(
            &store,
            &schedule,
            &pool,
            &queue,
            head.as_ref(),
            &mut registry,
            &status,
        ) {
            last_error = Some(error.to_string());
            failed = true;
            break;
        }
        status.send_modify(|s| {
            s.maintenance_pending = pending_maintenance.is_some();
            s.backup_jobs = backups.active();
        });
        if schedule.entries.is_empty()
            && let Some((options, reply)) = pending_maintenance.take()
        {
            let result = maintenance::run(&mut store, &mut registry, schedule.frontier, options);
            if let Ok(report) = &result {
                status.send_modify(|s| s.last_maintenance = Some(report.clone()));
            } else {
                last_error = result.as_ref().err().map(ToString::to_string);
                failed = true;
            }
            let _ = reply.send(result);
            if failed {
                break;
            }
            continue;
        }
        if !requests && head.is_none() && schedule.entries.is_empty() {
            break;
        }
        let mut progress = false;
        if pending_maintenance.is_none()
            && let Some(pending) = &head
        {
            if let Some(prepared) = &pending.prepared {
                if schedule.entries.len() < options.assigned_backlog_count
                    && prepared.bytes.len() as u64
                        <= options.assigned_backlog_bytes - schedule.assigned_bytes
                    && prepared.reservation <= options.execution_bytes - schedule.reserved_bytes
                {
                    if let Err(error) = register(
                        &mut store,
                        &mut schedule,
                        &mut head,
                        &mut receiver,
                        &control,
                        &pool,
                        &options,
                        &status,
                        &mut pending_request,
                    ) {
                        last_error = Some(error.to_string());
                        failed = true;
                        break;
                    }
                    progress = true;
                }
            } else if schedule.entries.is_empty() {
                let pending = head.take().unwrap();
                let result = engine::commit(&mut store, &pending.command);
                failed = result
                    .as_ref()
                    .is_err_and(|e| !matches!(e, Error::Rejected(_)));
                if failed {
                    last_error = result.as_ref().err().map(ToString::to_string);
                }
                if let Ok(receipt) = &result {
                    schedule.frontier = receipt.sequence;
                }
                let _ = pending.reply.send(result);
                if failed {
                    break;
                }
                progress = true;
            }
        }
        match dispatch(&store, &mut schedule, &mut pool, options.execution_window) {
            Ok(dispatched) => progress |= dispatched,
            Err(error) => {
                last_error = Some(error.to_string());
                failed = true;
                break;
            }
        }
        if progress {
            continue;
        }
        let event = if let Some(request) = pending_request.take() {
            Event::Request(Some(request))
        } else {
            wait(
                &mut pool,
                &mut receiver,
                &mut control,
                requests && head.is_none() && pending_maintenance.is_none(),
                controls && pending_maintenance.is_none(),
                prefer_requests,
            )
        };
        match event {
            Event::Complete(Some(completion)) => {
                if let Err(error) = complete(&mut store, &mut schedule, &mut pool, completion) {
                    last_error = Some(error.to_string());
                    failed = true;
                    break;
                }
            }
            Event::Complete(None) => {
                failed = true;
                break;
            }
            Event::Control(None) => controls = false,
            Event::Control(Some(Control::Maintenance { options, reply })) => {
                prefer_requests = true;
                pending_maintenance = Some((options, reply));
            }
            Event::Control(Some(Control::Backup { destination, reply })) => {
                prefer_requests = true;
                maintenance::start_backup(
                    &mut backups,
                    &store,
                    destination,
                    reply,
                    #[cfg(test)]
                    pool.hooks.clone(),
                );
            }
            Event::Control(Some(Control::Snapshot { reply })) => {
                prefer_requests = true;
                let _ = reply.send(Ok(snapshot::capture(
                    &mut registry,
                    &store,
                    schedule.frontier,
                )));
            }
            Event::Control(Some(Control::Retention { operation, reply })) => {
                prefer_requests = true;
                let result =
                    cursor::handle(&mut store, &mut registry, operation, schedule.frontier);
                failed = result.as_ref().is_err_and(|error| {
                    matches!(
                        error,
                        Error::Storage(_) | Error::Uncertain { .. } | Error::Read(_)
                    )
                });
                if failed {
                    last_error = result.as_ref().err().map(ToString::to_string);
                }
                let _ = reply.send(result);
                if failed {
                    break;
                }
            }
            Event::Request(None) => requests = false,
            Event::Request(Some(Request::Close)) => {
                receiver.close();
                budget::close(&queue);
            }
            Event::Request(Some(Request::Import {
                batch,
                reply,
                permit,
                slot,
            })) => {
                prefer_requests = false;
                // Imports are restricted to replicas, which cannot have a
                // prepared local head or unresolved local canonical work.
                let result = if !store.is_read_only() {
                    Err(Error::InvalidInput(
                        "logical import requires a read-only replica",
                    ))
                } else if head.is_some()
                    || !schedule.entries.is_empty()
                    || schedule.frontier != store.manifest().durable_sequence
                {
                    Err(Error::Storage(storage::Error::NeedsRecovery))
                } else {
                    replica::install(
                        &mut store,
                        &batch,
                        &options,
                        &status,
                        #[cfg(test)]
                        &pool.hooks,
                    )
                };
                failed = result.as_ref().is_err_and(|error| {
                    matches!(
                        error,
                        Error::Storage(_) | Error::Read(_) | Error::Uncertain { .. }
                    )
                });
                if failed {
                    last_error = result.as_ref().err().map(ToString::to_string);
                }
                if let Ok(watermark) = &result {
                    schedule.frontier = watermark.sequence();
                    status.send_modify(|s| {
                        s.visibility_frontier = schedule.frontier;
                        s.checkpoint = store.manifest().checkpoint_sequence;
                        s.durable_frontier = store.manifest().durable_sequence;
                        s.log_tail = s.durable_frontier;
                    });
                }
                drop(batch);
                drop((permit, slot));
                let _ = reply.send(result);
                if failed {
                    break;
                }
            }
            Event::Request(Some(Request::Execute {
                mut command,
                reply,
                permit,
            })) => {
                prefer_requests = false;
                if store.is_read_only() {
                    let _ = reply.send(Err(Error::ReadOnly));
                    continue;
                }
                status.send_modify(|s| s.prepared_head_bytes = options.preparation_bytes);
                match prepare(
                    &store,
                    &mut command,
                    &options,
                    store.manifest().durable_sequence,
                    store.manifest().durable_digest,
                ) {
                    Ok(prepared) => {
                        head = Some(Head {
                            command,
                            prepared,
                            reply,
                            _permit: permit,
                        })
                    }
                    Err(error) => {
                        failed =
                            !matches!(error, Error::Rejected(_) | Error::OperationalLimit { .. });
                        if failed {
                            last_error = Some(error.to_string());
                        }
                        let _ = reply.send(Err(error));
                        if failed {
                            break;
                        }
                    }
                }
            }
        }
    }
    receiver.close();
    control.close();
    budget::close(&queue);
    let _ = sample(
        &store,
        &schedule,
        &pool,
        &queue,
        head.as_ref(),
        &mut registry,
        &status,
    );
    if failed {
        last_error = pool
            .failure
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .or(last_error)
            .or_else(|| Some("database execution requires reopen".into()));
    }
    status.send_modify(|s| {
        s.poisoned = failed;
        s.closed = true;
        s.last_error = last_error.clone();
    });
    if let Some(head) = head {
        let _ = head.reply.send(Err(Error::Closed));
    }
    if let Some((_, reply)) = pending_maintenance {
        let _ = reply.send(Err(Error::Closed));
    }
    for entry in schedule.entries {
        let _ = entry.reply.send(Err(Error::Uncertain {
            sequence: Some(entry.sequence),
            source: Some(workers::failure(
                last_error
                    .clone()
                    .unwrap_or_else(|| "database execution requires reopen".into()),
            )),
        }));
    }
    while let Some(request) = pending_request.take().or_else(|| receiver.blocking_recv()) {
        match request {
            Request::Execute { reply, .. } => {
                let _ = reply.send(Err(Error::Closed));
            }
            Request::Import { reply, .. } => {
                let _ = reply.send(Err(Error::Closed));
            }
            Request::Close => {}
        }
    }
    while let Some(request) = control.blocking_recv() {
        match request {
            Control::Maintenance { reply, .. } => {
                let _ = reply.send(Err(Error::Closed));
            }
            Control::Backup { reply, .. } => {
                let _ = reply.send(Err(Error::Closed));
            }
            Control::Snapshot { reply } => {
                let _ = reply.send(Err(Error::Closed));
            }
            Control::Retention { reply, .. } => {
                let _ = reply.send(Err(Error::Closed));
            }
        }
    }
    drop(pool);
    drop(backups);
    snapshot::revoke_all(&mut registry);
    status.send_modify(|s| {
        s.active_workers = 0;
        s.backup_jobs = 0;
        s.maintenance_pending = false;
    });
}

#[cfg(test)]
mod tests {
    use std::ops::Bound::Unbounded;
    use std::time::Duration;
    use std::time::Instant;

    use super::super::workers::test_support::Action;
    use super::super::workers::test_support::Gate;
    use super::*;
    use crate::database as db;
    use crate::storage::Genesis;
    use crate::storage::Store;
    use crate::storage::TreeId;
    use crate::tx;
    use crate::vm::AccessManifest;
    use crate::vm::AccessMode;
    use crate::vm::CatalogueOperation;
    use crate::vm::Outcome;
    use crate::vm::Scope;
    use crate::vm::Type;
    use crate::vm::Value;

    fn transaction(transaction: crate::Transaction) -> record::Command {
        record::Command::Transaction {
            transaction,
            claims: crate::Limits::default().try_into().unwrap(),
            manifest: None,
        }
    }

    fn table() -> record::Command {
        record::Command::Catalogue(CatalogueOperation::Create {
            name: "data".into(),
            key: Type::U64,
            value: Type::U64,
        })
    }

    async fn fixture(options: EngineOptions) -> (tempfile::TempDir, db::Database) {
        let directory = tempfile::tempdir().unwrap();
        let database = db::create_with_options(
            directory.path().join("db"),
            db::CreateOptions {
                database_id: Some([1; 16]),
                cursor_namespace: Some([2; 16]),
                ..Default::default()
            },
            options,
        )
        .await
        .unwrap();
        (directory, database)
    }

    fn reference(
        directory: &tempfile::TempDir,
        name: &str,
    ) -> Store {
        storage::create(
            directory.path().join(name),
            Genesis {
                database_id: [1; 16],
                initial_policy: crate::Limits::default().try_into().unwrap(),
            },
            [2; 16],
        )
        .unwrap()
    }

    async fn enqueue(
        database: &db::Database,
        command: record::Command,
    ) -> oneshot::Receiver<Result<Receipt>> {
        let permit = budget::reserve(&database.queue, &command).await.unwrap();
        let (reply, result) = oneshot::channel();
        database
            .sender
            .send(Request::Execute {
                command: Box::new(command),
                reply,
                permit,
            })
            .await
            .unwrap();
        result
    }

    async fn wait_status(
        database: &db::Database,
        predicate: impl Fn(&EngineStatus) -> bool,
    ) -> EngineStatus {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let status = db::status(database);
            if predicate(&status) {
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "status wait timed out: {status:?}"
            );
            tokio::task::yield_now().await;
        }
    }

    fn value(receipt: Receipt) -> Value {
        let Outcome::Success { value, .. } = receipt.outcome else {
            panic!("unexpected abort")
        };
        value
    }

    fn broad(mut command: record::Command) -> record::Command {
        let record::Command::Transaction { manifest, .. } = &mut command else {
            unreachable!()
        };
        *manifest = Some(AccessManifest::new([(Scope::Table(1), AccessMode::Write)]).unwrap());
        command
    }

    async fn queued(
        options: &EngineOptions,
        commands: Vec<record::Command>,
    ) -> (
        budget::Queue,
        mpsc::Receiver<Request>,
        Vec<oneshot::Receiver<Result<Receipt>>>,
    ) {
        let queue = budget::Queue::new(options);
        let (sender, receiver) = mpsc::channel(options.submission_queue_count);
        let mut receipts = Vec::new();
        for command in commands {
            let permit = budget::reserve(&queue, &command).await.unwrap();
            let (reply, receipt) = oneshot::channel();
            sender
                .try_send(Request::Execute {
                    command: Box::new(command),
                    reply,
                    permit,
                })
                .unwrap();
            receipts.push(receipt);
        }
        (queue, receiver, receipts)
    }

    fn queued_head(
        store: &Store,
        receiver: &mut mpsc::Receiver<Request>,
        options: &EngineOptions,
    ) -> Option<Head> {
        let Request::Execute {
            mut command,
            reply,
            permit,
        } = receiver.try_recv().unwrap()
        else {
            unreachable!()
        };
        let prepared = prepare(
            store,
            &mut command,
            options,
            store.manifest().durable_sequence,
            store.manifest().durable_digest,
        )
        .unwrap();
        Some(Head {
            command,
            prepared,
            reply,
            _permit: permit,
        })
    }

    #[tokio::test]
    async fn queued_log_group_is_durable_before_dispatch_and_completions_share_a_checkpoint() {
        let directory = tempfile::tempdir().unwrap();
        let mut store = reference(&directory, "group");
        let options = EngineOptions {
            workers: 3,
            ..Default::default()
        };
        let (queue, mut receiver, mut receipts) = queued(
            &options,
            vec![
                transaction(tx! { return 1; }.unwrap()),
                transaction(tx! { return 2; }.unwrap()),
                transaction(tx! { return 3; }.unwrap()),
            ],
        )
        .await;
        let mut head = queued_head(&store, &mut receiver, &options);
        let (_, control) = mpsc::channel(1);
        let (status, _) = watch::channel(initial_status(options.clone()));
        let hooks = Arc::new(workers::test_support::Hooks::default());
        let mut pool = workers::start(options.workers, hooks.clone()).unwrap();
        let mut schedule = Schedule {
            frontier: 0,
            entries: VecDeque::new(),
            assigned_bytes: 0,
            reserved_bytes: 0,
        };
        let generation = store.manifest().generation;
        let mut barrier = None;
        register(
            &mut store,
            &mut schedule,
            &mut head,
            &mut receiver,
            &control,
            &pool,
            &options,
            &status,
            &mut barrier,
        )
        .unwrap();
        assert!(barrier.is_none());
        assert!(head.is_none());
        assert_eq!(store.manifest().generation, generation + 1);
        assert_eq!(store.manifest().durable_sequence, 3);
        assert_eq!(store.manifest().checkpoint_sequence, 0);
        assert_eq!(schedule.entries.len(), 3);
        assert!(schedule.reserved_bytes <= options.execution_bytes);
        assert_eq!(queue.count.available_permits(), queue.max_count);
        assert_eq!(queue.bytes.available_permits(), queue.max_bytes);
        assert!(hooks.started.lock().unwrap().is_empty());
        assert!(entries(&store, TreeId::Outcomes).is_empty());
        assert!(dispatch(&store, &mut schedule, &mut pool, options.execution_window).unwrap());
        let deadline = Instant::now() + Duration::from_secs(15);
        while pool.completed.len() != 3 {
            assert!(Instant::now() < deadline, "workers did not complete");
            tokio::task::yield_now().await;
        }
        for receipt in &mut receipts {
            assert!(matches!(
                receipt.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ));
        }
        let first = pool.completed.try_recv().unwrap();
        complete(&mut store, &mut schedule, &mut pool, first).unwrap();
        assert_eq!(store.manifest().generation, generation + 2);
        assert_eq!(store.manifest().checkpoint_sequence, 3);
        assert_eq!(schedule.frontier, 3);
        assert!(schedule.entries.is_empty());
        assert_eq!((schedule.assigned_bytes, schedule.reserved_bytes), (0, 0));
        for (index, receipt) in receipts.into_iter().enumerate() {
            let receipt = receipt.await.unwrap().unwrap();
            assert_eq!(receipt.sequence, index as u64 + 1);
            assert_eq!(value(receipt), Value::I64(index as i64 + 1));
        }
        drop(pool);
        drop(store);
        let mut reopened = storage::open(directory.path().join("group")).unwrap();
        engine::recover(&mut reopened).unwrap();
        assert_eq!(reopened.manifest().generation, generation + 2);
    }

    #[tokio::test]
    async fn queued_groups_keep_rejections_backpressure_and_administrative_barriers_ordered() {
        let directory = tempfile::tempdir().unwrap();
        let mut store = reference(&directory, "group");
        let options = EngineOptions {
            workers: 2,
            assigned_backlog_count: 2,
            ..Default::default()
        };
        let (queue, mut receiver, mut receipts) = queued(
            &options,
            vec![
                transaction(tx! { return 1; }.unwrap()),
                transaction(tx! { tables { data: u64 => u64 = 1 } data[1] = 1; }.unwrap()),
                transaction(tx! { return 2; }.unwrap()),
                transaction(tx! { return 3; }.unwrap()),
                table(),
                transaction(tx! { tables { data: u64 => u64 = 4 } data[1] = 42; }.unwrap()),
            ],
        )
        .await;
        let mut head = queued_head(&store, &mut receiver, &options);
        let (_, control) = mpsc::channel(1);
        let (status, _) = watch::channel(initial_status(options.clone()));
        let mut pool = workers::start(options.workers, Arc::default()).unwrap();
        let mut schedule = Schedule {
            frontier: 0,
            entries: VecDeque::new(),
            assigned_bytes: 0,
            reserved_bytes: 0,
        };
        let mut barrier = None;
        register(
            &mut store,
            &mut schedule,
            &mut head,
            &mut receiver,
            &control,
            &pool,
            &options,
            &status,
            &mut barrier,
        )
        .unwrap();
        assert!(barrier.is_none());
        assert_eq!(store.manifest().durable_sequence, 2);
        assert_eq!(schedule.entries.len(), 2);
        assert_eq!(
            head.as_ref()
                .unwrap()
                .prepared
                .as_ref()
                .unwrap()
                .transaction
                .sequence(),
            3
        );
        assert_eq!(queue.max_count - queue.count.available_permits(), 3);
        assert!(matches!(
            receipts.remove(1).await.unwrap(),
            Err(Error::Rejected(_))
        ));
        // A cancelled receiver must not cancel canonical work or free its
        // sequence.
        drop(receipts.remove(1));
        dispatch(&store, &mut schedule, &mut pool, options.execution_window).unwrap();
        while !schedule.entries.is_empty() {
            let completion = pool.completed.recv().await.unwrap();
            complete(&mut store, &mut schedule, &mut pool, completion).unwrap();
        }
        register(
            &mut store,
            &mut schedule,
            &mut head,
            &mut receiver,
            &control,
            &pool,
            &options,
            &status,
            &mut barrier,
        )
        .unwrap();
        assert!(matches!(&barrier, Some(Request::Execute { command, .. })
            if matches!(command.as_ref(), record::Command::Catalogue(_))));
        assert!(head.is_none());
        assert_eq!(receiver.len(), 1);
        assert_eq!(store.manifest().durable_sequence, 3);
        dispatch(&store, &mut schedule, &mut pool, options.execution_window).unwrap();
        let completion = pool.completed.recv().await.unwrap();
        complete(&mut store, &mut schedule, &mut pool, completion).unwrap();
        let Request::Execute {
            command,
            reply,
            permit,
        } = barrier.take().unwrap()
        else {
            unreachable!()
        };
        let receipt = engine::commit(&mut store, &command).unwrap();
        schedule.frontier = receipt.sequence;
        assert_eq!(receipt.sequence, 4);
        reply.send(Ok(receipt)).unwrap();
        drop(permit);
        head = queued_head(&store, &mut receiver, &options);
        register(
            &mut store,
            &mut schedule,
            &mut head,
            &mut receiver,
            &control,
            &pool,
            &options,
            &status,
            &mut barrier,
        )
        .unwrap();
        dispatch(&store, &mut schedule, &mut pool, options.execution_window).unwrap();
        let completion = pool.completed.recv().await.unwrap();
        complete(&mut store, &mut schedule, &mut pool, completion).unwrap();
        for (receipt, sequence) in receipts.into_iter().zip([1, 3, 4, 5]) {
            assert_eq!(receipt.await.unwrap().unwrap().sequence, sequence);
        }
        assert_eq!(
            rows(&store, 4, 5),
            vec![(1_u64.to_be_bytes().to_vec(), 42_u64.to_le_bytes().to_vec())]
        );
        assert_eq!(queue.count.available_permits(), queue.max_count);
    }

    #[tokio::test]
    async fn queued_control_and_close_stop_log_group_selection() {
        for close in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let mut store = reference(&directory, "group");
            let options = EngineOptions::default();
            let (queue, mut receiver, _receipts) = queued(
                &options,
                vec![
                    transaction(tx! { return 1; }.unwrap()),
                    transaction(tx! { return 2; }.unwrap()),
                ],
            )
            .await;
            let mut head = queued_head(&store, &mut receiver, &options);
            let (sender, control) = mpsc::channel(1);
            let (reply, _snapshot) = oneshot::channel();
            sender.try_send(Control::Snapshot { reply }).unwrap();
            if close {
                let (sender, replacement) = mpsc::channel(1);
                sender.try_send(Request::Close).unwrap();
                receiver = replacement;
            }
            let (status, _) = watch::channel(initial_status(options.clone()));
            let pool = workers::start(options.workers, Arc::default()).unwrap();
            let mut schedule = Schedule {
                frontier: 0,
                entries: VecDeque::new(),
                assigned_bytes: 0,
                reserved_bytes: 0,
            };
            let mut control = control;
            if close {
                control.try_recv().unwrap();
            }
            let mut barrier = None;
            register(
                &mut store,
                &mut schedule,
                &mut head,
                &mut receiver,
                &control,
                &pool,
                &options,
                &status,
                &mut barrier,
            )
            .unwrap();
            assert_eq!(store.manifest().durable_sequence, 1);
            assert_eq!(schedule.entries.len(), 1);
            if close {
                assert!(matches!(barrier, Some(Request::Close)));
            } else {
                assert!(barrier.is_none());
                assert_eq!(receiver.len(), 1);
                assert_eq!(control.len(), 1);
                assert_eq!(queue.max_count - queue.count.available_permits(), 1);
            }
        }
    }

    #[tokio::test]
    async fn log_groups_bound_work_even_with_larger_admission_limits() {
        let directory = tempfile::tempdir().unwrap();
        let mut store = reference(&directory, "group");
        let options = EngineOptions {
            submission_queue_count: 128,
            assigned_backlog_count: 128,
            execution_bytes: u64::MAX,
            ..Default::default()
        };
        let (queue, mut receiver, _receipts) =
            queued(&options, vec![transaction(tx! { return 1; }.unwrap()); 70]).await;
        let mut head = queued_head(&store, &mut receiver, &options);
        let (_, control) = mpsc::channel(1);
        let (status, _) = watch::channel(initial_status(options.clone()));
        let pool = workers::start(options.workers, Arc::default()).unwrap();
        let mut schedule = Schedule {
            frontier: 0,
            entries: VecDeque::new(),
            assigned_bytes: 0,
            reserved_bytes: 0,
        };
        register(
            &mut store,
            &mut schedule,
            &mut head,
            &mut receiver,
            &control,
            &pool,
            &options,
            &status,
            &mut None,
        )
        .unwrap();
        assert_eq!(store.manifest().durable_sequence, 64);
        assert_eq!(store.manifest().generation, 2);
        assert_eq!(schedule.entries.len(), 64);
        assert!(head.is_none());
        assert_eq!(receiver.len(), 6);
        assert_eq!(queue.max_count - queue.count.available_permits(), 6);
    }

    #[tokio::test]
    async fn grouped_publication_failure_never_dispatches_and_worker_failure_replays_whole_group() {
        for publication_failure in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let store = reference(&directory, "group");
            let options = EngineOptions {
                workers: 1,
                ..Default::default()
            };
            let (queue, receiver, mut receipts) = queued(
                &options,
                vec![
                    transaction(tx! { return 1; }.unwrap()),
                    transaction(tx! { return 2; }.unwrap()),
                    transaction(tx! { return 3; }.unwrap()),
                    table(),
                ],
            )
            .await;
            let path = store.directory().to_owned();
            if publication_failure {
                std::fs::create_dir(path.join("manifest.pending")).unwrap();
            }
            let (_, control) = mpsc::channel(1);
            let (status, observed) = watch::channel(initial_status(options.clone()));
            let hooks = Arc::new(workers::test_support::Hooks::default());
            hooks.actions.lock().unwrap().insert(1, Action::Error);
            let pool = workers::start(options.workers, hooks.clone()).unwrap();
            let writer_queue = queue.clone();
            thread::spawn(move || {
                run(
                    store,
                    receiver,
                    control,
                    writer_queue,
                    options,
                    status,
                    pool,
                )
            })
            .join()
            .unwrap();
            assert!(observed.borrow().poisoned);
            assert_eq!(observed.borrow().checkpoint, 0);
            assert_eq!(
                observed.borrow().durable_frontier,
                if publication_failure { 0 } else { 3 }
            );
            assert_eq!(
                *hooks.started.lock().unwrap(),
                if publication_failure { vec![] } else { vec![1] }
            );
            assert!(matches!(
                receipts.pop().unwrap().await.unwrap(),
                Err(Error::Closed)
            ));
            for (index, receipt) in receipts.into_iter().enumerate() {
                assert!(
                    matches!(receipt.await.unwrap(), Err(Error::Uncertain { sequence: Some(n), .. }) if n == index as u64 + 1)
                );
            }
            assert_eq!(queue.count.available_permits(), queue.max_count);
            if publication_failure {
                std::fs::remove_dir(path.join("manifest.pending")).unwrap();
            }
            let mut reopened = storage::open(path).unwrap();
            assert_eq!(reopened.manifest().checkpoint_sequence, 0);
            assert_eq!(
                reopened.manifest().generation,
                if publication_failure { 1 } else { 2 }
            );
            engine::recover(&mut reopened).unwrap();
            let expected = if publication_failure { 0 } else { 3 };
            assert_eq!(reopened.manifest().checkpoint_sequence, expected);
            assert_eq!(
                entries(&reopened, TreeId::Outcomes).len(),
                expected as usize
            );
            assert_eq!(
                engine::commit(&mut reopened, &transaction(tx! { return 4; }.unwrap()))
                    .unwrap()
                    .sequence,
                expected + 1
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn production_workers_wait_for_all_writers_and_publish_only_prefixes() {
        let (_directory, database) = fixture(EngineOptions {
            workers: 3,
            execution_window: 8,
            assigned_backlog_count: 9,
            ..Default::default()
        })
        .await;
        enqueue(&database, table()).await.await.unwrap().unwrap();
        enqueue(
            &database,
            transaction(tx! { tables { data: u64 => u64 = 1 } data[1] = 0; data[2] = 5; }.unwrap()),
        )
        .await
        .await
        .unwrap()
        .unwrap();
        let (old, cursor) = db::snapshot_and_cursor(&database, db::CursorKind::Resolved, "prefix")
            .await
            .unwrap();
        let gate = Arc::new(Gate::default());
        database
            .hooks
            .actions
            .lock()
            .unwrap()
            .insert(3, Action::Gate(gate.clone()));
        let intermediate = Arc::new(Gate::default());
        database
            .hooks
            .actions
            .lock()
            .unwrap()
            .insert(8, Action::Gate(intermediate.clone()));
        let commands = [
            transaction(tx! { tables { data: u64 => u64 = 1 } data[1] = 1; }.unwrap()),
            transaction(tx! { tables { data: u64 => u64 = 1 } data[1] = 2; }.unwrap()),
            broad(transaction(
                tx! { tables { data: u64 => u64 = 1 } if false { data[1] = 3; } }.unwrap(),
            )),
            broad(transaction(
                tx! { tables { data: u64 => u64 = 1 } data[1] = 4; require(false); }.unwrap(),
            )),
            transaction(tx! { tables { data: u64 => u64 = 1 } return data[1]; }.unwrap()),
            transaction(tx! { tables { data: u64 => u64 = 1 } data[2] = 77; }.unwrap()),
            transaction(tx! { tables { data: u64 => u64 = 1 } return data[2]; }.unwrap()),
            transaction(tx! { tables { data: u64 => u64 = 1 } data[2] = 88; }.unwrap()),
            transaction(tx! { return 42_u64; }.unwrap()),
        ];
        let mut receipts = Vec::new();
        for command in commands {
            receipts.push(enqueue(&database, command).await);
        }
        wait_status(&database, |s| {
            s.durable_frontier == 11 && s.resolved_above_frontier == 4
        })
        .await;
        assert!(database.hooks.started.lock().unwrap().contains(&10));
        assert!(!database.hooks.started.lock().unwrap().contains(&9));
        // N=10 is installed before N=8. The dependent at N=9 must capture
        // the root after N=8 installs, then ignore N=10 using its sequence
        // bound.
        intermediate.release();
        let status = wait_status(&database, |s| {
            s.durable_frontier == 11 && s.resolved_above_frontier == 6
        })
        .await;
        assert_eq!((status.visibility_frontier, status.checkpoint), (2, 2));
        assert_eq!(status.oldest_unresolved, Some(3));
        assert_eq!(status.dependency_waiting, 1);
        assert_eq!(status.assigned_count, 9);
        assert!(status.reserved_execution_bytes <= status.options.execution_bytes);
        let started = database.hooks.started.lock().unwrap().clone();
        assert!(started.contains(&3) && started.contains(&4) && started.contains(&9));
        assert!(!started.contains(&7) && !started.contains(&11));
        // These requests bypass transaction admission while a real worker is
        // held.
        let prefix = db::snapshot(&database).await.unwrap();
        assert_eq!(prefix.sequence(), 2);
        assert_eq!(
            db::get(&prefix, 1, &Value::U64(1)).unwrap(),
            Some(Value::U64(0))
        );
        assert_eq!(
            db::get(&prefix, 1, &Value::U64(2)).unwrap(),
            Some(Value::U64(5))
        );
        let batch = db::read_feed(
            &database,
            &cursor,
            old.watermark(),
            db::BatchLimits::default(),
        )
        .await
        .unwrap();
        assert_eq!(batch.end_inclusive, 2);
        let (_, live_cursor) =
            db::snapshot_and_cursor(&database, db::CursorKind::Logical, "during-hole")
                .await
                .unwrap();
        assert_eq!(
            db::reopen_cursor(&database, &live_cursor)
                .await
                .unwrap()
                .baseline,
            2
        );
        assert!(matches!(
            db::acknowledge_cursor(
                &database,
                &cursor,
                db::Watermark::new(database.database_id(), 4).unwrap()
            )
            .await,
            Err(Error::InvalidInput(_))
        ));
        assert_eq!(db::retention_status(&database).await.unwrap().history, 2);
        gate.release();
        let mut results = Vec::new();
        for receipt in receipts {
            results.push(receipt.await.unwrap().unwrap());
        }
        assert_eq!(value(results[4].clone()), Value::U64(2));
        assert_eq!(value(results[6].clone()), Value::U64(77));
        assert!(matches!(results[3].outcome, Outcome::Aborted(_)));
        let latest = db::snapshot(&database).await.unwrap();
        assert_eq!(latest.sequence(), 11);
        assert_eq!(
            db::get(&latest, 1, &Value::U64(1)).unwrap(),
            Some(Value::U64(2))
        );
        assert_eq!(
            db::get(&latest, 1, &Value::U64(2)).unwrap(),
            Some(Value::U64(88))
        );
        assert_eq!(
            db::get(&old, 1, &Value::U64(1)).unwrap(),
            Some(Value::U64(0))
        );
        let batch = db::read_feed(
            &database,
            &cursor,
            old.watermark(),
            db::BatchLimits::default(),
        )
        .await
        .unwrap();
        let db::FeedRecords::Resolved(records) = batch.records else {
            unreachable!()
        };
        assert_eq!(
            records
                .iter()
                .map(|r| r.outcome.clone())
                .collect::<Vec<_>>(),
            results
                .iter()
                .map(|r| r.outcome.clone())
                .collect::<Vec<_>>()
        );
        db::close(&database).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reservations_bound_assigned_backlog_and_queue_without_starving_oldest() {
        let directory = tempfile::tempdir().unwrap();
        let store = reference(&directory, "reservation");
        let mut options = EngineOptions {
            workers: 2,
            submission_queue_count: 2,
            assigned_backlog_count: 8,
            ..Default::default()
        };
        let command = transaction(tx! { return 42_u64; }.unwrap());
        let required = prepare(
            &store,
            &mut command.clone(),
            &options,
            store.manifest().durable_sequence,
            store.manifest().durable_digest,
        )
        .unwrap()
        .unwrap()
        .reservation;
        options.execution_bytes = required * 2;
        options.submission_queue_bytes = budget::input_bytes(&command).unwrap() as usize * 2;
        let (_directory, database) = fixture(options).await;
        let gate = Arc::new(Gate::default());
        database
            .hooks
            .actions
            .lock()
            .unwrap()
            .insert(1, Action::Gate(gate.clone()));
        let mut receipts = Vec::new();
        for _ in 0..4 {
            receipts.push(enqueue(&database, command.clone()).await);
        }
        let status = wait_status(&database, |s| {
            s.prepared_head_bytes == required
                && s.resolved_above_frontier == 1
                && s.queued_count == 2
        })
        .await;
        assert_eq!(status.durable_frontier, 2);
        assert_eq!(status.reserved_execution_bytes, required * 2);
        assert_eq!(status.assigned_count, 2);
        assert_eq!(
            status.queued_bytes,
            status.options.submission_queue_bytes as u64
        );
        let mut fifth = pin!(db::execute(
            &database,
            tx! { return 42_u64; }.unwrap(),
            crate::Limits::default()
        ));
        assert!(matches!(
            fifth.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
        assert_eq!(db::snapshot(&database).await.unwrap().sequence(), 0);
        gate.release();
        for receipt in receipts {
            assert_eq!(value(receipt.await.unwrap().unwrap()), Value::U64(42));
        }
        assert_eq!(fifth.await.unwrap().sequence, 5);
        db::close(&database).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn admin_barriers_validate_queued_work_against_resulting_metadata() {
        let (_directory, database) = fixture(EngineOptions::default()).await;
        enqueue(&database, table()).await.await.unwrap().unwrap();
        let gate = Arc::new(Gate::default());
        database
            .hooks
            .actions
            .lock()
            .unwrap()
            .insert(2, Action::Gate(gate.clone()));
        let first = enqueue(&database, transaction(tx! { return 1; }.unwrap())).await;
        let drop_table = enqueue(
            &database,
            record::Command::Catalogue(CatalogueOperation::Drop { table: 1 }),
        )
        .await;
        let stale = enqueue(
            &database,
            transaction(tx! { tables { data: u64 => u64 = 1 } data[1] = 1; }.unwrap()),
        )
        .await;
        let limits = enqueue(
            &database,
            record::Command::Limits(
                crate::Limits {
                    program_bytes: 0,
                    ..Default::default()
                }
                .try_into()
                .unwrap(),
            ),
        )
        .await;
        let stale_policy = enqueue(&database, transaction(tx! { return 42; }.unwrap())).await;
        let restore = enqueue(
            &database,
            record::Command::Limits(crate::Limits::default().try_into().unwrap()),
        )
        .await;
        let last = enqueue(&database, transaction(tx! { return 42; }.unwrap())).await;
        let status = wait_status(&database, |s| s.administrative_barrier == Some(3)).await;
        assert_eq!(status.durable_frontier, 2);
        assert_eq!(db::snapshot(&database).await.unwrap().sequence(), 1);
        gate.release();
        assert_eq!(first.await.unwrap().unwrap().sequence, 2);
        assert_eq!(drop_table.await.unwrap().unwrap().sequence, 3);
        assert!(matches!(stale.await.unwrap(), Err(Error::Rejected(_))));
        assert_eq!(limits.await.unwrap().unwrap().sequence, 4);
        assert!(matches!(
            stale_policy.await.unwrap(),
            Err(Error::Rejected(_))
        ));
        assert_eq!(restore.await.unwrap().unwrap().sequence, 5);
        assert_eq!(last.await.unwrap().unwrap().sequence, 6);
        db::close(&database).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn worker_panics_and_system_errors_poison_and_replay_ignores_process_limits() {
        for fault in [Action::Panic, Action::Error] {
            let (directory, database) = fixture(EngineOptions {
                workers: 2,
                ..Default::default()
            })
            .await;
            enqueue(&database, table()).await.await.unwrap().unwrap();
            let gate = Arc::new(Gate::default());
            database
                .hooks
                .actions
                .lock()
                .unwrap()
                .extend([(2, Action::Gate(gate.clone())), (3, fault)]);
            let first = enqueue(
                &database,
                transaction(tx! { tables { data: u64 => u64 = 1 } data[1] = 10; }.unwrap()),
            )
            .await;
            let failed = enqueue(
                &database,
                transaction(tx! { tables { data: u64 => u64 = 1 } data[2] = 20; }.unwrap()),
            )
            .await;
            let status = wait_status(&database, |s| s.poisoned).await;
            assert_eq!(status.checkpoint, 1);
            assert_eq!(status.durable_frontier, 3);
            assert!(matches!(
                first.await.unwrap(),
                Err(Error::Uncertain {
                    sequence: Some(2),
                    ..
                })
            ));
            assert!(matches!(
                failed.await.unwrap(),
                Err(Error::Uncertain {
                    sequence: Some(3),
                    ..
                })
            ));
            assert!(matches!(
                db::execute(
                    &database,
                    tx! { return 0; }.unwrap(),
                    crate::Limits::default()
                )
                .await,
                Err(Error::Closed)
            ));
            gate.release();
            let _ = db::close(&database).await;
            let database = db::open_with_options(
                directory.path().join("db"),
                EngineOptions {
                    workers: 1,
                    execution_window: 1,
                    execution_bytes: 1,
                    preparation_bytes: 1,
                    assigned_backlog_bytes: 1,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            let snapshot = db::snapshot(&database).await.unwrap();
            assert_eq!(snapshot.sequence(), 3);
            assert_eq!(
                db::get(&snapshot, 1, &Value::U64(1)).unwrap(),
                Some(Value::U64(10))
            );
            assert_eq!(
                db::get(&snapshot, 1, &Value::U64(2)).unwrap(),
                Some(Value::U64(20))
            );
            assert!(matches!(
                db::execute(
                    &database,
                    tx! { return 42; }.unwrap(),
                    crate::Limits::default()
                )
                .await,
                Err(Error::OperationalLimit { .. })
            ));
            assert_eq!(db::snapshot(&database).await.unwrap().sequence(), 3);
            db::close(&database).await.unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn close_waits_for_gated_workers_and_dependents_then_rejects_unread_controls() {
        let (directory, database) = fixture(EngineOptions {
            workers: 2,
            ..Default::default()
        })
        .await;
        enqueue(&database, table()).await.await.unwrap().unwrap();
        enqueue(
            &database,
            transaction(tx! { tables { data: u64 => u64 = 1 } data[1] = 10; }.unwrap()),
        )
        .await
        .await
        .unwrap()
        .unwrap();
        let old = db::snapshot(&database).await.unwrap();
        let mut scan = db::scan(&old, 1, Unbounded, Unbounded).unwrap();
        let gate = Arc::new(Gate::default());
        database
            .hooks
            .actions
            .lock()
            .unwrap()
            .insert(3, Action::Gate(gate.clone()));
        let increment = transaction(
            tx! { tables { data: u64 => u64 = 1 } data[1] += 1; return data[1]; }.unwrap(),
        );
        let mut first = enqueue(&database, increment.clone()).await;
        let mut dependent = enqueue(&database, increment).await;
        wait_status(&database, |s| {
            s.durable_frontier == 4 && s.dependency_waiting == 1
        })
        .await;

        let mut closing = pin!(db::close(&database));
        assert!(matches!(
            closing
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
        database.sender.closed().await;
        let mut repeated = pin!(db::close(&database));
        assert!(matches!(
            repeated
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
        assert!(matches!(
            first.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert!(matches!(
            dependent.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert_eq!(
            db::get(&old, 1, &Value::U64(1)).unwrap(),
            Some(Value::U64(10))
        );
        assert!(matches!(
            storage::open(directory.path().join("db")),
            Err(storage::Error::Locked)
        ));

        // Reserved sends can arrive after Receiver::close. Hold them until the
        // shutdown decision so neither control can race into normal execution.
        let snapshot_slot = database.control.reserve().await.unwrap();
        let cursor_slot = database.control.reserve().await.unwrap();
        gate.release();
        assert_eq!(value(first.await.unwrap().unwrap()), Value::U64(11));
        assert_eq!(value(dependent.await.unwrap().unwrap()), Value::U64(12));
        let status = wait_status(&database, |s| s.closed).await;
        assert!(!status.poisoned);
        assert_eq!(
            (
                status.visibility_frontier,
                status.checkpoint,
                status.durable_frontier
            ),
            (4, 4, 4)
        );
        assert!(matches!(
            closing
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
        let (reply, snapshot_reply) = oneshot::channel();
        snapshot_slot.send(Control::Snapshot { reply });
        let (reply, cursor_reply) = oneshot::channel();
        cursor_slot.send(Control::Retention {
            operation: cursor::Operation::SnapshotTail {
                kind: db::CursorKind::Resolved,
                label: "not-executed".into(),
            },
            reply,
        });
        assert!(matches!(snapshot_reply.await.unwrap(), Err(Error::Closed)));
        assert!(matches!(cursor_reply.await.unwrap(), Err(Error::Closed)));
        closing.await.unwrap();
        assert!(matches!(repeated.await, Err(Error::Closed)));
        assert!(matches!(db::close(&database).await, Err(Error::Closed)));
        assert!(old.is_revoked());
        assert!(matches!(scan.next(), Some(Err(Error::SnapshotRevoked))));

        let reopened = db::open(directory.path().join("db")).await.unwrap();
        let latest = db::snapshot(&reopened).await.unwrap();
        assert_eq!(latest.sequence(), 4);
        assert_eq!(
            db::get(&latest, 1, &Value::U64(1)).unwrap(),
            Some(Value::U64(12))
        );
        assert!(db::list_cursors(&reopened).await.unwrap().is_empty());
        db::close(&reopened).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropping_all_handles_drains_gated_assigned_and_unassigned_work() {
        let (directory, database) = fixture(EngineOptions {
            workers: 2,
            assigned_backlog_count: 2,
            submission_queue_count: 4,
            ..Default::default()
        })
        .await;
        enqueue(&database, table()).await.await.unwrap().unwrap();
        enqueue(
            &database,
            transaction(tx! { tables { data: u64 => u64 = 1 } data[1] = 10; }.unwrap()),
        )
        .await
        .await
        .unwrap()
        .unwrap();
        let old = db::snapshot(&database).await.unwrap();
        let mut scan = db::scan(&old, 1, Unbounded, Unbounded).unwrap();
        let gate = Arc::new(Gate::default());
        database
            .hooks
            .actions
            .lock()
            .unwrap()
            .insert(3, Action::Gate(gate.clone()));
        let increment = transaction(
            tx! { tables { data: u64 => u64 = 1 } data[1] += 1; return data[1]; }.unwrap(),
        );
        let mut receipts = Vec::new();
        for _ in 0..4 {
            receipts.push(enqueue(&database, increment.clone()).await);
        }
        wait_status(&database, |s| {
            s.durable_frontier == 4
                && s.dependency_waiting == 1
                && s.prepared_head_bytes > 0
                && s.queued_count == 2
        })
        .await;
        let clone = database.clone();
        let mut stopped = database.stopped.clone();
        drop(database);
        drop(clone);
        let mut shutdown = pin!(stopped.changed());
        assert!(matches!(
            shutdown
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
        assert!(!old.is_revoked());
        assert_eq!(
            db::get(&old, 1, &Value::U64(1)).unwrap(),
            Some(Value::U64(10))
        );
        assert!(matches!(
            storage::open(directory.path().join("db")),
            Err(storage::Error::Locked)
        ));
        gate.release();
        for (index, receipt) in receipts.into_iter().enumerate() {
            let receipt = receipt.await.unwrap().unwrap();
            assert_eq!(receipt.sequence, index as u64 + 3);
            assert_eq!(value(receipt), Value::U64(index as u64 + 11));
        }
        assert!(shutdown.await.is_err());
        assert!(old.is_revoked());
        assert!(matches!(scan.next(), Some(Err(Error::SnapshotRevoked))));
        let reopened = db::open(directory.path().join("db")).await.unwrap();
        let latest = db::snapshot(&reopened).await.unwrap();
        assert_eq!(latest.sequence(), 6);
        assert_eq!(
            db::get(&latest, 1, &Value::U64(1)).unwrap(),
            Some(Value::U64(14))
        );
        db::close(&reopened).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn checkpoint_failure_after_install_keeps_f_and_rejects_unread_controls_before_replay() {
        let (directory, database) = fixture(EngineOptions {
            workers: 2,
            assigned_backlog_count: 2,
            ..Default::default()
        })
        .await;
        let path = directory.path().join("db");
        enqueue(&database, table()).await.await.unwrap().unwrap();
        enqueue(
            &database,
            transaction(tx! { tables { data: u64 => u64 = 1 } data[1] = 10; }.unwrap()),
        )
        .await
        .await
        .unwrap()
        .unwrap();
        let old = db::snapshot(&database).await.unwrap();
        let gate = Arc::new(Gate::default());
        database
            .hooks
            .actions
            .lock()
            .unwrap()
            .insert(3, Action::Gate(gate.clone()));
        let increment = transaction(
            tx! { tables { data: u64 => u64 = 1 } data[1] += 1; return data[1]; }.unwrap(),
        );
        let first = enqueue(&database, increment.clone()).await;
        let dependent = enqueue(&database, increment.clone()).await;
        let queued = enqueue(&database, increment).await;
        wait_status(&database, |s| {
            s.durable_frontier == 4 && s.dependency_waiting == 1 && s.prepared_head_bytes > 0
        })
        .await;
        let current = std::fs::read(path.join("CURRENT")).unwrap();
        let snapshot_slot = database.control.reserve().await.unwrap();
        let cursor_slot = database.control.reserve().await.unwrap();

        // Only obstruct unpublished output. The selected manifest, committed
        // log and pages remain intact, and both appends have already published
        // D.
        let unpublished = path.join("manifest.pending");
        std::fs::create_dir(&unpublished).unwrap();
        gate.release();
        let status = wait_status(&database, |s| s.poisoned).await;
        assert_eq!(
            (
                status.visibility_frontier,
                status.checkpoint,
                status.durable_frontier
            ),
            (3, 2, 4)
        );
        assert_eq!(status.oldest_unresolved, Some(4));
        assert!(status.last_error.is_some());
        assert!(database.hooks.started.lock().unwrap().contains(&3));
        assert!(!database.hooks.started.lock().unwrap().contains(&4));
        assert_eq!(std::fs::read(path.join("CURRENT")).unwrap(), current);
        assert!(matches!(
            first.await.unwrap(),
            Err(Error::Uncertain {
                sequence: Some(3),
                ..
            })
        ));
        assert!(matches!(
            dependent.await.unwrap(),
            Err(Error::Uncertain {
                sequence: Some(4),
                ..
            })
        ));
        assert!(matches!(queued.await.unwrap(), Err(Error::Closed)));
        let mut closing = pin!(db::close(&database));
        assert!(matches!(
            closing
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
        let (reply, snapshot_reply) = oneshot::channel();
        snapshot_slot.send(Control::Snapshot { reply });
        let (reply, cursor_reply) = oneshot::channel();
        cursor_slot.send(Control::Retention {
            operation: cursor::Operation::SnapshotTail {
                kind: db::CursorKind::Resolved,
                label: "not-executed".into(),
            },
            reply,
        });
        assert!(matches!(snapshot_reply.await.unwrap(), Err(Error::Closed)));
        assert!(matches!(cursor_reply.await.unwrap(), Err(Error::Closed)));
        assert!(matches!(closing.await, Err(Error::Closed)));
        assert!(old.is_revoked());
        std::fs::remove_dir(unpublished).unwrap();

        let store = storage::open(&path).unwrap();
        assert_eq!(
            (
                store.manifest().checkpoint_sequence,
                store.manifest().durable_sequence
            ),
            (2, 4)
        );
        assert!(
            vm::read_outcome(&storage::view(&store), 3)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            rows(&store, 1, 2),
            vec![(1_u64.to_be_bytes().to_vec(), 10_u64.to_le_bytes().to_vec())]
        );
        drop(store);
        let reopened = db::open(path).await.unwrap();
        let latest = db::snapshot(&reopened).await.unwrap();
        assert_eq!(latest.sequence(), 4);
        assert_eq!(
            db::get(&latest, 1, &Value::U64(1)).unwrap(),
            Some(Value::U64(12))
        );
        assert!(db::list_cursors(&reopened).await.unwrap().is_empty());
        db::close(&reopened).await.unwrap();
    }

    fn entries(
        store: &Store,
        tree: TreeId,
    ) -> Vec<storage::Entry> {
        storage::scan(&storage::view(store), tree, Unbounded, Unbounded)
            .unwrap()
            .collect::<storage::Result<_>>()
            .unwrap()
    }

    fn rows(
        store: &Store,
        table: u64,
        sequence: u64,
    ) -> Vec<storage::Entry> {
        storage::mvcc::scan(&storage::view(store), table, sequence, Unbounded, Unbounded)
            .unwrap()
            .collect::<storage::Result<_>>()
            .unwrap()
    }

    #[test]
    fn window_arithmetic_cannot_wrap_at_reserved_sequence_boundary() {
        assert!(in_window(
            u64::MAX - 2,
            u64::MAX - 3,
            u64::MAX - 2,
            u64::MAX
        ));
        assert!(!in_window(4, 2, 3, u64::MAX));
        assert!(!in_window(5, 2, 5, 2));
        assert!(!in_window(2, 2, 5, 2));
    }

    #[test]
    fn every_legal_small_schedule_matches_serial_outcomes_and_visible_states() {
        let cases = [
            vec![
                transaction(tx! { tables { data: u64 => u64 = 1 } data[1] = 1; }.unwrap()),
                broad(transaction(tx! { tables { data: u64 => u64 = 1 } if false { data[1] = 2; } }.unwrap())),
                broad(transaction(tx! { tables { data: u64 => u64 = 1 } data[1] = 3; abort(); }.unwrap())),
                transaction(tx! { tables { data: u64 => u64 = 1 } return data[1]; }.unwrap()),
            ],
            vec![
                transaction(tx! { tables { data: u64 => u64 = 1 } data[1] = 1; }.unwrap()),
                transaction(tx! { tables { data: u64 => u64 = 1 } data[1] = 2; }.unwrap()),
                transaction(tx! { tables { data: u64 => u64 = 1 } return data[1]; }.unwrap()),
                transaction(tx! { tables { data: u64 => u64 = 1 } delete(data[1]); }.unwrap()),
            ],
            vec![
                transaction(tx! { tables { data: u64 => u64 = 1 } insert(data[1], 1); }.unwrap()),
                transaction(tx! { tables { data: u64 => u64 = 1 } data[2] = data[1]; }.unwrap()),
                transaction(tx! { tables { data: u64 => u64 = 1 } return scan_bounded(data, unbounded, unbounded, 0, 5, 100); }.unwrap()),
                transaction(tx! { tables { data: u64 => u64 = 1 } data[3] = 9; }.unwrap()),
            ],
        ];
        let mut schedules = 0;
        for commands in cases {
            let directory = tempfile::tempdir().unwrap();
            let mut serial = reference(&directory, "serial");
            record::execute(&mut serial, 1, [1; 32], &table()).unwrap();
            let mut manifests = Vec::new();
            for (index, command) in commands.iter().enumerate() {
                let prepared =
                    record::prepare(&storage::view(&serial), index as u64 + 2, command).unwrap();
                let record::Command::Transaction { manifest, .. } = prepared else {
                    unreachable!()
                };
                manifests.push(manifest.unwrap());
                record::execute(
                    &mut serial,
                    index as u64 + 2,
                    [index as u8 + 2; 32],
                    command,
                )
                .unwrap();
            }
            let mut permutations = Vec::new();
            legal_orders(&manifests, &mut Vec::new(), &mut permutations);
            for order in permutations {
                let mut parallel = reference(&directory, &format!("schedule-{schedules}"));
                schedules += 1;
                record::execute(&mut parallel, 1, [1; 32], &table()).unwrap();
                let mut resolved = vec![false; commands.len()];
                for index in order {
                    let sequence = index as u64 + 2;
                    let record::Command::Transaction {
                        transaction,
                        claims,
                        manifest,
                    } = &commands[index]
                    else {
                        unreachable!()
                    };
                    let view = storage::view(&parallel);
                    let prepared = vm::prepare_transaction(
                        &view,
                        sequence,
                        transaction,
                        claims,
                        manifest.as_ref(),
                    )
                    .unwrap();
                    let outcome = vm::interpret_prepared(&view, &prepared).unwrap();
                    vm::install(&mut parallel, sequence, [sequence as u8; 32], 1, &outcome)
                        .unwrap();
                    resolved[index] = true;
                    let frontier = 1 + resolved.iter().take_while(|&&done| done).count() as u64;
                    assert_eq!(rows(&parallel, 1, frontier), rows(&serial, 1, frontier));
                    assert_eq!(
                        vm::read_outcome(&storage::view(&parallel), sequence).unwrap(),
                        vm::read_outcome(&storage::view(&serial), sequence).unwrap()
                    );
                }
                for tree in [
                    TreeId::State,
                    TreeId::Catalogue,
                    TreeId::Policy,
                    TreeId::Outcomes,
                ] {
                    assert_eq!(entries(&parallel, tree), entries(&serial, tree));
                }
            }
        }
        assert!(schedules >= 15);
    }

    fn legal_orders(
        manifests: &[AccessManifest],
        prefix: &mut Vec<usize>,
        orders: &mut Vec<Vec<usize>>,
    ) {
        if prefix.len() == manifests.len() {
            orders.push(prefix.clone());
            return;
        }
        for next in 0..manifests.len() {
            if prefix.contains(&next)
                || (0..next).any(|prior| {
                    !prefix.contains(&prior)
                        && overlaps(
                            &manifests[prior].writes().cloned().collect::<Vec<_>>(),
                            &manifests[next],
                        )
                })
            {
                continue;
            }
            prefix.push(next);
            legal_orders(manifests, prefix, orders);
            prefix.pop();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn randomized_mixed_live_logs_match_serial_at_observed_visible_prefixes() {
        for (seed, workers, window) in [(17_u64, 1, 1), (91, 2, 3), (831, 4, 16)] {
            let (directory, database) = fixture(EngineOptions {
                workers,
                execution_window: window,
                ..Default::default()
            })
            .await;
            let mut serial = reference(&directory, "serial");
            let mut random = seed;
            let mut id = 1;
            let mut ids = vec![id];
            let mut claims = crate::Limits::default();
            let mut commands = vec![
                table(),
                transaction(
                    tx! {
                        tables { data: u64 => u64 = id }
                        data[1] = 1; data[2] = 2; data[3] = 3; data[4] = 4;
                    }
                    .unwrap(),
                ),
            ];
            for index in 0..32 {
                if index == 6 {
                    commands.push(record::Command::Catalogue(CatalogueOperation::Rename {
                        table: id,
                        name: "renamed".into(),
                    }));
                    commands.push(record::Command::Catalogue(CatalogueOperation::Create {
                        name: "renamed".into(),
                        key: Type::U64,
                        value: Type::U64,
                    }));
                }
                if index == 12 || index == 18 {
                    claims.overlay_bytes = if index == 12 {
                        0
                    } else {
                        crate::Limits::default().overlay_bytes
                    };
                    commands.push(record::Command::Limits(claims.try_into().unwrap()));
                }
                if index == 24 {
                    commands.push(record::Command::Catalogue(CatalogueOperation::Drop {
                        table: id,
                    }));
                    id = commands.len() as u64 + 1;
                    ids.push(id);
                    commands.push(record::Command::Catalogue(CatalogueOperation::Create {
                        name: "renamed".into(),
                        key: Type::U64,
                        value: Type::U64,
                    }));
                }
                random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
                let key = random % 4 + 1;
                let program = match (random >> 32) % 9 {
                    0 => tx! { captures { key: u64 = key } tables { data: u64 => u64 = id } data[key] = 9; },
                    1 => tx! { captures { key: u64 = key } tables { data: u64 => u64 = id } data[key] += 1; return data[key]; },
                    2 => tx! { captures { key: u64 = key } tables { data: u64 => u64 = id } if false { data[key] = 99; } },
                    3 => tx! { captures { key: u64 = key } tables { data: u64 => u64 = id } data[key] = 99; require(false, 17); },
                    4 => tx! { captures { key: u64 = key } tables { data: u64 => u64 = id } delete(data[key]); },
                    5 => tx! { captures { key: u64 = key } tables { data: u64 => u64 = id } insert(data[key], 1); },
                    6 => tx! { tables { data: u64 => u64 = id } let target = data[1] % 4 + 1; data[target] = 7; return data[target]; },
                    7 => tx! { tables { data: u64 => u64 = id } return scan_bounded(data, unbounded, unbounded, 0, 10, 200); },
                    _ => tx! { captures { key: u64 = key } tables { data: u64 => u64 = id } return exists(data[key]); },
                }.unwrap();
                commands.push(record::Command::Transaction {
                    transaction: program,
                    claims: claims.try_into().unwrap(),
                    manifest: None,
                });
            }
            let expected = commands
                .iter()
                .map(|command| engine::commit(&mut serial, command).unwrap())
                .collect::<Vec<_>>();
            let gate = Arc::new(Gate::default());
            database
                .hooks
                .actions
                .lock()
                .unwrap()
                .insert(2, Action::Gate(gate.clone()));
            for sequence in 3..=commands.len() as u64 {
                database.hooks.actions.lock().unwrap().insert(
                    sequence,
                    Action::Delay(Duration::from_millis((sequence * seed) % 4)),
                );
            }
            let mut receipts = Vec::new();
            for command in commands {
                receipts.push(enqueue(&database, command).await);
            }
            wait_status(&database, |s| s.oldest_unresolved == Some(2)).await;
            assert_prefix(&database, &serial, &ids).await;
            gate.release();
            let deadline = Instant::now() + Duration::from_secs(15);
            while db::status(&database).visibility_frontier < expected.len() as u64 {
                assert_prefix(&database, &serial, &ids).await;
                assert!(Instant::now() < deadline, "randomized execution stalled");
                tokio::task::yield_now().await;
            }
            assert_prefix(&database, &serial, &ids).await;
            for (receipt, expected) in receipts.into_iter().zip(expected) {
                assert_eq!(receipt.await.unwrap().unwrap(), expected);
            }
            db::close(&database).await.unwrap();
            let live = storage::open(directory.path().join("db")).unwrap();
            for tree in [
                TreeId::State,
                TreeId::Catalogue,
                TreeId::Policy,
                TreeId::Outcomes,
            ] {
                assert_eq!(
                    entries(&live, tree),
                    entries(&serial, tree),
                    "seed {seed}, {tree:?}"
                );
            }
            drop(live);
            let reopened = db::open_with_options(
                directory.path().join("db"),
                EngineOptions {
                    workers: 1,
                    execution_window: 1,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            assert_prefix(&reopened, &serial, &ids).await;
            db::close(&reopened).await.unwrap();
        }
    }

    async fn assert_prefix(
        database: &db::Database,
        serial: &Store,
        ids: &[u64],
    ) {
        let snapshot = db::snapshot(database).await.unwrap();
        let expected = ids
            .iter()
            .filter_map(|&id| {
                vm::catalogue_version(&storage::view(serial), id, snapshot.sequence()).unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(db::catalogue(&snapshot).unwrap(), expected);
        for catalogue in expected.into_iter().filter(|c| c.live) {
            let expected = rows(serial, catalogue.table.id, snapshot.sequence())
                .into_iter()
                .map(|(key, value)| {
                    (
                        Value::U64(u64::from_be_bytes(key.try_into().unwrap())),
                        Value::U64(u64::from_le_bytes(value.try_into().unwrap())),
                    )
                })
                .collect::<Vec<_>>();
            let actual = db::scan(&snapshot, catalogue.table.id, Unbounded, Unbounded)
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .unwrap();
            assert_eq!(actual, expected, "visible prefix {}", snapshot.sequence());
        }
    }

    #[test]
    fn cursor_publication_at_c_less_than_f_filters_roots_and_preserves_replay_metadata() {
        let directory = tempfile::tempdir().unwrap();
        let mut store = reference(&directory, "db");
        engine::commit(&mut store, &table()).unwrap();
        let commands = [
            transaction(tx! { tables { data: u64 => u64 = 1 } data[1] = 10; }.unwrap()),
            transaction(tx! { tables { data: u64 => u64 = 1 } data[1] += 1; }.unwrap()),
            transaction(tx! { tables { data: u64 => u64 = 1 } data[2] = 20; }.unwrap()),
        ];
        for (index, command) in commands.into_iter().enumerate() {
            let prepared = prepare(
                &store,
                &mut command.clone(),
                &EngineOptions::default(),
                store.manifest().durable_sequence,
                store.manifest().durable_digest,
            )
            .unwrap()
            .unwrap();
            let sequence = index as u64 + 2;
            let digest = engine::append(&mut store, sequence, &prepared.bytes).unwrap();
            if sequence != 3 {
                let outcome =
                    vm::interpret_prepared(&storage::view(&store), &prepared.transaction).unwrap();
                vm::install(&mut store, sequence, digest, 1, &outcome).unwrap();
            }
        }
        let mut registry = snapshot::Registry::default();
        let response = cursor::handle(
            &mut store,
            &mut registry,
            cursor::Operation::SnapshotTail {
                kind: db::CursorKind::Logical,
                label: "post-checkpoint".into(),
            },
            2,
        )
        .unwrap();
        let cursor::Response::SnapshotTail(snapshot, token) = response else {
            unreachable!()
        };
        assert_eq!(
            (
                store.manifest().checkpoint_sequence,
                store.manifest().durable_sequence
            ),
            (1, 4)
        );
        assert_eq!(cursor::lookup(&store, &token).unwrap().unwrap().baseline, 2);
        let checkpoint = storage::checkpoint_view(&store);
        assert!(vm::read_outcome(&checkpoint, 2).unwrap().is_none());
        assert!(vm::read_outcome(&checkpoint, 4).unwrap().is_none());
        assert!(
            storage::mvcc::get(&checkpoint, 1, &1_u64.to_be_bytes(), 4)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            db::get(&snapshot, 1, &Value::U64(1)).unwrap(),
            Some(Value::U64(10))
        );
        assert_eq!(db::get(&snapshot, 1, &Value::U64(2)).unwrap(), None);
        assert_eq!(
            cursor::retention_floors(&store, &mut registry, 2).unwrap(),
            (2, 2)
        );
        let resolved = cursor::handle(
            &mut store,
            &mut registry,
            cursor::Operation::Checkout {
                baseline: db::Watermark::new([1; 16], 1).unwrap(),
                kind: db::CursorKind::Resolved,
                label: "feed".into(),
            },
            2,
        )
        .unwrap();
        let cursor::Response::Token(resolved) = resolved else {
            unreachable!()
        };
        let batch = super::super::feed::batch(
            &store,
            &resolved,
            db::Watermark::new([1; 16], 1).unwrap(),
            db::BatchLimits::default(),
            2,
        )
        .unwrap();
        assert_eq!(batch.end_inclusive, 2);
        cursor::handle(
            &mut store,
            &mut registry,
            cursor::Operation::Ack {
                token: resolved,
                watermark: db::Watermark::new([1; 16], 2).unwrap(),
            },
            2,
        )
        .unwrap();
        drop(checkpoint);
        snapshot::revoke_all(&mut registry);
        drop(store);
        let mut recovered = storage::open(directory.path().join("db")).unwrap();
        assert_eq!(recovered.manifest().checkpoint_sequence, 1);
        assert_eq!(
            cursor::lookup(&recovered, &token)
                .unwrap()
                .unwrap()
                .baseline,
            2
        );
        engine::recover(&mut recovered).unwrap();
        assert_eq!(recovered.manifest().checkpoint_sequence, 4);
        assert_eq!(
            rows(&recovered, 1, 4),
            vec![
                (1_u64.to_be_bytes().to_vec(), 11_u64.to_le_bytes().to_vec()),
                (2_u64.to_be_bytes().to_vec(), 20_u64.to_le_bytes().to_vec()),
            ]
        );
        assert_eq!(
            cursor::lookup(&recovered, &resolved)
                .unwrap()
                .unwrap()
                .baseline,
            2
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn assigned_count_and_log_byte_limits_each_stop_sequence_assignment() {
        let directory = tempfile::tempdir().unwrap();
        let store = reference(&directory, "sizes");
        let command = transaction(tx! { return 42_u64; }.unwrap());
        let length = prepare(
            &store,
            &mut command.clone(),
            &EngineOptions::default(),
            store.manifest().durable_sequence,
            store.manifest().durable_digest,
        )
        .unwrap()
        .unwrap()
        .bytes
        .len() as u64;
        for options in [
            EngineOptions {
                assigned_backlog_count: 1,
                ..Default::default()
            },
            EngineOptions {
                assigned_backlog_bytes: length,
                ..Default::default()
            },
        ] {
            let (_directory, database) = fixture(options).await;
            let gate = Arc::new(Gate::default());
            database
                .hooks
                .actions
                .lock()
                .unwrap()
                .insert(1, Action::Gate(gate.clone()));
            let first = enqueue(&database, command.clone()).await;
            let second = enqueue(&database, command.clone()).await;
            let status = wait_status(&database, |s| {
                s.prepared_head_bytes > 0 && s.durable_frontier == 1
            })
            .await;
            assert_eq!(status.assigned_count, 1);
            assert_eq!(status.assigned_bytes, length);
            gate.release();
            assert_eq!(first.await.unwrap().unwrap().sequence, 1);
            assert_eq!(second.await.unwrap().unwrap().sequence, 2);
            db::close(&database).await.unwrap();
        }
    }

    #[tokio::test]
    async fn a_record_that_cannot_fit_is_rejected_operationally_without_sequence_or_abort() {
        for (resource, options) in [
            (
                "submission_queue_bytes",
                EngineOptions {
                    submission_queue_bytes: 1,
                    ..Default::default()
                },
            ),
            (
                "assigned_backlog_bytes",
                EngineOptions {
                    assigned_backlog_bytes: 1,
                    ..Default::default()
                },
            ),
            (
                "execution_bytes",
                EngineOptions {
                    execution_bytes: 1,
                    ..Default::default()
                },
            ),
            (
                "preparation_bytes",
                EngineOptions {
                    preparation_bytes: 1,
                    ..Default::default()
                },
            ),
        ] {
            let (_directory, database) = fixture(options).await;
            let error = db::execute(
                &database,
                tx! { return 42; }.unwrap(),
                crate::Limits::default(),
            )
            .await
            .unwrap_err();
            assert!(
                matches!(error, Error::OperationalLimit { resource: found, required, limit: 1 } if found == resource && required > 1)
            );
            assert_eq!(db::snapshot(&database).await.unwrap().sequence(), 0);
            assert_eq!(db::status(&database).durable_frontier, 0);
            assert!(!db::status(&database).poisoned);
            db::close(&database).await.unwrap();
        }
    }
}
