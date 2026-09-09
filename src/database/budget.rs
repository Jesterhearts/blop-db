//! Count and byte admission permits, held until rejection or durable
//! assignment.

use std::sync::Arc;

use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

use super::Error;
use super::Result;
use super::record::Command;
use crate::vm;

/// Process settings, not persisted semantic policy. Reservations are
/// conservative accounting units, not an RSS or disk-space limit. Retained
/// visible history, storage page traversal and OS/allocator overhead are
/// separate. Preparation has its own reservation and can coexist with assigned
/// execution reservations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EngineOptions {
    pub workers: usize,
    pub execution_window: u64,
    /// Checkpoint after this many newly visible records. Receipts before then
    /// are recovered from the durable log. Use 1 to checkpoint every visible
    /// prefix before releasing its receipts. Must be between 1 and 4096.
    pub checkpoint_interval: u64,
    pub submission_queue_count: usize,
    pub submission_queue_bytes: usize,
    pub assigned_backlog_count: usize,
    pub assigned_backlog_bytes: u64,
    /// Full lifetime reservations, retained through visible receipts.
    pub execution_bytes: u64,
    /// Bounds decoding, access analysis and the retained prepared queue head.
    pub preparation_bytes: u64,
}

impl Default for EngineOptions {
    fn default() -> Self {
        Self {
            workers: std::thread::available_parallelism().map_or(2, |n| n.get().clamp(2, 4)),
            execution_window: 64,
            checkpoint_interval: 64,
            submission_queue_count: 64,
            submission_queue_bytes: 64 * 1024 * 1024,
            assigned_backlog_count: 64,
            assigned_backlog_bytes: 64 * 1024 * 1024,
            execution_bytes: 512 * 1024 * 1024,
            preparation_bytes: 512 * 1024 * 1024,
        }
    }
}

pub(super) fn validate(options: &EngineOptions) -> Result<()> {
    if !(1..=256).contains(&options.workers)
        || options.execution_window == 0
        || !(1..=4096).contains(&options.checkpoint_interval)
        || options.submission_queue_count == 0
        || options.submission_queue_count > Semaphore::MAX_PERMITS
        || options.submission_queue_bytes == 0
        || options.submission_queue_bytes > (u32::MAX as usize).min(Semaphore::MAX_PERMITS)
        || options.assigned_backlog_count == 0
        || options.assigned_backlog_count > u32::MAX as usize
        || options.assigned_backlog_bytes == 0
        || options.execution_bytes == 0
        || options.preparation_bytes == 0
    {
        return Err(Error::InvalidInput("invalid operational engine limits"));
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub(super) struct Queue {
    pub import: Arc<Semaphore>,
    pub count: Arc<Semaphore>,
    pub bytes: Arc<Semaphore>,
    pub max_count: usize,
    pub max_bytes: usize,
}

impl Queue {
    pub fn new(options: &EngineOptions) -> Self {
        Self {
            import: Arc::new(Semaphore::new(1)),
            count: Arc::new(Semaphore::new(options.submission_queue_count)),
            bytes: Arc::new(Semaphore::new(options.submission_queue_bytes)),
            max_count: options.submission_queue_count,
            max_bytes: options.submission_queue_bytes,
        }
    }
}

pub(super) struct Permit {
    _count: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
}

pub(super) async fn reserve(
    queue: &Queue,
    command: &Command,
) -> Result<Permit> {
    let bytes = input_bytes(command)?;
    reserve_batch(queue, 1, bytes).await
}

pub(super) async fn reserve_batch(
    queue: &Queue,
    count: usize,
    bytes: u64,
) -> Result<Permit> {
    if count > queue.max_count {
        return Err(Error::OperationalLimit {
            resource: "submission_queue_count",
            required: count as u64,
            limit: queue.max_count as u64,
        });
    }
    if bytes > queue.max_bytes as u64 {
        return Err(Error::OperationalLimit {
            resource: "submission_queue_bytes",
            required: bytes,
            limit: queue.max_bytes as u64,
        });
    }
    let count = queue
        .count
        .clone()
        .acquire_many_owned(count as u32)
        .await
        .map_err(|_| Error::Closed)?;
    let bytes = queue
        .bytes
        .clone()
        .acquire_many_owned(bytes as u32)
        .await
        .map_err(|_| Error::Closed)?;
    Ok(Permit {
        _count: count,
        _bytes: bytes,
    })
}

pub(super) fn input_bytes(command: &Command) -> Result<u64> {
    let bytes = match command {
        Command::Transaction {
            transaction,
            manifest,
            ..
        } => {
            let scopes: u64 = manifest
                .iter()
                .flat_map(|m| m.entries())
                .map(|(scope, _)| {
                    128 + match scope {
                        vm::Scope::Key(_, key) => key.len() as u64 * 2,
                        _ => 0,
                    }
                })
                .sum();
            transaction.program_bytes().len() as u64
                + transaction.argument_bytes().len() as u64
                + scopes
        }
        Command::Catalogue(operation) => {
            vm::validate_catalogue(operation).map_err(super::engine::rejection)?;
            match operation {
                vm::CatalogueOperation::Create { name, key, value } => {
                    name.len() as u64
                        + (key.descriptor().len() + value.descriptor().len()) as u64 * 64
                }
                vm::CatalogueOperation::Rename { name, .. } => name.len() as u64,
                vm::CatalogueOperation::Drop { .. } => 8,
            }
        }
        Command::Limits(_) => 140,
    };
    Ok(bytes.saturating_add(512))
}

pub(super) fn close(queue: &Queue) {
    queue.import.close();
    queue.count.close();
    queue.bytes.close();
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::pin;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;

    use super::*;

    #[tokio::test]
    async fn byte_pressure_waits_independently_of_count_and_cancellation_releases_permits() {
        let command = Command::Transaction {
            transaction: crate::tx! { return 42; }.unwrap(),
            claims: crate::Limits::default().try_into().unwrap(),
            manifest: None,
        };
        let options = EngineOptions {
            submission_queue_count: 4,
            submission_queue_bytes: input_bytes(&command).unwrap() as usize,
            ..Default::default()
        };
        let queue = Queue::new(&options);
        let first = reserve(&queue, &command).await.unwrap();
        assert_eq!(queue.count.available_permits(), 3);
        assert_eq!(queue.bytes.available_permits(), 0);
        {
            let mut second = pin!(reserve(&queue, &command));
            assert!(matches!(
                second
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop())),
                Poll::Pending
            ));
            assert_eq!(queue.count.available_permits(), 2);
        }
        assert_eq!(queue.count.available_permits(), 3);
        drop(first);
        let second = reserve(&queue, &command).await.unwrap();
        close(&queue);
        assert!(matches!(
            reserve(&queue, &command).await,
            Err(Error::Closed)
        ));
        drop(second);
        assert_eq!(queue.count.available_permits(), 4);
        assert_eq!(
            queue.bytes.available_permits(),
            options.submission_queue_bytes
        );
    }

    #[test]
    fn invalid_options_are_rejected_before_constructing_channels_or_threads() {
        for options in [
            EngineOptions {
                workers: 0,
                ..Default::default()
            },
            EngineOptions {
                workers: 257,
                ..Default::default()
            },
            EngineOptions {
                execution_window: 0,
                ..Default::default()
            },
            EngineOptions {
                checkpoint_interval: 0,
                ..Default::default()
            },
            EngineOptions {
                checkpoint_interval: 4097,
                ..Default::default()
            },
            EngineOptions {
                submission_queue_count: 0,
                ..Default::default()
            },
            EngineOptions {
                submission_queue_bytes: usize::MAX,
                ..Default::default()
            },
            EngineOptions {
                assigned_backlog_count: 0,
                ..Default::default()
            },
            EngineOptions {
                assigned_backlog_bytes: 0,
                ..Default::default()
            },
            EngineOptions {
                execution_bytes: 0,
                ..Default::default()
            },
            EngineOptions {
                preparation_bytes: 0,
                ..Default::default()
            },
        ] {
            assert!(matches!(validate(&options), Err(Error::InvalidInput(_))));
        }
    }
}
