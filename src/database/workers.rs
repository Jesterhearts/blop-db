//! Persistent interpreters. Only the coordinator owns mutable storage.

use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::thread::{
    self,
};

use crate::storage;
use crate::vm;

pub(super) struct Job {
    pub prepared: vm::PreparedTransaction,
    pub view: storage::View,
}

pub(super) struct Completion {
    pub worker: usize,
    pub sequence: u64,
    pub outcome: vm::Result<vm::Outcome>,
}

pub(super) struct Pool {
    senders: Vec<mpsc::SyncSender<Job>>,
    threads: Vec<JoinHandle<()>>,
    pub completed: tokio::sync::mpsc::Receiver<Completion>,
    pub poisoned: Arc<AtomicBool>,
    pub failure: Arc<Mutex<Option<String>>>,
    pub busy: Vec<bool>,
}

impl Drop for Pool {
    fn drop(&mut self) {
        self.senders.clear();
        // A failed coordinator must not leave a worker blocked sending output.
        self.completed.close();
        for worker in self.threads.drain(..) {
            let _ = worker.join();
        }
    }
}

pub(super) fn start(
    count: usize,
    #[cfg(test)] hooks: Arc<test_support::Hooks>,
) -> std::io::Result<Pool> {
    let (complete, completed) = tokio::sync::mpsc::channel(count);
    let mut pool = Pool {
        senders: Vec::new(),
        threads: Vec::new(),
        completed,
        poisoned: Arc::new(AtomicBool::new(false)),
        failure: Arc::default(),
        busy: vec![false; count],
    };
    for worker in 0..count {
        let (send, receive) = mpsc::sync_channel::<Job>(1);
        let complete = complete.clone();
        let poisoned = pool.poisoned.clone();
        let fault = pool.failure.clone();
        #[cfg(test)]
        let hooks = hooks.clone();
        let thread = thread::Builder::new()
            .name(format!("blop-db-vm-{worker}"))
            .spawn(move || {
                while let Ok(job) = receive.recv() {
                    let sequence = job.prepared.sequence();
                    let outcome = catch_unwind(AssertUnwindSafe(|| {
                        #[cfg(test)]
                        test_support::before(&hooks, sequence)?;
                        if poisoned.load(Ordering::Acquire) {
                            return Err(failure("worker pool requires recovery"));
                        }
                        vm::interpret_prepared(&job.view, &job.prepared)
                    }))
                    .unwrap_or_else(|_| Err(failure("transaction worker panicked")));
                    // Drop the pinned roots and decoded program before
                    // returning the worker slot or
                    // permitting its lifetime reservation to retire.
                    drop(job);
                    let failed = outcome.is_err();
                    if let Err(error) = &outcome {
                        let mut fault = fault.lock().unwrap_or_else(|error| error.into_inner());
                        fault.get_or_insert_with(|| format!("record {sequence}: {error}"));
                        poisoned.store(true, Ordering::Release);
                    }
                    if complete
                        .blocking_send(Completion {
                            worker,
                            sequence,
                            outcome,
                        })
                        .is_err()
                        || failed
                    {
                        break;
                    }
                }
            })?;
        pool.senders.push(send);
        pool.threads.push(thread);
    }
    Ok(pool)
}

pub(super) fn dispatch(
    pool: &mut Pool,
    worker: usize,
    job: Job,
) -> vm::Result<()> {
    if pool.poisoned.load(Ordering::Acquire) {
        return Err(failure("worker pool requires recovery"));
    }
    pool.senders[worker]
        .send(job)
        .map_err(|_| failure("transaction worker disconnected"))?;
    pool.busy[worker] = true;
    Ok(())
}

pub(super) fn failure(message: impl Into<String>) -> vm::Error {
    vm::Error::Storage(storage::Error::Io(std::io::Error::other(message.into())))
}

#[cfg(test)]
pub(super) mod test_support {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::Condvar;
    use std::sync::Mutex;
    use std::time::Duration;

    #[derive(Default, Debug)]
    pub struct Hooks {
        pub actions: Mutex<BTreeMap<u64, Action>>,
        pub started: Mutex<Vec<u64>>,
        pub changed: Condvar,
    }

    #[derive(Clone, Debug)]
    pub enum Action {
        Gate(Arc<Gate>),
        Panic,
        Error,
        Delay(Duration),
    }

    #[derive(Default, Debug)]
    pub struct Gate {
        released: Mutex<bool>,
        changed: Condvar,
    }

    impl Gate {
        pub fn release(&self) {
            *self.released.lock().unwrap() = true;
            self.changed.notify_all();
        }
    }

    pub fn before(
        hooks: &Hooks,
        sequence: u64,
    ) -> crate::vm::Result<()> {
        hooks.started.lock().unwrap().push(sequence);
        hooks.changed.notify_all();
        let action = hooks.actions.lock().unwrap().get(&sequence).cloned();
        match action {
            Some(Action::Gate(gate)) => {
                let released = gate.released.lock().unwrap();
                let (_released, timed) = gate
                    .changed
                    .wait_timeout_while(released, Duration::from_secs(20), |v| !*v)
                    .unwrap();
                assert!(!timed.timed_out(), "test failed to release worker gate");
            }
            Some(Action::Panic) => panic!("injected worker panic"),
            Some(Action::Error) => return Err(super::failure("injected worker I/O failure")),
            Some(Action::Delay(duration)) => std::thread::sleep(duration),
            None => {}
        }
        Ok(())
    }
}
