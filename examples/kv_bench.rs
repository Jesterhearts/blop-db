//! KV comparison with independent clients. See BENCHMARKS.md for the contract.

#[cfg(any(unix, windows))]
fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    bench::run()
}

#[cfg(not(any(unix, windows)))]
fn main() {
    eprintln!("The KV benchmark requires Unix or Windows.");
    std::process::exit(1);
}

#[cfg(any(unix, windows))]
mod bench {
    use std::hint::black_box;
    use std::path::Path;
    use std::path::PathBuf;
    use std::sync::Barrier;
    use std::time::Duration;
    use std::time::Instant;

    use blop_db::Limits;
    use blop_db::Transaction;
    use blop_db::database;
    use blop_db::storage;
    use blop_db::tx;
    use blop_db::vm;
    use blop_db::vm::CatalogueOperation;
    use blop_db::vm::Outcome;
    use blop_db::vm::Type;
    use blop_db::vm::Value;
    use redb::ReadableDatabase;
    use rusqlite::params;

    type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
    const TABLE: redb::TableDefinition<&[u8], &[u8]> = redb::TableDefinition::new("kv");
    const VALUE_BYTES: usize = 128;
    const SEED: u64 = 0x426c_6f70_4b56_3031;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Mode {
        Buffered,
        Durable,
    }

    impl Mode {
        fn name(self) -> &'static str {
            match self {
                Self::Buffered => "buffered",
                Self::Durable => "durable",
            }
        }
    }

    struct Config {
        keys: usize,
        reads: usize,
        repeats: usize,
        directory: PathBuf,
        modes: Vec<Mode>,
        concurrency: Concurrency,
    }

    #[derive(Clone, Copy, Debug)]
    struct Concurrency {
        clients: usize,
        workers: usize,
    }

    fn config(args: impl IntoIterator<Item = String>) -> Result<Option<Config>> {
        let mut config = Config {
            keys: 1_000,
            reads: 100_000,
            repeats: 3,
            directory: std::env::temp_dir(),
            modes: vec![Mode::Buffered, Mode::Durable],
            concurrency: Concurrency {
                clients: 1,
                workers: database::EngineOptions::default().workers,
            },
        };
        let mut args = args.into_iter();
        while let Some(argument) = args.next() {
            if argument == "--help" {
                println!(
                    "Usage: cargo run --release --example kv_bench -- [options]\n--keys N       \
                     distinct keys and writes per phase (default 1000)\n--reads N      cached \
                     random reads (default 100000)\n--repeats N    measured fresh databases \
                     (default 3)\n--dir PATH     existing parent for temporary databases (default \
                     OS temp)\n--mode MODE    all, buffered or durable (default all)\n--clients N    \
                     independent clients, 1..256 (default 1; buffered requires 1)\n--workers N    \
                     blop interpreter workers, 1..256 (default EngineOptions workers)"
                );
                return Ok(None);
            }
            let value = args.next().ok_or("each option requires a value")?;
            match argument.as_str() {
                "--keys" => config.keys = value.parse()?,
                "--reads" => config.reads = value.parse()?,
                "--repeats" => config.repeats = value.parse()?,
                "--dir" => config.directory = value.into(),
                "--clients" => config.concurrency.clients = value.parse()?,
                "--workers" => config.concurrency.workers = value.parse()?,
                "--mode" => {
                    config.modes = match value.as_str() {
                        "all" => vec![Mode::Buffered, Mode::Durable],
                        "buffered" => vec![Mode::Buffered],
                        "durable" => vec![Mode::Durable],
                        _ => return Err("mode must be all, buffered or durable".into()),
                    };
                }
                _ => return Err(format!("unknown option: {argument}").into()),
            }
        }
        if config.keys == 0 || config.reads == 0 || config.repeats == 0 {
            return Err("keys, reads and repeats must be positive".into());
        }
        if !(1..=256).contains(&config.concurrency.clients)
            || !(1..=256).contains(&config.concurrency.workers)
        {
            return Err("clients and workers must be between 1 and 256".into());
        }
        if config.concurrency.clients > config.keys || config.concurrency.clients > config.reads {
            return Err("keys and reads must each be at least the client count".into());
        }
        if config.concurrency.clients != 1 && config.modes.contains(&Mode::Buffered) {
            return Err("multiple clients require --mode durable; buffered blop is serial".into());
        }
        if u64::try_from(config.keys)?
            .checked_mul(2)
            .and_then(|n| n.checked_add(2))
            .is_none()
        {
            return Err("too many keys for the transaction sequence space".into());
        }
        if !config.directory.is_dir() {
            return Err("--dir must name an existing directory".into());
        }
        Ok(Some(config))
    }

    struct Workload {
        keys: Vec<[u8; 8]>,
        initial: Vec<[u8; VALUE_BYTES]>,
        updated: Vec<[u8; VALUE_BYTES]>,
        inserts: Vec<usize>,
        updates: Vec<usize>,
        reads: Vec<usize>,
    }

    fn random(state: &mut u64) -> u64 {
        *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = *state;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }

    fn workload(
        keys: usize,
        reads: usize,
    ) -> Workload {
        let mut state = SEED;
        let mut inserts: Vec<_> = (0..keys).collect();
        let mut updates = inserts.clone();
        for order in [&mut inserts, &mut updates] {
            for index in (1..keys).rev() {
                order.swap(index, (random(&mut state) % (index as u64 + 1)) as usize);
            }
        }
        let keys: Vec<_> = (0..keys as u64).map(u64::to_be_bytes).collect();
        let values = |marker| {
            keys.iter()
                .map(|key| {
                    let mut value = [marker; VALUE_BYTES];
                    value[..8].copy_from_slice(key);
                    value
                })
                .collect()
        };
        Workload {
            initial: values(1),
            updated: values(2),
            reads: (0..reads)
                .map(|_| (random(&mut state) % keys.len() as u64) as usize)
                .collect(),
            keys,
            inserts,
            updates,
        }
    }

    fn report(
        engine: &str,
        mode: Mode,
        operation: &str,
        trial: usize,
        count: usize,
        elapsed: Duration,
        concurrency: Concurrency,
    ) {
        let elapsed = elapsed.as_secs_f64();
        if trial != 0 {
            println!(
                "{engine},{},{operation},{trial},{count},{elapsed:.6},{:.1},{:.3},{},{}",
                mode.name(),
                count as f64 / elapsed,
                elapsed * 1_000_000.0 / count as f64,
                concurrency.clients,
                if engine == "blop" && mode == Mode::Durable {
                    concurrency.workers
                } else {
                    0
                },
            );
        }
    }

    fn measure_clients<S: Send>(
        states: Vec<S>,
        operation: impl Fn(usize, &mut S) -> Result<()> + Sync,
    ) -> Result<Duration> {
        let ready = Barrier::new(states.len() + 1);
        let start = Barrier::new(states.len() + 1);
        std::thread::scope(|scope| {
            let ready = &ready;
            let start = &start;
            let operation = &operation;
            let handles: Vec<_> = states
                .into_iter()
                .enumerate()
                .map(|(client, mut state)| {
                    scope.spawn(move || {
                        ready.wait();
                        start.wait();
                        let result = operation(client, &mut state);
                        (Instant::now(), result, state)
                    })
                })
                .collect();
            ready.wait();
            let begin = Instant::now();
            start.wait();
            // Join every client even when one fails; never report a partial
            // workload.
            let results: Vec<_> = handles.into_iter().map(|handle| handle.join()).collect();
            let mut end = begin;
            for result in results {
                let (finished, result, _state) = result.map_err(|_| "benchmark client panicked")?;
                result?;
                end = end.max(finished);
            }
            Ok(end.duration_since(begin))
        })
    }

    fn success(outcome: Outcome) -> Result<Value> {
        match outcome {
            Outcome::Success { value, .. } => Ok(value),
            Outcome::Aborted(abort) => {
                Err(format!("benchmark transaction aborted: {abort:?}").into())
            }
        }
    }

    fn put(
        key: u64,
        value: &[u8],
    ) -> Result<Transaction> {
        Ok(tx! {
            captures { key: u64 = key, value: bytes<128> = value }
            tables { kv: u64 => bytes<128> = 1 }
            kv[key] = value;
        }?)
    }

    fn get(key: u64) -> Result<Transaction> {
        Ok(tx! {
            captures { key: u64 = key }
            tables { kv: u64 => bytes<128> = 1 }
            return kv[key];
        }?)
    }

    fn blop_read(
        view: &storage::View,
        sequence: u64,
        claims: &storage::LimitPolicy,
        key: usize,
    ) -> Result<Value> {
        let transaction = get(key as u64)?;
        success(vm::interpret(
            view,
            sequence,
            transaction.program_bytes(),
            transaction.argument_bytes(),
            claims,
        )?)
    }

    async fn blop(
        path: &Path,
        data: &Workload,
        mode: Mode,
        trial: usize,
        concurrency: Concurrency,
    ) -> Result<()> {
        let claims = Limits::default();
        let policy = storage::LimitPolicy::try_from(claims)?;
        let catalogue = CatalogueOperation::Create {
            name: "kv".into(),
            key: Type::U64,
            value: Type::Bytes(VALUE_BYTES as u32),
        };
        let sequence = 2 * data.keys.len() as u64 + 2;
        let store = match mode {
            Mode::Buffered => {
                let mut store = storage::create(
                    path,
                    storage::Genesis {
                        database_id: [1; 16],
                        initial_policy: policy.clone(),
                    },
                    [2; 16],
                )?;
                // Fixture digests are permitted only in an isolated reference
                // store.
                assert_eq!(
                    success(vm::execute_catalogue(&mut store, 1, [1; 32], &catalogue)?)?,
                    Value::U64(1)
                );
                let mut next = 2_u64;
                for (operation, order, values) in [
                    ("insert", &data.inserts, &data.initial),
                    ("update", &data.updates, &data.updated),
                ] {
                    let start = Instant::now();
                    for &key in order {
                        let transaction = put(key as u64, &values[key])?;
                        black_box(success(vm::execute(
                            &mut store,
                            next,
                            [1; 32],
                            &transaction,
                            &policy,
                        )?)?);
                        next += 1;
                    }
                    report(
                        "blop",
                        mode,
                        operation,
                        trial,
                        order.len(),
                        start.elapsed(),
                        concurrency,
                    );
                }
                store
            }
            Mode::Durable => {
                let options = database::EngineOptions {
                    workers: concurrency.workers,
                    ..Default::default()
                };
                let db = database::create_with_options(
                    path,
                    database::CreateOptions::default(),
                    options.clone(),
                )
                .await?;
                assert_eq!(
                    success(database::execute_catalogue(&db, catalogue).await?.outcome)?,
                    Value::U64(1)
                );
                for (operation, order, values) in [
                    ("insert", &data.inserts, &data.initial),
                    ("update", &data.updates, &data.updated),
                ] {
                    let runtimes = (0..concurrency.clients)
                        .map(|_| tokio::runtime::Builder::new_current_thread().build())
                        .collect::<std::io::Result<Vec<_>>>()?;
                    let elapsed = measure_clients(runtimes.iter().collect(), |client, runtime| {
                        runtime.block_on(async {
                            for &key in order.iter().skip(client).step_by(concurrency.clients) {
                                let transaction = put(key as u64, &values[key])?;
                                black_box(success(
                                    database::execute(&db, transaction, claims).await?.outcome,
                                )?);
                            }
                            Ok(())
                        })
                    });
                    for runtime in runtimes {
                        runtime.shutdown_background();
                    }
                    report(
                        "blop",
                        mode,
                        operation,
                        trial,
                        order.len(),
                        elapsed?,
                        concurrency,
                    );
                }
                database::close(&db).await?;
                let db = database::open_with_options(path, options).await?;
                let snapshot = database::snapshot(&db).await?;
                assert_eq!(snapshot.sequence(), sequence - 1);
                for (key, expected) in data.updated.iter().enumerate() {
                    assert_eq!(
                        database::get(&snapshot, 1, &Value::U64(key as u64))?,
                        Some(Value::Bytes(expected.to_vec()))
                    );
                }
                let elapsed =
                    measure_clients(vec![&snapshot; concurrency.clients], |client, snapshot| {
                        for &key in data.reads.iter().skip(client).step_by(concurrency.clients) {
                            black_box(
                                database::get(snapshot, 1, &Value::U64(key as u64))?
                                    .ok_or("missing snapshot key")?,
                            );
                        }
                        Ok(())
                    })?;
                report(
                    "blop",
                    mode,
                    "get_snapshot",
                    trial,
                    data.reads.len(),
                    elapsed,
                    concurrency,
                );
                database::close(&db).await?;
                // Verify the persisted checkpoint without logging verification
                // reads.
                let store = storage::open(path)?;
                assert_eq!(store.manifest().checkpoint_sequence, sequence - 1);
                store
            }
        };
        let view = storage::view(&store);
        for (key, expected) in data.updated.iter().enumerate() {
            let expected = Value::Bytes(expected.to_vec());
            assert_eq!(blop_read(&view, sequence, &policy, key)?, expected);
            assert_eq!(
                storage::mvcc::get(&view, 1, &data.keys[key], sequence - 1)?,
                Some(expected.encode())
            );
        }
        if mode == Mode::Buffered {
            let start = Instant::now();
            for &key in &data.reads {
                black_box(blop_read(&view, sequence, &policy, key)?);
            }
            report(
                "blop",
                mode,
                "get_vm",
                trial,
                data.reads.len(),
                start.elapsed(),
                concurrency,
            );

            let start = Instant::now();
            for &key in &data.reads {
                black_box(
                    storage::mvcc::get(&view, 1, &data.keys[key], sequence - 1)?
                        .ok_or("missing MVCC key")?,
                );
            }
            report(
                "blop",
                mode,
                "get_mvcc",
                trial,
                data.reads.len(),
                start.elapsed(),
                concurrency,
            );
        }
        Ok(())
    }

    fn redb(
        path: &Path,
        data: &Workload,
        mode: Mode,
        trial: usize,
        concurrency: Concurrency,
    ) -> Result<()> {
        let mut db = redb::Database::builder()
            .set_cache_size(64 * 1024 * 1024)
            .create(path)?;
        let setup = db.begin_write()?;
        setup.open_table(TABLE)?;
        setup.commit()?;
        for (operation, order, values) in [
            ("insert", &data.inserts, &data.initial),
            ("update", &data.updates, &data.updated),
        ] {
            let elapsed = measure_clients(vec![&db; concurrency.clients], |client, db| {
                for &key in order.iter().skip(client).step_by(concurrency.clients) {
                    let mut transaction = db.begin_write()?;
                    transaction.set_durability(match mode {
                        Mode::Buffered => redb::Durability::None,
                        Mode::Durable => redb::Durability::Immediate,
                    })?;
                    {
                        let mut table = transaction.open_table(TABLE)?;
                        table.insert(data.keys[key].as_slice(), values[key].as_slice())?;
                    }
                    transaction.commit()?;
                }
                Ok(())
            })?;
            report(
                "redb",
                mode,
                operation,
                trial,
                order.len(),
                elapsed,
                concurrency,
            );
        }
        if mode == Mode::Durable {
            drop(db);
            db = redb::Database::builder()
                .set_cache_size(64 * 1024 * 1024)
                .open(path)?;
        }
        let snapshot = db.begin_read()?;
        let table = snapshot.open_table(TABLE)?;
        for (key, expected) in data.keys.iter().zip(&data.updated) {
            assert_eq!(
                table
                    .get(key.as_slice())?
                    .ok_or("missing redb key")?
                    .value(),
                expected
            );
        }
        let elapsed = measure_clients(vec![&table; concurrency.clients], |client, table| {
            for &key in data.reads.iter().skip(client).step_by(concurrency.clients) {
                black_box(
                    table
                        .get(data.keys[key].as_slice())?
                        .ok_or("missing redb key")?
                        .value()
                        .to_vec(),
                );
            }
            Ok(())
        })?;
        report(
            "redb",
            mode,
            "get",
            trial,
            data.reads.len(),
            elapsed,
            concurrency,
        );
        Ok(())
    }

    fn sqlite(
        path: &Path,
        data: &Workload,
        mode: Mode,
        trial: usize,
        concurrency: Concurrency,
    ) -> Result<()> {
        let db = rusqlite::Connection::open(path)?;
        db.execute_batch(
            "PRAGMA journal_mode = WAL; PRAGMA mmap_size = 0; PRAGMA cache_size = -65536;
            CREATE TABLE kv (key BLOB PRIMARY KEY, value BLOB NOT NULL) WITHOUT ROWID;",
        )?;
        db.close().map_err(|(_, error)| error)?;
        let connections = || -> Result<Vec<rusqlite::Connection>> {
            (0..concurrency.clients)
                .map(|_| {
                    let db = rusqlite::Connection::open(path)?;
                    db.busy_timeout(Duration::from_secs(60))?;
                    db.pragma_update(None, "mmap_size", 0)?;
                    db.pragma_update(None, "cache_size", -(65536 / concurrency.clients as i64))?;
                    db.pragma_update(
                        None,
                        "synchronous",
                        match mode {
                            Mode::Buffered => "OFF",
                            Mode::Durable => "FULL",
                        },
                    )?;
                    Ok(db)
                })
                .collect()
        };
        for (operation, order, values) in [
            ("insert", &data.inserts, &data.initial),
            ("update", &data.updates, &data.updated),
        ] {
            let elapsed = measure_clients(connections()?, |client, db| {
                let mut put = db.prepare(
                    "INSERT INTO kv (key, value) VALUES (?1, ?2)
                    ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                )?;
                for &key in order.iter().skip(client).step_by(concurrency.clients) {
                    // Each statement is its own autocommit transaction.
                    assert_eq!(
                        put.execute(params![data.keys[key].as_slice(), values[key].as_slice()])?,
                        1
                    );
                }
                Ok(())
            })?;
            report(
                "sqlite",
                mode,
                operation,
                trial,
                order.len(),
                elapsed,
                concurrency,
            );
        }
        let readers = connections()?;
        for db in &readers {
            db.execute_batch("BEGIN")?;
            let mut get = db.prepare_cached("SELECT value FROM kv WHERE key = ?1")?;
            for (key, expected) in data.keys.iter().zip(&data.updated) {
                let value: Vec<u8> = get.query_row([key.as_slice()], |row| row.get(0))?;
                assert_eq!(value, expected);
            }
        }
        let elapsed = measure_clients(readers, |client, db| {
            let mut get = db.prepare_cached("SELECT value FROM kv WHERE key = ?1")?;
            for &key in data.reads.iter().skip(client).step_by(concurrency.clients) {
                let value: Vec<u8> =
                    get.query_row([data.keys[key].as_slice()], |row| row.get(0))?;
                black_box(value);
            }
            Ok(())
        })?;
        report(
            "sqlite",
            mode,
            "get",
            trial,
            data.reads.len(),
            elapsed,
            concurrency,
        );
        Ok(())
    }

    async fn run_trial(
        parent: &Path,
        data: &Workload,
        mode: Mode,
        trial: usize,
        concurrency: Concurrency,
    ) -> Result<()> {
        // Rotate engine order between trials to reduce systematic order bias.
        for offset in 0..3 {
            let temporary = tempfile::Builder::new()
                .prefix("blop-kv-bench-")
                .tempdir_in(parent)?;
            let path = temporary.path().join("database");
            match (trial + offset) % 3 {
                0 => blop(&path, data, mode, trial, concurrency).await?,
                1 => redb(&path, data, mode, trial, concurrency)?,
                _ => sqlite(&path, data, mode, trial, concurrency)?,
            }
            temporary.close()?;
        }
        Ok(())
    }

    #[tokio::main(flavor = "current_thread")]
    pub async fn run() -> Result<()> {
        let Some(config) = config(std::env::args().skip(1))? else {
            return Ok(());
        };
        if cfg!(debug_assertions) {
            return Err("benchmark requires --release".into());
        }
        println!(
            "# keys={} value_bytes={VALUE_BYTES} reads={} repeats={} seed={SEED:#x}",
            config.keys, config.reads, config.repeats
        );
        println!(
            "# directory={} sqlite_version={}",
            config.directory.display(),
            rusqlite::version()
        );
        println!(
            "# clients={} blop_workers={}; one outstanding operation/client; one key/transaction; \
             buffered blop has no log or durability",
            config.concurrency.clients, config.concurrency.workers
        );
        println!(
            "# get_vm includes binding/validation; get_mvcc is a lower-level diagnostic, not a \
             public read API"
        );
        println!(
            "# discarded warm-up (at least 64 keys); full value verification before cached reads; \
             fresh databases per trial"
        );
        println!("# amortized_us is inverse aggregate throughput, NOT mean request latency");
        println!(
            "engine,mode,operation,trial,operations,seconds,ops_per_second,amortized_us,clients,\
             blop_workers"
        );
        let data = workload(config.keys, config.reads);
        let warmup = workload(
            64.max(config.concurrency.clients),
            128.max(config.concurrency.clients),
        );
        for mode in config.modes {
            run_trial(&config.directory, &warmup, mode, 0, config.concurrency).await?;
            for trial in 1..=config.repeats {
                run_trial(&config.directory, &data, mode, trial, config.concurrency).await?;
            }
        }
        Ok(())
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn workload_is_repeatable_and_updates_every_key() {
            let first = workload(100, 1_000);
            let second = workload(100, 1_000);
            assert_eq!(first.inserts, second.inserts);
            assert_eq!(first.updates, second.updates);
            assert_eq!(first.reads, second.reads);
            assert_ne!(first.inserts, first.updates);
            for mut order in [first.inserts, first.updates] {
                order.sort_unstable();
                assert_eq!(order, (0..100).collect::<Vec<_>>());
            }
            assert!(first.reads.iter().all(|&key| key < 100));
            assert_eq!(first.keys[42], 42_u64.to_be_bytes());
            assert_eq!(&first.initial[42][..8], &first.keys[42]);
            assert_eq!(&first.updated[42][..8], &first.keys[42]);
            assert_ne!(first.initial[42], first.updated[42]);
        }

        #[test]
        fn invalid_options_are_rejected() {
            for args in [
                vec!["--keys", "0"],
                vec!["--reads", "0"],
                vec!["--repeats", "0"],
                vec!["--keys", "no"],
                vec!["--mode", "no"],
                vec!["--unknown", "1"],
                vec!["--keys"],
                vec!["--clients", "0"],
                vec!["--clients", "257"],
                vec!["--workers", "0"],
                vec!["--workers", "257"],
                vec!["--clients", "2"],
                vec!["--mode", "durable", "--keys", "2", "--clients", "3"],
                vec!["--mode", "durable", "--reads", "2", "--clients", "3"],
            ] {
                assert!(config(args.into_iter().map(str::to_owned)).is_err());
            }
        }

        #[test]
        fn clients_visit_every_operation_once_and_propagate_failures() -> Result<()> {
            use std::sync::atomic::AtomicUsize;
            use std::sync::atomic::Ordering;

            let visits: Vec<_> = (0..11).map(|_| AtomicUsize::new(0)).collect();
            measure_clients(vec![(); 3], |client, ()| {
                for visited in visits.iter().skip(client).step_by(3) {
                    visited.fetch_add(1, Ordering::Relaxed);
                }
                Ok(())
            })?;
            assert!(visits.iter().all(|n| n.load(Ordering::Relaxed) == 1));
            let completed = AtomicUsize::new(0);
            let result = measure_clients(vec![(); 3], |client, ()| {
                completed.fetch_add(1, Ordering::Relaxed);
                if client == 1 {
                    return Err("client failed".into());
                }
                Ok(())
            });
            assert_eq!(result.unwrap_err().to_string(), "client failed");
            assert_eq!(completed.load(Ordering::Relaxed), 3);
            completed.store(0, Ordering::Relaxed);
            let result = measure_clients(vec![(); 3], |client, ()| {
                completed.fetch_add(1, Ordering::Relaxed);
                assert_ne!(client, 1, "test client panic");
                Ok(())
            });
            assert_eq!(result.unwrap_err().to_string(), "benchmark client panicked");
            assert_eq!(completed.load(Ordering::Relaxed), 3);
            Ok(())
        }

        #[tokio::test(flavor = "current_thread")]
        async fn engines_preserve_values_and_durable_checkpoints() -> Result<()> {
            let directory = tempfile::tempdir()?;
            let data = workload(8, 16);
            for mode in [Mode::Buffered, Mode::Durable] {
                run_trial(
                    directory.path(),
                    &data,
                    mode,
                    0,
                    Concurrency {
                        clients: 1,
                        workers: 1,
                    },
                )
                .await?;
            }
            run_trial(
                directory.path(),
                &data,
                Mode::Durable,
                0,
                Concurrency {
                    clients: 3,
                    workers: 2,
                },
            )
            .await?;
            assert_eq!(std::fs::read_dir(directory.path())?.count(), 0);
            Ok(())
        }
    }
}
