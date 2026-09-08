//! Single-client KV comparison. See BENCHMARKS.md for the measurement contract.

#[cfg(any(unix, windows))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
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

    type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
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
    }

    fn config(args: impl IntoIterator<Item = String>) -> Result<Option<Config>> {
        let mut config = Config {
            keys: 1_000,
            reads: 100_000,
            repeats: 3,
            directory: std::env::temp_dir(),
            modes: vec![Mode::Buffered, Mode::Durable],
        };
        let mut args = args.into_iter();
        while let Some(argument) = args.next() {
            if argument == "--help" {
                println!(
                    "Usage: cargo run --release --example kv_bench -- [options]\n--keys N       \
                     distinct keys and writes per phase (default 1000)\n--reads N      cached \
                     random reads (default 100000)\n--repeats N    measured fresh databases \
                     (default 3)\n--dir PATH     existing parent for temporary databases (default \
                     OS temp)\n--mode MODE    all, buffered or durable (default all)"
                );
                return Ok(None);
            }
            let value = args.next().ok_or("each option requires a value")?;
            match argument.as_str() {
                "--keys" => config.keys = value.parse()?,
                "--reads" => config.reads = value.parse()?,
                "--repeats" => config.repeats = value.parse()?,
                "--dir" => config.directory = value.into(),
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
        start: Instant,
    ) {
        let elapsed = start.elapsed().as_secs_f64();
        if trial != 0 {
            println!(
                "{engine},{},{operation},{trial},{count},{elapsed:.6},{:.1},{:.3}",
                mode.name(),
                count as f64 / elapsed,
                elapsed * 1_000_000.0 / count as f64,
            );
        }
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
                    report("blop", mode, operation, trial, order.len(), start);
                }
                store
            }
            Mode::Durable => {
                let db = database::create(path, database::CreateOptions::default()).await?;
                assert_eq!(
                    success(database::execute_catalogue(&db, catalogue).await?.outcome)?,
                    Value::U64(1)
                );
                for (operation, order, values) in [
                    ("insert", &data.inserts, &data.initial),
                    ("update", &data.updates, &data.updated),
                ] {
                    let start = Instant::now();
                    for &key in order {
                        let transaction = put(key as u64, &values[key])?;
                        black_box(success(
                            database::execute(&db, transaction, claims).await?.outcome,
                        )?);
                    }
                    report("blop", mode, operation, trial, order.len(), start);
                }
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
            report("blop", mode, "get_vm", trial, data.reads.len(), start);

            let start = Instant::now();
            for &key in &data.reads {
                black_box(
                    storage::mvcc::get(&view, 1, &data.keys[key], sequence - 1)?
                        .ok_or("missing MVCC key")?,
                );
            }
            report("blop", mode, "get_mvcc", trial, data.reads.len(), start);
        }
        Ok(())
    }

    fn redb(
        path: &Path,
        data: &Workload,
        mode: Mode,
        trial: usize,
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
            let start = Instant::now();
            for &key in order {
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
            report("redb", mode, operation, trial, order.len(), start);
        }
        if mode == Mode::Durable {
            drop(db);
            db = redb::Database::open(path)?;
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
        if mode == Mode::Buffered {
            let start = Instant::now();
            for &key in &data.reads {
                black_box(
                    table
                        .get(data.keys[key].as_slice())?
                        .ok_or("missing redb key")?
                        .value()
                        .to_vec(),
                );
            }
            report("redb", mode, "get", trial, data.reads.len(), start);
        }
        Ok(())
    }

    fn sqlite(
        path: &Path,
        data: &Workload,
        mode: Mode,
        trial: usize,
    ) -> Result<()> {
        let mut db = rusqlite::Connection::open(path)?;
        db.execute_batch(
            "PRAGMA journal_mode = WAL; PRAGMA mmap_size = 0; PRAGMA cache_size = -65536;
            CREATE TABLE kv (key BLOB PRIMARY KEY, value BLOB NOT NULL) WITHOUT ROWID;",
        )?;
        db.pragma_update(
            None,
            "synchronous",
            match mode {
                Mode::Buffered => "OFF",
                Mode::Durable => "FULL",
            },
        )?;
        {
            let mut put = db.prepare(
                "INSERT INTO kv (key, value) VALUES (?1, ?2)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            )?;
            for (operation, order, values) in [
                ("insert", &data.inserts, &data.initial),
                ("update", &data.updates, &data.updated),
            ] {
                let start = Instant::now();
                for &key in order {
                    // Each statement is its own autocommit transaction.
                    assert_eq!(
                        put.execute(params![data.keys[key].as_slice(), values[key].as_slice()])?,
                        1
                    );
                }
                report("sqlite", mode, operation, trial, order.len(), start);
            }
        }
        if mode == Mode::Durable {
            db.close().map_err(|(_, error)| error)?;
            db = rusqlite::Connection::open(path)?;
        }
        let snapshot = db.transaction()?;
        let mut get = snapshot.prepare("SELECT value FROM kv WHERE key = ?1")?;
        for (key, expected) in data.keys.iter().zip(&data.updated) {
            let value: Vec<u8> = get.query_row([key.as_slice()], |row| row.get(0))?;
            assert_eq!(value, expected);
        }
        if mode == Mode::Buffered {
            let start = Instant::now();
            for &key in &data.reads {
                let value: Vec<u8> =
                    get.query_row([data.keys[key].as_slice()], |row| row.get(0))?;
                black_box(value);
            }
            report("sqlite", mode, "get", trial, data.reads.len(), start);
        }
        Ok(())
    }

    async fn run_trial(
        parent: &Path,
        data: &Workload,
        mode: Mode,
        trial: usize,
    ) -> Result<()> {
        // Rotate engine order between trials to reduce systematic order bias.
        for offset in 0..3 {
            let temporary = tempfile::Builder::new()
                .prefix("blop-kv-bench-")
                .tempdir_in(parent)?;
            let path = temporary.path().join("database");
            match (trial + offset) % 3 {
                0 => blop(&path, data, mode, trial).await?,
                1 => redb(&path, data, mode, trial)?,
                _ => sqlite(&path, data, mode, trial)?,
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
        println!("# one client; one operation/transaction; buffered blop has no log or durability");
        println!(
            "# get_vm includes binding/validation; get_mvcc is a lower-level diagnostic, not a \
             public read API"
        );
        println!(
            "# discarded 64-key warm-up; full value verification before cached reads; fresh \
             databases per trial"
        );
        println!("engine,mode,operation,trial,operations,seconds,ops_per_second,mean_us");
        let data = workload(config.keys, config.reads);
        let warmup = workload(64, 128);
        for mode in config.modes {
            run_trial(&config.directory, &warmup, mode, 0).await?;
            for trial in 1..=config.repeats {
                run_trial(&config.directory, &data, mode, trial).await?;
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
            ] {
                assert!(config(args.into_iter().map(str::to_owned)).is_err());
            }
        }

        #[tokio::test(flavor = "current_thread")]
        async fn engines_preserve_values_and_durable_checkpoints() -> Result<()> {
            let directory = tempfile::tempdir()?;
            let data = workload(8, 16);
            for mode in [Mode::Buffered, Mode::Durable] {
                run_trial(directory.path(), &data, mode, 0).await?;
            }
            assert_eq!(std::fs::read_dir(directory.path())?.count(), 0);
            Ok(())
        }
    }
}
