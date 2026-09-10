use std::future::Future;
use std::ops::Bound::Unbounded;
use std::pin::pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;

use sha2::Digest;
use sha2::Sha256;

use super::*;
use crate::database as db;
use crate::database::workers::test_support::Action;
use crate::database::workers::test_support::Gate;
use crate::tx;
use crate::vm::CatalogueOperation;
use crate::vm::Outcome;
use crate::vm::Type;
use crate::vm::Value;

async fn fixture() -> (tempfile::TempDir, Database, Database, CursorToken) {
    let directory = tempfile::tempdir().unwrap();
    let source = db::create(directory.path().join("source"), Default::default())
        .await
        .unwrap();
    db::execute_catalogue(
        &source,
        CatalogueOperation::Create {
            name: "data".into(),
            key: Type::U64,
            value: Type::U64,
        },
    )
    .await
    .unwrap();
    db::execute(
        &source,
        tx! { tables { data: u64 => u64 = 1 } data[1] = 10; }.unwrap(),
        Default::default(),
    )
    .await
    .unwrap();
    let (_, cursor) = db::snapshot_and_cursor(&source, CursorKind::Logical, "replication")
        .await
        .unwrap();
    db::backup(&source, directory.path().join("replica"))
        .await
        .unwrap();
    let replica = db::attach(
        directory.path().join("replica"),
        db::AttachMode::ReadOnlyReplica,
    )
    .await
    .unwrap();
    (directory, source, replica, cursor)
}

fn watermark(
    database: &Database,
    sequence: u64,
) -> Watermark {
    Watermark::new(database.database_id(), sequence).unwrap()
}

async fn suffix(
    source: &Database,
    cursor: &CursorToken,
    start: u64,
    count: usize,
) -> FeedBatch {
    read_logical_feed(
        source,
        cursor,
        watermark(source, start),
        BatchLimits {
            max_records: count,
            ..Default::default()
        },
    )
    .await
    .unwrap()
}

fn rows(snapshot: &db::Snapshot) -> Vec<(u64, Value, Value)> {
    let mut rows = Vec::new();
    for version in db::catalogue(snapshot)
        .unwrap()
        .into_iter()
        .filter(|v| v.live)
    {
        for row in db::scan(snapshot, version.table.id, Unbounded, Unbounded).unwrap() {
            let (key, value) = row.unwrap();
            rows.push((version.table.id, key, value));
        }
    }
    rows
}

async fn write(
    source: &Database,
    value: u64,
) {
    db::execute(
        source,
        tx! {
            captures { value: u64 = value }
            tables { data: u64 => u64 = 1 }
            data[1] = value;
        }
        .unwrap(),
        Default::default(),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn public_backup_import_matches_every_prefix_preserves_snapshots_and_rotates_segments() {
    let (directory, source, replica, cursor) = fixture().await;
    let (old, local_cursor) = db::snapshot_and_cursor(&replica, CursorKind::LogReplica, "old")
        .await
        .unwrap();
    db::backup(&replica, directory.path().join("grouped"))
        .await
        .unwrap();
    let mut expected = Vec::new();
    for n in 0..12 {
        let receipt = match n {
            0 => {
                db::execute(
                    &source,
                    tx! {
                        tables { data: u64 => u64 = 1 }
                        let key = data[1]; data[key + 1] = 42; return data[key + 1];
                    }
                    .unwrap(),
                    Default::default(),
                )
                .await
            }
            1 => {
                db::execute(
                    &source,
                    tx! {
                        tables { data: u64 => u64 = 1 }
                        data[1] = 99; require(false, 71);
                    }
                    .unwrap(),
                    Default::default(),
                )
                .await
            }
            2 => {
                db::execute_catalogue(
                    &source,
                    CatalogueOperation::Rename {
                        table: 1,
                        name: "renamed".into(),
                    },
                )
                .await
            }
            3 => {
                db::execute_catalogue(
                    &source,
                    CatalogueOperation::Create {
                        name: "extra".into(),
                        key: Type::U64,
                        value: Type::U64,
                    },
                )
                .await
            }
            4 => {
                db::execute_catalogue(
                    &source,
                    CatalogueOperation::Create {
                        name: "extra".into(),
                        key: Type::U64,
                        value: Type::U64,
                    },
                )
                .await
            }
            5 => {
                db::execute_limits(
                    &source,
                    crate::Limits {
                        writes: 0,
                        ..Default::default()
                    },
                )
                .await
            }
            6 => {
                db::execute(
                    &source,
                    tx! { tables { data: u64 => u64 = 1 } data[1] = 55; }.unwrap(),
                    crate::Limits {
                        writes: 0,
                        ..Default::default()
                    },
                )
                .await
            }
            7 => db::execute_limits(&source, Default::default()).await,
            8 => {
                db::execute(
                    &source,
                    tx! { tables { data: u64 => u64 = 1 } delete(data[11]); }.unwrap(),
                    Default::default(),
                )
                .await
            }
            9 => db::execute_catalogue(&source, CatalogueOperation::Drop { table: 6 }).await,
            10 => {
                db::execute(
                    &source,
                    tx! { return 100_u64; }.unwrap(),
                    Default::default(),
                )
                .await
            }
            _ => {
                db::execute(
                    &source,
                    tx! { tables { data: u64 => u64 = 1 } data[1] += 1; }.unwrap(),
                    Default::default(),
                )
                .await
            }
        }
        .unwrap();
        expected.push((receipt, db::snapshot(&source).await.unwrap()));
        if n % 3 == 0 {
            db::maintain(
                &source,
                db::MaintenanceOptions {
                    collect_history: false,
                    compact: false,
                    rotate_log: true,
                },
            )
            .await
            .unwrap();
        }
    }
    let mut after = old.watermark();
    for (receipt, snapshot) in &expected {
        let batch = suffix(&source, &cursor, after.sequence(), 1).await;
        assert_eq!(batch.end_inclusive, receipt.sequence);
        let bytes = batch.encode().unwrap();
        after = import_logical(&replica, &bytes).await.unwrap();
        let actual = db::snapshot(&replica).await.unwrap();
        assert_eq!(actual.watermark(), snapshot.watermark());
        assert_eq!(
            db::catalogue(&actual).unwrap(),
            db::catalogue(snapshot).unwrap()
        );
        assert_eq!(rows(&actual), rows(snapshot));
        let local = suffix(&replica, &local_cursor, receipt.sequence - 1, 1).await;
        assert_eq!(local.encode().unwrap(), bytes);
        assert_eq!(db::status(&replica).visibility_frontier, receipt.sequence);
        db::maintain(&replica, db::MaintenanceOptions::default())
            .await
            .unwrap();
        assert_eq!(rows(&old), vec![(1, Value::U64(1), Value::U64(10))]);
    }
    db::maintain(&source, db::MaintenanceOptions::default())
        .await
        .unwrap();
    let whole = suffix(&source, &cursor, 2, 64).await;
    assert_eq!(whole, suffix(&replica, &local_cursor, 2, 64).await);
    assert_eq!(
        db::reopen_cursor(&source, &cursor).await.unwrap().baseline,
        2
    );
    assert_eq!(
        db::reopen_cursor(&replica, &local_cursor)
            .await
            .unwrap()
            .baseline,
        2
    );
    assert_eq!(db::retention_status(&replica).await.unwrap().log, 3);
    let grouped = db::attach(
        directory.path().join("grouped"),
        db::AttachMode::ReadOnlyReplica,
    )
    .await
    .unwrap();
    import_logical(&grouped, &whole.encode().unwrap())
        .await
        .unwrap();
    let grouped_snapshot = db::snapshot(&grouped).await.unwrap();
    assert_eq!(rows(&grouped_snapshot), rows(&expected.last().unwrap().1));
    assert_eq!(
        db::catalogue(&grouped_snapshot).unwrap(),
        db::catalogue(&expected.last().unwrap().1).unwrap()
    );
    db::close(&grouped).await.unwrap();
    assert!(matches!(
        db::execute(&replica, tx! { return 1; }.unwrap(), Default::default()).await,
        Err(Error::ReadOnly)
    ));
    assert!(matches!(
        db::execute_limits(&replica, Default::default()).await,
        Err(Error::ReadOnly)
    ));
    assert!(matches!(
        db::execute_catalogue(&replica, CatalogueOperation::Drop { table: 1 }).await,
        Err(Error::ReadOnly)
    ));
    db::close(&replica).await.unwrap();
    let replica = db::open(directory.path().join("replica")).await.unwrap();
    assert!(replica.is_read_only());
    assert_eq!(suffix(&replica, &local_cursor, 2, 64).await, whole);
    db::close(&replica).await.unwrap();
    db::close(&source).await.unwrap();
}

#[tokio::test]
async fn complete_record_limits_empty_baselines_and_cursor_kinds() {
    let (_directory, source, replica, cursor) = fixture().await;
    write(&source, 20).await;
    write(&source, 30).await;
    let first = suffix(&source, &cursor, 2, 1).await;
    let gap = suffix(&source, &cursor, 3, 1).await;
    assert!(matches!(
        import_logical(&replica, &gap.encode().unwrap()).await,
        Err(Error::InvalidInput(_))
    ));
    let required = first.encode().unwrap().len();
    for max_bytes in [db::EMPTY_BATCH_BYTES, required - 1] {
        assert!(
            matches!(read_logical_feed(&source, &cursor, watermark(&source, 2), BatchLimits { max_records: 2, max_bytes }).await,
            Err(Error::BatchTooSmall { required: n }) if n == required)
        );
    }
    assert_eq!(
        read_logical_feed(
            &source,
            &cursor,
            watermark(&source, 2),
            BatchLimits {
                max_records: 2,
                max_bytes: required
            }
        )
        .await
        .unwrap(),
        first
    );
    let empty = suffix(&source, &cursor, 2, 0).await;
    assert_eq!(empty.start_exclusive, empty.end_inclusive);
    assert_eq!(
        import_logical(&replica, &empty.encode().unwrap())
            .await
            .unwrap()
            .sequence(),
        2
    );
    let future = suffix(&source, &cursor, 4, 0).await;
    assert!(matches!(
        import_logical(&replica, &future.encode().unwrap()).await,
        Err(Error::InvalidInput(_))
    ));
    assert!(matches!(
        import_logical(&source, &first.encode().unwrap()).await,
        Err(Error::InvalidInput(_))
    ));
    let resolved = db::checkout_cursor(
        &source,
        watermark(&source, 2),
        CursorKind::Resolved,
        "resolved",
    )
    .await
    .unwrap();
    assert!(matches!(
        read_logical_feed(
            &source,
            &resolved,
            watermark(&source, 2),
            Default::default()
        )
        .await,
        Err(Error::InvalidToken(_))
    ));
    let log = db::checkout_cursor(
        &source,
        watermark(&source, 2),
        CursorKind::LogReplica,
        "log",
    )
    .await
    .unwrap();
    assert_eq!(suffix(&source, &log, 2, 1).await, first);
    db::close(&source).await.unwrap();
    db::close(&replica).await.unwrap();
}

#[tokio::test]
async fn all_outcomes_validate_before_first_append_and_bad_inputs_leave_replica_usable() {
    let (directory, source, replica, cursor) = fixture().await;
    write(&source, 20).await;
    db::execute(
        &source,
        tx! { require(false, 123); }.unwrap(),
        Default::default(),
    )
    .await
    .unwrap();
    let good = suffix(&source, &cursor, 2, 2).await;
    let selected = std::fs::read(directory.path().join("replica/CURRENT")).unwrap();
    for case in 0..6 {
        let mut bad = good.clone();
        let FeedRecords::Logical(records) = &mut bad.records else {
            unreachable!()
        };
        match case {
            0 => bad.database_id[0] ^= 1,
            1 => {
                if let Outcome::Aborted(abort) = &mut records[1].outcome.outcome {
                    abort.user_code += 1;
                }
            }
            2 => {
                if let Outcome::Success { effects, .. } = &mut records[0].outcome.outcome
                    && let vm::Effect::Put { value, .. } = &mut effects[0]
                {
                    value[0] ^= 1;
                }
            }
            3 => {
                if let Outcome::Success {
                    result_type, value, ..
                } = &mut records[0].outcome.outcome
                {
                    *result_type = Type::U64;
                    *value = Value::U64(999);
                }
            }
            4 => {
                bad.start_exclusive += 1;
                bad.end_inclusive += 1;
            }
            5 => {
                records[0].log_record[28] ^= 1;
                let bytes = &mut records[0].log_record;
                let split = bytes.len() - 8;
                let crc = crc32c::crc32c(&bytes[..split]);
                bytes[split..split + 4].copy_from_slice(&crc.to_le_bytes());
                records[0].outcome.record_digest = Sha256::digest(bytes).into();
                records.truncate(1);
                bad.end_inclusive = 3;
            }
            _ => unreachable!(),
        }
        if let Ok(encoded) = bad.encode() {
            assert!(
                import_logical(&replica, &encoded).await.is_err(),
                "case {case}"
            );
        } else {
            assert_eq!(case, 4);
        }
        assert_eq!(
            std::fs::read(directory.path().join("replica/CURRENT")).unwrap(),
            selected
        );
        assert_eq!(db::snapshot(&replica).await.unwrap().sequence(), 2);
        assert!(!db::status(&replica).poisoned);
    }
    let bytes = good.encode().unwrap();
    assert_eq!(
        import_logical(&replica, &bytes).await.unwrap().sequence(),
        4
    );
    assert!(matches!(
        import_logical(&replica, &bytes).await,
        Err(Error::InvalidInput(_))
    ));
    db::close(&source).await.unwrap();
    db::close(&replica).await.unwrap();
}

#[tokio::test]
async fn decode_rejects_broken_chain_even_with_recomputed_log_outcome_and_batch_crcs() {
    let (_directory, source, replica, cursor) = fixture().await;
    write(&source, 20).await;
    write(&source, 30).await;
    let good = suffix(&source, &cursor, 2, 2).await;
    let FeedRecords::Logical(mut records) = good.records.clone() else {
        unreachable!()
    };
    let log = &mut records[1].log_record;
    log[28] ^= 1;
    let split = log.len() - 8;
    let crc = crc32c::crc32c(&log[..split]);
    log[split..split + 4].copy_from_slice(&crc.to_le_bytes());
    records[1].outcome.record_digest = Sha256::digest(log).into();
    // Individually valid records, deliberately assembled without encode's
    // inter-record chain check. Decode must independently reject this wire.
    let mut wire = good.encode().unwrap()[..56].to_vec();
    for record in records {
        let outcome = exchange::encode_outcome(&record.outcome).unwrap();
        for blob in [record.log_record, outcome] {
            wire.extend_from_slice(&(blob.len() as u32).to_le_bytes());
            wire.extend_from_slice(&blob);
        }
    }
    wire.extend_from_slice(&crc32c::crc32c(&wire).to_le_bytes());
    assert!(matches!(
        FeedBatch::decode(&wire),
        Err(Error::InvalidFormat(
            "invalid logical envelope or hash chain"
        ))
    ));
    assert!(matches!(
        import_logical(&replica, &wire).await,
        Err(Error::InvalidFormat(_))
    ));
    assert_eq!(db::snapshot(&replica).await.unwrap().sequence(), 2);
    db::close(&source).await.unwrap();
    db::close(&replica).await.unwrap();
}

#[tokio::test]
async fn import_fault_phases_recover_only_preverified_durable_prefixes() {
    for (phase, expected) in [(0, 2), (2, 2), (1, 2), (5, 3), (6, 4)] {
        let (directory, source, replica, cursor) = fixture().await;
        write(&source, 20).await;
        write(&source, 30).await;
        let batch = suffix(&source, &cursor, 2, 2).await;
        let selected = std::fs::read(directory.path().join("replica/CURRENT")).unwrap();
        replica
            .hooks
            .actions
            .lock()
            .unwrap()
            .insert(phase, Action::Error);
        assert!(matches!(
            import_logical(&replica, &batch.encode().unwrap()).await,
            Err(Error::Uncertain { .. })
        ));
        let _ = db::close(&replica).await;
        assert!(db::status(&replica).poisoned);
        assert_eq!(
            std::fs::read(directory.path().join("replica/CURRENT")).unwrap(),
            selected
        );
        let store = storage::open(directory.path().join("replica")).unwrap();
        assert_eq!(store.manifest().durable_sequence, expected);
        assert_eq!(store.manifest().checkpoint_sequence, 2);
        drop(store);
        let replica = db::open_with_options(
            directory.path().join("replica"),
            EngineOptions {
                preparation_bytes: 1,
                execution_bytes: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let snapshot = db::snapshot(&replica).await.unwrap();
        assert_eq!(snapshot.sequence(), expected);
        assert_eq!(
            db::get(&snapshot, 1, &Value::U64(1)).unwrap(),
            Some(Value::U64(match expected {
                2 => 10,
                3 => 20,
                _ => 30,
            }))
        );
        db::close(&replica).await.unwrap();
        let replica = db::open(directory.path().join("replica")).await.unwrap();
        let remaining = suffix(&source, &cursor, expected, 2).await;
        assert_eq!(
            import_logical(&replica, &remaining.encode().unwrap())
                .await
                .unwrap()
                .sequence(),
            4
        );
        db::close(&replica).await.unwrap();
        db::close(&source).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_import_holds_aggregate_permits_and_defers_controls_until_live_replay() {
    let (_directory, source, replica, cursor) = fixture().await;
    write(&source, 20).await;
    write(&source, 30).await;
    let bytes = suffix(&source, &cursor, 2, 2).await.encode().unwrap();
    let gate = Arc::new(Gate::default());
    replica
        .hooks
        .actions
        .lock()
        .unwrap()
        .insert(1, Action::Gate(gate.clone()));
    let importer = {
        let replica = replica.clone();
        let bytes = bytes.clone();
        tokio::spawn(async move { import_logical(&replica, &bytes).await })
    };
    let deadline = Instant::now() + Duration::from_secs(15);
    while !replica.hooks.started.lock().unwrap().contains(&1) {
        assert!(Instant::now() < deadline);
        tokio::task::yield_now().await;
    }
    let status = db::status(&replica);
    assert_eq!(status.queued_count, 2);
    assert!(status.queued_bytes > bytes.len() as u64);
    let mut second = pin!(import_logical(&replica, &bytes));
    let mut snapshot = pin!(db::snapshot(&replica));
    for poll in [
        second
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .map(|_| ()),
        snapshot
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .map(|_| ()),
    ] {
        assert!(matches!(poll, Poll::Pending));
    }
    assert_eq!(db::status(&replica).queued_count, 2);
    importer.abort();
    assert!(importer.await.unwrap_err().is_cancelled());
    assert_eq!(db::status(&replica).queued_count, 2);
    gate.release();
    assert_eq!(snapshot.await.unwrap().sequence(), 4);
    assert!(matches!(second.await, Err(Error::InvalidInput(_))));
    db::close(&replica).await.unwrap();
    db::close(&source).await.unwrap();
}

#[tokio::test]
async fn import_capacity_is_definite_and_recovery_does_not_apply_process_limits() {
    for resource in 0..6 {
        let (directory, source, replica, cursor) = fixture().await;
        write(&source, 20).await;
        write(&source, 30).await;
        db::close(&replica).await.unwrap();
        let mut options = EngineOptions::default();
        match resource {
            0 => options.submission_queue_count = 1,
            1 => options.submission_queue_bytes = 1,
            2 => options.assigned_backlog_count = 1,
            3 => options.assigned_backlog_bytes = 1,
            4 => options.preparation_bytes = 1,
            _ => options.execution_bytes = 1,
        }
        let replica = db::open_with_options(directory.path().join("replica"), options)
            .await
            .unwrap();
        let bytes = suffix(&source, &cursor, 2, 2).await.encode().unwrap();
        assert!(matches!(
            import_logical(&replica, &bytes).await,
            Err(Error::OperationalLimit { .. })
        ));
        assert_eq!(db::snapshot(&replica).await.unwrap().sequence(), 2);
        assert!(!db::status(&replica).poisoned);
        db::close(&replica).await.unwrap();
        db::close(&source).await.unwrap();
    }
}

#[tokio::test]
async fn crash_child() {
    let Some(root) = std::env::var_os("BLOP_IMPORT_CRASH_ROOT") else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    let phase = std::env::var("BLOP_IMPORT_CRASH_PHASE")
        .unwrap()
        .parse()
        .unwrap();
    let source = db::open(root.join("source")).await.unwrap();
    let cursor = db::list_cursors(&source).await.unwrap()[0].token;
    let replica = db::open(root.join("replica")).await.unwrap();
    replica
        .hooks
        .actions
        .lock()
        .unwrap()
        .insert(phase, Action::Exit);
    let batch = suffix(&source, &cursor, 2, 2).await;
    import_logical(&replica, &batch.encode().unwrap())
        .await
        .unwrap();
    panic!("import failed to reach crash hook");
}

#[tokio::test]
async fn process_exit_discards_validation_or_recovers_selected_verified_prefix_without_a_journal() {
    for (phase, expected) in [(2, 2), (1, 2), (5, 3), (6, 4)] {
        let (directory, source, replica, cursor) = fixture().await;
        write(&source, 20).await;
        write(&source, 30).await;
        let whole = suffix(&source, &cursor, 2, 2).await;
        let selected = std::fs::read(directory.path().join("replica/CURRENT")).unwrap();
        db::close(&source).await.unwrap();
        db::close(&replica).await.unwrap();
        let temporary = tempfile::tempdir_in(directory.path()).unwrap();
        let exit = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "database::replica::tests::crash_child",
                "--nocapture",
            ])
            .env("BLOP_IMPORT_CRASH_ROOT", directory.path())
            .env("BLOP_IMPORT_CRASH_PHASE", phase.to_string())
            .env("TMPDIR", temporary.path())
            .env("TMP", temporary.path())
            .env("TEMP", temporary.path())
            .status()
            .unwrap();
        assert_eq!(exit.code(), Some(77));
        assert_eq!(
            std::fs::read(directory.path().join("replica/CURRENT")).unwrap(),
            selected
        );
        let replica = db::open(directory.path().join("replica")).await.unwrap();
        let snapshot = db::snapshot(&replica).await.unwrap();
        assert_eq!(snapshot.sequence(), expected);
        let local_cursor = db::checkout_cursor(
            &replica,
            watermark(&replica, 2),
            CursorKind::Logical,
            "recovered",
        )
        .await
        .unwrap();
        let recovered = suffix(&replica, &local_cursor, 2, 2).await;
        let FeedRecords::Logical(records) = &whole.records else {
            unreachable!()
        };
        assert_eq!(
            recovered.records,
            FeedRecords::Logical(records[..(expected - 2) as usize].to_vec())
        );
        if phase == 2 {
            assert!(
                std::fs::read_dir(temporary.path())
                    .unwrap()
                    .next()
                    .is_some()
            );
        }
        // Disposable crash orphans are deliberately left present while opening
        // the real replica. They cannot participate in CURRENT selection.
        db::close(&replica).await.unwrap();
    }
}

#[tokio::test]
async fn matching_crc_does_not_substitute_for_the_canonical_record_digest() {
    let (_directory, source, replica, cursor) = fixture().await;
    write(&source, 20).await;
    let good = suffix(&source, &cursor, 2, 1).await;
    let mut wire = good.encode().unwrap();
    let FeedRecords::Logical(records) = &good.records else {
        unreachable!()
    };
    let log = &mut wire[60..60 + records[0].log_record.len()];
    let end = log.len() - 8;
    let target = crc32c::crc32c(&log[..end]);
    log[64] ^= 1;
    let changed = crc32c::crc32c(&log[..end]);
    // Solve for a four-byte CRC correction over GF(2), deliberately retaining
    // the original CRC and original outcome SHA256. CRC is not a chain anchor.
    let mut basis = [(0_u32, 0_u32); 32];
    for bit in 0..32 {
        log[end - 4 + bit / 8] ^= 1 << (bit % 8);
        let mut vector = crc32c::crc32c(&log[..end]) ^ changed;
        log[end - 4 + bit / 8] ^= 1 << (bit % 8);
        let mut mask = 1_u32 << bit;
        while vector != 0 {
            let pivot = vector.ilog2() as usize;
            if basis[pivot].0 == 0 {
                basis[pivot] = (vector, mask);
                break;
            }
            vector ^= basis[pivot].0;
            mask ^= basis[pivot].1;
        }
    }
    let mut remaining = target ^ changed;
    let mut correction = 0_u32;
    while remaining != 0 {
        let (vector, mask) = basis[remaining.ilog2() as usize];
        assert_ne!(vector, 0);
        remaining ^= vector;
        correction ^= mask;
    }
    for (byte, correction) in log[end - 4..end].iter_mut().zip(correction.to_le_bytes()) {
        *byte ^= correction;
    }
    assert_eq!(crc32c::crc32c(&log[..end]), target);
    assert_ne!(
        <[u8; 32]>::from(Sha256::digest(log)),
        records[0].outcome.record_digest
    );
    let split = wire.len() - 4;
    let crc = crc32c::crc32c(&wire[..split]);
    wire[split..].copy_from_slice(&crc.to_le_bytes());
    assert!(matches!(
        FeedBatch::decode(&wire),
        Err(Error::InvalidFormat(
            "logical record outcome digest mismatch"
        ))
    ));
    assert!(matches!(
        import_logical(&replica, &wire).await,
        Err(Error::InvalidFormat(_))
    ));
    assert_eq!(db::snapshot(&replica).await.unwrap().sequence(), 2);
    db::close(&replica).await.unwrap();
    db::close(&source).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn logical_feed_excludes_durable_records_and_installed_outcomes_above_visibility() {
    let (_directory, source, replica, cursor) = fixture().await;
    let gate = Arc::new(Gate::default());
    source
        .hooks
        .actions
        .lock()
        .unwrap()
        .insert(3, Action::Gate(gate.clone()));
    let writer = {
        let source = source.clone();
        tokio::spawn(async move { write(&source, 20).await })
    };
    let deadline = Instant::now() + Duration::from_secs(15);
    while !source.hooks.started.lock().unwrap().contains(&3) {
        assert!(Instant::now() < deadline);
        tokio::task::yield_now().await;
    }
    let independent = {
        let source = source.clone();
        tokio::spawn(async move {
            db::execute(
                &source,
                tx! { tables { data: u64 => u64 = 1 } data[2] = 30; }.unwrap(),
                Default::default(),
            )
            .await
            .unwrap()
        })
    };
    while db::status(&source).resolved_above_frontier != 1 {
        assert!(Instant::now() < deadline);
        tokio::task::yield_now().await;
    }
    assert_eq!(db::status(&source).durable_frontier, 4);
    let empty = suffix(&source, &cursor, 2, 64).await;
    assert_eq!(empty.start_exclusive, empty.end_inclusive);
    assert_eq!(
        import_logical(&replica, &empty.encode().unwrap())
            .await
            .unwrap()
            .sequence(),
        2
    );
    gate.release();
    writer.await.unwrap();
    independent.await.unwrap();
    let batch = suffix(&source, &cursor, 2, 64).await;
    assert_eq!(batch.end_inclusive, 4);
    import_logical(&replica, &batch.encode().unwrap())
        .await
        .unwrap();
    let source_snapshot = db::snapshot(&source).await.unwrap();
    let replica_snapshot = db::snapshot(&replica).await.unwrap();
    assert_eq!(rows(&source_snapshot), rows(&replica_snapshot));
    db::close(&replica).await.unwrap();
    db::close(&source).await.unwrap();
}

#[tokio::test]
async fn malformed_later_body_is_rejected_before_enqueueing_any_part_of_the_batch() {
    let (_directory, source, replica, cursor) = fixture().await;
    write(&source, 20).await;
    write(&source, 30).await;
    let mut bad = suffix(&source, &cursor, 2, 2).await;
    let FeedRecords::Logical(records) = &mut bad.records else {
        unreachable!()
    };
    let log = &mut records[1].log_record;
    log[64] = 2;
    let split = log.len() - 8;
    let crc = crc32c::crc32c(&log[..split]);
    log[split..split + 4].copy_from_slice(&crc.to_le_bytes());
    records[1].outcome.record_digest = Sha256::digest(log).into();
    let encoded = bad.encode().unwrap();
    assert!(matches!(
        import_logical(&replica, &encoded).await,
        Err(Error::Rejected(vm::Error::Unsupported {
            format: "transaction",
            version: 2
        }))
    ));
    assert!(replica.hooks.started.lock().unwrap().is_empty());
    assert_eq!(db::status(&replica).queued_count, 0);
    assert_eq!(db::snapshot(&replica).await.unwrap().sequence(), 2);
    let valid = suffix(&source, &cursor, 2, 2).await;
    import_logical(&replica, &valid.encode().unwrap())
        .await
        .unwrap();
    db::close(&replica).await.unwrap();
    db::close(&source).await.unwrap();
}

#[tokio::test]
async fn empty_prefix_replica_imports_from_the_genesis_anchor() {
    let directory = tempfile::tempdir().unwrap();
    let source = db::create(directory.path().join("source"), Default::default())
        .await
        .unwrap();
    let (empty, cursor) = db::snapshot_and_cursor(&source, CursorKind::Logical, "genesis")
        .await
        .unwrap();
    assert_eq!(empty.sequence(), 0);
    db::backup(&source, directory.path().join("replica"))
        .await
        .unwrap();
    let replica = db::attach(
        directory.path().join("replica"),
        db::AttachMode::ReadOnlyReplica,
    )
    .await
    .unwrap();
    db::execute_catalogue(
        &source,
        CatalogueOperation::Create {
            name: "data".into(),
            key: Type::U64,
            value: Type::U64,
        },
    )
    .await
    .unwrap();
    write(&source, 42).await;
    let batch = suffix(&source, &cursor, 0, 2).await;
    assert_eq!(
        import_logical(&replica, &batch.encode().unwrap())
            .await
            .unwrap()
            .sequence(),
        2
    );
    let snapshot = db::snapshot(&replica).await.unwrap();
    assert_eq!(
        db::get(&snapshot, 1, &Value::U64(1)).unwrap(),
        Some(Value::U64(42))
    );
    assert!(db::catalogue(&empty).unwrap().is_empty());
    db::close(&replica).await.unwrap();
    db::close(&source).await.unwrap();
}
