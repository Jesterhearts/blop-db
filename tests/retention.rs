#![cfg(any(unix, windows))]

use std::ops::Bound::Excluded;
use std::ops::Bound::Included;
use std::ops::Bound::Unbounded;

use blop_db::Database;
use blop_db::Limits;
use blop_db::database::BatchLimits;
use blop_db::database::CreateOptions;
use blop_db::database::CursorKind;
use blop_db::database::CursorToken;
use blop_db::database::Error;
use blop_db::database::FeedBatch;
use blop_db::database::FeedRecords;
use blop_db::database::Watermark;
use blop_db::database::{
    self as db,
};
use blop_db::storage;
use blop_db::tx;
use blop_db::vm::CatalogueOperation;
use blop_db::vm::Effect;
use blop_db::vm::Outcome;
use blop_db::vm::Type;
use blop_db::vm::Value;
use sha2::Digest;
use sha2::Sha256;

async fn fixture() -> (tempfile::TempDir, Database) {
    let directory = tempfile::tempdir().unwrap();
    let database = db::create(directory.path().join("db"), CreateOptions::default())
        .await
        .unwrap();
    (directory, database)
}

async fn table(database: &Database) -> u64 {
    let receipt = db::execute_catalogue(
        database,
        CatalogueOperation::Create {
            name: "items".into(),
            key: Type::U64,
            value: Type::I64,
        },
    )
    .await
    .unwrap();
    let Outcome::Success {
        value: Value::U64(id),
        ..
    } = receipt.outcome
    else {
        panic!("table creation failed");
    };
    id
}

async fn seed(database: &Database) -> u64 {
    let id = table(database).await;
    db::execute(
        database,
        tx! { tables { items: u64 => i64 = id } items[1] = 10; items[2] = 20; }.unwrap(),
        Limits::default(),
    )
    .await
    .unwrap();
    id
}

fn watermark(
    database: &Database,
    sequence: u64,
) -> Watermark {
    Watermark::new(database.database_id(), sequence).unwrap()
}

fn crc(bytes: &mut [u8]) {
    let split = bytes.len() - 4;
    let crc = crc32c::crc32c(&bytes[..split]);
    bytes[split..].copy_from_slice(&crc.to_le_bytes());
}

fn altered_token(
    token: CursorToken,
    range: std::ops::Range<usize>,
    value: &[u8],
) -> CursorToken {
    let mut bytes = token.encode();
    bytes[range].copy_from_slice(value);
    crc(&mut bytes);
    CursorToken::decode(&bytes).unwrap()
}

#[tokio::test]
async fn bounded_feed_continuations_account_for_every_sequence_without_acknowledging() {
    let (_directory, database) = fixture().await;
    let baseline = watermark(&database, 0);
    let token = db::checkout_cursor(&database, baseline, CursorKind::Resolved, "chunks")
        .await
        .unwrap();
    let mut expected = Vec::new();
    for number in 0_u64..7 {
        let receipt = db::execute(
            &database,
            tx! {
                captures { number: u64 = number }
                if number % 2 == 0 { abort(7); }
                return number;
            }
            .unwrap(),
            Limits::default(),
        )
        .await
        .unwrap();
        expected.push((receipt.sequence, receipt.outcome));
    }
    let mut after = baseline;
    let mut actual = Vec::new();
    loop {
        let batch = db::read_feed(
            &database,
            &token,
            after,
            BatchLimits {
                max_records: 2,
                ..BatchLimits::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(batch.start_exclusive, after.sequence());
        after = batch.watermark().unwrap();
        let FeedRecords::Resolved(records) = batch.records else {
            unreachable!()
        };
        if records.is_empty() {
            break;
        }
        actual.extend(
            records
                .into_iter()
                .map(|record| (record.sequence, record.outcome)),
        );
    }
    assert_eq!(actual, expected);
    assert_eq!(after.sequence(), 7);
    assert_eq!(
        db::reopen_cursor(&database, &token).await.unwrap().baseline,
        0
    );
    db::close(&database).await.unwrap();
}

#[tokio::test]
async fn snapshots_and_iterators_keep_original_values_names_and_liveness() {
    let (_directory, database) = fixture().await;
    let empty = db::snapshot(&database).await.unwrap();
    assert_eq!(empty.sequence(), 0);
    assert!(db::catalogue(&empty).unwrap().is_empty());
    let id = seed(&database).await;
    let snapshot = db::snapshot(&database).await.unwrap();
    let clone = snapshot.clone();
    assert_eq!(snapshot.sequence(), 2);
    let mut rows = db::scan(&snapshot, id, Unbounded, Unbounded).unwrap();
    assert_eq!(
        rows.next().unwrap().unwrap(),
        (Value::U64(1), Value::I64(10))
    );
    db::execute(
        &database,
        tx! { tables { items: u64 => i64 = id } items[1] = 100; delete(items[2]); items[3] = 30; }
            .unwrap(),
        Limits::default(),
    )
    .await
    .unwrap();
    db::execute_catalogue(
        &database,
        CatalogueOperation::Rename {
            table: id,
            name: "renamed".into(),
        },
    )
    .await
    .unwrap();
    let renamed = db::snapshot(&database).await.unwrap();
    assert_eq!(db::catalogue(&renamed).unwrap()[0].name, "renamed");
    db::execute_catalogue(&database, CatalogueOperation::Drop { table: id })
        .await
        .unwrap();
    assert_eq!(
        db::get(&snapshot, id, &Value::U64(1)).unwrap(),
        Some(Value::I64(10))
    );
    assert_eq!(db::get(&snapshot, id, &Value::U64(3)).unwrap(), None);
    assert_eq!(
        rows.next().unwrap().unwrap(),
        (Value::U64(2), Value::I64(20))
    );
    assert!(rows.next().is_none());
    assert_eq!(db::catalogue(&snapshot).unwrap()[0].name, "items");
    assert!(db::catalogue(&snapshot).unwrap()[0].live);
    assert!(db::get(&snapshot, id, &Value::I64(1)).is_err());
    assert!(db::get(&empty, id, &Value::U64(1)).is_err());
    let dropped = db::snapshot(&database).await.unwrap();
    assert!(!db::catalogue(&dropped).unwrap()[0].live);
    assert!(db::get(&dropped, id, &Value::U64(1)).is_err());
    assert!(db::scan(&dropped, id, Unbounded, Unbounded).is_err());
    assert_eq!(
        db::get(&renamed, id, &Value::U64(1)).unwrap(),
        Some(Value::I64(100))
    );
    assert_eq!(db::get(&renamed, id, &Value::U64(2)).unwrap(), None);
    let mut active = db::scan(&snapshot, id, Unbounded, Unbounded).unwrap();
    assert!(active.next().unwrap().is_ok());
    assert!(db::revoke(&snapshot));
    assert!(!db::revoke(&clone));
    for scan in [&mut rows, &mut active] {
        assert!(matches!(scan.next(), Some(Err(Error::SnapshotRevoked))));
        assert!(matches!(scan.next(), Some(Err(Error::SnapshotRevoked))));
    }
    assert!(matches!(
        db::get(&clone, id, &Value::U64(1)),
        Err(Error::SnapshotRevoked)
    ));
    assert!(matches!(
        db::catalogue(&snapshot),
        Err(Error::SnapshotRevoked)
    ));
    assert!(matches!(
        db::scan(&snapshot, id, Unbounded, Unbounded),
        Err(Error::SnapshotRevoked)
    ));
    db::close(&database).await.unwrap();
}

#[tokio::test]
async fn typed_scan_bounds_and_empty_ranges_are_checked_at_capture() {
    let (_directory, database) = fixture().await;
    let id = seed(&database).await;
    let snapshot = db::snapshot(&database).await.unwrap();
    let one = Value::U64(1);
    let two = Value::U64(2);
    assert_eq!(
        db::scan(&snapshot, id, Included(&one), Included(&one))
            .unwrap()
            .collect::<db::Result<Vec<_>>>()
            .unwrap(),
        vec![(one.clone(), Value::I64(10))]
    );
    assert!(
        db::scan(&snapshot, id, Excluded(&one), Included(&one))
            .unwrap()
            .next()
            .is_none()
    );
    assert!(db::scan(&snapshot, id, Included(&two), Included(&one)).is_err());
    assert!(db::scan(&snapshot, id, Included(&Value::I64(1)), Unbounded).is_err());
    assert_eq!(
        db::scan(&snapshot, id, Excluded(&one), Included(&two))
            .unwrap()
            .collect::<db::Result<Vec<_>>>()
            .unwrap(),
        vec![(two, Value::I64(20))]
    );
    db::close(&database).await.unwrap();
}

#[tokio::test]
async fn close_revokes_idle_handles_and_scans_before_releasing_directory_lock() {
    let (directory, database) = fixture().await;
    let id = seed(&database).await;
    let snapshot = db::snapshot(&database).await.unwrap();
    let mut scan = db::scan(&snapshot, id, Unbounded, Unbounded).unwrap();
    assert!(scan.next().unwrap().is_ok());
    let mut idle = db::scan(&snapshot, id, Unbounded, Unbounded).unwrap();
    db::close(&database).await.unwrap();
    assert!(snapshot.is_revoked());
    assert!(matches!(scan.next(), Some(Err(Error::SnapshotRevoked))));
    assert!(matches!(idle.next(), Some(Err(Error::SnapshotRevoked))));
    let reopened = db::open(directory.path().join("db")).await.unwrap();
    assert!(matches!(db::close(&database).await, Err(Error::Closed)));
    db::close(&reopened).await.unwrap();
}

#[tokio::test]
async fn durable_cursor_lifecycle_checks_identities_kinds_and_issued_ids() {
    let (directory, database) = fixture().await;
    let baseline = watermark(&database, 0);
    let mut tokens = Vec::new();
    for kind in [
        CursorKind::Resolved,
        CursorKind::Logical,
        CursorKind::LogReplica,
    ] {
        let token = db::checkout_cursor(&database, baseline, kind, "same label")
            .await
            .unwrap();
        assert_eq!(token.cursor_id(), tokens.len() as u64 + 1);
        tokens.push(token);
    }
    let token = tokens[0];
    for bad in [
        altered_token(token, 12..28, &[9; 16]),
        altered_token(token, 28..44, &[9; 16]),
        altered_token(token, 44..52, &99_u64.to_le_bytes()),
        altered_token(token, 10..11, &[2]),
    ] {
        assert!(matches!(
            db::reopen_cursor(&database, &bad).await,
            Err(Error::InvalidToken(_))
        ));
        assert!(matches!(
            db::release_cursor(&database, &bad).await,
            Err(Error::InvalidToken(_))
        ));
        assert!(matches!(
            db::acknowledge_cursor(&database, &bad, baseline).await,
            Err(Error::InvalidToken(_))
        ));
    }
    let wrong_namespace = altered_token(
        altered_token(token, 44..52, &99_u64.to_le_bytes()),
        28..44,
        &[9; 16],
    );
    assert!(matches!(
        db::reopen_cursor(&database, &wrong_namespace).await,
        Err(Error::InvalidToken("cursor database or namespace mismatch"))
    ));
    assert!(matches!(
        db::checkout_cursor(
            &database,
            Watermark::new([9; 16], 0).unwrap(),
            CursorKind::Resolved,
            "bad"
        )
        .await,
        Err(Error::InvalidToken(_))
    ));
    assert!(matches!(
        db::checkout_cursor(
            &database,
            watermark(&database, 1),
            CursorKind::Resolved,
            "future"
        )
        .await,
        Err(Error::HistoryUnavailable)
    ));
    for label in ["a\0b".to_owned(), "a".repeat(256)] {
        assert!(matches!(
            db::checkout_cursor(&database, baseline, CursorKind::Resolved, label).await,
            Err(Error::InvalidInput(_))
        ));
    }
    assert_eq!(
        db::execute(&database, tx! { return 42; }.unwrap(), Limits::default())
            .await
            .unwrap()
            .sequence,
        1
    );
    let committed = watermark(&database, 1);
    for token in &tokens {
        db::acknowledge_cursor(&database, token, committed)
            .await
            .unwrap();
        db::acknowledge_cursor(&database, token, committed)
            .await
            .unwrap();
        assert!(matches!(
            db::acknowledge_cursor(&database, token, baseline).await,
            Err(Error::InvalidInput(_))
        ));
        assert!(matches!(
            db::acknowledge_cursor(&database, token, watermark(&database, 2)).await,
            Err(Error::InvalidInput(_))
        ));
        assert!(matches!(
            db::acknowledge_cursor(&database, token, Watermark::new([9; 16], 1).unwrap()).await,
            Err(Error::InvalidToken(_))
        ));
    }
    db::close(&database).await.unwrap();
    let store = storage::open(directory.path().join("db")).unwrap();
    assert_eq!(store.manifest().checkpoint_sequence, 1);
    assert_eq!(store.manifest().durable_sequence, 1);
    assert_eq!(store.manifest().next_cursor_id, 4);
    drop(store);
    let database = db::open(directory.path().join("db")).await.unwrap();
    assert_eq!(db::list_cursors(&database).await.unwrap().len(), 3);
    for token in tokens {
        let token = CursorToken::decode(&token.encode()).unwrap();
        let info = db::reopen_cursor(&database, &token).await.unwrap();
        assert_eq!(info.baseline, 1);
        assert_eq!(info.label, "same label");
        db::release_cursor(&database, &token).await.unwrap();
        db::release_cursor(&database, &token).await.unwrap();
        assert!(matches!(
            db::reopen_cursor(&database, &token).await,
            Err(Error::CursorReleased)
        ));
        assert!(matches!(
            db::acknowledge_cursor(&database, &token, committed).await,
            Err(Error::CursorReleased)
        ));
    }
    db::close(&database).await.unwrap();
    let database = db::open(directory.path().join("db")).await.unwrap();
    assert!(db::list_cursors(&database).await.unwrap().is_empty());
    assert!(matches!(
        db::reopen_cursor(&database, &token).await,
        Err(Error::CursorReleased)
    ));
    db::release_cursor(&database, &token).await.unwrap();
    let next = db::checkout_cursor(&database, committed, CursorKind::Resolved, "")
        .await
        .unwrap();
    assert_eq!(next.cursor_id(), 4);
    assert_eq!(
        db::execute(&database, tx! { return 7; }.unwrap(), Limits::default())
            .await
            .unwrap()
            .sequence,
        2
    );
    db::close(&database).await.unwrap();
}

#[tokio::test]
async fn snapshot_tail_and_retention_floors_are_independent() {
    let (directory, database) = fixture().await;
    let old = db::snapshot(&database).await.unwrap();
    let id = seed(&database).await;
    let (snapshot, cursor) = db::snapshot_and_cursor(&database, CursorKind::Resolved, "build")
        .await
        .unwrap();
    assert_eq!(snapshot.sequence(), 2);
    assert_eq!(
        db::reopen_cursor(&database, &cursor)
            .await
            .unwrap()
            .baseline,
        2
    );
    let log = db::checkout_cursor(
        &database,
        watermark(&database, 1),
        CursorKind::Logical,
        "log",
    )
    .await
    .unwrap();
    assert_eq!(
        db::retention_status(&database).await.unwrap(),
        db::RetentionFloors { history: 0, log: 2 }
    );
    db::revoke(&old);
    assert_eq!(db::retention_status(&database).await.unwrap().history, 1);
    db::release_cursor(&database, &log).await.unwrap();
    db::execute(
        &database,
        tx! { tables { items: u64 => i64 = id } items[1] = 30; }.unwrap(),
        Limits::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        db::get(&snapshot, id, &Value::U64(1)).unwrap(),
        Some(Value::I64(10))
    );
    let at = snapshot.watermark();
    assert!(db::revoke(&snapshot));
    assert_eq!(
        db::retention_status(&database).await.unwrap(),
        db::RetentionFloors { history: 2, log: 2 }
    );
    let batch = db::read_feed(&database, &cursor, at, BatchLimits::default())
        .await
        .unwrap();
    assert_eq!(batch.start_exclusive, 2);
    assert_eq!(batch.end_inclusive, 3);
    assert_eq!(
        db::reopen_cursor(&database, &cursor)
            .await
            .unwrap()
            .baseline,
        2
    );
    db::close(&database).await.unwrap();
    let database = db::open(directory.path().join("db")).await.unwrap();
    assert_eq!(
        db::reopen_cursor(&database, &cursor)
            .await
            .unwrap()
            .baseline,
        2
    );
    assert_eq!(
        db::read_feed(&database, &cursor, at, BatchLimits::default())
            .await
            .unwrap(),
        batch
    );
    db::acknowledge_cursor(&database, &cursor, batch.watermark().unwrap())
        .await
        .unwrap();
    assert!(matches!(
        db::read_feed(&database, &cursor, at, BatchLimits::default()).await,
        Err(Error::HistoryUnavailable)
    ));
    db::close(&database).await.unwrap();
}

#[tokio::test]
async fn resolved_feed_has_exact_effects_aborts_no_writes_and_administration() {
    let (directory, database) = fixture().await;
    let zero = watermark(&database, 0);
    let cursor = db::checkout_cursor(&database, zero, CursorKind::Resolved, "consumer")
        .await
        .unwrap();
    let id = seed(&database).await;
    db::execute(
        &database,
        tx! { tables { items: u64 => i64 = id } items[1] = 100; delete(items[2]); }.unwrap(),
        Limits::default(),
    )
    .await
    .unwrap();
    db::execute(
        &database,
        tx! { tables { items: u64 => i64 = id } items[1] = 999; require(false, 12); }.unwrap(),
        Limits::default(),
    )
    .await
    .unwrap();
    db::execute(&database, tx! { return 42; }.unwrap(), Limits::default())
        .await
        .unwrap();
    db::execute_catalogue(
        &database,
        CatalogueOperation::Rename {
            table: id,
            name: "new".into(),
        },
    )
    .await
    .unwrap();
    db::execute_catalogue(&database, CatalogueOperation::Drop { table: id })
        .await
        .unwrap();
    db::execute_limits(
        &database,
        Limits {
            writes: 2,
            ..Limits::default()
        },
    )
    .await
    .unwrap();
    let batch = db::read_feed(&database, &cursor, zero, BatchLimits::default())
        .await
        .unwrap();
    assert_eq!(batch.end_inclusive, 8);
    let FeedRecords::Resolved(records) = &batch.records else {
        panic!("resolved feed");
    };
    assert_eq!(
        records
            .iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>(),
        (1..=8).collect::<Vec<_>>()
    );
    assert!(
        matches!(&records[0].outcome, Outcome::Success { effects, .. } if matches!(&effects[..], [Effect::Catalogue { .. }]))
    );
    assert!(
        matches!(&records[1].outcome, Outcome::Success { effects, .. } if effects == &vec![Effect::Put { table: id, key: 1_u64.to_be_bytes().to_vec(), value: 10_i64.to_le_bytes().to_vec() }, Effect::Put { table: id, key: 2_u64.to_be_bytes().to_vec(), value: 20_i64.to_le_bytes().to_vec() }])
    );
    assert!(
        matches!(&records[2].outcome, Outcome::Success { effects, .. } if effects == &vec![Effect::Put { table: id, key: 1_u64.to_be_bytes().to_vec(), value: 100_i64.to_le_bytes().to_vec() }, Effect::Delete { table: id, key: 2_u64.to_be_bytes().to_vec() }])
    );
    assert!(matches!(&records[3].outcome, Outcome::Aborted(_)));
    assert!(matches!(&records[4].outcome, Outcome::Success { effects, .. } if effects.is_empty()));
    for record in &records[5..7] {
        assert_eq!(record.record_kind, 2);
    }
    assert!(
        matches!(&records[7].outcome, Outcome::Success { effects, .. } if matches!(&effects[..], [Effect::Limits { .. }]))
    );
    let encoded = batch.encode().unwrap();
    assert_eq!(FeedBatch::decode(&encoded).unwrap(), batch);
    let first = db::read_feed(
        &database,
        &cursor,
        zero,
        BatchLimits {
            max_records: 1,
            ..BatchLimits::default()
        },
    )
    .await
    .unwrap();
    let first_size = first.encode().unwrap().len();
    assert!(
        matches!(db::read_feed(&database, &cursor, zero, BatchLimits { max_records: 8, max_bytes: first_size - 1 }).await, Err(Error::BatchTooSmall { required }) if required == first_size)
    );
    assert_eq!(
        db::read_feed(
            &database,
            &cursor,
            zero,
            BatchLimits {
                max_records: 8,
                max_bytes: first_size
            }
        )
        .await
        .unwrap(),
        first
    );
    for (after, limits) in [
        (
            zero,
            BatchLimits {
                max_records: 0,
                max_bytes: 60,
            },
        ),
        (watermark(&database, 8), BatchLimits::default()),
    ] {
        let empty = db::read_feed(&database, &cursor, after, limits)
            .await
            .unwrap();
        assert_eq!(empty.start_exclusive, after.sequence());
        assert_eq!(empty.end_inclusive, after.sequence());
        assert_eq!(empty.encode().unwrap().len(), 60);
        assert_eq!(FeedBatch::decode(&empty.encode().unwrap()).unwrap(), empty);
    }
    for max_bytes in [0, 59, db::MAX_BATCH_BYTES + 1] {
        assert!(matches!(
            db::read_feed(
                &database,
                &cursor,
                zero,
                BatchLimits {
                    max_bytes,
                    ..BatchLimits::default()
                }
            )
            .await,
            Err(Error::InvalidInput(_))
        ));
    }
    assert!(matches!(
        db::read_feed(
            &database,
            &cursor,
            watermark(&database, 9),
            BatchLimits::default()
        )
        .await,
        Err(Error::HistoryUnavailable)
    ));
    assert!(matches!(
        db::read_feed(
            &database,
            &cursor,
            Watermark::new([9; 16], 0).unwrap(),
            BatchLimits::default()
        )
        .await,
        Err(Error::InvalidToken(_))
    ));
    db::close(&database).await.unwrap();
    let database = db::open(directory.path().join("db")).await.unwrap();
    assert_eq!(
        db::read_feed(&database, &cursor, zero, BatchLimits::default())
            .await
            .unwrap(),
        batch
    );
    db::close(&database).await.unwrap();
}

#[tokio::test]
async fn cursor_tokens_and_watermarks_reject_malformed_frames_before_fields() {
    let (_directory, database) = fixture().await;
    let token = db::checkout_cursor(&database, watermark(&database, 0), CursorKind::Resolved, "")
        .await
        .unwrap();
    let bytes = token.encode();
    assert_eq!(bytes.len(), 56);
    for length in 0..bytes.len() {
        assert!(CursorToken::decode(&bytes[..length]).is_err());
    }
    for offset in [0, 8, 10, 11, 12, 28, 44, 52] {
        let mut bad = bytes;
        bad[offset] ^= 0xff;
        assert!(matches!(
            CursorToken::decode(&bad),
            Err(Error::InvalidToken("exchange CRC mismatch"))
        ));
    }
    for (range, value) in [
        (0..8, vec![0; 8]),
        (8..10, vec![2, 0]),
        (10..11, vec![0]),
        (10..11, vec![4]),
        (11..12, vec![1]),
        (12..28, vec![0; 16]),
        (28..44, vec![0; 16]),
        (44..52, vec![0; 8]),
        (44..52, vec![255; 8]),
    ] {
        let mut bad = bytes;
        bad[range].copy_from_slice(&value);
        crc(&mut bad);
        assert!(matches!(
            CursorToken::decode(&bad),
            Err(Error::InvalidToken(_))
        ));
    }
    let mut long = bytes.to_vec();
    long.push(0);
    assert!(CursorToken::decode(&long).is_err());
    for sequence in [0, 1, u64::MAX - 1] {
        let mark = watermark(&database, sequence);
        assert_eq!(mark.encode().len(), 40);
        assert_eq!(Watermark::decode(&mark.encode()).unwrap(), mark);
        let hex = mark.to_hex();
        assert_eq!(hex.len(), 80);
        assert_eq!(Watermark::from_hex(&hex).unwrap(), mark);
        for bad in [
            hex.to_uppercase(),
            format!("{hex}\n"),
            format!("0x{hex}"),
            hex[1..].to_owned(),
            "g".repeat(80),
        ] {
            assert!(Watermark::from_hex(&bad).is_err());
        }
        let bytes = mark.encode();
        for length in 0..40 {
            assert!(Watermark::decode(&bytes[..length]).is_err());
        }
        for (range, value) in [
            (0..8, vec![0; 8]),
            (8..10, vec![2, 0]),
            (10..12, vec![1, 0]),
            (12..28, vec![0; 16]),
            (28..36, vec![255; 8]),
        ] {
            let mut bad = bytes;
            bad[range].copy_from_slice(&value);
            assert!(matches!(
                Watermark::decode(&bad),
                Err(Error::InvalidFormat("exchange CRC mismatch"))
            ));
            crc(&mut bad);
            assert!(Watermark::decode(&bad).is_err());
        }
    }
    assert!(Watermark::new([0; 16], 0).is_err());
    assert!(Watermark::new(database.database_id(), u64::MAX).is_err());
    db::close(&database).await.unwrap();
}

#[tokio::test]
async fn feed_codecs_validate_real_log_envelopes_digests_chains_and_bounded_counts() {
    let (_directory, database) = fixture().await;
    let zero = watermark(&database, 0);
    let cursor = db::checkout_cursor(&database, zero, CursorKind::Resolved, "codec")
        .await
        .unwrap();
    seed(&database).await;
    db::execute(
        &database,
        tx! { require(false); }.unwrap(),
        Limits::default(),
    )
    .await
    .unwrap();
    let batch = db::read_feed(&database, &cursor, zero, BatchLimits::default())
        .await
        .unwrap();
    let bytes = batch.encode().unwrap();
    for length in 0..bytes.len() {
        assert!(FeedBatch::decode(&bytes[..length]).is_err());
    }
    for (range, value) in [
        (0..8, vec![0; 8]),
        (8..10, vec![2, 0]),
        (10..11, vec![3]),
        (11..12, vec![1]),
        (12..16, vec![0; 4]),
        (16..32, vec![0; 16]),
        (32..40, vec![255; 8]),
        (40..48, vec![0; 8]),
        (48..52, vec![255; 4]),
        (52..56, vec![1; 4]),
        (56..60, vec![255; 4]),
    ] {
        let mut bad = bytes.clone();
        bad[range].copy_from_slice(&value);
        assert!(matches!(
            FeedBatch::decode(&bad),
            Err(Error::InvalidFormat("exchange CRC mismatch"))
        ));
        crc(&mut bad);
        assert!(FeedBatch::decode(&bad).is_err());
    }
    let mut trailing = bytes.clone();
    trailing.insert(trailing.len() - 4, 0);
    let length = trailing.len() as u32;
    trailing[12..16].copy_from_slice(&length.to_le_bytes());
    crc(&mut trailing);
    assert!(FeedBatch::decode(&trailing).is_err());
    let mut invalid = batch.clone();
    invalid.end_inclusive = 2;
    assert!(invalid.encode().is_err());
    let mut invalid = batch.clone();
    if let FeedRecords::Resolved(records) = &mut invalid.records {
        records[1].sequence = 1;
    }
    assert!(invalid.encode().is_err());
    let logical_cursor = db::checkout_cursor(&database, zero, CursorKind::Logical, "codec")
        .await
        .unwrap();
    let source = db::read_logical_feed(&database, &logical_cursor, zero, BatchLimits::default())
        .await
        .unwrap();
    let FeedRecords::Logical(logical) = source.records else {
        unreachable!()
    };
    let FeedRecords::Resolved(outcomes) = batch.records else {
        unreachable!()
    };
    assert_eq!(
        logical
            .iter()
            .map(|record| record.outcome.clone())
            .collect::<Vec<_>>(),
        outcomes
    );
    let batch = FeedBatch {
        database_id: database.database_id(),
        start_exclusive: 0,
        end_inclusive: 3,
        records: FeedRecords::Logical(logical.clone()),
    };
    assert_eq!(FeedBatch::decode(&batch.encode().unwrap()).unwrap(), batch);
    for case in 0..5 {
        let mut records = logical.clone();
        match case {
            0 => records[1].outcome.record_digest[0] ^= 1,
            1 => records[1].outcome.record_kind = 2,
            2 => records[1].outcome.sequence = 3,
            3 => records[1].log_record[64] ^= 1,
            4 => {
                let bytes = &mut records[1].log_record;
                bytes[28] ^= 1;
                let split = bytes.len() - 8;
                let crc = crc32c::crc32c(&bytes[..split]);
                bytes[split..split + 4].copy_from_slice(&crc.to_le_bytes());
                records[1].outcome.record_digest = Sha256::digest(bytes).into();
            }
            _ => unreachable!(),
        }
        let bad = FeedBatch {
            records: FeedRecords::Logical(records),
            ..batch.clone()
        };
        assert!(bad.encode().is_err(), "case {case}");
    }
    let mut wire = batch.encode().unwrap();
    wire[60 + 64] ^= 1;
    crc(&mut wire);
    assert!(matches!(
        FeedBatch::decode(&wire),
        Err(Error::InvalidFormat("logical record CRC mismatch"))
    ));
    assert!(FeedBatch::decode(&vec![0; db::MAX_BATCH_BYTES + 1]).is_err());
    db::close(&database).await.unwrap();
}

#[tokio::test]
async fn history_and_log_floors_prevent_resurrection_but_allow_empty_tail() {
    let (directory, database) = fixture().await;
    seed(&database).await;
    let zero = watermark(&database, 0);
    let one = watermark(&database, 1);
    let two = watermark(&database, 2);
    db::close(&database).await.unwrap();
    let path = directory.path().join("db");
    let mut store = storage::open(&path).unwrap();
    let mut manifest = store.manifest().clone();
    manifest.history_floor = 1;
    manifest.log_floor = 3;
    manifest.segments.clear();
    let view = storage::view(&store);
    storage::publish(&mut store, &view, manifest).unwrap();
    drop(view);
    drop(store);
    let database = db::open(&path).await.unwrap();
    assert!(matches!(
        db::checkout_cursor(&database, zero, CursorKind::Resolved, "gone").await,
        Err(Error::HistoryUnavailable)
    ));
    db::checkout_cursor(&database, one, CursorKind::Resolved, "retained")
        .await
        .unwrap();
    for kind in [CursorKind::Logical, CursorKind::LogReplica] {
        assert!(matches!(
            db::checkout_cursor(&database, one, kind, "no log").await,
            Err(Error::HistoryUnavailable)
        ));
        db::checkout_cursor(&database, two, kind, "empty tail")
            .await
            .unwrap();
    }
    assert_eq!(db::list_cursors(&database).await.unwrap().len(), 3);
    db::close(&database).await.unwrap();
}

#[tokio::test]
async fn cursor_id_exhaustion_does_not_wrap_reuse_or_consume_sequences() {
    let (directory, database) = fixture().await;
    let zero = watermark(&database, 0);
    db::close(&database).await.unwrap();
    let path = directory.path().join("db");
    let mut store = storage::open(&path).unwrap();
    let mut manifest = store.manifest().clone();
    manifest.next_cursor_id = u64::MAX - 1;
    let view = storage::view(&store);
    storage::publish(&mut store, &view, manifest).unwrap();
    drop(view);
    drop(store);
    let database = db::open(&path).await.unwrap();
    let token = db::checkout_cursor(&database, zero, CursorKind::Resolved, "last")
        .await
        .unwrap();
    assert_eq!(token.cursor_id(), u64::MAX - 1);
    assert!(matches!(
        db::checkout_cursor(&database, zero, CursorKind::Resolved, "overflow").await,
        Err(Error::CursorIdExhausted)
    ));
    db::release_cursor(&database, &token).await.unwrap();
    assert!(matches!(
        db::checkout_cursor(&database, zero, CursorKind::Resolved, "reuse").await,
        Err(Error::CursorIdExhausted)
    ));
    assert_eq!(
        db::execute(&database, tx! { return 42; }.unwrap(), Limits::default())
            .await
            .unwrap()
            .sequence,
        1
    );
    db::close(&database).await.unwrap();
    let database = db::open(&path).await.unwrap();
    assert!(matches!(
        db::checkout_cursor(&database, zero, CursorKind::Resolved, "restart").await,
        Err(Error::CursorIdExhausted)
    ));
    db::close(&database).await.unwrap();
}

#[tokio::test]
async fn failed_cursor_publication_poisoning_stops_writer_and_recovers_old_claim() {
    for operation in 0..3 {
        let (directory, database) = fixture().await;
        let zero = watermark(&database, 0);
        let token = db::checkout_cursor(&database, zero, CursorKind::Resolved, "existing")
            .await
            .unwrap();
        db::execute(&database, tx! { return 42; }.unwrap(), Limits::default())
            .await
            .unwrap();
        let path = directory.path().join("db");
        std::fs::create_dir(path.join("CURRENT.pending")).unwrap();
        let result = match operation {
            0 => db::checkout_cursor(&database, zero, CursorKind::Resolved, "failed")
                .await
                .map(|_| ()),
            1 => db::acknowledge_cursor(&database, &token, watermark(&database, 1)).await,
            2 => db::release_cursor(&database, &token).await,
            _ => unreachable!(),
        };
        assert!(matches!(
            result,
            Err(Error::Uncertain { sequence: None, .. })
        ));
        assert!(matches!(
            db::execute(&database, tx! { return 99; }.unwrap(), Limits::default()).await,
            Err(Error::Closed)
        ));
        assert!(matches!(db::snapshot(&database).await, Err(Error::Closed)));
        assert!(matches!(db::close(&database).await, Err(Error::Closed)));
        std::fs::remove_dir(path.join("CURRENT.pending")).unwrap();
        let database = db::open(&path).await.unwrap();
        assert_eq!(
            db::reopen_cursor(&database, &token).await.unwrap().baseline,
            0
        );
        assert_eq!(db::list_cursors(&database).await.unwrap().len(), 1);
        assert_eq!(
            db::execute(&database, tx! { return 7; }.unwrap(), Limits::default())
                .await
                .unwrap()
                .sequence,
            2
        );
        db::close(&database).await.unwrap();
    }
}

#[tokio::test]
async fn logical_checkout_checks_files_not_just_numeric_log_floor() {
    let (directory, database) = fixture().await;
    seed(&database).await;
    let path = directory.path().join("db/log-00000000000000000001.bin");
    let moved = directory.path().join("held-log");
    std::fs::rename(&path, &moved).unwrap();
    for kind in [CursorKind::Logical, CursorKind::LogReplica] {
        assert!(matches!(
            db::checkout_cursor(&database, watermark(&database, 0), kind, "missing").await,
            Err(Error::HistoryUnavailable)
        ));
    }
    assert!(db::list_cursors(&database).await.unwrap().is_empty());
    std::fs::rename(&moved, &path).unwrap();
    let token = db::checkout_cursor(
        &database,
        watermark(&database, 0),
        CursorKind::Logical,
        "restored",
    )
    .await
    .unwrap();
    assert!(matches!(
        db::read_feed(
            &database,
            &token,
            watermark(&database, 0),
            BatchLimits::default()
        )
        .await,
        Err(Error::InvalidToken(_))
    ));
    db::close(&database).await.unwrap();
}
