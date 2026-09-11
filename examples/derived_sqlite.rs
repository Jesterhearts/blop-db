//! Build and resume a SQLite mirror from the source database's resolved feed.
//!
//! Run `cargo run --example derived_sqlite -- SOURCE_DIRECTORY INDEX_SQLITE`.
//! Initial creation requires source history from sequence zero. The example
//! rejects missing baseline history and does not rebuild from a snapshot.
//!
//! Keep the SQLite file and its journals together, and use one consumer process
//! per index. On restart, resume from SQLite's committed source watermark in
//! design format H.2. The source cursor may lag behind that committed progress.
//! The mirror also maintains a deterministic index of encoded-value hashes.

use std::path::Path;

use blop_db::database::BatchLimits;
use blop_db::database::CursorKind;
use blop_db::database::CursorToken;
use blop_db::database::Database;
use blop_db::database::FeedBatch;
use blop_db::database::FeedRecords;
use blop_db::database::Watermark;
use blop_db::database::{
    self as db,
};
use blop_db::vm::Effect;
use blop_db::vm::Outcome;
use blop_db::vm::{
    self,
};
use rusqlite::Connection;
use rusqlite::OptionalExtension;
use rusqlite::TransactionBehavior;
use rusqlite::params;
use sha2::Digest;
use sha2::Sha256;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn open_index(path: &Path) -> Result<Connection> {
    let connection = Connection::open(path)?;
    connection.execute_batch(
        "PRAGMA journal_mode = DELETE; PRAGMA synchronous = FULL;
        CREATE TABLE IF NOT EXISTS progress (
            singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
            version INTEGER NOT NULL CHECK(version = 1),
            token BLOB NOT NULL, watermark BLOB NOT NULL);
        CREATE TABLE IF NOT EXISTS catalogue (
            table_id BLOB PRIMARY KEY, name TEXT NOT NULL, live INTEGER NOT NULL,
            key_schema BLOB NOT NULL, value_schema BLOB NOT NULL) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS mirror (
            table_id BLOB NOT NULL, key BLOB NOT NULL, value BLOB NOT NULL,
            value_hash BLOB NOT NULL, PRIMARY KEY(table_id, key)) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS mirror_by_value ON mirror(value_hash, table_id, key);
        CREATE TABLE IF NOT EXISTS source_records (
            sequence BLOB PRIMARY KEY, outcome BLOB NOT NULL) WITHOUT ROWID;",
    )?;
    Ok(connection)
}

fn committed(connection: &Connection) -> Result<Option<(CursorToken, Watermark)>> {
    let row: Option<(u32, Vec<u8>, Vec<u8>)> = connection
        .query_row(
            "SELECT version, token, watermark FROM progress WHERE singleton = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    row.map(|(version, token, watermark)| {
        if version != 1 {
            return Err("unsupported derived index version".into());
        }
        Ok((CursorToken::decode(&token)?, Watermark::decode(&watermark)?))
    })
    .transpose()
}

async fn resume(
    connection: &mut Connection,
    source: &Database,
) -> Result<(CursorToken, Watermark)> {
    let (token, watermark) = match committed(connection)? {
        Some(progress) => progress,
        None => {
            let zero = Watermark::new(source.database_id(), 0)?;
            let token =
                db::checkout_cursor(source, zero, CursorKind::Resolved, "derived-sqlite").await?;
            // A crash before this commit may leave an unused source cursor, but
            // can never acknowledge or publish a partially initialized mirror.
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let rows: i64 = transaction.query_row(
                "SELECT (SELECT count(*) FROM mirror) + (SELECT count(*) FROM catalogue)
                    + (SELECT count(*) FROM source_records)",
                [],
                |row| row.get(0),
            )?;
            if rows != 0 {
                return Err("derived data has no committed watermark".into());
            }
            transaction.execute(
                "INSERT INTO progress VALUES (1, 1, ?1, ?2)",
                params![token.encode().as_slice(), zero.encode().as_slice()],
            )?;
            transaction.commit()?;
            (token, zero)
        }
    };
    if watermark.database_id() != source.database_id() || token.kind() != CursorKind::Resolved {
        return Err("derived index belongs to a different source or cursor kind".into());
    }
    let cursor = db::reopen_cursor(source, &token).await?;
    if cursor.baseline > watermark.sequence() {
        return Err("source cursor is ahead of the committed derived watermark".into());
    }
    db::read_feed(
        source,
        &token,
        watermark,
        BatchLimits {
            max_records: 0,
            ..Default::default()
        },
    )
    .await?;
    db::acknowledge_cursor(source, &token, watermark).await?;
    Ok((token, watermark))
}

fn stage(
    connection: &rusqlite::Transaction<'_>,
    batch: &FeedBatch,
) -> Result<Watermark> {
    let (_, previous) = committed(connection)?.ok_or("missing derived watermark")?;
    if batch.database_id != previous.database_id() || batch.start_exclusive != previous.sequence() {
        return Err("derived batch is not the next committed source suffix".into());
    }
    let FeedRecords::Resolved(records) = &batch.records else {
        return Err("derived consumer requires resolved records".into());
    };
    if batch.end_inclusive.checked_sub(batch.start_exclusive) != Some(records.len() as u64) {
        return Err("nonconsecutive derived batch".into());
    }
    for (index, record) in records.iter().enumerate() {
        if record.sequence != previous.sequence() + index as u64 + 1 {
            return Err("nonconsecutive derived record".into());
        }
        if let Outcome::Success { effects, .. } = &record.outcome {
            for effect in effects {
                match effect {
                    Effect::Put { table, key, value } => {
                        let live: bool = connection.query_row(
                            "SELECT live FROM catalogue WHERE table_id = ?1",
                            [table.to_be_bytes().as_slice()],
                            |row| row.get(0),
                        )?;
                        if !live {
                            return Err("data effect on a dropped table".into());
                        }
                        connection.execute(
                            "INSERT INTO mirror VALUES (?1, ?2, ?3, ?4)
                            ON CONFLICT(table_id, key) DO UPDATE SET value=excluded.value, \
                             value_hash=excluded.value_hash",
                            params![
                                table.to_be_bytes().as_slice(),
                                key,
                                value,
                                Sha256::digest(value).as_slice()
                            ],
                        )?;
                    }
                    Effect::Delete { table, key } => {
                        connection.execute(
                            "DELETE FROM mirror WHERE table_id = ?1 AND key = ?2",
                            params![table.to_be_bytes().as_slice(), key],
                        )?;
                    }
                    Effect::Catalogue { table, value } => {
                        let version = vm::decode_catalogue(*table, value)?;
                        connection.execute(
                            "INSERT INTO catalogue VALUES (?1, ?2, ?3, ?4, ?5)
                            ON CONFLICT(table_id) DO UPDATE SET name=excluded.name, \
                             live=excluded.live,
                                key_schema=excluded.key_schema, value_schema=excluded.value_schema",
                            params![
                                table.to_be_bytes().as_slice(),
                                version.name,
                                version.live,
                                version.table.key.descriptor(),
                                version.table.value.descriptor()
                            ],
                        )?;
                        if !version.live {
                            connection.execute(
                                "DELETE FROM mirror WHERE table_id = ?1",
                                [table.to_be_bytes().as_slice()],
                            )?;
                        }
                    }
                    Effect::Limits { .. } => {}
                }
            }
        }
        // This audit table retains record boundaries, including aborts and
        // no-write events. Its unique sequence also detects accidental replay.
        let encoded = vm::encode_outcome(
            record.sequence,
            record.record_digest,
            record.record_kind,
            &record.outcome,
        )?;
        connection.execute(
            "INSERT INTO source_records VALUES (?1, ?2)",
            params![record.sequence.to_be_bytes().as_slice(), encoded],
        )?;
    }
    let watermark = batch.watermark()?;
    connection.execute(
        "UPDATE progress SET watermark = ?1 WHERE singleton = 1",
        [watermark.encode().as_slice()],
    )?;
    Ok(watermark)
}

async fn consume(
    connection: &mut Connection,
    source: &Database,
    token: &CursorToken,
) -> Result<Watermark> {
    let (_, previous) = committed(connection)?.ok_or("missing derived watermark")?;
    let mut limits = BatchLimits {
        max_records: 64,
        max_bytes: 4 * 1024 * 1024,
    };
    let batch = loop {
        match db::read_feed(source, token, previous, limits).await {
            Ok(batch) => break batch,
            Err(db::Error::BatchTooSmall { required })
                if required > limits.max_bytes && required <= db::MAX_BATCH_BYTES =>
            {
                limits.max_bytes = required;
                limits.max_records = 1;
            }
            Err(error) => return Err(error.into()),
        }
    };
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let watermark = stage(&transaction, &batch)?;
    // If commit reports an error, do not acknowledge or retry staged
    // operations. Reopen SQLite and use committed() to discover which whole
    // commit survived.
    transaction.commit()?;
    db::acknowledge_cursor(source, token, watermark).await?;
    Ok(watermark)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let arguments: Vec<_> = std::env::args_os().skip(1).collect();
    if arguments.len() != 2 {
        return Err("usage: derived_sqlite SOURCE_DIRECTORY INDEX_SQLITE".into());
    }
    let source = db::open(&arguments[0]).await?;
    let mut connection = open_index(Path::new(&arguments[1]))?;
    let (token, mut previous) = resume(&mut connection, &source).await?;
    loop {
        let watermark = consume(&mut connection, &source, &token).await?;
        if watermark == previous {
            break;
        }
        previous = watermark;
    }
    println!("Committed source watermark: {}", previous.to_hex());
    db::close(&source).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use blop_db::tx;
    use blop_db::vm::CatalogueOperation;
    use blop_db::vm::Type;
    use blop_db::vm::Value;

    use super::*;

    async fn writes(source: &Database) {
        db::execute_catalogue(
            source,
            CatalogueOperation::Create {
                name: "data".into(),
                key: Type::U64,
                value: Type::U64,
            },
        )
        .await
        .unwrap();
        db::execute(
            source,
            tx! {
                tables { data: u64 => u64 = 1 } data[1] = 10; data[2] = 20;
            }
            .unwrap(),
            Default::default(),
        )
        .await
        .unwrap();
        db::execute(
            source,
            tx! {
                tables { data: u64 => u64 = 1 } data[1] = 99; abort(7);
            }
            .unwrap(),
            Default::default(),
        )
        .await
        .unwrap();
        db::execute(source, tx! { return 42; }.unwrap(), Default::default())
            .await
            .unwrap();
        db::execute_limits(source, Default::default())
            .await
            .unwrap();
    }

    fn verify(
        connection: &Connection,
        sequence: u64,
    ) {
        let count: i64 = connection
            .query_row("SELECT count(*) FROM source_records", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count as u64, sequence);
        let rows: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> = connection
            .prepare("SELECT key, value, value_hash FROM mirror ORDER BY key")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        let expected: Vec<_> = [(1_u64, 10_u64), (2, 20)]
            .map(|(key, value)| {
                let value = value.to_le_bytes().to_vec();
                (
                    key.to_be_bytes().to_vec(),
                    value.clone(),
                    Sha256::digest(&value).to_vec(),
                )
            })
            .into_iter()
            .collect();
        assert_eq!(rows, expected);
    }

    #[tokio::test]
    async fn crash_child() {
        let Some(root) = std::env::var_os("BLOP_DERIVED_CRASH_ROOT") else {
            return;
        };
        let root = std::path::PathBuf::from(root);
        let phase: u8 = std::env::var("BLOP_DERIVED_CRASH_PHASE")
            .unwrap()
            .parse()
            .unwrap();
        let source = db::open(root.join("source")).await.unwrap();
        let mut connection = open_index(&root.join("index.sqlite")).unwrap();
        let (token, previous) = resume(&mut connection, &source).await.unwrap();
        let batch = db::read_feed(&source, &token, previous, Default::default())
            .await
            .unwrap();
        let transaction = connection.transaction().unwrap();
        let next = stage(&transaction, &batch).unwrap();
        if phase == 0 {
            std::process::exit(77);
        }
        transaction.commit().unwrap();
        if phase == 2 {
            db::acknowledge_cursor(&source, &token, next).await.unwrap();
        }
        // No destructors, close, acknowledgement (phase 1), or completion
        // reply.
        std::process::exit(77);
    }

    #[tokio::test]
    async fn restart_before_commit_after_commit_before_ack_and_after_ack_uses_sqlite_watermark() {
        // Cases 0/1 also model the two possible recovered states of an
        // uncertain SQLite commit. The consumer never guesses which
        // side was selected.
        for phase in 0..3 {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("index.sqlite");
            let source_path = directory.path().join("source");
            let source = db::create(&source_path, Default::default()).await.unwrap();
            let mut connection = open_index(&path).unwrap();
            let (token, _) = resume(&mut connection, &source).await.unwrap();
            writes(&source).await;
            drop(connection);
            db::close(&source).await.unwrap();
            let exit = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "tests::crash_child", "--nocapture"])
                .env("BLOP_DERIVED_CRASH_ROOT", directory.path())
                .env("BLOP_DERIVED_CRASH_PHASE", phase.to_string())
                .status()
                .unwrap();
            assert_eq!(exit.code(), Some(77));
            let source = db::open(&source_path).await.unwrap();
            let mut connection = open_index(&path).unwrap();
            let recovered = committed(&connection).unwrap().unwrap().1;
            assert_eq!(recovered.sequence(), if phase == 0 { 0 } else { 5 });
            assert_eq!(
                db::reopen_cursor(&source, &token).await.unwrap().baseline,
                if phase == 2 { 5 } else { 0 }
            );
            let (token, resumed) = resume(&mut connection, &source).await.unwrap();
            assert_eq!(resumed, recovered);
            assert_eq!(
                consume(&mut connection, &source, &token)
                    .await
                    .unwrap()
                    .sequence(),
                5
            );
            assert_eq!(
                consume(&mut connection, &source, &token)
                    .await
                    .unwrap()
                    .sequence(),
                5
            );
            verify(&connection, 5);
            db::close(&source).await.unwrap();
        }
    }

    #[tokio::test]
    async fn catalogue_rename_drop_and_complete_record_rollback() {
        let directory = tempfile::tempdir().unwrap();
        let source = db::create(directory.path().join("source"), Default::default())
            .await
            .unwrap();
        let mut connection = open_index(&directory.path().join("index.sqlite")).unwrap();
        let (token, _) = resume(&mut connection, &source).await.unwrap();
        writes(&source).await;
        consume(&mut connection, &source, &token).await.unwrap();
        db::execute_catalogue(
            &source,
            CatalogueOperation::Rename {
                table: 1,
                name: "renamed".into(),
            },
        )
        .await
        .unwrap();
        consume(&mut connection, &source, &token).await.unwrap();
        assert_eq!(
            connection
                .query_row("SELECT name FROM catalogue", [], |r| r.get::<_, String>(0))
                .unwrap(),
            "renamed"
        );
        let snapshot = db::snapshot(&source).await.unwrap();
        assert_eq!(
            db::get(&snapshot, 1, &Value::U64(1)).unwrap(),
            Some(Value::U64(10))
        );
        db::execute_catalogue(&source, CatalogueOperation::Drop { table: 1 })
            .await
            .unwrap();
        consume(&mut connection, &source, &token).await.unwrap();
        assert_eq!(
            connection
                .query_row("SELECT count(*) FROM mirror", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert!(
            !connection
                .query_row("SELECT live FROM catalogue", [], |r| r.get::<_, bool>(0))
                .unwrap()
        );
        db::close(&source).await.unwrap();
    }

    #[tokio::test]
    async fn oversized_record_retries_without_splitting_effects_or_losing_progress() {
        let directory = tempfile::tempdir().unwrap();
        let source = db::create_with_options(
            directory.path().join("source"),
            Default::default(),
            db::EngineOptions {
                preparation_bytes: 1024 * 1024 * 1024,
                execution_bytes: 1024 * 1024 * 1024,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let path = directory.path().join("index.sqlite");
        let mut connection = open_index(&path).unwrap();
        let (token, _) = resume(&mut connection, &source).await.unwrap();
        db::execute_catalogue(
            &source,
            CatalogueOperation::Create {
                name: "data".into(),
                key: Type::U64,
                value: Type::Bytes(5 * 1024 * 1024),
            },
        )
        .await
        .unwrap();
        let previous = consume(&mut connection, &source, &token).await.unwrap();
        assert_eq!(previous.sequence(), 1);
        let payload = vec![7_u8; 5 * 1024 * 1024];
        let receipt = db::execute(
            &source,
            tx! {
                captures { payload: bytes<5242880> = payload }
                tables { data: u64 => bytes<5242880> = 1 }
                data[1] = payload;
            }
            .unwrap(),
            Default::default(),
        )
        .await
        .unwrap();
        let Outcome::Success { effects, .. } = &receipt.outcome else {
            panic!("large source write aborted");
        };
        let [
            Effect::Put {
                value: expected, ..
            },
        ] = effects.as_slice()
        else {
            panic!("unexpected large source effects");
        };
        db::execute(&source, tx! { return 42; }.unwrap(), Default::default())
            .await
            .unwrap();
        assert!(matches!(
            db::read_feed(&source, &token, previous, BatchLimits {
                max_records: 64, max_bytes: 4 * 1024 * 1024,
            }).await,
            Err(db::Error::BatchTooSmall { required })
                if required > 4 * 1024 * 1024 && required <= db::MAX_BATCH_BYTES
        ));
        assert_eq!(
            db::reopen_cursor(&source, &token).await.unwrap().baseline,
            1
        );
        let next = consume(&mut connection, &source, &token).await.unwrap();
        assert_eq!(next.sequence(), receipt.sequence);
        drop(connection);
        let mut connection = open_index(&path).unwrap();
        assert_eq!(committed(&connection).unwrap().unwrap().1, next);
        assert_eq!(
            db::reopen_cursor(&source, &token).await.unwrap().baseline,
            next.sequence()
        );
        let (value, hash): (Vec<u8>, Vec<u8>) = connection
            .query_row("SELECT value, value_hash FROM mirror", [], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .unwrap();
        assert_eq!(&value, expected);
        assert_eq!(hash, Sha256::digest(expected).to_vec());
        for _ in 0..2 {
            assert_eq!(
                consume(&mut connection, &source, &token)
                    .await
                    .unwrap()
                    .sequence(),
                3
            );
        }
        let count: i64 = connection
            .query_row("SELECT count(*) FROM source_records", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 3);
        db::close(&source).await.unwrap();
    }

    #[tokio::test]
    async fn wrong_source_and_unavailable_history_are_not_empty_rebuilds() {
        let directory = tempfile::tempdir().unwrap();
        let source = db::create(directory.path().join("source"), Default::default())
            .await
            .unwrap();
        let other = db::create(directory.path().join("other"), Default::default())
            .await
            .unwrap();
        let mut connection = open_index(&directory.path().join("index.sqlite")).unwrap();
        resume(&mut connection, &source).await.unwrap();
        assert!(resume(&mut connection, &other).await.is_err());
        writes(&other).await;
        db::maintain(&other, Default::default()).await.unwrap();
        let mut fresh = open_index(&directory.path().join("fresh.sqlite")).unwrap();
        assert!(resume(&mut fresh, &other).await.is_err());
        assert!(committed(&fresh).unwrap().is_none());
        db::close(&source).await.unwrap();
        db::close(&other).await.unwrap();
    }
}
