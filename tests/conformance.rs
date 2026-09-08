#![cfg(any(unix, windows))]

use std::ops::Bound::Unbounded;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;

use blop_db::Database;
use blop_db::Limits;
use blop_db::Transaction;
use blop_db::database as db;
use blop_db::storage;
use blop_db::tx;
use blop_db::vm;
use storage::LimitPolicy;
use storage::Mutation;
use storage::Store;
use storage::TreeId;
use storage::View;
use storage::mvcc::StateKey;
use storage::mvcc::StateValue;
use vm::Abort;
use vm::AbortReason;
use vm::CatalogueOperation;
use vm::Effect;
use vm::Outcome;
use vm::Type;
use vm::Value;

fn policy() -> LimitPolicy {
    Limits::default().try_into().unwrap()
}

fn fixture() -> (tempfile::TempDir, Store) {
    let directory = tempfile::tempdir().unwrap();
    let mut store = storage::create(
        directory.path().join("db"),
        storage::Genesis {
            database_id: [1; 16],
            initial_policy: policy(),
        },
        [2; 16],
    )
    .unwrap();
    vm::execute_catalogue(
        &mut store,
        1,
        [1; 32],
        &CatalogueOperation::Create {
            name: "items".into(),
            key: Type::U64,
            value: Type::I64,
        },
    )
    .unwrap();
    (directory, store)
}

fn entries(
    view: &View,
    tree: TreeId,
) -> Vec<storage::Entry> {
    storage::scan(view, tree, Unbounded, Unbounded)
        .unwrap()
        .collect::<storage::Result<_>>()
        .unwrap()
}

fn version(
    key: u64,
    sequence: u64,
    value: Option<i64>,
) -> Mutation {
    Mutation {
        tree: TreeId::State,
        key: StateKey::new(1, key.to_be_bytes().to_vec(), sequence)
            .unwrap()
            .encode(),
        value: Some(
            value
                .map_or(StateValue::Delete, |value| {
                    StateValue::Put(Value::I64(value).encode())
                })
                .encode()
                .unwrap(),
        ),
    }
}

fn scan_fixture(shape: usize) -> (tempfile::TempDir, Store) {
    let (directory, mut store) = fixture();
    for sequence in 2..5 {
        vm::execute(
            &mut store,
            sequence,
            [sequence as u8; 32],
            &tx! {}.unwrap(),
            &policy(),
        )
        .unwrap();
    }
    vm::execute(
        &mut store,
        5,
        [5; 32],
        &tx! {
            tables { items: u64 => i64 = 1 }
            items[1] = 10; delete(items[2]); items[3] = 30; items[4] = 40;
        }
        .unwrap(),
        &policy(),
    )
    .unwrap();
    if shape != 0 {
        // Only physical MVCC fixture data is manufactured. Catalogue, policy
        // and baseline outcomes above came through the real reference
        // interpreter.
        let mut changes = Vec::new();
        for key in 1..=4 {
            for sequence in 2..5 {
                changes.push(version(key, sequence, Some(-1)));
            }
        }
        changes.push(version(6, 6, Some(60)));
        changes.push(version(6, 8, None));
        for key in 0..256 {
            for sequence in [12, 15, 20] {
                changes.push(version(key, sequence, Some(999)));
            }
        }
        if shape == 2 {
            changes.reverse();
        }
        storage::apply(&mut store, &changes).unwrap();
    }
    (directory, store)
}

fn decoded(outcome: &Outcome) -> Outcome {
    vm::decode_outcome(&vm::encode_outcome(11, [11; 32], 1, outcome).unwrap())
        .unwrap()
        .outcome
}

#[test]
fn scan_outcomes_ignore_page_shapes_invisible_versions_and_overlay_history() {
    let fixtures: Vec<_> = (0..3).map(scan_fixture).collect();
    let views: Vec<_> = fixtures
        .iter()
        .map(|(_, store)| storage::view(store))
        .collect();
    assert_eq!(entries(&views[0], TreeId::State).len(), 4);
    assert!(entries(&views[1], TreeId::State).len() > 750);
    assert_eq!(
        entries(&views[1], TreeId::State),
        entries(&views[2], TreeId::State)
    );
    let exact = tx! { tables { items: u64 => i64 = 1 }
    return scan_bounded(items, unbounded, unbounded, 0, 3, 48); }
    .unwrap();
    let two = tx! { tables { items: u64 => i64 = 1 }
    return scan_bounded(items, unbounded, unbounded, 0, 2, 32); }
    .unwrap();
    let short = tx! { tables { items: u64 => i64 = 1 }
    return scan_bounded(items, unbounded, unbounded, 0, 3, 47); }
    .unwrap();
    let twice = tx! { tables { items: u64 => i64 = 1 }
    scan_bounded(items, unbounded, unbounded, 0, 2, 32);
    return scan_bounded(items, unbounded, unbounded, 0, 2, 32); }
    .unwrap();
    let zero = tx! { tables { items: u64 => i64 = 1 }
    return scan_bounded(items, unbounded, unbounded, 0, 0, 0); }
    .unwrap();
    let empty = tx! { tables { items: u64 => i64 = 1 }
    return scan_bounded(items, 2, 2, 3, 3, 0); }
    .unwrap();
    let overlay = tx! { tables { items: u64 => i64 = 1 }
    items[1] = 11; items[2] = 20; delete(items[3]);
    return scan_bounded(items, unbounded, unbounded, 0, 3, 48); }
    .unwrap();
    let churn = tx! { tables { items: u64 => i64 = 1 }
    items[1] = -1; delete(items[1]); items[1] = 11;
    items[2] = -2; delete(items[2]); items[2] = 20;
    items[3] = -3; delete(items[3]);
    return scan_bounded(items, unbounded, unbounded, 0, 3, 48); }
    .unwrap();

    let mut cases = vec![
        (&exact, policy(), None, vec![(1, 10), (3, 30), (4, 40)]),
        (&two, policy(), None, vec![(1, 10), (3, 30)]),
        (&short, policy(), Some(14), vec![]),
        (&zero, policy(), None, vec![]),
        (&empty, policy(), None, vec![]),
        (&overlay, policy(), None, vec![(1, 11), (2, 20), (4, 40)]),
    ];
    // The row/key checks precede destination-register assignment. Deliberately
    // overlap exhausted budgets so a different charge order cannot pass.
    for (transaction, limits, expected) in [
        (&exact, vec![(9, 2), (12, 0)], 9),
        (&twice, vec![(12, 36), (13, 3), (14, 48)], 13),
        (&twice, vec![(12, 36), (14, 63)], 14),
        (&exact, vec![(11, 51), (12, 0)], 11),
        (&exact, vec![(12, 51)], 12),
        (&exact, vec![(10, 7), (11, 7), (9, 0)], 10),
        (&exact, vec![(11, 7), (9, 0)], 11),
    ] {
        let mut values = *policy().values();
        for (id, value) in limits {
            values[id - 1] = value;
        }
        cases.push((
            transaction,
            LimitPolicy::new(values).unwrap(),
            Some(expected),
            vec![],
        ));
    }
    let mut exact_claims = *policy().values();
    for (id, value) in [(9, 3), (10, 8), (11, 52), (12, 52), (13, 3), (14, 48)] {
        exact_claims[id - 1] = value;
    }
    cases.push((
        &exact,
        LimitPolicy::new(exact_claims).unwrap(),
        None,
        vec![(1, 10), (3, 30), (4, 40)],
    ));
    for (transaction, claims, resource, rows) in cases {
        let mut outcomes = Vec::new();
        for view in &views {
            let prepared = vm::prepare_transaction(view, 11, transaction, &claims, None).unwrap();
            let outcome = decoded(&vm::interpret_prepared(view, &prepared).unwrap());
            match (&outcome, resource) {
                (Outcome::Aborted(abort), Some(detail)) => {
                    assert_eq!(abort.reason, AbortReason::ResourceLimit);
                    assert_eq!(abort.detail, detail);
                }
                (Outcome::Success { value, .. }, None) => {
                    assert_eq!(
                        *value,
                        Value::Rows(
                            rows.iter()
                                .map(|&(key, value)| (Value::U64(key), Value::I64(value)))
                                .collect()
                        )
                    );
                }
                _ => panic!("unexpected outcome: {outcome:?}, expected resource {resource:?}"),
            }
            outcomes.push(outcome);
        }
        assert!(outcomes.windows(2).all(|pair| pair[0] == pair[1]));
    }
    let mut short_register = *policy().values();
    short_register[10] = 51;
    for view in &views {
        for claims in [policy(), LimitPolicy::new(short_register).unwrap()] {
            let outcomes: Vec<_> = [&overlay, &churn]
                .into_iter()
                .map(|transaction| {
                    let prepared =
                        vm::prepare_transaction(view, 11, transaction, &claims, None).unwrap();
                    decoded(&vm::interpret_prepared(view, &prepared).unwrap())
                })
                .collect();
            if claims.values()[10] == 51 {
                for outcome in outcomes {
                    assert!(matches!(
                        outcome,
                        Outcome::Aborted(Abort {
                            reason: AbortReason::ResourceLimit,
                            detail: 11,
                            ..
                        })
                    ));
                }
            } else {
                assert_eq!(outcomes[0], outcomes[1]);
            }
        }
        assert!(vm::read_outcome(view, 11).unwrap().is_none());
    }
}

fn random(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn mutate(
    bytes: &[u8],
    iteration: usize,
    state: &mut u64,
) -> Vec<u8> {
    let mut bytes = bytes.to_vec();
    let offset = random(state) as usize % bytes.len().max(1);
    match iteration % 6 {
        0 => bytes.truncate(iteration / 6 % (bytes.len() + 1)),
        1 if !bytes.is_empty() => bytes[offset] ^= 1 << (random(state) % 8),
        2 if !bytes.is_empty() => {
            let end = (offset + 4).min(bytes.len());
            bytes[offset..end].fill(if iteration & 8 == 0 { 0xff } else { 0 });
        }
        3 => bytes
            .splice(offset..offset, [random(state) as u8; 4])
            .for_each(drop),
        4 if !bytes.is_empty() => {
            bytes.remove(offset);
        }
        _ if !bytes.is_empty() => bytes[offset] = random(state) as u8,
        _ => {}
    }
    bytes
}

#[test]
fn seeded_bytecode_and_argument_mutations_never_panic_or_install_rejections() {
    let (_directory, mut store) = fixture();
    let corpus = [
        tx! { captures { key: u64 = 7_u64, value: i64 = 42_i64, gate: bool = true }
            tables { items: u64 => i64 = 1 }
            items[key] = value;
            if gate { return items[key]; } else { abort(9); }
        }
        .unwrap(),
        tx! { captures { text: string<16> = "text", raw: bytes<8> = b"\0\xff",
            pair: (bool, u64) = (false, 7_u64) }
            return (text, raw, pair);
        }
        .unwrap(),
    ];
    let view = storage::view(&store);
    let trees = [
        TreeId::State,
        TreeId::Catalogue,
        TreeId::Policy,
        TreeId::Outcomes,
    ];
    let before = trees.map(|tree| entries(&view, tree));
    let mut state = 0x96c7_4d02_a9b3_8165;
    let mut rejected = [0; 2];
    let mut accepted = 0;
    for transaction in &corpus {
        for iteration in 0..4096 {
            for (side, rejected) in rejected.iter_mut().enumerate() {
                let changed = mutate(
                    if side == 0 {
                        transaction.program_bytes()
                    } else {
                        transaction.argument_bytes()
                    },
                    iteration,
                    &mut state,
                );
                let (program, arguments) = if side == 0 {
                    (changed.as_slice(), transaction.argument_bytes())
                } else {
                    (transaction.program_bytes(), changed.as_slice())
                };
                let result = catch_unwind(AssertUnwindSafe(|| {
                    vm::interpret(&view, 2, program, arguments, &policy())
                }))
                .unwrap_or_else(|_| panic!("seeded VM panic: iteration={iteration}, side={side}"));
                match result {
                    Err(_) => {
                        *rejected += 1;
                        assert!(
                            vm::execute_bytes(
                                &mut store,
                                2,
                                [2; 32],
                                program,
                                arguments,
                                &policy()
                            )
                            .is_err()
                        );
                        assert_eq!(
                            trees.map(|tree| entries(&storage::view(&store), tree)),
                            before
                        );
                    }
                    Ok(outcome) => {
                        accepted += 1;
                        // A mutated but accepted program can validly succeed or
                        // abort.
                        let encoded = vm::encode_outcome(2, [2; 32], 1, &outcome).unwrap();
                        assert_eq!(vm::decode_outcome(&encoded).unwrap().outcome, outcome);
                    }
                }
            }
        }
    }
    assert!(rejected.into_iter().all(|count| count > 1000));
    assert!(accepted > 0);
    assert!(
        vm::read_outcome(&storage::view(&store), 2)
            .unwrap()
            .is_none()
    );
}

#[test]
fn seeded_type_descriptor_mutations_are_bounded_and_panic_free() {
    let corpus = [
        Type::Tuple(vec![
            Type::Boolean,
            Type::Bytes(8),
            Type::String(16),
            Type::Tuple(vec![Type::U64]),
        ]),
        Type::Rows {
            max_rows: 3,
            key: Box::new(Type::U64),
            value: Box::new(Type::I64),
        },
        Type::Tuple(vec![Type::Tuple(vec![Type::Unit; 32]); 4]),
    ];
    let mut state = 0x6d54_7c13_e02b_98a1;
    for ty in corpus {
        let original = ty.descriptor();
        for iteration in 0..4096 {
            let bytes = mutate(&original, iteration, &mut state);
            catch_unwind(|| {
                if let Ok(ty) = Type::decode(&bytes) {
                    assert_eq!(Type::decode(&ty.descriptor()).unwrap(), ty);
                }
                let _ = storage::encoding::Schema::decode(&bytes);
            })
            .unwrap_or_else(|_| panic!("seeded descriptor panic: iteration={iteration}"));
        }
    }
}

#[test]
fn seeded_access_and_storage_manifest_mutations_are_panic_free() {
    let (_directory, store) = fixture();
    let access = vm::AccessManifest::new([
        (vm::Scope::Table(1), vm::AccessMode::Read),
        (
            vm::Scope::Key(1, 7_u64.to_be_bytes().to_vec()),
            vm::AccessMode::Write,
        ),
    ])
    .unwrap()
    .encode()
    .unwrap();
    let mut manifest = store.manifest().clone();
    manifest.durable_sequence = 2;
    manifest.durable_digest = [4; 32];
    manifest.next_segment_id = 2;
    manifest.segments.push(storage::SegmentDescriptor {
        segment_id: 1,
        first_sequence: 1,
        last_sequence: 2,
        committed_bytes: 520,
        predecessor_digest: manifest.genesis_digest,
        last_digest: [4; 32],
    });
    let manifest = manifest.encode().unwrap();
    let mut state = 0xa581_3c7d_8e29_b604;
    for iteration in 0..8192 {
        let access = mutate(&access, iteration, &mut state);
        let mut bytes = mutate(&manifest, iteration, &mut state);
        if iteration % 2 == 0 && bytes.len() >= 4 {
            // Reach semantic validation as well as checksum rejection.
            let end = bytes.len() - 4;
            let crc = crc32c::crc32c(&bytes[..end]);
            bytes[end..].copy_from_slice(&crc.to_le_bytes());
        }
        catch_unwind(|| {
            if let Ok(value) = vm::AccessManifest::decode(&access) {
                assert_eq!(value.encode().unwrap(), access);
            }
            if let Ok(value) = storage::Manifest::decode(&bytes) {
                assert_eq!(value.encode().unwrap(), bytes);
            }
        })
        .unwrap_or_else(|_| panic!("seeded manifest panic: iteration={iteration}"));
    }
}

#[test]
fn seeded_outcome_mutations_validate_complete_frames_without_panicking() {
    let corpus = [
        Outcome::Success {
            result_type: Type::Tuple(vec![Type::String(8), Type::I64]),
            value: Value::Tuple(vec![Value::String("result".into()), Value::I64(42)]),
            effects: vec![
                Effect::Put {
                    table: 1,
                    key: 1_u64.to_be_bytes().to_vec(),
                    value: Value::I64(42).encode(),
                },
                Effect::Delete {
                    table: 1,
                    key: 2_u64.to_be_bytes().to_vec(),
                },
            ],
        },
        Outcome::Success {
            result_type: Type::Rows {
                max_rows: 2,
                key: Box::new(Type::U64),
                value: Box::new(Type::I64),
            },
            value: Value::Rows(vec![
                (Value::U64(1), Value::I64(10)),
                (Value::U64(3), Value::I64(30)),
            ]),
            effects: vec![],
        },
        Outcome::Aborted(Abort {
            reason: AbortReason::ResourceLimit,
            instruction: 5,
            user_code: 0,
            detail: 14,
        }),
    ];
    let mut state = 0x835a_91c6_42fd_70be;
    for outcome in corpus {
        let original = vm::encode_outcome(11, [11; 32], 1, &outcome).unwrap();
        for iteration in 0..4096 {
            let bytes = mutate(&original, iteration, &mut state);
            catch_unwind(|| {
                if let Ok(record) = vm::decode_outcome(&bytes) {
                    let encoded = vm::encode_outcome(
                        record.sequence,
                        record.record_digest,
                        record.record_kind,
                        &record.outcome,
                    )
                    .unwrap();
                    assert_eq!(vm::decode_outcome(&encoded).unwrap(), record);
                }
            })
            .unwrap_or_else(|_| panic!("seeded outcome panic: iteration={iteration}"));
        }
    }
}

async fn table(
    database: &Database,
    name: &str,
    value: Type,
) -> u64 {
    let receipt = db::execute_catalogue(
        database,
        CatalogueOperation::Create {
            name: name.into(),
            key: Type::U64,
            value,
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

fn request(
    business: u64,
    requests: u64,
    id: u64,
    payload: &[u8],
    abort: bool,
) -> Transaction {
    tx! {
        captures { id: u64 = id, payload: bytes<16> = payload, cancel: bool = abort }
        tables { business: u64 => i64 = business, requests: u64 => (bytes<16>, i64) = requests }
        let result = business[0] + 1;
        business[0] = result;
        insert(requests[id], (payload, result));
        require(!cancel, 77);
        return result;
    }
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn caller_request_ids_atomically_deduplicate_business_writes_and_bounded_results() {
    let directory = tempfile::tempdir().unwrap();
    let database = db::create(directory.path().join("db"), db::CreateOptions::default())
        .await
        .unwrap();
    let business = table(&database, "business", Type::I64).await;
    let requests = table(
        &database,
        "requests",
        Type::Tuple(vec![Type::Bytes(16), Type::I64]),
    )
    .await;
    db::execute(
        &database,
        tx! { tables { business: u64 => i64 = business } business[0] = 0; }.unwrap(),
        Limits::default(),
    )
    .await
    .unwrap();
    let aborted = db::execute(
        &database,
        request(business, requests, 1, b"payload", true),
        Limits::default(),
    )
    .await
    .unwrap();
    assert!(matches!(
        aborted.outcome,
        Outcome::Aborted(Abort {
            reason: AbortReason::RequireFailed,
            user_code: 77,
            ..
        })
    ));
    let snapshot = db::snapshot(&database).await.unwrap();
    assert_eq!(
        db::get(&snapshot, business, &Value::U64(0)).unwrap(),
        Some(Value::I64(0))
    );
    assert_eq!(db::get(&snapshot, requests, &Value::U64(1)).unwrap(), None);
    drop(snapshot);

    let transaction = request(business, requests, 1, b"payload", false);
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let database = database.clone();
        let transaction = transaction.clone();
        tasks.push(tokio::spawn(async move {
            db::execute(&database, transaction, Limits::default())
                .await
                .unwrap()
        }));
    }
    let mut successes = 0;
    for task in tasks {
        match task.await.unwrap().outcome {
            Outcome::Success { value, .. } => {
                assert_eq!(value, Value::I64(1));
                successes += 1;
            }
            Outcome::Aborted(abort) => assert_eq!(abort.reason, AbortReason::KeyExists),
        }
    }
    assert_eq!(successes, 1);
    for payload in [b"payload".as_slice(), b"different"] {
        let receipt = db::execute(
            &database,
            request(business, requests, 1, payload, false),
            Limits::default(),
        )
        .await
        .unwrap();
        assert!(matches!(
            receipt.outcome,
            Outcome::Aborted(Abort {
                reason: AbortReason::KeyExists,
                ..
            })
        ));
    }

    // Dropping after one poll deliberately leaves admission uncertain. No
    // private scheduler hook is used, and the test does not assume that
    // enqueue occurred.
    let cancelled = request(business, requests, 2, b"cancelled", false);
    let mut pending = Box::pin(db::execute(&database, cancelled.clone(), Limits::default()));
    let first =
        std::future::poll_fn(|context| std::task::Poll::Ready(pending.as_mut().poll(context)))
            .await;
    drop(pending);
    let retry = db::execute(&database, cancelled, Limits::default())
        .await
        .unwrap();
    if let std::task::Poll::Ready(result) = first {
        assert!(matches!(result.unwrap().outcome, Outcome::Success { .. }));
        assert!(matches!(
            retry.outcome,
            Outcome::Aborted(Abort {
                reason: AbortReason::KeyExists,
                ..
            })
        ));
    } else {
        assert!(matches!(
            retry.outcome,
            Outcome::Success { .. }
                | Outcome::Aborted(Abort {
                    reason: AbortReason::KeyExists,
                    ..
                })
        ));
    }
    let snapshot = db::snapshot(&database).await.unwrap();
    assert_eq!(
        db::get(&snapshot, business, &Value::U64(0)).unwrap(),
        Some(Value::I64(2))
    );
    let records = db::scan(&snapshot, requests, Unbounded, Unbounded)
        .unwrap()
        .collect::<db::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        records,
        vec![
            (
                Value::U64(1),
                Value::Tuple(vec![Value::Bytes(b"payload".to_vec()), Value::I64(1)])
            ),
            (
                Value::U64(2),
                Value::Tuple(vec![Value::Bytes(b"cancelled".to_vec()), Value::I64(2)])
            ),
        ]
    );
    assert!(
        records
            .iter()
            .all(|(_, value)| value.encoded_len() <= 4 + 16 + 8)
    );
    drop(snapshot);
    db::close(&database).await.unwrap();
}
