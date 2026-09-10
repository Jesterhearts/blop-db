//! Error injection exercises real files, but does not emulate power loss, lost
//! device caches, or reordered writes. Recovery always follows CURRENT.

use super::*;
use crate::storage::platform::faults::Event;
use crate::storage::platform::faults::Failure;
use crate::storage::platform::faults::Guard;
use crate::storage::platform::faults::Operation;
use crate::storage::platform::faults::Phase;
use crate::vm;

fn publication_fixture(extend: bool) -> (tempfile::TempDir, Store, Manifest) {
    let (directory, mut store) = new_store();
    let policy = store.genesis.initial_policy.clone();
    let (mut manifest, digests) = write_log(&store, 1);
    vm::execute_limits(&mut store, 1, digests[1], &policy).unwrap();
    apply(&mut store, &[cursor(1, 0)]).unwrap();
    manifest.checkpoint_sequence = 1;
    manifest.checkpoint_digest = digests[1];
    manifest.next_cursor_id = 2;
    let checkpoint = view(&store);
    publish(&mut store, &checkpoint, manifest).unwrap();
    drop(checkpoint);

    let (mut next, digests) = write_log(&store, 2);
    if extend {
        let added = next.segments.pop().unwrap();
        let added_path = store
            .directory
            .join(format!("log-{:020}.bin", added.segment_id));
        let bytes = fs::read(&added_path).unwrap();
        let segment = next.segments.last_mut().unwrap();
        OpenOptions::new()
            .append(true)
            .open(
                store
                    .directory
                    .join(format!("log-{:020}.bin", segment.segment_id)),
            )
            .unwrap()
            .write_all(&bytes[96..])
            .unwrap();
        segment.last_sequence = added.last_sequence;
        segment.last_digest = added.last_digest;
        segment.committed_bytes += added.committed_bytes - 96;
        fs::remove_file(added_path).unwrap();
    }
    for sequence in 2..=3 {
        vm::execute_limits(
            &mut store,
            sequence,
            digests[(sequence - 1) as usize],
            &policy,
        )
        .unwrap();
    }
    apply(&mut store, &[cursor(2, 1)]).unwrap();
    next.checkpoint_sequence = 3;
    next.checkpoint_digest = next.durable_digest;
    next.next_cursor_id = 3;
    (directory, store, next)
}

fn contents(view: &View) -> [Vec<Entry>; 5] {
    TREES.map(|tree| {
        scan(view, tree, Bound::Unbounded, Bound::Unbounded)
            .unwrap()
            .collect::<Result<_>>()
            .unwrap()
    })
}

#[test]
fn every_publication_boundary_selects_exactly_one_complete_checkpoint() {
    for extend in [false, true] {
        let (_directory, mut store, next) = publication_fixture(extend);
        let checkpoint = view(&store);
        let guard = Guard::new(None);
        publish(&mut store, &checkpoint, next).unwrap();
        let trace = guard.trace();
        drop(guard);
        let current_rename = trace
            .iter()
            .position(|event| *event == Event(Operation::Rename("CURRENT".into()), Phase::Before))
            .unwrap();
        assert_eq!(trace.len(), if extend { 18 } else { 20 });
        for boundary in 0..trace.len() {
            let (_directory, mut store, mut next) = publication_fixture(extend);
            let path = store.directory.clone();
            let old = store.manifest.clone();
            let old_contents = contents(&checkpoint_view(&store));
            let checkpoint = view(&store);
            let new_contents = contents(&checkpoint);
            let guard = Guard::new(Some((boundary, Failure::Error)));
            assert!(
                matches!(
                    publish(&mut store, &checkpoint, next.clone()),
                    Err(Error::Io(_))
                ),
                "extend={extend}, boundary={boundary}, event={:?}",
                trace[boundary]
            );
            assert_eq!(guard.trace(), trace[..=boundary]);
            drop(guard);
            assert!(matches!(apply(&mut store, &[]), Err(Error::NeedsRecovery)));
            next.generation = old.generation + 1;
            next.roots = checkpoint.roots;
            next.page_count = store.pages.page_count();
            let current = Current::decode(&fs::read(path.join("CURRENT")).unwrap()).unwrap();
            if boundary <= current_rename {
                assert_eq!(current.generation, old.generation);
            }
            let (expected, expected_contents) = if current.generation == old.generation {
                (old, old_contents)
            } else {
                assert_eq!(current.generation, next.generation);
                (next, new_contents)
            };
            assert_eq!(
                current.digest,
                <[u8; 32]>::from(Sha256::digest(expected.encode().unwrap()))
            );
            drop(checkpoint);
            drop(store);
            let reopened = open(&path).unwrap();
            assert_eq!(reopened.selected, expected);
            assert_eq!(reopened.manifest.durable_sequence, 3);
            assert_eq!(contents(&view(&reopened)), expected_contents);
            drop(reopened);
            // Neither an intact older manifest nor a plausible marker may
            // repair missing authority. The protocol has no
            // completion-marker fallback.
            fs::write(path.join("COMPLETE"), b"complete").unwrap();
            fs::remove_file(path.join("CURRENT")).unwrap();
            assert!(matches!(open(&path), Err(Error::Corrupt(_))));
        }
    }
}

#[test]
fn pending_metadata_does_not_select_checkpoint_and_short_tails_are_trimmed() {
    for extend in [false, true] {
        let (_directory, store, next) = publication_fixture(extend);
        let path = store.directory.clone();
        let old = store.manifest.clone();
        let expected = contents(&checkpoint_view(&store));
        let manifest = next.encode().unwrap();
        fs::write(
            path.join("manifest.pending"),
            &manifest[..manifest.len() / 2],
        )
        .unwrap();
        fs::write(path.join("CURRENT.pending"), b"BLOPCU01\x01").unwrap();
        fs::write(path.join("manifest-00000000000000000999.bin"), b"partial").unwrap();
        fs::write(path.join("COMPLETE"), b"complete").unwrap();
        let pages = path.join(format!("pages-{:020}.bin", old.page_file_id));
        OpenOptions::new()
            .append(true)
            .open(&pages)
            .unwrap()
            .write_all(b"partial page")
            .unwrap();
        let active = next.segments.last().unwrap();
        let log = path.join(format!("log-{:020}.bin", active.segment_id));
        OpenOptions::new()
            .append(true)
            .open(&log)
            .unwrap()
            .write_all(b"partial record")
            .unwrap();
        drop(store);
        let reopened = open(&path).unwrap();
        assert_eq!(reopened.selected, old);
        assert_eq!(reopened.manifest.durable_sequence, 3);
        assert_eq!(contents(&view(&reopened)), expected);
        assert_eq!(fs::metadata(&pages).unwrap().len(), old.page_count * 16_384);
        assert_eq!(fs::metadata(&log).unwrap().len(), active.committed_bytes);
        drop(reopened);
        // Short selected files are committed corruption, not discarded tails.
        let selected_log = path.join("log-00000000000000000001.bin");
        for (selected, committed) in [
            (&pages, old.page_count * 16_384),
            (&selected_log, old.segments[0].committed_bytes),
        ] {
            let original = fs::read(selected).unwrap();
            OpenOptions::new()
                .write(true)
                .open(selected)
                .unwrap()
                .set_len(committed - 1)
                .unwrap();
            assert!(matches!(open(&path), Err(Error::Corrupt(_))));
            assert_eq!(fs::metadata(selected).unwrap().len(), committed - 1);
            fs::write(selected, original).unwrap();
        }
    }
}

fn multiwrite_fixture() -> (tempfile::TempDir, Store, crate::Transaction) {
    let (directory, mut store) = new_store();
    let policy = crate::Limits::default().try_into().unwrap();
    vm::execute_limits(&mut store, 1, [1; 32], &policy).unwrap();
    vm::execute_catalogue(
        &mut store,
        2,
        [2; 32],
        &vm::CatalogueOperation::Create {
            name: "items".into(),
            key: vm::Type::U64,
            value: vm::Type::Bytes(20_000),
        },
    )
    .unwrap();
    let payload = vec![7_u8; 20_000];
    let transaction = crate::tx! {
        captures { payload: bytes<20000> = payload }
        tables { items: u64 => bytes<20000> = 2 }
        items[1] = payload; items[2] = payload; items[3] = payload;
        return 42;
    }
    .unwrap();
    (directory, store, transaction)
}

#[test]
fn every_multiwrite_failure_preserves_live_roots_and_installs_no_outcome() {
    let (_directory, mut store, transaction) = multiwrite_fixture();
    let policy = crate::Limits::default().try_into().unwrap();
    let guard = Guard::new(None);
    let expected = vm::execute(&mut store, 3, [3; 32], &transaction, &policy).unwrap();
    assert!(matches!(expected, vm::Outcome::Success { .. }));
    let trace = guard.trace();
    drop(guard);
    assert!(trace.len() >= 12, "must exercise several real page writes");
    assert_eq!(
        vm::read_outcome(&view(&store), 3).unwrap().unwrap().outcome,
        expected
    );
    for (boundary, event) in trace.iter().enumerate() {
        let modes: &[Failure] = if event.1 == Phase::Before {
            &[Failure::Error, Failure::PartialWrite]
        } else {
            &[Failure::Error]
        };
        for &mode in modes {
            let (_directory, mut store, transaction) = multiwrite_fixture();
            let pinned = view(&store);
            let before = contents(&pinned);
            let roots = store.roots;
            let guard = Guard::new(Some((boundary, mode)));
            assert!(
                matches!(
                    vm::execute(&mut store, 3, [3; 32], &transaction, &policy),
                    Err(vm::Error::Storage(Error::Io(_)))
                ),
                "boundary={boundary}, mode={mode:?}"
            );
            assert_eq!(guard.trace(), trace[..=boundary]);
            drop(guard);
            assert_eq!(store.roots, roots);
            assert_eq!(contents(&pinned), before);
            assert_eq!(contents(&view(&store)), before);
            assert!(vm::read_outcome(&view(&store), 3).unwrap().is_none());
            assert!(matches!(apply(&mut store, &[]), Err(Error::NeedsRecovery)));
        }
    }
}
