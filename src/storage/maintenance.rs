//! Private candidate construction and whole-file reclamation. Logical history
//! validation belongs to the database coordinator before `install`.

use std::fs;
use std::fs::OpenOptions;
use std::ops::Bound::Unbounded;
use std::sync::Arc;

use super::Error;
use super::Manifest;
use super::Result;
use super::Store;
use super::TreeId;
use super::View;
use super::mvcc::StateKey;
use super::page::Node;
use super::page::PageFile;
use super::page::PageReader;
use super::platform;
use super::store;
use super::tree;

pub(crate) struct Candidate {
    pub(crate) view: View,
    pub(crate) manifest: Manifest,
    pub(crate) versions_removed: u64,
    pub(crate) outcomes_removed: u64,
    pages: Option<PageFile>,
    source_roots: [u64; 5],
}

/// Descending physical versions make the first version at/below G the baseline.
/// F bounds G, never the versions selected for preservation.
fn keep_state(
    key: &StateKey,
    floor: u64,
    baseline: &mut Option<(u64, Vec<u8>)>,
) -> bool {
    if key.sequence() > floor {
        return true;
    }
    if baseline
        .as_ref()
        .is_some_and(|(table, address)| *table == key.table_id() && address.as_slice() == key.key())
    {
        return false;
    }
    *baseline = Some((key.table_id(), key.key().to_vec()));
    true
}

/// Build pruned roots without replacing live roots. This also works with holes
/// and installed versions above F; those versions must all survive.
pub(crate) fn collect_view(
    store: &mut Store,
    floor: u64,
    frontier: u64,
) -> Result<(View, u64, u64)> {
    store::writable(store)?;
    if floor > frontier || floor < store.manifest().history_floor {
        return Err(Error::InvalidInput("invalid collection floor"));
    }
    let source = store::view(store);
    let mut roots = source.roots;
    let mut baseline = None;
    let mut removed = [0; 2];
    for (index, tree) in [TreeId::State, TreeId::Outcomes].into_iter().enumerate() {
        for entry in store::scan(&source, tree, Unbounded, Unbounded)? {
            let (key, _) = entry?;
            let keep = match tree {
                TreeId::State => keep_state(&StateKey::decode(&key)?, floor, &mut baseline),
                _ => store::entry_sequence(tree, &key)? > floor,
            };
            if !keep {
                roots[tree.index()] =
                    tree::delete(&mut store.pages, tree, roots[tree.index()], &key)?;
                removed[index] += 1;
            }
        }
    }
    Ok((
        View {
            roots,
            ..store::view(store)
        },
        removed[0],
        removed[1],
    ))
}

pub(crate) fn prepare(
    store: &mut Store,
    history: u64,
    log: u64,
    collect: bool,
    compact: bool,
) -> Result<Candidate> {
    store::writable(store)?;
    let mut manifest = store.manifest().clone();
    let source_roots = store::view(store).roots;
    if manifest.checkpoint_sequence != manifest.durable_sequence
        || history > manifest.checkpoint_sequence
        || log > manifest.checkpoint_sequence + 1
        || history < manifest.history_floor
        || log < manifest.log_floor
    {
        return Err(Error::InvalidInput(
            "maintenance requires valid floors and a drained checkpoint",
        ));
    }
    let (mut view, versions_removed, outcomes_removed) = if collect {
        collect_view(store, history, manifest.checkpoint_sequence)?
    } else {
        (store::view(store), 0, 0)
    };
    if collect {
        manifest.history_floor = history;
        manifest
            .segments
            .retain(|segment| segment.last_sequence >= log);
        manifest.log_floor = manifest
            .segments
            .first()
            .map_or(manifest.durable_sequence + 1, |segment| {
                segment.first_sequence
            });
    }
    let pages = if compact {
        let (file, id) = allocate(store, "pages", manifest.next_page_file_id)?;
        let mut pages = PageFile::create(file, manifest.database_id, id)?;
        let mut roots = [0; 5];
        for tree in store::TREES {
            tree::validate(&view.reader, tree, view.roots[tree.index()])?;
            roots[tree.index()] =
                copy_tree(&view.reader, &mut pages, tree, view.roots[tree.index()])?;
        }
        view = View {
            reader: pages.reader(),
            lease: Arc::clone(&store.lease),
            roots,
            validation: [None; 4],
        };
        manifest.page_file_id = id;
        manifest.next_page_file_id = id + 1;
        manifest.page_count = pages.page_count();
        Some(pages)
    } else {
        manifest.page_count = store.pages.page_count();
        None
    };
    manifest.roots = view.roots;
    Ok(Candidate {
        view,
        manifest,
        pages,
        versions_removed,
        outcomes_removed,
        source_roots,
    })
}

/// Copy each reachable node once, rather than inserting entries through COW.
/// Only a traversal stack and one decoded overflow value are resident at once.
fn copy_tree(
    reader: &PageReader,
    pages: &mut PageFile,
    tree: TreeId,
    root: u64,
) -> Result<u64> {
    if root == 0 {
        return Ok(0);
    }
    let mut node = reader.node(tree, root)?;
    match &mut node {
        Node::Leaf(cells) => {
            for cell in cells {
                cell.value = pages.store_value(tree, &reader.value(tree, &cell.value)?)?;
            }
        }
        Node::Internal { children, .. } => {
            for child in children {
                *child = copy_tree(reader, pages, tree, *child)?;
            }
        }
    }
    pages.append_node(tree, &node)
}

pub(crate) fn allocate(
    store: &Store,
    prefix: &str,
    mut id: u64,
) -> Result<(fs::File, u64)> {
    store::writable(store)?;
    loop {
        if id == 0 || id == u64::MAX {
            return Err(Error::Exhausted);
        }
        match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(store.directory().join(format!("{prefix}-{id:020}.bin")))
        {
            Ok(file) => return Ok((file, id)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => id += 1,
            Err(error) => return Err(error.into()),
        }
    }
}

pub(crate) fn install(
    store: &mut Store,
    candidate: Candidate,
) -> Result<()> {
    store::adopt(
        store,
        candidate.view,
        candidate.manifest,
        candidate.pages,
        candidate.source_roots,
    )
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Reclaimed {
    pub files: u64,
    pub bytes: u64,
    pub deferred_files: u64,
    pub deferred_bytes: u64,
}

/// One shared directory lease conservatively pins ALL obsolete files. Only the
/// coordinator calls this, after durable selection, with no private candidates.
pub(crate) fn reclaim(store: &Store) -> Result<Reclaimed> {
    store::writable(store)?;
    let pinned = Arc::strong_count(&store.lease) != 1;
    let manifest = store.manifest();
    let mut report = Reclaimed::default();
    for entry in fs::read_dir(store.directory())? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some((prefix, suffix)) = name.split_once('-') else {
            continue;
        };
        let Some(number) = suffix.strip_suffix(".bin") else {
            continue;
        };
        if number.len() != 20 || !number.bytes().all(|byte| byte.is_ascii_digit()) {
            continue;
        }
        let Ok(id) = number.parse::<u64>() else {
            continue;
        };
        let obsolete = match prefix {
            "pages" => id != manifest.page_file_id,
            "manifest" => id != manifest.generation,
            "log" => manifest
                .segments
                .binary_search_by_key(&id, |s| s.segment_id)
                .is_err(),
            _ => false,
        };
        if !obsolete || !entry.file_type()?.is_file() {
            continue;
        }
        let bytes = entry.metadata()?.len();
        if pinned {
            report.deferred_files += 1;
            report.deferred_bytes += bytes;
        } else {
            fs::remove_file(entry.path())?;
            report.files += 1;
            report.bytes += bytes;
        }
    }
    if report.files != 0 {
        platform::sync_directory(&platform::open_directory(store.directory())?)?;
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Genesis;
    use crate::storage::LimitPolicy;
    use crate::storage::Mutation;
    use crate::storage::mvcc;
    use crate::storage::mvcc::StateValue;
    use crate::storage::{
        self,
    };

    fn fixture() -> (tempfile::TempDir, Store) {
        let directory = tempfile::tempdir().unwrap();
        let store = storage::create(
            directory.path().join("db"),
            Genesis {
                database_id: [1; 16],
                initial_policy: LimitPolicy::new([0; 17]).unwrap(),
            },
            [2; 16],
        )
        .unwrap();
        (directory, store)
    }

    #[test]
    fn collection_keeps_middle_future_versions_baselines_and_tombstones() {
        let (_directory, mut store) = fixture();
        for (sequence, value) in [(2, 2), (3, 3), (9, 9), (6, 6), (4, 4)] {
            storage::apply(
                &mut store,
                &[
                    Mutation {
                        tree: TreeId::State,
                        key: StateKey::new(1, b"a".to_vec(), sequence).unwrap().encode(),
                        value: Some(StateValue::Put(vec![value]).encode().unwrap()),
                    },
                    Mutation {
                        tree: TreeId::Outcomes,
                        key: sequence.to_be_bytes().to_vec(),
                        value: Some(vec![value]),
                    },
                ],
            )
            .unwrap();
        }
        for sequence in [2, 3] {
            storage::apply(
                &mut store,
                &[Mutation {
                    tree: TreeId::State,
                    key: StateKey::new(1, b"b".to_vec(), sequence).unwrap().encode(),
                    value: Some(
                        if sequence == 3 {
                            StateValue::Delete
                        } else {
                            StateValue::Put(vec![2])
                        }
                        .encode()
                        .unwrap(),
                    ),
                }],
            )
            .unwrap();
        }
        let before = storage::view(&store);
        let (candidate, versions, outcomes) = collect_view(&mut store, 3, 4).unwrap();
        assert_eq!((versions, outcomes), (2, 2));
        let kept_outcomes: Vec<_> =
            storage::scan(&candidate, TreeId::Outcomes, Unbounded, Unbounded)
                .unwrap()
                .map(|entry| u64::from_be_bytes(entry.unwrap().0.try_into().unwrap()))
                .collect();
        assert_eq!(kept_outcomes, [4, 6, 9]);
        let kept: Vec<_> = storage::scan(&candidate, TreeId::State, Unbounded, Unbounded)
            .unwrap()
            .map(|entry| {
                let (key, _) = entry.unwrap();
                let key = StateKey::decode(&key).unwrap();
                (key.key().to_vec(), key.sequence())
            })
            .collect();
        assert_eq!(
            kept,
            vec![
                (b"a".to_vec(), 9),
                (b"a".to_vec(), 6),
                (b"a".to_vec(), 4),
                (b"a".to_vec(), 3),
                (b"b".to_vec(), 3)
            ]
        );
        // The blind writer at 9 is already installed. A reader at 7 still needs
        // 6.
        assert_eq!(mvcc::get(&candidate, 1, b"a", 6).unwrap(), Some(vec![6]));
        assert_eq!(mvcc::get(&candidate, 1, b"a", 3).unwrap(), Some(vec![3]));
        assert_eq!(mvcc::get(&candidate, 1, b"b", 4).unwrap(), None);
        assert_eq!(mvcc::get(&before, 1, b"a", 2).unwrap(), Some(vec![2]));
        assert_eq!(
            mvcc::get(&storage::view(&store), 1, b"a", 2).unwrap(),
            Some(vec![2])
        );
        assert!(collect_view(&mut store, 5, 4).is_err());
    }

    #[test]
    fn reachable_copy_rewrites_internal_and_overflow_pages_without_cow_garbage() {
        let (_directory, mut store) = fixture();
        let mut root = 0;
        for key in 0_u64..600 {
            root = tree::put(
                &mut store.pages,
                TreeId::State,
                root,
                &key.to_be_bytes(),
                &vec![key as u8; 16_321],
            )
            .unwrap();
        }
        let reader = store.pages.reader();
        assert!(matches!(
            reader.node(TreeId::State, root).unwrap(),
            Node::Internal { .. }
        ));
        let mut pages = PageFile::create(tempfile::tempfile().unwrap(), [1; 16], 7).unwrap();
        let copied = copy_tree(&reader, &mut pages, TreeId::State, root).unwrap();
        tree::validate(&pages.reader(), TreeId::State, copied).unwrap();
        let original = tree::scan(&reader, TreeId::State, root, Unbounded, Unbounded)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        let actual = tree::scan(&pages.reader(), TreeId::State, copied, Unbounded, Unbounded)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(actual, original);
        assert!(
            pages.page_count() < 1210,
            "only overflow and reachable tree nodes should be copied"
        );
        assert!(pages.page_count() < store.pages.page_count());
    }

    #[test]
    fn handover_rejects_old_views_and_reclaims_only_after_all_pins_retire() {
        let (_directory, mut store) = fixture();
        let old = storage::view(&store);
        let path = store.directory().join("pages-00000000000000000001.bin");
        let candidate = prepare(&mut store, 0, 1, true, true).unwrap();
        install(&mut store, candidate).unwrap();
        assert_eq!(store.manifest().page_file_id, 2);
        assert!(path.exists());
        assert!(reclaim(&store).unwrap().deferred_files >= 2);
        let manifest = store.manifest().clone();
        assert!(storage::publish(&mut store, &old, manifest).is_err());
        let scan = storage::scan(&old, TreeId::Policy, Unbounded, Unbounded).unwrap();
        drop(old);
        assert_eq!(reclaim(&store).unwrap().files, 0);
        drop(scan);
        assert!(reclaim(&store).unwrap().files >= 2);
        assert!(!path.exists());
        assert!(
            store
                .directory()
                .join("pages-00000000000000000002.bin")
                .exists()
        );
    }

    #[test]
    fn allocation_skips_orphans_and_never_allocates_reserved_ids() {
        let (_directory, store) = fixture();
        fs::write(
            store.directory().join("log-00000000000000000001.bin"),
            b"orphan",
        )
        .unwrap();
        let (_, id) = allocate(&store, "log", 1).unwrap();
        assert_eq!(id, 2);
        assert_eq!(
            fs::read(store.directory().join("log-00000000000000000001.bin")).unwrap(),
            b"orphan"
        );
        assert!(matches!(
            allocate(&store, "log", u64::MAX),
            Err(Error::Exhausted)
        ));
        assert!(matches!(
            allocate(&store, "pages", 0),
            Err(Error::Exhausted)
        ));
    }

    #[test]
    fn handover_rejects_installations_since_candidate_capture() {
        let (_directory, mut store) = fixture();
        let candidate = prepare(&mut store, 0, 1, true, true).unwrap();
        storage::apply(
            &mut store,
            &[Mutation {
                tree: TreeId::State,
                key: StateKey::new(1, b"a".to_vec(), 2).unwrap().encode(),
                value: Some(StateValue::Put(vec![2]).encode().unwrap()),
            }],
        )
        .unwrap();
        assert!(matches!(
            install(&mut store, candidate),
            Err(Error::InvalidInput(_))
        ));
        assert_eq!(store.manifest().page_file_id, 1);
        assert_eq!(
            mvcc::get(&storage::view(&store), 1, b"a", 2).unwrap(),
            Some(vec![2])
        );
    }
}
