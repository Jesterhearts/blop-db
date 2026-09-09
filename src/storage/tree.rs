//! Immutable-root B+ tree operations. Only the changed path and repair siblings
//! are copied.

use std::collections::HashSet;
use std::ops::Bound;
use std::sync::Arc;

use super::Entry;
use super::Error;
use super::Result;
use super::TreeId;
use super::page::HEADER_SIZE;
use super::page::LeafCell;
use super::page::MAX_KEY;
use super::page::MAX_VALUE;
use super::page::Node;
use super::page::PAGE_SIZE;
use super::page::PageFile;
use super::page::PageReader;
use super::page::leaf_size;

pub(super) fn get(
    reader: &PageReader,
    tree: TreeId,
    root: u64,
    key: &[u8],
) -> Result<Option<Vec<u8>>> {
    check_key(key)?;
    if root == 0 {
        return Ok(None);
    }
    let mut id = root;
    let mut expected = None;
    loop {
        match load_shared(reader, tree, id, expected)?.as_ref() {
            Node::Leaf(cells) => {
                return cells
                    .binary_search_by(|cell| cell.key.as_slice().cmp(key))
                    .ok()
                    .map(|index| reader.value(tree, &cells[index].value))
                    .transpose();
            }
            Node::Internal {
                level,
                keys,
                children,
            } => {
                id = children[route(keys, key)];
                expected = Some(level - 1);
            }
        }
    }
}

struct Frame {
    node: Arc<Node>,
    next: usize,
}

/// Owns a pinned reader, one leaf, and the ancestor path, not the complete
/// result set.
pub struct Scan {
    reader: PageReader,
    tree: TreeId,
    stack: Vec<Frame>,
    leaf: Option<Arc<Node>>,
    next: usize,
    upper: Bound<Vec<u8>>,
    last: Option<Vec<u8>>,
    done: bool,
}

pub(super) fn scan(
    reader: &PageReader,
    tree: TreeId,
    root: u64,
    lower: Bound<&[u8]>,
    upper: Bound<&[u8]>,
) -> Result<Scan> {
    for bound in [&lower, &upper] {
        if let Bound::Included(key) | Bound::Excluded(key) = bound {
            check_key(key)?;
        }
    }
    let empty = match (lower, upper) {
        (Bound::Included(a), Bound::Included(b)) => a > b,
        (Bound::Included(a) | Bound::Excluded(a), Bound::Included(b) | Bound::Excluded(b)) => {
            a >= b
        }
        _ => false,
    };
    let mut scan = Scan {
        reader: reader.clone(),
        tree,
        stack: Vec::new(),
        leaf: None,
        next: 0,
        upper: upper.map(<[u8]>::to_vec),
        last: None,
        done: root == 0 || empty,
    };
    if !scan.done {
        descend(&mut scan, root, None, lower)?;
    }
    Ok(scan)
}

fn descend(
    scan: &mut Scan,
    mut id: u64,
    mut expected: Option<u8>,
    lower: Bound<&[u8]>,
) -> Result<()> {
    loop {
        let node = load_shared(&scan.reader, scan.tree, id, expected)?;
        match node.as_ref() {
            Node::Leaf(cells) => {
                let start = cells.partition_point(|cell| match lower {
                    Bound::Unbounded => false,
                    Bound::Included(key) => cell.key.as_slice() < key,
                    Bound::Excluded(key) => cell.key.as_slice() <= key,
                });
                scan.leaf = Some(node);
                scan.next = start;
                return Ok(());
            }
            Node::Internal {
                level,
                keys,
                children,
            } => {
                let index = match lower {
                    Bound::Unbounded => 0,
                    Bound::Included(key) | Bound::Excluded(key) => route(keys, key),
                };
                id = children[index];
                expected = Some(level - 1);
                scan.stack.push(Frame {
                    node,
                    next: index + 1,
                });
            }
        }
    }
}

fn advance(scan: &mut Scan) -> Result<Option<Entry>> {
    loop {
        let cell = match scan.leaf.as_deref() {
            Some(Node::Leaf(cells)) => cells.get(scan.next),
            _ => None,
        };
        if let Some(cell) = cell {
            scan.next += 1;
            let beyond = match &scan.upper {
                Bound::Unbounded => false,
                Bound::Included(key) => cell.key > *key,
                Bound::Excluded(key) => cell.key >= *key,
            };
            if beyond {
                return Ok(None);
            }
            if scan.last.as_ref().is_some_and(|last| *last >= cell.key) {
                return Err(Error::Corrupt("scan keys are not strictly ordered"));
            }
            let value = scan.reader.value(scan.tree, &cell.value)?;
            scan.last = Some(cell.key.clone());
            return Ok(Some((cell.key.clone(), value)));
        }
        let Some(frame) = scan.stack.last_mut() else {
            return Ok(None);
        };
        let Node::Internal {
            level, children, ..
        } = frame.node.as_ref()
        else {
            unreachable!("scan ancestors are internal nodes");
        };
        if frame.next == children.len() {
            scan.stack.pop();
            continue;
        }
        let id = children[frame.next];
        let expected = level - 1;
        frame.next += 1;
        descend(scan, id, Some(expected), Bound::Unbounded)?;
    }
}

impl Iterator for Scan {
    type Item = Result<Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        match advance(self) {
            Ok(Some(entry)) => Some(Ok(entry)),
            Ok(None) => {
                self.done = true;
                None
            }
            Err(error) => {
                self.done = true;
                Some(Err(error))
            }
        }
    }
}

impl std::iter::FusedIterator for Scan {}

pub(super) fn put(
    file: &mut PageFile,
    tree: TreeId,
    root: u64,
    key: &[u8],
    value: &[u8],
) -> Result<u64> {
    check_key(key)?;
    if value.len() > MAX_VALUE {
        return Err(Error::InvalidInput("value exceeds 128 MiB"));
    }
    if root == 0 {
        let value = file.store_value(tree, value)?;
        return file.append_node(
            tree,
            &Node::Leaf(vec![LeafCell {
                key: key.to_vec(),
                value,
            }]),
        );
    }
    finish(file, tree, root, key, Some(value))
}

pub(super) fn delete(
    file: &mut PageFile,
    tree: TreeId,
    root: u64,
    key: &[u8],
) -> Result<u64> {
    check_key(key)?;
    if root == 0 {
        return Ok(0);
    }
    finish(file, tree, root, key, None)
}

#[derive(Debug)]
struct Child {
    id: u64,
    min: Vec<u8>,
}

struct Edit {
    level: u8,
    children: Vec<Child>,
}

fn finish(
    file: &mut PageFile,
    tree: TreeId,
    root: u64,
    key: &[u8],
    value: Option<&[u8]>,
) -> Result<u64> {
    let Some(edit) = update(file, tree, root, key, value, None)? else {
        return Ok(root);
    };
    match edit.children.len() {
        0 => Ok(0),
        1 => Ok(edit.children[0].id),
        _ => {
            let level = edit.level.checked_add(1).ok_or(Error::Exhausted)?;
            file.append_node(tree, &internal(level, &edit.children))
        }
    }
}

fn update(
    file: &mut PageFile,
    tree: TreeId,
    id: u64,
    key: &[u8],
    value: Option<&[u8]>,
    expected: Option<u8>,
) -> Result<Option<Edit>> {
    let reader = file.reader();
    match load(&reader, tree, id, expected)? {
        Node::Leaf(mut cells) => {
            let index = cells.binary_search_by(|cell| cell.key.as_slice().cmp(key));
            match (index, value) {
                (Err(_), None) => return Ok(None),
                (Ok(index), None) => {
                    cells.remove(index);
                }
                (index, Some(value)) => {
                    let cell = LeafCell {
                        key: key.to_vec(),
                        value: file.store_value(tree, value)?,
                    };
                    match index {
                        Ok(index) => cells[index] = cell,
                        Err(index) => cells.insert(index, cell),
                    }
                }
            }
            Ok(Some(write_leaves(file, tree, cells)?))
        }
        Node::Internal {
            level,
            keys,
            children,
        } => {
            let index = route(&keys, key);
            let Some(edit) = update(file, tree, children[index], key, value, Some(level - 1))?
            else {
                return Ok(None);
            };
            let mut children = child_list(&reader, tree, level, keys, children)?;
            if !edit.children.is_empty() && edit.level != level - 1 {
                if level < 2 || edit.level != level - 2 || edit.children.len() != 1 {
                    return Err(Error::Corrupt("invalid deletion level transition"));
                }
                repair(file, tree, level, &mut children, index, edit.children)?;
            } else {
                children.splice(index..=index, edit.children);
            }
            if children.len() == 1 {
                Ok(Some(Edit {
                    level: level - 1,
                    children,
                }))
            } else {
                Ok(Some(write_internal(file, tree, level, children)?))
            }
        }
    }
}

fn repair(
    file: &mut PageFile,
    tree: TreeId,
    level: u8,
    children: &mut Vec<Child>,
    index: usize,
    mut underflow: Vec<Child>,
) -> Result<()> {
    let sibling = if index == 0 { 1 } else { index - 1 };
    let reader = file.reader();
    let Node::Internal {
        level: sibling_level,
        keys,
        children: ids,
    } = load(&reader, tree, children[sibling].id, Some(level - 1))?
    else {
        return Err(Error::Corrupt("underflow sibling is not internal"));
    };
    let mut combined = child_list(&reader, tree, sibling_level, keys, ids)?;
    if sibling < index {
        combined.append(&mut underflow);
    } else {
        underflow.append(&mut combined);
        combined = underflow;
    }
    let repaired = write_internal(file, tree, sibling_level, combined)?;
    children.splice(index.min(sibling)..=index.max(sibling), repaired.children);
    Ok(())
}

fn child_list(
    reader: &PageReader,
    tree: TreeId,
    level: u8,
    keys: Vec<Vec<u8>>,
    children: Vec<u64>,
) -> Result<Vec<Child>> {
    let min = minimum(reader, tree, children[0], level - 1)?;
    Ok(children
        .into_iter()
        .zip(std::iter::once(min).chain(keys))
        .map(|(id, min)| Child { id, min })
        .collect())
}

fn minimum(
    reader: &PageReader,
    tree: TreeId,
    mut id: u64,
    mut expected: u8,
) -> Result<Vec<u8>> {
    loop {
        match load_shared(reader, tree, id, Some(expected))?.as_ref() {
            Node::Leaf(cells) => return Ok(cells[0].key.clone()),
            Node::Internal {
                level, children, ..
            } => {
                id = children[0];
                expected = level - 1;
            }
        }
    }
}

fn write_leaves(
    file: &mut PageFile,
    tree: TreeId,
    mut cells: Vec<LeafCell>,
) -> Result<Edit> {
    let mut children = Vec::new();
    if cells.is_empty() {
        return Ok(Edit { level: 0, children });
    }
    let sizes: Vec<_> = cells.iter().map(leaf_size).collect();
    if HEADER_SIZE + sizes.iter().sum::<usize>() > PAGE_SIZE {
        let split = split_point(&sizes, false)?;
        let right = cells.split_off(split);
        for cells in [cells, right] {
            let min = cells[0].key.clone();
            children.push(Child {
                id: file.append_node(tree, &Node::Leaf(cells))?,
                min,
            });
        }
    } else {
        let min = cells[0].key.clone();
        children.push(Child {
            id: file.append_node(tree, &Node::Leaf(cells))?,
            min,
        });
    }
    Ok(Edit { level: 0, children })
}

fn internal(
    level: u8,
    children: &[Child],
) -> Node {
    Node::Internal {
        level,
        keys: children[1..]
            .iter()
            .map(|child| child.min.clone())
            .collect(),
        children: children.iter().map(|child| child.id).collect(),
    }
}

fn write_internal(
    file: &mut PageFile,
    tree: TreeId,
    level: u8,
    mut children: Vec<Child>,
) -> Result<Edit> {
    let sizes: Vec<_> = children.iter().map(|child| 16 + child.min.len()).collect();
    let mut output = Vec::new();
    if HEADER_SIZE + sizes[1..].iter().sum::<usize>() > PAGE_SIZE {
        let split = split_point(&sizes, true)?;
        let right = children.split_off(split);
        for children in [children, right] {
            let min = children[0].min.clone();
            output.push(Child {
                id: file.append_node(tree, &internal(level, &children))?,
                min,
            });
        }
    } else {
        let min = children[0].min.clone();
        output.push(Child {
            id: file.append_node(tree, &internal(level, &children))?,
            min,
        });
    }
    Ok(Edit {
        level,
        children: output,
    })
}

fn split_point(
    sizes: &[usize],
    internal: bool,
) -> Result<usize> {
    let minimum = if internal { 2 } else { 1 };
    let total = sizes.iter().sum::<usize>();
    let mut left = 0;
    let mut best = None;
    for index in 1..sizes.len() {
        left += sizes[index - 1];
        if index < minimum || sizes.len() - index < minimum {
            continue;
        }
        let left_size = HEADER_SIZE + left - if internal { sizes[0] } else { 0 };
        let right_size = HEADER_SIZE + total - left - if internal { sizes[index] } else { 0 };
        if left_size <= PAGE_SIZE && right_size <= PAGE_SIZE {
            let difference = left_size.abs_diff(right_size);
            if best.is_none_or(|(_, previous)| difference < previous) {
                best = Some((index, difference));
            }
        }
    }
    best.map(|(index, _)| index)
        .ok_or(Error::InvalidInput("node cannot be split"))
}

/// Checks every reachable node and overflow page, including unique ownership.
pub(super) fn validate(
    reader: &PageReader,
    tree: TreeId,
    root: u64,
) -> Result<()> {
    if root != 0 {
        validate_subtree(reader, tree, root, None, &mut HashSet::new())?;
    }
    Ok(())
}

fn validate_subtree(
    reader: &PageReader,
    tree: TreeId,
    id: u64,
    expected: Option<u8>,
    owned: &mut HashSet<u64>,
) -> Result<(Vec<u8>, Vec<u8>)> {
    if !owned.insert(id) {
        return Err(Error::Corrupt("repeated page ownership"));
    }
    match load(reader, tree, id, expected)? {
        Node::Leaf(cells) => {
            for cell in &cells {
                reader.validate_value(tree, &cell.value, owned)?;
            }
            Ok((cells[0].key.clone(), cells.last().unwrap().key.clone()))
        }
        Node::Internal {
            level,
            keys,
            children,
        } => {
            let (min, mut max) =
                validate_subtree(reader, tree, children[0], Some(level - 1), owned)?;
            for (separator, child) in keys.iter().zip(&children[1..]) {
                let (child_min, child_max) =
                    validate_subtree(reader, tree, *child, Some(level - 1), owned)?;
                if *separator != child_min || max >= child_min {
                    return Err(Error::Corrupt(
                        "inexact separator or overlapping child ranges",
                    ));
                }
                max = child_max;
            }
            Ok((min, max))
        }
    }
}

fn load(
    reader: &PageReader,
    tree: TreeId,
    id: u64,
    expected: Option<u8>,
) -> Result<Node> {
    let node = reader.node(tree, id)?;
    if expected.is_some_and(|expected| node.level() != expected) {
        return Err(Error::Corrupt("child level does not decrease by one"));
    }
    Ok(node)
}

fn load_shared(
    reader: &PageReader,
    tree: TreeId,
    id: u64,
    expected: Option<u8>,
) -> Result<Arc<Node>> {
    let node = reader.shared_node(tree, id)?;
    if expected.is_some_and(|expected| node.level() != expected) {
        return Err(Error::Corrupt("child level does not decrease by one"));
    }
    Ok(node)
}

fn route(
    keys: &[Vec<u8>],
    key: &[u8],
) -> usize {
    keys.partition_point(|separator| separator.as_slice() <= key)
}

fn check_key(key: &[u8]) -> Result<()> {
    if key.len() > MAX_KEY {
        return Err(Error::InvalidInput("key exceeds 2066 bytes"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs::File;

    use super::super::page::INLINE_LIMIT;
    use super::super::page::PAYLOAD_SIZE;
    use super::super::page::Value;
    use super::super::platform::read_exact_at;
    use super::super::platform::write_all_at;
    use super::*;

    fn create() -> PageFile {
        PageFile::create(tempfile::tempfile().unwrap(), [1; 16], 1).unwrap()
    }

    fn entries(
        reader: &PageReader,
        root: u64,
    ) -> Vec<Entry> {
        scan(
            reader,
            TreeId::State,
            root,
            Bound::Unbounded,
            Bound::Unbounded,
        )
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap()
    }

    fn compare(
        file: &PageFile,
        root: u64,
        model: &BTreeMap<Vec<u8>, Vec<u8>>,
    ) {
        let reader = file.reader();
        validate(&reader, TreeId::State, root).unwrap();
        assert_eq!(
            entries(&reader, root),
            model
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<Vec<_>>()
        );
    }

    fn long_key(id: u64) -> Vec<u8> {
        let mut key = vec![(id % 251) as u8; MAX_KEY];
        key[..8].copy_from_slice(&id.to_be_bytes());
        key
    }

    fn random(seed: &mut u64) -> u64 {
        *seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        *seed >> 16
    }

    fn shuffle(
        ids: &mut [u64],
        seed: &mut u64,
    ) {
        for index in (1..ids.len()).rev() {
            ids.swap(index, random(seed) as usize % (index + 1));
        }
    }

    #[test]
    fn empty_tree_empty_key_replacement_and_missing_delete() {
        let mut file = create();
        assert!(
            get(&file.reader(), TreeId::State, 0, b"a")
                .unwrap()
                .is_none()
        );
        assert!(entries(&file.reader(), 0).is_empty());
        validate(&file.reader(), TreeId::State, 0).unwrap();
        assert_eq!(delete(&mut file, TreeId::State, 0, b"a").unwrap(), 0);
        let first = put(&mut file, TreeId::State, 0, b"", b"").unwrap();
        let pinned = file.reader();
        assert_eq!(
            get(&pinned, TreeId::State, first, b"").unwrap(),
            Some(vec![])
        );
        let second = put(&mut file, TreeId::State, first, b"", b"replaced").unwrap();
        assert_eq!(
            get(&file.reader(), TreeId::State, second, b"").unwrap(),
            Some(b"replaced".to_vec())
        );
        assert_eq!(
            get(&pinned, TreeId::State, first, b"").unwrap(),
            Some(vec![])
        );
        let before = file.page_count();
        assert_eq!(
            delete(&mut file, TreeId::State, second, b"absent").unwrap(),
            second
        );
        assert_eq!(file.page_count(), before);
        assert_eq!(delete(&mut file, TreeId::State, second, b"").unwrap(), 0);
        assert_eq!(
            get(&file.reader(), TreeId::State, second, b"").unwrap(),
            Some(b"replaced".to_vec())
        );
    }

    #[test]
    fn full_key_and_value_limits_and_storage_transitions() {
        let mut file = create();
        let oversized_key = vec![0; MAX_KEY + 1];
        let key = &oversized_key[..MAX_KEY];
        let mut root = 0;
        for length in [
            0,
            INLINE_LIMIT,
            INLINE_LIMIT + 1,
            PAYLOAD_SIZE,
            PAYLOAD_SIZE + 1,
            1,
        ] {
            let value = vec![length as u8; length];
            let old_root = root;
            root = put(&mut file, TreeId::State, root, key, &value).unwrap();
            assert_eq!(
                get(&file.reader(), TreeId::State, root, key).unwrap(),
                Some(value)
            );
            validate(&file.reader(), TreeId::State, root).unwrap();
            validate(&file.reader(), TreeId::State, old_root).unwrap();
        }
        assert!(matches!(
            put(&mut file, TreeId::State, root, &oversized_key, b""),
            Err(Error::InvalidInput(_))
        ));
        assert!(matches!(
            get(&file.reader(), TreeId::State, root, &oversized_key),
            Err(Error::InvalidInput(_))
        ));
        assert!(matches!(
            delete(&mut file, TreeId::State, root, &oversized_key),
            Err(Error::InvalidInput(_))
        ));
        assert!(matches!(
            scan(
                &file.reader(),
                TreeId::State,
                root,
                Bound::Included(&oversized_key),
                Bound::Unbounded
            ),
            Err(Error::InvalidInput(_))
        ));
        assert!(matches!(
            scan(
                &file.reader(),
                TreeId::State,
                root,
                Bound::Unbounded,
                Bound::Excluded(&oversized_key)
            ),
            Err(Error::InvalidInput(_))
        ));

        let mut value = vec![0x5a; MAX_VALUE + 1];
        let before = file.page_count();
        assert!(matches!(
            put(&mut file, TreeId::State, root, key, &value),
            Err(Error::InvalidInput(_))
        ));
        assert_eq!(file.page_count(), before);
        value.truncate(MAX_VALUE);
        root = put(&mut file, TreeId::State, root, key, &value).unwrap();
        assert_eq!(
            get(&file.reader(), TreeId::State, root, key)
                .unwrap()
                .as_ref(),
            Some(&value)
        );
        validate(&file.reader(), TreeId::State, root).unwrap();
    }

    #[test]
    fn split_propagation_random_deletion_and_retained_roots_match_model() {
        let mut file = create();
        let mut root = 0;
        let mut model = BTreeMap::new();
        let mut seed = 71;
        let mut ids: Vec<_> = (0..600).collect();
        shuffle(&mut ids, &mut seed);
        let mut snapshots = Vec::new();
        for (index, id) in ids.iter().enumerate() {
            let key = long_key(*id);
            let length =
                [0, 17, INLINE_LIMIT, INLINE_LIMIT + 1, PAYLOAD_SIZE + 1][*id as usize % 5];
            let value = vec![*id as u8; length];
            root = put(&mut file, TreeId::State, root, &key, &value).unwrap();
            model.insert(key, value);
            if index % 67 == 0 {
                compare(&file, root, &model);
            }
            if index == 199 || index == 399 {
                snapshots.push((file.reader(), root, model.clone()));
            }
        }
        compare(&file, root, &model);
        let initial_level = file.reader().node(TreeId::State, root).unwrap().level();
        assert!(initial_level >= 3);
        let Node::Internal { keys, .. } = file.reader().node(TreeId::State, root).unwrap() else {
            panic!()
        };
        for key in keys {
            assert_eq!(
                get(&file.reader(), TreeId::State, root, &key)
                    .unwrap()
                    .as_ref(),
                model.get(&key)
            );
        }
        let reader = file.reader();
        let mut lazy = scan(
            &reader,
            TreeId::State,
            root,
            Bound::Unbounded,
            Bound::Unbounded,
        )
        .unwrap();
        let first = lazy.next().unwrap().unwrap();
        let saved: Vec<_> = model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();

        let key = long_key(300);
        let count = file.page_count();
        root = put(&mut file, TreeId::State, root, &key, b"replacement").unwrap();
        assert_eq!(file.page_count() - count, u64::from(initial_level) + 1);
        model.insert(key, b"replacement".to_vec());
        shuffle(&mut ids, &mut seed);
        for (index, id) in ids.iter().enumerate() {
            let key = long_key(*id);
            root = delete(&mut file, TreeId::State, root, &key).unwrap();
            model.remove(&key);
            if index % 19 == 0 {
                compare(&file, root, &model);
            }
        }
        assert_eq!(root, 0);
        compare(&file, root, &model);
        assert_eq!(first, saved[0]);
        assert_eq!(lazy.collect::<Result<Vec<_>>>().unwrap(), saved[1..]);
        for (reader, root, model) in snapshots {
            validate(&reader, TreeId::State, root).unwrap();
            assert_eq!(
                entries(&reader, root),
                model.into_iter().collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn mixed_incremental_updates_match_ordered_map() {
        let mut file = create();
        file.enable_cache();
        let mut root = 0;
        let mut model = BTreeMap::new();
        let mut seed = 291;
        for step in 0..5_000 {
            let id = random(&mut seed) % 600;
            let key = id.to_be_bytes().to_vec();
            if random(&mut seed).is_multiple_of(3) {
                root = delete(&mut file, TreeId::State, root, &key).unwrap();
                model.remove(&key);
            } else {
                let length = [0, 3, 129, 1024, 1025][random(&mut seed) as usize % 5];
                let value = vec![step as u8; length];
                root = put(&mut file, TreeId::State, root, &key, &value).unwrap();
                model.insert(key.clone(), value);
            }
            assert_eq!(
                get(&file.reader(), TreeId::State, root, &key)
                    .unwrap()
                    .as_ref(),
                model.get(&key)
            );
            if step % 101 == 0 {
                compare(&file, root, &model);
            }
        }
        compare(&file, root, &model);
    }

    #[test]
    fn all_range_bound_combinations_across_different_shapes() {
        let mut file = create();
        let mut roots = [0; 2];
        for index in 0..80u64 {
            roots[0] = put(
                &mut file,
                TreeId::State,
                roots[0],
                &long_key(index * 2),
                &[index as u8],
            )
            .unwrap();
            let reverse = 79 - index;
            roots[1] = put(
                &mut file,
                TreeId::State,
                roots[1],
                &long_key(reverse * 2),
                &[reverse as u8],
            )
            .unwrap();
        }
        let reader = file.reader();
        let all = entries(&reader, roots[0]);
        assert_eq!(all, entries(&reader, roots[1]));
        let endpoints: Vec<_> = [0, 1, 2, 49, 50, 79, 80, 157, 158, 159, 200]
            .into_iter()
            .map(long_key)
            .collect();
        let bounds: Vec<_> = std::iter::once(Bound::Unbounded)
            .chain(endpoints.iter().flat_map(|key| {
                [
                    Bound::Included(key.as_slice()),
                    Bound::Excluded(key.as_slice()),
                ]
            }))
            .collect();
        for lower in &bounds {
            for upper in &bounds {
                let expected: Vec<_> = all
                    .iter()
                    .filter(|(key, _)| {
                        let above = match lower {
                            Bound::Unbounded => true,
                            Bound::Included(bound) => key.as_slice() >= *bound,
                            Bound::Excluded(bound) => key.as_slice() > *bound,
                        };
                        let below = match upper {
                            Bound::Unbounded => true,
                            Bound::Included(bound) => key.as_slice() <= *bound,
                            Bound::Excluded(bound) => key.as_slice() < *bound,
                        };
                        above && below
                    })
                    .cloned()
                    .collect();
                for root in roots {
                    let actual = scan(&reader, TreeId::State, root, *lower, *upper)
                        .unwrap()
                        .collect::<Result<Vec<_>>>()
                        .unwrap();
                    assert_eq!(actual, expected);
                }
            }
        }
    }

    fn leaf(
        file: &mut PageFile,
        keys: &[&[u8]],
    ) -> u64 {
        let mut cells = Vec::with_capacity(keys.len());
        for key in keys {
            cells.push(LeafCell {
                key: key.to_vec(),
                value: file.store_value(TreeId::State, key).unwrap(),
            });
        }
        file.append_node(TreeId::State, &Node::Leaf(cells)).unwrap()
    }

    fn branch(
        file: &mut PageFile,
        level: u8,
        keys: &[&[u8]],
        children: Vec<u64>,
    ) -> u64 {
        file.append_node(
            TreeId::State,
            &Node::Internal {
                level,
                keys: keys.iter().map(|key| key.to_vec()).collect(),
                children,
            },
        )
        .unwrap()
    }

    #[test]
    fn cached_point_reads_and_scans_reuse_nodes_across_pinned_roots() {
        let mut file = create();
        file.enable_cache();
        let a = leaf(&mut file, &[b"a", b"b"]);
        let c = leaf(&mut file, &[b"c", b"d"]);
        let root = branch(&mut file, 1, &[b"c"], vec![a, c]);
        let reader = file.reader();
        let root_node = reader.shared_node(TreeId::State, root).unwrap();
        let leaf_node = reader.shared_node(TreeId::State, a).unwrap();
        let expected = entries(&reader.uncached(), root);
        for _ in 0..3 {
            assert_eq!(
                get(&reader, TreeId::State, root, b"b").unwrap(),
                Some(b"b".to_vec())
            );
            let mut rows = scan(
                &reader,
                TreeId::State,
                root,
                Bound::Excluded(b"a"),
                Bound::Included(b"c"),
            )
            .unwrap();
            assert!(Arc::ptr_eq(&rows.stack[0].node, &root_node));
            assert!(Arc::ptr_eq(rows.leaf.as_ref().unwrap(), &leaf_node));
            assert_eq!(rows.next().unwrap().unwrap(), expected[1]);
            assert_eq!(rows.collect::<Result<Vec<_>>>().unwrap(), expected[2..3]);
        }
        let changed = put(&mut file, TreeId::State, root, b"b", b"new").unwrap();
        let newer = file.reader();
        assert_eq!(
            get(&newer, TreeId::State, changed, b"b").unwrap(),
            Some(b"new".to_vec())
        );
        assert_eq!(entries(&reader, root), expected);
        assert_eq!(entries(&newer, root), expected);
        assert!(matches!(
            get(&reader, TreeId::State, changed, b"b"),
            Err(Error::Corrupt(_))
        ));
        validate(&newer.uncached(), TreeId::State, changed).unwrap();
    }

    #[test]
    fn deleting_subtree_minimum_updates_exact_separator() {
        let mut file = create();
        let a = leaf(&mut file, &[b"a", b"b"]);
        let c = leaf(&mut file, &[b"c", b"d"]);
        let root = branch(&mut file, 1, &[b"c"], vec![a, c]);
        let new = delete(&mut file, TreeId::State, root, b"c").unwrap();
        validate(&file.reader(), TreeId::State, new).unwrap();
        let Node::Internal { keys, children, .. } = file.reader().node(TreeId::State, new).unwrap()
        else {
            panic!()
        };
        assert_eq!(keys, vec![b"d".to_vec()]);
        assert_eq!(children[0], a);
        assert_eq!(entries(&file.reader(), root).len(), 4);
        assert_eq!(entries(&file.reader(), new).len(), 3);
    }

    #[test]
    fn internal_underflow_repairs_either_sibling_and_collapses_root() {
        for deleted in [b"a", b"d"] {
            let mut file = create();
            let a = leaf(&mut file, &[b"a"]);
            let b = leaf(&mut file, &[b"b"]);
            let c = leaf(&mut file, &[b"c"]);
            let d = leaf(&mut file, &[b"d"]);
            let left = branch(&mut file, 1, &[b"b"], vec![a, b]);
            let right = branch(&mut file, 1, &[b"d"], vec![c, d]);
            let root = branch(&mut file, 2, &[b"c"], vec![left, right]);
            validate(&file.reader(), TreeId::State, root).unwrap();
            let new = delete(&mut file, TreeId::State, root, deleted).unwrap();
            validate(&file.reader(), TreeId::State, new).unwrap();
            assert_eq!(file.reader().node(TreeId::State, new).unwrap().level(), 1);
            assert_eq!(entries(&file.reader(), new).len(), 3);
            assert_eq!(entries(&file.reader(), root).len(), 4);
        }
    }

    #[test]
    fn structural_validator_rejects_ranges_levels_cycles_and_child_ownership() {
        let mut file = create();
        let a = leaf(&mut file, &[b"a", b"c"]);
        let d = leaf(&mut file, &[b"d", b"f"]);
        let overlapping = leaf(&mut file, &[b"b", b"e"]);
        let foreign = file
            .append_node(
                TreeId::Policy,
                &Node::Leaf(vec![LeafCell {
                    key: b"d".to_vec(),
                    value: Value::Inline(vec![]),
                }]),
            )
            .unwrap();
        let overflow = file.store_value(TreeId::State, &vec![0; 1025]).unwrap();
        let Value::Overflow { head, .. } = overflow else {
            panic!()
        };
        let cases = [
            branch(&mut file, 1, &[b"c"], vec![a, d]),
            branch(&mut file, 1, &[b"e"], vec![a, d]),
            branch(&mut file, 1, &[b"b"], vec![a, overlapping]),
            branch(&mut file, 2, &[b"d"], vec![a, d]),
            branch(&mut file, 1, &[b"d"], vec![a, foreign]),
            branch(&mut file, 1, &[b"d"], vec![a, a]),
            branch(&mut file, 1, &[b"d"], vec![a, head]),
        ];
        for root in cases {
            assert!(matches!(
                validate(&file.reader(), TreeId::State, root),
                Err(Error::Corrupt(_))
            ));
        }
        let future = file.page_count();
        let cycle = branch(&mut file, 1, &[b"d"], vec![future, d]);
        assert_eq!(cycle, future);
        assert!(matches!(
            validate(&file.reader(), TreeId::State, cycle),
            Err(Error::Corrupt(_))
        ));
        assert!(get(&file.reader(), TreeId::State, cycle, b"a").is_err());
        assert!(
            scan(
                &file.reader(),
                TreeId::State,
                cycle,
                Bound::Unbounded,
                Bound::Unbounded
            )
            .is_err()
        );
        assert!(validate(&file.reader(), TreeId::State, file.page_count()).is_err());
        let valid = branch(&mut file, 1, &[b"d"], vec![a, d]);
        validate(&file.reader(), TreeId::State, valid).unwrap();
    }

    #[test]
    fn overflow_ownership_is_exclusive_within_but_not_between_roots() {
        let mut file = create();
        let value = file
            .store_value(TreeId::State, &vec![5; PAYLOAD_SIZE + 1])
            .unwrap();
        let a = file
            .append_node(
                TreeId::State,
                &Node::Leaf(vec![LeafCell {
                    key: b"a".to_vec(),
                    value: value.clone(),
                }]),
            )
            .unwrap();
        let b = file
            .append_node(
                TreeId::State,
                &Node::Leaf(vec![LeafCell {
                    key: b"b".to_vec(),
                    value: value.clone(),
                }]),
            )
            .unwrap();
        validate(&file.reader(), TreeId::State, a).unwrap();
        validate(&file.reader(), TreeId::State, b).unwrap();
        let shared_leaves = branch(&mut file, 1, &[b"b"], vec![a, b]);
        assert!(matches!(
            validate(&file.reader(), TreeId::State, shared_leaves),
            Err(Error::Corrupt(_))
        ));
        let shared_cells = file
            .append_node(
                TreeId::State,
                &Node::Leaf(vec![
                    LeafCell {
                        key: b"a".to_vec(),
                        value: value.clone(),
                    },
                    LeafCell {
                        key: b"b".to_vec(),
                        value,
                    },
                ]),
            )
            .unwrap();
        assert!(matches!(
            validate(&file.reader(), TreeId::State, shared_cells),
            Err(Error::Corrupt(_))
        ));
        let new = put(&mut file, TreeId::State, a, b"c", b"new").unwrap();
        validate(&file.reader(), TreeId::State, new).unwrap();
        validate(&file.reader(), TreeId::State, a).unwrap();
    }

    fn corrupt(
        file: &File,
        id: u64,
        offset: usize,
        data: &[u8],
    ) {
        let mut bytes = [0u8; PAGE_SIZE];
        read_exact_at(file, &mut bytes, id * PAGE_SIZE as u64).unwrap();
        bytes[offset..offset + data.len()].copy_from_slice(data);
        bytes[60..64].fill(0);
        let crc = crc32c::crc32c(&bytes);
        bytes[60..64].copy_from_slice(&crc.to_le_bytes());
        write_all_at(file, &bytes, id * PAGE_SIZE as u64).unwrap();
    }

    #[test]
    fn scans_load_values_lazily_stop_at_upper_bound_and_fuse_on_error() {
        let raw = tempfile::tempfile().unwrap();
        let mut file = PageFile::create(raw.try_clone().unwrap(), [0; 16], 1).unwrap();
        let a = leaf(&mut file, &[b"a"]);
        let value = file.store_value(TreeId::State, &vec![1; 1025]).unwrap();
        let Value::Overflow { head, .. } = value else {
            panic!()
        };
        let b = file
            .append_node(
                TreeId::State,
                &Node::Leaf(vec![LeafCell {
                    key: b"b".to_vec(),
                    value,
                }]),
            )
            .unwrap();
        let root = branch(&mut file, 1, &[b"b"], vec![a, b]);
        corrupt(&raw, head, 16, &2u32.to_le_bytes());
        let reader = file.reader();
        let mut bounded = scan(
            &reader,
            TreeId::State,
            root,
            Bound::Unbounded,
            Bound::Excluded(b"b"),
        )
        .unwrap();
        assert_eq!(bounded.next().unwrap().unwrap().0, b"a");
        assert!(bounded.next().is_none());
        assert!(bounded.next().is_none());
        let mut full = scan(
            &reader,
            TreeId::State,
            root,
            Bound::Unbounded,
            Bound::Unbounded,
        )
        .unwrap();
        assert_eq!(full.next().unwrap().unwrap().0, b"a");
        assert!(matches!(full.next(), Some(Err(Error::Corrupt(_)))));
        assert!(full.next().is_none());
        assert!(full.next().is_none());
        assert!(validate(&reader, TreeId::State, root).is_err());
    }

    #[test]
    fn deleting_a_short_minimum_can_split_a_full_internal_root() {
        let mut file = create();
        let mut children = vec![Child {
            id: leaf(&mut file, &[b""]),
            min: vec![],
        }];
        for id in 1..=900u16 {
            let short = id.to_be_bytes().to_vec();
            let mut long = short.clone();
            long.resize(MAX_KEY, 0xfe);
            let child = leaf(&mut file, &[&short, &long]);
            children.push(Child {
                id: child,
                min: short,
            });
        }
        let old_root = file
            .append_node(TreeId::State, &internal(1, &children))
            .unwrap();
        validate(&file.reader(), TreeId::State, old_root).unwrap();
        let old_reader = file.reader();
        let key = 450u16.to_be_bytes();
        let root = delete(&mut file, TreeId::State, old_root, &key).unwrap();
        validate(&file.reader(), TreeId::State, root).unwrap();
        assert_eq!(file.reader().node(TreeId::State, root).unwrap().level(), 2);
        assert!(
            get(&file.reader(), TreeId::State, root, &key)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            get(&old_reader, TreeId::State, old_root, &key).unwrap(),
            Some(key.to_vec())
        );
        assert_eq!(entries(&file.reader(), root).len(), 1_800);
    }

    #[test]
    fn pinned_snapshot_reads_are_independent_of_concurrent_appends_and_writer_drop() {
        let mut file = create();
        file.enable_cache();
        let mut root = 0;
        for id in 0..200 {
            root = put(&mut file, TreeId::State, root, &long_key(id), &[id as u8]).unwrap();
        }
        let reader = file.reader();
        let snapshot_root = root;
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let start = barrier.clone();
        let task = std::thread::spawn(move || {
            let expected = entries(&reader, snapshot_root);
            start.wait();
            for _ in 0..20 {
                assert_eq!(entries(&reader, snapshot_root), expected);
                assert_eq!(
                    get(&reader, TreeId::State, snapshot_root, &long_key(127)).unwrap(),
                    Some(vec![127])
                );
                validate(&reader, TreeId::State, snapshot_root).unwrap();
            }
            reader
        });
        barrier.wait();
        for id in 0..500 {
            root = put(&mut file, TreeId::State, root, &long_key(id), b"changed").unwrap();
        }
        drop(file);
        let pinned = task.join().unwrap();
        assert_eq!(entries(&pinned, snapshot_root).len(), 200);
    }

    #[test]
    fn maximal_encoded_depth_is_bounded_even_for_an_invalid_tree() {
        let mut file = create();
        let a = leaf(&mut file, &[b"a"]);
        let b = leaf(&mut file, &[b"b"]);
        let mut root = a;
        for level in 1..=u8::MAX {
            root = branch(&mut file, level, &[b"b"], vec![root, b]);
        }
        let reader = file.reader();
        assert_eq!(
            get(&reader, TreeId::State, root, b"a").unwrap(),
            Some(b"a".to_vec())
        );
        let mut scan = scan(
            &reader,
            TreeId::State,
            root,
            Bound::Unbounded,
            Bound::Unbounded,
        )
        .unwrap();
        assert_eq!(scan.next().unwrap().unwrap().0, b"a");
        assert_eq!(scan.next().unwrap().unwrap().0, b"b");
        assert!(matches!(scan.next(), Some(Err(Error::Corrupt(_)))));
        assert!(matches!(
            validate(&reader, TreeId::State, root),
            Err(Error::Corrupt(_))
        ));
        assert!(matches!(
            validate(&reader, TreeId::State, u64::MAX),
            Err(Error::Corrupt(_))
        ));
    }
}
