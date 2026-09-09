//! Per-descriptor, bounded decoded-node storage. Overflow values are never
//! retained. Readers can keep nodes alive after eviction, outside this budget.

use std::sync::Arc;
use std::sync::Mutex;

use super::Error;
use super::Node;
use super::Result;
use super::TreeId;
use super::Value;

const CACHE_BYTES: usize = 64 * 1024 * 1024;
const SHARDS: usize = 16;
const SLOTS: usize = 256;
const WAYS: usize = 4;

pub(super) struct PageCache {
    shards: [Mutex<Shard>; SHARDS],
}

struct Entry {
    id: u64,
    tree: TreeId,
    max_reference: u64,
    node: Arc<Node>,
    bytes: usize,
    used: u64,
}

struct Shard {
    entries: Box<[Option<Entry>]>,
    bytes: usize,
    limit: usize,
    clock: u64,
    hand: usize,
}

impl PageCache {
    pub(super) fn new() -> Self {
        // Reserve fixed bookkeeping and Arc storage as well as node
        // allocations.
        let limit = (CACHE_BYTES - size_of::<Self>() - 2 * size_of::<usize>()) / SHARDS;
        Self {
            shards: std::array::from_fn(|_| Mutex::new(Shard::new(limit))),
        }
    }

    pub(super) fn get(
        &self,
        tree: TreeId,
        id: u64,
        page_count: u64,
    ) -> Result<Option<Arc<Node>>> {
        let mut shard = self.shards[id as usize % SHARDS]
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        shard.clock = shard.clock.saturating_add(1);
        let used = shard.clock;
        let start = set_start(id);
        let Some(entry) = shard.entries[start..start + WAYS]
            .iter_mut()
            .flatten()
            .find(|entry| entry.id == id)
        else {
            return Ok(None);
        };
        if entry.tree != tree {
            return Err(Error::Corrupt("page identity or tree mismatch"));
        }
        // A newer prefix may have populated this slot. Validate all references,
        // including children that the current point lookup will not traverse.
        if id >= page_count || entry.max_reference >= page_count {
            return Err(Error::Corrupt("page reference outside tree prefix"));
        }
        entry.used = used;
        Ok(Some(entry.node.clone()))
    }

    pub(super) fn insert(
        &self,
        tree: TreeId,
        id: u64,
        node: Arc<Node>,
    ) {
        let (bytes, max_reference) = node_allocation(&node);
        let mut shard = self.shards[id as usize % SHARDS]
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        shard.insert(Entry {
            id,
            tree,
            max_reference,
            node,
            bytes,
            used: 0,
        });
    }
}

impl Shard {
    fn new(limit: usize) -> Self {
        Self {
            entries: std::iter::repeat_with(|| None).take(SLOTS).collect(),
            bytes: 0,
            limit: limit.saturating_sub(SLOTS * size_of::<Option<Entry>>()),
            clock: 0,
            hand: 0,
        }
    }

    fn insert(
        &mut self,
        mut entry: Entry,
    ) {
        if entry.bytes > self.limit {
            return;
        }
        let start = set_start(entry.id);
        let slots = &self.entries[start..start + WAYS];
        if slots.iter().flatten().any(|old| old.id == entry.id) {
            return;
        }
        let index = start
            + slots.iter().position(Option::is_none).unwrap_or_else(|| {
                slots
                    .iter()
                    .enumerate()
                    .min_by_key(|(_, old)| old.as_ref().unwrap().used)
                    .unwrap()
                    .0
            });
        self.remove(index);
        while self.bytes + entry.bytes > self.limit {
            self.remove(self.hand);
            self.hand = (self.hand + 1) % SLOTS;
        }
        self.clock = self.clock.saturating_add(1);
        entry.used = self.clock;
        self.bytes += entry.bytes;
        self.entries[index] = Some(entry);
    }

    fn remove(
        &mut self,
        index: usize,
    ) {
        if let Some(entry) = self.entries[index].take() {
            self.bytes -= entry.bytes;
        }
    }
}

fn set_start(id: u64) -> usize {
    (id / SHARDS as u64) as usize % (SLOTS / WAYS) * WAYS
}

fn node_allocation(node: &Node) -> (usize, u64) {
    let mut bytes = size_of::<Node>() + 2 * size_of::<usize>();
    let mut max_reference = 0;
    match node {
        Node::Leaf(cells) => {
            bytes += cells.capacity() * size_of::<super::LeafCell>();
            for cell in cells {
                bytes += cell.key.capacity();
                match &cell.value {
                    Value::Inline(value) => bytes += value.capacity(),
                    Value::Overflow { head, .. } => max_reference = max_reference.max(*head),
                }
            }
        }
        Node::Internal { keys, children, .. } => {
            bytes += keys.capacity() * size_of::<Vec<u8>>()
                + keys.iter().map(Vec::capacity).sum::<usize>()
                + children.capacity() * size_of::<u64>();
            max_reference = children.iter().copied().max().unwrap_or(0);
        }
    }
    (bytes, max_reference)
}

#[cfg(test)]
mod tests {
    use super::super::LeafCell;
    use super::super::PAGE_SIZE;
    use super::*;

    fn node() -> Arc<Node> {
        Arc::new(Node::Leaf(vec![LeafCell {
            key: vec![1],
            value: Value::Inline(vec![2; 1024]),
        }]))
    }

    #[test]
    fn fixed_metadata_and_retained_allocations_fit_the_budget() {
        let cache = PageCache::new();
        let mut budget = size_of::<PageCache>() + 2 * size_of::<usize>();
        for shard in &cache.shards {
            let shard = shard.lock().unwrap();
            budget += shard.limit + shard.entries.len() * size_of::<Option<Entry>>();
        }
        assert!(budget <= CACHE_BYTES);

        let node = Arc::new(Node::Leaf(
            (0..600)
                .map(|_| LeafCell {
                    key: vec![1; 8],
                    value: Value::Inline(vec![]),
                })
                .collect(),
        ));
        let (bytes, _) = node_allocation(&node);
        assert!(bytes > PAGE_SIZE);
        for id in 1..=10_000 {
            cache.insert(TreeId::State, id, node.clone());
        }
        let mut retained = 0;
        for shard in &cache.shards {
            let shard = shard.lock().unwrap();
            assert!(shard.bytes <= shard.limit);
            assert_eq!(
                shard.bytes,
                shard
                    .entries
                    .iter()
                    .flatten()
                    .map(|entry| entry.bytes)
                    .sum()
            );
            retained += shard.bytes;
        }
        assert!(retained < CACHE_BYTES);
        assert!(cache.get(TreeId::State, 1, 10_001).unwrap().is_none());
        assert!(cache.get(TreeId::State, 10_000, 10_001).unwrap().is_some());
    }

    #[test]
    fn set_collisions_evict_the_least_recently_used_node() {
        let cache = PageCache::new();
        let stride = (SHARDS * SLOTS / WAYS) as u64;
        let first = node();
        for index in 0..WAYS {
            cache.insert(TreeId::State, 1 + index as u64 * stride, first.clone());
        }
        assert!(Arc::ptr_eq(
            &cache.get(TreeId::State, 1, u64::MAX).unwrap().unwrap(),
            &first
        ));
        cache.insert(TreeId::State, 1 + WAYS as u64 * stride, node());
        assert!(cache.get(TreeId::State, 1, u64::MAX).unwrap().is_some());
        assert!(
            cache
                .get(TreeId::State, 1 + stride, u64::MAX)
                .unwrap()
                .is_none()
        );
        assert!(matches!(first.as_ref(), Node::Leaf(cells) if cells[0].key == [1]));
    }

    #[test]
    fn allocation_accounting_includes_capacity_and_rejects_oversized_entries() {
        let mut cells = Vec::with_capacity(64);
        let mut key = Vec::with_capacity(4096);
        key.push(1);
        cells.push(LeafCell {
            key,
            value: Value::Overflow {
                len: super::super::MAX_VALUE,
                head: 9,
            },
        });
        let node = Arc::new(Node::Leaf(cells));
        let (bytes, max_reference) = node_allocation(&node);
        assert_eq!(max_reference, 9);
        assert_eq!(
            bytes,
            size_of::<Node>() + 2 * size_of::<usize>() + 64 * size_of::<LeafCell>() + 4096
        );
        let mut shard = Shard::new(SLOTS * size_of::<Option<Entry>>() + bytes - 1);
        shard.insert(Entry {
            id: 1,
            tree: TreeId::State,
            max_reference,
            node,
            bytes,
            used: 0,
        });
        assert_eq!(shard.bytes, 0);
        assert!(shard.entries.iter().all(Option::is_none));
    }
}
