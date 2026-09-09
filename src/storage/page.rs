//! Version 1 page encoding and positional, append-only file access.

mod cache;

use std::collections::HashSet;
use std::fs::File;
use std::io;
use std::sync::Arc;

use cache::PageCache;

use super::Error;
use super::Result;
use super::TreeId;
use super::platform::read_exact_at;
use super::platform::sync_file;
use super::platform::write_all_at;

pub(super) const PAGE_SIZE: usize = 16_384;
pub(super) const HEADER_SIZE: usize = 64;
pub(super) const PAYLOAD_SIZE: usize = PAGE_SIZE - HEADER_SIZE;
pub(super) const MAX_KEY: usize = 2_066;
pub(super) const INLINE_LIMIT: usize = 1_024;
pub(super) const MAX_VALUE: usize = 128 * 1024 * 1024;
const MAX_PAGES: u64 = 1 << 48;
type Bytes = Box<[u8; PAGE_SIZE]>;

/// The descriptor must be opened for positional I/O, without `O_APPEND`.
pub(super) struct PageFile {
    reader: PageReader,
    next_page: u64,
    poisoned: bool,
}

/// A pinned descriptor and immutable readable prefix, independent of later
/// appends.
#[derive(Clone)]
pub(super) struct PageReader {
    file: Arc<File>,
    page_count: u64,
    cache: Option<Arc<PageCache>>,
}

#[derive(Clone, Debug)]
pub(super) enum Value {
    Inline(Vec<u8>),
    Overflow { len: usize, head: u64 },
}

#[derive(Clone, Debug)]
pub(super) struct LeafCell {
    pub key: Vec<u8>,
    pub value: Value,
}

#[derive(Clone, Debug)]
pub(super) enum Node {
    Leaf(Vec<LeafCell>),
    Internal {
        level: u8,
        keys: Vec<Vec<u8>>,
        children: Vec<u64>,
    },
}

impl Node {
    pub(super) fn level(&self) -> u8 {
        match self {
            Self::Leaf(_) => 0,
            Self::Internal { level, .. } => *level,
        }
    }
}

impl PageFile {
    /// Creates page zero in an empty file; never overwrites an existing file.
    pub(super) fn create(
        file: File,
        database_id: [u8; 16],
        file_id: u64,
    ) -> Result<Self> {
        if file.metadata()?.len() != 0 {
            return Err(Error::InvalidInput("page file is not empty"));
        }
        let mut bytes = header(0, 0, 0);
        bytes[40..44].copy_from_slice(&32u32.to_le_bytes());
        bytes[64..72].copy_from_slice(b"BLOPST01");
        bytes[72..88].copy_from_slice(&database_id);
        bytes[88..96].copy_from_slice(&file_id.to_le_bytes());
        checksum(&mut bytes);
        write_all_at(&file, bytes.as_slice(), 0)?;
        Ok(Self {
            reader: PageReader {
                file: Arc::new(file),
                page_count: 1,
                cache: None,
            },
            next_page: 1,
            poisoned: false,
        })
    }

    /// Checks the committed prefix and identities without discarding a crash
    /// tail.
    pub(super) fn open(
        file: File,
        database_id: [u8; 16],
        file_id: u64,
        page_count: u64,
    ) -> Result<Self> {
        if !(1..=MAX_PAGES).contains(&page_count) {
            return Err(Error::Corrupt("invalid page count"));
        }
        let length = file.metadata()?.len();
        if length < page_count * PAGE_SIZE as u64 {
            return Err(Error::Corrupt("truncated page file"));
        }
        let reader = PageReader {
            file: Arc::new(file),
            page_count,
            cache: None,
        };
        let bytes = reader.read(0, None)?;
        if bytes[72..88] != database_id || u64_at(bytes.as_slice(), 88) != file_id {
            return Err(Error::Corrupt("page file identity mismatch"));
        }
        Ok(Self {
            reader,
            next_page: length.div_ceil(PAGE_SIZE as u64),
            poisoned: false,
        })
    }

    pub(super) fn reader(&self) -> PageReader {
        self.reader.clone()
    }

    /// Enables bounded decoded-node reuse for subsequently captured readers.
    /// Call only after recovery; explicit disk verification must use
    /// `uncached`.
    pub(super) fn enable_cache(&mut self) {
        self.reader
            .cache
            .get_or_insert_with(|| Arc::new(PageCache::new()));
    }

    pub(super) fn page_count(&self) -> u64 {
        self.reader.page_count
    }

    pub(super) fn sync(&self) -> Result<()> {
        if self.poisoned {
            return Err(Error::NeedsRecovery);
        }
        sync_file(&self.reader.file)?;
        Ok(())
    }

    /// The caller must validate all published roots before removing the tail.
    pub(super) fn truncate_tail(&mut self) -> Result<()> {
        if self.poisoned {
            return Err(Error::NeedsRecovery);
        }
        if let Err(error) = self
            .reader
            .file
            .set_len(self.reader.page_count * PAGE_SIZE as u64)
        {
            self.poisoned = true;
            return Err(error.into());
        }
        self.next_page = self.reader.page_count;
        Ok(())
    }

    pub(super) fn append_node(
        &mut self,
        tree: TreeId,
        node: &Node,
    ) -> Result<u64> {
        self.append(encode_node(tree, node)?)
    }

    pub(super) fn store_value(
        &mut self,
        tree: TreeId,
        value: &[u8],
    ) -> Result<Value> {
        if value.len() > MAX_VALUE {
            return Err(Error::InvalidInput("value exceeds 128 MiB"));
        }
        if value.len() <= INLINE_LIMIT {
            return Ok(Value::Inline(value.to_vec()));
        }
        let mut head = 0u64;
        // Writing backwards makes every link refer to an already completed
        // page.
        for chunk in value.chunks(PAYLOAD_SIZE).rev() {
            let mut bytes = header(3, 0, tree as u32);
            bytes[32..40].copy_from_slice(&head.to_le_bytes());
            bytes[40..44].copy_from_slice(&(chunk.len() as u32).to_le_bytes());
            bytes[64..64 + chunk.len()].copy_from_slice(chunk);
            head = self.append(bytes)?;
        }
        Ok(Value::Overflow {
            len: value.len(),
            head,
        })
    }

    fn append(
        &mut self,
        mut bytes: Bytes,
    ) -> Result<u64> {
        if self.poisoned {
            return Err(Error::NeedsRecovery);
        }
        if self.next_page >= MAX_PAGES {
            return Err(Error::Exhausted);
        }
        let id = self.next_page;
        // Reserve before I/O. An uncertain write must never be retried at this
        // ID.
        self.next_page += 1;
        bytes[8..16].copy_from_slice(&id.to_le_bytes());
        checksum(&mut bytes);
        if let Err(error) = write_all_at(&self.reader.file, bytes.as_slice(), id * PAGE_SIZE as u64)
        {
            self.poisoned = true;
            return Err(error.into());
        }
        self.reader.page_count = self.next_page;
        Ok(id)
    }
}

impl PageReader {
    /// Keeps the descriptor and pinned prefix, but bypasses cached contents.
    pub(super) fn uncached(&self) -> Self {
        Self {
            file: self.file.clone(),
            page_count: self.page_count,
            cache: None,
        }
    }

    pub(super) fn same_file(
        &self,
        other: &Self,
    ) -> bool {
        Arc::ptr_eq(&self.file, &other.file)
    }

    pub(super) fn node(
        &self,
        tree: TreeId,
        id: u64,
    ) -> Result<Node> {
        if self.cache.is_some() {
            return self.shared_node(tree, id).map(|node| (*node).clone());
        }
        let bytes = self.read(id, Some(tree))?;
        decode_node(&bytes, self.page_count)
    }

    pub(super) fn shared_node(
        &self,
        tree: TreeId,
        id: u64,
    ) -> Result<Arc<Node>> {
        if !valid_ref(id, self.page_count) {
            return Err(Error::Corrupt("page reference outside tree prefix"));
        }
        if let Some(cache) = &self.cache
            && let Some(node) = cache.get(tree, id, self.page_count)?
        {
            return Ok(node);
        }
        let bytes = self.read(id, Some(tree))?;
        let node = Arc::new(decode_node(&bytes, self.page_count)?);
        if let Some(cache) = &self.cache {
            cache.insert(tree, id, node.clone());
        }
        Ok(node)
    }

    pub(super) fn value(
        &self,
        tree: TreeId,
        value: &Value,
    ) -> Result<Vec<u8>> {
        match value {
            Value::Inline(bytes) => Ok(bytes.clone()),
            Value::Overflow { len, head } => {
                let mut output = Vec::with_capacity(*len);
                walk_overflow(
                    self,
                    tree,
                    *head,
                    *len,
                    &mut HashSet::new(),
                    Some(&mut output),
                )?;
                Ok(output)
            }
        }
    }

    pub(super) fn validate_value(
        &self,
        tree: TreeId,
        value: &Value,
        owned: &mut HashSet<u64>,
    ) -> Result<()> {
        if let Value::Overflow { len, head } = value {
            walk_overflow(self, tree, *head, *len, owned, None)?;
        }
        Ok(())
    }

    fn read(
        &self,
        id: u64,
        tree: Option<TreeId>,
    ) -> Result<Bytes> {
        if id >= self.page_count || (id == 0) != tree.is_none() {
            return Err(Error::Corrupt("page reference outside tree prefix"));
        }
        let mut bytes = Box::new([0; PAGE_SIZE]);
        if let Err(error) = read_exact_at(&self.file, bytes.as_mut_slice(), id * PAGE_SIZE as u64) {
            return Err(if error.kind() == io::ErrorKind::UnexpectedEof {
                Error::Corrupt("truncated page")
            } else {
                error.into()
            });
        }
        validate_header(&mut bytes, id, tree, self.page_count)?;
        Ok(bytes)
    }
}

fn walk_overflow(
    reader: &PageReader,
    tree: TreeId,
    mut id: u64,
    mut remaining: usize,
    owned: &mut HashSet<u64>,
    mut output: Option<&mut Vec<u8>>,
) -> Result<()> {
    if !(INLINE_LIMIT + 1..=MAX_VALUE).contains(&remaining) {
        return Err(Error::Corrupt("invalid overflow value length"));
    }
    while remaining != 0 {
        if !owned.insert(id) {
            return Err(Error::Corrupt("repeated page ownership"));
        }
        let bytes = reader.read(id, Some(tree))?;
        let length = remaining.min(PAYLOAD_SIZE);
        if bytes[6] != 3 || u32_at(bytes.as_slice(), 40) as usize != length {
            return Err(Error::Corrupt("incorrect overflow payload length or kind"));
        }
        if let Some(output) = output.as_mut() {
            output.extend_from_slice(&bytes[64..64 + length]);
        }
        id = u64_at(bytes.as_slice(), 32);
        remaining -= length;
        if (id == 0) != (remaining == 0) {
            return Err(Error::Corrupt("incorrect overflow chain length"));
        }
    }
    Ok(())
}

fn header(
    kind: u8,
    level: u8,
    tree: u32,
) -> Bytes {
    let mut bytes = Box::new([0; PAGE_SIZE]);
    bytes[..4].copy_from_slice(b"BLP1");
    bytes[4..6].copy_from_slice(&1u16.to_le_bytes());
    bytes[6] = kind;
    bytes[7] = level;
    bytes[16..20].copy_from_slice(&tree.to_le_bytes());
    bytes[24..26].copy_from_slice(&(HEADER_SIZE as u16).to_le_bytes());
    bytes[26..28].copy_from_slice(&(PAGE_SIZE as u16).to_le_bytes());
    bytes
}

fn checksum(bytes: &mut [u8; PAGE_SIZE]) {
    bytes[60..64].fill(0);
    let crc = crc32c::crc32c(bytes);
    bytes[60..64].copy_from_slice(&crc.to_le_bytes());
}

fn validate_header(
    bytes: &mut [u8; PAGE_SIZE],
    id: u64,
    tree: Option<TreeId>,
    count: u64,
) -> Result<()> {
    let crc = u32_at(bytes, 60);
    bytes[60..64].fill(0);
    let valid_crc = crc32c::crc32c(bytes) == crc;
    bytes[60..64].copy_from_slice(&crc.to_le_bytes());
    if &bytes[..4] != b"BLP1" || !valid_crc {
        return Err(Error::Corrupt("page magic or checksum mismatch"));
    }
    let version = u16_at(bytes, 4);
    if version != 1 {
        return Err(Error::Unsupported {
            format: "page",
            version,
        });
    }
    if u64_at(bytes, 8) != id || u32_at(bytes, 16) != tree.map_or(0, |tree| tree as u32) {
        return Err(Error::Corrupt("page identity or tree mismatch"));
    }
    if bytes[22..24]
        .iter()
        .chain(&bytes[28..32])
        .chain(&bytes[44..60])
        .any(|b| *b != 0)
    {
        return Err(Error::Corrupt("nonzero reserved page bytes"));
    }
    let cells = usize::from(u16_at(bytes, 20));
    let lower = usize::from(u16_at(bytes, 24));
    let upper = usize::from(u16_at(bytes, 26));
    let link = u64_at(bytes, 32);
    let payload = u32_at(bytes, 40) as usize;
    match bytes[6] {
        0 if id == 0 => {
            if bytes[7] != 0
                || cells != 0
                || lower != 64
                || upper != PAGE_SIZE
                || link != 0
                || payload != 32
                || &bytes[64..72] != b"BLOPST01"
                || bytes[96..].iter().any(|b| *b != 0)
            {
                return Err(Error::Corrupt("invalid file header"));
            }
        }
        1 | 2 if id != 0 => {
            if cells == 0
                || lower != 64 + 4 * cells
                || lower > upper
                || upper > PAGE_SIZE
                || payload != 0
                || bytes[lower..upper].iter().any(|b| *b != 0)
            {
                return Err(Error::Corrupt("invalid node boundaries"));
            }
            if bytes[6] == 1 {
                if bytes[7] != 0 || link != 0 {
                    return Err(Error::Corrupt("invalid leaf header"));
                }
            } else if bytes[7] == 0 || !valid_ref(link, count) {
                return Err(Error::Corrupt("invalid internal header"));
            }
        }
        3 if id != 0 => {
            if bytes[7] != 0
                || cells != 0
                || lower != 64
                || upper != PAGE_SIZE
                || !(1..=PAYLOAD_SIZE).contains(&payload)
                || (link != 0 && !valid_ref(link, count))
                || bytes[64 + payload..].iter().any(|b| *b != 0)
            {
                return Err(Error::Corrupt("invalid overflow header"));
            }
        }
        _ => return Err(Error::Corrupt("invalid page kind")),
    }
    Ok(())
}

fn decode_node(
    bytes: &[u8; PAGE_SIZE],
    page_count: u64,
) -> Result<Node> {
    let leaf = match bytes[6] {
        1 => true,
        2 => false,
        _ => return Err(Error::Corrupt("tree reference is not a node")),
    };
    let count = usize::from(u16_at(bytes, 20));
    let upper = usize::from(u16_at(bytes, 26));
    let mut end = PAGE_SIZE;
    let mut keys = Vec::with_capacity(count);
    let mut cells = Vec::new();
    let mut children = vec![u64_at(bytes, 32)];
    for index in 0..count {
        let offset = usize::from(u16_at(bytes, 64 + 4 * index));
        let length = usize::from(u16_at(bytes, 66 + 4 * index));
        let fixed = if leaf { 16 } else { 12 };
        if offset < upper || offset + length != end || length < fixed {
            return Err(Error::Corrupt("noncanonical node slots"));
        }
        let cell = &bytes[offset..end];
        let key_len = usize::from(u16_at(cell, 0));
        if key_len > MAX_KEY || fixed + key_len > length {
            return Err(Error::Corrupt("invalid cell key length"));
        }
        let key = cell[fixed..fixed + key_len].to_vec();
        if keys
            .last()
            .is_some_and(|previous: &Vec<u8>| previous >= &key)
        {
            return Err(Error::Corrupt("node keys are not strictly ordered"));
        }
        if leaf {
            let value_len = u32_at(cell, 4) as usize;
            let head = u64_at(cell, 8);
            if cell[3] != 0 || value_len > MAX_VALUE {
                return Err(Error::Corrupt("invalid leaf value header"));
            }
            let value = match cell[2] {
                0 if value_len <= INLINE_LIMIT
                    && head == 0
                    && length == 16 + key_len + value_len =>
                {
                    Value::Inline(cell[16 + key_len..].to_vec())
                }
                1 if value_len > INLINE_LIMIT
                    && valid_ref(head, page_count)
                    && length == 16 + key_len =>
                {
                    Value::Overflow {
                        len: value_len,
                        head,
                    }
                }
                _ => return Err(Error::Corrupt("invalid leaf value storage")),
            };
            cells.push(LeafCell {
                key: key.clone(),
                value,
            });
        } else {
            let child = u64_at(cell, 4);
            if u16_at(cell, 2) != 0 || length != 12 + key_len || !valid_ref(child, page_count) {
                return Err(Error::Corrupt("invalid internal cell"));
            }
            children.push(child);
        }
        keys.push(key);
        end = offset;
    }
    if end != upper {
        return Err(Error::Corrupt("cell packing does not meet upper boundary"));
    }
    Ok(if leaf {
        Node::Leaf(cells)
    } else {
        Node::Internal {
            level: bytes[7],
            keys,
            children,
        }
    })
}

pub(super) fn leaf_size(cell: &LeafCell) -> usize {
    4 + 16
        + cell.key.len()
        + match &cell.value {
            Value::Inline(value) => value.len(),
            Value::Overflow { .. } => 0,
        }
}

fn encode_node(
    tree: TreeId,
    node: &Node,
) -> Result<Bytes> {
    let (kind, count, size) = match node {
        Node::Leaf(cells) => (
            1,
            cells.len(),
            HEADER_SIZE + cells.iter().map(leaf_size).sum::<usize>(),
        ),
        Node::Internal {
            keys,
            children,
            level,
        } => {
            if *level == 0 || children.len() != keys.len() + 1 || children.contains(&0) {
                return Err(Error::InvalidInput("invalid internal node"));
            }
            (
                2,
                keys.len(),
                HEADER_SIZE + keys.iter().map(|key| 16 + key.len()).sum::<usize>(),
            )
        }
    };
    if count == 0 || size > PAGE_SIZE {
        return Err(Error::InvalidInput("node does not fit a page"));
    }
    let mut bytes = header(kind, node.level(), tree as u32);
    bytes[20..22].copy_from_slice(&(count as u16).to_le_bytes());
    bytes[24..26].copy_from_slice(&((64 + 4 * count) as u16).to_le_bytes());
    let mut end = PAGE_SIZE;
    for index in 0..count {
        let (key, length) = match node {
            Node::Leaf(cells) => (&cells[index].key, leaf_size(&cells[index]) - 4),
            Node::Internal { keys, .. } => (&keys[index], 12 + keys[index].len()),
        };
        if key.len() > MAX_KEY {
            return Err(Error::InvalidInput("key exceeds 2066 bytes"));
        }
        let start = end - length;
        bytes[64 + 4 * index..66 + 4 * index].copy_from_slice(&(start as u16).to_le_bytes());
        bytes[66 + 4 * index..68 + 4 * index].copy_from_slice(&(length as u16).to_le_bytes());
        let cell = &mut bytes[start..end];
        cell[..2].copy_from_slice(&(key.len() as u16).to_le_bytes());
        match node {
            Node::Leaf(cells) => {
                cell[16..16 + key.len()].copy_from_slice(key);
                match &cells[index].value {
                    Value::Inline(value) => {
                        cell[4..8].copy_from_slice(&(value.len() as u32).to_le_bytes());
                        cell[16 + key.len()..].copy_from_slice(value);
                    }
                    Value::Overflow { len, head } => {
                        cell[2] = 1;
                        cell[4..8].copy_from_slice(&(*len as u32).to_le_bytes());
                        cell[8..16].copy_from_slice(&head.to_le_bytes());
                    }
                }
            }
            Node::Internal { children, .. } => {
                cell[4..12].copy_from_slice(&children[index + 1].to_le_bytes());
                cell[12..].copy_from_slice(key);
            }
        }
        end = start;
    }
    if let Node::Internal { children, .. } = node {
        bytes[32..40].copy_from_slice(&children[0].to_le_bytes());
    }
    bytes[26..28].copy_from_slice(&(end as u16).to_le_bytes());
    Ok(bytes)
}

fn valid_ref(
    id: u64,
    count: u64,
) -> bool {
    id != 0 && id < count
}

fn u16_at(
    bytes: &[u8],
    offset: usize,
) -> u16 {
    u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap())
}

fn u32_at(
    bytes: &[u8],
    offset: usize,
) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
}

fn u64_at(
    bytes: &[u8],
    offset: usize,
) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create() -> PageFile {
        PageFile::create(tempfile::tempfile().unwrap(), [7; 16], 19).unwrap()
    }

    fn leaf() -> Node {
        Node::Leaf(vec![
            LeafCell {
                key: b"a".to_vec(),
                value: Value::Inline(vec![1, 2]),
            },
            LeafCell {
                key: b"b".to_vec(),
                value: Value::Inline(vec![3]),
            },
        ])
    }

    fn raw(
        file: &PageFile,
        id: u64,
    ) -> Bytes {
        let mut bytes = Box::new([0; PAGE_SIZE]);
        read_exact_at(
            &file.reader.file,
            bytes.as_mut_slice(),
            id * PAGE_SIZE as u64,
        )
        .unwrap();
        bytes
    }

    fn overwrite(
        file: &PageFile,
        id: u64,
        bytes: &mut [u8; PAGE_SIZE],
    ) {
        checksum(bytes);
        write_all_at(&file.reader.file, bytes, id * PAGE_SIZE as u64).unwrap();
    }

    #[test]
    fn cache_is_opt_in_shared_and_bypassed_by_explicit_disk_reads() {
        let mut file = create();
        let id = file.append_node(TreeId::State, &leaf()).unwrap();
        let original = raw(&file, id);
        let before = file.reader();
        assert!(before.cache.is_none());
        file.enable_cache();
        let reader = file.reader();
        file.enable_cache();
        assert!(Arc::ptr_eq(
            reader.cache.as_ref().unwrap(),
            file.reader.cache.as_ref().unwrap()
        ));
        let node = reader.shared_node(TreeId::State, id).unwrap();
        let clone = reader.clone();
        assert!(Arc::ptr_eq(
            &node,
            &clone.shared_node(TreeId::State, id).unwrap()
        ));
        let Node::Leaf(mut owned) = reader.node(TreeId::State, id).unwrap() else {
            panic!()
        };
        owned[0].key.clear();
        assert!(matches!(node.as_ref(), Node::Leaf(cells) if cells[0].key == b"a"));
        let uncached = reader.uncached();
        assert!(reader.same_file(&uncached));
        assert_eq!(reader.page_count, uncached.page_count);
        assert!(uncached.cache.is_none());

        write_all_at(&file.reader.file, &[0], id * PAGE_SIZE as u64).unwrap();
        assert!(Arc::ptr_eq(
            &node,
            &reader.shared_node(TreeId::State, id).unwrap()
        ));
        assert!(matches!(
            uncached.node(TreeId::State, id),
            Err(Error::Corrupt(_))
        ));
        assert!(matches!(
            before.node(TreeId::State, id),
            Err(Error::Corrupt(_))
        ));
        assert!(matches!(
            reader.shared_node(TreeId::Policy, id),
            Err(Error::Corrupt(_))
        ));
        write_all_at(
            &file.reader.file,
            original.as_slice(),
            id * PAGE_SIZE as u64,
        )
        .unwrap();
        uncached.node(TreeId::State, id).unwrap();
    }

    #[test]
    fn cached_nodes_recheck_pinned_prefix_and_every_child_reference() {
        for future_child in 0..2 {
            let mut file = create();
            file.enable_cache();
            let first = file.append_node(TreeId::State, &leaf()).unwrap();
            let future = file.page_count() + 1;
            let mut children = vec![first, first];
            children[future_child] = future;
            let root = file
                .append_node(
                    TreeId::State,
                    &Node::Internal {
                        level: 1,
                        keys: vec![b"c".to_vec()],
                        children,
                    },
                )
                .unwrap();
            let pinned = file.reader();
            assert_eq!(file.append_node(TreeId::State, &leaf()).unwrap(), future);
            let newer = file.reader();
            newer.shared_node(TreeId::State, root).unwrap();
            newer.shared_node(TreeId::State, future).unwrap();
            assert!(matches!(
                pinned.shared_node(TreeId::State, root),
                Err(Error::Corrupt(_))
            ));
            assert!(matches!(
                pinned.shared_node(TreeId::State, future),
                Err(Error::Corrupt(_))
            ));
            pinned.shared_node(TreeId::State, first).unwrap();
        }

        let mut file = create();
        file.enable_cache();
        let root = file
            .append_node(
                TreeId::State,
                &Node::Leaf(vec![LeafCell {
                    key: b"a".to_vec(),
                    value: Value::Overflow {
                        len: INLINE_LIMIT + 1,
                        head: 2,
                    },
                }]),
            )
            .unwrap();
        let pinned = file.reader();
        let value = file
            .store_value(TreeId::State, &vec![5; INLINE_LIMIT + 1])
            .unwrap();
        let newer = file.reader();
        newer.shared_node(TreeId::State, root).unwrap();
        assert_eq!(
            newer.value(TreeId::State, &value).unwrap(),
            vec![5; INLINE_LIMIT + 1]
        );
        assert!(matches!(
            pinned.shared_node(TreeId::State, root),
            Err(Error::Corrupt(_))
        ));
        assert!(matches!(
            pinned.value(TreeId::State, &value),
            Err(Error::Corrupt(_))
        ));
    }

    #[test]
    fn failed_decodes_are_not_cached_and_reopen_has_an_independent_cache() {
        let mut file = create();
        file.enable_cache();
        let id = file.append_node(TreeId::State, &leaf()).unwrap();
        let mut bytes = raw(&file, id);
        let original = bytes.clone();
        bytes[66..68].fill(0);
        overwrite(&file, id, &mut bytes);
        let reader = file.reader();
        assert!(matches!(
            reader.shared_node(TreeId::State, id),
            Err(Error::Corrupt(_))
        ));
        write_all_at(
            &file.reader.file,
            original.as_slice(),
            id * PAGE_SIZE as u64,
        )
        .unwrap();
        let node = reader.shared_node(TreeId::State, id).unwrap();
        let mut reopened = PageFile::open(
            file.reader.file.try_clone().unwrap(),
            [7; 16],
            19,
            file.page_count(),
        )
        .unwrap();
        assert!(reopened.reader.cache.is_none());
        reopened.enable_cache();
        let second = reopened.reader().shared_node(TreeId::State, id).unwrap();
        assert!(!Arc::ptr_eq(&node, &second));
        assert!(!reader.same_file(&reopened.reader()));

        let mut other = create();
        other.enable_cache();
        let other_id = other
            .append_node(
                TreeId::State,
                &Node::Leaf(vec![LeafCell {
                    key: b"other".to_vec(),
                    value: Value::Inline(vec![]),
                }]),
            )
            .unwrap();
        assert_eq!(id, other_id);
        assert!(
            matches!(other.reader().shared_node(TreeId::State, id).unwrap().as_ref(), Node::Leaf(cells) if cells[0].key == b"other")
        );
        assert!(matches!(node.as_ref(), Node::Leaf(cells) if cells[0].key == b"a"));
    }

    #[test]
    fn exact_page_zero_and_h4_leaf_bytes() {
        let mut file = create();
        let mut expected = Box::new([0u8; PAGE_SIZE]);
        expected[..4].copy_from_slice(b"BLP1");
        expected[4] = 1;
        expected[24] = 64;
        expected[27] = 64;
        expected[40] = 32;
        expected[64..72].copy_from_slice(b"BLOPST01");
        expected[72..88].fill(7);
        expected[88] = 19;
        let crc = crc32c::crc32c(expected.as_slice());
        expected[60..64].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(raw(&file, 0), expected);

        let key = vec![
            0, 0, 0, 0, 0, 0, 0, 1, 0x61, 0, 0xff, 0, 0xff, 0, 0, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xf6,
        ];
        let id = file
            .append_node(
                TreeId::State,
                &Node::Leaf(vec![LeafCell {
                    key: key.clone(),
                    value: Value::Inline(vec![0]),
                }]),
            )
            .unwrap();
        let bytes = raw(&file, id);
        assert_eq!(id, 1);
        assert_eq!(bytes[6], 1);
        assert_eq!(u64_at(bytes.as_slice(), 8), 1);
        assert_eq!(u32_at(bytes.as_slice(), 16), 1);
        assert_eq!(u16_at(bytes.as_slice(), 20), 1);
        assert_eq!(u16_at(bytes.as_slice(), 24), 68);
        assert_eq!(u16_at(bytes.as_slice(), 26), 16_344);
        assert_eq!(u16_at(bytes.as_slice(), 64), 16_344);
        assert_eq!(u16_at(bytes.as_slice(), 66), 40);
        assert_eq!(
            &bytes[16_344..16_360],
            &[23, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        );
        assert_eq!(&bytes[16_360..16_383], key);
        assert_eq!(bytes[16_383], 0);
        assert!(bytes[68..16_344].iter().all(|b| *b == 0));
        file.reader().node(TreeId::State, id).unwrap();
    }

    #[test]
    fn open_checks_prefix_identity_and_preserves_tail() {
        let file = create();
        let descriptor = || file.reader.file.try_clone().unwrap();
        assert!(matches!(
            PageFile::create(descriptor(), [7; 16], 19),
            Err(Error::InvalidInput(_))
        ));
        for count in [0, 2, MAX_PAGES, MAX_PAGES + 1] {
            assert!(matches!(
                PageFile::open(descriptor(), [7; 16], 19, count),
                Err(Error::Corrupt(_))
            ));
        }
        assert!(PageFile::open(descriptor(), [8; 16], 19, 1).is_err());
        assert!(PageFile::open(descriptor(), [7; 16], 20, 1).is_err());
        write_all_at(&file.reader.file, &[0xee; 27], PAGE_SIZE as u64).unwrap();
        let mut reopened = PageFile::open(descriptor(), [7; 16], 19, 1).unwrap();
        assert_eq!(reopened.page_count(), 1);
        assert_eq!(
            file.reader.file.metadata().unwrap().len(),
            PAGE_SIZE as u64 + 27
        );
        assert!(reopened.reader().node(TreeId::State, 1).is_err());
        reopened.truncate_tail().unwrap();
        assert_eq!(file.reader.file.metadata().unwrap().len(), PAGE_SIZE as u64);
        assert_eq!(reopened.append_node(TreeId::State, &leaf()).unwrap(), 1);
        reopened.sync().unwrap();
    }

    #[test]
    fn append_never_overwrites_an_untrimmed_tail_or_failed_allocation() {
        let file = create();
        write_all_at(&file.reader.file, &[9; 3], PAGE_SIZE as u64).unwrap();
        let mut reopened =
            PageFile::open(file.reader.file.try_clone().unwrap(), [7; 16], 19, 1).unwrap();
        let pinned = reopened.reader();
        assert_eq!(reopened.append_node(TreeId::State, &leaf()).unwrap(), 2);
        assert!(pinned.node(TreeId::State, 2).is_err());
        assert_eq!(reopened.page_count(), 3);
        let mut tail = [0; 3];
        read_exact_at(&file.reader.file, &mut tail, PAGE_SIZE as u64).unwrap();
        assert_eq!(tail, [9; 3]);

        let named = tempfile::NamedTempFile::new().unwrap();
        PageFile::create(named.reopen().unwrap(), [0; 16], 1).unwrap();
        let read_only = File::open(named.path()).unwrap();
        let mut failed = PageFile::open(read_only, [0; 16], 1, 1).unwrap();
        assert!(matches!(
            failed.append_node(TreeId::State, &leaf()),
            Err(Error::Io(_))
        ));
        assert_eq!(failed.next_page, 2);
        assert_eq!(failed.page_count(), 1);
        assert!(matches!(
            failed.append_node(TreeId::State, &leaf()),
            Err(Error::NeedsRecovery)
        ));
        assert!(matches!(failed.truncate_tail(), Err(Error::NeedsRecovery)));
    }

    #[test]
    fn every_page_truncation_is_rejected() {
        let mut file = create();
        let id = file.append_node(TreeId::State, &leaf()).unwrap();
        let reader = file.reader();
        for length in (0..PAGE_SIZE as u64).rev() {
            file.reader.file.set_len(PAGE_SIZE as u64 + length).unwrap();
            assert!(matches!(
                reader.node(TreeId::State, id),
                Err(Error::Corrupt(_))
            ));
        }
        for length in (0..PAGE_SIZE as u64).rev() {
            file.reader.file.set_len(length).unwrap();
            assert!(matches!(
                PageFile::open(file.reader.file.try_clone().unwrap(), [7; 16], 19, 1),
                Err(Error::Corrupt(_))
            ));
        }
    }

    #[test]
    fn checksums_versions_reserved_fields_and_file_header_are_checked() {
        let file = create();
        let original = raw(&file, 0);
        for offset in [
            0,
            6,
            7,
            8,
            16,
            20,
            22,
            24,
            26,
            28,
            32,
            40,
            44,
            59,
            64,
            96,
            PAGE_SIZE - 1,
        ] {
            let mut bytes = original.clone();
            bytes[offset] ^= 1;
            overwrite(&file, 0, &mut bytes);
            assert!(
                matches!(
                    PageFile::open(file.reader.file.try_clone().unwrap(), [7; 16], 19, 1),
                    Err(Error::Corrupt(_))
                ),
                "offset {offset}"
            );
        }
        let mut bytes = original.clone();
        bytes[4] = 2;
        overwrite(&file, 0, &mut bytes);
        assert!(matches!(
            PageFile::open(file.reader.file.try_clone().unwrap(), [7; 16], 19, 1),
            Err(Error::Unsupported { version: 2, .. })
        ));
        write_all_at(&file.reader.file, original.as_slice(), 0).unwrap();
        write_all_at(&file.reader.file, &[1], 100).unwrap();
        assert!(matches!(
            PageFile::open(file.reader.file.try_clone().unwrap(), [7; 16], 19, 1),
            Err(Error::Corrupt(_))
        ));
    }

    #[test]
    fn malformed_slots_and_leaf_cells_are_rejected_with_valid_crc() {
        let mut file = create();
        let id = file.append_node(TreeId::State, &leaf()).unwrap();
        let original = raw(&file, id);
        let a = usize::from(u16_at(original.as_slice(), 64));
        let b = usize::from(u16_at(original.as_slice(), 68));
        let cases: Vec<(usize, Vec<u8>)> = vec![
            (6, vec![9]),
            (7, vec![1]),
            (16, vec![2]),
            (22, vec![1]),
            (20, u16::MAX.to_le_bytes().to_vec()),
            (24, vec![0, 0]),
            (26, u16::MAX.to_le_bytes().to_vec()),
            (28, vec![1]),
            (32, vec![1]),
            (40, vec![1]),
            (44, vec![1]),
            (72, vec![1]),
            (64, (a as u16 - 1).to_le_bytes().to_vec()),
            (66, 0u16.to_le_bytes().to_vec()),
            (68, (a as u16).to_le_bytes().to_vec()),
            (a, 2067u16.to_le_bytes().to_vec()),
            (a + 2, vec![2]),
            (a + 3, vec![1]),
            (a + 4, u32::MAX.to_le_bytes().to_vec()),
            (a + 4, 1u32.to_le_bytes().to_vec()),
            (a + 8, vec![1]),
            (b + 16, b"a".to_vec()),
            (b + 16, vec![0]),
        ];
        for (offset, replacement) in cases {
            let mut bytes = original.clone();
            bytes[offset..offset + replacement.len()].copy_from_slice(&replacement);
            overwrite(&file, id, &mut bytes);
            assert!(
                matches!(
                    file.reader().node(TreeId::State, id),
                    Err(Error::Corrupt(_))
                ),
                "offset {offset}"
            );
        }
        let mut bytes = original.clone();
        bytes[64..68].copy_from_slice(&original[68..72]);
        bytes[68..72].copy_from_slice(&original[64..68]);
        overwrite(&file, id, &mut bytes);
        assert!(file.reader().node(TreeId::State, id).is_err());
    }

    #[test]
    fn overflow_thresholds_and_payload_packing() {
        let mut file = create();
        for length in [
            0,
            1,
            INLINE_LIMIT,
            INLINE_LIMIT + 1,
            PAYLOAD_SIZE,
            PAYLOAD_SIZE + 1,
            2 * PAYLOAD_SIZE,
            2 * PAYLOAD_SIZE + 1,
        ] {
            let input: Vec<_> = (0..length).map(|i| (i % 251) as u8).collect();
            let before = file.page_count();
            let value = file.store_value(TreeId::State, &input).unwrap();
            let pages = if length <= INLINE_LIMIT {
                0
            } else {
                length.div_ceil(PAYLOAD_SIZE)
            };
            assert_eq!(file.page_count() - before, pages as u64);
            let reader = file.reader();
            assert_eq!(reader.value(TreeId::State, &value).unwrap(), input);
            reader
                .validate_value(TreeId::State, &value, &mut HashSet::new())
                .unwrap();
            if let Value::Overflow { mut head, .. } = value {
                let mut remaining = length;
                while head != 0 {
                    let bytes = raw(&file, head);
                    let payload = remaining.min(PAYLOAD_SIZE);
                    assert_eq!(u32_at(bytes.as_slice(), 40) as usize, payload);
                    assert!(bytes[64 + payload..].iter().all(|b| *b == 0));
                    assert_eq!(u16_at(bytes.as_slice(), 24), 64);
                    assert_eq!(u16_at(bytes.as_slice(), 26) as usize, PAGE_SIZE);
                    remaining -= payload;
                    head = u64_at(bytes.as_slice(), 32);
                }
                assert_eq!(remaining, 0);
            }
        }
    }

    #[test]
    fn overflow_length_kind_tree_links_and_unused_bytes_are_checked() {
        let mut file = create();
        let value = file
            .store_value(TreeId::State, &vec![8; PAYLOAD_SIZE + 1])
            .unwrap();
        let Value::Overflow { head, .. } = value else {
            panic!()
        };
        let original = raw(&file, head);
        let final_id = u64_at(original.as_slice(), 32);
        let final_page = raw(&file, final_id);
        for (id, offset, data) in [
            (head, 6, vec![1]),
            (head, 7, vec![1]),
            (head, 16, vec![2]),
            (head, 20, vec![1]),
            (head, 24, vec![0]),
            (head, 26, vec![0, 0]),
            (head, 32, 0u64.to_le_bytes().to_vec()),
            (head, 32, head.to_le_bytes().to_vec()),
            (head, 32, file.page_count().to_le_bytes().to_vec()),
            (head, 40, 0u32.to_le_bytes().to_vec()),
            (head, 40, (PAYLOAD_SIZE as u32 - 1).to_le_bytes().to_vec()),
            (head, 40, u32::MAX.to_le_bytes().to_vec()),
            (final_id, 32, head.to_le_bytes().to_vec()),
            (final_id, 40, 2u32.to_le_bytes().to_vec()),
            (final_id, 65, vec![1]),
        ] {
            let mut bytes = if id == head {
                original.clone()
            } else {
                final_page.clone()
            };
            bytes[offset..offset + data.len()].copy_from_slice(&data);
            overwrite(&file, id, &mut bytes);
            assert!(
                matches!(
                    file.reader().value(TreeId::State, &value),
                    Err(Error::Corrupt(_))
                ),
                "page {id}, offset {offset}"
            );
            write_all_at(
                &file.reader.file,
                original.as_slice(),
                head * PAGE_SIZE as u64,
            )
            .unwrap();
            write_all_at(
                &file.reader.file,
                final_page.as_slice(),
                final_id * PAGE_SIZE as u64,
            )
            .unwrap();
        }
        write_all_at(&file.reader.file, &[9], head * PAGE_SIZE as u64 + 64).unwrap();
        assert!(file.reader().value(TreeId::State, &value).is_err());
    }

    #[test]
    fn hostile_node_headers_are_bounded_before_allocation_or_slicing() {
        let mut file = create();
        let id = file.append_node(TreeId::State, &leaf()).unwrap();
        let original = raw(&file, id);
        let mut random = 1u64;
        for _ in 0..2_000 {
            random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
            let mut bytes = original.clone();
            let offset = match (random >> 32) % 3 {
                0 => random as usize % 72,
                1 => PAGE_SIZE - 38 + random as usize % 38,
                _ => usize::from(u16_at(original.as_slice(), 26)) + random as usize % 16,
            };
            bytes[offset] = (random >> 48) as u8;
            overwrite(&file, id, &mut bytes);
            let _ = file.reader().node(TreeId::State, id);
        }
    }

    #[test]
    fn inline_overflow_canonical_choice_and_internal_cells_are_checked() {
        let mut file = create();
        let value = file
            .store_value(TreeId::State, &vec![1; INLINE_LIMIT + 1])
            .unwrap();
        let Value::Overflow { head, .. } = value else {
            panic!()
        };
        for value in [
            Value::Inline(vec![1; INLINE_LIMIT + 1]),
            Value::Overflow {
                len: INLINE_LIMIT,
                head,
            },
            Value::Overflow {
                len: MAX_VALUE + 1,
                head,
            },
            Value::Overflow {
                len: INLINE_LIMIT + 1,
                head: 0,
            },
            Value::Overflow {
                len: INLINE_LIMIT + 1,
                head: u64::MAX,
            },
        ] {
            let id = file
                .append_node(
                    TreeId::State,
                    &Node::Leaf(vec![LeafCell { key: vec![], value }]),
                )
                .unwrap();
            assert!(matches!(
                file.reader().node(TreeId::State, id),
                Err(Error::Corrupt(_))
            ));
        }
        let a = file.append_node(TreeId::State, &leaf()).unwrap();
        let id = file
            .append_node(
                TreeId::State,
                &Node::Internal {
                    level: 1,
                    keys: vec![b"c".to_vec()],
                    children: vec![a, a],
                },
            )
            .unwrap();
        let original = raw(&file, id);
        let cell = PAGE_SIZE - 13;
        assert_eq!(u16_at(original.as_slice(), 64) as usize, cell);
        assert_eq!(u16_at(original.as_slice(), 66), 13);
        assert_eq!(&original[cell..cell + 4], &[1, 0, 0, 0]);
        assert_eq!(u64_at(original.as_slice(), cell + 4), a);
        assert_eq!(original[PAGE_SIZE - 1], b'c');
        for (offset, data) in [
            (7, vec![0]),
            (32, 0u64.to_le_bytes().to_vec()),
            (32, file.page_count().to_le_bytes().to_vec()),
            (cell + 2, vec![1]),
            (cell + 4, 0u64.to_le_bytes().to_vec()),
            (cell + 4, file.page_count().to_le_bytes().to_vec()),
            (cell, 0u16.to_le_bytes().to_vec()),
        ] {
            let mut bytes = original.clone();
            bytes[offset..offset + data.len()].copy_from_slice(&data);
            overwrite(&file, id, &mut bytes);
            assert!(
                matches!(
                    file.reader().node(TreeId::State, id),
                    Err(Error::Corrupt(_))
                ),
                "offset {offset}"
            );
        }
    }
}
