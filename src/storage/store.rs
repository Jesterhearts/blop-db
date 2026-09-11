pub(crate) mod wal;

use std::fs::File;
use std::fs::OpenOptions;
use std::fs::TryLockError;
use std::fs::{
    self,
};
use std::io::Read;
use std::io::Write;
use std::ops::Bound;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use sha2::Digest;
use sha2::Sha256;

use super::Current;
use super::Entry;
use super::Error;
use super::Genesis;
use super::Manifest;
use super::Result;
use super::TreeId;
use super::checkpoint;
use super::checkpoint::RuntimeValidation;
use super::log_validation;
use super::metadata;
use super::mvcc::StateKey;
use super::page::MAX_KEY;
use super::page::MAX_VALUE;
use super::page::PageFile;
use super::page::PageReader;
use super::platform;
use super::tree;

pub(super) const TREES: [TreeId; 5] = [
    TreeId::State,
    TreeId::Catalogue,
    TreeId::Policy,
    TreeId::Outcomes,
    TreeId::Cursors,
];

pub(super) struct DirectoryLease(File);

impl Drop for DirectoryLease {
    fn drop(&mut self) {
        // Closing alone can leave a flock held by a descriptor transiently
        // inherited across fork. Release when the last registered owner
        // retires.
        let _ = self.0.unlock();
    }
}

/// Exclusive directory ownership and the latest installed physical roots.
///
/// Mutating functions require exclusive access. Views and scans can be read
/// concurrently.
pub struct Store {
    directory: PathBuf,
    pub(super) lease: Arc<DirectoryLease>,
    pub(super) pages: PageFile,
    genesis: Genesis,
    manifest: Manifest,
    selected: Manifest,
    roots: [u64; 5],
    poisoned: bool,
    pub(crate) rotate_next: bool,
    read_only: bool,
    runtime: Option<RuntimeValidation>,
    #[cfg(test)]
    fail_after: Option<usize>,
}

impl Store {
    pub(crate) fn selected_manifest(&self) -> &Manifest {
        &self.selected
    }

    /// Return live durable log bounds and selected checkpoint and retention
    /// metadata.
    ///
    /// The live durable frontier can exceed the frontier in the on-disk
    /// manifest.
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    pub fn genesis(&self) -> &Genesis {
        &self.genesis
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }

    pub fn is_read_only(&self) -> bool {
        self.read_only
    }
}

/// Immutable physical roots pinned against reclamation.
///
/// The roots may contain versions above the public visible frontier.
#[derive(Clone)]
pub struct View {
    pub(super) reader: PageReader,
    pub(super) lease: Arc<DirectoryLease>,
    pub(super) roots: [u64; 5],
    pub(super) validation: checkpoint::Proofs,
}

/// One physical edit; `None` removes the entry and its version from the tree.
///
/// Validate system values, schemas, and complete outcomes before installation.
/// To record a transaction deletion without removing history, put an encoded
/// [`StateValue::Delete`](super::mvcc::StateValue::Delete) at its versioned
/// key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Mutation {
    pub tree: TreeId,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

/// A lazy ordered physical scan that retains the directory lease.
pub struct Scan {
    inner: tree::Scan,
    _lease: Arc<DirectoryLease>,
}

impl Iterator for Scan {
    type Item = Result<Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl std::iter::FusedIterator for Scan {}

/// Create storage in a new directory.
///
/// Select unique nonzero identities outside transaction code. The destination
/// must not exist. Failure may leave an incomplete directory; do not open it as
/// an empty database or initialize over it.
pub fn create(
    path: impl AsRef<Path>,
    genesis: Genesis,
    cursor_namespace: [u8; 16],
) -> Result<Store> {
    let genesis_bytes = genesis.encode()?;
    if cursor_namespace == [0; 16] {
        return Err(Error::InvalidInput("zero cursor namespace"));
    }
    let path = path.as_ref();
    fs::create_dir(path)?;
    let directory = fs::canonicalize(path)?;
    let parent = platform::open_directory(
        directory
            .parent()
            .ok_or(Error::InvalidInput("directory has no parent"))?,
    )?;
    platform::sync_directory(&parent)?;
    let lease = lock(&directory)?;
    write_new(&directory.join("GENESIS"), &genesis_bytes)?;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(directory.join("pages-00000000000000000001.bin"))?;
    let mut pages = PageFile::create(file, genesis.database_id, 1)?;
    let policy_root = tree::put(
        &mut pages,
        TreeId::Policy,
        0,
        &0_u64.to_be_bytes(),
        &genesis.initial_policy.encode(),
    )?;
    let digest = Sha256::digest(&genesis_bytes).into();
    let manifest = Manifest {
        database_id: genesis.database_id,
        genesis_digest: digest,
        cursor_namespace,
        generation: 1,
        page_file_id: 1,
        page_count: pages.page_count(),
        checkpoint_sequence: 0,
        checkpoint_digest: digest,
        durable_sequence: 0,
        durable_digest: digest,
        history_floor: 0,
        log_floor: 1,
        next_cursor_id: 1,
        next_segment_id: 1,
        next_page_file_id: 2,
        roots: [0, 0, policy_root, 0, 0],
        segments: Vec::new(),
    };
    let mut store = Store {
        directory,
        lease,
        pages,
        genesis,
        roots: manifest.roots,
        selected: manifest.clone(),
        manifest,
        poisoned: false,
        rotate_next: false,
        read_only: false,
        runtime: None,
        #[cfg(test)]
        fail_after: None,
    };
    let manifest = store.manifest.clone();
    publish_files(&mut store, &manifest)?;
    Ok(store)
}

/// Restore the CURRENT-selected checkpoint and validate reachable physical
/// storage.
///
/// Discover and flush complete linked WAL groups beyond the selected log
/// bounds. Trim physically short terminal appends after validation, but reject
/// complete malformed groups.
///
/// The returned durable frontier may exceed the checkpoint. This function does
/// not replay those later records. The engine must replay them before serving
/// public reads or resuming transactions.
pub fn open(path: impl AsRef<Path>) -> Result<Store> {
    open_directory(path.as_ref(), false)
}

pub(super) fn open_directory(
    path: &Path,
    attaching: bool,
) -> Result<Store> {
    let directory = fs::canonicalize(path)?;
    let lease = lock(&directory)?;
    if !attaching && directory.join("ATTACH_REQUIRED").try_exists()? {
        return Err(Error::AttachRequired);
    }
    let read_only = directory.join("READ_ONLY").try_exists()?;
    let current = Current::decode(&read_bounded(&directory.join("CURRENT"), 64)?)?;
    let bytes = read_bounded(
        &directory.join(format!("manifest-{:020}.bin", current.generation)),
        16 * 1024 * 1024,
    )?;
    if <[u8; 32]>::from(Sha256::digest(&bytes)) != current.digest {
        return Err(Error::Corrupt("selected manifest digest mismatch"));
    }
    let mut manifest = Manifest::decode(&bytes)?;
    if manifest.generation != current.generation {
        return Err(Error::Corrupt("selected manifest generation mismatch"));
    }
    let bytes = read_bounded(&directory.join("GENESIS"), 180)?;
    let genesis = Genesis::decode(&bytes)?;
    if genesis.database_id != manifest.database_id
        || <[u8; 32]>::from(Sha256::digest(&bytes)) != manifest.genesis_digest
    {
        return Err(Error::Corrupt("genesis identity or digest mismatch"));
    }
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(directory.join(format!("pages-{:020}.bin", manifest.page_file_id)))
        .map_err(authoritative_error)?;
    let mut pages = PageFile::open(
        file,
        manifest.database_id,
        manifest.page_file_id,
        manifest.page_count,
    )?;
    validate_checkpoint(&pages.reader(), &manifest, &genesis)?;
    metadata::validate_logs(&directory, &manifest)?;
    let selected = manifest.clone();
    manifest = wal::recover_tail(&directory, &manifest)?;
    pages.truncate_tail()?;
    if let Some(segment) = manifest.segments.last() {
        OpenOptions::new()
            .write(true)
            .open(directory.join(format!("log-{:020}.bin", segment.segment_id)))?
            .set_len(segment.committed_bytes)?;
    }
    Ok(Store {
        directory,
        lease,
        pages,
        genesis,
        roots: manifest.roots,
        selected,
        manifest,
        poisoned: false,
        // A reopened owner always starts a new segment. No empty segment needs
        // to be persisted to make a requested rotation survive restart.
        rotate_next: true,
        read_only,
        runtime: None,
        #[cfg(test)]
        fail_after: None,
    })
}

/// Pin the latest installed roots, including any materialization above the
/// checkpoint.
pub fn view(store: &Store) -> View {
    View {
        reader: store.pages.reader(),
        lease: Arc::clone(&store.lease),
        roots: store.roots,
        validation: store
            .runtime
            .as_ref()
            .map_or([None; 4], |runtime| runtime.live),
    }
}

/// Pin only the last durably published checkpoint roots.
pub fn checkpoint_view(store: &Store) -> View {
    View {
        roots: store.manifest.roots,
        validation: store
            .runtime
            .as_ref()
            .map_or([None; 4], |runtime| runtime.published),
        ..view(store)
    }
}

/// Enable bounded caches only after public database recovery. The first
/// publication still fully validates roots and logs before seeding proofs.
pub(crate) fn enable_runtime_cache(store: &mut Store) {
    store.pages.enable_cache();
    if store.runtime.is_none() {
        store.runtime = Some(RuntimeValidation::new(store.roots == store.manifest.roots));
    }
}

pub fn get(
    view: &View,
    tree: TreeId,
    key: &[u8],
) -> Result<Option<Vec<u8>>> {
    tree::get(&view.reader, tree, view.roots[tree.index()], key)
}

pub fn scan(
    view: &View,
    tree: TreeId,
    lower: Bound<&[u8]>,
    upper: Bound<&[u8]>,
) -> Result<Scan> {
    Ok(Scan {
        inner: tree::scan(&view.reader, tree, view.roots[tree.index()], lower, upper)?,
        _lease: Arc::clone(&view.lease),
    })
}

/// Install a complete physical batch against the latest roots.
///
/// On failure, leave the roots unchanged. Include all final transaction
/// versions and their complete outcome in the same batch. This operation does
/// not publish durability or visibility and is not an application write
/// transaction.
pub fn apply(
    store: &mut Store,
    changes: &[Mutation],
) -> Result<()> {
    writable(store)?;
    for change in changes {
        if change.key.len() > MAX_KEY || change.value.as_ref().is_some_and(|v| v.len() > MAX_VALUE)
        {
            return Err(Error::InvalidInput(
                "physical key or value exceeds its format ceiling",
            ));
        }
    }
    let mut roots = store.roots;
    for change in changes {
        let root = roots[change.tree.index()];
        let result = match &change.value {
            Some(value) => tree::put(&mut store.pages, change.tree, root, &change.key, value),
            None => tree::delete(&mut store.pages, change.tree, root, &change.key),
        };
        match result {
            Ok(root) => roots[change.tree.index()] = root,
            Err(error) => {
                store.poisoned = true;
                return Err(error);
            }
        }
    }
    if let Some(runtime) = &mut store.runtime {
        checkpoint::observe(
            runtime,
            store.roots,
            roots,
            changes,
            &store.manifest,
            &store.genesis,
        );
    }
    store.roots = roots;
    Ok(())
}

/// Build a separate checkpoint root set, excluding all logical entries above
/// `sequence`.
///
/// Preserve live roots and all older history. The caller must establish that
/// every record through the selected sequence is resolved. This operation does
/// not collect history.
pub fn prepare_checkpoint(
    store: &mut Store,
    sequence: u64,
) -> Result<View> {
    writable(store)?;
    if sequence < store.manifest.checkpoint_sequence || sequence == u64::MAX {
        return Err(Error::InvalidInput("invalid checkpoint sequence"));
    }
    let source = view(store);
    let mut roots = source.roots;
    if let Some(pending) = store
        .runtime
        .as_ref()
        .and_then(|runtime| runtime.pending.as_ref())
        && source
            .validation
            .iter()
            .enumerate()
            .all(|(index, proof)| proof.is_some_and(|proof| proof.root == roots[index]))
    {
        for ((index, key), version) in &pending.keys {
            if *version > sequence {
                match tree::delete(&mut store.pages, TREES[*index], roots[*index], key) {
                    Ok(root) => roots[*index] = root,
                    Err(error) => {
                        store.poisoned = true;
                        return Err(error);
                    }
                }
            }
        }
        let validation = std::array::from_fn(|index| {
            source.validation[index].map(|proof| checkpoint::ValidatedTree {
                root: roots[index],
                sequence: proof.sequence.min(sequence),
            })
        });
        return Ok(View {
            roots,
            validation,
            ..view(store)
        });
    }
    for tree in TREES[..4].iter().copied() {
        for entry in scan(&source, tree, Bound::Unbounded, Bound::Unbounded)? {
            let (key, _) = entry?;
            if entry_sequence(tree, &key)? > sequence {
                match tree::delete(&mut store.pages, tree, roots[tree.index()], &key) {
                    Ok(root) => roots[tree.index()] = root,
                    Err(error) => {
                        store.poisoned = true;
                        return Err(error);
                    }
                }
            }
        }
    }
    Ok(View {
        roots,
        ..view(store)
    })
}

/// Publish a complete checkpoint and engine-supplied metadata durably.
///
/// Start `manifest` from the latest [`Store::manifest`], then update only
/// frontiers, log descriptors and next cursor/segment IDs. Page identity,
/// roots, page count and the next generation are managed here. Stale manifests
/// and views from another owner are rejected.
///
/// Check physical storage, system framing, log envelopes, and nondecreasing
/// metadata. The engine remains responsible for log bodies, schemas, outcomes,
/// catalogue semantics, correct execution, and active retention claims.
/// Supplied log segment prefixes must already exist; this function does not
/// write them.
///
/// A live database owner may reuse bounded validation results for immutable
/// prefixes. Low-level owners and full recovery validate the files instead.
/// After a publication I/O failure, drop handles and reopen before further
/// work.
pub fn publish(
    store: &mut Store,
    checkpoint: &View,
    mut manifest: Manifest,
) -> Result<()> {
    writable(store)?;
    if !Arc::ptr_eq(&store.lease, &checkpoint.lease)
        || !store.pages.reader().same_file(&checkpoint.reader)
    {
        return Err(Error::InvalidInput(
            "checkpoint belongs to a different storage owner",
        ));
    }
    validate_transition(store, &manifest)?;
    if checkpoint.roots[TreeId::Cursors.index()] != store.roots[TreeId::Cursors.index()] {
        return Err(Error::InvalidInput("checkpoint has stale cursor metadata"));
    }
    manifest.generation = next_generation(store)?;
    manifest.roots = checkpoint.roots;
    // Reusing the selected roots needs only their already durable page prefix.
    // Leave post-checkpoint materialization outside it until roots change; log
    // publications need not flush those reconstructible pages. Never roll back
    // allocation of previously published page IDs.
    manifest.page_count = if store.runtime.is_some() && checkpoint.roots == store.manifest.roots {
        store.manifest.page_count
    } else {
        store.pages.page_count()
    };
    manifest.encode()?;
    let proofs = checkpoint::validate(
        &checkpoint.reader,
        &manifest,
        &store.genesis,
        if store.runtime.is_some() {
            checkpoint.validation
        } else {
            [None; 4]
        },
    )?;
    let mut logs = match store
        .runtime
        .as_ref()
        .and_then(|runtime| runtime.logs.as_ref())
    {
        Some(proof) => {
            log_validation::validate_extension(&store.directory, &store.manifest, &manifest, proof)?
        }
        None => None,
    };
    if logs.is_none() {
        metadata::validate_logs(&store.directory, &manifest)?;
        validate_anchors(store, &manifest)?;
        logs = log_validation::LogValidation::at_checkpoint(&manifest);
    }
    if let Err(error) = publish_files(store, &manifest) {
        store.poisoned = true;
        return Err(error);
    }
    if let Some(runtime) = &mut store.runtime {
        checkpoint::published(runtime, store.roots, &manifest, proofs);
        runtime.logs = logs;
    }
    store.selected = manifest.clone();
    store.manifest = manifest;
    Ok(())
}

pub(super) fn writable(store: &Store) -> Result<()> {
    if store.poisoned {
        Err(Error::NeedsRecovery)
    } else {
        Ok(())
    }
}

fn validate_transition(
    store: &Store,
    next: &Manifest,
) -> Result<()> {
    let old = &store.manifest;
    if next.generation != old.generation
        || next.database_id != old.database_id
        || next.genesis_digest != old.genesis_digest
        || next.cursor_namespace != old.cursor_namespace
        || next.page_file_id != old.page_file_id
        || next.next_page_file_id != old.next_page_file_id
    {
        return Err(Error::InvalidInput(
            "stale manifest or changed storage identity",
        ));
    }
    if next.checkpoint_sequence < old.checkpoint_sequence
        || next.durable_sequence < old.durable_sequence
        || next.history_floor < old.history_floor
        || next.log_floor < old.log_floor
        || next.next_cursor_id < old.next_cursor_id
        || next.next_segment_id < old.next_segment_id
        || (next.checkpoint_sequence == old.checkpoint_sequence
            && next.checkpoint_digest != old.checkpoint_digest)
        || (next.durable_sequence == old.durable_sequence
            && next.durable_digest != old.durable_digest)
    {
        return Err(Error::InvalidInput(
            "publication rolls back metadata or changes an anchor",
        ));
    }
    for segment in &next.segments {
        if let Ok(index) = old
            .segments
            .binary_search_by_key(&segment.segment_id, |s| s.segment_id)
        {
            let previous = &old.segments[index];
            if segment.first_sequence != previous.first_sequence
                || segment.predecessor_digest != previous.predecessor_digest
                || segment.last_sequence < previous.last_sequence
                || segment.committed_bytes < previous.committed_bytes
                || (segment != previous && Some(previous) != old.segments.last())
            {
                return Err(Error::InvalidInput("published segment prefix was replaced"));
            }
        } else if segment.segment_id < old.next_segment_id {
            return Err(Error::InvalidInput("published segment ID was reused"));
        }
    }
    Ok(())
}

fn validate_anchors(
    store: &Store,
    next: &Manifest,
) -> Result<()> {
    let old = &store.manifest;
    if next.durable_sequence > old.durable_sequence {
        // The new retained log must demonstrate extension of the acknowledged
        // old history.
        let mut anchor = next.clone();
        anchor.checkpoint_sequence = old.durable_sequence;
        anchor.checkpoint_digest = old.durable_digest;
        anchor.history_floor = anchor.history_floor.min(anchor.checkpoint_sequence);
        anchor.encode()?;
        metadata::validate_logs(&store.directory, &anchor)?;
    }
    if next.checkpoint_sequence > old.checkpoint_sequence
        && next.checkpoint_sequence <= old.durable_sequence
    {
        let mut anchor = old.clone();
        anchor.checkpoint_sequence = next.checkpoint_sequence;
        anchor.checkpoint_digest = next.checkpoint_digest;
        metadata::validate_logs(&store.directory, &anchor)?;
    }
    Ok(())
}

pub(super) fn entry_sequence(
    tree: TreeId,
    key: &[u8],
) -> Result<u64> {
    let sequence = match tree {
        TreeId::State => StateKey::decode(key).map_err(persisted)?.sequence(),
        TreeId::Catalogue => {
            if key.len() != 16 {
                return Err(Error::Corrupt("invalid catalogue key length"));
            }
            let table = u64::from_be_bytes(key[..8].try_into().unwrap());
            let sequence = !u64::from_be_bytes(key[8..].try_into().unwrap());
            if table == 0 || table > sequence {
                return Err(Error::Corrupt("invalid catalogue version identity"));
            }
            sequence
        }
        TreeId::Policy | TreeId::Outcomes | TreeId::Cursors => {
            if key.len() != 8 {
                return Err(Error::Corrupt("invalid system key length"));
            }
            let sequence = u64::from_be_bytes(key.try_into().unwrap());
            if tree != TreeId::Policy && sequence == 0 {
                return Err(Error::Corrupt("reserved system key"));
            }
            sequence
        }
    };
    if sequence == u64::MAX {
        return Err(Error::Corrupt("reserved system sequence or ID"));
    }
    Ok(sequence)
}

fn validate_checkpoint(
    reader: &PageReader,
    manifest: &Manifest,
    genesis: &Genesis,
) -> Result<()> {
    checkpoint::validate(reader, manifest, genesis, [None; 4]).map(|_| ())
}

fn persisted(error: Error) -> Error {
    match error {
        Error::InvalidInput(reason) => Error::Corrupt(reason),
        other => other,
    }
}

pub(super) fn lock(directory: &Path) -> Result<Arc<DirectoryLease>> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(directory.join("LOCK"))?;
    match file.try_lock() {
        Ok(()) => Ok(Arc::new(DirectoryLease(file))),
        Err(TryLockError::WouldBlock) => Err(Error::Locked),
        Err(TryLockError::Error(error)) => Err(error.into()),
    }
}

fn authoritative_error(error: std::io::Error) -> Error {
    if error.kind() == std::io::ErrorKind::NotFound {
        Error::Corrupt("missing authoritative storage file")
    } else {
        error.into()
    }
}

pub(super) fn read_bounded(
    path: &Path,
    maximum: u64,
) -> Result<Vec<u8>> {
    let file = File::open(path).map_err(authoritative_error)?;
    if file.metadata()?.len() > maximum {
        return Err(Error::Corrupt("metadata file exceeds its format ceiling"));
    }
    let mut bytes = Vec::new();
    file.take(maximum + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > maximum {
        return Err(Error::Corrupt("metadata file exceeds its format ceiling"));
    }
    Ok(bytes)
}

fn next_generation(store: &Store) -> Result<u64> {
    let mut generation = store.manifest.generation;
    loop {
        generation = generation
            .checked_add(1)
            .filter(|id| *id != u64::MAX)
            .ok_or(Error::Exhausted)?;
        if !store
            .directory
            .join(format!("manifest-{generation:020}.bin"))
            .try_exists()?
        {
            return Ok(generation);
        }
    }
}

pub(super) fn write_new(
    path: &Path,
    bytes: &[u8],
) -> Result<()> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(bytes)?;
    platform::sync_file(&file)?;
    Ok(())
}

fn publish_files(
    store: &mut Store,
    manifest: &Manifest,
) -> Result<()> {
    let bytes = manifest.encode()?;
    let current = Current {
        generation: manifest.generation,
        digest: Sha256::digest(&bytes).into(),
    }
    .encode()?;
    // Immutable prefixes already selected by CURRENT are already durable.
    // The low-level path keeps its unconditional flush protocol.
    let reuse = store.runtime.is_some();
    let new_pages = manifest.page_file_id != store.manifest.page_file_id
        || manifest.page_count != store.manifest.page_count;
    if !reuse || new_pages {
        store.pages.sync()?;
    }
    publication_step(store)?;
    let mut new_files = manifest.page_file_id != store.manifest.page_file_id;
    for segment in &manifest.segments {
        let previous = store
            .manifest
            .segments
            .binary_search_by_key(&segment.segment_id, |s| s.segment_id)
            .ok()
            .map(|index| &store.manifest.segments[index]);
        new_files |= previous.is_none();
        if !reuse || previous != Some(segment) {
            platform::sync_file_path(
                &store
                    .directory
                    .join(format!("log-{:020}.bin", segment.segment_id)),
            )?;
        }
        publication_step(store)?;
    }
    let directory = platform::open_directory(&store.directory)?;
    if !reuse || new_files {
        platform::sync_directory(&directory)?;
    }
    publication_step(store)?;
    let temporary = store.directory.join("manifest.pending");
    remove_temporary(&temporary)?;
    write_new(&temporary, &bytes)?;
    publication_step(store)?;
    let final_path = store
        .directory
        .join(format!("manifest-{:020}.bin", manifest.generation));
    if final_path.try_exists()? {
        return Err(Error::InvalidInput("manifest generation already exists"));
    }
    platform::rename(&temporary, &final_path)?;
    publication_step(store)?;
    platform::sync_directory(&directory)?;
    publication_step(store)?;
    let temporary = store.directory.join("CURRENT.pending");
    remove_temporary(&temporary)?;
    write_new(&temporary, &current)?;
    publication_step(store)?;
    platform::rename(&temporary, &store.directory.join("CURRENT"))?;
    publication_step(store)?;
    platform::sync_directory(&directory)?;
    publication_step(store)?;
    Ok(())
}

fn remove_temporary(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn publication_step(_store: &mut Store) -> Result<()> {
    #[cfg(test)]
    if let Some(remaining) = &mut _store.fail_after {
        if *remaining == 0 {
            return Err(std::io::Error::other("injected publication interruption").into());
        }
        *remaining -= 1;
    }
    Ok(())
}

/// Adopt new storage after the caller drains installations and validates
/// history.
///
/// Ordinary publication cannot change file identity or live roots; handover
/// requires this separate path.
pub(super) fn adopt(
    store: &mut Store,
    checkpoint: View,
    mut manifest: Manifest,
    pages: Option<PageFile>,
    source_roots: [u64; 5],
) -> Result<()> {
    writable(store)?;
    if !Arc::ptr_eq(&store.lease, &checkpoint.lease)
        || manifest.checkpoint_sequence != manifest.durable_sequence
        || manifest.checkpoint_sequence != store.manifest.checkpoint_sequence
        || source_roots != store.roots
    {
        return Err(Error::InvalidInput(
            "handover requires a current drained checkpoint",
        ));
    }
    let mut ordinary = manifest.clone();
    ordinary.page_file_id = store.manifest.page_file_id;
    ordinary.next_page_file_id = store.manifest.next_page_file_id;
    validate_transition(store, &ordinary)?;
    match &pages {
        Some(pages)
            if manifest.page_file_id >= store.manifest.next_page_file_id
                && manifest.page_file_id != u64::MAX
                && manifest.next_page_file_id == manifest.page_file_id + 1
                && pages.reader().same_file(&checkpoint.reader) => {}
        None if manifest.page_file_id == store.manifest.page_file_id
            && manifest.next_page_file_id == store.manifest.next_page_file_id
            && store.pages.reader().same_file(&checkpoint.reader) => {}
        _ => return Err(Error::InvalidInput("invalid page file handover")),
    }
    manifest.generation = next_generation(store)?;
    manifest.roots = checkpoint.roots;
    manifest.page_count = pages.as_ref().unwrap_or(&store.pages).page_count();
    manifest.encode()?;
    validate_checkpoint(&checkpoint.reader, &manifest, &store.genesis)?;
    metadata::validate_logs(&store.directory, &manifest)?;
    validate_anchors(store, &manifest)?;
    if let Err(error) = pages
        .as_ref()
        .map_or(Ok(()), PageFile::sync)
        .and_then(|()| publish_files(store, &manifest))
    {
        store.poisoned = true;
        return Err(error);
    }
    if let Some(pages) = pages {
        store.pages = pages;
    }
    store.roots = manifest.roots;
    store.selected = manifest.clone();
    store.manifest = manifest;
    if store.runtime.is_some() {
        store.runtime = Some(RuntimeValidation::new(true));
        store.pages.enable_cache();
    }
    Ok(())
}

pub(super) fn renew_namespace(
    store: &mut Store,
    namespace: [u8; 16],
) -> Result<()> {
    writable(store)?;
    if namespace == [0; 16] || namespace == store.manifest.cursor_namespace {
        return Err(Error::InvalidInput(
            "attachment requires a fresh cursor namespace",
        ));
    }
    let mut manifest = store.manifest.clone();
    manifest.cursor_namespace = namespace;
    manifest.generation = next_generation(store)?;
    if let Err(error) = publish_files(store, &manifest) {
        store.poisoned = true;
        return Err(error);
    }
    store.selected = manifest.clone();
    store.manifest = manifest;
    Ok(())
}

pub(super) fn set_read_only(
    store: &mut Store,
    read_only: bool,
) {
    store.read_only = read_only;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::LimitPolicy;
    use crate::storage::SegmentDescriptor;
    use crate::storage::mvcc;
    use crate::storage::mvcc::StateValue;

    fn new_store() -> (tempfile::TempDir, Store) {
        let directory = tempfile::tempdir().unwrap();
        let store = create(
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

    fn cursor(
        id: u64,
        baseline: u64,
    ) -> Mutation {
        let mut value = vec![1, 0, 1, 0];
        value.extend_from_slice(&baseline.to_le_bytes());
        value.extend_from_slice(&0_u32.to_le_bytes());
        Mutation {
            tree: TreeId::Cursors,
            key: id.to_be_bytes().to_vec(),
            value: Some(value),
        }
    }

    fn row(
        key: &[u8],
        sequence: u64,
        value: StateValue,
    ) -> Mutation {
        Mutation {
            tree: TreeId::State,
            key: StateKey::new(1, key.to_vec(), sequence).unwrap().encode(),
            value: Some(value.encode().unwrap()),
        }
    }

    fn publish_cursors(
        store: &mut Store,
        next_cursor_id: u64,
    ) -> Result<()> {
        let checkpoint = view(store);
        let mut manifest = store.manifest.clone();
        manifest.next_cursor_id = next_cursor_id;
        publish(store, &checkpoint, manifest)
    }

    // The log fixture supplies valid envelopes and policies, without executing
    // any records.
    fn write_log(
        store: &Store,
        count: u64,
    ) -> (Manifest, Vec<[u8; 32]>) {
        let id = store.manifest.next_segment_id;
        let first = store.manifest.durable_sequence + 1;
        let mut bytes = vec![0; 96];
        bytes[..8].copy_from_slice(b"BLOPLG01");
        bytes[8..10].copy_from_slice(&1_u16.to_le_bytes());
        bytes[10..12].copy_from_slice(&96_u16.to_le_bytes());
        bytes[16..32].copy_from_slice(&store.manifest.database_id);
        bytes[32..40].copy_from_slice(&id.to_le_bytes());
        bytes[40..48].copy_from_slice(&first.to_le_bytes());
        bytes[48..80].copy_from_slice(&store.manifest.durable_digest);
        let crc = crc32c::crc32c(&bytes);
        bytes[92..96].copy_from_slice(&crc.to_le_bytes());
        let mut digests = vec![store.manifest.durable_digest];
        for sequence in first..first + count {
            let mut record = vec![0; 64];
            record[..4].copy_from_slice(b"BLR1");
            record[4..6].copy_from_slice(&64_u16.to_le_bytes());
            record[6..8].copy_from_slice(&1_u16.to_le_bytes());
            record[8..12].copy_from_slice(&212_u32.to_le_bytes());
            record[12..16].copy_from_slice(&140_u32.to_le_bytes());
            record[16..24].copy_from_slice(&sequence.to_le_bytes());
            record[24] = 3;
            record[28..60].copy_from_slice(digests.last().unwrap());
            record.extend_from_slice(&store.genesis.initial_policy.encode());
            let crc = crc32c::crc32c(&record);
            record.extend_from_slice(&crc.to_le_bytes());
            record.extend_from_slice(&212_u32.to_le_bytes());
            digests.push(Sha256::digest(&record).into());
            bytes.extend_from_slice(&crate::storage::wal::tests::single(&record));
        }
        fs::write(store.directory.join(format!("log-{id:020}.bin")), &bytes).unwrap();
        let mut manifest = store.manifest.clone();
        manifest.durable_sequence = first + count - 1;
        manifest.durable_digest = *digests.last().unwrap();
        manifest.next_segment_id = id + 1;
        manifest.segments.push(SegmentDescriptor {
            segment_id: id,
            first_sequence: first,
            last_sequence: first + count - 1,
            committed_bytes: bytes.len() as u64,
            predecessor_digest: digests[0],
            last_digest: *digests.last().unwrap(),
        });
        (manifest, digests)
    }

    #[test]
    fn runtime_proofs_and_pending_edits_match_full_prefix_validation() {
        let (_directory, mut store) = new_store();
        enable_runtime_cache(&mut store);
        let (manifest, digests) = write_log(&store, 65);
        let checkpoint = checkpoint_view(&store);
        publish(&mut store, &checkpoint, manifest).unwrap();
        drop(checkpoint);
        for sequence in (2_u64..=65).rev() {
            apply(
                &mut store,
                &[row(
                    &sequence.to_be_bytes(),
                    sequence,
                    StateValue::Put(vec![sequence as u8]),
                )],
            )
            .unwrap();
        }
        let live = view(&store);
        let mut wrong = store.manifest.clone();
        wrong.checkpoint_sequence = 2;
        wrong.checkpoint_digest = digests[2];
        assert!(matches!(
            publish(&mut store, &live, wrong),
            Err(Error::Corrupt(_))
        ));
        for sequence in [2, 17, 34, 65] {
            let prefix = prepare_checkpoint(&mut store, sequence).unwrap();
            let pending = store.runtime.as_mut().unwrap().pending.take();
            let reference = prepare_checkpoint(&mut store, sequence).unwrap();
            store.runtime.as_mut().unwrap().pending = pending;
            for tree in TREES {
                let entries = |view: &View| {
                    scan(view, tree, Bound::Unbounded, Bound::Unbounded)
                        .unwrap()
                        .collect::<Result<Vec<_>>>()
                        .unwrap()
                };
                assert_eq!(entries(&prefix), entries(&reference));
            }
            let mut manifest = store.manifest.clone();
            manifest.checkpoint_sequence = sequence;
            manifest.checkpoint_digest = digests[sequence as usize];
            manifest.roots = prefix.roots;
            manifest.page_count = store.pages.page_count();
            validate_checkpoint(&prefix.reader, &manifest, &store.genesis).unwrap();
            assert_eq!(
                scan(&prefix, TreeId::State, Bound::Unbounded, Bound::Unbounded)
                    .unwrap()
                    .count(),
                (sequence - 1) as usize
            );
            publish(&mut store, &prefix, manifest).unwrap();
            let runtime = store.runtime.as_ref().unwrap();
            assert_eq!(
                runtime.pending.as_ref().unwrap().keys.len(),
                (65 - sequence) as usize
            );
            assert!(
                runtime
                    .published
                    .iter()
                    .flatten()
                    .all(|proof| proof.sequence <= sequence)
            );
        }
        assert!(store.runtime.as_ref().unwrap().logs.is_some());
        assert_eq!(
            scan(&live, TreeId::State, Bound::Unbounded, Bound::Unbounded)
                .unwrap()
                .count(),
            64
        );
        let path = store.directory.clone();
        drop(live);
        drop(store);
        let reopened = open(path).unwrap();
        assert!(reopened.runtime.is_none());
        assert_eq!(reopened.manifest.checkpoint_sequence, 65);
    }

    #[test]
    fn invalid_runtime_edits_invalidate_proofs_and_still_fail_publication() {
        for change in [
            Mutation {
                tree: TreeId::State,
                key: vec![1],
                value: Some(vec![1]),
            },
            Mutation {
                tree: TreeId::Policy,
                key: 0_u64.to_be_bytes().to_vec(),
                value: None,
            },
            Mutation {
                tree: TreeId::Policy,
                key: 0_u64.to_be_bytes().to_vec(),
                value: Some(LimitPolicy::new([1; 17]).unwrap().encode()),
            },
            Mutation {
                tree: TreeId::Policy,
                key: 0_u64.to_be_bytes().to_vec(),
                value: Some(vec![1]),
            },
            row(b"key", 2, StateValue::Put(vec![1])),
        ] {
            let (_directory, mut store) = new_store();
            enable_runtime_cache(&mut store);
            let checkpoint = view(&store);
            let manifest = store.manifest.clone();
            publish(&mut store, &checkpoint, manifest).unwrap();
            let previous = store.manifest.clone();
            apply(&mut store, &[change]).unwrap();
            let checkpoint = view(&store);
            assert!(publish(&mut store, &checkpoint, previous.clone()).is_err());
            assert_eq!(store.manifest, previous);
        }
    }

    #[test]
    fn full_validation_and_reopen_bypass_a_warm_runtime_cache() {
        let (_directory, mut store) = new_store();
        enable_runtime_cache(&mut store);
        let checkpoint = view(&store);
        get(&checkpoint, TreeId::Policy, &0_u64.to_be_bytes()).unwrap();
        let path = store.directory.clone();
        let file = OpenOptions::new()
            .write(true)
            .open(path.join("pages-00000000000000000001.bin"))
            .unwrap();
        platform::write_all_at(
            &file,
            &[0xff],
            checkpoint.roots[2] * super::super::page::PAGE_SIZE as u64,
        )
        .unwrap();
        assert!(matches!(
            validate_checkpoint(&checkpoint.reader, &store.manifest, &store.genesis),
            Err(Error::Corrupt(_))
        ));
        drop(checkpoint);
        drop(store);
        assert!(matches!(open(path), Err(Error::Corrupt(_))));
    }

    #[test]
    fn runtime_publication_flushes_new_data_and_survives_every_io_boundary() {
        use platform::faults::Event;
        use platform::faults::Failure;
        use platform::faults::Guard;
        use platform::faults::Operation;
        use platform::faults::Phase;

        for checkpointing in [false, true] {
            let mut failures = None;
            for fault in std::iter::once(None).chain((0..64).map(Some)) {
                if fault.is_some_and(|index| index >= failures.unwrap()) {
                    break;
                }
                let (_directory, mut store) = new_store();
                enable_runtime_cache(&mut store);
                let (mut manifest, _) = write_log(&store, 3);
                let previous = checkpoint_view(&store);
                publish(&mut store, &previous, manifest).unwrap();
                apply(
                    &mut store,
                    &[
                        row(b"a", 2, StateValue::Put(vec![2])),
                        row(b"b", 3, StateValue::Put(vec![3])),
                    ],
                )
                .unwrap();
                if checkpointing {
                    manifest = store.manifest.clone();
                    manifest.checkpoint_sequence = 3;
                    manifest.checkpoint_digest = manifest.durable_digest;
                } else {
                    (manifest, _) = write_log(&store, 1);
                }
                let checkpoint = if checkpointing {
                    view(&store)
                } else {
                    checkpoint_view(&store)
                };
                let guard = Guard::new(fault.map(|index| (index, Failure::Error)));
                let result = publish(&mut store, &checkpoint, manifest);
                let trace = guard.trace();
                drop(guard);
                if fault.is_none() {
                    result.unwrap();
                    assert_eq!(
                        store.manifest.page_count,
                        if checkpointing {
                            store.pages.page_count()
                        } else {
                            2
                        }
                    );
                    assert!(store.pages.page_count() > 2);
                    failures = Some(trace.len());
                    assert_eq!(
                        trace
                            .iter()
                            .filter(|e| matches!(e, Event(Operation::SyncFile, Phase::Before)))
                            .count(),
                        3
                    );
                    assert_eq!(
                        trace
                            .iter()
                            .filter(|e| matches!(e, Event(Operation::SyncDirectory, Phase::Before)))
                            .count(),
                        if checkpointing { 2 } else { 3 }
                    );
                } else {
                    assert!(result.is_err());
                    assert!(matches!(writable(&store), Err(Error::NeedsRecovery)));
                }
                let path = store.directory.clone();
                drop(previous);
                drop(checkpoint);
                drop(store);
                let reopened = open(path).unwrap();
                assert!(matches!(reopened.manifest.durable_sequence, 3 | 4));
                assert!(matches!(reopened.manifest.checkpoint_sequence, 0 | 3));
                let rows = scan(
                    &view(&reopened),
                    TreeId::State,
                    Bound::Unbounded,
                    Bound::Unbounded,
                )
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .unwrap();
                assert_eq!(
                    rows.len(),
                    if reopened.manifest.checkpoint_sequence == 3 {
                        2
                    } else {
                        0
                    }
                );
            }
        }
    }

    #[test]
    fn creation_reopening_and_incomplete_directories_are_explicit() {
        let (_directory, store) = new_store();
        let path = store.directory.clone();
        let initial = store.manifest.clone();
        assert_eq!(initial.generation, 1);
        assert_eq!(initial.page_count, 2);
        assert_eq!(initial.checkpoint_sequence, 0);
        assert_eq!(initial.durable_sequence, 0);
        assert_eq!(fs::metadata(path.join("GENESIS")).unwrap().len(), 180);
        assert_eq!(fs::metadata(path.join("CURRENT")).unwrap().len(), 64);
        assert!(create(&path, store.genesis.clone(), [2; 16]).is_err());
        assert!(matches!(open(&path), Err(Error::Locked)));
        drop(store);
        let reopened = open(&path).unwrap();
        assert_eq!(reopened.manifest, initial);
        drop(reopened);
        fs::remove_file(path.join("CURRENT")).unwrap();
        assert!(matches!(open(&path), Err(Error::Corrupt(_))));
    }

    #[test]
    fn batch_installation_is_atomic_and_reads_pin_the_old_root() {
        let (_directory, mut store) = new_store();
        let old = view(&store);
        let changes = [
            row(b"a", 2, StateValue::Put(vec![7; 16_321])),
            row(b"b", 2, StateValue::Delete),
        ];
        apply(&mut store, &changes).unwrap();
        assert_eq!(mvcc::get(&old, 1, b"a", 2).unwrap(), None);
        assert_eq!(
            mvcc::get(&view(&store), 1, b"a", 2).unwrap(),
            Some(vec![7; 16_321])
        );
        assert_eq!(
            mvcc::get(&checkpoint_view(&store), 1, b"a", 2).unwrap(),
            None
        );
        let roots = store.roots;
        let count = store.pages.page_count();
        let invalid = [
            cursor(1, 0),
            Mutation {
                tree: TreeId::State,
                key: vec![0; MAX_KEY + 1],
                value: None,
            },
        ];
        assert!(matches!(
            apply(&mut store, &invalid),
            Err(Error::InvalidInput(_))
        ));
        assert_eq!(store.roots, roots);
        assert_eq!(store.pages.page_count(), count);
        assert_eq!(
            get(&view(&store), TreeId::Cursors, &1_u64.to_be_bytes()).unwrap(),
            None
        );
    }

    #[test]
    fn unpublished_installs_and_partial_page_tails_disappear_on_reopen() {
        let (_directory, mut store) = new_store();
        let path = store.directory.clone();
        apply(&mut store, &[row(b"a", 2, StateValue::Put(vec![9; 1_025]))]).unwrap();
        let pages = path.join("pages-00000000000000000001.bin");
        let mut file = OpenOptions::new().append(true).open(&pages).unwrap();
        file.write_all(b"partial page").unwrap();
        drop(store);
        let reopened = open(&path).unwrap();
        assert_eq!(fs::metadata(pages).unwrap().len(), 2 * 16_384);
        assert_eq!(mvcc::get(&view(&reopened), 1, b"a", 2).unwrap(), None);
    }

    #[test]
    fn publication_reopens_all_roots_and_skips_unselected_generations() {
        let (_directory, mut store) = new_store();
        let path = store.directory.clone();
        apply(&mut store, &[cursor(1, 0), cursor(2, 0)]).unwrap();
        fs::write(path.join("manifest-00000000000000000002.bin"), b"orphan").unwrap();
        publish_cursors(&mut store, 3).unwrap();
        assert_eq!(store.manifest.generation, 3);
        let manifest = store.manifest.clone();
        let mut orphan = manifest.clone();
        orphan.generation = 99;
        fs::write(
            path.join("manifest-00000000000000000099.bin"),
            orphan.encode().unwrap(),
        )
        .unwrap();
        drop(store);
        let reopened = open(&path).unwrap();
        assert_eq!(reopened.manifest, manifest);
        for id in [1_u64, 2] {
            assert_eq!(
                get(&view(&reopened), TreeId::Cursors, &id.to_be_bytes()).unwrap(),
                cursor(id, 0).value
            );
        }
    }

    #[test]
    fn interrupted_publications_select_only_complete_old_or_new_roots() {
        for step in 0..8 {
            let (_directory, mut store) = new_store();
            let path = store.directory.clone();
            apply(&mut store, &[cursor(1, 0), cursor(2, 0)]).unwrap();
            store.fail_after = Some(step);
            assert!(matches!(publish_cursors(&mut store, 3), Err(Error::Io(_))));
            assert!(matches!(apply(&mut store, &[]), Err(Error::NeedsRecovery)));
            assert!(matches!(
                publish_cursors(&mut store, 3),
                Err(Error::NeedsRecovery)
            ));
            drop(store);
            let reopened = open(&path).unwrap();
            let selected_new = step >= 6;
            assert_eq!(
                reopened.manifest.generation,
                if selected_new { 2 } else { 1 }
            );
            for id in [1_u64, 2] {
                assert_eq!(
                    get(&view(&reopened), TreeId::Cursors, &id.to_be_bytes()).unwrap(),
                    if selected_new {
                        cursor(id, 0).value
                    } else {
                        None
                    }
                );
            }
        }
    }

    #[test]
    fn idle_views_and_scans_hold_the_directory_lock_after_writer_drop() {
        let (_directory, mut store) = new_store();
        let path = store.directory.clone();
        apply(&mut store, &[cursor(1, 0)]).unwrap();
        let pinned = view(&store);
        let mut scan = scan(&pinned, TreeId::Cursors, Bound::Unbounded, Bound::Unbounded).unwrap();
        drop(store);
        assert!(matches!(open(&path), Err(Error::Locked)));
        drop(pinned);
        assert!(matches!(open(&path), Err(Error::Locked)));
        assert_eq!(scan.next().unwrap().unwrap().1, cursor(1, 0).value.unwrap());
        drop(scan);
        assert!(open(&path).is_ok());
    }

    #[test]
    fn inherited_descriptor_does_not_extend_the_logical_directory_lease() {
        let directory = tempfile::tempdir().unwrap();
        let lease = lock(directory.path()).unwrap();
        let inherited = lease.0.try_clone().unwrap();
        drop(lease);
        // A subprocess can briefly inherit the open file description before
        // exec closes CLOEXEC descriptors. Only registered roots own the lease.
        assert!(lock(directory.path()).is_ok());
        drop(inherited);
    }

    #[test]
    fn checkpoint_filters_future_versions_without_changing_live_roots() {
        let (_directory, mut store) = new_store();
        let (manifest, digests) = write_log(&store, 3);
        let initial = checkpoint_view(&store);
        publish(&mut store, &initial, manifest).unwrap();
        apply(
            &mut store,
            &[
                row(b"a", 2, StateValue::Put(vec![8; 16_321])),
                row(b"a", 3, StateValue::Delete),
                row(b"b", 3, StateValue::Put(vec![9])),
            ],
        )
        .unwrap();
        let checkpoint = prepare_checkpoint(&mut store, 2).unwrap();
        assert_eq!(
            mvcc::get(&checkpoint, 1, b"a", 3).unwrap(),
            Some(vec![8; 16_321])
        );
        assert_eq!(mvcc::get(&view(&store), 1, b"a", 3).unwrap(), None);
        let mut manifest = store.manifest.clone();
        manifest.checkpoint_sequence = 2;
        manifest.checkpoint_digest = digests[2];
        let live = view(&store);
        assert!(publish(&mut store, &live, manifest.clone()).is_err());
        publish(&mut store, &checkpoint, manifest).unwrap();
        let path = store.directory.clone();
        drop(checkpoint);
        drop(initial);
        drop(live);
        drop(store);
        let recovered = open(&path).unwrap();
        assert_eq!(recovered.manifest.checkpoint_sequence, 2);
        assert_eq!(recovered.manifest.durable_sequence, 3);
        assert_eq!(
            mvcc::get(&view(&recovered), 1, b"a", 3).unwrap(),
            Some(vec![8; 16_321])
        );
        assert_eq!(mvcc::get(&view(&recovered), 1, b"b", 3).unwrap(), None);
    }

    #[test]
    fn published_page_ids_are_not_reused_when_an_older_view_is_republished() {
        let (_directory, mut store) = new_store();
        let old = view(&store);
        apply(&mut store, &[row(b"a", 2, StateValue::Put(vec![1; 1025]))]).unwrap();
        let checkpoint = prepare_checkpoint(&mut store, 0).unwrap();
        let manifest = store.manifest.clone();
        publish(&mut store, &checkpoint, manifest).unwrap();
        let count = store.manifest.page_count;
        enable_runtime_cache(&mut store);
        let manifest = store.manifest.clone();
        publish(&mut store, &old, manifest).unwrap();
        assert_eq!(store.manifest.page_count, count);
    }

    #[test]
    fn stale_metadata_foreign_views_and_invalid_retention_are_rejected() {
        let (_directory, mut store) = new_store();
        let (_other_directory, other) = new_store();
        let original = store.manifest.clone();
        assert!(matches!(
            publish(&mut store, &view(&other), original.clone()),
            Err(Error::InvalidInput(_))
        ));
        let stale = view(&store);
        apply(&mut store, &[cursor(1, 0)]).unwrap();
        assert!(matches!(
            publish(&mut store, &stale, original.clone()),
            Err(Error::InvalidInput(_))
        ));
        publish_cursors(&mut store, 2).unwrap();
        let current = view(&store);
        assert!(matches!(
            publish(&mut store, &current, original),
            Err(Error::InvalidInput(_))
        ));
        let mut manifest = store.manifest.clone();
        manifest.history_floor = 1;
        assert!(publish(&mut store, &current, manifest).is_err());
        let mut manifest = store.manifest.clone();
        manifest.next_cursor_id = 1;
        assert!(publish(&mut store, &current, manifest).is_err());
        assert!(!store.poisoned);
    }

    #[test]
    fn changed_genesis_policy_and_invalid_cursor_frames_cannot_be_published() {
        let (_directory, mut store) = new_store();
        let original = store.genesis.initial_policy.encode();
        let mut values = [0; 17];
        values[0] = 1;
        let mut change = Mutation {
            tree: TreeId::Policy,
            key: vec![0; 8],
            value: Some(LimitPolicy::new(values).unwrap().encode()),
        };
        apply(&mut store, &[change.clone()]).unwrap();
        assert!(matches!(
            publish_cursors(&mut store, 1),
            Err(Error::Corrupt(_))
        ));
        change.value = Some(original);
        apply(&mut store, &[change]).unwrap();
        let mut invalid = cursor(1, 0);
        invalid.value.as_mut().unwrap()[3] = 1;
        apply(&mut store, &[invalid]).unwrap();
        assert!(matches!(
            publish_cursors(&mut store, 2),
            Err(Error::Corrupt(_))
        ));
    }

    #[test]
    fn referenced_corruption_never_falls_back_but_unreachable_pages_are_ignored() {
        let (_directory, mut store) = new_store();
        let path = store.directory.clone();
        apply(&mut store, &[cursor(1, 0)]).unwrap();
        publish_cursors(&mut store, 2).unwrap();
        let dead_root = store.manifest.roots[4];
        apply(
            &mut store,
            &[Mutation {
                tree: TreeId::Cursors,
                key: 1_u64.to_be_bytes().to_vec(),
                value: None,
            }],
        )
        .unwrap();
        publish_cursors(&mut store, 2).unwrap();
        let file = OpenOptions::new()
            .write(true)
            .open(path.join("pages-00000000000000000001.bin"))
            .unwrap();
        platform::write_all_at(&file, b"bad", dead_root * 16_384).unwrap();
        let selected_policy = store.manifest.roots[2];
        drop(store);
        drop(open(&path).unwrap());
        platform::write_all_at(&file, b"bad", selected_policy * 16_384).unwrap();
        assert!(matches!(open(&path), Err(Error::Corrupt(_))));
        fs::remove_file(path.join("pages-00000000000000000001.bin")).unwrap();
        assert!(matches!(open(&path), Err(Error::Corrupt(_))));
    }

    #[test]
    fn durable_log_tail_is_not_replayed_and_only_uncommitted_bytes_are_trimmed() {
        let (_directory, mut store) = new_store();
        let (manifest, _) = write_log(&store, 3);
        let committed = manifest.segments[0].committed_bytes;
        let initial = view(&store);
        publish(&mut store, &initial, manifest).unwrap();
        let path = store.directory.clone();
        let log = path.join("log-00000000000000000001.bin");
        OpenOptions::new()
            .append(true)
            .open(&log)
            .unwrap()
            .write_all(b"unpublished corrupt tail")
            .unwrap();
        drop(initial);
        drop(store);
        let reopened = open(&path).unwrap();
        assert_eq!(reopened.manifest.checkpoint_sequence, 0);
        assert_eq!(reopened.manifest.durable_sequence, 3);
        assert_eq!(fs::metadata(&log).unwrap().len(), committed);
        drop(reopened);
        OpenOptions::new()
            .write(true)
            .open(&log)
            .unwrap()
            .set_len(committed - 1)
            .unwrap();
        assert!(matches!(open(&path), Err(Error::Corrupt(_))));
        assert_eq!(fs::metadata(&log).unwrap().len(), committed - 1);
    }

    #[test]
    fn extending_durability_requires_a_log_link_to_the_published_anchor() {
        let (_directory, mut store) = new_store();
        let initial = view(&store);
        let mut invented = store.manifest.clone();
        invented.checkpoint_sequence = 1;
        invented.durable_sequence = 1;
        invented.checkpoint_digest = [9; 32];
        invented.durable_digest = [9; 32];
        invented.log_floor = 2;
        assert!(publish(&mut store, &initial, invented).is_err());
        assert_eq!(store.manifest.generation, 1);
        let (manifest, _) = write_log(&store, 1);
        publish(&mut store, &initial, manifest).unwrap();
    }

    #[test]
    fn every_logical_tree_is_bounded_by_the_checkpoint_but_cursors_are_current() {
        for tree in [TreeId::Catalogue, TreeId::Policy, TreeId::Outcomes] {
            let (_directory, mut store) = new_store();
            let key = if tree == TreeId::Catalogue {
                [1_u64.to_be_bytes(), (!2_u64).to_be_bytes()].concat()
            } else {
                2_u64.to_be_bytes().to_vec()
            };
            let value = if tree == TreeId::Policy {
                store.genesis.initial_policy.encode()
            } else {
                // Filtering is independent of engine-validated
                // catalogue/outcome payloads.
                vec![1, 0]
            };
            apply(
                &mut store,
                &[
                    Mutation {
                        tree,
                        key: key.clone(),
                        value: Some(value.clone()),
                    },
                    cursor(1, 0),
                ],
            )
            .unwrap();
            assert!(matches!(
                publish_cursors(&mut store, 2),
                Err(Error::Corrupt(_))
            ));
            let checkpoint = prepare_checkpoint(&mut store, 0).unwrap();
            assert_eq!(get(&checkpoint, tree, &key).unwrap(), None);
            assert_eq!(get(&view(&store), tree, &key).unwrap(), Some(value));
            assert_eq!(
                get(&checkpoint, TreeId::Cursors, &1_u64.to_be_bytes()).unwrap(),
                cursor(1, 0).value
            );
            let mut manifest = store.manifest.clone();
            manifest.next_cursor_id = 2;
            publish(&mut store, &checkpoint, manifest).unwrap();
        }
    }

    #[test]
    fn log_retirement_and_new_segments_preserve_the_checkpoint_anchor() {
        let (_directory, mut store) = new_store();
        let (manifest, digests) = write_log(&store, 3);
        let initial = view(&store);
        publish(&mut store, &initial, manifest).unwrap();
        let checkpoint = prepare_checkpoint(&mut store, 3).unwrap();
        let mut manifest = store.manifest.clone();
        manifest.checkpoint_sequence = 3;
        manifest.checkpoint_digest = digests[3];
        manifest.log_floor = 4;
        manifest.segments.clear();
        publish(&mut store, &checkpoint, manifest).unwrap();
        assert!(store.manifest.segments.is_empty());
        let (manifest, new_digests) = write_log(&store, 1);
        assert_eq!(new_digests[0], digests[3]);
        assert_eq!(manifest.segments[0].segment_id, 2);
        assert_eq!(manifest.segments[0].first_sequence, 4);
        publish(&mut store, &checkpoint, manifest).unwrap();
        let path = store.directory.clone();
        drop(initial);
        drop(checkpoint);
        drop(store);
        // The retired segment is not an authority and does not supply recovery
        // bytes.
        fs::remove_file(path.join("log-00000000000000000001.bin")).unwrap();
        let reopened = open(&path).unwrap();
        assert_eq!(reopened.manifest.checkpoint_sequence, 3);
        assert_eq!(reopened.manifest.durable_sequence, 4);
        assert_eq!(reopened.manifest.durable_digest, new_digests[1]);
    }

    #[test]
    fn current_digest_generation_and_selected_manifest_are_checked_together() {
        let (_directory, store) = new_store();
        let path = store.directory.clone();
        let mut manifest = store.manifest.clone();
        let selected = path.join("manifest-00000000000000000001.bin");
        drop(store);
        let good_manifest = fs::read(&selected).unwrap();
        fs::write(&selected, &good_manifest[..good_manifest.len() - 1]).unwrap();
        assert!(matches!(
            open(&path),
            Err(Error::Corrupt("selected manifest digest mismatch"))
        ));
        manifest.generation = 2;
        let wrong_generation = manifest.encode().unwrap();
        fs::write(&selected, &wrong_generation).unwrap();
        fs::write(
            path.join("CURRENT"),
            Current {
                generation: 1,
                digest: Sha256::digest(&wrong_generation).into(),
            }
            .encode()
            .unwrap(),
        )
        .unwrap();
        assert!(matches!(
            open(&path),
            Err(Error::Corrupt("selected manifest generation mismatch"))
        ));
        fs::remove_file(selected).unwrap();
        assert!(matches!(
            open(&path),
            Err(Error::Corrupt("missing authoritative storage file"))
        ));
    }

    #[test]
    fn interrupted_file_handover_never_reclaims_before_selection_and_poisons() {
        for step in 0..8 {
            let (_directory, mut store) = new_store();
            let path = store.directory.clone();
            let (mut manifest, digests) = write_log(&store, 1);
            manifest.checkpoint_sequence = 1;
            manifest.checkpoint_digest = digests[1];
            let checkpoint = view(&store);
            publish(&mut store, &checkpoint, manifest).unwrap();
            drop(checkpoint);
            let candidate =
                super::super::maintenance::prepare(&mut store, 1, 2, true, true).unwrap();
            store.fail_after = Some(step);
            assert!(super::super::maintenance::install(&mut store, candidate).is_err());
            assert!(matches!(
                super::super::maintenance::reclaim(&store),
                Err(Error::NeedsRecovery)
            ));
            assert!(matches!(apply(&mut store, &[]), Err(Error::NeedsRecovery)));
            assert!(path.join("pages-00000000000000000001.bin").exists());
            assert!(path.join("pages-00000000000000000002.bin").exists());
            assert!(path.join("log-00000000000000000001.bin").exists());
            drop(store);
            let recovered = open(&path).unwrap();
            assert_eq!(
                recovered.manifest.page_file_id,
                if step >= 6 { 2 } else { 1 }
            );
            super::super::maintenance::reclaim(&recovered).unwrap();
            assert!(
                path.join(format!("pages-{:020}.bin", recovered.manifest.page_file_id))
                    .exists()
            );
            assert_eq!(path.join("log-00000000000000000001.bin").exists(), step < 6);
        }
    }

    #[test]
    fn interrupted_namespace_publication_preserves_claims_and_requires_attach_retry() {
        for step in 0..8 {
            let (_directory, mut store) = new_store();
            let path = store.directory.clone();
            apply(&mut store, &[cursor(1, 0)]).unwrap();
            publish_cursors(&mut store, 2).unwrap();
            write_new(&path.join("ATTACH_REQUIRED"), b"attach").unwrap();
            store.fail_after = Some(step);
            assert!(renew_namespace(&mut store, [3; 16]).is_err());
            assert!(matches!(
                renew_namespace(&mut store, [4; 16]),
                Err(Error::NeedsRecovery)
            ));
            drop(store);
            assert!(matches!(open(&path), Err(Error::AttachRequired)));
            let store = super::super::backup::attach(&path, [4; 16], true).unwrap();
            assert_eq!(store.manifest.cursor_namespace, [4; 16]);
            assert_eq!(store.manifest.next_cursor_id, 2);
            assert_eq!(
                get(&view(&store), TreeId::Cursors, &1_u64.to_be_bytes()).unwrap(),
                cursor(1, 0).value
            );
            assert!(store.is_read_only());
            drop(store);
            assert!(open(&path).unwrap().is_read_only());
        }
    }

    mod fault_tests;
}
