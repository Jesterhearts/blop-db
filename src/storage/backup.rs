//! Pinned physical directory images and explicit local-identity attachment.

use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Read;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use sha2::Digest;
use sha2::Sha256;

use super::Manifest;
use super::Result;
use super::Store;
use super::page::PAGE_SIZE;
use super::platform;
use super::store;

pub(crate) struct Image {
    pub(crate) manifest: Manifest,
    genesis: Vec<u8>,
    selected: Vec<u8>,
    current: Vec<u8>,
    directory: PathBuf,
    _lease: Arc<store::DirectoryLease>,
}

/// Called on the sequencer, never by a worker independently reading CURRENT.
pub(crate) fn capture(store: &Store) -> Result<Image> {
    store::writable(store)?;
    let manifest = store.manifest().clone();
    let directory = store.directory();
    let genesis = store::read_bounded(&directory.join("GENESIS"), 180)?;
    let selected = store::read_bounded(
        &directory.join(format!("manifest-{:020}.bin", manifest.generation)),
        16 * 1024 * 1024,
    )?;
    let current = store::read_bounded(&directory.join("CURRENT"), 64)?;
    let pointer = super::Current::decode(&current)?;
    if genesis != store.genesis().encode()?
        || selected != store.selected_manifest().encode()?
        || pointer.generation != manifest.generation
        || pointer.digest != <[u8; 32]>::from(Sha256::digest(&selected))
    {
        return Err(super::Error::Corrupt(
            "backup metadata differs from pinned publication",
        ));
    }
    // A WAL owner can advance D without a CURRENT publication. Freeze its
    // proven prefix into destination metadata while preserving the selected
    // checkpoint roots, rather than copying a moving source tail.
    let selected = manifest.encode()?;
    let current = super::Current {
        generation: manifest.generation,
        digest: Sha256::digest(&selected).into(),
    }
    .encode()?
    .to_vec();
    Ok(Image {
        manifest,
        genesis,
        selected,
        current,
        directory: directory.to_owned(),
        _lease: Arc::clone(&store.lease),
    })
}

/// Consumes the pin on a blocking worker. A dropped reply cannot cancel I/O or
/// release the source lease. Failed output remains an incomplete directory.
pub(crate) fn copy(
    image: Image,
    destination: &Path,
) -> Result<Manifest> {
    fs::create_dir(destination)?;
    let destination = fs::canonicalize(destination)?;
    let _destination_lease = store::lock(&destination)?;
    let parent = platform::open_directory(
        destination
            .parent()
            .ok_or(super::Error::InvalidInput("backup directory has no parent"))?,
    )?;
    platform::sync_directory(&parent)?;
    let directory = platform::open_directory(&destination)?;
    store::write_new(
        &destination.join("ATTACH_REQUIRED"),
        b"BLOP attach required 1\n",
    )?;
    platform::sync_directory(&directory)?;
    store::write_new(&destination.join("GENESIS"), &image.genesis)?;
    // The shared source lease prevents removal, so open one source at a time
    // instead of exhausting file descriptors on a long retained log.
    let files = std::iter::once((
        format!("pages-{:020}.bin", image.manifest.page_file_id),
        image.manifest.page_count * PAGE_SIZE as u64,
    ))
    .chain(image.manifest.segments.iter().map(|segment| {
        (
            format!("log-{:020}.bin", segment.segment_id),
            segment.committed_bytes,
        )
    }));
    for (name, length) in files {
        let file = File::open(image.directory.join(&name))?;
        let mut output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(destination.join(name))?;
        let copied = io::copy(&mut file.take(length), &mut output)?;
        if copied != length {
            return Err(super::Error::Corrupt("backup source prefix is truncated"));
        }
        platform::sync_file(&output)?;
    }
    store::write_new(
        &destination.join(format!("manifest-{:020}.bin", image.manifest.generation)),
        &image.selected,
    )?;
    platform::sync_directory(&directory)?;
    store::write_new(&destination.join("CURRENT.pending"), &image.current)?;
    platform::rename(
        &destination.join("CURRENT.pending"),
        &destination.join("CURRENT"),
    )?;
    platform::sync_directory(&directory)?;
    Ok(image.manifest)
}

/// The caller selects a new random namespace outside the VM. Marker removal is
/// last: an interrupted attach is either complete or still requires attachment.
pub(crate) fn attach(
    path: &Path,
    namespace: [u8; 16],
    read_only: bool,
) -> Result<Store> {
    let mut store = store::open_directory(path, true)?;
    let directory = platform::open_directory(store.directory())?;
    let marker = store.directory().join("ATTACH_REQUIRED");
    if !marker.try_exists()? {
        store::write_new(&marker, b"BLOP attach required 1\n")?;
    }
    platform::sync_directory(&directory)?;
    store::renew_namespace(&mut store, namespace)?;
    let role = store.directory().join("READ_ONLY");
    if read_only {
        if !role.try_exists()? {
            store::write_new(&role, b"BLOP read-only replica 1\n")?;
        }
    } else if role.try_exists()? {
        fs::remove_file(role)?;
    }
    platform::sync_directory(&directory)?;
    store::set_read_only(&mut store, read_only);
    fs::remove_file(marker)?;
    platform::sync_directory(&directory)?;
    Ok(store)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Error;
    use crate::storage::Genesis;
    use crate::storage::LimitPolicy;
    use crate::storage::{
        self,
    };

    #[test]
    fn failed_copy_leaves_attach_marker_but_never_selects_incomplete_files_or_overwrites() {
        let directory = tempfile::tempdir().unwrap();
        let store = storage::create(
            directory.path().join("source"),
            Genesis {
                database_id: [1; 16],
                initial_policy: LimitPolicy::new([0; 17]).unwrap(),
            },
            [2; 16],
        )
        .unwrap();
        let image = capture(&store).unwrap();
        // Simulate loss of committed source bytes after capture. The
        // destination must not publish even a checksum-valid manifest
        // over a short copy.
        OpenOptions::new()
            .write(true)
            .open(store.directory().join("pages-00000000000000000001.bin"))
            .unwrap()
            .set_len(PAGE_SIZE as u64)
            .unwrap();
        let destination = directory.path().join("backup");
        assert!(matches!(copy(image, &destination), Err(Error::Corrupt(_))));
        assert!(destination.join("ATTACH_REQUIRED").exists());
        assert!(!destination.join("CURRENT").exists());
        assert!(matches!(
            storage::open(&destination),
            Err(Error::AttachRequired)
        ));
        let original = fs::read(destination.join("GENESIS")).unwrap();
        assert!(matches!(
            copy(capture(&store).unwrap(), &destination),
            Err(Error::Io(_))
        ));
        assert_eq!(fs::read(destination.join("GENESIS")).unwrap(), original);
    }

    #[test]
    fn capture_rejects_metadata_that_no_longer_matches_the_selected_store() {
        let directory = tempfile::tempdir().unwrap();
        let store = storage::create(
            directory.path().join("source"),
            Genesis {
                database_id: [1; 16],
                initial_policy: LimitPolicy::new([0; 17]).unwrap(),
            },
            [2; 16],
        )
        .unwrap();
        let mut current =
            storage::Current::decode(&fs::read(store.directory().join("CURRENT")).unwrap())
                .unwrap();
        current.generation += 1;
        fs::write(store.directory().join("CURRENT"), current.encode().unwrap()).unwrap();
        assert!(matches!(capture(&store), Err(Error::Corrupt(_))));
    }
}
