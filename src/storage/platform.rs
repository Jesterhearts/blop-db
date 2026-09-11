//! Provide the platform-specific file operations required for durable storage.
//!
//! Do not mix offset and cursor-based I/O on one handle: Windows offset I/O
//! also changes the cursor. Open synchronous handles without append mode.
//! Publication callers must serialize filename changes and use paths in the
//! same directory.

use std::fs::File;
use std::fs::OpenOptions;
use std::io;
#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::ffi::OsStrExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;
use std::path::Path;

/// Read the requested bytes at an offset, retrying interrupted operations.
///
/// Return an error if the file is too short.
pub(super) fn read_exact_at(
    file: &File,
    mut bytes: &mut [u8],
    mut offset: u64,
) -> io::Result<()> {
    check_range(offset, bytes.len())?;
    while !bytes.is_empty() {
        #[cfg(unix)]
        let result = file.read_at(bytes, offset);
        #[cfg(windows)]
        let result = file.seek_read(bytes, offset);
        match result {
            Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
            Ok(count) => {
                offset += count as u64;
                bytes = &mut bytes[count..];
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

/// Complete an offset write without consulting the handle's current cursor.
pub(super) fn write_all_at(
    file: &File,
    mut bytes: &[u8],
    mut offset: u64,
) -> io::Result<()> {
    check_range(offset, bytes.len())?;
    #[cfg(test)]
    let operation = faults::Operation::Write {
        offset,
        length: bytes.len(),
    };
    #[cfg(test)]
    if faults::hit(operation.clone(), faults::Phase::Before)? {
        let _guard = faults::Guard::suspend();
        write_all_at(file, &bytes[..bytes.len().div_ceil(2)], offset)?;
        return Err(faults::error());
    }
    while !bytes.is_empty() {
        #[cfg(unix)]
        let result = file.write_at(bytes, offset);
        #[cfg(windows)]
        let result = file.seek_write(bytes, offset);
        match result {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(count) => {
                offset += count as u64;
                bytes = &bytes[count..];
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    #[cfg(test)]
    faults::hit(operation, faults::Phase::After)?;
    Ok(())
}

fn check_range(
    offset: u64,
    length: usize,
) -> io::Result<()> {
    if offset
        .checked_add(length as u64)
        .is_none_or(|end| end > i64::MAX as u64)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "file offset exceeds the signed 64-bit range",
        ));
    }
    Ok(())
}

/// File handles passed here must have write access on Windows.
pub(super) fn sync_file(file: &File) -> io::Result<()> {
    #[cfg(test)]
    faults::hit(faults::Operation::SyncFile, faults::Phase::Before)?;
    file.sync_all()?;
    #[cfg(test)]
    faults::hit(faults::Operation::SyncFile, faults::Phase::After)?;
    Ok(())
}

pub(super) fn sync_file_path(path: &Path) -> io::Result<()> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(windows)]
    options.write(true);
    sync_file(&options.open(path)?)
}

pub(super) fn open_directory(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(windows)]
    {
        use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_BACKUP_SEMANTICS;
        // Directory handles need this flag; FlushFileBuffers requires
        // GENERIC_WRITE.
        options.write(true).custom_flags(FILE_FLAG_BACKUP_SEMANTICS);
    }
    let file = options.open(path)?;
    if !file.metadata()?.is_dir() {
        return Err(io::ErrorKind::NotADirectory.into());
    }
    Ok(file)
}

/// Never substitute a no-op when a filesystem does not support directory
/// flushing.
pub(super) fn sync_directory(directory: &File) -> io::Result<()> {
    #[cfg(test)]
    faults::hit(faults::Operation::SyncDirectory, faults::Phase::Before)?;
    directory.sync_all()?;
    #[cfg(test)]
    faults::hit(faults::Operation::SyncDirectory, faults::Phase::After)?;
    Ok(())
}

/// Replace a filename atomically without first deleting the destination.
///
/// Flush the source file before replacement and the directory afterwards.
/// Cross-volume copying is not a fallback. Windows write-through adds
/// protection but does not replace directory flushing or crash tests.
pub(super) fn rename(
    source: &Path,
    destination: &Path,
) -> io::Result<()> {
    if source.parent().is_none() || source.parent() != destination.parent() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "publication rename must stay in one directory",
        ));
    }
    #[cfg(test)]
    let operation = faults::Operation::Rename(
        destination
            .file_name()
            .unwrap_or(destination.as_os_str())
            .to_owned(),
    );
    #[cfg(test)]
    faults::hit(operation.clone(), faults::Phase::Before)?;
    #[cfg(unix)]
    {
        std::fs::rename(source, destination)?;
    }
    #[cfg(windows)]
    {
        use windows_sys::Win32::Storage::FileSystem::MOVEFILE_REPLACE_EXISTING;
        use windows_sys::Win32::Storage::FileSystem::MOVEFILE_WRITE_THROUGH;
        use windows_sys::Win32::Storage::FileSystem::MoveFileExW;
        let source = wide_path(source)?;
        let destination = wide_path(destination)?;
        // SAFETY: Both paths are live, NUL-terminated UTF-16 buffers without
        // interior NULs.
        let result = unsafe {
            MoveFileExW(
                source.as_ptr(),
                destination.as_ptr(),
                MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
            )
        };
        if result == 0 {
            return Err(io::Error::last_os_error());
        }
    }
    #[cfg(test)]
    faults::hit(operation, faults::Phase::After)?;
    Ok(())
}

#[cfg(windows)]
fn wide_path(path: &Path) -> io::Result<Vec<u16>> {
    let mut encoded: Vec<_> = path.as_os_str().encode_wide().collect();
    if encoded.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "path contains NUL",
        ));
    }
    encoded.push(0);
    Ok(encoded)
}

/// Inject scoped errors into real I/O operations.
///
/// This does not simulate power loss or write reordering.
#[cfg(test)]
pub(crate) mod faults {
    use std::cell::RefCell;
    use std::ffi::OsString;
    use std::marker::PhantomData;
    use std::rc::Rc;

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub(crate) enum Operation {
        Write { offset: u64, length: usize },
        SyncFile,
        SyncDirectory,
        Rename(OsString),
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub(crate) enum Phase {
        Before,
        After,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub(crate) struct Event(pub Operation, pub Phase);

    #[derive(Clone, Copy, Debug)]
    pub(crate) enum Failure {
        Error,
        PartialWrite,
        Exit,
    }

    struct State {
        failure: Option<(usize, Failure)>,
        trace: Vec<Event>,
    }

    thread_local! {
        static STATE: RefCell<Option<State>> = const { RefCell::new(None) };
    }

    // A scope must be dropped on its originating thread to restore that
    // thread's state.
    pub(crate) struct Guard {
        previous: Option<State>,
        _local: PhantomData<Rc<()>>,
    }

    impl Guard {
        pub(crate) fn new(failure: Option<(usize, Failure)>) -> Self {
            Self {
                previous: STATE.replace(Some(State {
                    failure,
                    trace: Vec::new(),
                })),
                _local: PhantomData,
            }
        }

        pub(super) fn suspend() -> Self {
            Self {
                previous: STATE.replace(None),
                _local: PhantomData,
            }
        }

        pub(crate) fn trace(&self) -> Vec<Event> {
            STATE.with_borrow(|state| state.as_ref().unwrap().trace.clone())
        }
    }

    impl Drop for Guard {
        fn drop(&mut self) {
            STATE.replace(self.previous.take());
        }
    }

    pub(super) fn error() -> std::io::Error {
        std::io::Error::other("injected filesystem boundary failure")
    }

    pub(super) fn hit(
        operation: Operation,
        phase: Phase,
    ) -> std::io::Result<bool> {
        STATE.with_borrow_mut(|state| {
            let Some(state) = state else { return Ok(false) };
            let index = state.trace.len();
            let partial = matches!(operation, Operation::Write { .. }) && phase == Phase::Before;
            state.trace.push(Event(operation, phase));
            match state.failure {
                Some((target, failure)) if target == index => match failure {
                    Failure::Exit => std::process::exit(77),
                    Failure::PartialWrite if partial => Ok(true),
                    _ => Err(error()),
                },
                _ => Ok(false),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    #[test]
    fn fault_scopes_restore_nested_state_on_unwind_and_do_not_cross_threads() {
        let file = tempfile::tempfile().unwrap();
        let outer = faults::Guard::new(Some((2, faults::Failure::Error)));
        sync_file(&file).unwrap();
        let result = std::panic::catch_unwind(|| {
            let _inner = faults::Guard::new(Some((0, faults::Failure::Error)));
            assert!(sync_file(&file).is_err());
            panic!("unwind nested scope");
        });
        assert!(result.is_err());
        std::thread::scope(|scope| {
            scope.spawn(|| sync_file(&file).unwrap()).join().unwrap();
        });
        assert_eq!(outer.trace().len(), 2);
        assert!(sync_file(&file).is_err());
        drop(outer);
        sync_file(&file).unwrap();
    }

    #[test]
    fn offset_io_reports_partial_eof_and_does_not_touch_other_bytes() {
        let file = tempfile::tempfile().unwrap();
        write_all_at(&file, b"abcdef", 0).unwrap();
        write_all_at(&file, b"XY", 2).unwrap();
        let mut bytes = [0; 6];
        read_exact_at(&file, &mut bytes, 0).unwrap();
        assert_eq!(&bytes, b"abXYef");
        let mut bytes = [9; 3];
        assert_eq!(
            read_exact_at(&file, &mut bytes, 5).unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
        assert_eq!(bytes, [b'f', 9, 9]);
        read_exact_at(&file, &mut [], 6).unwrap();
        write_all_at(&file, &[], 6).unwrap();
        assert_eq!(file.metadata().unwrap().len(), 6);
    }

    #[test]
    fn invalid_offsets_are_rejected_before_io_and_file_errors_propagate() {
        let file = tempfile::NamedTempFile::new().unwrap();
        write_all_at(file.as_file(), b"old", 0).unwrap();
        for offset in [i64::MAX as u64, u64::MAX] {
            assert_eq!(
                write_all_at(file.as_file(), b"new", offset)
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidInput
            );
            assert_eq!(
                read_exact_at(file.as_file(), &mut [0; 3], offset)
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidInput
            );
        }
        let read_only = File::open(file.path()).unwrap();
        assert!(write_all_at(&read_only, b"new", 0).is_err());
        assert_eq!(fs::read(file.path()).unwrap(), b"old");
    }

    #[test]
    fn shared_handles_use_explicit_offsets_for_each_operation() {
        let file = tempfile::tempfile().unwrap();
        file.set_len(8 * 256).unwrap();
        std::thread::scope(|scope| {
            for index in 0..8_u8 {
                let file = &file;
                scope.spawn(move || {
                    let expected = [index; 256];
                    for _ in 0..100 {
                        write_all_at(file, &expected, u64::from(index) * 256).unwrap();
                        let mut actual = [0; 256];
                        read_exact_at(file, &mut actual, u64::from(index) * 256).unwrap();
                        assert_eq!(actual, expected);
                    }
                });
            }
        });
    }

    #[test]
    fn file_and_directory_flushes_do_not_create_or_truncate_files() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("data");
        fs::write(&path, b"value").unwrap();
        sync_file_path(&path).unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"value");
        assert!(sync_file_path(&directory.path().join("missing")).is_err());
        assert!(!directory.path().join("missing").exists());
        assert!(open_directory(&path).is_err());
        let handle = open_directory(directory.path()).unwrap();
        sync_directory(&handle).unwrap();
    }

    #[test]
    fn rename_publishes_and_replaces_without_a_copy_or_delete_fallback() {
        let directory = tempfile::tempdir().unwrap();
        let destination = directory.path().join("CURRENT");
        let source = directory.path().join("CURRENT.pending");
        for contents in [b"first".as_slice(), b"replacement"] {
            fs::write(&source, contents).unwrap();
            sync_file_path(&source).unwrap();
            rename(&source, &destination).unwrap();
            sync_directory(&open_directory(directory.path()).unwrap()).unwrap();
            assert_eq!(fs::read(&destination).unwrap(), contents);
            assert!(!source.exists());
        }
        assert!(rename(&source, &destination).is_err());
        assert_eq!(fs::read(&destination).unwrap(), b"replacement");
        fs::write(&source, b"new").unwrap();
        let other = tempfile::tempdir().unwrap();
        assert_eq!(
            rename(&source, &other.path().join("CURRENT"))
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(fs::read(&source).unwrap(), b"new");
    }

    #[cfg(windows)]
    #[test]
    fn native_paths_preserve_utf16_and_reject_interior_nuls() {
        use std::ffi::OsString;
        use std::os::windows::ffi::OsStringExt;
        let path = OsString::from_wide(&[b'a' as u16, 0xd800]);
        assert_eq!(
            wide_path(Path::new(&path)).unwrap(),
            [b'a' as u16, 0xd800, 0]
        );
        assert_eq!(
            wide_path(Path::new("a\0b")).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }
}
