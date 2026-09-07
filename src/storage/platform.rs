//! Filesystem operations whose platform semantics matter to storage correctness.
//!
//! Offset I/O must not be mixed with cursor-based I/O on the same handle: Windows offset
//! operations also change the cursor. Handles must be synchronous and opened without append.
//! Publication callers serialize namespace changes and supply paths in one directory.

use std::{fs::File, fs::OpenOptions, io, path::Path};

#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::{
    ffi::OsStrExt,
    fs::{FileExt, OpenOptionsExt},
};

/// Complete an offset read, retrying interruptions and reporting short files explicitly.
pub(super) fn read_exact_at(file: &File, mut bytes: &mut [u8], mut offset: u64) -> io::Result<()> {
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
pub(super) fn write_all_at(file: &File, mut bytes: &[u8], mut offset: u64) -> io::Result<()> {
    check_range(offset, bytes.len())?;
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
    Ok(())
}

fn check_range(offset: u64, length: usize) -> io::Result<()> {
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
    file.sync_all()
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
        // Directory handles need this flag; FlushFileBuffers requires GENERIC_WRITE.
        options.write(true).custom_flags(FILE_FLAG_BACKUP_SEMANTICS);
    }
    let file = options.open(path)?;
    if !file.metadata()?.is_dir() {
        return Err(io::ErrorKind::NotADirectory.into());
    }
    Ok(file)
}

/// Never substitute a no-op when a filesystem does not support directory flushing.
pub(super) fn sync_directory(directory: &File) -> io::Result<()> {
    directory.sync_all()
}

/// Replace a name without a delete-first gap or cross-volume copy fallback.
///
/// The caller flushes the source first and the directory afterwards. Windows write-through
/// is additional protection, not a substitute for the directory flush or crash testing.
pub(super) fn rename(source: &Path, destination: &Path) -> io::Result<()> {
    if source.parent().is_none() || source.parent() != destination.parent() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "publication rename must stay in one directory",
        ));
    }
    #[cfg(unix)]
    {
        std::fs::rename(source, destination)
    }
    #[cfg(windows)]
    {
        use windows_sys::Win32::Storage::FileSystem::{
            MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
        };
        let source = wide_path(source)?;
        let destination = wide_path(destination)?;
        // SAFETY: Both paths are live, NUL-terminated UTF-16 buffers without interior NULs.
        let result = unsafe {
            MoveFileExW(
                source.as_ptr(),
                destination.as_ptr(),
                MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
            )
        };
        if result == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
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

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

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
