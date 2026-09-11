//! Store immutable pages and publish version 1 write-ahead log (WAL) groups.
//!
//! This engine-facing API uses append-only, copy-on-write pages. Supply
//! validated system entries in physical batches. The caller must check schemas
//! and catalogue consistency, record complete outcomes, make logs durable
//! before execution, select resolved prefixes, and protect retention claims.
//!
//! A [`View`] pins physical roots; it does not establish public visibility. Use
//! [`crate::database`] for transaction execution, sequencing, logical log
//! writing, and changefeeds.
//!
//! # Platform requirements
//!
//! Durability requires atomic same-directory replacement and working file and
//! directory synchronization. Synchronization failures are returned as errors.
//! The Windows backend is experimental and has not been runtime-tested. One
//! process owns the directory until its store, views, and scans are dropped.

pub(crate) mod backup;
mod checkpoint;
pub mod encoding;
mod log_validation;
pub(crate) mod maintenance;
mod metadata;
pub mod mvcc;
mod page;
mod platform;
#[cfg(test)]
pub(crate) use platform::faults;
mod store;
mod tree;
pub(crate) mod wal;

use std::fmt;
use std::io;

pub use metadata::Current;
pub use metadata::Genesis;
pub use metadata::LimitPolicy;
pub use metadata::Manifest;
pub use metadata::SegmentDescriptor;
pub use store::Mutation;
pub use store::Scan;
pub use store::Store;
pub use store::View;
pub use store::apply;
pub use store::checkpoint_view;
pub use store::create;
pub(crate) use store::enable_runtime_cache;
pub use store::get;
pub use store::open;
pub use store::prepare_checkpoint;
pub use store::publish;
pub use store::scan;
pub use store::view;
pub(crate) use store::wal::append_wal;

/// A physical system-tree identity from storage format 1.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum TreeId {
    State = 1,
    Catalogue = 2,
    Policy = 3,
    Outcomes = 4,
    Cursors = 5,
}

impl TreeId {
    pub(crate) fn index(self) -> usize {
        self as usize - 1
    }
}

/// An owned physical key and value; scans order entries by unsigned key bytes.
pub type Entry = (Vec<u8>, Vec<u8>);

/// A storage failure, never a transaction's semantic abort.
#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Corrupt(&'static str),
    InvalidInput(&'static str),
    Unsupported { format: &'static str, version: u16 },
    Locked,
    AttachRequired,
    NeedsRecovery,
    Exhausted,
}

impl fmt::Display for Error {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "storage I/O error: {error}"),
            Self::Corrupt(reason) => write!(f, "storage corruption: {reason}"),
            Self::InvalidInput(reason) => write!(f, "invalid storage input: {reason}"),
            Self::Unsupported { format, version } => {
                write!(f, "unsupported {format} version {version}")
            }
            Self::Locked => f.write_str("database directory already has an owner"),
            Self::AttachRequired => f.write_str("copied directory requires explicit attachment"),
            Self::NeedsRecovery => {
                f.write_str("storage must be reopened after an uncertain failure")
            }
            Self::Exhausted => f.write_str("storage identifier or address space exhausted"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

pub type Result<T> = std::result::Result<T, Error>;
