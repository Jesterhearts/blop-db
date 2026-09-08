//! Append-only, copy-on-write storage using the version 1 page and publication
//! formats.
//!
//! This is an engine-facing API, not a transaction execution API. Physical
//! batches contain already-validated system entries. The caller owns
//! catalogue/schema consistency, outcomes, durability-before-execution,
//! resolved-prefix selection and retention claims. A [`View`] is an immutable
//! physical root set, not an externally visible database snapshot.
//!
//! Durability requires atomic same-directory rename and working file and
//! directory synchronization. The Windows backend is experimental and has not
//! been runtime-tested; synchronization errors are propagated, not ignored. One
//! process owns the directory until its store, views and scans have all been
//! dropped. No VM, sequencer, logical log writer or changefeed is included.

pub(crate) mod backup;
pub mod encoding;
pub(crate) mod maintenance;
mod metadata;
pub mod mvcc;
mod page;
mod platform;
mod store;
mod tree;

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
pub use store::get;
pub use store::open;
pub use store::prepare_checkpoint;
pub use store::publish;
pub use store::scan;
pub use store::view;

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

/// An owned physical key and value, in unsigned lexicographic key order.
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
