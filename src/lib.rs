#![doc = include_str!("../README.md")]

mod bind;

#[cfg(any(unix, windows))]
mod limits;

#[cfg(any(unix, windows))]
pub use limits::Limits;

#[cfg(any(unix, windows))]
pub mod database;

#[cfg(any(unix, windows))]
pub use database::CursorKind;
#[cfg(any(unix, windows))]
pub use database::CursorToken;
#[cfg(any(unix, windows))]
pub use database::Database;
#[cfg(any(unix, windows))]
pub use database::FeedBatch;
#[cfg(any(unix, windows))]
pub use database::Snapshot;
#[cfg(any(unix, windows))]
pub use database::Watermark;

#[cfg(any(unix, windows))]
pub mod storage;

#[cfg(any(unix, windows))]
pub mod vm;

pub use bind::BuildError;
pub use bind::Transaction;
#[doc(hidden)]
pub use blop_db_macros::compile_tx as __compile_tx;

#[doc(hidden)]
pub mod __private {
    pub use crate::bind::bind_program;
    pub use crate::bind::push_blob;
}

/// Compile a transaction program and bind its runtime inputs.
///
/// Returns a [`Result<Transaction, BuildError>`]. This constructs a
/// transaction; it does not execute the program.
///
/// ```
/// let transaction = blop_db::tx! { -> i64 { return 42; } }?;
/// assert!(transaction.program_bytes().starts_with(b"BLOPVM01"));
/// # Ok::<(), blop_db::BuildError>(())
/// ```
#[macro_export]
macro_rules! tx {
    ($($tokens:tt)*) => {
        $crate::__compile_tx! { $crate; $($tokens)* }
    };
}
