//! Isolated from parallel unit tests: fork temporarily inherits their file
//! descriptions until exec, which can otherwise delay unrelated lock release.

#![cfg(any(unix, windows))]

use blop_db::storage::Error;
use blop_db::storage::Genesis;
use blop_db::storage::LimitPolicy;
use blop_db::storage::{
    self,
};

#[test]
fn directory_lock_is_exclusive_in_a_native_child_process() {
    const VARIABLE: &str = "BLOP_DB_LOCK_CHILD";
    if let Some(path) = std::env::var_os(VARIABLE) {
        assert!(matches!(
            storage::open(std::path::PathBuf::from(path)),
            Err(Error::Locked)
        ));
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let store = storage::create(
        directory.path().join("db"),
        Genesis {
            database_id: [1; 16],
            initial_policy: LimitPolicy::new([0; 17]).unwrap(),
        },
        [2; 16],
    )
    .unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "directory_lock_is_exclusive_in_a_native_child_process",
            "--nocapture",
        ])
        .env(VARIABLE, store.directory())
        .status()
        .unwrap();
    assert!(status.success());
}
