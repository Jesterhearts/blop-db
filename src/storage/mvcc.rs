//! Encode versioned state and read it through pinned physical roots.
//!
//! A `View` does not establish public visibility. Supply a protected historical
//! sequence or the sequence before a transaction. Check table liveness and
//! validate keys and values against its schema. These functions do not enforce
//! the visible frontier or merge private transaction writes.

use std::borrow::Cow;
use std::ops::Bound;

use super::Entry;
use super::Error;
use super::Result;
use super::Scan;
use super::TreeId;
use super::View;
use super::encoding::MAX_KEY_BYTES;
use super::encoding::MAX_VALUE_BYTES;
use super::encoding::escape_into;
use super::encoding::unescape;

const MAX_STATE_KEY_BYTES: usize = 8 + 2 * MAX_KEY_BYTES + 2 + 8;

/// A valid physical row address and version, independent of its table schema.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StateKey {
    table_id: u64,
    key: Vec<u8>,
    sequence: u64,
}

impl StateKey {
    pub fn new(
        table_id: u64,
        key: Vec<u8>,
        sequence: u64,
    ) -> Result<Self> {
        check_snapshot(table_id, sequence)?;
        check_key(&key)?;
        if sequence <= table_id {
            return Err(Error::InvalidInput("row sequence must exceed its table ID"));
        }
        Ok(Self {
            table_id,
            key,
            sequence,
        })
    }

    pub fn table_id(&self) -> u64 {
        self.table_id
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    pub fn encode(&self) -> Vec<u8> {
        seek_key(self.table_id, &self.key, self.sequence)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        if !(18..=MAX_STATE_KEY_BYTES).contains(&encoded.len()) {
            return Err(Error::InvalidInput("invalid state key length"));
        }
        let table_id = u64::from_be_bytes(encoded[..8].try_into().unwrap());
        let frame = &encoded[8..encoded.len() - 8];
        let (key, consumed) = unescape(frame)?;
        if consumed != frame.len() {
            return Err(Error::InvalidInput("trailing state key frame bytes"));
        }
        let sequence = !u64::from_be_bytes(encoded[encoded.len() - 8..].try_into().unwrap());
        Self::new(table_id, key, sequence)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StateValue {
    Put(Vec<u8>),
    Delete,
}

impl StateValue {
    pub fn encode(&self) -> Result<Vec<u8>> {
        match self {
            Self::Delete => Ok(vec![0]),
            Self::Put(value) => {
                if value.len() > MAX_VALUE_BYTES {
                    return Err(Error::InvalidInput("state value exceeds 16 MiB"));
                }
                let mut encoded = Vec::with_capacity(5 + value.len());
                encoded.push(1);
                encoded.extend_from_slice(&(value.len() as u32).to_le_bytes());
                encoded.extend_from_slice(value);
                Ok(encoded)
            }
        }
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        Ok(match value_payload(encoded)? {
            Some(value) => Self::Put(value.to_vec()),
            None => Self::Delete,
        })
    }
}

fn value_payload(encoded: &[u8]) -> Result<Option<&[u8]>> {
    if encoded == [0] {
        return Ok(None);
    }
    if encoded.len() < 5 || encoded[0] != 1 {
        return Err(Error::InvalidInput("invalid state value tag or length"));
    }
    let length = u32::from_le_bytes(encoded[1..5].try_into().unwrap()) as usize;
    if length > MAX_VALUE_BYTES || length != encoded.len() - 5 {
        return Err(Error::InvalidInput("invalid state Put length"));
    }
    Ok(Some(&encoded[5..]))
}

fn check_snapshot(
    table_id: u64,
    sequence: u64,
) -> Result<()> {
    if table_id == 0 || table_id == u64::MAX {
        return Err(Error::InvalidInput("invalid state table ID"));
    }
    if sequence == u64::MAX {
        return Err(Error::InvalidInput("reserved snapshot sequence"));
    }
    Ok(())
}

fn check_key(key: &[u8]) -> Result<()> {
    if key.len() > MAX_KEY_BYTES {
        return Err(Error::InvalidInput("user key exceeds 1,024 bytes"));
    }
    Ok(())
}

fn address_prefix(
    table_id: u64,
    key: &[u8],
) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(18 + 2 * key.len());
    prefix.extend_from_slice(&table_id.to_be_bytes());
    escape_into(key, &mut prefix);
    prefix
}

fn seek_key(
    table_id: u64,
    key: &[u8],
    sequence: u64,
) -> Vec<u8> {
    let mut encoded = address_prefix(table_id, key);
    encoded.extend_from_slice(&(!sequence).to_be_bytes());
    encoded
}

fn after_key(
    table_id: u64,
    key: &[u8],
) -> Vec<u8> {
    let mut end = address_prefix(table_id, key);
    // Replacing the terminator's last 00 by 01 skips every version, but no
    // extension of the key.
    *end.last_mut().unwrap() = 1;
    end
}

fn corrupt_input(error: Error) -> Error {
    match error {
        Error::InvalidInput(reason) => Error::Corrupt(reason),
        error => error,
    }
}

/// Return the newest value at or before `sequence`, unless it is a tombstone.
///
/// A tombstone means absence; do not fall back to an older Put.
///
/// The caller protects the snapshot and checks table liveness and schema
/// validity. Sequence zero is a valid seek bound even though it is not a stored
/// row version.
pub fn get(
    view: &View,
    table: u64,
    key: &[u8],
    sequence: u64,
) -> Result<Option<Vec<u8>>> {
    Ok(read_value(view, table, key, sequence)?.map(|value| value.as_bytes().to_vec()))
}

/// A validated Put payload backed by retained bytes.
///
/// Inline bytes retain their immutable leaf; overflow bytes use an owned
/// buffer. Neither form retains a file handle or directory lease.
pub(crate) struct ReadValue(ReadSource);

enum ReadSource {
    Inline(super::tree::LocatedCell),
    Overflow(Vec<u8>),
}

impl ReadValue {
    pub(crate) fn as_bytes(&self) -> &[u8] {
        let encoded = match &self.0 {
            ReadSource::Inline(located) => {
                let super::page::Value::Inline(bytes) = &located.cell().value else {
                    unreachable!("inline reads retain an inline cell");
                };
                bytes
            }
            ReadSource::Overflow(bytes) => bytes,
        };
        &encoded[5..]
    }
}

pub(crate) fn read_value(
    view: &View,
    table: u64,
    key: &[u8],
    sequence: u64,
) -> Result<Option<ReadValue>> {
    check_snapshot(table, sequence)?;
    check_key(key)?;
    let lower = seek_key(table, key, sequence);
    let prefix_len = lower.len() - 8;
    let Some(located) = super::tree::seek(
        &view.reader,
        TreeId::State,
        view.roots[TreeId::State.index()],
        &lower,
    )?
    else {
        return Ok(None);
    };
    let cell = located.cell();
    // For a candidate at or above the seek bound, this exact prefix test is
    // equivalent to the exclusive upper bound ending in 00 01. It also checks
    // the canonical address without decoding or allocating another key.
    if !cell.key.starts_with(&lower[..prefix_len]) {
        return Ok(None);
    }
    if cell.key.len() != lower.len() {
        return Err(Error::Corrupt(
            "state seeker returned an unexpected address",
        ));
    }
    let stored_sequence = !u64::from_be_bytes(cell.key[prefix_len..].try_into().unwrap());
    if stored_sequence <= table || stored_sequence > sequence {
        return Err(Error::Corrupt("invalid state seeker sequence"));
    }
    let encoded_value = view.reader.value_bytes(TreeId::State, &cell.value)?;
    if value_payload(&encoded_value)
        .map_err(corrupt_input)?
        .is_none()
    {
        return Ok(None);
    }
    Ok(Some(ReadValue(match encoded_value {
        Cow::Borrowed(_) => ReadSource::Inline(located),
        Cow::Owned(bytes) => ReadSource::Overflow(bytes),
    })))
}

/// A lazy scan of logical `(canonical_key, schema_encoded_value)` entries.
///
/// The iterator owns a pinned physical reader rather than the complete result
/// set. It validates each consumed physical key and value frame, including
/// invisible and obsolete versions. The caller must validate schemas.
pub struct StateScan {
    physical: Scan,
    table_id: u64,
    sequence: u64,
    selected_key: Option<Vec<u8>>,
    done: bool,
}

/// Scan a table at a sequence whose history the caller protects.
///
/// The caller must check the catalogue and handle any transaction overlay.
pub fn scan(
    view: &View,
    table: u64,
    sequence: u64,
    lower: Bound<&[u8]>,
    upper: Bound<&[u8]>,
) -> Result<StateScan> {
    check_snapshot(table, sequence)?;
    for bound in [lower, upper] {
        if let Bound::Included(key) | Bound::Excluded(key) = bound {
            check_key(key)?;
        }
    }
    let lower = match lower {
        Bound::Unbounded => table.to_be_bytes().to_vec(),
        Bound::Included(key) => seek_key(table, key, sequence),
        Bound::Excluded(key) => after_key(table, key),
    };
    let upper = match upper {
        Bound::Unbounded => (table + 1).to_be_bytes().to_vec(),
        Bound::Included(key) => after_key(table, key),
        Bound::Excluded(key) => address_prefix(table, key),
    };
    let physical = super::scan(
        view,
        TreeId::State,
        Bound::Included(&lower),
        Bound::Excluded(&upper),
    )?;
    Ok(StateScan {
        physical,
        table_id: table,
        sequence,
        selected_key: None,
        done: false,
    })
}

fn advance(
    physical: &mut Scan,
    table_id: u64,
    sequence: u64,
    selected_key: &mut Option<Vec<u8>>,
) -> Result<Option<Entry>> {
    for entry in physical {
        let (encoded_key, encoded_value) = entry?;
        let key = StateKey::decode(&encoded_key).map_err(corrupt_input)?;
        let value = value_payload(&encoded_value).map_err(corrupt_input)?;
        if key.table_id != table_id {
            return Err(Error::Corrupt("state scan crossed its table boundary"));
        }
        if key.sequence > sequence || selected_key.as_deref() == Some(key.key.as_slice()) {
            continue;
        }
        *selected_key = Some(key.key.clone());
        if let Some(value) = value {
            return Ok(Some((key.key, value.to_vec())));
        }
    }
    Ok(None)
}

impl Iterator for StateScan {
    type Item = Result<Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        match advance(
            &mut self.physical,
            self.table_id,
            self.sequence,
            &mut self.selected_key,
        ) {
            Ok(Some(entry)) => Some(Ok(entry)),
            Ok(None) => {
                self.done = true;
                None
            }
            Err(error) => {
                self.done = true;
                Some(Err(error))
            }
        }
    }
}

impl std::iter::FusedIterator for StateScan {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Genesis;
    use crate::storage::LimitPolicy;
    use crate::storage::Mutation;
    use crate::storage::Store;
    use crate::storage::apply;
    use crate::storage::create;
    use crate::storage::view;

    fn create_store() -> (tempfile::TempDir, Store) {
        let directory = tempfile::tempdir().unwrap();
        let genesis = Genesis {
            database_id: [1; 16],
            initial_policy: LimitPolicy::new([0; 17]).unwrap(),
        };
        let store = create(directory.path().join("db"), genesis, [2; 16]).unwrap();
        (directory, store)
    }

    fn mutation(
        table: u64,
        key: &[u8],
        sequence: u64,
        value: StateValue,
    ) -> Mutation {
        Mutation {
            tree: TreeId::State,
            key: StateKey::new(table, key.to_vec(), sequence)
                .unwrap()
                .encode(),
            value: Some(value.encode().unwrap()),
        }
    }

    #[test]
    fn fixed_state_key_vector_and_descending_versions() {
        let key = StateKey::new(1, b"a\0\0".to_vec(), 9).unwrap();
        let expected = [
            0, 0, 0, 0, 0, 0, 0, 1, b'a', 0, 0xff, 0, 0xff, 0, 0, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xf6,
        ];
        assert_eq!(key.encode(), expected);
        assert_eq!(StateKey::decode(&expected).unwrap(), key);
        assert_eq!(key.table_id(), 1);
        assert_eq!(key.key(), b"a\0\0");
        assert_eq!(key.sequence(), 9);
        let newer = StateKey::new(1, key.key().to_vec(), 10).unwrap();
        assert!(newer.encode() < key.encode());
        for bytes in [vec![], vec![0; MAX_KEY_BYTES], vec![0xff; MAX_KEY_BYTES]] {
            let key = StateKey::new(u64::MAX - 2, bytes, u64::MAX - 1).unwrap();
            assert_eq!(StateKey::decode(&key.encode()).unwrap(), key);
            assert!(key.encode().len() <= MAX_STATE_KEY_BYTES);
        }
        let largest = StateKey::new(1, vec![0; MAX_KEY_BYTES], 2).unwrap();
        assert_eq!(largest.encode().len(), MAX_STATE_KEY_BYTES);
        let mut ordered = Vec::new();
        for table in [1, 2, u64::MAX - 2] {
            for key in [b"".as_slice(), b"\0", b"a", b"a\0", b"\xff"] {
                for sequence in [u64::MAX - 1, table + 1] {
                    let encoded = StateKey::new(table, key.to_vec(), sequence)
                        .unwrap()
                        .encode();
                    if ordered.last() != Some(&encoded) {
                        ordered.push(encoded);
                    }
                }
            }
        }
        assert!(ordered.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn state_keys_reject_invalid_ids_bounds_frames_and_truncation() {
        for (table, sequence) in [(0, 1), (u64::MAX, 2), (1, 0), (1, 1), (2, 1), (1, u64::MAX)] {
            assert!(matches!(
                StateKey::new(table, vec![], sequence),
                Err(Error::InvalidInput(_))
            ));
            assert!(matches!(
                StateKey::decode(&seek_key(table, &[], sequence)),
                Err(Error::InvalidInput(_))
            ));
        }
        assert!(StateKey::new(1, vec![1; MAX_KEY_BYTES + 1], 2).is_err());
        assert!(StateKey::decode(&seek_key(1, &vec![1; MAX_KEY_BYTES + 1], 2)).is_err());
        let encoded = StateKey::new(1, b"a\0b".to_vec(), 9).unwrap().encode();
        for end in 0..encoded.len() {
            assert!(StateKey::decode(&encoded[..end]).is_err());
        }
        assert!(StateKey::decode(&[encoded.clone(), vec![0]].concat()).is_err());
        let mut invalid_escape = encoded.clone();
        invalid_escape[10] = 1;
        assert!(StateKey::decode(&invalid_escape).is_err());
        let mut missing_terminator = encoded;
        missing_terminator.remove(missing_terminator.len() - 9);
        assert!(StateKey::decode(&missing_terminator).is_err());
    }

    #[test]
    fn state_values_are_exact_and_distinguish_empty_put_from_delete() {
        assert_eq!(StateValue::Delete.encode().unwrap(), [0]);
        assert_eq!(StateValue::Put(vec![]).encode().unwrap(), [1, 0, 0, 0, 0]);
        assert_eq!(
            StateValue::Put(vec![42]).encode().unwrap(),
            [1, 1, 0, 0, 0, 42]
        );
        for value in [
            StateValue::Delete,
            StateValue::Put(vec![]),
            StateValue::Put(vec![0; MAX_VALUE_BYTES]),
        ] {
            assert_eq!(StateValue::decode(&value.encode().unwrap()).unwrap(), value);
        }
        assert!(
            StateValue::Put(vec![0; MAX_VALUE_BYTES + 1])
                .encode()
                .is_err()
        );
        for invalid in [
            &b""[..],
            &[0, 0],
            &[2],
            &[1],
            &[1, 0, 0, 0],
            &[1, 1, 0, 0, 0],
            &[1, 0, 0, 0, 0, 0],
            &[1, 0xff, 0xff, 0xff, 0xff],
        ] {
            assert!(matches!(
                StateValue::decode(invalid),
                Err(Error::InvalidInput(_))
            ));
        }
    }

    #[test]
    fn snapshots_tombstones_and_out_of_order_installation() {
        let (_directory, mut store) = create_store();
        for change in [
            mutation(1, b"a\0\0", 10, StateValue::Put(vec![10])),
            mutation(1, b"a\0\0", 7, StateValue::Put(vec![7])),
            mutation(1, b"a\0\0", 9, StateValue::Delete),
            mutation(1, b"b\0\0", 12, StateValue::Put(vec![12])),
            mutation(1, b"b\0\0", 3, StateValue::Put(vec![3])),
            mutation(1, b"c\0\0", 8, StateValue::Put(vec![])),
            mutation(2, b"a\0\0", 4, StateValue::Put(vec![99])),
        ] {
            apply(&mut store, &[change]).unwrap();
        }
        let pinned = view(&store);
        for (sequence, expected) in [
            (0, None),
            (6, None),
            (7, Some(vec![7])),
            (8, Some(vec![7])),
            (9, None),
            (10, Some(vec![10])),
        ] {
            assert_eq!(get(&pinned, 1, b"a\0\0", sequence).unwrap(), expected);
        }
        assert_eq!(get(&pinned, 1, b"c\0\0", 9).unwrap(), Some(vec![]));
        assert_eq!(get(&pinned, 1, b"missing", 9).unwrap(), None);
        let rows = scan(&pinned, 1, 9, Bound::Unbounded, Bound::Unbounded)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            rows,
            [(b"b\0\0".to_vec(), vec![3]), (b"c\0\0".to_vec(), vec![])]
        );
        assert!(
            scan(&pinned, 1, 0, Bound::Unbounded, Bound::Unbounded)
                .unwrap()
                .next()
                .is_none()
        );

        let mut old_scan = scan(&pinned, 1, 20, Bound::Unbounded, Bound::Unbounded).unwrap();
        apply(&mut store, &[mutation(1, b"a\0\0", 15, StateValue::Delete)]).unwrap();
        drop(pinned);
        assert_eq!(
            old_scan.next().unwrap().unwrap(),
            (b"a\0\0".to_vec(), vec![10])
        );
        assert_eq!(get(&view(&store), 1, b"a\0\0", 20).unwrap(), None);
    }

    #[test]
    fn all_bound_combinations_match_logical_intervals() {
        let (_directory, mut store) = create_store();
        let keys: &[&[u8]] = &[b"", b"\0", b"\0\0", b"a", b"a\0", b"aa", b"\xff"];
        for table in [1, 2] {
            for &key in keys {
                apply(
                    &mut store,
                    &[
                        mutation(table, key, 9, StateValue::Put(vec![9])),
                        mutation(table, key, 4, StateValue::Put(vec![4])),
                    ],
                )
                .unwrap();
            }
        }
        let pinned = view(&store);
        let mut bounds = vec![Bound::Unbounded];
        for &key in keys.iter().chain([b"absent".as_slice()].iter()) {
            bounds.push(Bound::Included(key));
            bounds.push(Bound::Excluded(key));
        }
        for &lower in &bounds {
            for &upper in &bounds {
                let expected: Vec<_> = keys
                    .iter()
                    .copied()
                    .filter(|key| {
                        let above = match lower {
                            Bound::Unbounded => true,
                            Bound::Included(lower) => *key >= lower,
                            Bound::Excluded(lower) => *key > lower,
                        };
                        let below = match upper {
                            Bound::Unbounded => true,
                            Bound::Included(upper) => *key <= upper,
                            Bound::Excluded(upper) => *key < upper,
                        };
                        above && below
                    })
                    .map(|key| (key.to_vec(), vec![4]))
                    .collect();
                let actual = scan(&pinned, 1, 6, lower, upper)
                    .unwrap()
                    .collect::<Result<Vec<_>>>()
                    .unwrap();
                assert_eq!(actual, expected, "lower={lower:?}, upper={upper:?}");
            }
        }
    }

    #[test]
    fn scans_validate_consumed_frames_and_fuse_without_lookahead() {
        let (_directory, mut store) = create_store();
        let mut invalid = mutation(1, b"b", 4, StateValue::Delete);
        invalid.value = Some(vec![0, 0]);
        apply(
            &mut store,
            &[mutation(1, b"a", 3, StateValue::Put(vec![3])), invalid],
        )
        .unwrap();
        let pinned = view(&store);
        let mut rows = scan(&pinned, 1, 5, Bound::Unbounded, Bound::Unbounded).unwrap();
        assert_eq!(rows.next().unwrap().unwrap(), (b"a".to_vec(), vec![3]));
        assert!(matches!(rows.next(), Some(Err(Error::Corrupt(_)))));
        assert!(rows.next().is_none());
        assert!(rows.next().is_none());
        assert!(matches!(get(&pinned, 1, b"b", 5), Err(Error::Corrupt(_))));
        assert_eq!(get(&pinned, 1, b"a", 5).unwrap(), Some(vec![3]));

        let mut invisible = scan(&pinned, 1, 0, Bound::Unbounded, Bound::Unbounded).unwrap();
        assert!(matches!(invisible.next(), Some(Err(Error::Corrupt(_)))));
        let mut invalid_old = mutation(1, b"a", 2, StateValue::Delete);
        invalid_old.value = Some(vec![2]);
        apply(&mut store, &[invalid_old]).unwrap();
        let mut obsolete = scan(
            &view(&store),
            1,
            5,
            Bound::Included(b"a"),
            Bound::Included(b"a"),
        )
        .unwrap();
        assert!(obsolete.next().unwrap().is_ok());
        assert!(matches!(obsolete.next(), Some(Err(Error::Corrupt(_)))));
    }

    #[test]
    fn read_values_own_their_bytes_without_pinning_files_or_directory_ownership() {
        let (directory, mut store) = create_store();
        super::super::enable_runtime_cache(&mut store);
        apply(
            &mut store,
            &[
                mutation(1, b"inline", 2, StateValue::Put(vec![7; 128])),
                mutation(1, b"overflow", 2, StateValue::Put(vec![9; 4096])),
                mutation(1, b"empty", 2, StateValue::Put(vec![])),
                mutation(1, b"deleted", 2, StateValue::Delete),
            ],
        )
        .unwrap();
        let pinned = view(&store);
        let inline = read_value(&pinned, 1, b"inline", 2).unwrap().unwrap();
        let overflow = read_value(&pinned, 1, b"overflow", 2).unwrap().unwrap();
        let empty = read_value(&pinned, 1, b"empty", 2).unwrap().unwrap();
        assert!(read_value(&pinned, 1, b"deleted", 2).unwrap().is_none());
        let path = store.directory().to_owned();
        drop(pinned);
        drop(store);
        // Recovery can truncate the uncheckpointed page suffix while returned
        // bytes remain readable. A hidden storage lease would prevent reopen.
        let reopened = super::super::open(path).unwrap();
        assert_eq!(inline.as_bytes(), &[7; 128]);
        assert_eq!(overflow.as_bytes(), &[9; 4096]);
        assert!(empty.as_bytes().is_empty());
        drop(reopened);
        drop(directory);
    }

    #[test]
    fn point_reads_match_snapshot_scans_for_versions_key_extensions_and_overflow() {
        let (_directory, mut store) = create_store();
        super::super::enable_runtime_cache(&mut store);
        let keys: Vec<Vec<u8>> = std::iter::once(Vec::new())
            .chain((0..80).map(|n| vec![n, 0, n]))
            .chain([vec![0], vec![0, 0], vec![0xff], vec![0; MAX_KEY_BYTES]])
            .collect();
        let mut changes = Vec::new();
        for table in [1, 2] {
            for (index, key) in keys.iter().enumerate() {
                for sequence in [9, 3, 6] {
                    let value = if sequence == 6 && index % 2 == 0 {
                        StateValue::Delete
                    } else {
                        StateValue::Put(vec![
                            sequence as u8;
                            if index % 3 == 0 { 2048 } else { index }
                        ])
                    };
                    changes.push(mutation(table, key, sequence, value));
                }
            }
        }
        apply(&mut store, &changes).unwrap();
        let pinned = view(&store);
        for table in [1, 2, 3] {
            for sequence in [0, 2, 3, 5, 6, 8, 9, u64::MAX - 1] {
                let expected: std::collections::BTreeMap<_, _> =
                    scan(&pinned, table, sequence, Bound::Unbounded, Bound::Unbounded)
                        .unwrap()
                        .collect::<Result<_>>()
                        .unwrap();
                for key in keys.iter().chain([&vec![0, 1], &vec![0xff, 0]]) {
                    assert_eq!(
                        get(&pinned, table, key, sequence).unwrap().as_ref(),
                        expected.get(key)
                    );
                }
            }
        }
    }

    #[test]
    fn point_reads_reject_malformed_matching_frames_without_loading_unrelated_values() {
        for suffix in [vec![], vec![0xff; 7], vec![0xff; 9]] {
            let (_directory, mut store) = create_store();
            let mut key = address_prefix(1, b"a");
            key.extend_from_slice(&suffix);
            apply(
                &mut store,
                &[Mutation {
                    tree: TreeId::State,
                    key,
                    value: Some(vec![0]),
                }],
            )
            .unwrap();
            // A short prefix sorts before the seek bound, so it is not a
            // candidate. Other malformed suffixes inside the interval fail.
            let result = get(&view(&store), 1, b"a", u64::MAX - 1);
            if !suffix.is_empty() {
                assert!(matches!(result, Err(Error::Corrupt(_))));
            } else {
                assert_eq!(result.unwrap(), None);
            }
        }
        let (_directory, mut store) = create_store();
        let mut invalid = mutation(1, b"a\0", 3, StateValue::Delete);
        invalid.value = Some(vec![2]);
        apply(&mut store, &[invalid]).unwrap();
        assert_eq!(get(&view(&store), 1, b"a", 5).unwrap(), None);
        assert!(matches!(
            get(&view(&store), 1, b"a\0", 5),
            Err(Error::Corrupt(_))
        ));
    }

    #[test]
    fn persisted_invalid_sequence_is_corrupt_but_invalid_requests_are_input_errors() {
        let (_directory, mut store) = create_store();
        apply(
            &mut store,
            &[Mutation {
                tree: TreeId::State,
                key: seek_key(1, b"a", 1),
                value: Some(vec![0]),
            }],
        )
        .unwrap();
        let pinned = view(&store);
        assert!(matches!(get(&pinned, 1, b"a", 2), Err(Error::Corrupt(_))));
        assert!(matches!(
            scan(&pinned, 1, 2, Bound::Unbounded, Bound::Unbounded)
                .unwrap()
                .next(),
            Some(Err(Error::Corrupt(_)))
        ));
        for (table, sequence) in [(0, 0), (u64::MAX, 0), (1, u64::MAX)] {
            assert!(matches!(
                get(&pinned, table, b"a", sequence),
                Err(Error::InvalidInput(_))
            ));
            assert!(matches!(
                scan(&pinned, table, sequence, Bound::Unbounded, Bound::Unbounded),
                Err(Error::InvalidInput(_))
            ));
        }
        let oversized = vec![0; MAX_KEY_BYTES + 1];
        assert!(matches!(
            get(&pinned, 1, &oversized, 2),
            Err(Error::InvalidInput(_))
        ));
        assert!(scan(&pinned, 1, 2, Bound::Included(&oversized), Bound::Unbounded).is_err());
        assert!(scan(&pinned, 1, 2, Bound::Unbounded, Bound::Excluded(&oversized)).is_err());
        assert!(
            scan(&pinned, u64::MAX - 1, 0, Bound::Unbounded, Bound::Unbounded)
                .unwrap()
                .next()
                .is_none()
        );
    }
}
