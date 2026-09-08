//! Storage adaptation for the single-threaded reference interpreter.
//!
//! Callers protect historical reads and supply the pre-record snapshot.
//! Installing a result changes physical roots only: it neither writes a log nor
//! publishes a durable or visible frontier. Recovery must exclude
//! post-checkpoint materialization before replay; this module does not retry
//! resolved sequences.

use std::collections::BTreeMap;
use std::ops::Bound;

use super::Abort;
use super::AbortReason;
use super::Effect;
use super::Error;
use super::Outcome;
use super::Result;
use super::Table;
use super::Type;
use super::Value;
use crate::storage;
use crate::storage::LimitPolicy;
use crate::storage::Mutation;
use crate::storage::Store;
use crate::storage::TreeId;
use crate::storage::View;
use crate::storage::encoding::Schema;
use crate::storage::mvcc;

pub(super) type Overlay = BTreeMap<(u64, Vec<u8>), Option<Vec<u8>>>;

/// A catalogue request, validated before its state-dependent outcome is
/// computed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CatalogueOperation {
    Create {
        name: String,
        key: Type,
        value: Type,
    },
    Rename {
        table: u64,
        name: String,
    },
    Drop {
        table: u64,
    },
}

/// Complete immutable-schema catalogue metadata at one effective sequence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogueVersion {
    pub live: bool,
    pub name: String,
    pub table: Table,
}

pub(super) fn input_error(error: storage::Error) -> Error {
    match error {
        storage::Error::InvalidInput(reason) => Error::Invalid(reason),
        storage::Error::Unsupported { format, version } => Error::Unsupported { format, version },
        error => Error::Storage(error),
    }
}

pub(super) fn persisted(error: Error) -> Error {
    match error {
        Error::Invalid(reason) | Error::Storage(storage::Error::InvalidInput(reason)) => {
            Error::Storage(storage::Error::Corrupt(reason))
        }
        error => error,
    }
}

pub(super) fn valid_id(id: u64) -> Result<()> {
    if id == 0 || id == u64::MAX {
        return Err(Error::Invalid("reserved table ID or record sequence"));
    }
    Ok(())
}

fn valid_name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > 255 || name.as_bytes().contains(&0) {
        return Err(Error::Invalid(
            "table name must contain 1 through 255 UTF-8 bytes without NUL",
        ));
    }
    Ok(())
}

fn take<'a>(
    input: &mut &'a [u8],
    length: usize,
) -> Result<&'a [u8]> {
    let (bytes, rest) = input
        .split_at_checked(length)
        .ok_or(Error::Invalid("truncated catalogue value"))?;
    *input = rest;
    Ok(bytes)
}

fn read_blob<'a>(input: &mut &'a [u8]) -> Result<&'a [u8]> {
    let length = u32::from_le_bytes(take(input, 4)?.try_into().unwrap()) as usize;
    take(input, length)
}

fn blob(
    output: &mut Vec<u8>,
    bytes: &[u8],
) -> Result<()> {
    let length = u32::try_from(bytes.len()).map_err(|_| Error::Invalid("blob exceeds u32"))?;
    output.extend_from_slice(&length.to_le_bytes());
    output.extend_from_slice(bytes);
    Ok(())
}

fn table_schema(
    bytes: &[u8],
    key: bool,
) -> Result<Schema> {
    let schema = Schema::decode(bytes).map_err(input_error)?;
    if key && schema.max_key_bytes() > 1_024 {
        return Err(Error::Invalid(
            "table key schema exceeds 1,024 canonical bytes",
        ));
    }
    Ok(schema)
}

fn schema_descriptor(
    ty: &Type,
    key: bool,
) -> Result<Vec<u8>> {
    // Public enum construction can bypass Type::decode and even panic in
    // descriptor(). Bound the structure before invoking that encoder.
    let mut pending = vec![(ty, 1)];
    let mut bytes = 2;
    while let Some((ty, depth)) = pending.pop() {
        if depth > 16 {
            return Err(Error::Invalid("schema nesting exceeds depth 16"));
        }
        bytes += match ty {
            Type::Unit | Type::Boolean | Type::I64 | Type::U64 => 1,
            Type::Bytes(_) | Type::String(_) => 5,
            Type::Tuple(fields) => {
                if fields.len() > 256 {
                    return Err(Error::Invalid("tuple exceeds 256 fields"));
                }
                pending.extend(fields.iter().map(|field| (field, depth + 1)));
                3
            }
            Type::Rows { .. } => return Err(Error::Invalid("Rows is not a storage schema type")),
        };
        if bytes > 65_536 {
            return Err(Error::Invalid("schema descriptor exceeds 65,536 bytes"));
        }
    }
    let descriptor = ty.descriptor();
    table_schema(&descriptor, key)?;
    Ok(descriptor)
}

/// Construct a descending-version seek key. Sequence zero is useful for seeking
/// the empty pre-creation view, but is not a valid stored catalogue version.
pub fn catalogue_key(
    id: u64,
    sequence: u64,
) -> Vec<u8> {
    [id.to_be_bytes(), (!sequence).to_be_bytes()].concat()
}

/// Decode a stored catalogue key as (table ID, effective sequence).
pub fn decode_catalogue_key(bytes: &[u8]) -> Result<(u64, u64)> {
    if bytes.len() != 16 {
        return Err(Error::Storage(storage::Error::Corrupt(
            "invalid catalogue key length",
        )));
    }
    let id = u64::from_be_bytes(bytes[..8].try_into().unwrap());
    let sequence = !u64::from_be_bytes(bytes[8..].try_into().unwrap());
    valid_id(id).map_err(persisted)?;
    valid_id(sequence).map_err(persisted)?;
    if sequence < id {
        return Err(Error::Storage(storage::Error::Corrupt(
            "catalogue version predates creation",
        )));
    }
    Ok((id, sequence))
}

/// Decode complete D.1 metadata, including immutable schema bounds. The caller
/// supplies the table ID from its enclosing tree key or outcome effect.
pub fn decode_catalogue(
    id: u64,
    bytes: &[u8],
) -> Result<CatalogueVersion> {
    valid_id(id)?;
    let mut input = bytes;
    let version = u16::from_le_bytes(take(&mut input, 2)?.try_into().unwrap());
    if version != 1 {
        return Err(Error::Unsupported {
            format: "catalogue",
            version,
        });
    }
    let live = match take(&mut input, 1)?[0] {
        1 => true,
        2 => false,
        _ => return Err(Error::Invalid("invalid catalogue state")),
    };
    if take(&mut input, 1)? != [0] {
        return Err(Error::Invalid("nonzero catalogue reserved field"));
    }
    let name = std::str::from_utf8(read_blob(&mut input)?)
        .map_err(|_| Error::Invalid("invalid UTF-8 table name"))?;
    valid_name(name)?;
    let key = read_blob(&mut input)?;
    table_schema(key, true)?;
    let key = Type::decode(key)?;
    let value = read_blob(&mut input)?;
    table_schema(value, false)?;
    let value = Type::decode(value)?;
    if !input.is_empty() {
        return Err(Error::Invalid("trailing catalogue bytes"));
    }
    Ok(CatalogueVersion {
        live,
        name: name.to_owned(),
        table: Table { id, key, value },
    })
}

/// Encode complete D.1 metadata after validating public names and type enums.
pub fn encode_catalogue(version: &CatalogueVersion) -> Result<Vec<u8>> {
    valid_id(version.table.id)?;
    valid_name(&version.name)?;
    let key = schema_descriptor(&version.table.key, true)?;
    let value = schema_descriptor(&version.table.value, false)?;
    let mut bytes = vec![1, 0, if version.live { 1 } else { 2 }, 0];
    blob(&mut bytes, version.name.as_bytes())?;
    blob(&mut bytes, &key)?;
    blob(&mut bytes, &value)?;
    Ok(bytes)
}

/// Read the last catalogue version at or before a protected snapshot sequence.
/// A dropped version is returned as metadata, not skipped in favour of a live
/// one.
pub fn catalogue_version(
    view: &View,
    id: u64,
    sequence: u64,
) -> Result<Option<CatalogueVersion>> {
    valid_id(id)?;
    if sequence == u64::MAX {
        return Err(Error::Invalid("reserved snapshot sequence"));
    }
    let lower = catalogue_key(id, sequence);
    let upper = (id + 1).to_be_bytes();
    let mut entries = storage::scan(
        view,
        TreeId::Catalogue,
        Bound::Included(&lower),
        Bound::Excluded(&upper),
    )?;
    let Some((key, value)) = entries.next().transpose()? else {
        return Ok(None);
    };
    let (found_id, found_sequence) = decode_catalogue_key(&key)?;
    if found_id != id || found_sequence > sequence {
        return Err(Error::Storage(storage::Error::Corrupt(
            "unexpected catalogue seek result",
        )));
    }
    Ok(Some(decode_catalogue(id, &value).map_err(persisted)?))
}

pub(super) fn table(
    view: &View,
    id: u64,
    sequence: u64,
) -> Result<Table> {
    catalogue_version(view, id, sequence)?
        .filter(|version| version.live)
        .map(|version| version.table)
        .ok_or(Error::Invalid("table is unknown or dropped"))
}

pub(super) fn policy(
    view: &View,
    sequence: u64,
) -> Result<LimitPolicy> {
    if sequence == u64::MAX {
        return Err(Error::Invalid("reserved snapshot sequence"));
    }
    let upper = sequence.to_be_bytes();
    let entries = storage::scan(
        view,
        TreeId::Policy,
        Bound::Unbounded,
        Bound::Included(&upper),
    )?;
    let mut latest = None;
    for entry in entries {
        let (key, value) = entry?;
        if key.len() != 8 {
            return Err(Error::Storage(storage::Error::Corrupt(
                "invalid policy key length",
            )));
        }
        if latest.is_none() && key != [0; 8] {
            return Err(Error::Storage(storage::Error::Corrupt(
                "missing genesis policy",
            )));
        }
        latest = Some(LimitPolicy::decode(&value).map_err(input_error)?);
    }
    latest.ok_or(Error::Storage(storage::Error::Corrupt(
        "missing genesis policy",
    )))
}

pub(super) fn get(
    view: &View,
    table: u64,
    key: &[u8],
    sequence: u64,
    overlay: &Overlay,
) -> Result<Option<Vec<u8>>> {
    if let Some(value) = overlay.get(&(table, key.to_vec())) {
        return Ok(value.clone());
    }
    Ok(mvcc::get(view, table, key, sequence)?)
}

pub(super) fn scan<'a>(
    view: &View,
    table: u64,
    sequence: u64,
    lower: Bound<&[u8]>,
    upper: Bound<&[u8]>,
    overlay: &'a Overlay,
) -> Result<impl Iterator<Item = Result<storage::Entry>> + 'a + use<'a>> {
    valid_id(table)?;
    if sequence == u64::MAX {
        return Err(Error::Invalid("reserved snapshot sequence"));
    }
    for bound in [lower, upper] {
        if let Bound::Included(key) | Bound::Excluded(key) = bound
            && key.len() > 1_024
        {
            return Err(Error::Invalid(
                "scan endpoint exceeds 1,024 canonical bytes",
            ));
        }
    }
    let overlay_lower = match lower {
        Bound::Unbounded => Bound::Included((table, Vec::new())),
        Bound::Included(key) => Bound::Included((table, key.to_vec())),
        Bound::Excluded(key) => Bound::Excluded((table, key.to_vec())),
    };
    let overlay_upper = match upper {
        Bound::Unbounded => Bound::Excluded((table + 1, Vec::new())),
        Bound::Included(key) => Bound::Included((table, key.to_vec())),
        Bound::Excluded(key) => Bound::Excluded((table, key.to_vec())),
    };
    let mut overlay = overlay.range((overlay_lower, overlay_upper)).peekable();
    let mut lower = lower.map(<[u8]>::to_vec);
    let upper = upper.map(<[u8]>::to_vec);
    let view = view.clone();
    let mut base: Option<mvcc::StateScan> = None;
    let mut done = false;
    Ok(std::iter::from_fn(move || {
        if done {
            return None;
        }
        loop {
            // Split at overlay addresses instead of peeking one base row ahead.
            // This keeps later values and errors outside a row-limited
            // selection.
            if base.is_none() {
                let next_key = overlay.peek().map(|((_, key), _)| key.as_slice());
                let empty = next_key.is_some_and(|key| match &lower {
                    Bound::Unbounded => key.is_empty(),
                    Bound::Included(start) | Bound::Excluded(start) => start.as_slice() >= key,
                });
                if !empty {
                    let end = next_key.map_or(upper.as_ref().map(Vec::as_slice), Bound::Excluded);
                    match mvcc::scan(
                        &view,
                        table,
                        sequence,
                        lower.as_ref().map(Vec::as_slice),
                        end,
                    ) {
                        Ok(scan) => base = Some(scan),
                        Err(error) => {
                            done = true;
                            return Some(Err(error.into()));
                        }
                    }
                }
            }
            if let Some(entry) = base.as_mut().and_then(Iterator::next) {
                if entry.is_err() {
                    done = true;
                }
                return Some(entry.map_err(Error::from));
            }
            base = None;
            let Some(((_, key), value)) = overlay.next() else {
                done = true;
                return None;
            };
            lower = Bound::Excluded(key.clone());
            if let Some(value) = value {
                return Some(Ok((key.clone(), value.clone())));
            }
        }
    }))
}

fn name_in_use(
    view: &View,
    sequence: u64,
    name: &str,
    except: Option<u64>,
) -> Result<bool> {
    let entries = storage::scan(view, TreeId::Catalogue, Bound::Unbounded, Bound::Unbounded)?;
    let mut selected = None;
    for entry in entries {
        let (key, value) = entry?;
        let (id, effective) = decode_catalogue_key(&key)?;
        if effective > sequence || selected == Some(id) {
            continue;
        }
        selected = Some(id);
        let version = decode_catalogue(id, &value).map_err(persisted)?;
        if version.live && version.name == name && except != Some(id) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn administrative_abort(reason: AbortReason) -> Outcome {
    Outcome::Aborted(Abort {
        reason,
        instruction: u32::MAX,
        user_code: 0,
        detail: 0,
    })
}

/// Validate syntax only; name conflicts and table liveness are durable
/// outcomes.
pub(crate) fn validate_catalogue(operation: &CatalogueOperation) -> Result<()> {
    match operation {
        CatalogueOperation::Create { name, key, value } => {
            valid_name(name)?;
            schema_descriptor(key, true)?;
            schema_descriptor(value, false)?;
        }
        CatalogueOperation::Rename { table, name } => {
            valid_id(*table)?;
            valid_name(name)?;
        }
        CatalogueOperation::Drop { table } => valid_id(*table)?,
    }
    Ok(())
}

/// Compute an administrative record at N against its historical state at N - 1.
pub(super) fn catalogue(
    view: &View,
    sequence: u64,
    operation: &CatalogueOperation,
) -> Result<Outcome> {
    valid_id(sequence)?;
    validate_catalogue(operation)?;
    let (version, result_type, value) = match operation {
        CatalogueOperation::Create { name, key, value } => {
            if name_in_use(view, sequence - 1, name, None)? {
                return Ok(administrative_abort(AbortReason::NameInUse));
            }
            (
                CatalogueVersion {
                    live: true,
                    name: name.clone(),
                    table: Table {
                        id: sequence,
                        key: key.clone(),
                        value: value.clone(),
                    },
                },
                Type::U64,
                Value::U64(sequence),
            )
        }
        CatalogueOperation::Rename { table, name } => {
            let Some(mut version) =
                catalogue_version(view, *table, sequence - 1)?.filter(|v| v.live)
            else {
                return Ok(administrative_abort(AbortReason::TableNotLive));
            };
            if name_in_use(view, sequence - 1, name, Some(*table))? {
                return Ok(administrative_abort(AbortReason::NameInUse));
            }
            version.name = name.clone();
            (version, Type::Unit, Value::Unit)
        }
        CatalogueOperation::Drop { table } => {
            let Some(mut version) =
                catalogue_version(view, *table, sequence - 1)?.filter(|v| v.live)
            else {
                return Ok(administrative_abort(AbortReason::TableNotLive));
            };
            version.live = false;
            (version, Type::Unit, Value::Unit)
        }
    };
    Ok(Outcome::Success {
        result_type,
        value,
        effects: vec![Effect::Catalogue {
            table: version.table.id,
            value: encode_catalogue(&version)?,
        }],
    })
}

pub(super) fn limits(policy: &LimitPolicy) -> Outcome {
    Outcome::Success {
        result_type: Type::Unit,
        value: Value::Unit,
        effects: vec![Effect::Limits {
            policy: policy.clone(),
        }],
    }
}

/// Install validated interpreter output once, with its complete D.3 outcome.
/// The caller owns sequencing and must exclude speculative materialization when
/// replaying from a checkpoint. This is not an idempotent replay API.
pub(crate) fn install(
    store: &mut Store,
    sequence: u64,
    digest: [u8; 32],
    kind: u8,
    outcome: &Outcome,
) -> Result<()> {
    valid_id(sequence)?;
    if !(1..=3).contains(&kind) {
        return Err(Error::Invalid("invalid outcome record kind"));
    }
    if sequence <= store.manifest().checkpoint_sequence {
        return Err(Error::Invalid("sequence is already checkpointed"));
    }
    if storage::get(
        &storage::view(store),
        TreeId::Outcomes,
        &sequence.to_be_bytes(),
    )?
    .is_some()
    {
        return Err(Error::Invalid("sequence is already resolved"));
    }
    let bytes = super::encode_outcome(sequence, digest, kind, outcome)?;
    let mut changes = Vec::new();
    if let Outcome::Success { effects, .. } = outcome {
        for effect in effects {
            let (tree, key, value) = match effect {
                Effect::Put { table, key, value } => (
                    TreeId::State,
                    mvcc::StateKey::new(*table, key.clone(), sequence)?.encode(),
                    mvcc::StateValue::Put(value.clone()).encode()?,
                ),
                Effect::Delete { table, key } => (
                    TreeId::State,
                    mvcc::StateKey::new(*table, key.clone(), sequence)?.encode(),
                    mvcc::StateValue::Delete.encode()?,
                ),
                Effect::Catalogue { table, value } => (
                    TreeId::Catalogue,
                    catalogue_key(*table, sequence),
                    value.clone(),
                ),
                Effect::Limits { policy } => {
                    let value = policy.encode();
                    (TreeId::Policy, sequence.to_be_bytes().to_vec(), value)
                }
            };
            changes.push(Mutation {
                tree,
                key,
                value: Some(value),
            });
        }
    }
    changes.push(Mutation {
        tree: TreeId::Outcomes,
        key: sequence.to_be_bytes().to_vec(),
        value: Some(bytes),
    });
    storage::apply(store, &changes)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_store() -> (tempfile::TempDir, Store) {
        let directory = tempfile::tempdir().unwrap();
        let store = storage::create(
            directory.path().join("database"),
            storage::Genesis {
                database_id: [1; 16],
                initial_policy: LimitPolicy::new([0; 17]).unwrap(),
            },
            [2; 16],
        )
        .unwrap();
        (directory, store)
    }

    fn create(name: &str) -> CatalogueOperation {
        CatalogueOperation::Create {
            name: name.into(),
            key: Type::Boolean,
            value: Type::Boolean,
        }
    }

    fn administer(
        store: &mut Store,
        sequence: u64,
        operation: CatalogueOperation,
    ) -> Outcome {
        let outcome = catalogue(&storage::view(store), sequence, &operation).unwrap();
        install(store, sequence, [3; 32], 2, &outcome).unwrap();
        outcome
    }

    fn abort_reason(outcome: &Outcome) -> u16 {
        let Outcome::Aborted(abort) = outcome else {
            panic!("expected administrative abort");
        };
        assert_eq!(abort.instruction, u32::MAX);
        assert_eq!(abort.user_code, 0);
        assert_eq!(abort.detail, 0);
        match abort.reason {
            AbortReason::NameInUse => 16,
            AbortReason::TableNotLive => 17,
            _ => panic!("expected administrative reason"),
        }
    }

    fn success(effects: Vec<Effect>) -> Outcome {
        Outcome::Success {
            result_type: Type::Unit,
            value: Value::Unit,
            effects,
        }
    }

    fn raw(
        store: &mut Store,
        tree: TreeId,
        key: Vec<u8>,
        value: Vec<u8>,
    ) {
        storage::apply(
            store,
            &[Mutation {
                tree,
                key,
                value: Some(value),
            }],
        )
        .unwrap();
    }

    fn state(
        store: &mut Store,
        table: u64,
        key: &[u8],
        sequence: u64,
        value: mvcc::StateValue,
    ) {
        raw(
            store,
            TreeId::State,
            mvcc::StateKey::new(table, key.to_vec(), sequence)
                .unwrap()
                .encode(),
            value.encode().unwrap(),
        );
    }

    #[test]
    fn historical_catalogue_names_liveness_and_identity() {
        let (_directory, mut store) = create_store();
        let created = administer(&mut store, 1, create("first"));
        assert!(matches!(
            created,
            Outcome::Success {
                result_type: Type::U64,
                value: Value::U64(1),
                ..
            }
        ));
        administer(&mut store, 2, create("second"));
        let duplicate = administer(&mut store, 3, create("first"));
        assert_eq!(abort_reason(&duplicate), AbortReason::NameInUse as u16);
        let missing = administer(
            &mut store,
            4,
            CatalogueOperation::Rename {
                table: 3,
                name: "first".into(),
            },
        );
        assert_eq!(abort_reason(&missing), AbortReason::TableNotLive as u16);
        let same = administer(
            &mut store,
            5,
            CatalogueOperation::Rename {
                table: 1,
                name: "first".into(),
            },
        );
        assert!(matches!(
            same,
            Outcome::Success {
                result_type: Type::Unit,
                value: Value::Unit,
                ..
            }
        ));
        let conflict = administer(
            &mut store,
            6,
            CatalogueOperation::Rename {
                table: 1,
                name: "second".into(),
            },
        );
        assert_eq!(abort_reason(&conflict), AbortReason::NameInUse as u16);
        administer(
            &mut store,
            7,
            CatalogueOperation::Rename {
                table: 1,
                name: "renamed".into(),
            },
        );
        administer(&mut store, 8, CatalogueOperation::Drop { table: 1 });
        administer(&mut store, 9, create("renamed"));
        let dropped = administer(&mut store, 10, CatalogueOperation::Drop { table: 1 });
        assert_eq!(abort_reason(&dropped), AbortReason::TableNotLive as u16);

        let view = storage::view(&store);
        assert!(matches!(table(&view, 1, 0), Err(Error::Invalid(_))));
        for sequence in [1, 5, 7] {
            assert_eq!(
                table(&view, 1, sequence).unwrap(),
                Table {
                    id: 1,
                    key: Type::Boolean,
                    value: Type::Boolean
                }
            );
        }
        for sequence in [8, 9, 10] {
            assert!(matches!(table(&view, 1, sequence), Err(Error::Invalid(_))));
        }
        assert!(table(&view, 9, 8).is_err());
        assert_eq!(table(&view, 9, 9).unwrap().id, 9);
        for (sequence, name, live) in [
            (1, "first", true),
            (7, "renamed", true),
            (8, "renamed", false),
        ] {
            let version = catalogue_version(&view, 1, sequence).unwrap().unwrap();
            assert_eq!(version.name, name);
            assert_eq!(version.live, live);
        }
        assert!(name_in_use(&view, 6, "first", None).unwrap());
        assert!(!name_in_use(&view, 7, "first", None).unwrap());
        assert!(!name_in_use(&view, 8, "renamed", None).unwrap());
        assert!(name_in_use(&view, 9, "renamed", None).unwrap());
        let historical = catalogue(&view, 7, &create("first")).unwrap();
        assert_eq!(abort_reason(&historical), AbortReason::NameInUse as u16);
    }

    #[test]
    fn catalogue_validates_public_types_and_names_before_state_checks() {
        let (_directory, mut store) = create_store();
        administer(&mut store, 1, create("occupied"));
        let view = storage::view(&store);
        for name in [
            String::new(),
            "a\0b".into(),
            "a".repeat(256),
            "\u{e9}".repeat(128),
        ] {
            assert!(matches!(
                catalogue(&view, 2, &create(&name)),
                Err(Error::Invalid(_))
            ));
            assert!(matches!(
                catalogue(&view, 2, &CatalogueOperation::Rename { table: 999, name }),
                Err(Error::Invalid(_))
            ));
        }
        let rows = Type::Rows {
            max_rows: 0,
            key: Box::new(Type::Unit),
            value: Box::new(Type::Unit),
        };
        let mut deep = Type::Unit;
        for _ in 0..16 {
            deep = Type::Tuple(vec![deep]);
        }
        for ty in [
            rows.clone(),
            Type::Tuple(vec![rows]),
            Type::Tuple(vec![Type::Unit; 65_536]),
            deep,
            Type::Bytes(u32::MAX),
        ] {
            for key in [true, false] {
                let operation = CatalogueOperation::Create {
                    name: "occupied".into(),
                    key: if key { ty.clone() } else { Type::Unit },
                    value: if key { Type::Unit } else { ty.clone() },
                };
                assert!(matches!(
                    catalogue(&view, 2, &operation),
                    Err(Error::Invalid(_))
                ));
            }
        }
        let oversized_key = CatalogueOperation::Create {
            name: "key".into(),
            key: Type::Bytes(512),
            value: Type::Unit,
        };
        assert!(matches!(
            catalogue(&view, 2, &oversized_key),
            Err(Error::Invalid(_))
        ));
        let valid = CatalogueOperation::Create {
            name: "\u{e9}".repeat(127) + "a",
            key: Type::Bytes(511),
            value: Type::Bytes(16 * 1024 * 1024 - 4),
        };
        assert!(matches!(
            catalogue(&view, 2, &valid),
            Ok(Outcome::Success { .. })
        ));
        for name in ["Occupied", "e\u{301}", "\u{e9}"] {
            assert!(matches!(
                catalogue(&view, 2, &create(name)),
                Ok(Outcome::Success { .. })
            ));
        }
        for sequence in [0, u64::MAX] {
            assert!(matches!(
                catalogue(&view, sequence, &create("valid")),
                Err(Error::Invalid(_))
            ));
        }
    }

    #[test]
    fn persisted_catalogue_is_corrupt_or_unsupported_not_a_semantic_abort() {
        let (_directory, mut store) = create_store();
        let version = CatalogueVersion {
            live: true,
            name: "a".into(),
            table: Table {
                id: 1,
                key: Type::Boolean,
                value: Type::Unit,
            },
        };
        let valid = encode_catalogue(&version).unwrap();
        for length in 0..valid.len() {
            raw(
                &mut store,
                TreeId::Catalogue,
                catalogue_key(1, 1),
                valid[..length].to_vec(),
            );
            assert!(matches!(
                table(&storage::view(&store), 1, 1),
                Err(Error::Storage(storage::Error::Corrupt(_)))
            ));
        }
        for offset in [2, 3, 4, 8, 15] {
            let mut bytes = valid.clone();
            bytes[offset] = 0xff;
            raw(&mut store, TreeId::Catalogue, catalogue_key(1, 1), bytes);
            assert!(
                matches!(
                    table(&storage::view(&store), 1, 1),
                    Err(Error::Storage(storage::Error::Corrupt(_)))
                ),
                "offset {offset}"
            );
        }
        let mut trailing = valid.clone();
        trailing.push(0);
        raw(&mut store, TreeId::Catalogue, catalogue_key(1, 1), trailing);
        assert!(matches!(
            table(&storage::view(&store), 1, 1),
            Err(Error::Storage(storage::Error::Corrupt(_)))
        ));
        for offset in [0, 13, 20] {
            let mut bytes = valid.clone();
            bytes[offset] = 2;
            raw(&mut store, TreeId::Catalogue, catalogue_key(1, 1), bytes);
            assert!(
                matches!(
                    table(&storage::view(&store), 1, 1),
                    Err(Error::Unsupported { version: 2, .. })
                ),
                "offset {offset}"
            );
            assert!(matches!(
                catalogue(
                    &storage::view(&store),
                    2,
                    &CatalogueOperation::Drop { table: 1 }
                ),
                Err(Error::Unsupported { version: 2, .. })
            ));
        }
        raw(&mut store, TreeId::Catalogue, catalogue_key(1, 1), valid);
        raw(
            &mut store,
            TreeId::Catalogue,
            catalogue_key(1, 3),
            vec![0xff],
        );
        assert!(table(&storage::view(&store), 1, 2).is_ok());
        assert!(matches!(
            table(&storage::view(&store), 1, 3),
            Err(Error::Storage(storage::Error::Corrupt(_)))
        ));
    }

    #[test]
    fn policy_uses_historical_versions_including_zero_limits() {
        let (_directory, mut store) = create_store();
        let first = LimitPolicy::new([1; 17]).unwrap();
        let second = LimitPolicy::new([2; 17]).unwrap();
        install(&mut store, 2, [2; 32], 3, &limits(&first)).unwrap();
        install(&mut store, 5, [5; 32], 3, &limits(&second)).unwrap();
        let view = storage::view(&store);
        for (sequence, expected) in [
            (0, [0; 17]),
            (1, [0; 17]),
            (2, [1; 17]),
            (4, [1; 17]),
            (5, [2; 17]),
            (9, [2; 17]),
        ] {
            assert_eq!(*policy(&view, sequence).unwrap().values(), expected);
        }
        let mut unsupported = second.encode();
        unsupported[0] = 2;
        raw(
            &mut store,
            TreeId::Policy,
            6_u64.to_be_bytes().to_vec(),
            unsupported,
        );
        assert!(policy(&storage::view(&store), 5).is_ok());
        assert!(matches!(
            policy(&storage::view(&store), 6),
            Err(Error::Unsupported { version: 2, .. })
        ));
        raw(
            &mut store,
            TreeId::Policy,
            6_u64.to_be_bytes().to_vec(),
            vec![1],
        );
        assert!(matches!(
            policy(&storage::view(&store), 6),
            Err(Error::Storage(storage::Error::Corrupt(_)))
        ));
        storage::apply(
            &mut store,
            &[Mutation {
                tree: TreeId::Policy,
                key: 0_u64.to_be_bytes().to_vec(),
                value: None,
            }],
        )
        .unwrap();
        assert!(matches!(
            policy(&storage::view(&store), 5),
            Err(Error::Storage(storage::Error::Corrupt(_)))
        ));
    }

    #[test]
    fn point_and_lazy_range_reads_merge_final_overlay_and_mvcc() {
        let (_directory, mut store) = create_store();
        for (key, sequence, value) in [
            (b"a".as_slice(), 2, mvcc::StateValue::Put(vec![1])),
            (b"a", 4, mvcc::StateValue::Delete),
            (b"b", 2, mvcc::StateValue::Put(vec![2])),
            (b"b", 8, mvcc::StateValue::Put(vec![8])),
            (b"c", 2, mvcc::StateValue::Put(vec![3])),
            (b"d", 2, mvcc::StateValue::Put(vec![4])),
            (b"e", 2, mvcc::StateValue::Put(vec![5])),
        ] {
            state(&mut store, 1, key, sequence, value);
        }
        state(&mut store, 2, b"a", 3, mvcc::StateValue::Put(vec![99]));
        let overlay = Overlay::from([
            ((1, vec![]), Some(vec![0])),
            ((1, b"b".to_vec()), Some(vec![20])),
            ((1, b"c".to_vec()), None),
            ((1, b"cc".to_vec()), Some(vec![30])),
            ((1, b"z".to_vec()), None),
            ((2, b"b".to_vec()), Some(vec![99])),
        ]);
        let view = storage::view(&store);
        assert_eq!(get(&view, 1, b"a", 3, &overlay).unwrap(), Some(vec![1]));
        assert_eq!(get(&view, 1, b"a", 5, &overlay).unwrap(), None);
        assert_eq!(get(&view, 1, b"b", 5, &overlay).unwrap(), Some(vec![20]));
        assert_eq!(get(&view, 1, b"c", 5, &overlay).unwrap(), None);
        assert_eq!(get(&view, 1, b"d", 5, &overlay).unwrap(), Some(vec![4]));
        let all: Vec<_> = scan(&view, 1, 5, Bound::Unbounded, Bound::Unbounded, &overlay)
            .unwrap()
            .collect::<Result<_>>()
            .unwrap();
        assert_eq!(
            all,
            vec![
                (vec![], vec![0]),
                (b"b".to_vec(), vec![20]),
                (b"cc".to_vec(), vec![30]),
                (b"d".to_vec(), vec![4]),
                (b"e".to_vec(), vec![5])
            ]
        );
        for (lower, upper, expected) in [
            (
                Bound::Included(b"b".as_slice()),
                Bound::Included(b"cc".as_slice()),
                vec![(b"b".to_vec(), vec![20]), (b"cc".to_vec(), vec![30])],
            ),
            (
                Bound::Excluded(b"b".as_slice()),
                Bound::Excluded(b"d".as_slice()),
                vec![(b"cc".to_vec(), vec![30])],
            ),
            (
                Bound::Included(b"b".as_slice()),
                Bound::Included(b"b".as_slice()),
                vec![(b"b".to_vec(), vec![20])],
            ),
        ] {
            assert_eq!(
                scan(&view, 1, 5, lower, upper, &overlay)
                    .unwrap()
                    .collect::<Result<Vec<_>>>()
                    .unwrap(),
                expected
            );
        }
        let no_overlay = Overlay::new();
        assert_eq!(
            scan(
                &view,
                1,
                3,
                Bound::Included(b"b"),
                Bound::Included(b"b"),
                &no_overlay
            )
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap(),
            vec![(b"b".to_vec(), vec![2])]
        );
    }

    #[test]
    fn scans_do_not_decode_past_selected_rows_and_errors_remain_system_errors() {
        let (_directory, mut store) = create_store();
        state(&mut store, 1, b"b", 2, mvcc::StateValue::Put(vec![2]));
        raw(
            &mut store,
            TreeId::State,
            mvcc::StateKey::new(1, b"z".to_vec(), 2).unwrap().encode(),
            vec![0xff],
        );
        let view = storage::view(&store);
        let overlay = Overlay::from([
            ((1, b"a".to_vec()), Some(vec![1])),
            ((1, b"c".to_vec()), Some(vec![3])),
        ]);
        let mut rows = scan(&view, 1, 2, Bound::Unbounded, Bound::Unbounded, &overlay).unwrap();
        assert_eq!(rows.next().unwrap().unwrap(), (b"a".to_vec(), vec![1]));
        assert_eq!(rows.next().unwrap().unwrap(), (b"b".to_vec(), vec![2]));
        assert_eq!(rows.next().unwrap().unwrap(), (b"c".to_vec(), vec![3]));
        assert!(matches!(
            rows.next(),
            Some(Err(Error::Storage(storage::Error::Corrupt(_))))
        ));
        assert!(rows.next().is_none());
        assert!(rows.next().is_none());
        let selected = scan(&view, 1, 2, Bound::Unbounded, Bound::Unbounded, &overlay)
            .unwrap()
            .take(3)
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(selected.len(), 3);
        let overlay = Overlay::from([((1, b"z".to_vec()), None)]);
        assert_eq!(get(&view, 1, b"z", 2, &overlay).unwrap(), None);
        assert_eq!(
            scan(&view, 1, 2, Bound::Unbounded, Bound::Unbounded, &overlay)
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .unwrap(),
            vec![(b"b".to_vec(), vec![2])]
        );
        assert!(matches!(
            get(&view, 1, b"z", 2, &Overlay::new()),
            Err(Error::Storage(storage::Error::Corrupt(_)))
        ));
    }

    #[test]
    fn exact_success_vector_and_final_state_install_together() {
        let (_directory, mut store) = create_store();
        administer(&mut store, 1, create("flags"));
        let before = storage::view(&store);
        let outcome = Outcome::Success {
            result_type: Type::U64,
            value: Value::U64(9),
            effects: vec![
                Effect::Put {
                    table: 1,
                    key: vec![1],
                    value: vec![0],
                },
                Effect::Delete {
                    table: 1,
                    key: vec![0],
                },
            ],
        };
        install(&mut store, 2, [0x55; 32], 1, &outcome).unwrap();
        let mut expected = vec![1, 0, 1, 0, 2, 0, 0, 0, 0, 0, 0, 0];
        expected.extend([0x55; 32]);
        expected.extend([
            0, 0, 0, 0, 255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 19, 0,
            0, 0, 3, 0, 0, 0, 1, 0, 3, 8, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 2, 1, 0, 0,
            0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0,
        ]);
        let after = storage::view(&store);
        assert_eq!(
            storage::get(&after, TreeId::Outcomes, &2_u64.to_be_bytes()).unwrap(),
            Some(expected)
        );
        for key in [0, 1] {
            let key = mvcc::StateKey::new(1, vec![key], 2).unwrap().encode();
            assert!(
                storage::get(&before, TreeId::State, &key)
                    .unwrap()
                    .is_none()
            );
            assert!(storage::get(&after, TreeId::State, &key).unwrap().is_some());
        }
        assert!(
            storage::get(&before, TreeId::Outcomes, &2_u64.to_be_bytes())
                .unwrap()
                .is_none()
        );
        assert_eq!(get(&after, 1, &[0], 2, &Overlay::new()).unwrap(), None);
        assert_eq!(
            get(&after, 1, &[1], 2, &Overlay::new()).unwrap(),
            Some(vec![0])
        );
        assert_eq!(store.manifest().checkpoint_sequence, 0);
        assert_eq!(store.manifest().durable_sequence, 0);
        assert!(
            storage::get(
                &storage::checkpoint_view(&store),
                TreeId::Outcomes,
                &2_u64.to_be_bytes()
            )
            .unwrap()
            .is_none()
        );
    }

    #[test]
    fn exact_abort_vector_has_no_return_or_effects() {
        let (_directory, mut store) = create_store();
        let abort = Outcome::Aborted(Abort {
            reason: AbortReason::RequireFailed,
            instruction: 7,
            user_code: 0x01020304,
            detail: 0,
        });
        install(&mut store, 1, [0x66; 32], 1, &abort).unwrap();
        let mut expected = vec![1, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0];
        expected.extend([0x66; 32]);
        expected.extend([
            3, 0, 0, 0, 7, 0, 0, 0, 4, 3, 2, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ]);
        let view = storage::view(&store);
        assert_eq!(
            storage::get(&view, TreeId::Outcomes, &1_u64.to_be_bytes()).unwrap(),
            Some(expected)
        );
        assert!(
            storage::scan(&view, TreeId::State, Bound::Unbounded, Bound::Unbounded)
                .unwrap()
                .next()
                .is_none()
        );
        assert!(
            storage::scan(&view, TreeId::Catalogue, Bound::Unbounded, Bound::Unbounded)
                .unwrap()
                .next()
                .is_none()
        );
    }

    #[test]
    fn rejected_installations_leave_all_roots_unchanged_and_cannot_retry() {
        let (_directory, mut store) = create_store();
        let good = Effect::Put {
            table: 1,
            key: vec![0],
            value: vec![1],
        };
        let invalid = success(vec![
            good,
            Effect::Put {
                table: 3,
                key: vec![],
                value: vec![],
            },
        ]);
        assert!(install(&mut store, 3, [3; 32], 1, &invalid).is_err());
        for (kind, outcome) in [
            (2, success(vec![])),
            (
                3,
                success(vec![Effect::Delete {
                    table: 1,
                    key: vec![0],
                }]),
            ),
            (
                1,
                success(vec![
                    Effect::Put {
                        table: 1,
                        key: vec![0],
                        value: vec![1],
                    },
                    Effect::Delete {
                        table: 1,
                        key: vec![0],
                    },
                ]),
            ),
            (1, administrative_abort(AbortReason::NameInUse)),
            (3, administrative_abort(AbortReason::TableNotLive)),
        ] {
            assert!(install(&mut store, 3, [3; 32], kind, &outcome).is_err());
        }
        for tree in [TreeId::State, TreeId::Catalogue, TreeId::Outcomes] {
            assert!(
                storage::scan(
                    &storage::view(&store),
                    tree,
                    Bound::Unbounded,
                    Bound::Unbounded
                )
                .unwrap()
                .next()
                .is_none()
            );
        }
        for sequence in [0, u64::MAX] {
            assert!(install(&mut store, sequence, [0; 32], 1, &success(vec![])).is_err());
        }
        for kind in [0, 4, 255] {
            assert!(install(&mut store, 3, [0; 32], kind, &success(vec![])).is_err());
        }
        install(&mut store, 3, [3; 32], 1, &success(vec![])).unwrap();
        let before = storage::get(
            &storage::view(&store),
            TreeId::Outcomes,
            &3_u64.to_be_bytes(),
        )
        .unwrap();
        assert!(install(&mut store, 3, [3; 32], 1, &success(vec![])).is_err());
        assert!(install(&mut store, 3, [4; 32], 1, &success(vec![])).is_err());
        assert_eq!(
            storage::get(
                &storage::view(&store),
                TreeId::Outcomes,
                &3_u64.to_be_bytes()
            )
            .unwrap(),
            before
        );
    }
}
