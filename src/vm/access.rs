//! Normalize and encode access declarations under design section C.4.
//!
//! These declarations describe possible accesses without executing the program
//! or reading stored rows.

use std::collections::BTreeMap;

use super::Error;
use super::Result;
use super::Table;
use crate::storage::LimitPolicy;
use crate::storage::encoding;
use crate::storage::encoding::Schema;

/// A set of addresses: one canonical key or every possible key in a table.
///
/// Key bytes use the table's ordered key encoding rather than its value
/// encoding. An empty encoded key still identifies a point, not the whole
/// table.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Scope {
    Table(u64),
    Key(u64, Vec<u8>),
}

impl Scope {
    pub fn table(&self) -> u64 {
        match self {
            Self::Table(table) | Self::Key(table, _) => *table,
        }
    }

    fn order_key(&self) -> (u64, u8, &[u8]) {
        match self {
            Self::Table(table) => (*table, 0, &[]),
            Self::Key(table, key) => (*table, 1, key),
        }
    }
}

/// Read and write modes in the C.4 encoding.
///
/// Unconditional STORE and DELETE need Write only. INSERT checks presence and
/// therefore needs both Read and Write.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum AccessMode {
    Read = 1,
    Write = 2,
    ReadWrite = 3,
}

fn mode(bits: u8) -> Result<AccessMode> {
    match bits {
        1 => Ok(AccessMode::Read),
        2 => Ok(AccessMode::Write),
        3 => Ok(AccessMode::ReadWrite),
        _ => Err(Error::Invalid("invalid access mode")),
    }
}

/// Sorted unique access declarations, normalized separately for reads and
/// writes.
///
/// A table scope suppresses point scopes of the same mode. Construction checks
/// structure. Preparation also checks historical schemas, program table
/// membership, access coverage, and resource 7 usage.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AccessManifest {
    entries: Vec<(Scope, AccessMode)>,
}

impl AccessManifest {
    /// Normalize declarations. Table reads do not broaden point writes, and
    /// table writes do not broaden point reads. Counts are checked at admission
    /// or encoding, after normalization, rather than on the input declarations.
    pub fn new(entries: impl IntoIterator<Item = (Scope, AccessMode)>) -> Result<Self> {
        let mut combined = BTreeMap::new();
        for (scope, mode) in entries {
            let (table, kind, key) = scope.order_key();
            if table == 0 || table == u64::MAX || key.len() > 1024 {
                return Err(Error::Invalid("invalid access scope table or key length"));
            }
            *combined.entry((table, kind, key.to_vec())).or_insert(0_u8) |= mode as u8;
        }
        let mut entries = Vec::new();
        let mut table_modes = (0, 0);
        for ((table, kind, key), mut bits) in combined {
            if table_modes.0 != table {
                table_modes = (table, 0);
            }
            let scope = if kind == 0 {
                table_modes.1 = bits;
                Scope::Table(table)
            } else {
                bits &= !table_modes.1;
                Scope::Key(table, key)
            };
            if bits != 0 {
                entries.push((scope, mode(bits)?));
            }
        }
        Ok(Self { entries })
    }

    pub fn entries(&self) -> &[(Scope, AccessMode)] {
        &self.entries
    }

    /// The normalized entry count charged to resource 7. A ReadWrite entry
    /// counts once, not once per mode.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn reads(&self) -> impl Iterator<Item = &Scope> {
        self.entries
            .iter()
            .filter_map(|(scope, mode)| ((*mode as u8) & 1 != 0).then_some(scope))
    }

    pub fn writes(&self) -> impl Iterator<Item = &Scope> {
        self.entries
            .iter()
            .filter_map(|(scope, mode)| ((*mode as u8) & 2 != 0).then_some(scope))
    }

    /// Encode the exact normalized declarations, never a newly derived subset.
    pub fn encode(&self) -> Result<Vec<u8>> {
        self.check_count(16_384)?;
        let mut bytes = (self.len() as u32).to_le_bytes().to_vec();
        for (scope, mode) in &self.entries {
            let (table, kind, key) = scope.order_key();
            bytes.extend_from_slice(&table.to_le_bytes());
            bytes.extend_from_slice(&[kind, *mode as u8, 0, 0]);
            bytes.extend_from_slice(&(key.len() as u32).to_le_bytes());
            bytes.extend_from_slice(key);
        }
        Ok(bytes)
    }

    /// Decode canonical C.4 bytes without repairing invalid declarations.
    ///
    /// Reject duplicates, unsorted entries, and points suppressed by table
    /// scopes. `prepare_transaction` later checks keys against historical
    /// schemas.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut input = bytes;
        let count = u32::from_le_bytes(take(&mut input, 4)?.try_into().unwrap()) as usize;
        if count > 16_384 || count > input.len() / 16 {
            return Err(Error::Invalid("invalid manifest entry count"));
        }
        let mut entries = Vec::with_capacity(count);
        let mut table_modes = (0, 0);
        for _ in 0..count {
            let table = u64::from_le_bytes(take(&mut input, 8)?.try_into().unwrap());
            let fields = take(&mut input, 4)?;
            let mode = mode(fields[1])?;
            if table == 0 || table == u64::MAX || fields[2..] != [0; 2] {
                return Err(Error::Invalid("invalid access scope fields"));
            }
            let length = u32::from_le_bytes(take(&mut input, 4)?.try_into().unwrap()) as usize;
            if length > 1024 {
                return Err(Error::Invalid("manifest key exceeds 1024 bytes"));
            }
            let key = take(&mut input, length)?;
            let scope = match fields[0] {
                0 if key.is_empty() => Scope::Table(table),
                1 => Scope::Key(table, key.to_vec()),
                _ => return Err(Error::Invalid("invalid access scope kind or table key")),
            };
            if entries
                .last()
                .is_some_and(|(previous, _): &(Scope, AccessMode)| {
                    previous.order_key() >= scope.order_key()
                })
            {
                return Err(Error::Invalid("manifest scopes are not strictly ordered"));
            }
            if table_modes.0 != table {
                table_modes = (table, 0);
            }
            match &scope {
                Scope::Table(_) => table_modes.1 = mode as u8,
                Scope::Key(_, _) if table_modes.1 & mode as u8 != 0 => {
                    return Err(Error::Invalid("manifest point mode is suppressed by table"));
                }
                Scope::Key(_, _) => {}
            }
            entries.push((scope, mode));
        }
        if !input.is_empty() {
            return Err(Error::Invalid("trailing access manifest bytes"));
        }
        Ok(Self { entries })
    }

    pub(crate) fn check_count(
        &self,
        limit: u64,
    ) -> Result<()> {
        if self.len() as u64 > limit.min(16_384) {
            return Err(Error::Invalid("manifest_scopes exceeds claim"));
        }
        Ok(())
    }
}

/// Return whether two address sets intersect.
///
/// The caller supplies mode and sequence rules. Design section 8 requires
/// dependencies from earlier writes to later reads only.
pub fn overlap(
    left: &Scope,
    right: &Scope,
) -> bool {
    if left.table() != right.table() {
        return false;
    }
    match (left, right) {
        (Scope::Key(_, a), Scope::Key(_, b)) => a == b,
        _ => true,
    }
}

fn take<'a>(
    input: &mut &'a [u8],
    length: usize,
) -> Result<&'a [u8]> {
    let (bytes, rest) = input
        .split_at_checked(length)
        .ok_or(Error::Invalid("truncated access manifest"))?;
    *input = rest;
    Ok(bytes)
}

pub(super) fn validate(
    manifest: &AccessManifest,
    required: &AccessManifest,
    tables: &[Table],
    claims: &LimitPolicy,
) -> Result<()> {
    manifest.check_count(claims.values()[6])?;
    let mut declarations = BTreeMap::new();
    let mut schemas = BTreeMap::new();
    for (scope, mode) in &manifest.entries {
        let index = tables
            .binary_search_by_key(&scope.table(), |table| table.id)
            .map_err(|_| Error::Invalid("manifest table is not in program table array"))?;
        if let Scope::Key(table, key) = scope {
            if let std::collections::btree_map::Entry::Vacant(entry) = schemas.entry(*table) {
                entry.insert(Schema::decode(&tables[index].key.descriptor())?);
            }
            encoding::decode_key(&schemas[table], key).map_err(|_| {
                Error::Invalid("manifest key is not canonical under historical schema")
            })?;
        }
        declarations.insert(scope.order_key(), *mode as u8);
    }
    for (scope, required_mode) in &required.entries {
        let table_mode = declarations
            .get(&(scope.table(), 0, &[][..]))
            .copied()
            .unwrap_or(0);
        let point_mode = declarations.get(&scope.order_key()).copied().unwrap_or(0);
        if (table_mode | point_mode) & *required_mode as u8 != *required_mode as u8 {
            return Err(Error::Invalid("manifest does not cover program accesses"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::AccessMode::Read;
    use super::AccessMode::ReadWrite;
    use super::AccessMode::Write;
    use super::Scope::Key;
    use super::Scope::Table as WholeTable;
    use super::*;
    use crate::vm::Type;

    fn wire(entries: &[(u64, u8, u8, &[u8])]) -> Vec<u8> {
        let mut bytes = (entries.len() as u32).to_le_bytes().to_vec();
        for (table, kind, mode, key) in entries {
            bytes.extend_from_slice(&table.to_le_bytes());
            bytes.extend_from_slice(&[*kind, *mode, 0, 0]);
            bytes.extend_from_slice(&(key.len() as u32).to_le_bytes());
            bytes.extend_from_slice(key);
        }
        bytes
    }

    #[test]
    fn canonical_modes_and_zero_width_points_round_trip_exactly() {
        for mode in [Read, Write, ReadWrite] {
            let manifest =
                AccessManifest::new([(WholeTable(2), mode), (Key(1, vec![]), mode)]).unwrap();
            let bytes = wire(&[(1, 1, mode as u8, &[]), (2, 0, mode as u8, &[])]);
            assert_eq!(manifest.encode().unwrap(), bytes);
            assert_eq!(AccessManifest::decode(&bytes).unwrap(), manifest);
            assert_eq!(manifest.len(), 2);
            assert_eq!(manifest.reads().count(), if mode == Write { 0 } else { 2 });
            assert_eq!(manifest.writes().count(), if mode == Read { 0 } else { 2 });
            for length in 0..bytes.len() {
                assert!(AccessManifest::decode(&bytes[..length]).is_err());
            }
        }
        assert_eq!(AccessManifest::default().encode().unwrap(), [0; 4]);
    }

    #[test]
    fn normalization_merges_alias_modes_and_suppresses_only_the_matching_mode() {
        for table_mode in [Read, Write, ReadWrite] {
            for point_mode in [Read, Write, ReadWrite] {
                let manifest = AccessManifest::new([
                    (Key(256, vec![1]), point_mode),
                    (WholeTable(256), table_mode),
                    (Key(1, vec![2]), Write),
                    (Key(1, vec![1]), Read),
                    (Key(1, vec![2]), Read),
                    (Key(1, vec![2]), Write),
                ])
                .unwrap();
                let mut expected = vec![
                    (Key(1, vec![1]), Read),
                    (Key(1, vec![2]), ReadWrite),
                    (WholeTable(256), table_mode),
                ];
                let remaining = point_mode as u8 & !(table_mode as u8);
                if remaining != 0 {
                    expected.push((Key(256, vec![1]), mode(remaining).unwrap()));
                }
                assert_eq!(manifest.entries(), expected);
                assert_eq!(
                    AccessManifest::new(manifest.entries.clone()).unwrap(),
                    manifest
                );
                assert_eq!(
                    AccessManifest::decode(&manifest.encode().unwrap()).unwrap(),
                    manifest
                );
            }
        }
        let manifest = AccessManifest::new([
            (WholeTable(1), Read),
            (Key(1, vec![]), ReadWrite),
            (WholeTable(1), Write),
        ])
        .unwrap();
        assert_eq!(manifest.entries(), [(WholeTable(1), ReadWrite)]);
    }

    #[test]
    fn wire_decoder_rejects_noncanonical_fields_order_duplicates_and_suppression() {
        for entries in [
            vec![(0, 0, 1, &[][..])],
            vec![(u64::MAX, 0, 1, &[])],
            vec![(1, 2, 1, &[])],
            vec![(1, 0, 0, &[])],
            vec![(1, 0, 4, &[])],
            vec![(1, 0, 1, &[0])],
            vec![(2, 0, 1, &[]), (1, 0, 1, &[])],
            vec![(1, 1, 1, &[]), (1, 0, 2, &[])],
            vec![(1, 1, 1, &[2]), (1, 1, 1, &[1])],
            vec![(1, 1, 1, &[]), (1, 1, 2, &[])],
            vec![(1, 0, 1, &[]), (1, 0, 2, &[])],
            vec![(1, 0, 1, &[]), (1, 1, 3, &[])],
            vec![(1, 0, 2, &[]), (1, 1, 3, &[])],
            vec![(1, 0, 3, &[]), (1, 1, 1, &[])],
        ] {
            assert!(
                AccessManifest::decode(&wire(&entries)).is_err(),
                "{entries:?}"
            );
        }
        let original = wire(&[(1, 1, 1, &[])]);
        for offset in [14, 15] {
            let mut bytes = original.clone();
            bytes[offset] = 1;
            assert!(AccessManifest::decode(&bytes).is_err());
        }
        let mut bytes = original.clone();
        bytes.push(0);
        assert!(AccessManifest::decode(&bytes).is_err());
        for offset in [0, 16] {
            let mut bytes = original.clone();
            bytes[offset..offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());
            assert!(AccessManifest::decode(&bytes).is_err());
        }
        assert!(AccessManifest::decode(&wire(&[(1, 1, 1, &[0; 1025])])).is_err());
        for scope in [WholeTable(0), Key(u64::MAX, vec![]), Key(1, vec![0; 1025])] {
            assert!(AccessManifest::new([(scope, Read)]).is_err());
        }
    }

    #[test]
    fn overlap_is_address_based_symmetric_and_includes_absent_keys() {
        let scopes = [
            WholeTable(1),
            Key(1, vec![]),
            Key(1, vec![1]),
            WholeTable(2),
            Key(2, vec![]),
        ];
        let expected = [
            [true, true, true, false, false],
            [true, true, false, false, false],
            [true, false, true, false, false],
            [false, false, false, true, true],
            [false, false, false, true, true],
        ];
        for (a, left) in scopes.iter().enumerate() {
            for (b, right) in scopes.iter().enumerate() {
                assert_eq!(overlap(left, right), expected[a][b]);
                assert_eq!(overlap(left, right), overlap(right, left));
            }
        }
    }

    #[test]
    fn coverage_checks_modes_membership_and_canonical_schema_keys() {
        let tables = [
            Table {
                id: 1,
                key: Type::Boolean,
                value: Type::Unit,
            },
            Table {
                id: 2,
                key: Type::Unit,
                value: Type::Unit,
            },
            Table {
                id: 3,
                key: Type::Tuple(vec![]),
                value: Type::Unit,
            },
            Table {
                id: 4,
                key: Type::String(2),
                value: Type::Unit,
            },
        ];
        let claims = crate::Limits::default().try_into().unwrap();
        let required = AccessManifest::new([(Key(1, vec![0]), ReadWrite)]).unwrap();
        for entries in [
            vec![(WholeTable(1), ReadWrite)],
            vec![(WholeTable(1), Write), (Key(1, vec![0]), Read)],
            vec![(WholeTable(1), Read), (Key(1, vec![0]), Write)],
            vec![(Key(1, vec![0]), ReadWrite)],
        ] {
            validate(
                &AccessManifest::new(entries).unwrap(),
                &required,
                &tables,
                &claims,
            )
            .unwrap();
        }
        for scope in [
            Key(1, vec![2]),
            Key(1, vec![]),
            Key(2, vec![0]),
            Key(4, vec![b'a']),
            Key(4, vec![0, 1]),
            Key(4, vec![255, 0, 0]),
            WholeTable(9),
        ] {
            let manifest = AccessManifest::new([(scope, Read)]).unwrap();
            assert!(validate(&manifest, &AccessManifest::default(), &tables, &claims).is_err());
        }
        for scope in [Key(2, vec![]), Key(3, vec![]), Key(4, vec![b'a', 0, 0])] {
            let manifest = AccessManifest::new([(scope, Read)]).unwrap();
            validate(&manifest, &AccessManifest::default(), &tables, &claims).unwrap();
        }
        for entries in [
            vec![],
            vec![(WholeTable(1), Write)],
            vec![(Key(1, vec![1]), ReadWrite)],
        ] {
            assert!(
                validate(
                    &AccessManifest::new(entries).unwrap(),
                    &required,
                    &tables,
                    &claims
                )
                .is_err()
            );
        }
        let whole = AccessManifest::new([(WholeTable(1), Read)]).unwrap();
        assert!(validate(&required, &whole, &tables, &claims).is_err());
    }

    #[test]
    fn scope_budget_charges_normalized_entries_not_modes_or_input_count() {
        let aliases =
            AccessManifest::new((0..20_000).map(|_| (Key(1, vec![]), ReadWrite))).unwrap();
        aliases.check_count(1).unwrap();
        assert!(aliases.check_count(0).is_err());
        let distinct = AccessManifest::new((1..=16_385).map(|id| (WholeTable(id), Read))).unwrap();
        assert!(distinct.encode().is_err());
        assert!(distinct.check_count(u64::MAX).is_err());
    }
}
