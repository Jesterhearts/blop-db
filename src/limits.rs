//! Set named resource limits for transactions and database policy.

use crate::storage::Error;
use crate::storage::LimitPolicy;

/// Logical resource bounds used as transaction claims or database policy.
///
/// Fields follow ascending resource-ID order in design section D.2. Zero is
/// valid and can disable a category of work. Conversion to [`LimitPolicy`]
/// checks each field against its format ceiling.
///
/// Byte limits measure logical encodings, not physical memory or disk usage.
/// They exclude page framing, allocation overhead, hashing costs, and obsolete
/// MVCC entries. Shared backing memory does not reduce the logical charge.
///
/// [`Default`] uses finite format ceilings. Override fields for the
/// application, for example `Limits { writes: 100, ..Limits::default() }`.
/// Defaults apply only when creating a database or submitting a transaction.
/// Decoding and replay use the stored claims and policies.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Limits {
    /// Maximum program bytes, including its header and constants.
    pub program_bytes: u64,
    /// Decoded instruction count at admission; instruction visits at execution.
    pub instructions: u64,
    /// Declared register count.
    pub registers: u64,
    /// Declared and supplied argument count.
    pub arguments: u64,
    /// Maximum argument bytes, including the count and Blob length prefixes.
    pub argument_bytes: u64,
    /// Program table count.
    pub tables: u64,
    /// Normalized manifest entry count.
    pub manifest_scopes: u64,
    /// Executed LOAD, EXISTS, INSERT, STORE, and DELETE count, including
    /// repeats.
    pub point_accesses: u64,
    /// Distinct `(table_id, canonical_key)` addresses across point accesses and
    /// selected range rows.
    pub distinct_keys: u64,
    /// Maximum canonical user-key bytes, including range endpoints and selected
    /// rows.
    pub key_bytes: u64,
    /// Maximum schema-encoded bytes of any argument, constant, initialized
    /// register, loaded value, or new stored value.
    pub value_bytes: u64,
    /// Sum of logical encoded bytes in currently initialized registers.
    /// Replacing a register replaces its charge; Rows includes its count and
    /// schema-encoded keys and values.
    pub register_bytes: u64,
    /// Sum of selected logical rows across all executed scans.
    pub range_rows: u64,
    /// Sum of canonical key and schema-encoded value bytes for selected scan
    /// rows.
    pub range_bytes: u64,
    /// Distinct addresses currently in the final-write overlay, including
    /// tombstones.
    pub writes: u64,
    /// Sum of `8 + key_length + 1 + value_length` over current overlay entries.
    /// Lengths use canonical keys and schema-encoded values.
    /// Replacing an entry replaces its charge; tombstones have zero value
    /// length.
    pub overlay_bytes: u64,
    /// Maximum returned value bytes, excluding its TypeDesc and Blob wrapper.
    pub result_bytes: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            program_bytes: 16 * 1024 * 1024,
            instructions: 65_535,
            registers: 65_535,
            arguments: 65_535,
            argument_bytes: 16 * 1024 * 1024,
            tables: 65_535,
            manifest_scopes: 16_384,
            point_accesses: 65_535,
            distinct_keys: 65_535,
            key_bytes: 1_024,
            value_bytes: 16 * 1024 * 1024,
            register_bytes: 64 * 1024 * 1024,
            range_rows: 65_535,
            range_bytes: 64 * 1024 * 1024,
            writes: 65_535,
            overlay_bytes: 64 * 1024 * 1024,
            result_bytes: 16 * 1024 * 1024,
        }
    }
}

impl TryFrom<Limits> for LimitPolicy {
    type Error = Error;

    fn try_from(limits: Limits) -> Result<Self, Self::Error> {
        Self::new([
            limits.program_bytes,
            limits.instructions,
            limits.registers,
            limits.arguments,
            limits.argument_bytes,
            limits.tables,
            limits.manifest_scopes,
            limits.point_accesses,
            limits.distinct_keys,
            limits.key_bytes,
            limits.value_bytes,
            limits.register_bytes,
            limits.range_rows,
            limits.range_bytes,
            limits.writes,
            limits.overlay_bytes,
            limits.result_bytes,
        ])
    }
}

impl From<&LimitPolicy> for Limits {
    fn from(policy: &LimitPolicy) -> Self {
        let values = policy.values();
        Self {
            program_bytes: values[0],
            instructions: values[1],
            registers: values[2],
            arguments: values[3],
            argument_bytes: values[4],
            tables: values[5],
            manifest_scopes: values[6],
            point_accesses: values[7],
            distinct_keys: values[8],
            key_bytes: values[9],
            value_bytes: values[10],
            register_bytes: values[11],
            range_rows: values[12],
            range_bytes: values[13],
            writes: values[14],
            overlay_bytes: values[15],
            result_bytes: values[16],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fields_follow_resource_id_order_and_round_trip_through_the_policy_codec() {
        let limits = Limits {
            program_bytes: 1,
            instructions: 2,
            registers: 3,
            arguments: 4,
            argument_bytes: 5,
            tables: 6,
            manifest_scopes: 7,
            point_accesses: 8,
            distinct_keys: 9,
            key_bytes: 10,
            value_bytes: 11,
            register_bytes: 12,
            range_rows: 13,
            range_bytes: 14,
            writes: 15,
            overlay_bytes: 16,
            result_bytes: 17,
        };
        let policy = LimitPolicy::try_from(limits).unwrap();
        assert_eq!(
            policy.values(),
            &std::array::from_fn(|index| index as u64 + 1)
        );

        let decoded = LimitPolicy::decode(&policy.encode()).unwrap();
        assert_eq!(decoded, policy);
        assert_eq!(Limits::from(&decoded), limits);
    }

    #[test]
    fn defaults_match_the_format_ceilings() {
        let limits = Limits::default();
        let policy = LimitPolicy::try_from(limits).unwrap();
        assert_eq!(
            policy.values(),
            &[
                16 * 1024 * 1024,
                65_535,
                65_535,
                65_535,
                16 * 1024 * 1024,
                65_535,
                16_384,
                65_535,
                65_535,
                1_024,
                16 * 1024 * 1024,
                64 * 1024 * 1024,
                65_535,
                64 * 1024 * 1024,
                65_535,
                64 * 1024 * 1024,
                16 * 1024 * 1024,
            ]
        );
        assert_eq!(Limits::from(&policy), limits);
    }

    #[test]
    fn every_field_rejects_values_above_its_ceiling() {
        let fields: [fn(&mut Limits) -> &mut u64; 17] = [
            |limits| &mut limits.program_bytes,
            |limits| &mut limits.instructions,
            |limits| &mut limits.registers,
            |limits| &mut limits.arguments,
            |limits| &mut limits.argument_bytes,
            |limits| &mut limits.tables,
            |limits| &mut limits.manifest_scopes,
            |limits| &mut limits.point_accesses,
            |limits| &mut limits.distinct_keys,
            |limits| &mut limits.key_bytes,
            |limits| &mut limits.value_bytes,
            |limits| &mut limits.register_bytes,
            |limits| &mut limits.range_rows,
            |limits| &mut limits.range_bytes,
            |limits| &mut limits.writes,
            |limits| &mut limits.overlay_bytes,
            |limits| &mut limits.result_bytes,
        ];
        for (index, field) in fields.into_iter().enumerate() {
            let mut limits = Limits::default();
            *field(&mut limits) += 1;
            assert!(
                matches!(LimitPolicy::try_from(limits), Err(Error::InvalidInput(_))),
                "resource {} accepted a value above its hard ceiling",
                index + 1
            );
        }
    }

    #[test]
    fn zero_limits_are_preserved_without_applying_defaults() {
        let limits = Limits {
            program_bytes: 0,
            instructions: 0,
            registers: 0,
            arguments: 0,
            argument_bytes: 0,
            tables: 0,
            manifest_scopes: 0,
            point_accesses: 0,
            distinct_keys: 0,
            key_bytes: 0,
            value_bytes: 0,
            register_bytes: 0,
            range_rows: 0,
            range_bytes: 0,
            writes: 0,
            overlay_bytes: 0,
            result_bytes: 0,
        };
        let policy = LimitPolicy::try_from(limits).unwrap();
        assert_eq!(policy.values(), &[0; 17]);
        let decoded = LimitPolicy::decode(&policy.encode()).unwrap();
        assert_eq!(Limits::from(&decoded), limits);
    }
}
