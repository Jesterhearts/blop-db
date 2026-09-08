//! Canonical record bodies for the serial writer and replay.
//!
//! Decoding checks the body format; validation checks the complete program and
//! arguments against the historical catalogue and policy before execution.
//! Preparation fills absent manifests. Decoded declarations are never replaced
//! with a narrower derivation: the recorded count defines resource 7 usage.

use crate::Transaction;
use crate::storage;
use crate::storage::LimitPolicy;
use crate::vm;
use crate::vm::CatalogueOperation;
use crate::vm::Error;
use crate::vm::Result;
use crate::vm::Type;

#[derive(Clone, Debug)]
pub(super) enum Command {
    Transaction {
        transaction: Transaction,
        claims: LimitPolicy,
        manifest: Option<vm::AccessManifest>,
    },
    Catalogue(CatalogueOperation),
    Limits(LimitPolicy),
}

pub(super) fn validate(
    view: &storage::View,
    sequence: u64,
    command: &Command,
) -> Result<()> {
    prepare(view, sequence, command)?;
    Ok(())
}

pub(super) fn prepare(
    view: &storage::View,
    sequence: u64,
    command: &Command,
) -> Result<Command> {
    if sequence == 0 || sequence == u64::MAX {
        return Err(Error::Invalid("reserved record sequence"));
    }
    match command {
        Command::Transaction {
            transaction,
            claims,
            manifest,
        } => {
            if let Some(manifest) = manifest {
                manifest.check_count(claims.values()[6])?;
            }
            let prepared =
                vm::prepare_transaction(view, sequence, transaction, claims, manifest.as_ref())?;
            Ok(Command::Transaction {
                transaction: transaction.clone(),
                claims: claims.clone(),
                manifest: Some(prepared.manifest().clone()),
            })
        }
        Command::Catalogue(operation) => {
            vm::validate_catalogue(operation)?;
            Ok(command.clone())
        }
        // LimitPolicy construction enforces the fixed administrative ceilings.
        Command::Limits(_) => Ok(command.clone()),
    }
}

pub(super) fn encode(command: &Command) -> Result<(u8, Vec<u8>)> {
    match command {
        Command::Transaction {
            transaction,
            claims,
            manifest,
        } => {
            let manifest = manifest
                .as_ref()
                .ok_or(Error::Invalid("transaction needs manifest preparation"))?;
            manifest.check_count(claims.values()[6])?;
            let manifest = manifest.encode()?;
            let mut body = vec![1, 0, 0, 0];
            blob(&mut body, transaction.program_bytes())?;
            blob(&mut body, transaction.argument_bytes())?;
            blob(&mut body, &manifest)?;
            for value in claims.values() {
                body.extend_from_slice(&value.to_le_bytes());
            }
            Ok((1, body))
        }
        Command::Catalogue(operation) => {
            // Public Type values must be bounded before descriptor() can run.
            vm::validate_catalogue(operation)?;
            let mut body = vec![1, 0];
            match operation {
                CatalogueOperation::Create { name, key, value } => {
                    body.extend_from_slice(&[1, 0]);
                    blob(&mut body, name.as_bytes())?;
                    blob(&mut body, &key.descriptor())?;
                    blob(&mut body, &value.descriptor())?;
                }
                CatalogueOperation::Rename { table, name } => {
                    body.extend_from_slice(&[2, 0]);
                    body.extend_from_slice(&table.to_le_bytes());
                    blob(&mut body, name.as_bytes())?;
                }
                CatalogueOperation::Drop { table } => {
                    body.extend_from_slice(&[3, 0]);
                    body.extend_from_slice(&table.to_le_bytes());
                }
            }
            Ok((2, body))
        }
        Command::Limits(policy) => Ok((3, policy.encode())),
    }
}

pub(super) fn decode(
    kind: u8,
    body: &[u8],
) -> Result<Command> {
    let format = match kind {
        1 => "transaction",
        2 => "administrative",
        3 => "limit policy",
        _ => return Err(Error::Invalid("unknown record kind")),
    };
    if body.len() > 64 * 1024 * 1024 - 72 {
        return Err(Error::Invalid("log record exceeds 64 MiB"));
    }
    let mut input = body;
    let version = u16::from_le_bytes(take(&mut input, 2)?.try_into().unwrap());
    if version != 1 {
        return Err(Error::Unsupported { format, version });
    }
    let command = match kind {
        1 => {
            if take(&mut input, 2)? != [0; 2] {
                return Err(Error::Invalid("nonzero transaction reserved field"));
            }
            let program = read_blob(&mut input)?;
            let arguments = read_blob(&mut input)?;
            let manifest = read_blob(&mut input)?;
            let claims = read_budget(&mut input)?;
            if program.len() as u64 > claims.values()[0]
                || arguments.len() as u64 > claims.values()[4]
            {
                return Err(Error::Invalid("transaction bytes exceed claims"));
            }
            let transaction = Transaction::from_parts(program.to_vec(), arguments.to_vec());
            let manifest = vm::AccessManifest::decode(manifest)?;
            manifest.check_count(claims.values()[6])?;
            Command::Transaction {
                transaction,
                claims,
                manifest: Some(manifest),
            }
        }
        2 => {
            let operation = take(&mut input, 1)?[0];
            if take(&mut input, 1)? != [0] {
                return Err(Error::Invalid("nonzero administrative reserved field"));
            }
            let operation = match operation {
                1 => CatalogueOperation::Create {
                    name: read_name(&mut input)?,
                    key: Type::decode(read_blob(&mut input)?)?,
                    value: Type::decode(read_blob(&mut input)?)?,
                },
                2 => CatalogueOperation::Rename {
                    table: u64::from_le_bytes(take(&mut input, 8)?.try_into().unwrap()),
                    name: read_name(&mut input)?,
                },
                3 => CatalogueOperation::Drop {
                    table: u64::from_le_bytes(take(&mut input, 8)?.try_into().unwrap()),
                },
                _ => return Err(Error::Invalid("unknown catalogue operation")),
            };
            vm::validate_catalogue(&operation)?;
            Command::Catalogue(operation)
        }
        3 => {
            if take(&mut input, 2)? != [0; 2] {
                return Err(Error::Invalid("nonzero limit policy reserved field"));
            }
            Command::Limits(read_budget(&mut input)?)
        }
        _ => unreachable!(),
    };
    if !input.is_empty() {
        return Err(Error::Invalid("trailing record body bytes"));
    }
    Ok(command)
}

pub(super) fn execute(
    store: &mut storage::Store,
    sequence: u64,
    digest: [u8; 32],
    command: &Command,
) -> Result<vm::Outcome> {
    validate(&storage::view(store), sequence, command)?;
    match command {
        Command::Transaction {
            transaction,
            claims,
            ..
        } => vm::execute(store, sequence, digest, transaction, claims),
        Command::Catalogue(operation) => vm::execute_catalogue(store, sequence, digest, operation),
        Command::Limits(policy) => vm::execute_limits(store, sequence, digest, policy),
    }
}

fn take<'a>(
    input: &mut &'a [u8],
    length: usize,
) -> Result<&'a [u8]> {
    let (bytes, rest) = input
        .split_at_checked(length)
        .ok_or(Error::Invalid("truncated record body"))?;
    *input = rest;
    Ok(bytes)
}

fn read_blob<'a>(input: &mut &'a [u8]) -> Result<&'a [u8]> {
    let length = u32::from_le_bytes(take(input, 4)?.try_into().unwrap()) as usize;
    take(input, length)
}

fn read_name(input: &mut &[u8]) -> Result<String> {
    let bytes = read_blob(input)?;
    if bytes.is_empty() || bytes.len() > 255 || bytes.contains(&0) {
        return Err(Error::Invalid("invalid table name"));
    }
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|_| Error::Invalid("invalid UTF-8 table name"))
}

fn read_budget(input: &mut &[u8]) -> Result<LimitPolicy> {
    let mut values = [0; 17];
    for value in &mut values {
        *value = u64::from_le_bytes(take(input, 8)?.try_into().unwrap());
    }
    LimitPolicy::new(values).map_err(|error| match error {
        storage::Error::InvalidInput(reason) => Error::Invalid(reason),
        error => Error::Storage(error),
    })
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

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use super::*;
    use crate::storage::TreeId;
    use crate::storage::mvcc;
    use crate::tx;
    use crate::vm::AbortReason;
    use crate::vm::Outcome;

    fn policy() -> LimitPolicy {
        LimitPolicy::new([
            1_048_576, 1024, 1024, 64, 1_048_576, 64, 64, 1024, 1024, 1024, 1_048_576, 8_388_608,
            1024, 8_388_608, 1024, 8_388_608, 1_048_576,
        ])
        .unwrap()
    }

    fn transaction(transaction: Transaction) -> Command {
        let manifest = vm::AccessManifest::new(
            vm::transaction_tables(transaction.program_bytes())
                .unwrap()
                .into_iter()
                .map(|id| (vm::Scope::Table(id), vm::AccessMode::ReadWrite)),
        )
        .unwrap();
        Command::Transaction {
            transaction,
            claims: policy(),
            manifest: Some(manifest),
        }
    }

    fn commands() -> [Command; 5] {
        [
            transaction(
                tx! {
                    captures { key: u64 = 7_u64 }
                    tables { unused: u64 => i64 = 9, data: u64 => i64 = 3 }
                    return data[key];
                }
                .unwrap(),
            ),
            Command::Catalogue(CatalogueOperation::Create {
                name: "data".into(),
                key: Type::U64,
                value: Type::Tuple(vec![Type::I64, Type::String(16)]),
            }),
            Command::Catalogue(CatalogueOperation::Rename {
                table: 3,
                name: "renamed".into(),
            }),
            Command::Catalogue(CatalogueOperation::Drop { table: 3 }),
            Command::Limits(policy()),
        ]
    }

    fn create() -> (tempfile::TempDir, storage::Store) {
        let directory = tempfile::tempdir().unwrap();
        let store = storage::create(
            directory.path().join("db"),
            storage::Genesis {
                database_id: [1; 16],
                initial_policy: policy(),
            },
            [2; 16],
        )
        .unwrap();
        (directory, store)
    }

    fn entries(
        view: &storage::View,
        tree: TreeId,
    ) -> Vec<storage::Entry> {
        storage::scan(view, tree, Bound::Unbounded, Bound::Unbounded)
            .unwrap()
            .collect::<storage::Result<_>>()
            .unwrap()
    }

    #[test]
    fn every_command_round_trips_without_changing_its_bytes() {
        for command in commands() {
            let (kind, body) = encode(&command).unwrap();
            let decoded = decode(kind, &body).unwrap();
            assert_eq!(encode(&decoded).unwrap(), (kind, body));
            match (command, decoded) {
                (
                    Command::Transaction {
                        transaction: a,
                        claims: ac,
                        manifest: am,
                    },
                    Command::Transaction {
                        transaction: b,
                        claims: bc,
                        manifest: bm,
                    },
                ) => {
                    assert_eq!(a, b);
                    assert_eq!(ac, bc);
                    assert_eq!(am, bm);
                }
                (Command::Catalogue(a), Command::Catalogue(b)) => assert_eq!(a, b),
                (Command::Limits(a), Command::Limits(b)) => assert_eq!(a, b),
                _ => panic!("record kind changed"),
            }
        }
    }

    #[test]
    fn transaction_body_has_exact_broad_manifest_and_unprefixed_budget() {
        let [command, ..] = commands();
        let Command::Transaction {
            transaction: input,
            claims,
            ..
        } = &command
        else {
            unreachable!()
        };
        let (kind, body) = encode(&command).unwrap();
        assert_eq!(kind, 1);
        let mut expected = vec![1, 0, 0, 0];
        for bytes in [input.program_bytes(), input.argument_bytes()] {
            expected.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            expected.extend_from_slice(bytes);
        }
        expected.extend_from_slice(&[
            36, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0,
            0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0,
        ]);
        let budget_offset = expected.len();
        for value in claims.values() {
            expected.extend_from_slice(&value.to_le_bytes());
        }
        assert_eq!(body, expected);
        assert_eq!(body.len() - budget_offset, 17 * 8);
        let command = transaction(tx! { return 42; }.unwrap());
        let (_, body) = encode(&command).unwrap();
        assert_eq!(
            &body[body.len() - 17 * 8 - 8..body.len() - 17 * 8],
            &[4, 0, 0, 0, 0, 0, 0, 0]
        );
    }

    #[test]
    fn catalogue_and_policy_bodies_use_canonical_operands() {
        assert_eq!(
            encode(&Command::Catalogue(CatalogueOperation::Create {
                name: "a".into(),
                key: Type::Unit,
                value: Type::Boolean,
            }))
            .unwrap(),
            (
                2,
                vec![
                    1, 0, 1, 0, 1, 0, 0, 0, b'a', 3, 0, 0, 0, 1, 0, 0, 3, 0, 0, 0, 1, 0, 1,
                ]
            )
        );
        assert_eq!(
            encode(&Command::Catalogue(CatalogueOperation::Rename {
                table: 9,
                name: "a".into(),
            }))
            .unwrap(),
            (
                2,
                vec![1, 0, 2, 0, 9, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, b'a']
            )
        );
        assert_eq!(
            encode(&Command::Catalogue(CatalogueOperation::Drop { table: 9 })).unwrap(),
            (2, vec![1, 0, 3, 0, 9, 0, 0, 0, 0, 0, 0, 0])
        );
        let zero = LimitPolicy::new([0; 17]).unwrap();
        let mut expected = vec![0; 140];
        expected[0] = 1;
        assert_eq!(encode(&Command::Limits(zero)).unwrap(), (3, expected));
    }

    #[test]
    fn bodies_reject_all_truncations_trailing_bytes_versions_and_reserved_fields() {
        for command in commands() {
            let (kind, original) = encode(&command).unwrap();
            for length in 0..original.len() {
                assert!(
                    decode(kind, &original[..length]).is_err(),
                    "kind {kind}, length {length}"
                );
            }
            let mut bytes = original.clone();
            bytes.push(0);
            assert!(matches!(decode(kind, &bytes), Err(Error::Invalid(_))));
            for offset in if kind == 2 { 3..4 } else { 2..4 } {
                let mut bytes = original.clone();
                bytes[offset] = 1;
                assert!(matches!(decode(kind, &bytes), Err(Error::Invalid(_))));
            }
            let mut bytes = original;
            bytes[0] = 2;
            assert!(matches!(
                decode(kind, &bytes),
                Err(Error::Unsupported { version: 2, .. })
            ));
        }
        for kind in [0, 4, 255] {
            assert!(matches!(
                decode(kind, &[1, 0, 0, 0]),
                Err(Error::Invalid(_))
            ));
        }
        assert!(matches!(decode(2, &[1, 0, 4, 0]), Err(Error::Invalid(_))));
    }

    #[test]
    fn decoding_rejects_noncanonical_manifests_lengths_and_claim_ceilings() {
        let [command, ..] = commands();
        let Command::Transaction { transaction, .. } = &command else {
            unreachable!()
        };
        let (_, original) = encode(&command).unwrap();
        let arguments_length = 8 + transaction.program_bytes().len();
        let manifest_length = arguments_length + 4 + transaction.argument_bytes().len();
        let manifest = manifest_length + 4;
        for offset in [4, arguments_length, manifest_length] {
            let mut bytes = original.clone();
            bytes[offset..offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());
            assert!(matches!(decode(1, &bytes), Err(Error::Invalid(_))));
        }
        for (offset, value) in [
            (0, 0),
            (0, 3),
            (12, 2),
            (13, 0),
            (13, 4),
            (14, 1),
            (15, 1),
            (16, 1),
            (20, 3),
        ] {
            let mut bytes = original.clone();
            bytes[manifest + offset] = value;
            assert!(
                matches!(decode(1, &bytes), Err(Error::Invalid(_))),
                "offset {offset}"
            );
        }
        let mut bytes = original.clone();
        bytes[manifest + 4..manifest + 20].copy_from_slice(&original[manifest + 20..manifest + 36]);
        bytes[manifest + 20..manifest + 36].copy_from_slice(&original[manifest + 4..manifest + 20]);
        assert!(matches!(decode(1, &bytes), Err(Error::Invalid(_))));

        // Canonical point scopes and all three modes retain their exact bytes.
        let mut bytes = original.clone();
        bytes[manifest_length..manifest].copy_from_slice(&44_u32.to_le_bytes());
        bytes[manifest + 12] = 1;
        bytes[manifest + 16..manifest + 20].copy_from_slice(&8_u32.to_le_bytes());
        bytes.splice(manifest + 20..manifest + 20, 7_u64.to_be_bytes());
        for mode in 1..=3 {
            bytes[manifest + 13] = mode;
            assert_eq!(encode(&decode(1, &bytes).unwrap()).unwrap().1, bytes);
        }

        for command in [command, Command::Limits(policy())] {
            let (kind, original) = encode(&command).unwrap();
            let budget = original.len() - 17 * 8;
            for index in 0..17 {
                let mut bytes = original.clone();
                bytes[budget + index * 8..budget + (index + 1) * 8]
                    .copy_from_slice(&u64::MAX.to_le_bytes());
                assert!(matches!(decode(kind, &bytes), Err(Error::Invalid(_))));
            }
        }
    }

    #[test]
    fn catalogue_encoding_validates_public_types_before_their_panicking_encoder() {
        for key in [
            Type::Tuple(vec![Type::Unit; 65_536]),
            Type::Bytes(u32::MAX),
            Type::Rows {
                max_rows: 0,
                key: Box::new(Type::Unit),
                value: Box::new(Type::Unit),
            },
        ] {
            let command = Command::Catalogue(CatalogueOperation::Create {
                name: "valid".into(),
                key,
                value: Type::Unit,
            });
            assert!(matches!(encode(&command), Err(Error::Invalid(_))));
        }
        let (_, create) = encode(&Command::Catalogue(CatalogueOperation::Create {
            name: "a".into(),
            key: Type::Unit,
            value: Type::Boolean,
        }))
        .unwrap();
        for (offset, value) in [(4, 0), (8, 0), (8, 255), (15, 255)] {
            let mut bytes = create.clone();
            bytes[offset] = value;
            assert!(decode(2, &bytes).is_err());
        }
        for id in [0_u64, u64::MAX] {
            let mut bytes = vec![1, 0, 3, 0];
            bytes.extend_from_slice(&id.to_le_bytes());
            assert!(matches!(decode(2, &bytes), Err(Error::Invalid(_))));
        }
    }

    #[test]
    fn scope_claims_are_checked_before_encoding_validation_and_execution() {
        let (_directory, mut store) = create();
        let mut values = *policy().values();
        values[6] = 0;
        let command = Command::Transaction {
            transaction: tx! { tables { data: u64 => i64 = 1 } return 42; }.unwrap(),
            claims: LimitPolicy::new(values).unwrap(),
            manifest: Some(
                vm::AccessManifest::new([(vm::Scope::Table(1), vm::AccessMode::ReadWrite)])
                    .unwrap(),
            ),
        };
        assert!(matches!(
            encode(&command),
            Err(Error::Invalid("manifest_scopes exceeds claim"))
        ));
        assert!(matches!(
            validate(&storage::view(&store), 1, &command),
            Err(Error::Invalid("manifest_scopes exceeds claim"))
        ));
        assert!(matches!(
            execute(&mut store, 1, [1; 32], &command),
            Err(Error::Invalid("manifest_scopes exceeds claim"))
        ));
        assert!(entries(&storage::view(&store), TreeId::Outcomes).is_empty());
        let (_, mut bytes) = encode(&commands()[0]).unwrap();
        let scope_claim = bytes.len() - 17 * 8 + 6 * 8;
        bytes[scope_claim..scope_claim + 8].fill(0);
        assert!(matches!(
            decode(1, &bytes),
            Err(Error::Invalid("manifest_scopes exceeds claim"))
        ));
    }

    #[test]
    fn validation_uses_historical_policy_and_never_installs_semantic_outcomes() {
        let (_directory, mut store) = create();
        let command = transaction(tx! { abort(19); }.unwrap());
        validate(&storage::view(&store), 1, &command).unwrap();
        assert!(entries(&storage::view(&store), TreeId::Outcomes).is_empty());
        assert!(matches!(
            execute(&mut store, 1, [1; 32], &command).unwrap(),
            Outcome::Aborted(vm::Abort {
                reason: AbortReason::ExplicitAbort,
                user_code: 19,
                ..
            })
        ));
        let zero = Command::Limits(LimitPolicy::new([0; 17]).unwrap());
        execute(&mut store, 2, [2; 32], &zero).unwrap();
        let view = storage::view(&store);
        let before = entries(&view, TreeId::Outcomes);
        validate(&view, 2, &command).unwrap();
        assert!(matches!(
            validate(&view, 3, &command),
            Err(Error::Invalid(
                "transaction claims exceed historical policy"
            ))
        ));
        assert_eq!(entries(&storage::view(&store), TreeId::Outcomes), before);
        validate(&view, 3, &Command::Limits(policy())).unwrap();
        for sequence in [0, u64::MAX] {
            assert!(matches!(
                validate(&view, sequence, &zero),
                Err(Error::Invalid(_))
            ));
        }
    }

    #[test]
    fn admission_does_not_read_rows_or_resolve_catalogue_conflicts() {
        let (_directory, mut store) = create();
        let create = Command::Catalogue(CatalogueOperation::Create {
            name: "data".into(),
            key: Type::U64,
            value: Type::I64,
        });
        execute(&mut store, 1, [1; 32], &create).unwrap();
        execute(
            &mut store,
            2,
            [2; 32],
            &transaction(tx! { tables { data: u64 => i64 = 1 } data[7] = 1; }.unwrap()),
        )
        .unwrap();
        storage::apply(
            &mut store,
            &[storage::Mutation {
                tree: TreeId::State,
                key: mvcc::StateKey::new(1, 7_u64.to_be_bytes().to_vec(), 2)
                    .unwrap()
                    .encode(),
                value: Some(vec![255]),
            }],
        )
        .unwrap();
        let command = transaction(tx! { tables { data: u64 => i64 = 1 } return data[7]; }.unwrap());
        let view = storage::view(&store);
        let before = entries(&view, TreeId::Outcomes);
        validate(&view, 3, &command).unwrap();
        assert!(matches!(
            execute(&mut store, 3, [3; 32], &command),
            Err(Error::Storage(storage::Error::Corrupt(_)))
        ));
        validate(&view, 3, &create).unwrap();
        let missing = Command::Catalogue(CatalogueOperation::Drop { table: 999 });
        validate(&view, 3, &missing).unwrap();
        assert_eq!(entries(&storage::view(&store), TreeId::Outcomes), before);
        assert!(matches!(
            execute(&mut store, 3, [3; 32], &create).unwrap(),
            Outcome::Aborted(vm::Abort {
                reason: AbortReason::NameInUse,
                ..
            })
        ));
        assert!(matches!(
            execute(&mut store, 4, [4; 32], &missing).unwrap(),
            Outcome::Aborted(vm::Abort {
                reason: AbortReason::TableNotLive,
                ..
            })
        ));
    }

    #[test]
    fn preparation_derives_points_and_retains_supplied_counts_after_decode() {
        let (_directory, mut store) = create();
        execute(
            &mut store,
            1,
            [1; 32],
            &Command::Catalogue(CatalogueOperation::Create {
                name: "data".into(),
                key: Type::U64,
                value: Type::U64,
            }),
        )
        .unwrap();
        let transaction =
            tx! { tables { data: u64 => u64 = 1 } data[40 + 2] = 7; return data[42]; }.unwrap();
        let raw = Command::Transaction {
            transaction: transaction.clone(),
            claims: policy(),
            manifest: None,
        };
        assert!(encode(&raw).is_err());
        let view = storage::view(&store);
        let prepared = prepare(&view, 2, &raw).unwrap();
        let expected = vm::AccessManifest::new([(
            vm::Scope::Key(1, 42_u64.to_be_bytes().to_vec()),
            vm::AccessMode::ReadWrite,
        )])
        .unwrap();
        let Command::Transaction { manifest, .. } = prepared else {
            unreachable!()
        };
        assert_eq!(manifest, Some(expected));

        let supplied = vm::AccessManifest::new([
            (
                vm::Scope::Key(1, 42_u64.to_be_bytes().to_vec()),
                vm::AccessMode::ReadWrite,
            ),
            (
                vm::Scope::Key(1, 99_u64.to_be_bytes().to_vec()),
                vm::AccessMode::Read,
            ),
        ])
        .unwrap();
        let command = Command::Transaction {
            transaction,
            claims: policy(),
            manifest: Some(supplied.clone()),
        };
        let (kind, bytes) = encode(&command).unwrap();
        let decoded = decode(kind, &bytes).unwrap();
        let prepared = prepare(&view, 2, &decoded).unwrap();
        assert_eq!(encode(&prepared).unwrap(), (kind, bytes.clone()));
        let Command::Transaction { manifest, .. } = prepared else {
            unreachable!()
        };
        assert_eq!(manifest, Some(supplied));
        let mut invalid_count = bytes;
        let offset = invalid_count.len() - 17 * 8 + 6 * 8;
        invalid_count[offset..offset + 8].copy_from_slice(&1_u64.to_le_bytes());
        assert!(matches!(
            decode(kind, &invalid_count),
            Err(Error::Invalid("manifest_scopes exceeds claim"))
        ));
    }

    #[test]
    fn supplied_table_scope_can_fit_when_derived_points_exceed_resource_seven() {
        let (_directory, mut store) = create();
        execute(
            &mut store,
            1,
            [1; 32],
            &Command::Catalogue(CatalogueOperation::Create {
                name: "data".into(),
                key: Type::U64,
                value: Type::U64,
            }),
        )
        .unwrap();
        let mut values = *policy().values();
        values[6] = 1;
        let command = Command::Transaction {
            transaction: tx! { tables { data: u64 => u64 = 1 } data[1] = 42; data[2] = 42; }
                .unwrap(),
            claims: LimitPolicy::new(values).unwrap(),
            manifest: None,
        };
        assert!(matches!(
            validate(&storage::view(&store), 2, &command),
            Err(Error::Invalid("manifest_scopes exceeds claim"))
        ));
        let Command::Transaction {
            transaction,
            claims,
            ..
        } = command
        else {
            unreachable!()
        };
        let command = Command::Transaction {
            transaction,
            claims,
            manifest: Some(
                vm::AccessManifest::new([(vm::Scope::Table(1), vm::AccessMode::Write)]).unwrap(),
            ),
        };
        validate(&storage::view(&store), 2, &command).unwrap();
        assert!(matches!(
            execute(&mut store, 2, [2; 32], &command).unwrap(),
            Outcome::Success { .. }
        ));
        let mut values = *policy().values();
        values[6] = 0;
        let unused = Command::Transaction {
            transaction: tx! { tables { data: u64 => u64 = 1 } return 42; }.unwrap(),
            claims: LimitPolicy::new(values).unwrap(),
            manifest: None,
        };
        let prepared = prepare(&storage::view(&store), 3, &unused).unwrap();
        let Command::Transaction { manifest, .. } = prepared else {
            unreachable!()
        };
        assert!(manifest.unwrap().is_empty());
    }

    #[test]
    fn point_validation_uses_historical_schemas_not_recreated_names() {
        let (_directory, mut store) = create();
        execute(
            &mut store,
            1,
            [1; 32],
            &Command::Catalogue(CatalogueOperation::Create {
                name: "data".into(),
                key: Type::Unit,
                value: Type::U64,
            }),
        )
        .unwrap();
        execute(
            &mut store,
            2,
            [2; 32],
            &Command::Catalogue(CatalogueOperation::Drop { table: 1 }),
        )
        .unwrap();
        execute(
            &mut store,
            3,
            [3; 32],
            &Command::Catalogue(CatalogueOperation::Create {
                name: "data".into(),
                key: Type::Boolean,
                value: Type::U64,
            }),
        )
        .unwrap();
        let command = Command::Transaction {
            transaction: tx! { tables { data: () => u64 = 1 } delete(data[()]); }.unwrap(),
            claims: policy(),
            manifest: Some(
                vm::AccessManifest::new([(vm::Scope::Key(1, vec![]), vm::AccessMode::Write)])
                    .unwrap(),
            ),
        };
        let view = storage::view(&store);
        validate(&view, 2, &command).unwrap();
        assert!(validate(&view, 4, &command).is_err());
        let mut wrong_key = command.clone();
        let Command::Transaction { manifest, .. } = &mut wrong_key else {
            unreachable!()
        };
        *manifest = Some(
            vm::AccessManifest::new([(vm::Scope::Key(1, vec![0]), vm::AccessMode::Write)]).unwrap(),
        );
        assert!(validate(&view, 2, &wrong_key).is_err());
        let mut wrong_table = command;
        let Command::Transaction { manifest, .. } = &mut wrong_table else {
            unreachable!()
        };
        *manifest =
            Some(vm::AccessManifest::new([(vm::Scope::Table(3), vm::AccessMode::Write)]).unwrap());
        assert!(matches!(
            validate(&view, 2, &wrong_table),
            Err(Error::Invalid(
                "manifest table is not in program table array"
            ))
        ));
    }

    #[test]
    fn prepared_program_is_reusable_and_runtime_failures_do_not_prune_scopes() {
        let (_directory, mut store) = create();
        execute(
            &mut store,
            1,
            [1; 32],
            &Command::Catalogue(CatalogueOperation::Create {
                name: "data".into(),
                key: Type::U64,
                value: Type::U64,
            }),
        )
        .unwrap();
        let view = storage::view(&store);
        let transaction =
            tx! { tables { data: u64 => u64 = 1 } data[7] = 42; return data[7]; }.unwrap();
        let prepared = vm::prepare_transaction(&view, 2, &transaction, &policy(), None).unwrap();
        assert_eq!(prepared.sequence(), 2);
        assert_eq!(prepared.tables()[0].id, 1);
        assert!(prepared.instruction_count() > 0);
        assert_eq!(prepared.manifest().len(), 1);
        let outcome = vm::interpret_prepared(&view, &prepared).unwrap();
        assert_eq!(vm::interpret_prepared(&view, &prepared).unwrap(), outcome);
        assert!(matches!(
            outcome,
            Outcome::Success {
                value: vm::Value::U64(42),
                ..
            }
        ));
        assert!(entries(&view, TreeId::State).is_empty());

        let mut values = *policy().values();
        values[11] = 0;
        let claims = LimitPolicy::new(values).unwrap();
        let limited = vm::prepare_transaction(&view, 2, &transaction, &claims, None).unwrap();
        assert_eq!(limited.manifest(), prepared.manifest());
        assert!(matches!(
            vm::interpret_prepared(&view, &limited).unwrap(),
            Outcome::Aborted(vm::Abort {
                reason: AbortReason::ResourceLimit,
                detail: 12,
                ..
            })
        ));

        let transaction = tx! { tables { data: u64 => u64 = 1 } let failed = 1_u64 / 0_u64; delete(data[failed]); }.unwrap();
        let failed = vm::prepare_transaction(&view, 2, &transaction, &policy(), None).unwrap();
        assert_eq!(
            failed.manifest().entries(),
            [(vm::Scope::Table(1), vm::AccessMode::Write)]
        );
        assert!(matches!(
            vm::interpret_prepared(&view, &failed).unwrap(),
            Outcome::Aborted(vm::Abort {
                reason: AbortReason::DivisionByZero,
                ..
            })
        ));
    }
}
