//! Live-owner proofs for immutable roots and bounded above-checkpoint edits.
//! Full validation remains the boundary for recovery and untrusted root sets.
//! Only successful library-owned COW edits inherit a proof, after checking new
//! system entries. Deletions keep conservative sequence maxima. Root identities
//! are never reused within a file, and file handover discards every proof.

use std::collections::BTreeMap;
use std::ops::Bound;

use super::Error;
use super::Genesis;
use super::LimitPolicy;
use super::Manifest;
use super::Mutation;
use super::Result;
use super::TreeId;
use super::log_validation::LogValidation;
use super::mvcc::StateValue;
use super::page::PageReader;
use super::store::TREES;
use super::store::entry_sequence;
use super::tree;

#[derive(Clone, Copy, Debug)]
pub(super) struct ValidatedTree {
    pub root: u64,
    pub sequence: u64,
}

pub(super) type Proofs = [Option<ValidatedTree>; 4];

pub(super) struct RuntimeValidation {
    pub live: Proofs,
    pub published: Proofs,
    pub pending: Option<Pending>,
    pub logs: Option<LogValidation>,
}

#[derive(Default)]
pub(super) struct Pending {
    pub keys: BTreeMap<(usize, Vec<u8>), u64>,
    bytes: usize,
}

impl RuntimeValidation {
    pub fn new(checkpoint_is_live: bool) -> Self {
        Self {
            live: [None; 4],
            published: [None; 4],
            pending: checkpoint_is_live.then(Pending::default),
            logs: None,
        }
    }
}

pub(super) fn observe(
    validation: &mut RuntimeValidation,
    previous: [u64; 5],
    roots: [u64; 5],
    changes: &[Mutation],
    manifest: &Manifest,
    genesis: &Genesis,
) {
    for (index, proof) in validation.live.iter_mut().enumerate() {
        if proof.is_some_and(|proof| proof.root != previous[index]) {
            *proof = None;
        }
    }
    for change in changes {
        let index = change.tree.index();
        if index == TreeId::Cursors.index() {
            continue;
        }
        let sequence = entry_sequence(change.tree, &change.key);
        let valid = sequence.is_ok()
            && change
                .value
                .as_ref()
                .is_none_or(|value| valid_value(change.tree, value).is_ok())
            && !(change.tree == TreeId::Policy
                && change.key == 0_u64.to_be_bytes()
                && change.value.as_deref() != Some(genesis.initial_policy.encode().as_slice()));
        if valid {
            if let Some(proof) = &mut validation.live[index]
                && change.value.is_some()
            {
                proof.sequence = proof.sequence.max(*sequence.as_ref().unwrap());
            }
        } else {
            // Preserve public apply/publish error timing: a bad edit
            // invalidates its proof instead of being silently
            // trusted or rejected early.
            validation.live[index] = None;
        }
        if let Some(pending) = &mut validation.pending {
            let key = (index, change.key.clone());
            if pending.keys.remove(&key).is_some() {
                pending.bytes -= change.key.len() + 96;
            }
            if let Ok(sequence) = sequence {
                if change.value.is_some() && sequence > manifest.checkpoint_sequence {
                    let bytes = change.key.len() + 96;
                    if pending.bytes + bytes > 1024 * 1024 {
                        validation.pending = None;
                    } else {
                        pending.bytes += bytes;
                        pending.keys.insert(key, sequence);
                    }
                }
            } else {
                validation.pending = None;
            }
        }
    }
    for (index, proof) in validation.live.iter_mut().enumerate() {
        if let Some(proof) = proof {
            proof.root = roots[index];
        }
    }
}

pub(super) fn published(
    validation: &mut RuntimeValidation,
    roots: [u64; 5],
    manifest: &Manifest,
    proofs: Proofs,
) {
    validation.published = proofs;
    for (index, proof) in proofs.into_iter().enumerate() {
        if roots[index] == manifest.roots[index] {
            validation.live[index] = proof;
        }
    }
    if let Some(pending) = &mut validation.pending {
        pending
            .keys
            .retain(|_, sequence| *sequence > manifest.checkpoint_sequence);
        pending.bytes = pending.keys.keys().map(|(_, key)| key.len() + 96).sum();
    } else if roots[..4] == manifest.roots[..4] {
        validation.pending = Some(Pending::default());
    }
}

pub(super) fn validate(
    reader: &PageReader,
    manifest: &Manifest,
    genesis: &Genesis,
    proofs: Proofs,
) -> Result<Proofs> {
    let reader = reader.uncached();
    let mut validated = [None; 4];
    for tree in TREES {
        let index = tree.index();
        let root = manifest.roots[index];
        if index < 4
            && let Some(proof) = proofs[index]
            && proof.root == root
            && proof.sequence <= manifest.checkpoint_sequence
        {
            validated[index] = Some(proof);
            continue;
        }
        tree::validate(&reader, tree, root)?;
        let mut maximum = 0;
        for entry in tree::scan(&reader, tree, root, Bound::Unbounded, Bound::Unbounded)? {
            let (key, value) = entry?;
            let sequence = entry_sequence(tree, &key)?;
            if tree != TreeId::Cursors && sequence > manifest.checkpoint_sequence {
                return Err(Error::Corrupt(
                    "checkpoint contains a post-checkpoint version",
                ));
            }
            maximum = maximum.max(sequence);
            valid_value(tree, &value)?;
            if tree == TreeId::Cursors {
                validate_cursor(sequence, &value, manifest)?;
            }
        }
        if tree == TreeId::Policy
            && tree::get(&reader, tree, root, &0_u64.to_be_bytes())?
                != Some(genesis.initial_policy.encode())
        {
            return Err(Error::Corrupt("initial policy differs from genesis"));
        }
        if index < 4 {
            validated[index] = Some(ValidatedTree {
                root,
                sequence: maximum,
            });
        }
    }
    Ok(validated)
}

fn valid_value(
    tree: TreeId,
    value: &[u8],
) -> Result<()> {
    match tree {
        TreeId::State => {
            StateValue::decode(value).map_err(|error| match error {
                Error::InvalidInput(reason) => Error::Corrupt(reason),
                other => other,
            })?;
        }
        TreeId::Policy => {
            LimitPolicy::decode(value)?;
        }
        _ => {}
    }
    Ok(())
}

fn validate_cursor(
    id: u64,
    value: &[u8],
    manifest: &Manifest,
) -> Result<()> {
    if id >= manifest.next_cursor_id || value.len() < 16 || value.len() > 271 {
        return Err(Error::Corrupt("invalid cursor identity or value length"));
    }
    let version = u16::from_le_bytes(value[..2].try_into().unwrap());
    if version != 1 {
        return Err(Error::Unsupported {
            format: "cursor",
            version,
        });
    }
    let baseline = u64::from_le_bytes(value[4..12].try_into().unwrap());
    let length = u32::from_le_bytes(value[12..16].try_into().unwrap()) as usize;
    if !(1..=3).contains(&value[2])
        || value[3] != 0
        || baseline > manifest.durable_sequence
        || baseline < manifest.history_floor
        || (value[2] != 1 && manifest.log_floor > baseline + 1)
        || length != value.len() - 16
        || value[16..].contains(&0)
        || std::str::from_utf8(&value[16..]).is_err()
    {
        return Err(Error::Corrupt("invalid cursor value or retention floor"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::mvcc::StateKey;

    #[test]
    fn above_checkpoint_tracking_is_bounded_and_pruned() {
        let directory = tempfile::tempdir().unwrap();
        let genesis = Genesis {
            database_id: [1; 16],
            initial_policy: LimitPolicy::new([0; 17]).unwrap(),
        };
        let store =
            crate::storage::create(directory.path().join("db"), genesis.clone(), [2; 16]).unwrap();
        let manifest = store.manifest();
        let mut validation = RuntimeValidation::new(true);
        let changes: Vec<_> = (2_u64..=1101)
            .map(|sequence| Mutation {
                tree: TreeId::State,
                key: StateKey::new(1, vec![1; 1000], sequence).unwrap().encode(),
                value: Some(StateValue::Put(vec![1]).encode().unwrap()),
            })
            .collect();
        observe(
            &mut validation,
            manifest.roots,
            manifest.roots,
            &changes[..2],
            manifest,
            &genesis,
        );
        assert_eq!(validation.pending.as_ref().unwrap().keys.len(), 2);
        let mut next = manifest.clone();
        next.checkpoint_sequence = 2;
        published(&mut validation, manifest.roots, &next, [None; 4]);
        assert_eq!(validation.pending.as_ref().unwrap().keys.len(), 1);
        observe(
            &mut validation,
            manifest.roots,
            manifest.roots,
            &changes,
            &next,
            &genesis,
        );
        assert!(validation.pending.is_none());
        published(&mut validation, manifest.roots, &next, [None; 4]);
        assert!(validation.pending.as_ref().unwrap().keys.is_empty());
    }
}
