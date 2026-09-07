use std::fmt;

const MAX_ARGUMENT_BYTES: usize = 16 * 1024 * 1024;

/// A compiled transaction program and its separately encoded arguments.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Transaction {
    program: Vec<u8>,
    arguments: Vec<u8>,
}

impl Transaction {
    /// Return the complete program container, including its header.
    pub fn program_bytes(&self) -> &[u8] {
        &self.program
    }

    /// Return the encoded arguments, including their count and Blob lengths.
    pub fn argument_bytes(&self) -> &[u8] {
        &self.arguments
    }

    /// Take ownership of the program bytes and argument bytes, in that order.
    pub fn into_parts(self) -> (Vec<u8>, Vec<u8>) {
        (self.program, self.arguments)
    }
}

/// A failure to bind runtime inputs to a compiled transaction program.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BuildError {
    /// A captured byte or string value exceeds its declared byte bound.
    BoundExceeded { max_bytes: u32, actual_bytes: usize },
    /// The complete encoded arguments would exceed 16 MiB.
    ArgumentsTooLarge,
    /// A table ID is zero or the reserved value `u64::MAX`.
    InvalidTableId(u64),
    /// Separate table declarations were bound to the same table ID.
    DuplicateTableId(u64),
    /// The compiler supplied invalid table counts, offsets or patch indices.
    InvalidTemplate,
}

impl fmt::Display for BuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BoundExceeded {
                max_bytes,
                actual_bytes,
            } => write!(
                f,
                "value is {actual_bytes} bytes, exceeding the bound of {max_bytes} bytes"
            ),
            Self::ArgumentsTooLarge => f.write_str("encoded arguments exceed the 16 MiB limit"),
            Self::InvalidTableId(id) => write!(f, "invalid table ID {id}: the ID is reserved"),
            Self::DuplicateTableId(id) => {
                write!(f, "table ID {id} is bound to separate table declarations")
            }
            Self::InvalidTemplate => {
                f.write_str("compiler-generated program template or table patches are invalid")
            }
        }
    }
}

impl std::error::Error for BuildError {}

/// Append one length-prefixed Blob without changing `out` on error.
pub fn push_blob(out: &mut Vec<u8>, bytes: &[u8], max_bytes: u32) -> Result<(), BuildError> {
    let length = u32::try_from(bytes.len())
        .ok()
        .filter(|&length| length <= max_bytes)
        .ok_or(BuildError::BoundExceeded {
            max_bytes,
            actual_bytes: bytes.len(),
        })?;
    out.len()
        .checked_add(4)
        .and_then(|length| length.checked_add(bytes.len()))
        .filter(|&length| length <= MAX_ARGUMENT_BYTES)
        .ok_or(BuildError::ArgumentsTooLarge)?;

    out.extend_from_slice(&length.to_le_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

/// Bind table IDs to a compiler-validated template and attach encoded arguments.
///
/// Each patch contains an absolute byte offset and an original table declaration
/// index. Only the table array and these u16 operands are rewritten. Checks here
/// protect binding ranges and indices; they do not validate bytecode or arguments.
pub fn bind_program(
    template: &'static [u8],
    table_offset: usize,
    patches: &[(usize, u16)],
    table_ids: &[u64],
    arguments: Vec<u8>,
) -> Result<Transaction, BuildError> {
    if arguments.len() > MAX_ARGUMENT_BYTES {
        return Err(BuildError::ArgumentsTooLarge);
    }
    if table_ids.len() > usize::from(u16::MAX) {
        return Err(BuildError::InvalidTemplate);
    }
    let table_bytes = table_ids
        .len()
        .checked_mul(8)
        .ok_or(BuildError::InvalidTemplate)?;
    let table_end = table_offset
        .checked_add(table_bytes)
        .filter(|&end| end <= template.len())
        .ok_or(BuildError::InvalidTemplate)?;

    for &id in table_ids {
        if id == 0 || id == u64::MAX {
            return Err(BuildError::InvalidTableId(id));
        }
    }
    let mut sorted_tables: Vec<_> = table_ids.iter().copied().enumerate().collect();
    sorted_tables.sort_unstable_by_key(|&(_, id)| id);
    for pair in sorted_tables.windows(2) {
        if pair[0].1 == pair[1].1 {
            return Err(BuildError::DuplicateTableId(pair[0].1));
        }
    }

    let mut sorted_indices = vec![0_u16; table_ids.len()];
    let mut program = template.to_vec();
    for (slot, (bytes, &(declaration, id))) in program[table_offset..table_end]
        .as_chunks_mut::<8>()
        .0
        .iter_mut()
        .zip(&sorted_tables)
        .enumerate()
    {
        bytes.copy_from_slice(&id.to_le_bytes());
        sorted_indices[declaration] = slot as u16;
    }
    for &(offset, declaration) in patches {
        let slot = sorted_indices
            .get(usize::from(declaration))
            .ok_or(BuildError::InvalidTemplate)?;
        let end = offset.checked_add(2).ok_or(BuildError::InvalidTemplate)?;
        if offset < table_end {
            return Err(BuildError::InvalidTemplate);
        }
        let operand = program
            .get_mut(offset..end)
            .ok_or(BuildError::InvalidTemplate)?;
        operand.copy_from_slice(&slot.to_le_bytes());
    }

    Ok(Transaction { program, arguments })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blobs_use_exact_little_endian_lengths_and_preserve_the_prefix() {
        let mut arguments = 2_u32.to_le_bytes().to_vec();
        push_blob(&mut arguments, &[0, 0xff, 0x80], 3).unwrap();
        push_blob(&mut arguments, &[], 0).unwrap();
        assert_eq!(
            arguments,
            [2, 0, 0, 0, 3, 0, 0, 0, 0, 0xff, 0x80, 0, 0, 0, 0]
        );

        let mut blob = Vec::new();
        push_blob(&mut blob, &[0x7f; 256], 256).unwrap();
        assert_eq!(&blob[..4], &[0, 1, 0, 0]);
        assert_eq!(&blob[4..], &[0x7f; 256]);
    }

    #[test]
    fn blob_bound_errors_leave_output_unchanged() {
        let mut output = vec![1, 2, 3];
        assert_eq!(
            push_blob(&mut output, b"abcd", 3),
            Err(BuildError::BoundExceeded {
                max_bytes: 3,
                actual_bytes: 4,
            })
        );
        assert_eq!(output, [1, 2, 3]);
        assert_eq!(
            push_blob(&mut output, b"a", 0),
            Err(BuildError::BoundExceeded {
                max_bytes: 0,
                actual_bytes: 1,
            })
        );
        assert_eq!(output, [1, 2, 3]);
    }

    #[test]
    fn blob_total_limit_includes_prefix_and_is_checked_before_mutation() {
        let mut output = vec![0x55; MAX_ARGUMENT_BYTES - 5];
        push_blob(&mut output, b"x", 1).unwrap();
        assert_eq!(output.len(), MAX_ARGUMENT_BYTES);
        assert_eq!(&output[MAX_ARGUMENT_BYTES - 5..], &[1, 0, 0, 0, b'x']);
        let original = output.clone();
        assert_eq!(
            push_blob(&mut output, &[], 0),
            Err(BuildError::ArgumentsTooLarge)
        );
        assert_eq!(output, original);

        output.truncate(MAX_ARGUMENT_BYTES - 4);
        let original = output.clone();
        assert_eq!(
            push_blob(&mut output, b"x", 1),
            Err(BuildError::ArgumentsTooLarge)
        );
        assert_eq!(output, original);
        push_blob(&mut output, &[], 0).unwrap();
        assert_eq!(output.len(), MAX_ARGUMENT_BYTES);
    }

    #[test]
    fn binding_sorts_ids_and_remaps_each_declaration_without_changing_other_bytes() {
        static TEMPLATE: [u8; 42] = [0xa5; 42];
        let patches = [(30, 0), (34, 1), (36, 2), (40, 0)];
        let arguments = vec![1, 0, 0, 0, 1, 0, 0, 0, 0x7f];
        let transaction =
            bind_program(&TEMPLATE, 4, &patches, &[90, 10, 50], arguments.clone()).unwrap();

        let mut expected = TEMPLATE.to_vec();
        expected[4..12].copy_from_slice(&10_u64.to_le_bytes());
        expected[12..20].copy_from_slice(&50_u64.to_le_bytes());
        expected[20..28].copy_from_slice(&90_u64.to_le_bytes());
        expected[30..32].copy_from_slice(&2_u16.to_le_bytes());
        expected[34..36].copy_from_slice(&0_u16.to_le_bytes());
        expected[36..38].copy_from_slice(&1_u16.to_le_bytes());
        expected[40..42].copy_from_slice(&2_u16.to_le_bytes());
        assert_eq!(transaction.program_bytes(), expected);
        assert_eq!(transaction.argument_bytes(), arguments);
        assert_eq!(transaction.clone(), transaction);
        assert_eq!(transaction.into_parts(), (expected, arguments));
        assert_eq!(TEMPLATE, [0xa5; 42]);

        let rebound = bind_program(&TEMPLATE, 4, &patches, &[10, 50, 90], vec![]).unwrap();
        assert_eq!(&rebound.program_bytes()[30..32], &[0, 0]);
        assert_eq!(&rebound.program_bytes()[34..36], &[1, 0]);
        assert_eq!(&rebound.program_bytes()[36..38], &[2, 0]);
        assert_eq!(TEMPLATE, [0xa5; 42]);
    }

    #[test]
    fn binding_rejects_reserved_and_duplicate_ids() {
        for id in [0, u64::MAX] {
            assert_eq!(
                bind_program(&[0; 8], 0, &[], &[id], vec![]),
                Err(BuildError::InvalidTableId(id))
            );
        }
        assert_eq!(
            bind_program(&[0; 24], 0, &[], &[9, 1, 9], vec![]),
            Err(BuildError::DuplicateTableId(9))
        );
        let transaction = bind_program(&[0; 16], 0, &[], &[u64::MAX - 1, 1], vec![]).unwrap();
        assert_eq!(&transaction.program_bytes()[..8], &1_u64.to_le_bytes());
        assert_eq!(
            &transaction.program_bytes()[8..],
            &(u64::MAX - 1).to_le_bytes()
        );
    }

    #[test]
    fn binding_checks_argument_limit_and_supports_no_tables() {
        let arguments = vec![0; MAX_ARGUMENT_BYTES];
        let transaction = bind_program(b"unchanged", 9, &[], &[], arguments.clone()).unwrap();
        assert_eq!(transaction.into_parts(), (b"unchanged".to_vec(), arguments));
        assert_eq!(
            bind_program(b"unchanged", 9, &[], &[], vec![0; MAX_ARGUMENT_BYTES + 1]),
            Err(BuildError::ArgumentsTooLarge)
        );
    }

    #[test]
    fn binding_rejects_invalid_table_ranges_without_panicking() {
        for offset in [9, usize::MAX] {
            assert_eq!(
                bind_program(&[0; 16], offset, &[], &[1], vec![]),
                Err(BuildError::InvalidTemplate)
            );
        }
        assert_eq!(
            bind_program(&[], 1, &[], &[], vec![]),
            Err(BuildError::InvalidTemplate)
        );
        assert_eq!(
            bind_program(&[], 0, &[], &vec![1; usize::from(u16::MAX) + 1], vec![]),
            Err(BuildError::InvalidTemplate)
        );
    }

    #[test]
    fn binding_rejects_invalid_patches_without_changing_the_static_template() {
        static TEMPLATE: [u8; 16] = [0xa5; 16];
        for patch in [(15, 0), (16, 0), (usize::MAX, 0), (8, 1), (7, 0), (0, 0)] {
            assert_eq!(
                bind_program(&TEMPLATE, 0, &[(8, 0), patch], &[1], vec![]),
                Err(BuildError::InvalidTemplate)
            );
            assert_eq!(TEMPLATE, [0xa5; 16]);
        }
        assert_eq!(
            bind_program(&TEMPLATE, 0, &[(0, 0)], &[], vec![]),
            Err(BuildError::InvalidTemplate)
        );
    }
}
