use proc_macro2::Span;
use syn::Result;

use crate::types::Type;
use crate::types::VALUE_LIMIT;

pub struct Instruction {
    pub opcode: u8,
    pub operands: Vec<u8>,
    pub table: Option<(usize, u16)>,
}

pub struct Encoded {
    pub bytes: Vec<u8>,
    pub table_offset: usize,
    pub patches: Vec<(usize, u16)>,
}

pub fn pool_index(
    len: usize,
    span: Span,
) -> Result<u16> {
    if len >= 65_535 {
        Err(syn::Error::new(
            span,
            "program exceeds an ISA 1 pool or instruction limit",
        ))
    } else {
        Ok(len as u16)
    }
}

pub fn encode(
    result: &Type,
    arguments: &[Type],
    registers: &[Type],
    table_count: usize,
    constants: &[(Type, Vec<u8>)],
    instructions: &[Instruction],
) -> Result<Encoded> {
    let span = Span::call_site();
    for count in [
        arguments.len(),
        registers.len(),
        table_count,
        constants.len(),
        instructions.len(),
    ] {
        if count > 65_535 {
            return Err(syn::Error::new(
                span,
                "program exceeds an ISA 1 pool or instruction limit",
            ));
        }
    }
    let mut bytes = b"BLOPVM01".to_vec();
    bytes.extend(1u16.to_le_bytes());
    bytes.extend(0u16.to_le_bytes());
    bytes.extend(0u32.to_le_bytes());
    for count in [
        registers.len(),
        arguments.len(),
        table_count,
        constants.len(),
    ] {
        bytes.extend((count as u16).to_le_bytes());
    }
    bytes.extend((instructions.len() as u32).to_le_bytes());
    bytes.extend(0u32.to_le_bytes());
    blob(&result.descriptor(), &mut bytes)?;
    let table_offset = bytes.len();
    bytes.resize(bytes.len() + 8 * table_count, 0);
    for ty in arguments.iter().chain(registers) {
        blob(&ty.descriptor(), &mut bytes)?;
    }
    for (ty, value) in constants {
        blob(&ty.descriptor(), &mut bytes)?;
        blob(value, &mut bytes)?;
    }
    let mut patches = Vec::new();
    for instruction in instructions {
        if bytes.len() + 4 + instruction.operands.len() > VALUE_LIMIT as usize {
            return Err(syn::Error::new(span, "program exceeds 16 MiB"));
        }
        bytes.extend([instruction.opcode, 0]);
        bytes.extend((instruction.operands.len() as u16).to_le_bytes());
        if let Some((offset, index)) = instruction.table {
            patches.push((bytes.len() + offset, index));
        }
        bytes.extend(&instruction.operands);
    }
    if bytes.len() > VALUE_LIMIT as usize {
        return Err(syn::Error::new(span, "program exceeds 16 MiB"));
    }
    let length = bytes.len() as u32;
    bytes[12..16].copy_from_slice(&length.to_le_bytes());
    Ok(Encoded {
        bytes,
        table_offset,
        patches,
    })
}

pub fn blob(
    value: &[u8],
    out: &mut Vec<u8>,
) -> Result<()> {
    if value.len() > VALUE_LIMIT as usize || out.len() + 4 + value.len() > VALUE_LIMIT as usize {
        return Err(syn::Error::new(
            Span::call_site(),
            "encoding exceeds 16 MiB",
        ));
    }
    out.extend((value.len() as u32).to_le_bytes());
    out.extend(value);
    Ok(())
}
