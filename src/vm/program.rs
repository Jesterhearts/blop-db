use super::Error;
use super::Result;
use super::Table;
use super::Type;
use super::Value;
use super::value::decode_value;
use crate::storage::LimitPolicy;

const MAX_PROGRAM_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug)]
pub(super) struct Program {
    pub(super) result_type: Type,
    pub(super) tables: Vec<Table>,
    pub(super) arguments: Vec<Value>,
    pub(super) register_types: Vec<Type>,
    pub(super) constants: Vec<Value>,
    pub(super) instructions: Vec<Instruction>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum Instruction {
    Const {
        dst: usize,
        constant: usize,
    },
    Arg {
        dst: usize,
        argument: usize,
    },
    Move {
        dst: usize,
        src: usize,
    },
    Binary {
        op: BinaryOp,
        dst: usize,
        left: usize,
        right: usize,
    },
    Unary {
        op: UnaryOp,
        dst: usize,
        src: usize,
    },
    Load {
        dst: usize,
        table: usize,
        key: usize,
    },
    Exists {
        dst: usize,
        table: usize,
        key: usize,
    },
    Insert {
        table: usize,
        key: usize,
        value: usize,
    },
    Store {
        table: usize,
        key: usize,
        value: usize,
    },
    Delete {
        table: usize,
        key: usize,
    },
    Scan {
        dst: usize,
        table: usize,
        lower: Option<usize>,
        upper: Option<usize>,
        flags: u8,
        row_limit: u32,
        byte_limit: u64,
    },
    Slice {
        dst: usize,
        src: usize,
        start: usize,
        length: usize,
    },
    Tuple {
        dst: usize,
        fields: Vec<usize>,
    },
    Field {
        dst: usize,
        src: usize,
        field: usize,
    },
    Jump {
        target: usize,
    },
    JumpIfFalse {
        condition: usize,
        target: usize,
    },
    Require {
        condition: usize,
        user_code: u32,
    },
    Abort {
        user_code: u32,
    },
    Return {
        src: usize,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Rem,
    Eq,
    Lt,
    Le,
    Gt,
    Ge,
    BoolAnd,
    BoolOr,
    BoolXor,
    BitAnd,
    BitOr,
    BitXor,
    Shl,
    Shr,
    RowsKey,
    RowsValue,
    Concat,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum UnaryOp {
    Neg,
    ToI64,
    ToU64,
    BoolNot,
    BitNot,
    RowsLen,
    ByteLen,
    Utf8Bytes,
    ParseUtf8,
    Sha256,
}

struct Header {
    registers: usize,
    arguments: usize,
    tables: usize,
    constants: usize,
    instructions: usize,
}

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(
        &mut self,
        length: usize,
    ) -> Result<&'a [u8]> {
        let (bytes, rest) = self
            .0
            .split_at_checked(length)
            .ok_or(Error::Invalid("truncated program or arguments"))?;
        self.0 = rest;
        Ok(bytes)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<usize> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()) as usize)
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn blob(&mut self) -> Result<&'a [u8]> {
        let length = self.u32()? as usize;
        self.take(length)
    }

    fn finish(self) -> Result<()> {
        if !self.0.is_empty() {
            return Err(Error::Invalid("trailing program or argument bytes"));
        }
        Ok(())
    }
}

fn header(bytes: &[u8]) -> Result<(Header, Reader<'_>)> {
    if bytes.len() > MAX_PROGRAM_BYTES {
        return Err(Error::Invalid("program_bytes exceeds hard ceiling"));
    }
    let mut reader = Reader(bytes);
    if reader.take(8)? != b"BLOPVM01" {
        return Err(Error::Invalid("invalid program magic"));
    }
    let version = reader.u16()? as u16;
    if version != 1 {
        return Err(Error::Unsupported {
            format: "ISA",
            version,
        });
    }
    if reader.u16()? != 0 {
        return Err(Error::Invalid("nonzero program flags"));
    }
    if reader.u32()? as usize != bytes.len() {
        return Err(Error::Invalid("invalid total program length"));
    }
    let header = Header {
        registers: reader.u16()?,
        arguments: reader.u16()?,
        tables: reader.u16()?,
        constants: reader.u16()?,
        instructions: reader.u32()? as usize,
    };
    if !(1..=65_535).contains(&header.instructions) {
        return Err(Error::Invalid(
            "instruction count must be from 1 through 65535",
        ));
    }
    if reader.u32()? != 0 {
        return Err(Error::Invalid("nonzero program reserved field"));
    }
    Ok((header, reader))
}

fn read_tables(
    reader: &mut Reader<'_>,
    count: usize,
) -> Result<Vec<u64>> {
    let mut ids = Vec::with_capacity(count);
    for _ in 0..count {
        let id = reader.u64()?;
        if id == 0 || id == u64::MAX || ids.last().is_some_and(|last| *last >= id) {
            return Err(Error::Invalid(
                "table IDs must be valid and strictly increasing",
            ));
        }
        ids.push(id);
    }
    Ok(ids)
}

pub(super) fn table_ids(program_bytes: &[u8]) -> Result<Vec<u64>> {
    let (header, mut reader) = header(program_bytes)?;
    Type::decode(reader.blob()?)?;
    read_tables(&mut reader, header.tables)
}

pub(super) fn decode(
    program_bytes: &[u8],
    argument_bytes: &[u8],
    tables: &[Table],
    claims: &LimitPolicy,
) -> Result<Program> {
    let (header, mut reader) = header(program_bytes)?;
    // LimitPolicy construction already enforces every resource's hard ceiling.
    let limits = claims.values();
    for (usage, limit, reason) in [
        (
            program_bytes.len(),
            limits[0],
            "program_bytes exceeds claim",
        ),
        (header.instructions, limits[1], "instructions exceeds claim"),
        (header.registers, limits[2], "registers exceeds claim"),
        (header.arguments, limits[3], "arguments exceeds claim"),
        (
            argument_bytes.len(),
            limits[4],
            "argument_bytes exceeds claim",
        ),
        (header.tables, limits[5], "tables exceeds claim"),
    ] {
        if usage as u64 > limit {
            return Err(Error::Invalid(reason));
        }
    }
    let result_type = Type::decode(reader.blob()?)?;
    let ids = read_tables(&mut reader, header.tables)?;
    if !ids.iter().copied().eq(tables.iter().map(|table| table.id)) {
        return Err(Error::Invalid(
            "resolved tables do not match program table IDs",
        ));
    }
    let mut argument_types = Vec::with_capacity(header.arguments);
    for _ in 0..header.arguments {
        let ty = Type::decode(reader.blob()?)?;
        if matches!(ty, Type::Rows { .. }) {
            return Err(Error::Invalid("Rows cannot be an argument"));
        }
        argument_types.push(ty);
    }
    let mut register_types = Vec::with_capacity(header.registers);
    for _ in 0..header.registers {
        register_types.push(Type::decode(reader.blob()?)?);
    }
    let mut constant_types = Vec::with_capacity(header.constants);
    let mut constants = Vec::with_capacity(header.constants);
    for _ in 0..header.constants {
        let ty = Type::decode(reader.blob()?)?;
        if matches!(ty, Type::Rows { .. }) {
            return Err(Error::Invalid("Rows cannot be a constant"));
        }
        let bytes = reader.blob()?;
        if bytes.len() as u64 > limits[10] {
            return Err(Error::Invalid("constant value_bytes exceeds claim"));
        }
        constants.push(decode_value(&ty, bytes)?);
        constant_types.push(ty);
    }
    let instructions = read_instructions(&mut reader, header.instructions)?;
    reader.finish()?;

    let mut reader = Reader(argument_bytes);
    if reader.u32()? as usize != header.arguments {
        return Err(Error::Invalid(
            "supplied argument count does not match program",
        ));
    }
    let mut arguments = Vec::with_capacity(header.arguments);
    for ty in &argument_types {
        let bytes = reader.blob()?;
        if bytes.len() as u64 > limits[10] {
            return Err(Error::Invalid("argument value_bytes exceeds claim"));
        }
        arguments.push(decode_value(ty, bytes)?);
    }
    reader.finish()?;
    let program = Program {
        result_type,
        tables: tables.to_vec(),
        arguments,
        register_types,
        constants,
        instructions,
    };
    verify(&program, &argument_types, &constant_types, claims)?;
    Ok(program)
}

fn read_instructions(
    reader: &mut Reader<'_>,
    count: usize,
) -> Result<Vec<Instruction>> {
    let mut instructions = Vec::with_capacity(count);
    for _ in 0..count {
        let opcode = reader.u8()?;
        if reader.u8()? != 0 {
            return Err(Error::Invalid("nonzero instruction flags"));
        }
        let length = reader.u16()?;
        let mut operands = Reader(reader.take(length)?);
        let instruction = match opcode {
            0x01 => Instruction::Const {
                dst: operands.u16()?,
                constant: operands.u16()?,
            },
            0x02 => Instruction::Arg {
                dst: operands.u16()?,
                argument: operands.u16()?,
            },
            0x03 => Instruction::Move {
                dst: operands.u16()?,
                src: operands.u16()?,
            },
            0x10..=0x14
            | 0x20..=0x24
            | 0x28..=0x2a
            | 0x30..=0x32
            | 0x34..=0x35
            | 0x4a..=0x4b
            | 0x51 => {
                let op = match opcode {
                    0x10 => BinaryOp::Add,
                    0x11 => BinaryOp::Sub,
                    0x12 => BinaryOp::Mul,
                    0x13 => BinaryOp::Div,
                    0x14 => BinaryOp::Rem,
                    0x20 => BinaryOp::Eq,
                    0x21 => BinaryOp::Lt,
                    0x22 => BinaryOp::Le,
                    0x23 => BinaryOp::Gt,
                    0x24 => BinaryOp::Ge,
                    0x28 => BinaryOp::BoolAnd,
                    0x29 => BinaryOp::BoolOr,
                    0x2a => BinaryOp::BoolXor,
                    0x30 => BinaryOp::BitAnd,
                    0x31 => BinaryOp::BitOr,
                    0x32 => BinaryOp::BitXor,
                    0x34 => BinaryOp::Shl,
                    0x35 => BinaryOp::Shr,
                    0x4a => BinaryOp::RowsKey,
                    0x4b => BinaryOp::RowsValue,
                    0x51 => BinaryOp::Concat,
                    _ => return Err(Error::Invalid("unknown ISA 1 opcode")),
                };
                Instruction::Binary {
                    op,
                    dst: operands.u16()?,
                    left: operands.u16()?,
                    right: operands.u16()?,
                }
            }
            0x15..=0x17 | 0x2b | 0x33 | 0x49 | 0x50 | 0x53..=0x55 => {
                let op = match opcode {
                    0x15 => UnaryOp::Neg,
                    0x16 => UnaryOp::ToI64,
                    0x17 => UnaryOp::ToU64,
                    0x2b => UnaryOp::BoolNot,
                    0x33 => UnaryOp::BitNot,
                    0x49 => UnaryOp::RowsLen,
                    0x50 => UnaryOp::ByteLen,
                    0x53 => UnaryOp::Utf8Bytes,
                    0x54 => UnaryOp::ParseUtf8,
                    0x55 => UnaryOp::Sha256,
                    _ => return Err(Error::Invalid("unknown ISA 1 opcode")),
                };
                Instruction::Unary {
                    op,
                    dst: operands.u16()?,
                    src: operands.u16()?,
                }
            }
            0x40 => Instruction::Load {
                dst: operands.u16()?,
                table: operands.u16()?,
                key: operands.u16()?,
            },
            0x41 => Instruction::Exists {
                dst: operands.u16()?,
                table: operands.u16()?,
                key: operands.u16()?,
            },
            0x42 => Instruction::Insert {
                table: operands.u16()?,
                key: operands.u16()?,
                value: operands.u16()?,
            },
            0x43 => Instruction::Store {
                table: operands.u16()?,
                key: operands.u16()?,
                value: operands.u16()?,
            },
            0x44 => Instruction::Delete {
                table: operands.u16()?,
                key: operands.u16()?,
            },
            0x48 => {
                let dst = operands.u16()?;
                let table = operands.u16()?;
                let lower = operands.u16()?;
                let upper = operands.u16()?;
                let lower = (lower != 0xffff).then_some(lower);
                let upper = (upper != 0xffff).then_some(upper);
                let flags = operands.u8()?;
                if flags & !3 != 0 || operands.u8()? != 0 {
                    return Err(Error::Invalid("invalid scan flags or reserved field"));
                }
                if (lower.is_none() && flags & 1 != 0) || (upper.is_none() && flags & 2 != 0) {
                    return Err(Error::Invalid("absent scan endpoint cannot be inclusive"));
                }
                Instruction::Scan {
                    dst,
                    table,
                    lower,
                    upper,
                    flags,
                    row_limit: operands.u32()?,
                    byte_limit: operands.u64()?,
                }
            }
            0x52 => Instruction::Slice {
                dst: operands.u16()?,
                src: operands.u16()?,
                start: operands.u16()?,
                length: operands.u16()?,
            },
            0x58 => {
                let dst = operands.u16()?;
                let count = operands.u16()?;
                if count > 256 {
                    return Err(Error::Invalid("TUPLE exceeds 256 fields"));
                }
                let fields = (0..count).map(|_| operands.u16()).collect::<Result<_>>()?;
                Instruction::Tuple { dst, fields }
            }
            0x59 => Instruction::Field {
                dst: operands.u16()?,
                src: operands.u16()?,
                field: operands.u16()?,
            },
            0x60 => Instruction::Jump {
                target: operands.u32()? as usize,
            },
            0x61 => Instruction::JumpIfFalse {
                condition: operands.u16()?,
                target: operands.u32()? as usize,
            },
            0x62 => Instruction::Require {
                condition: operands.u16()?,
                user_code: operands.u32()?,
            },
            0x63 => Instruction::Abort {
                user_code: operands.u32()?,
            },
            0x64 => Instruction::Return {
                src: operands.u16()?,
            },
            _ => return Err(Error::Invalid("unknown ISA 1 opcode")),
        };
        if !operands.0.is_empty() {
            return Err(Error::Invalid("incorrect instruction operand length"));
        }
        instructions.push(instruction);
    }
    Ok(instructions)
}

fn same_shape(
    a: &Type,
    b: &Type,
) -> Result<()> {
    if !a.same_shape(b) {
        return Err(Error::Invalid("instruction type shape mismatch"));
    }
    Ok(())
}

fn integer(ty: &Type) -> Result<()> {
    if !matches!(ty, Type::I64 | Type::U64) {
        return Err(Error::Invalid("instruction requires an integer"));
    }
    Ok(())
}

fn verify_instruction(
    instruction: &Instruction,
    program: &Program,
    argument_types: &[Type],
    constant_types: &[Type],
    initialized: &[u64],
    claims: &LimitPolicy,
) -> Result<Option<usize>> {
    let register = |index| {
        program
            .register_types
            .get(index)
            .ok_or(Error::Invalid("register index out of bounds"))
    };
    let source = |index| {
        let ty = register(index)?;
        if initialized[index / 64] & (1u64 << (index % 64)) == 0 {
            return Err(Error::Invalid(
                "source register is not definitely initialized",
            ));
        }
        Ok(ty)
    };
    let table = |index| {
        program
            .tables
            .get(index)
            .ok_or(Error::Invalid("table index out of bounds"))
    };
    let destination = match instruction {
        Instruction::Const { dst, constant } => {
            let ty = constant_types
                .get(*constant)
                .ok_or(Error::Invalid("constant index out of bounds"))?;
            same_shape(register(*dst)?, ty)?;
            Some(*dst)
        }
        Instruction::Arg { dst, argument } => {
            let ty = argument_types
                .get(*argument)
                .ok_or(Error::Invalid("argument index out of bounds"))?;
            same_shape(register(*dst)?, ty)?;
            Some(*dst)
        }
        Instruction::Move { dst, src } => {
            same_shape(register(*dst)?, source(*src)?)?;
            Some(*dst)
        }
        Instruction::Binary {
            op,
            dst,
            left,
            right,
        } => {
            let destination = register(*dst)?;
            let left = source(*left)?;
            let right = source(*right)?;
            match op {
                BinaryOp::Add
                | BinaryOp::Sub
                | BinaryOp::Mul
                | BinaryOp::Div
                | BinaryOp::Rem
                | BinaryOp::BitAnd
                | BinaryOp::BitOr
                | BinaryOp::BitXor => {
                    integer(destination)?;
                    same_shape(destination, left)?;
                    same_shape(destination, right)?;
                }
                BinaryOp::Eq | BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge => {
                    same_shape(destination, &Type::Boolean)?;
                    same_shape(left, right)?;
                    if matches!(left, Type::Rows { .. }) {
                        return Err(Error::Invalid("Rows cannot be compared"));
                    }
                }
                BinaryOp::BoolAnd | BinaryOp::BoolOr | BinaryOp::BoolXor => {
                    same_shape(destination, &Type::Boolean)?;
                    same_shape(left, &Type::Boolean)?;
                    same_shape(right, &Type::Boolean)?;
                }
                BinaryOp::Shl | BinaryOp::Shr => {
                    integer(destination)?;
                    same_shape(destination, left)?;
                    same_shape(right, &Type::U64)?;
                }
                BinaryOp::RowsKey | BinaryOp::RowsValue => {
                    let Type::Rows { key, value, .. } = left else {
                        return Err(Error::Invalid("Rows instruction requires a Rows source"));
                    };
                    same_shape(right, &Type::U64)?;
                    if *op == BinaryOp::RowsKey {
                        same_shape(destination, key)?;
                    } else {
                        same_shape(destination, value)?;
                    }
                }
                BinaryOp::Concat => {
                    if !matches!(destination, Type::Bytes(_) | Type::String(_)) {
                        return Err(Error::Invalid("CONCAT requires Bytes or String"));
                    }
                    same_shape(destination, left)?;
                    same_shape(destination, right)?;
                }
            }
            Some(*dst)
        }
        Instruction::Unary { op, dst, src } => {
            let destination = register(*dst)?;
            let source = source(*src)?;
            match op {
                UnaryOp::Neg => {
                    same_shape(destination, &Type::I64)?;
                    same_shape(source, &Type::I64)?;
                }
                UnaryOp::ToI64 => {
                    same_shape(destination, &Type::I64)?;
                    same_shape(source, &Type::U64)?;
                }
                UnaryOp::ToU64 => {
                    same_shape(destination, &Type::U64)?;
                    same_shape(source, &Type::I64)?;
                }
                UnaryOp::BoolNot => {
                    same_shape(destination, &Type::Boolean)?;
                    same_shape(source, &Type::Boolean)?;
                }
                UnaryOp::BitNot => {
                    integer(destination)?;
                    same_shape(destination, source)?;
                }
                UnaryOp::RowsLen => {
                    same_shape(destination, &Type::U64)?;
                    if !matches!(source, Type::Rows { .. }) {
                        return Err(Error::Invalid("Rows instruction requires a Rows source"));
                    }
                }
                UnaryOp::ByteLen => {
                    same_shape(destination, &Type::U64)?;
                    if !matches!(source, Type::Bytes(_) | Type::String(_)) {
                        return Err(Error::Invalid("BYTE_LEN requires Bytes or String"));
                    }
                }
                UnaryOp::Utf8Bytes => {
                    same_shape(destination, &Type::Bytes(0))?;
                    same_shape(source, &Type::String(0))?;
                }
                UnaryOp::ParseUtf8 => {
                    same_shape(destination, &Type::String(0))?;
                    same_shape(source, &Type::Bytes(0))?;
                }
                UnaryOp::Sha256 => {
                    same_shape(destination, &Type::Bytes(0))?;
                    same_shape(source, &Type::Bytes(0))?;
                }
            }
            Some(*dst)
        }
        Instruction::Load {
            dst,
            table: index,
            key,
        } => {
            let table = table(*index)?;
            same_shape(source(*key)?, &table.key)?;
            same_shape(register(*dst)?, &table.value)?;
            Some(*dst)
        }
        Instruction::Exists {
            dst,
            table: index,
            key,
        } => {
            same_shape(source(*key)?, &table(*index)?.key)?;
            same_shape(register(*dst)?, &Type::Boolean)?;
            Some(*dst)
        }
        Instruction::Insert {
            table: index,
            key,
            value,
        }
        | Instruction::Store {
            table: index,
            key,
            value,
        } => {
            let table = table(*index)?;
            same_shape(&table.key, source(*key)?)?;
            same_shape(&table.value, source(*value)?)?;
            None
        }
        Instruction::Delete { table: index, key } => {
            same_shape(&table(*index)?.key, source(*key)?)?;
            None
        }
        Instruction::Scan {
            dst,
            table: index,
            lower,
            upper,
            row_limit,
            byte_limit,
            ..
        } => {
            if u64::from(*row_limit) > claims.values()[12] {
                return Err(Error::Invalid("scan range_rows exceeds claim"));
            }
            if *byte_limit > claims.values()[13] {
                return Err(Error::Invalid("scan range_bytes exceeds claim"));
            }
            let table = table(*index)?;
            let Type::Rows { key, value, .. } = register(*dst)? else {
                return Err(Error::Invalid("scan destination must be Rows"));
            };
            same_shape(key, &table.key)?;
            same_shape(value, &table.value)?;
            for endpoint in [lower, upper].into_iter().flatten() {
                same_shape(source(*endpoint)?, &table.key)?;
            }
            Some(*dst)
        }
        Instruction::Slice {
            dst,
            src,
            start,
            length,
        } => {
            same_shape(register(*dst)?, &Type::Bytes(0))?;
            same_shape(source(*src)?, &Type::Bytes(0))?;
            same_shape(source(*start)?, &Type::U64)?;
            same_shape(source(*length)?, &Type::U64)?;
            Some(*dst)
        }
        Instruction::Tuple { dst, fields } => {
            let Type::Tuple(types) = register(*dst)? else {
                return Err(Error::Invalid("TUPLE destination must be a tuple"));
            };
            if types.len() != fields.len() {
                return Err(Error::Invalid("TUPLE arity does not match destination"));
            }
            for (ty, field) in types.iter().zip(fields) {
                same_shape(ty, source(*field)?)?;
            }
            Some(*dst)
        }
        Instruction::Field { dst, src, field } => {
            let Type::Tuple(fields) = source(*src)? else {
                return Err(Error::Invalid("FIELD source must be a tuple"));
            };
            let ty = fields
                .get(*field)
                .ok_or(Error::Invalid("FIELD index out of bounds"))?;
            same_shape(register(*dst)?, ty)?;
            Some(*dst)
        }
        Instruction::Jump { .. } | Instruction::Abort { .. } => None,
        Instruction::JumpIfFalse { condition, .. } | Instruction::Require { condition, .. } => {
            same_shape(&Type::Boolean, source(*condition)?)?;
            None
        }
        Instruction::Return { src } => {
            same_shape(&program.result_type, source(*src)?)?;
            None
        }
    };
    Ok(destination)
}

fn intersect(
    destination: &mut [u64],
    incoming: &[u64],
) {
    for (destination, incoming) in destination.iter_mut().zip(incoming) {
        *destination &= incoming;
    }
}

fn verify(
    program: &Program,
    argument_types: &[Type],
    constant_types: &[Type],
    claims: &LimitPolicy,
) -> Result<()> {
    let mut pending: Vec<Option<Vec<u64>>> = vec![None; program.instructions.len()];
    let mut fallthrough = Some(vec![0u64; program.register_types.len().div_ceil(64)]);
    // Forward-only edges make instruction order topological. Keep bitsets only
    // for pending targets, and intersect both branch paths even when the
    // supplied inputs would predict one outcome.
    for (index, instruction) in program.instructions.iter().enumerate() {
        let mut initialized = match (fallthrough.take(), pending[index].take()) {
            (Some(mut current), Some(incoming)) => {
                intersect(&mut current, &incoming);
                current
            }
            (Some(current), None) | (None, Some(current)) => current,
            (None, None) => return Err(Error::Invalid("unreachable instruction")),
        };
        if let Some(destination) = verify_instruction(
            instruction,
            program,
            argument_types,
            constant_types,
            &initialized,
            claims,
        )? {
            initialized[destination / 64] |= 1u64 << (destination % 64);
        }
        match instruction {
            Instruction::Jump { target } | Instruction::JumpIfFalse { target, .. } => {
                if *target <= index || *target >= program.instructions.len() {
                    return Err(Error::Invalid(
                        "branch target must be forward and within the program",
                    ));
                }
                if matches!(instruction, Instruction::JumpIfFalse { .. }) {
                    fallthrough = Some(initialized.clone());
                }
                if let Some(incoming) = &mut pending[*target] {
                    intersect(incoming, &initialized);
                } else {
                    pending[*target] = Some(initialized);
                }
            }
            Instruction::Abort { .. } | Instruction::Return { .. } => {}
            Instruction::Const { .. }
            | Instruction::Arg { .. }
            | Instruction::Move { .. }
            | Instruction::Binary { .. }
            | Instruction::Unary { .. }
            | Instruction::Load { .. }
            | Instruction::Exists { .. }
            | Instruction::Insert { .. }
            | Instruction::Store { .. }
            | Instruction::Delete { .. }
            | Instruction::Scan { .. }
            | Instruction::Slice { .. }
            | Instruction::Tuple { .. }
            | Instruction::Field { .. }
            | Instruction::Require { .. } => {
                fallthrough = Some(initialized);
            }
        }
    }
    if fallthrough.is_some() {
        return Err(Error::Invalid(
            "control flow falls off the instruction array",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug)]
    struct WireInstruction {
        opcode: u8,
        operands: Vec<u8>,
    }

    fn claims() -> LimitPolicy {
        LimitPolicy::new([
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
        ])
        .unwrap()
    }

    fn blob(
        bytes: &[u8],
        out: &mut Vec<u8>,
    ) {
        out.extend((bytes.len() as u32).to_le_bytes());
        out.extend(bytes);
    }

    fn arguments(values: &[Vec<u8>]) -> Vec<u8> {
        let mut out = (values.len() as u32).to_le_bytes().to_vec();
        for value in values {
            blob(value, &mut out);
        }
        out
    }

    fn encode(
        result: &Type,
        registers: &[Type],
        arguments: &[Type],
        constants: &[(Type, Vec<u8>)],
        tables: &[u64],
        instructions: &[WireInstruction],
    ) -> Vec<u8> {
        let mut out = b"BLOPVM01".to_vec();
        out.extend(1u16.to_le_bytes());
        out.extend([0; 6]);
        for count in [
            registers.len(),
            arguments.len(),
            tables.len(),
            constants.len(),
        ] {
            out.extend((count as u16).to_le_bytes());
        }
        out.extend((instructions.len() as u32).to_le_bytes());
        out.extend([0; 4]);
        blob(&result.descriptor(), &mut out);
        for table in tables {
            out.extend(table.to_le_bytes());
        }
        for ty in arguments.iter().chain(registers) {
            blob(&ty.descriptor(), &mut out);
        }
        for (ty, value) in constants {
            blob(&ty.descriptor(), &mut out);
            blob(value, &mut out);
        }
        for instruction in instructions {
            out.extend([instruction.opcode, 0]);
            out.extend((instruction.operands.len() as u16).to_le_bytes());
            out.extend(&instruction.operands);
        }
        fix_length(&mut out);
        out
    }

    fn fix_length(bytes: &mut [u8]) {
        let length = bytes.len() as u32;
        bytes[12..16].copy_from_slice(&length.to_le_bytes());
    }

    fn op(
        opcode: u8,
        words: &[u16],
    ) -> WireInstruction {
        WireInstruction {
            opcode,
            operands: words.iter().flat_map(|word| word.to_le_bytes()).collect(),
        }
    }

    fn jump(target: u32) -> WireInstruction {
        WireInstruction {
            opcode: 0x60,
            operands: target.to_le_bytes().to_vec(),
        }
    }

    fn branch(
        source: u16,
        target: u32,
    ) -> WireInstruction {
        let mut instruction = op(0x61, &[source]);
        instruction.operands.extend(target.to_le_bytes());
        instruction
    }

    fn scan(
        destination: u16,
        lo: u16,
        hi: u16,
        flags: u8,
        rows: u32,
        bytes: u64,
    ) -> WireInstruction {
        let mut instruction = op(0x48, &[destination, 0, lo, hi]);
        instruction.operands.extend([flags, 0]);
        instruction.operands.extend(rows.to_le_bytes());
        instruction.operands.extend(bytes.to_le_bytes());
        instruction
    }

    fn row_type() -> Type {
        Type::Rows {
            max_rows: 0,
            key: Box::new(Type::U64),
            value: Box::new(Type::I64),
        }
    }

    fn table() -> Table {
        Table {
            id: 7,
            key: Type::U64,
            value: Type::I64,
        }
    }

    fn zero(ty: &Type) -> Vec<u8> {
        match ty {
            Type::Unit => vec![],
            Type::Boolean => vec![0],
            Type::I64 | Type::U64 => vec![0; 8],
            Type::Bytes(_) | Type::String(_) => vec![0; 4],
            Type::Tuple(fields) => fields.iter().flat_map(zero).collect(),
            Type::Rows { .. } => panic!("Rows cannot be supplied"),
        }
    }

    fn check_case(
        registers: &[Type],
        mut instruction: WireInstruction,
    ) -> Result<Program> {
        let mut argument_types = Vec::new();
        let mut instructions = Vec::new();
        for (register, ty) in registers.iter().enumerate() {
            if matches!(ty, Type::Rows { .. }) {
                instructions.push(scan(register as u16, 0xffff, 0xffff, 0, 0, 0));
            } else {
                instructions.push(op(0x02, &[register as u16, argument_types.len() as u16]));
                argument_types.push(ty.clone());
            }
        }
        if matches!(instruction.opcode, 0x60 | 0x61) {
            let offset = if instruction.opcode == 0x60 { 0 } else { 2 };
            instruction.operands[offset..offset + 4]
                .copy_from_slice(&((instructions.len() + 1) as u32).to_le_bytes());
        }
        let terminal = matches!(instruction.opcode, 0x63 | 0x64);
        instructions.push(instruction);
        if !terminal {
            instructions.push(op(0x63, &[0, 0]));
        }
        let result = if instructions.last().unwrap().opcode == 0x64 {
            &registers[0]
        } else {
            &Type::Unit
        };
        let bytes = encode(
            result,
            registers,
            &argument_types,
            &[(Type::U64, vec![0; 8])],
            &[7],
            &instructions,
        );
        let values: Vec<_> = argument_types.iter().map(zero).collect();
        decode(&bytes, &arguments(&values), &[table()], &claims())
    }

    fn cases() -> Vec<(Vec<Type>, WireInstruction)> {
        use Type::*;
        let mut cases = vec![
            (vec![U64], op(0x01, &[0, 0])),
            (vec![U64], op(0x02, &[0, 0])),
            (vec![Bytes(0), Bytes(8)], op(0x03, &[0, 1])),
            (vec![I64, I64], op(0x15, &[0, 1])),
            (vec![I64, U64], op(0x16, &[0, 1])),
            (vec![U64, I64], op(0x17, &[0, 1])),
            (vec![Boolean, Boolean], op(0x2b, &[0, 1])),
            (vec![I64, U64], op(0x40, &[0, 0, 1])),
            (vec![Boolean, U64], op(0x41, &[0, 0, 1])),
            (vec![U64, I64], op(0x42, &[0, 0, 1])),
            (vec![U64, I64], op(0x43, &[0, 0, 1])),
            (vec![U64], op(0x44, &[0, 0])),
            (vec![row_type(), U64, U64], scan(0, 1, 2, 3, 1, 64)),
            (vec![U64, row_type()], op(0x49, &[0, 1])),
            (vec![U64, row_type(), U64], op(0x4a, &[0, 1, 2])),
            (vec![I64, row_type(), U64], op(0x4b, &[0, 1, 2])),
            (vec![Bytes(0), Bytes(8), U64, U64], op(0x52, &[0, 1, 2, 3])),
            (vec![Bytes(0), String(8)], op(0x53, &[0, 1])),
            (vec![String(0), Bytes(8)], op(0x54, &[0, 1])),
            (vec![Bytes(0), Bytes(8)], op(0x55, &[0, 1])),
            (
                vec![Tuple(vec![U64, Bytes(0)]), U64, Bytes(8)],
                op(0x58, &[0, 2, 1, 2]),
            ),
            (vec![Tuple(vec![])], op(0x58, &[0, 0])),
            (vec![Bytes(0), Tuple(vec![Bytes(8)])], op(0x59, &[0, 1, 0])),
            (vec![], jump(0)),
            (vec![Boolean], branch(0, 0)),
            (vec![Boolean], op(0x62, &[0, 0xffff, 0xffff])),
            (vec![], op(0x63, &[0xffff, 0xffff])),
            (vec![Unit], op(0x64, &[0])),
        ];
        for ty in [I64, U64] {
            for opcode in [0x10, 0x11, 0x12, 0x13, 0x14, 0x30, 0x31, 0x32] {
                cases.push((vec![ty.clone(); 3], op(opcode, &[0, 1, 2])));
            }
            cases.push((vec![ty.clone(); 2], op(0x33, &[0, 1])));
            for opcode in [0x34, 0x35] {
                cases.push((vec![ty.clone(), ty.clone(), U64], op(opcode, &[0, 1, 2])));
            }
        }
        for ty in [
            Unit,
            Boolean,
            I64,
            U64,
            Bytes(8),
            String(8),
            Tuple(vec![U64, String(8)]),
        ] {
            for opcode in 0x20..=0x24 {
                cases.push((
                    vec![Boolean, ty.clone(), ty.clone()],
                    op(opcode, &[0, 1, 2]),
                ));
            }
        }
        for opcode in 0x28..=0x2a {
            cases.push((vec![Boolean; 3], op(opcode, &[0, 1, 2])));
        }
        for ty in [Bytes(8), String(8)] {
            cases.push((vec![U64, ty.clone()], op(0x50, &[0, 1])));
            cases.push((vec![ty; 3], op(0x51, &[0, 1, 2])));
        }
        cases
    }

    fn read_wire(instruction: WireInstruction) -> Instruction {
        let mut bytes = vec![instruction.opcode, 0];
        bytes.extend((instruction.operands.len() as u16).to_le_bytes());
        bytes.extend(instruction.operands);
        read_instructions(&mut Reader(&bytes), 1).unwrap().remove(0)
    }

    #[test]
    fn binary_and_unary_wire_mapping_is_exact() {
        for (opcode, expected) in [
            (0x10, BinaryOp::Add),
            (0x11, BinaryOp::Sub),
            (0x12, BinaryOp::Mul),
            (0x13, BinaryOp::Div),
            (0x14, BinaryOp::Rem),
            (0x20, BinaryOp::Eq),
            (0x21, BinaryOp::Lt),
            (0x22, BinaryOp::Le),
            (0x23, BinaryOp::Gt),
            (0x24, BinaryOp::Ge),
            (0x28, BinaryOp::BoolAnd),
            (0x29, BinaryOp::BoolOr),
            (0x2a, BinaryOp::BoolXor),
            (0x30, BinaryOp::BitAnd),
            (0x31, BinaryOp::BitOr),
            (0x32, BinaryOp::BitXor),
            (0x34, BinaryOp::Shl),
            (0x35, BinaryOp::Shr),
            (0x4a, BinaryOp::RowsKey),
            (0x4b, BinaryOp::RowsValue),
            (0x51, BinaryOp::Concat),
        ] {
            assert_eq!(
                read_wire(op(opcode, &[0x1234, 0x5678, 0x9abc])),
                Instruction::Binary {
                    op: expected,
                    dst: 0x1234,
                    left: 0x5678,
                    right: 0x9abc,
                }
            );
        }
        for (opcode, expected) in [
            (0x15, UnaryOp::Neg),
            (0x16, UnaryOp::ToI64),
            (0x17, UnaryOp::ToU64),
            (0x2b, UnaryOp::BoolNot),
            (0x33, UnaryOp::BitNot),
            (0x49, UnaryOp::RowsLen),
            (0x50, UnaryOp::ByteLen),
            (0x53, UnaryOp::Utf8Bytes),
            (0x54, UnaryOp::ParseUtf8),
            (0x55, UnaryOp::Sha256),
        ] {
            assert_eq!(
                read_wire(op(opcode, &[0x1234, 0x5678])),
                Instruction::Unary {
                    op: expected,
                    dst: 0x1234,
                    src: 0x5678,
                }
            );
        }
    }

    #[test]
    fn instruction_operands_decode_to_named_fields() {
        use Instruction as I;
        for (wire, expected) in [
            (
                op(0x01, &[12, 34]),
                I::Const {
                    dst: 12,
                    constant: 34,
                },
            ),
            (
                op(0x02, &[12, 34]),
                I::Arg {
                    dst: 12,
                    argument: 34,
                },
            ),
            (op(0x03, &[12, 34]), I::Move { dst: 12, src: 34 }),
            (
                op(0x40, &[12, 34, 56]),
                I::Load {
                    dst: 12,
                    table: 34,
                    key: 56,
                },
            ),
            (
                op(0x41, &[12, 34, 56]),
                I::Exists {
                    dst: 12,
                    table: 34,
                    key: 56,
                },
            ),
            (
                op(0x42, &[12, 34, 56]),
                I::Insert {
                    table: 12,
                    key: 34,
                    value: 56,
                },
            ),
            (
                op(0x43, &[12, 34, 56]),
                I::Store {
                    table: 12,
                    key: 34,
                    value: 56,
                },
            ),
            (op(0x44, &[12, 34]), I::Delete { table: 12, key: 34 }),
            (
                scan(12, 34, 56, 3, 0x1234, 0x0102030405060708),
                I::Scan {
                    dst: 12,
                    table: 0,
                    lower: Some(34),
                    upper: Some(56),
                    flags: 3,
                    row_limit: 0x1234,
                    byte_limit: 0x0102030405060708,
                },
            ),
            (
                scan(12, 0xffff, 56, 2, 0, 0),
                I::Scan {
                    dst: 12,
                    table: 0,
                    lower: None,
                    upper: Some(56),
                    flags: 2,
                    row_limit: 0,
                    byte_limit: 0,
                },
            ),
            (
                scan(12, 34, 0xffff, 1, 0, 0),
                I::Scan {
                    dst: 12,
                    table: 0,
                    lower: Some(34),
                    upper: None,
                    flags: 1,
                    row_limit: 0,
                    byte_limit: 0,
                },
            ),
            (
                scan(12, 0xffff, 0xffff, 0, 0, 0),
                I::Scan {
                    dst: 12,
                    table: 0,
                    lower: None,
                    upper: None,
                    flags: 0,
                    row_limit: 0,
                    byte_limit: 0,
                },
            ),
            (
                op(0x52, &[12, 34, 56, 78]),
                I::Slice {
                    dst: 12,
                    src: 34,
                    start: 56,
                    length: 78,
                },
            ),
            (
                op(0x58, &[12, 2, 34, 56]),
                I::Tuple {
                    dst: 12,
                    fields: vec![34, 56],
                },
            ),
            (
                op(0x58, &[12, 0]),
                I::Tuple {
                    dst: 12,
                    fields: vec![],
                },
            ),
            (
                op(0x59, &[12, 34, 56]),
                I::Field {
                    dst: 12,
                    src: 34,
                    field: 56,
                },
            ),
            (jump(0x12345678), I::Jump { target: 0x12345678 }),
            (
                branch(12, 0x12345678),
                I::JumpIfFalse {
                    condition: 12,
                    target: 0x12345678,
                },
            ),
            (
                op(0x62, &[12, 0x5678, 0x1234]),
                I::Require {
                    condition: 12,
                    user_code: 0x12345678,
                },
            ),
            (
                op(0x63, &[0x5678, 0x1234]),
                I::Abort {
                    user_code: 0x12345678,
                },
            ),
            (op(0x64, &[12]), I::Return { src: 12 }),
        ] {
            assert_eq!(read_wire(wire), expected);
        }
    }

    #[test]
    fn decodes_every_opcode_and_checks_operand_layouts() {
        let cases = cases();
        for (registers, instruction) in &cases {
            assert!(
                check_case(registers, instruction.clone()).is_ok(),
                "{instruction:?}"
            );
            let mut short = instruction.clone();
            short.operands.pop();
            if !matches!(short.opcode, 0x60 | 0x61) {
                assert!(check_case(registers, short).is_err());
            }
            let mut long = instruction.clone();
            long.operands.push(0);
            assert!(check_case(registers, long).is_err());
        }
        for opcode in 0..=255 {
            if !cases
                .iter()
                .any(|(_, instruction)| instruction.opcode == opcode)
            {
                assert!(
                    check_case(&[], op(opcode, &[])).is_err(),
                    "opcode {opcode:#x}"
                );
            }
        }
    }

    #[test]
    fn rejects_wrong_shapes_for_each_source_and_destination() {
        for (registers, instruction) in cases() {
            if matches!(instruction.opcode, 0x02 | 0x64) {
                continue; // Their argument/result declarations are generated
                // from the register type.
            }
            for index in 0..registers.len() {
                let mut wrong = registers.clone();
                wrong[index] = if matches!(wrong[index], Type::Unit) {
                    Type::Boolean
                } else {
                    Type::Unit
                };
                assert!(
                    check_case(&wrong, instruction.clone()).is_err(),
                    "{instruction:?}, register {index}"
                );
            }
        }
        for opcode in 0x20..=0x24 {
            assert!(
                check_case(
                    &[Type::Boolean, row_type(), row_type()],
                    op(opcode, &[0, 1, 2])
                )
                .is_err()
            );
        }
    }

    #[test]
    fn decodes_macro_program_arguments_tables_and_control_flow() {
        let transaction = crate::tx! {
            captures { amount: i64 = 42_i64, key: u64 = 1_u64 }
            tables { data: u64 => i64 = 7_u64 }
            if exists(data[key]) {
                return data[key] + amount;
            } else {
                store(data[key], amount);
                return amount;
            }
        }
        .unwrap();
        assert_eq!(table_ids(transaction.program_bytes()).unwrap(), vec![7]);
        let decoded = decode(
            transaction.program_bytes(),
            transaction.argument_bytes(),
            &[table()],
            &claims(),
        )
        .unwrap();
        assert_eq!(decoded.result_type, Type::I64);
        assert_eq!(decoded.arguments, vec![Value::I64(42), Value::U64(1)]);
        assert_eq!(decoded.tables, vec![table()]);
    }

    #[test]
    fn rejects_truncation_padding_and_all_header_fields() {
        let valid = encode(&Type::Unit, &[], &[], &[], &[], &[op(0x63, &[0, 0])]);
        for length in 0..valid.len() {
            let mut bytes = valid[..length].to_vec();
            if bytes.len() >= 16 {
                fix_length(&mut bytes);
            }
            assert!(decode(&bytes, &arguments(&[]), &[], &claims()).is_err());
        }
        for (offset, value) in [(0, 0), (8, 2), (10, 1), (12, 0), (24, 0), (26, 1), (28, 1)] {
            let mut bytes = valid.clone();
            bytes[offset] = value;
            assert!(
                decode(&bytes, &arguments(&[]), &[], &claims()).is_err(),
                "offset {offset}"
            );
            assert!(table_ids(&bytes).is_err(), "offset {offset}");
        }
        let mut bytes = valid.clone();
        bytes.push(0);
        fix_length(&mut bytes);
        assert!(decode(&bytes, &arguments(&[]), &[], &claims()).is_err());
        let mut bytes = valid;
        let flags = bytes.len() - 7;
        bytes[flags] = 1;
        assert!(decode(&bytes, &arguments(&[]), &[], &claims()).is_err());
    }

    #[test]
    fn table_extraction_checks_descriptors_ids_and_exact_resolution() {
        for ids in [vec![0], vec![u64::MAX], vec![7, 7], vec![8, 7]] {
            let bytes = encode(&Type::Unit, &[], &[], &[], &ids, &[op(0x63, &[0, 0])]);
            assert!(table_ids(&bytes).is_err());
        }
        let bytes = encode(&Type::Unit, &[], &[], &[], &[7], &[op(0x63, &[0, 0])]);
        assert_eq!(table_ids(&bytes).unwrap(), vec![7]);
        assert!(decode(&bytes, &arguments(&[]), &[], &claims()).is_err());
        assert!(decode(&bytes, &arguments(&[]), &[table(), table()], &claims()).is_err());
        let mut wrong_table = table();
        wrong_table.id = 8;
        assert!(decode(&bytes, &arguments(&[]), &[wrong_table], &claims()).is_err());
        for length in 32..47 {
            let mut truncated = bytes[..length].to_vec();
            fix_length(&mut truncated);
            assert!(table_ids(&truncated).is_err());
        }
        let mut wrong = bytes.clone();
        wrong[32..36].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(table_ids(&wrong).is_err());
        let mut wrong = bytes;
        wrong[38] = 0xff;
        assert!(table_ids(&wrong).is_err());
    }

    #[test]
    fn rejects_cycles_unreachable_code_and_fallthrough() {
        let programs = [
            vec![jump(0)],
            vec![jump(1)],
            vec![jump(u32::MAX)],
            vec![jump(2), op(0x63, &[0, 0]), op(0x63, &[0, 0])],
            vec![op(0x63, &[0, 0]), op(0x63, &[0, 0])],
            vec![op(0x01, &[0, 0])],
            vec![op(0x01, &[0, 0]), branch(0, 0)],
            vec![op(0x01, &[0, 0]), branch(0, 2)],
        ];
        for instructions in programs {
            let bytes = encode(
                &Type::Unit,
                &[Type::Boolean],
                &[],
                &[(Type::Boolean, vec![1])],
                &[],
                &instructions,
            );
            assert!(
                decode(&bytes, &arguments(&[]), &[], &claims()).is_err(),
                "{instructions:?}"
            );
        }
        let instructions = [
            op(0x01, &[0, 0]),
            branch(0, 3),
            op(0x63, &[0, 0]),
            op(0x03, &[0, 0]),
        ];
        let bytes = encode(
            &Type::Unit,
            &[Type::Boolean],
            &[],
            &[(Type::Boolean, vec![1])],
            &[],
            &instructions,
        );
        assert!(decode(&bytes, &arguments(&[]), &[], &claims()).is_err());
    }

    #[test]
    fn joins_intersect_initialization_and_keep_predictable_edges() {
        let mut instructions = vec![
            op(0x01, &[0, 0]),
            branch(0, 4),
            op(0x01, &[1, 1]),
            jump(5),
            op(0x01, &[1, 1]),
            op(0x64, &[1]),
        ];
        let constants = [(Type::Boolean, vec![1]), (Type::U64, vec![0; 8])];
        let build = |instructions: &[WireInstruction]| {
            encode(
                &Type::U64,
                &[Type::Boolean, Type::U64],
                &[],
                &constants,
                &[],
                instructions,
            )
        };
        assert!(decode(&build(&instructions), &arguments(&[]), &[], &claims()).is_ok());
        instructions[4] = jump(5);
        assert!(decode(&build(&instructions), &arguments(&[]), &[], &claims()).is_err());
        instructions[4] = op(0x01, &[0, 1]);
        assert!(decode(&build(&instructions), &arguments(&[]), &[], &claims()).is_err());
        instructions[4] = op(0x63, &[0, 0]);
        assert!(decode(&build(&instructions), &arguments(&[]), &[], &claims()).is_ok());
        // Several explicit predecessors target the same join; one lacks
        // register 1.
        let instructions = [
            op(0x01, &[0, 0]),
            branch(0, 5),
            op(0x01, &[1, 1]),
            branch(0, 5),
            jump(5),
            op(0x64, &[1]),
        ];
        assert!(decode(&build(&instructions), &arguments(&[]), &[], &claims()).is_err());
    }

    #[test]
    fn reads_sources_before_aliasing_destinations_and_checks_pool_indices() {
        let registers = [Type::U64];
        let constants = [(Type::U64, vec![0; 8])];
        for instruction in [op(0x03, &[0, 0]), op(0x10, &[0, 0, 0])] {
            let instructions = [instruction.clone(), op(0x64, &[0])];
            let bytes = encode(&Type::U64, &registers, &[], &constants, &[], &instructions);
            assert!(decode(&bytes, &arguments(&[]), &[], &claims()).is_err());
            let instructions = [op(0x01, &[0, 0]), instruction, op(0x64, &[0])];
            let bytes = encode(&Type::U64, &registers, &[], &constants, &[], &instructions);
            assert!(decode(&bytes, &arguments(&[]), &[], &claims()).is_ok());
        }
        for instruction in [
            op(0x01, &[0, 1]),
            op(0x02, &[0, 0]),
            op(0x01, &[0xffff, 0]),
            op(0x03, &[0, 0xffff]),
            op(0x40, &[0, 0, 0]),
        ] {
            let instructions = [op(0x01, &[0, 0]), instruction, op(0x64, &[0])];
            let bytes = encode(&Type::U64, &registers, &[], &constants, &[], &instructions);
            assert!(decode(&bytes, &arguments(&[]), &[], &claims()).is_err());
        }
        for (result, argument) in [(Type::U64, Type::Boolean), (Type::Boolean, Type::U64)] {
            let bytes = encode(
                &result,
                &registers,
                std::slice::from_ref(&argument),
                &[],
                &[],
                &[op(0x02, &[0, 0]), op(0x64, &[0])],
            );
            assert!(decode(&bytes, &arguments(&[zero(&argument)]), &[], &claims()).is_err());
        }
    }

    #[test]
    fn scan_flags_endpoints_and_immediates_are_validated() {
        let registers = [row_type(), Type::U64];
        for (lo, hi, flags) in [
            (0xffff, 0xffff, 0),
            (1, 0xffff, 1),
            (0xffff, 1, 2),
            (1, 1, 3),
        ] {
            assert!(check_case(&registers, scan(0, lo, hi, flags, 1, 64)).is_ok());
        }
        for (lo, hi, flags) in [
            (0xffff, 0xffff, 1),
            (0xffff, 0xffff, 2),
            (1, 1, 4),
            (2, 1, 0),
            (1, 2, 0),
        ] {
            assert!(check_case(&registers, scan(0, lo, hi, flags, 0, 0)).is_err());
        }
        for (offset, value) in [(9, 1), (3, 1)] {
            let mut instruction = scan(0, 0xffff, 0xffff, 0, 0, 0);
            instruction.operands[offset] = value;
            assert!(check_case(&registers, instruction).is_err());
        }
        for instruction in [
            scan(0, 0xffff, 0xffff, 0, 65_536, 0),
            scan(0, 0xffff, 0xffff, 0, 0, 64 * 1024 * 1024 + 1),
        ] {
            assert!(check_case(&registers, instruction).is_err());
        }
        let bytes = encode(
            &row_type(),
            &registers,
            &[],
            &[],
            &[7],
            &[scan(0, 1, 0xffff, 0, 0, 0), op(0x64, &[0])],
        );
        assert!(decode(&bytes, &arguments(&[]), &[table()], &claims()).is_err());
    }

    #[test]
    fn arguments_constants_and_descriptors_are_fully_validated() {
        let instructions = [op(0x63, &[0, 0])];
        for (ty, value) in [
            (Type::Boolean, vec![2]),
            (Type::I64, vec![0; 7]),
            (Type::Unit, vec![0]),
            (Type::Bytes(0), vec![1, 0, 0, 0, 42]),
            (Type::String(1), vec![1, 0, 0, 0, 0xff]),
            (row_type(), vec![0; 4]),
        ] {
            let bytes = encode(
                &Type::Unit,
                &[],
                std::slice::from_ref(&ty),
                &[],
                &[],
                &instructions,
            );
            assert!(
                decode(
                    &bytes,
                    &arguments(std::slice::from_ref(&value)),
                    &[],
                    &claims()
                )
                .is_err()
            );
            let bytes = encode(&Type::Unit, &[], &[], &[(ty, value)], &[], &instructions);
            assert!(decode(&bytes, &arguments(&[]), &[], &claims()).is_err());
        }
        let bytes = encode(&Type::Unit, &[], &[Type::U64], &[], &[], &instructions);
        let valid = arguments(&[vec![0; 8]]);
        for length in 0..valid.len() {
            assert!(decode(&bytes, &valid[..length], &[], &claims()).is_err());
        }
        for invalid in [
            vec![0; 4],
            vec![2, 0, 0, 0],
            [valid, vec![0]].concat(),
            vec![1, 0, 0, 0, 255, 255, 255, 255],
        ] {
            assert!(decode(&bytes, &invalid, &[], &claims()).is_err());
        }
        for ty in [
            Type::Bytes(16 * 1024 * 1024),
            Type::Tuple(vec![Type::Unit; 257]),
            Type::Tuple(vec![row_type()]),
        ] {
            let bytes = encode(&Type::Unit, &[ty], &[], &[], &[], &instructions);
            assert!(decode(&bytes, &arguments(&[]), &[], &claims()).is_err());
        }
    }

    #[test]
    fn admission_checks_claims_not_destination_bounds() {
        let bytes = encode(
            &Type::Bytes(0),
            &[Type::Bytes(0), Type::Bytes(1000)],
            &[Type::Bytes(1000)],
            &[(Type::Bytes(1000), vec![1, 0, 0, 0, 42])],
            &[7],
            &[
                op(0x02, &[1, 0]),
                op(0x01, &[0, 0]),
                op(0x51, &[0, 0, 1]),
                op(0x64, &[0]),
            ],
        );
        let args = arguments(&[vec![1, 0, 0, 0, 42]]);
        let mut limits = *claims().values();
        for (index, amount) in [
            (0, bytes.len() as u64),
            (1, 4),
            (2, 2),
            (3, 1),
            (4, args.len() as u64),
            (5, 1),
            (10, 5),
        ] {
            let mut exact = limits;
            exact[index] = amount;
            assert!(
                decode(&bytes, &args, &[table()], &LimitPolicy::new(exact).unwrap()).is_ok(),
                "resource {}",
                index + 1
            );
            exact[index] -= 1;
            assert!(
                decode(&bytes, &args, &[table()], &LimitPolicy::new(exact).unwrap()).is_err(),
                "resource {}",
                index + 1
            );
        }
        limits[10] = 5;
        limits[11] = 0;
        limits[16] = 0;
        assert!(
            decode(
                &bytes,
                &args,
                &[table()],
                &LimitPolicy::new(limits).unwrap()
            )
            .is_ok()
        );
        let scan_bytes = encode(
            &row_type(),
            &[row_type()],
            &[],
            &[],
            &[7],
            &[scan(0, 0xffff, 0xffff, 0, 1, 8), op(0x64, &[0])],
        );
        for (index, amount) in [(12, 1), (13, 8)] {
            let mut limits = *claims().values();
            limits[index] = amount;
            assert!(
                decode(
                    &scan_bytes,
                    &arguments(&[]),
                    &[table()],
                    &LimitPolicy::new(limits).unwrap()
                )
                .is_ok()
            );
            limits[index] -= 1;
            assert!(
                decode(
                    &scan_bytes,
                    &arguments(&[]),
                    &[table()],
                    &LimitPolicy::new(limits).unwrap()
                )
                .is_err()
            );
        }
        let mut oversized = vec![0; MAX_PROGRAM_BYTES + 1];
        oversized[..8].copy_from_slice(b"BLOPVM01");
        assert!(table_ids(&oversized).is_err());
        assert!(decode(&oversized, &args, &[table()], &claims()).is_err());
        assert!(decode(&bytes, &oversized, &[table()], &claims()).is_err());
    }

    #[test]
    fn initialization_bitsets_cross_word_boundaries() {
        let registers = vec![Type::Unit; 130];
        for index in [0, 63, 64, 127, 128, 129] {
            let bytes = encode(
                &Type::Unit,
                &registers,
                &[],
                &[(Type::Unit, vec![])],
                &[],
                &[op(0x01, &[index, 0]), op(0x64, &[index])],
            );
            assert!(decode(&bytes, &arguments(&[]), &[], &claims()).is_ok());
            let other = (index + 1) % registers.len() as u16;
            let bytes = encode(
                &Type::Unit,
                &registers,
                &[],
                &[(Type::Unit, vec![])],
                &[],
                &[op(0x01, &[index, 0]), op(0x64, &[other])],
            );
            assert!(decode(&bytes, &arguments(&[]), &[], &claims()).is_err());
        }
    }

    #[test]
    fn tuple_arity_field_indices_and_argument_value_claims_are_checked() {
        let tuple = Type::Tuple(vec![Type::U64]);
        assert!(check_case(&[tuple.clone(), Type::U64], op(0x58, &[0, 0])).is_err());
        assert!(check_case(&[tuple.clone(), Type::U64], op(0x58, &[0, 2, 1, 1])).is_err());
        for field in [1, 0xffff] {
            assert!(check_case(&[Type::U64, tuple.clone()], op(0x59, &[0, 1, field])).is_err());
        }
        let bytes = encode(
            &Type::Unit,
            &[],
            &[Type::U64],
            &[],
            &[],
            &[op(0x63, &[0, 0])],
        );
        let mut limits = *claims().values();
        limits[10] = 7;
        assert!(matches!(
            decode(
                &bytes,
                &arguments(&[vec![0; 8]]),
                &[],
                &LimitPolicy::new(limits).unwrap()
            ),
            Err(Error::Invalid("argument value_bytes exceeds claim"))
        ));
    }

    #[test]
    fn accepts_maximum_instruction_and_register_counts() {
        let registers = vec![Type::Unit; 65_535];
        let mut instructions = vec![op(0x03, &[65_534, 65_534]); 65_535];
        instructions[0] = op(0x01, &[65_534, 0]);
        instructions[65_534] = op(0x64, &[65_534]);
        let bytes = encode(
            &Type::Unit,
            &registers,
            &[],
            &[(Type::Unit, vec![])],
            &[],
            &instructions,
        );
        assert!(decode(&bytes, &arguments(&[]), &[], &claims()).is_ok());
        instructions[65_534] = op(0x64, &[0xffff]);
        let bytes = encode(
            &Type::Unit,
            &registers,
            &[],
            &[(Type::Unit, vec![])],
            &[],
            &instructions,
        );
        assert!(decode(&bytes, &arguments(&[]), &[], &claims()).is_err());
    }

    #[test]
    fn arbitrary_single_byte_mutations_do_not_panic() {
        let bytes = encode(
            &row_type(),
            &[Type::Boolean, Type::U64, row_type()],
            &[Type::Boolean, Type::U64],
            &[(Type::Unit, vec![])],
            &[7],
            &[
                op(0x02, &[0, 0]),
                op(0x02, &[1, 1]),
                branch(0, 4),
                op(0x62, &[0, 0, 0]),
                scan(2, 1, 1, 3, 1, 16),
                op(0x64, &[2]),
            ],
        );
        let args = arguments(&[vec![1], vec![0; 8]]);
        let policy = claims();
        let tables = [table()];
        assert!(decode(&bytes, &args, &tables, &policy).is_ok());
        for index in 0..bytes.len() {
            let mut mutated = bytes.clone();
            for value in 0..=255 {
                mutated[index] = value;
                let _ = table_ids(&mutated);
                let _ = decode(&mutated, &args, &tables, &policy);
            }
        }
        for index in 0..args.len() {
            let mut mutated = args.clone();
            for value in 0..=255 {
                mutated[index] = value;
                let _ = decode(&bytes, &mutated, &tables, &policy);
            }
        }
    }
}
