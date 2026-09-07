use syn::{ExprCall, Result, spanned::Spanned};

use super::{
    Compiler, address, emit_table,
    expr::{binary_instruction, compile_expr, field_instruction, tuple_instruction, type_hint},
    immediate, path_name, table_info,
};
use crate::types::{Type, VALUE_LIMIT};

pub(super) fn compile_call(
    compiler: &mut Compiler,
    call: &ExprCall,
    expected: Option<&Type>,
) -> Result<u16> {
    let name = path_name(&call.func)?;
    let span = call.span();
    let args = &call.args;
    if name == "__capture" && args.len() == 1 {
        return compiler
            .captures
            .get(&path_name(&args[0])?)
            .copied()
            .ok_or_else(|| syn::Error::new(span, "unknown capture"));
    }
    if name == "tuple" {
        let mut fields = Vec::new();
        for (index, arg) in args.iter().enumerate() {
            let hint = match expected {
                Some(Type::Tuple(fields)) => fields.get(index),
                _ => None,
            };
            fields.push(compile_expr(compiler, arg, hint)?);
        }
        return tuple_instruction(compiler, fields, span);
    }
    let arity = match name.as_str() {
        "copy" | "load" | "exists" | "neg_checked" | "to_i64_checked" | "to_u64_checked"
        | "bool_not" | "bit_not" | "rows_len" | "byte_len" | "utf8_bytes" | "parse_utf8"
        | "sha256" => 1,
        "add_checked" | "sub_checked" | "mul_checked" | "div_checked" | "rem_checked" | "eq"
        | "lt" | "le" | "gt" | "ge" | "bool_and" | "bool_or" | "bool_xor" | "bit_and"
        | "bit_or" | "bit_xor" | "shl_wrap" | "shr" | "rows_key" | "rows_value" | "concat"
        | "field" => 2,
        "slice_bytes" => 3,
        "scan_bounded" => 6,
        _ => {
            return Err(syn::Error::new(
                span,
                "unknown VM operation; Rust function calls are not allowed",
            ));
        }
    };
    if args.len() != arity {
        return Err(syn::Error::new(
            span,
            format!("{name} requires {arity} operands"),
        ));
    }
    if name == "scan_bounded" {
        return scan(compiler, call);
    }
    if matches!(name.as_str(), "load" | "exists") {
        let (table, key) = address(compiler, &args[0])?;
        let ty = if name == "exists" {
            Type::Bool
        } else {
            table.value
        };
        let result = compiler.register(ty, span)?;
        emit_table(
            compiler,
            if name == "exists" { 0x41 } else { 0x40 },
            &[result, table.index, key],
            2,
            table.index,
            span,
        )?;
        return Ok(result);
    }
    let binary_opcode = match name.as_str() {
        "add_checked" => Some(0x10),
        "sub_checked" => Some(0x11),
        "mul_checked" => Some(0x12),
        "div_checked" => Some(0x13),
        "rem_checked" => Some(0x14),
        "eq" => Some(0x20),
        "lt" => Some(0x21),
        "le" => Some(0x22),
        "gt" => Some(0x23),
        "ge" => Some(0x24),
        "bool_and" => Some(0x28),
        "bool_or" => Some(0x29),
        "bool_xor" => Some(0x2a),
        "bit_and" => Some(0x30),
        "bit_or" => Some(0x31),
        "bit_xor" => Some(0x32),
        "shl_wrap" => Some(0x34),
        "shr" => Some(0x35),
        _ => None,
    };
    if let Some(opcode) = binary_opcode {
        let hint = if (0x20..=0x24).contains(&opcode) {
            type_hint(compiler, &args[1])
        } else if matches!(opcode, 0x34 | 0x35) {
            expected.cloned()
        } else {
            expected.cloned().or_else(|| type_hint(compiler, &args[1]))
        };
        let left = compile_expr(compiler, &args[0], hint.as_ref())?;
        let right_type = if matches!(opcode, 0x34 | 0x35) {
            Type::U64
        } else {
            compiler.ty(left).clone()
        };
        let right = compile_expr(compiler, &args[1], Some(&right_type))?;
        if (0x30..=0x32).contains(&opcode) && !compiler.ty(left).is_integer() {
            return Err(syn::Error::new(
                span,
                "bitwise intrinsics require integers; use bool_and/or/xor for booleans",
            ));
        }
        return binary_instruction(compiler, opcode, left, right, span);
    }
    let hint = match name.as_str() {
        "copy" | "concat" | "bit_not" => expected,
        "neg_checked" | "to_u64_checked" => Some(&Type::I64),
        "to_i64_checked" => Some(&Type::U64),
        "bool_not" => Some(&Type::Bool),
        _ => None,
    };
    let source = compile_expr(compiler, &args[0], hint)?;
    let source_type = compiler.ty(source).clone();
    if name == "field" {
        return field_instruction(compiler, source, immediate(&args[1], 255)? as usize, span);
    }
    let mut operands = vec![source];
    let (opcode, ty) = match (name.as_str(), &source_type) {
        ("copy", _) => (0x03, source_type.clone()),
        ("neg_checked", Type::I64) => (0x15, Type::I64),
        ("to_i64_checked", Type::U64) => (0x16, Type::I64),
        ("to_u64_checked", Type::I64) => (0x17, Type::U64),
        ("bool_not", Type::Bool) => (0x2b, Type::Bool),
        ("bit_not", Type::I64 | Type::U64) => (0x33, source_type.clone()),
        ("rows_len", Type::Rows(..)) => (0x49, Type::U64),
        ("rows_key" | "rows_value", Type::Rows(key, value, _)) => {
            operands.push(compile_expr(compiler, &args[1], Some(&Type::U64))?);
            if name == "rows_key" {
                (0x4a, (**key).clone())
            } else {
                (0x4b, (**value).clone())
            }
        }
        ("byte_len", Type::Bytes(_) | Type::String(_)) => (0x50, Type::U64),
        ("concat", Type::Bytes(a) | Type::String(a)) => {
            let right = compile_expr(compiler, &args[1], Some(&source_type))?;
            let b = match compiler.ty(right) {
                Type::Bytes(b) | Type::String(b) => *b,
                _ => unreachable!(),
            };
            operands.push(right);
            // A legal descriptor caps growth; the VM enforces this bound on the actual result.
            let bound = (u64::from(*a) + u64::from(b)).min(VALUE_LIMIT - 4) as u32;
            let ty = if matches!(source_type, Type::Bytes(_)) {
                Type::Bytes(bound)
            } else {
                Type::String(bound)
            };
            (0x51, ty)
        }
        ("slice_bytes", Type::Bytes(bound)) => {
            operands.push(compile_expr(compiler, &args[1], Some(&Type::U64))?);
            operands.push(compile_expr(compiler, &args[2], Some(&Type::U64))?);
            (0x52, Type::Bytes(*bound))
        }
        ("utf8_bytes", Type::String(bound)) => (0x53, Type::Bytes(*bound)),
        ("parse_utf8", Type::Bytes(bound)) => (0x54, Type::String(*bound)),
        ("sha256", Type::Bytes(_)) => (0x55, Type::Bytes(32)),
        _ => {
            return Err(syn::Error::new(
                span,
                format!("invalid operand for {name}: {source_type:?}"),
            ));
        }
    };
    let result = compiler.register(ty, span)?;
    operands.insert(0, result);
    compiler.emit(opcode, &operands, span)?;
    Ok(result)
}

fn scan(compiler: &mut Compiler, call: &ExprCall) -> Result<u16> {
    let span = call.span();
    let args = &call.args;
    let table = table_info(compiler, &args[0])?;
    let flags = immediate(&args[3], 3)? as u8;
    let row_limit = immediate(&args[4], 65_535)? as u32;
    let byte_limit = immediate(&args[5], 64 * 1024 * 1024)?;
    let mut endpoints = Vec::new();
    for (index, endpoint) in [&args[1], &args[2]].into_iter().enumerate() {
        if path_name(endpoint).ok().as_deref() == Some("unbounded") {
            if flags & (1 << index) != 0 {
                return Err(syn::Error::new(
                    endpoint.span(),
                    "unbounded endpoints cannot be inclusive",
                ));
            }
            endpoints.push(u16::MAX);
        } else {
            let register = compile_expr(compiler, endpoint, Some(&table.key))?;
            endpoints.push(register);
        }
    }
    let ty = Type::Rows(Box::new(table.key), Box::new(table.value), row_limit);
    let result = compiler.register(ty, span)?;
    let index = emit_table(
        compiler,
        0x48,
        &[result, table.index, endpoints[0], endpoints[1]],
        2,
        table.index,
        span,
    )?;
    let operands = &mut compiler.instructions[index].operands;
    operands.extend([flags, 0]);
    operands.extend(row_limit.to_le_bytes());
    operands.extend(byte_limit.to_le_bytes());
    Ok(result)
}
