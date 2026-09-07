use proc_macro2::Span;
use syn::BinOp;
use syn::Expr;
use syn::Lit;
use syn::Result;
use syn::UnOp;
use syn::spanned::Spanned;

use super::Compiler;
use super::address;
use super::constant;
use super::emit_table;
use super::intrinsics;
use super::path_name;
use super::require_shape;
use crate::bytecode::blob;
use crate::types::Type;

pub(super) fn compile_expr(
    compiler: &mut Compiler,
    expression: &Expr,
    expected: Option<&Type>,
) -> Result<u16> {
    let span = expression.span();
    let register = match expression {
        Expr::Paren(paren) => compile_expr(compiler, &paren.expr, expected)?,
        Expr::Group(group) => compile_expr(compiler, &group.expr, expected)?,
        Expr::Path(_) => {
            let name = path_name(expression)?;
            compiler
                .locals
                .get(&name)
                .map(|local| local.register)
                .ok_or_else(|| {
                    syn::Error::new(
                        span,
                        "unknown VM local; external values must be declared in captures { ... }",
                    )
                })?
        }
        Expr::Lit(literal) => compile_literal(compiler, &literal.lit, expected)?,
        Expr::Unary(unary) => {
            if matches!(unary.op, UnOp::Neg(_))
                && let Expr::Lit(literal) = &*unary.expr
                && let Lit::Int(integer) = &literal.lit
            {
                if !matches!(integer.suffix(), "" | "i64") {
                    return Err(syn::Error::new(span, "negative literals must be i64"));
                }
                let magnitude = integer.base10_parse::<u64>()?;
                if magnitude > (1u64 << 63) {
                    return Err(syn::Error::new(span, "integer literal is outside i64"));
                }
                constant(
                    compiler,
                    Type::I64,
                    (magnitude.wrapping_neg() as i64).to_le_bytes().to_vec(),
                    span,
                )?
            } else {
                let source = compile_expr(compiler, &unary.expr, expected)?;
                let opcode = match (&unary.op, compiler.ty(source)) {
                    (UnOp::Neg(_), Type::I64) => 0x15,
                    (UnOp::Not(_), Type::Bool) => 0x2b,
                    (UnOp::Not(_), Type::I64 | Type::U64) => 0x33,
                    _ => {
                        return Err(syn::Error::new(
                            span,
                            "unsupported unary operation or operand type",
                        ));
                    }
                };
                let result = compiler.register(compiler.ty(source).clone(), span)?;
                compiler.emit(opcode, &[result, source], span)?;
                result
            }
        }
        Expr::Binary(binary) => {
            if matches!(binary.op, BinOp::And(_) | BinOp::Or(_)) {
                short_circuit(
                    compiler,
                    &binary.left,
                    &binary.right,
                    matches!(binary.op, BinOp::And(_)),
                    span,
                )?
            } else {
                let opcode = binary_op(&binary.op)
                    .ok_or_else(|| syn::Error::new(span, "unsupported VM operator"))?;
                let hint = if (0x20..=0x24).contains(&opcode) {
                    type_hint(compiler, &binary.right)
                } else if matches!(opcode, 0x34 | 0x35) {
                    expected.cloned()
                } else {
                    expected
                        .cloned()
                        .or_else(|| type_hint(compiler, &binary.right))
                };
                let left = compile_expr(compiler, &binary.left, hint.as_ref())?;
                let right_type = if matches!(opcode, 0x34 | 0x35) {
                    Type::U64
                } else {
                    compiler.ty(left).clone()
                };
                let right = compile_expr(compiler, &binary.right, Some(&right_type))?;
                let mut result = binary_instruction(compiler, opcode, left, right, span)?;
                if matches!(binary.op, BinOp::Ne(_)) {
                    let inverted = compiler.register(Type::Bool, span)?;
                    compiler.emit(0x2b, &[inverted, result], span)?;
                    result = inverted;
                }
                result
            }
        }
        Expr::Index(_) => {
            let (table, key) = address(compiler, expression)?;
            let result = compiler.register(table.value, span)?;
            emit_table(
                compiler,
                0x40,
                &[result, table.index, key],
                2,
                table.index,
                span,
            )?;
            result
        }
        Expr::Tuple(tuple) => {
            if tuple.elems.is_empty() {
                constant(compiler, Type::Unit, vec![], span)?
            } else {
                let hints = match expected {
                    Some(Type::Tuple(fields)) => Some(fields),
                    _ => None,
                };
                let mut fields = Vec::new();
                for (index, field) in tuple.elems.iter().enumerate() {
                    fields.push(compile_expr(
                        compiler,
                        field,
                        hints.and_then(|fields| fields.get(index)),
                    )?);
                }
                tuple_instruction(compiler, fields, span)?
            }
        }
        Expr::Field(field) => {
            let source = compile_expr(compiler, &field.base, None)?;
            let syn::Member::Unnamed(index) = &field.member else {
                return Err(syn::Error::new(span, "VM tuples have numeric fields only"));
            };
            field_instruction(compiler, source, index.index as usize, span)?
        }
        Expr::Call(call) => intrinsics::compile_call(compiler, call, expected)?,
        _ => {
            return Err(syn::Error::new(
                span,
                "unsupported VM expression; loops, Rust calls, casts, and value-producing blocks \
                 are not allowed",
            ));
        }
    };
    if let Some(expected) = expected {
        require_shape(compiler.ty(register), expected, span)?;
    }
    Ok(register)
}

fn compile_literal(
    compiler: &mut Compiler,
    literal: &Lit,
    expected: Option<&Type>,
) -> Result<u16> {
    let span = literal.span();
    let (ty, bytes) = match literal {
        Lit::Bool(value) => (Type::Bool, vec![u8::from(value.value)]),
        Lit::Int(integer) => {
            let unsigned = match integer.suffix() {
                "u64" => true,
                "i64" => false,
                "" => expected == Some(&Type::U64),
                _ => {
                    return Err(syn::Error::new(
                        span,
                        "VM integer literals support only i64 and u64 suffixes",
                    ));
                }
            };
            if unsigned {
                (
                    Type::U64,
                    integer.base10_parse::<u64>()?.to_le_bytes().to_vec(),
                )
            } else {
                (
                    Type::I64,
                    integer.base10_parse::<i64>()?.to_le_bytes().to_vec(),
                )
            }
        }
        Lit::Str(value) => {
            let text = value.value();
            let mut bytes = Vec::new();
            blob(text.as_bytes(), &mut bytes)?;
            (Type::String(text.len() as u32), bytes)
        }
        Lit::ByteStr(value) => {
            let value = value.value();
            let mut bytes = Vec::new();
            blob(&value, &mut bytes)?;
            (Type::Bytes(value.len() as u32), bytes)
        }
        _ => return Err(syn::Error::new(span, "unsupported VM literal")),
    };
    constant(compiler, ty, bytes, span)
}

pub(super) fn type_hint(
    compiler: &Compiler,
    expression: &Expr,
) -> Option<Type> {
    match expression {
        Expr::Paren(paren) => type_hint(compiler, &paren.expr),
        Expr::Group(group) => type_hint(compiler, &group.expr),
        Expr::Path(_) => compiler
            .locals
            .get(&path_name(expression).ok()?)
            .map(|l| compiler.ty(l.register).clone()),
        Expr::Index(index) => compiler
            .tables
            .get(&path_name(&index.expr).ok()?)
            .map(|t| t.value.clone()),
        Expr::Lit(literal) => match &literal.lit {
            Lit::Int(integer) if integer.suffix() == "u64" => Some(Type::U64),
            Lit::Int(integer) if integer.suffix() == "i64" => Some(Type::I64),
            Lit::Bool(_) => Some(Type::Bool),
            _ => None,
        },
        Expr::Binary(binary) => match binary.op {
            BinOp::Eq(_)
            | BinOp::Ne(_)
            | BinOp::Lt(_)
            | BinOp::Le(_)
            | BinOp::Gt(_)
            | BinOp::Ge(_)
            | BinOp::And(_)
            | BinOp::Or(_) => Some(Type::Bool),
            BinOp::Shl(_) | BinOp::Shr(_) => type_hint(compiler, &binary.left),
            _ => type_hint(compiler, &binary.left).or_else(|| type_hint(compiler, &binary.right)),
        },
        Expr::Call(call)
            if path_name(&call.func).ok().as_deref() == Some("__capture")
                && call.args.len() == 1 =>
        {
            compiler
                .captures
                .get(&path_name(&call.args[0]).ok()?)
                .map(|r| compiler.ty(*r).clone())
        }
        _ => None,
    }
}

fn short_circuit(
    compiler: &mut Compiler,
    left: &Expr,
    right: &Expr,
    and: bool,
    span: Span,
) -> Result<u16> {
    let left = compile_expr(compiler, left, Some(&Type::Bool))?;
    let result = compiler.register(Type::Bool, span)?;
    compiler.emit(0x03, &[result, left], span)?;
    let branch = compiler.jump(Some(left), span)?;
    let end = if and {
        None
    } else {
        Some(compiler.jump(None, span)?)
    };
    if !and {
        compiler.patch_jump(branch);
    }
    let right = compile_expr(compiler, right, Some(&Type::Bool))?;
    compiler.emit(0x03, &[result, right], span)?;
    compiler.patch_jump(end.unwrap_or(branch));
    Ok(result)
}

pub(super) fn binary_instruction(
    compiler: &mut Compiler,
    mut opcode: u8,
    left: u16,
    right: u16,
    span: Span,
) -> Result<u16> {
    let a = compiler.ty(left);
    let b = compiler.ty(right);
    if matches!(opcode, 0x30..=0x32) && *a == Type::Bool {
        opcode -= 8;
    }
    let valid = match opcode {
        0x10..=0x14 | 0x30..=0x32 => a.is_integer() && a == b,
        0x20..=0x24 => !matches!(a, Type::Rows(..)) && a.same_shape(b),
        0x28..=0x2a => *a == Type::Bool && *b == Type::Bool,
        0x34 | 0x35 => a.is_integer() && *b == Type::U64,
        _ => false,
    };
    if !valid {
        return Err(syn::Error::new(
            span,
            format!("invalid VM operands: {a:?} and {b:?}"),
        ));
    }
    let result_type = if (0x20..=0x2a).contains(&opcode) {
        Type::Bool
    } else {
        a.clone()
    };
    let result = compiler.register(result_type, span)?;
    compiler.emit(opcode, &[result, left, right], span)?;
    Ok(result)
}

pub(super) fn tuple_instruction(
    compiler: &mut Compiler,
    fields: Vec<u16>,
    span: Span,
) -> Result<u16> {
    let ty = Type::Tuple(
        fields
            .iter()
            .map(|field| compiler.ty(*field).clone())
            .collect(),
    );
    let result = compiler.register(ty, span)?;
    let mut operands = vec![result, fields.len() as u16];
    operands.extend(fields);
    compiler.emit(0x58, &operands, span)?;
    Ok(result)
}

pub(super) fn field_instruction(
    compiler: &mut Compiler,
    source: u16,
    index: usize,
    span: Span,
) -> Result<u16> {
    let Type::Tuple(fields) = compiler.ty(source) else {
        return Err(syn::Error::new(span, "field requires a tuple"));
    };
    let ty = fields
        .get(index)
        .cloned()
        .ok_or_else(|| syn::Error::new(span, "tuple field index out of bounds"))?;
    let result = compiler.register(ty, span)?;
    compiler.emit(0x59, &[result, source, index as u16], span)?;
    Ok(result)
}

fn binary_op(op: &BinOp) -> Option<u8> {
    Some(match op {
        BinOp::Add(_) => 0x10,
        BinOp::Sub(_) => 0x11,
        BinOp::Mul(_) => 0x12,
        BinOp::Div(_) => 0x13,
        BinOp::Rem(_) => 0x14,
        BinOp::Eq(_) | BinOp::Ne(_) => 0x20,
        BinOp::Lt(_) => 0x21,
        BinOp::Le(_) => 0x22,
        BinOp::Gt(_) => 0x23,
        BinOp::Ge(_) => 0x24,
        BinOp::BitAnd(_) => 0x30,
        BinOp::BitOr(_) => 0x31,
        BinOp::BitXor(_) => 0x32,
        BinOp::Shl(_) => 0x34,
        BinOp::Shr(_) => 0x35,
        _ => return None,
    })
}

pub(super) fn assignment_op(op: &BinOp) -> Option<u8> {
    Some(match op {
        BinOp::AddAssign(_) => 0x10,
        BinOp::SubAssign(_) => 0x11,
        BinOp::MulAssign(_) => 0x12,
        BinOp::DivAssign(_) => 0x13,
        BinOp::RemAssign(_) => 0x14,
        BinOp::BitAndAssign(_) => 0x30,
        BinOp::BitOrAssign(_) => 0x31,
        BinOp::BitXorAssign(_) => 0x32,
        BinOp::ShlAssign(_) => 0x34,
        BinOp::ShrAssign(_) => 0x35,
        _ => return None,
    })
}
