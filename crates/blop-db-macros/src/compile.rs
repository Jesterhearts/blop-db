use std::collections::HashMap;

use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::{Block, Expr, Ident, Pat, Path, Result, Stmt, parse::Parser, spanned::Spanned};

use crate::{
    bytecode::{self, Instruction, pool_index},
    syntax::{Program, validate_name},
    types::{Type, parse_type, validate_type},
};

mod expr;
mod intrinsics;

#[derive(Clone)]
struct Local {
    register: u16,
    mutable: bool,
}

#[derive(Clone)]
struct TableInfo {
    index: u16,
    key: Type,
    value: Type,
}

struct Compiler {
    registers: Vec<Type>,
    constants: Vec<(Type, Vec<u8>)>,
    instructions: Vec<Instruction>,
    locals: HashMap<String, Local>,
    captures: HashMap<String, u16>,
    tables: HashMap<String, TableInfo>,
    result: Option<Type>,
    declared_result: bool,
}

impl Compiler {
    fn register(&mut self, ty: Type, span: Span) -> Result<u16> {
        validate_type(&ty, true, span)?;
        let register = pool_index(self.registers.len(), span)?;
        self.registers.push(ty);
        Ok(register)
    }

    fn emit(&mut self, opcode: u8, operands: &[u16], span: Span) -> Result<usize> {
        pool_index(self.instructions.len(), span)?;
        let index = self.instructions.len();
        self.instructions.push(Instruction {
            opcode,
            operands: operands.iter().flat_map(|n| n.to_le_bytes()).collect(),
            table: None,
        });
        Ok(index)
    }

    fn jump(&mut self, condition: Option<u16>, span: Span) -> Result<usize> {
        let index = if let Some(condition) = condition {
            self.emit(0x61, &[condition], span)?
        } else {
            self.emit(0x60, &[], span)?
        };
        self.instructions[index].operands.extend(0u32.to_le_bytes());
        Ok(index)
    }

    fn patch_jump(&mut self, index: usize) {
        let target = self.instructions.len() as u32;
        let operands = &mut self.instructions[index].operands;
        let offset = operands.len() - 4;
        operands[offset..].copy_from_slice(&target.to_le_bytes());
    }

    fn ty(&self, register: u16) -> &Type {
        &self.registers[usize::from(register)]
    }
}

pub fn compile(program: Program) -> Result<TokenStream> {
    let mut compiler = Compiler {
        registers: Vec::new(),
        constants: Vec::new(),
        instructions: Vec::new(),
        locals: HashMap::new(),
        captures: HashMap::new(),
        tables: HashMap::new(),
        declared_result: program.result.is_some(),
        result: program.result,
    };
    for (index, table) in program.tables.iter().enumerate() {
        let index = pool_index(index, table.name.span())?;
        compiler.tables.insert(
            table.name.to_string(),
            TableInfo {
                index,
                key: table.key.clone(),
                value: table.value.clone(),
            },
        );
    }
    for (index, capture) in program.captures.iter().enumerate() {
        let index = pool_index(index, capture.name.span())?;
        let register = compiler.register(capture.ty.clone(), capture.name.span())?;
        compiler.emit(0x02, &[register, index], capture.name.span())?;
        compiler.locals.insert(
            capture.name.to_string(),
            Local {
                register,
                mutable: false,
            },
        );
        compiler.captures.insert(capture.name.to_string(), register);
    }
    if !compile_block(&mut compiler, &program.body)? {
        if compiler.result.as_ref().is_some_and(|ty| *ty != Type::Unit) {
            return Err(syn::Error::new(
                program.body.span(),
                "not every path returns a value or aborts",
            ));
        }
        let unit = constant(&mut compiler, Type::Unit, vec![], program.body.span())?;
        compiler.emit(0x64, &[unit], program.body.span())?;
    }
    let result = compiler.result.unwrap_or(Type::Unit);
    validate_type(&result, true, program.body.span())?;
    let arguments = program
        .captures
        .iter()
        .map(|c| c.ty.clone())
        .collect::<Vec<_>>();
    let encoded = bytecode::encode(
        &result,
        &arguments,
        &compiler.registers,
        program.tables.len(),
        &compiler.constants,
        &compiler.instructions,
    )?;
    let bytes = syn::LitByteStr::new(&encoded.bytes, Span::call_site());
    let table_offset = encoded.table_offset;
    let patches = encoded
        .patches
        .iter()
        .map(|(offset, index)| quote!((#offset, #index)));
    let runtime = program.runtime;
    let count = program.captures.len() as u32;
    let captures = Ident::new("__blop_captures", Span::mixed_site());
    let table_ids = Ident::new("__blop_tables", Span::mixed_site());
    let args = Ident::new("__blop_arguments", Span::mixed_site());
    let value = Ident::new("__blop_value", Span::mixed_site());
    let capture_values = program.captures.iter().map(|c| &c.value);
    let table_values = program.tables.iter().map(|t| &t.id);
    let encoders = program.captures.iter().enumerate().map(|(index, capture)| {
        let index = syn::Index::from(index);
        let encode = encode_capture(&capture.ty, quote!(#captures.#index), &value, &runtime);
        quote! {
            let mut #value = ::std::vec::Vec::new();
            #encode
            #runtime::__private::push_blob(&mut #args, &#value, 16 * 1024 * 1024)?;
        }
    });
    Ok(quote! {{
        let #captures = (#(&(#capture_values),)*);
        let #table_ids: &[::core::primitive::u64] = &[#(#table_values),*];
        (|| -> ::core::result::Result<#runtime::Transaction, #runtime::BuildError> {
            let mut #args = ::std::vec::Vec::from(#count.to_le_bytes());
            #(#encoders)*
            #runtime::__private::bind_program(#bytes, #table_offset, &[#(#patches),*], #table_ids, #args)
        })()
    }})
}

fn encode_capture(ty: &Type, source: TokenStream, out: &Ident, runtime: &Path) -> TokenStream {
    match ty {
        Type::Unit => quote! { let _: &() = #source; },
        Type::Bool => {
            quote! { #out.push(::core::primitive::u8::from(*{ let value: &::core::primitive::bool = #source; value })); }
        }
        Type::I64 => {
            quote! {{ let value: &::core::primitive::i64 = #source; #out.extend_from_slice(&value.to_le_bytes()); }}
        }
        Type::U64 => {
            quote! {{ let value: &::core::primitive::u64 = #source; #out.extend_from_slice(&value.to_le_bytes()); }}
        }
        Type::Bytes(bound) => quote! {
            #runtime::__private::push_blob(&mut #out, ::core::convert::AsRef::<[::core::primitive::u8]>::as_ref(#source), #bound)?;
        },
        Type::String(bound) => quote! {
            #runtime::__private::push_blob(&mut #out, ::core::convert::AsRef::<::core::primitive::str>::as_ref(#source).as_bytes(), #bound)?;
        },
        Type::Tuple(fields) => {
            let names = (0..fields.len())
                .map(|index| Ident::new(&format!("__blop_field_{index}"), Span::mixed_site()))
                .collect::<Vec<_>>();
            let encoders = fields
                .iter()
                .zip(&names)
                .map(|(ty, name)| encode_capture(ty, quote!(#name), out, runtime));
            quote! {{
                let (#(#names,)*) = #source;
                #(#encoders)*
            }}
        }
        Type::Rows(..) => unreachable!("capture types exclude Rows"),
    }
}

fn compile_block(compiler: &mut Compiler, block: &Block) -> Result<bool> {
    let outer = compiler.locals.clone();
    let mut terminated = false;
    for statement in &block.stmts {
        if terminated {
            return Err(syn::Error::new(
                statement.span(),
                "unreachable VM statement",
            ));
        }
        terminated = compile_statement(compiler, statement)?;
    }
    compiler.locals = outer;
    Ok(terminated)
}

fn compile_statement(compiler: &mut Compiler, statement: &Stmt) -> Result<bool> {
    match statement {
        Stmt::Local(local) => {
            let (pattern, annotation) = match &local.pat {
                Pat::Type(typed) => {
                    let ty = &typed.ty;
                    (&*typed.pat, Some(parse_type.parse2(quote!(#ty))?))
                }
                pat => (pat, None),
            };
            let Pat::Ident(name) = pattern else {
                return Err(syn::Error::new(
                    pattern.span(),
                    "VM let requires a single local name",
                ));
            };
            validate_name(&name.ident)?;
            if name.by_ref.is_some() || name.subpat.is_some() {
                return Err(syn::Error::new(
                    pattern.span(),
                    "VM bindings do not support ref or subpatterns",
                ));
            }
            let init = local
                .init
                .as_ref()
                .ok_or_else(|| syn::Error::new(local.span(), "VM locals must be initialized"))?;
            if init.diverge.is_some() {
                return Err(syn::Error::new(
                    local.span(),
                    "let-else is not supported by the VM",
                ));
            }
            let source = expr::compile_expr(compiler, &init.expr, annotation.as_ref())?;
            let ty = annotation.unwrap_or_else(|| compiler.ty(source).clone());
            let register = compiler.register(ty, name.span())?;
            compiler.emit(0x03, &[register, source], name.span())?;
            compiler.locals.insert(
                name.ident.to_string(),
                Local {
                    register,
                    mutable: name.mutability.is_some(),
                },
            );
            Ok(false)
        }
        Stmt::Expr(expression, _) => compile_statement_expr(compiler, expression),
        _ => Err(syn::Error::new(
            statement.span(),
            "Rust items and macros cannot run in a VM program",
        )),
    }
}

fn compile_statement_expr(compiler: &mut Compiler, expression: &Expr) -> Result<bool> {
    let span = expression.span();
    match expression {
        Expr::If(branch) => {
            let condition = expr::compile_expr(compiler, &branch.cond, Some(&Type::Bool))?;
            let otherwise = compiler.jump(Some(condition), span)?;
            let then_terminated = compile_block(compiler, &branch.then_branch)?;
            if let Some((_, alternative)) = &branch.else_branch {
                let end = if then_terminated {
                    None
                } else {
                    Some(compiler.jump(None, span)?)
                };
                compiler.patch_jump(otherwise);
                let else_terminated = compile_statement_expr(compiler, alternative)?;
                if let Some(end) = end {
                    compiler.patch_jump(end);
                }
                Ok(then_terminated && else_terminated)
            } else {
                compiler.patch_jump(otherwise);
                Ok(false)
            }
        }
        Expr::Block(block) if block.label.is_none() => compile_block(compiler, &block.block),
        Expr::Return(ret) => {
            let expected = compiler.result.clone();
            let source = if let Some(value) = &ret.expr {
                expr::compile_expr(compiler, value, expected.as_ref())?
            } else {
                if expected.as_ref().is_some_and(|ty| *ty != Type::Unit) {
                    return Err(syn::Error::new(
                        span,
                        "return requires a value of the result type",
                    ));
                }
                constant(compiler, Type::Unit, vec![], span)?
            };
            let ty = compiler.ty(source).clone();
            compiler.result = Some(match &compiler.result {
                Some(result) if !compiler.declared_result => result.union(&ty),
                Some(result) => result.clone(),
                None => ty,
            });
            compiler.emit(0x64, &[source], span)?;
            Ok(true)
        }
        Expr::Assign(assign) => {
            assign_value(compiler, &assign.left, &assign.right, None)?;
            Ok(false)
        }
        Expr::Binary(binary) if expr::assignment_op(&binary.op).is_some() => {
            assign_value(
                compiler,
                &binary.left,
                &binary.right,
                expr::assignment_op(&binary.op),
            )?;
            Ok(false)
        }
        Expr::Call(call) => {
            let name = path_name(&call.func)?;
            if name == "require" || name == "abort" {
                let args = call.args.iter().collect::<Vec<_>>();
                let min = usize::from(name == "require");
                if args.len() < min || args.len() > min + 1 {
                    return Err(syn::Error::new(
                        span,
                        "require(condition[, code]) or abort([code]) expected",
                    ));
                }
                let index = if name == "require" {
                    let condition = expr::compile_expr(compiler, args[0], Some(&Type::Bool))?;
                    compiler.emit(0x62, &[condition], span)?
                } else {
                    compiler.emit(0x63, &[], span)?
                };
                let code = args
                    .get(min)
                    .map(|arg| immediate(arg, u64::from(u32::MAX)))
                    .transpose()?
                    .unwrap_or(0) as u32;
                compiler.instructions[index]
                    .operands
                    .extend(code.to_le_bytes());
                return Ok(name == "abort");
            }
            if matches!(name.as_str(), "insert" | "store" | "delete") {
                let required = if name == "delete" { 1 } else { 2 };
                if call.args.len() != required {
                    return Err(syn::Error::new(
                        span,
                        "expected delete(table[key]) or insert/store(table[key], value)",
                    ));
                }
                let (table, key) = address(compiler, &call.args[0])?;
                let mut operands = vec![table.index, key];
                if required == 2 {
                    operands.push(expr::compile_expr(
                        compiler,
                        &call.args[1],
                        Some(&table.value),
                    )?);
                }
                let opcode = match name.as_str() {
                    "insert" => 0x42,
                    "store" => 0x43,
                    _ => 0x44,
                };
                emit_table(compiler, opcode, &operands, 0, table.index, span)?;
                return Ok(false);
            }
            expr::compile_expr(compiler, expression, None)?;
            Ok(false)
        }
        Expr::Path(_) if path_name(expression)? == "abort" => {
            let index = compiler.emit(0x63, &[], span)?;
            compiler.instructions[index]
                .operands
                .extend(0u32.to_le_bytes());
            Ok(true)
        }
        _ => {
            expr::compile_expr(compiler, expression, None)?;
            Ok(false)
        }
    }
}

fn assign_value(
    compiler: &mut Compiler,
    target: &Expr,
    value: &Expr,
    opcode: Option<u8>,
) -> Result<()> {
    let span = target.span();
    let (local, address) = if let Expr::Index(_) = target {
        (None, Some(address(compiler, target)?))
    } else {
        let name = path_name(target)?;
        let local = compiler
            .locals
            .get(&name)
            .cloned()
            .ok_or_else(|| syn::Error::new(span, "unknown VM local"))?;
        if !local.mutable {
            return Err(syn::Error::new(
                span,
                "cannot assign to an immutable local or capture; use let mut",
            ));
        }
        (Some(local.register), None)
    };
    let ty = match &address {
        Some((table, _)) => table.value.clone(),
        None => compiler.ty(local.unwrap()).clone(),
    };
    let prior = if opcode.is_some() {
        Some(if let Some((table, key)) = &address {
            let register = compiler.register(table.value.clone(), span)?;
            emit_table(
                compiler,
                0x40,
                &[register, table.index, *key],
                2,
                table.index,
                span,
            )?;
            register
        } else {
            local.unwrap()
        })
    } else {
        None
    };
    let expected = if matches!(opcode, Some(0x34 | 0x35)) {
        &Type::U64
    } else {
        &ty
    };
    let mut source = expr::compile_expr(compiler, value, Some(expected))?;
    if let Some(opcode) = opcode {
        source = expr::binary_instruction(compiler, opcode, prior.unwrap(), source, span)?;
    }
    if let Some((table, key)) = address {
        emit_table(
            compiler,
            0x43,
            &[table.index, key, source],
            0,
            table.index,
            span,
        )?;
    } else {
        compiler.emit(0x03, &[local.unwrap(), source], span)?;
    }
    Ok(())
}

fn constant(compiler: &mut Compiler, ty: Type, bytes: Vec<u8>, span: Span) -> Result<u16> {
    let index = pool_index(compiler.constants.len(), span)?;
    let register = compiler.register(ty.clone(), span)?;
    compiler.constants.push((ty, bytes));
    compiler.emit(0x01, &[register, index], span)?;
    Ok(register)
}

fn path_name(expression: &Expr) -> Result<String> {
    if let Expr::Path(path) = expression
        && path.qself.is_none()
        && let Some(name) = path.path.get_ident()
    {
        return Ok(name.to_string());
    }
    Err(syn::Error::new(
        expression.span(),
        "expected a VM name, not a Rust path or call",
    ))
}

fn table_info(compiler: &Compiler, expression: &Expr) -> Result<TableInfo> {
    let name = path_name(expression)?;
    compiler.tables.get(&name).cloned().ok_or_else(|| {
        syn::Error::new(
            expression.span(),
            "unknown table; declare it in tables { ... }",
        )
    })
}

fn address(compiler: &mut Compiler, expression: &Expr) -> Result<(TableInfo, u16)> {
    let Expr::Index(index) = expression else {
        return Err(syn::Error::new(expression.span(), "expected table[key]"));
    };
    let table = table_info(compiler, &index.expr)?;
    let key = expr::compile_expr(compiler, &index.index, Some(&table.key))?;
    Ok((table, key))
}

fn emit_table(
    compiler: &mut Compiler,
    opcode: u8,
    operands: &[u16],
    offset: usize,
    table: u16,
    span: Span,
) -> Result<usize> {
    let index = compiler.emit(opcode, operands, span)?;
    compiler.instructions[index].table = Some((offset, table));
    Ok(index)
}

fn immediate(expression: &Expr, max: u64) -> Result<u64> {
    if let Expr::Lit(literal) = expression
        && let syn::Lit::Int(integer) = &literal.lit
    {
        let value = integer.base10_parse::<u64>()?;
        if value <= max && matches!(integer.suffix(), "" | "u16" | "u32" | "u64") {
            return Ok(value);
        }
    }
    Err(syn::Error::new(
        expression.span(),
        format!("expected an unsigned integer literal no greater than {max}"),
    ))
}

fn require_shape(actual: &Type, expected: &Type, span: Span) -> Result<()> {
    if !actual.same_shape(expected) {
        return Err(syn::Error::new(
            span,
            format!("VM type mismatch: expected {expected:?}, found {actual:?}"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(source: &str) -> Result<TokenStream> {
        compile(syn::parse_str(&format!("::blop_db; {source}"))?)
    }

    #[test]
    fn rejects_unsupported_computation_and_implicit_captures() {
        for (source, diagnostic) in [
            ("return outside;", "external values must be declared"),
            ("return $outside;", "unknown capture"),
            ("return $1;", "expected a capture name"),
            ("loop {}", "unsupported VM expression"),
            ("while true {}", "unsupported VM expression"),
            ("for x in 0..2 {}", "unsupported VM expression"),
            (
                "return arbitrary_rust_function();",
                "Rust function calls are not allowed",
            ),
            ("return std::time::Instant::now();", "expected a VM name"),
            ("println!(\"hello\");", "Rust items and macros cannot run"),
            ("fn helper() {}", "Rust items and macros cannot run"),
            ("return 1 as u64;", "unsupported VM expression"),
            (
                "let x = if true { 1 } else { 2 };",
                "unsupported VM expression",
            ),
        ] {
            let error = check(source).unwrap_err().to_string();
            assert!(error.contains(diagnostic), "{source}: {error}");
        }
    }

    #[test]
    fn rejects_invalid_types_bindings_and_control_flow() {
        for (source, diagnostic) in [
            ("captures { unbounded: u64 = 1 }", "reserved VM keyword"),
            ("captures { abort: i64 = 1 }", "reserved VM keyword"),
            ("let unbounded = 1;", "reserved VM keyword"),
            ("let abort = 1;", "reserved VM keyword"),
            (
                "tables { unbounded: u64 => i64 = 1 }",
                "reserved VM keyword",
            ),
            ("#[cfg(any())] let x = 1;", "attributes are not supported"),
            ("#[allow(unused)] return;", "attributes are not supported"),
            ("if 1 { abort; }", "VM type mismatch"),
            ("require(1);", "VM type mismatch"),
            ("require();", "require(condition"),
            ("abort(4294967296);", "unsigned integer literal"),
            ("let code = 1; abort(code);", "unsigned integer literal"),
            ("-> u64 { return -1; }", "VM type mismatch"),
            ("return 1u64 + 1i64;", "VM type mismatch"),
            ("return true + false;", "invalid VM operands"),
            (
                "return bit_and(true, false);",
                "bitwise intrinsics require integers",
            ),
            ("return 1u32;", "only i64 and u64"),
            ("return 9223372036854775808;", "number too large"),
            ("return -9223372036854775809;", "outside i64"),
            ("return (1,).1;", "tuple field index out of bounds"),
            ("let x; return x;", "must be initialized"),
            ("let x = 1; x = 2;", "immutable local"),
            ("captures { x: i64 = 1 } x = 2;", "immutable local"),
            ("if true { let x = 1; } return x;", "unknown VM local"),
            ("captures { x: i64 = 1, x: i64 = 2 }", "duplicate capture"),
            ("let mut x = 1; x = true;", "VM type mismatch"),
            ("-> i64 { if true { return 1; } }", "not every path"),
            (
                "if true { return 1; } else { return false; }",
                "VM type mismatch",
            ),
            ("-> i64 { return; }", "return requires a value"),
            ("return; return;", "unreachable VM statement"),
            ("abort; require(true);", "unreachable VM statement"),
            (
                "if true { return; } else { abort; } return;",
                "unreachable VM statement",
            ),
        ] {
            let error = check(source).unwrap_err().to_string();
            assert!(error.contains(diagnostic), "{source}: {error}");
        }
    }

    #[test]
    fn rejects_invalid_schemas_and_scan_operands() {
        for (source, diagnostic) in [
            (
                "captures { x: rows<u64, i64, 1> = value }",
                "Rows in a schema/capture",
            ),
            (
                "-> (rows<u64, i64, 1>,) { abort; }",
                "Rows in a schema/capture",
            ),
            (
                "captures { x: bytes<16777213> = value }",
                "exceeds ISA 1 bounds",
            ),
            ("tables { t: bytes<512> => i64 = 1 }", "key schema exceeds"),
            (
                "tables { t: u64 => rows<u64, i64, 1> = 1 }",
                "Rows in a schema/capture",
            ),
            ("return missing[0];", "unknown table"),
            (
                "tables { t: u64 => i64 = 1 } t[true] = 1;",
                "VM type mismatch",
            ),
            (
                "tables { t: u64 => i64 = 1 } insert(t[1], true);",
                "VM type mismatch",
            ),
            (
                "tables { t: u64 => i64 = 1 } scan_bounded(t, unbounded, unbounded, 1, 1, 1);",
                "cannot be inclusive",
            ),
            (
                "tables { t: u64 => i64 = 1 } scan_bounded(t, 0, 1, 4, 1, 1);",
                "unsigned integer literal",
            ),
            (
                "tables { t: u64 => i64 = 1 } scan_bounded(t, 0, 1, 0, 65536, 1);",
                "unsigned integer literal",
            ),
            (
                "tables { t: u64 => i64 = 1 } let n = 1; scan_bounded(t, 0, 1, 0, n, 1);",
                "unsigned integer literal",
            ),
            (
                "tables { t: u64 => i64 = 1 } let r = scan_bounded(t, 0, 1, 0, 1, 1); return r == r;",
                "invalid VM operands",
            ),
        ] {
            let error = check(source).unwrap_err().to_string();
            assert!(error.contains(diagnostic), "{source}: {error}");
        }
    }

    #[test]
    fn enforces_type_depth_tuple_arity_and_pool_limits() {
        let nested = |count| format!("{}u64{}", "(".repeat(count), ",)".repeat(count));
        check(&format!("-> {} {{ abort; }}", nested(15))).unwrap();
        assert!(check(&format!("-> {} {{ abort; }}", nested(16))).is_err());
        check(&format!("return ({});", "0,".repeat(256))).unwrap();
        assert!(check(&format!("return ({});", "0,".repeat(257))).is_err());
        assert_eq!(pool_index(65_534, Span::call_site()).unwrap(), 65_534);
        assert!(pool_index(65_535, Span::call_site()).is_err());
        check("tables { t: bytes<511> => i64 = 1 } abort;").unwrap();
    }

    #[test]
    fn accepts_opcode_aliases_and_rust_like_operator_precedence() {
        check(
            "captures { u: u64 = 2, s: i64 = -1, b: bool = true } {
            add_checked(u, 1); sub_checked(u, 1); mul_checked(u, 1);
            div_checked(u, 1); rem_checked(u, 1); neg_checked(s);
            eq(u, 1); lt(u, 1); le(u, 1); gt(u, 1); ge(u, 1);
            bool_not(b); bit_not(u); bit_and(u, 1); bit_or(u, 1); bit_xor(u, 1);
            require(b == (u > 1) || !b && 1 + 2 * 3 != 7);
            return;
        }",
        )
        .unwrap();
    }
}
