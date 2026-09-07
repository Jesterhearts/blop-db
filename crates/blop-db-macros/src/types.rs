use proc_macro2::Span;
use syn::Ident;
use syn::LitInt;
use syn::Result;
use syn::Token;
use syn::parenthesized;
use syn::parse::ParseStream;

pub const VALUE_LIMIT: u64 = 16 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Type {
    Unit,
    Bool,
    I64,
    U64,
    Bytes(u32),
    String(u32),
    Tuple(Vec<Type>),
    Rows(Box<Type>, Box<Type>, u32),
}

impl Type {
    pub fn is_integer(&self) -> bool {
        matches!(self, Self::I64 | Self::U64)
    }

    pub fn same_shape(
        &self,
        other: &Self,
    ) -> bool {
        match (self, other) {
            (Self::Bytes(_), Self::Bytes(_)) | (Self::String(_), Self::String(_)) => true,
            (Self::Tuple(a), Self::Tuple(b)) => {
                a.len() == b.len() && a.iter().zip(b).all(|(a, b)| a.same_shape(b))
            }
            (Self::Rows(ak, av, _), Self::Rows(bk, bv, _)) => {
                ak.same_shape(bk) && av.same_shape(bv)
            }
            _ => self == other,
        }
    }

    pub fn union(
        &self,
        other: &Self,
    ) -> Self {
        match (self, other) {
            (Self::Bytes(a), Self::Bytes(b)) => Self::Bytes((*a).max(*b)),
            (Self::String(a), Self::String(b)) => Self::String((*a).max(*b)),
            (Self::Tuple(a), Self::Tuple(b)) => {
                Self::Tuple(a.iter().zip(b).map(|(a, b)| a.union(b)).collect())
            }
            (Self::Rows(ak, av, a), Self::Rows(bk, bv, b)) => {
                Self::Rows(Box::new(ak.union(bk)), Box::new(av.union(bv)), (*a).max(*b))
            }
            _ => self.clone(),
        }
    }

    pub fn descriptor(&self) -> Vec<u8> {
        let mut bytes = vec![1, 0];
        encode_node(self, &mut bytes);
        bytes
    }
}

pub fn parse_type(input: ParseStream<'_>) -> Result<Type> {
    parse_type_at_depth(input, 1)
}

fn parse_type_at_depth(
    input: ParseStream<'_>,
    depth: u8,
) -> Result<Type> {
    let span = input.span();
    if depth > 16 {
        return Err(syn::Error::new(span, "VM types may nest at most 16 levels"));
    }
    let ty = if input.peek(syn::token::Paren) {
        let fields;
        parenthesized!(fields in input);
        if fields.is_empty() {
            Type::Unit
        } else {
            let mut types = vec![parse_type_at_depth(&fields, depth + 1)?];
            fields.parse::<Token![,]>()?;
            while !fields.is_empty() {
                types.push(parse_type_at_depth(&fields, depth + 1)?);
                if fields.is_empty() {
                    break;
                }
                fields.parse::<Token![,]>()?;
            }
            Type::Tuple(types)
        }
    } else {
        let name: Ident = input.parse()?;
        match name.to_string().as_str() {
            "bool" => Type::Bool,
            "i64" => Type::I64,
            "u64" => Type::U64,
            "bytes" | "string" => {
                input.parse::<Token![<]>()?;
                let bound = input.parse::<LitInt>()?.base10_parse()?;
                input.parse::<Token![>]>()?;
                if name == "bytes" {
                    Type::Bytes(bound)
                } else {
                    Type::String(bound)
                }
            }
            "tuple" => {
                input.parse::<Token![<]>()?;
                input.parse::<Token![>]>()?;
                Type::Tuple(vec![])
            }
            "rows" => {
                input.parse::<Token![<]>()?;
                let key = parse_type_at_depth(input, depth + 1)?;
                input.parse::<Token![,]>()?;
                let value = parse_type_at_depth(input, depth + 1)?;
                input.parse::<Token![,]>()?;
                let bound = input.parse::<LitInt>()?.base10_parse()?;
                input.parse::<Token![>]>()?;
                Type::Rows(Box::new(key), Box::new(value), bound)
            }
            _ => return Err(syn::Error::new(name.span(), "unsupported VM type")),
        }
    };
    validate_type(&ty, true, span)?;
    Ok(ty)
}

pub fn validate_type(
    ty: &Type,
    allow_rows: bool,
    span: Span,
) -> Result<()> {
    fn size(
        ty: &Type,
        depth: u8,
        allow_rows: bool,
    ) -> Option<u64> {
        if depth > 16 {
            return None;
        }
        let length = match ty {
            Type::Unit => 0,
            Type::Bool => 1,
            Type::I64 | Type::U64 => 8,
            Type::Bytes(n) | Type::String(n) => 4 + u64::from(*n),
            Type::Tuple(fields) if fields.len() <= 256 => {
                let mut length = 0u64;
                for field in fields {
                    length = length.checked_add(size(field, depth + 1, false)?)?;
                }
                length
            }
            Type::Rows(key, value, count) if allow_rows && *count <= 65_535 => {
                let row = size(key, depth + 1, false)? + size(value, depth + 1, false)?;
                row.checked_mul(u64::from(*count))?.checked_add(4)?
            }
            _ => return None,
        };
        (length <= VALUE_LIMIT).then_some(length)
    }
    if size(ty, 1, allow_rows).is_none() || ty.descriptor().len() > 65_536 {
        return Err(syn::Error::new(
            span,
            "type exceeds ISA 1 bounds, nesting, or tuple limits, or uses Rows in a schema/capture",
        ));
    }
    Ok(())
}

pub fn key_size(ty: &Type) -> u64 {
    match ty {
        Type::Unit => 0,
        Type::Bool => 1,
        Type::I64 | Type::U64 => 8,
        Type::Bytes(n) | Type::String(n) => 2 * u64::from(*n) + 2,
        Type::Tuple(fields) => fields.iter().map(key_size).sum(),
        Type::Rows(..) => u64::MAX,
    }
}

fn encode_node(
    ty: &Type,
    out: &mut Vec<u8>,
) {
    match ty {
        Type::Unit => out.push(0),
        Type::Bool => out.push(1),
        Type::I64 => out.push(2),
        Type::U64 => out.push(3),
        Type::Bytes(n) | Type::String(n) => {
            out.push(if matches!(ty, Type::Bytes(_)) { 4 } else { 5 });
            out.extend(n.to_le_bytes());
        }
        Type::Tuple(fields) => {
            out.push(6);
            out.extend((fields.len() as u16).to_le_bytes());
            for field in fields {
                encode_node(field, out);
            }
        }
        Type::Rows(key, value, n) => {
            out.push(0x20);
            out.extend(n.to_le_bytes());
            encode_node(key, out);
            encode_node(value, out);
        }
    }
}
