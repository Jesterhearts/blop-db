use std::collections::HashSet;

use proc_macro2::Group;
use proc_macro2::TokenStream;
use proc_macro2::TokenTree;
use syn::Block;
use syn::Expr;
use syn::Ident;
use syn::Path;
use syn::Result;
use syn::Token;
use syn::braced;
use syn::parse::Parse;
use syn::parse::ParseStream;

use crate::types::Type;
use crate::types::key_size;
use crate::types::parse_type;
use crate::types::validate_type;

pub struct Capture {
    pub name: Ident,
    pub ty: Type,
    pub value: Expr,
}

pub struct Table {
    pub name: Ident,
    pub key: Type,
    pub value: Type,
    pub id: Expr,
}

pub struct Program {
    pub runtime: Path,
    pub captures: Vec<Capture>,
    pub tables: Vec<Table>,
    pub result: Option<Type>,
    pub body: Block,
}

impl Parse for Program {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let runtime = input.parse()?;
        input.parse::<Token![;]>()?;
        let mut captures = Vec::new();
        let mut tables = Vec::new();
        let mut names = HashSet::new();
        if keyword(input, "captures") {
            input.parse::<Ident>()?;
            let fields;
            braced!(fields in input);
            while !fields.is_empty() {
                let name = unique_name(&fields, &mut names)?;
                fields.parse::<Token![:]>()?;
                let ty = parse_type(&fields)?;
                validate_type(&ty, false, name.span())?;
                fields.parse::<Token![=]>()?;
                captures.push(Capture {
                    name,
                    ty,
                    value: fields.parse()?,
                });
                if fields.is_empty() {
                    break;
                }
                fields.parse::<Token![,]>()?;
            }
        }
        if keyword(input, "tables") {
            input.parse::<Ident>()?;
            let fields;
            braced!(fields in input);
            while !fields.is_empty() {
                let name = unique_name(&fields, &mut names)?;
                fields.parse::<Token![:]>()?;
                let key = parse_type(&fields)?;
                fields.parse::<Token![=>]>()?;
                let value = parse_type(&fields)?;
                validate_type(&key, false, name.span())?;
                validate_type(&value, false, name.span())?;
                if key_size(&key) > 1024 {
                    return Err(syn::Error::new(
                        name.span(),
                        "table key schema exceeds 1024 bytes",
                    ));
                }
                fields.parse::<Token![=]>()?;
                tables.push(Table {
                    name,
                    key,
                    value,
                    id: fields.parse()?,
                });
                if fields.is_empty() {
                    break;
                }
                fields.parse::<Token![,]>()?;
            }
        }
        let result = if input.peek(Token![->]) {
            input.parse::<Token![->]>()?;
            Some(parse_type(input)?)
        } else {
            None
        };
        let capture_names = captures.iter().map(|c| c.name.to_string()).collect();
        let tokens = strip_capture_markers(input.parse()?, &capture_names)?;
        let body = if tokens.clone().into_iter().count() == 1
            && matches!(tokens.clone().into_iter().next(), Some(TokenTree::Group(g)) if g.delimiter() == proc_macro2::Delimiter::Brace)
        {
            syn::parse2(tokens)?
        } else {
            syn::parse2(quote::quote!({ #tokens }))?
        };
        Ok(Self {
            runtime,
            captures,
            tables,
            result,
            body,
        })
    }
}

fn keyword(
    input: ParseStream<'_>,
    name: &str,
) -> bool {
    input
        .fork()
        .parse::<Ident>()
        .is_ok_and(|ident| ident == name)
}

fn unique_name(
    input: ParseStream<'_>,
    names: &mut HashSet<String>,
) -> Result<Ident> {
    let name: Ident = input.parse()?;
    validate_name(&name)?;
    if !names.insert(name.to_string()) {
        return Err(syn::Error::new(
            name.span(),
            "duplicate capture or table name",
        ));
    }
    Ok(name)
}

pub fn validate_name(name: &Ident) -> Result<()> {
    if matches!(
        name.to_string().trim_start_matches("r#"),
        "abort" | "unbounded"
    ) {
        return Err(syn::Error::new(
            name.span(),
            "reserved VM keyword cannot be used as a binding name",
        ));
    }
    Ok(())
}

fn strip_capture_markers(
    tokens: TokenStream,
    names: &HashSet<String>,
) -> Result<TokenStream> {
    let mut out = TokenStream::new();
    let mut tokens = tokens.into_iter();
    while let Some(token) = tokens.next() {
        match token {
            TokenTree::Punct(punct) if punct.as_char() == '#' => {
                return Err(syn::Error::new(
                    punct.span(),
                    "attributes are not supported in VM programs",
                ));
            }
            TokenTree::Punct(punct) if punct.as_char() == '$' => {
                let Some(TokenTree::Ident(name)) = tokens.next() else {
                    return Err(syn::Error::new(
                        punct.span(),
                        "expected a capture name after `$`",
                    ));
                };
                if !names.contains(&name.to_string()) {
                    return Err(syn::Error::new(name.span(), "unknown capture"));
                }
                // A marker remains distinct from a shadowing local with the
                // same name.
                out.extend(quote::quote_spanned!(name.span()=> __capture(#name)));
            }
            TokenTree::Group(group) => {
                let mut replacement = Group::new(
                    group.delimiter(),
                    strip_capture_markers(group.stream(), names)?,
                );
                replacement.set_span(group.span());
                out.extend([TokenTree::Group(replacement)]);
            }
            token => out.extend([token]),
        }
    }
    Ok(out)
}
