use proc_macro::TokenStream;

mod bytecode;
mod compile;
mod syntax;
mod types;

#[proc_macro]
pub fn compile_tx(input: TokenStream) -> TokenStream {
    syn::parse(input)
        .and_then(compile::compile)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
