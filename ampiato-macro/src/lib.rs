use proc_macro::TokenStream;


mod macros;

#[proc_macro_attribute]
pub fn tem_fn(_attrs: TokenStream, input: TokenStream) -> TokenStream {
    macros::tem_fn(input.into()).into()
}
