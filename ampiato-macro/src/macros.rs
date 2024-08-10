use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::spanned::Spanned;

pub fn tem_fn(input: TokenStream) -> TokenStream {
    let tem_fn: syn::ItemFn = syn::parse2(input).unwrap();

    let sig = tem_fn.sig.clone();

    // if let ReturnType::Type(_, ty) = &mut sig.output {
    //     *ty = syn::parse_quote! { V<#ty> };
    // }
    
    let body = &tem_fn.block;
    let name = sig.ident.clone();
    let name = name.to_string();

    let mut selector_parts: Vec<String> = vec![];
    let mut selector_varnames: Vec<syn::Ident> = vec![];
    for arg in sig.inputs.iter().skip(1) {
        match arg {
            syn::FnArg::Receiver(_) => panic!("`tem_fn` can only be applied to function that don't take `self` as the first argument"),
            syn::FnArg::Typed(pat) => {
                let syn::PatType { pat, ty, .. } = pat;
                let name = quote! { #ty }.to_string();
                selector_parts.push(name.clone());

                let name = quote! { #pat }.to_string();
                selector_varnames.push(syn::Ident::new(&name, pat.span()));
            }
        };
    }
    if selector_parts.is_empty() {
        panic!("`tem_fn` must have at least one argument");
    }
    if let Some(n) = selector_parts.last() {
        if n != "Time" {
            panic!("`tem_fn` must have `Time` as the last argument");
        }
    }
    selector_parts.pop();
    selector_varnames.pop();
    let selector = if selector_parts.is_empty() {
        quote! { Selector::Unit(()) }
    } else {
        let variant_name = selector_parts.join("");
        let variant = syn::Ident::new(&variant_name, Span::call_site());

        quote! { Selector::#variant( #( #selector_varnames ), * ) }
    };

    let code = quote! {
        #[allow(non_snake_case)]
        #sig {
            db.register_fn(#name, #selector, t, |db| #body)
        }
    };

    code.into()
}

