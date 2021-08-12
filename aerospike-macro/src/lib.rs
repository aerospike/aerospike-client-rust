extern crate proc_macro;
use proc_macro::TokenStream;
use quote::quote;

#[doc(hidden)]
#[proc_macro_attribute]
pub fn test(_attr: TokenStream, input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::ItemFn);

    let ret = &input.sig.output;
    let name = &input.sig.ident;
    let body = &input.block;
    let attrs = &input.attrs;

    let result = if cfg!(feature = "rt-tokio") {
        quote! {
            #[test]
            #(#attrs)*
            fn #name() #ret {
                ::aerospike_rt::tokio::runtime::Runtime::new().unwrap().block_on( async {#body} )
            }
        }
    } else if cfg!(feature = "rt-async-std") {
        quote! {
            #[test]
            #(#attrs)*
            fn #name() #ret {
                ::aerospike_rt::async_std::task::block_on( async {#body} )
            }
        }
    } else {
        panic!("No runtime selected!");
    };

    result.into()
}
