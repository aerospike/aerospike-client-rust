extern crate proc_macro;
use proc_macro::TokenStream;
use quote::{quote, quote_spanned};
use syn::{parse_macro_input, DeriveInput, Generics, GenericParam, parse_quote, Data, Fields};
use syn::spanned::Spanned;

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

#[proc_macro_derive(ToBins)]
pub fn aerospike_bins(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;
    let generics = add_bins_trait_bounds(input.generics);
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let vals = convert_bin_source(&input.data);

    let expanded = quote! {
        impl #impl_generics ToBins for #name #ty_generics #where_clause {
            fn to_bins(&self) -> Vec<aerospike::Bin> {
                // Vec for bin Values
                let mut bins: Vec<aerospike::Bin> = Vec::new();
                // Bins token Stream
                #vals
                bins
            }
        }
    };
    TokenStream::from(expanded)
}

fn add_bins_trait_bounds(mut generics: Generics) -> Generics {
    for param in &mut generics.params {
        if let GenericParam::Type(ref mut type_param) = *param {
            type_param.bounds.push(parse_quote!(aerospike::ToBins));
        }
    }
    generics
}

fn convert_bin_source(data: &Data) -> proc_macro2::TokenStream {
    match *data {
        Data::Struct(ref data) => {
            match data.fields {
                Fields::Named(ref fields) => {
                    let recurse = fields.named.iter().map(|f| {
                        let name = &f.ident;
                        let name_str = &name.clone().unwrap().to_string();
                        // Build the bin Token Stream. `bins` references the vec from the expanded Token Stream. Using ToValue instead of Value to be able to parse sub-structs
                        quote_spanned! {f.span()=>
                            bins.push(aerospike::Bin::new(#name_str, aerospike::ToValue::to_value(&self.#name)));
                        }
                    });
                    quote! {
                        #(#recurse)*
                    }
                }
                _ => unimplemented!()
            }
        }
        Data::Enum(_) | Data::Union(_) => unimplemented!(),
    }
}


#[proc_macro_derive(ToValue)]
pub fn aerospike_value(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;
    let generics = add_value_trait_bounds(input.generics);
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let vals = convert_value_source(&input.data);

    let expanded = quote! {
        use std::collections::HashMap as AVHashMap;
        impl #impl_generics ToValue for #name #ty_generics #where_clause {
            fn to_value(&self) -> Value {
                // HashMap to collect the fields. Custom type name to not overlap with users imports
                let mut map: AVHashMap<aerospike::Value, aerospike::Value> = AVHashMap::new();
                // Values Token Stream
                #vals
                Value::from(map)
            }
        }
    };
    TokenStream::from(expanded)
}

fn add_value_trait_bounds(mut generics: Generics) -> Generics {
    for param in &mut generics.params {
        if let GenericParam::Type(ref mut type_param) = *param {
            type_param.bounds.push(parse_quote!(aerospike::ToValue));
        }
    }
    generics
}

fn convert_value_source(data: &Data) -> proc_macro2::TokenStream {
    match *data {
        Data::Struct(ref data) => {
            match data.fields {
                Fields::Named(ref fields) => {
                    let recurse = fields.named.iter().map(|f| {
                        let name = &f.ident;
                        let name_str = &name.clone().unwrap().to_string();
                        // `map` references the HashMap from the expanded TokenStream.
                        quote_spanned! {f.span()=>
                            map.insert(aerospike::Value::from(#name_str), aerospike::ToValue::to_value(&self.#name));
                        }
                    });
                    quote! {
                        #(#recurse)*
                    }
                }
                _ => unimplemented!()
            }
        }
        Data::Enum(_) | Data::Union(_) => unimplemented!(),
    }
}