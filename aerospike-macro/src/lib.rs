use crate::traits::readable::{convert_readable_bins, convert_readable_value_source};
use crate::traits::writable::{build_writable, convert_writable_value_source};
use quote::quote;
use syn::{parse_macro_input, parse_quote, DeriveInput, GenericParam};

mod traits;

#[doc(hidden)]
#[proc_macro_attribute]
pub fn test(
    _attr: proc_macro::TokenStream,
    input: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
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

#[proc_macro_derive(WritableBins, attributes(aerospike))]
pub fn writable_bins(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let mut input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;
    for param in &mut input.generics.params {
        if let GenericParam::Type(ref mut type_param) = *param {
            type_param
                .bounds
                .push(parse_quote!(aerospike::WritableValue));
        }
    }
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let ops = build_writable(&input.data);

    let expanded = quote! {
        impl #impl_generics WritableBins for #name #ty_generics #where_clause {
            #ops
        }
    };
    proc_macro::TokenStream::from(expanded)
}

#[proc_macro_derive(WritableValue, attributes(aerospike))]
pub fn writable_value(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let mut input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;
    for param in &mut input.generics.params {
        if let GenericParam::Type(ref mut type_param) = *param {
            type_param
                .bounds
                .push(parse_quote!(aerospike::derive::writable::WritableValue));
        }
    }
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let vals = convert_writable_value_source(&input.data);

    let expanded = quote! {
        impl #impl_generics WritableValue for #name #ty_generics #where_clause {
            fn write_as_value(&self, buffer: &mut Option<&mut aerospike::Buffer>) -> usize{
                // Vec for bin Values
                // Bins token Stream
                let mut size: usize = 0;
                #vals
                size
            }

            fn writable_value_particle_type(&self) -> aerospike::ParticleType {
                aerospike::ParticleType::MAP
            }
        }
    };
    proc_macro::TokenStream::from(expanded)
}

#[proc_macro_derive(ReadableBins, attributes(aerospike))]
pub fn readable_bins(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let mut input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;
    for param in &mut input.generics.params {
        if let GenericParam::Type(ref mut type_param) = *param {
            type_param
                .bounds
                .push(parse_quote!(aerospike::derive::readable::ReadableValue));
        }
    }
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let vals = convert_readable_bins(&input.data, &name);

    let expanded = quote! {
        impl #impl_generics ReadableBins for #name #ty_generics #where_clause {
            fn read_bins_from_bytes(data_points: &mut HashMap<String, aerospike::derive::readable::PreParsedBin>) -> aerospike::errors::Result<Self>{
                #vals
            }

        }
    };
    proc_macro::TokenStream::from(expanded)
}

#[proc_macro_derive(ReadableValue, attributes(aerospike))]
pub fn readable_value(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let mut input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;
    for param in &mut input.generics.params {
        if let GenericParam::Type(ref mut type_param) = *param {
            type_param
                .bounds
                .push(parse_quote!(aerospike::derive::readable::ReadableValue));
        }
    }
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let vals = convert_readable_value_source(&input.data, &name);

    let expanded = quote! {
        impl #impl_generics ReadableValue for #name #ty_generics #where_clause {
            #vals
        }
    };
    proc_macro::TokenStream::from(expanded)
}
