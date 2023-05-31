use quote::{quote, quote_spanned, ToTokens};
use syn::{parse_macro_input, DeriveInput, Generics, GenericParam, parse_quote, Data, Fields};
use syn::spanned::Spanned;
use crate::traits::writable::{convert_writable_source, convert_writable_value_source, writable_operation_count, writable_source_length};

mod traits;

#[doc(hidden)]
#[proc_macro_attribute]
pub fn test(_attr: proc_macro::TokenStream, input: proc_macro::TokenStream) -> proc_macro::TokenStream {
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


#[proc_macro_derive(WritableBins)]
pub fn writable_bins(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let mut input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;
    for param in &mut input.generics.params {
        if let GenericParam::Type(ref mut type_param) = *param {
            type_param.bounds.push(parse_quote!(aerospike::WritableValue));
        }
    }
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let vals = convert_writable_source(&input.data);
    let lengths = writable_source_length(&input.data);
    let op_count = writable_operation_count(&input.data);

    let expanded = quote! {
        impl #impl_generics WritableBins for #name #ty_generics #where_clause {
            fn write_as_bins(&self, buffer: &mut aerospike::Buffer, op_type: u8) -> aerospike::errors::Result<()>{
                // Vec for bin Values
                // Bins token Stream
                #vals

                Ok(())
            }

            fn writable_bins_size(&self) -> usize {
                let mut size: usize = 0;
                #lengths
                size
            }

            fn writable_bins_count(&self) -> usize {
                #op_count
            }
        }
    };
    proc_macro::TokenStream::from(expanded)
}

#[proc_macro_derive(WritableValue)]
pub fn writable_value(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let mut input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;
    for param in &mut input.generics.params {
        if let GenericParam::Type(ref mut type_param) = *param {
            type_param.bounds.push(parse_quote!(aerospike::WritableValue));
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