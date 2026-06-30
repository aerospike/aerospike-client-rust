extern crate proc_macro;

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::punctuated::Punctuated;
use syn::token::Comma;
use syn::{parse_macro_input, Attribute, Data, DeriveInput, Fields, Meta, Path, Type};

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
                let _ = env_logger::try_init();

                // ::aerospike_rt::tokio::runtime::Runtime::new().unwrap().block_on( async {#body} )
                // Use a shared runtime for the tests and the client:
                crate::common::RUNTIME.block_on( async {#body} )
            }
        }
    } else if cfg!(feature = "rt-async-std") {
        quote! {
            #[test]
            #(#attrs)*
            fn #name() #ret {
                let _ = env_logger::try_init();

                // Runtime is already shared for async_std
                ::aerospike_rt::async_std::task::block_on( async {#body} )
            }
        }
    } else {
        panic!("No runtime selected!");
    };

    result.into()
}

/// Per-field directives parsed from `#[config(...)]` attributes.
#[derive(Default)]
struct ConfigFieldAttrs {
    /// Exclude the field from the generated config entirely.
    skip: bool,
    /// Treat the field as a nested config (`Option<<Type>Config>`, serde-flattened).
    flatten: bool,
    /// Apply only in `merge_static_into` (not the dynamic `merge_into`).
    is_static: bool,
    /// `#[serde(rename = "...")]` to attach to the generated config field.
    rename: Option<String>,
    /// Conversion applied during merge: `target.field = with(value)`.
    with: Option<Path>,
}

/// Parses every `#[config(...)]` attribute on a field into a single directive set.
/// Supports comma-separated metas, e.g. `#[config(rename = "x", with = path::fn)]`.
fn parse_config_attrs(attrs: &[Attribute]) -> Result<ConfigFieldAttrs, syn::Error> {
    let mut out = ConfigFieldAttrs::default();
    for attr in attrs.iter().filter(|a| a.path().is_ident("config")) {
        let metas = attr.parse_args_with(Punctuated::<Meta, Comma>::parse_terminated)?;
        for meta in metas {
            match meta {
                Meta::Path(ref p) if p.is_ident("skip") => out.skip = true,
                Meta::Path(ref p) if p.is_ident("flatten") => out.flatten = true,
                Meta::Path(ref p) if p.is_ident("startup") => out.is_static = true,
                Meta::NameValue(ref nv) if nv.path.is_ident("rename") => {
                    if let syn::Expr::Lit(syn::ExprLit {
                        lit: syn::Lit::Str(s),
                        ..
                    }) = &nv.value
                    {
                        out.rename = Some(s.value());
                    } else {
                        return Err(syn::Error::new_spanned(
                            &nv.value,
                            "config(rename) expects a string literal",
                        ));
                    }
                }
                Meta::NameValue(ref nv) if nv.path.is_ident("with") => {
                    if let syn::Expr::Path(syn::ExprPath { path, .. }) = &nv.value {
                        out.with = Some(path.clone());
                    } else {
                        return Err(syn::Error::new_spanned(
                            &nv.value,
                            "config(with) expects a path to a conversion function",
                        ));
                    }
                }
                other => {
                    return Err(syn::Error::new_spanned(
                        other,
                        "unsupported config(...) directive (expected one of: skip, flatten, \
                         startup, rename = \"...\", with = path)",
                    ));
                }
            }
        }
    }
    Ok(out)
}

/// Derives a `<Name>Config` companion struct for a policy struct: every retained
/// field becomes `Option<T>`, deserializable (serde) and mergeable back onto the
/// original via `merge_into` (dynamic fields) / `merge_static_into` (fields tagged
/// `#[config(startup)]`).
///
/// Field attributes:
/// - `#[config(skip)]` — omit the field from the config.
/// - `#[config(flatten)]` — nest the field's own `<Type>Config` (serde-flattened),
///   so the field's keys appear inline in the parent.
/// - `#[config(startup)]` — apply only in `merge_static_into` (honored once at
///   construction; `static` is a reserved keyword so it can't be the attr name).
/// - `#[config(rename = "key")]` — serde key name for the generated field.
/// - `#[config(with = path::fn)]` — convert on merge: `target.field = fn(value)`.
#[proc_macro_derive(Config, attributes(config))]
pub fn derive_config(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let struct_name = input.ident;
    let generics = input.generics;
    let struct_attrs = input.attrs;

    let config_name = format_ident!("{}Config", struct_name);
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let fields = match input.data {
        Data::Struct(data_struct) => match data_struct.fields {
            Fields::Named(fields_named) => fields_named.named,
            _ => {
                return syn::Error::new_spanned(
                    struct_name,
                    "Config can only be derived for structs with named fields",
                )
                .to_compile_error()
                .into()
            }
        },
        _ => {
            return syn::Error::new_spanned(struct_name, "Config can only be derived for structs")
                .to_compile_error()
                .into()
        }
    };

    let mut config_fields = Vec::new();
    let mut dynamic_merge = Vec::new();
    let mut static_merge = Vec::new();

    for field in fields.iter() {
        let name = &field.ident;
        let ty = &field.ty;

        let cfg = match parse_config_attrs(&field.attrs) {
            Ok(c) => c,
            Err(e) => return e.to_compile_error().into(),
        };

        if cfg.skip {
            continue;
        }

        // Forward existing #[serde(...)] attrs, plus a rename if requested.
        let mut serde_attrs: Vec<proc_macro2::TokenStream> = field
            .attrs
            .iter()
            .filter(|a| a.path().is_ident("serde"))
            .map(|a| quote!(#a))
            .collect();
        if let Some(rename) = &cfg.rename {
            serde_attrs.push(quote!(#[serde(rename = #rename)]));
        }

        if cfg.flatten {
            let config_ty = match ty {
                Type::Path(type_path) => {
                    let ident = &type_path.path.segments.last().unwrap().ident;
                    format_ident!("{}Config", ident)
                }
                _ => {
                    return syn::Error::new_spanned(ty, "config(flatten) only supports path types")
                        .to_compile_error()
                        .into()
                }
            };

            config_fields.push(quote! {
                #[serde(flatten)]
                #(#serde_attrs)*
                pub #name: ::core::option::Option<#config_ty>
            });

            // A flattened (nested) policy is treated as dynamic; its own
            // generated merge handles any static fields it may contain.
            dynamic_merge.push(quote! {
                if let ::core::option::Option::Some(inner_cfg) = self.#name {
                    inner_cfg.merge_into(&mut target.#name);
                }
            });
        } else {
            config_fields.push(quote! {
                #(#serde_attrs)*
                pub #name: ::core::option::Option<#ty>
            });

            let assign = if let Some(with) = &cfg.with {
                quote! { target.#name = #with(value); }
            } else {
                quote! { target.#name = value; }
            };
            let merge = quote! {
                if let ::core::option::Option::Some(value) = self.#name {
                    #assign
                }
            };

            if cfg.is_static {
                static_merge.push(merge);
            } else {
                dynamic_merge.push(merge);
            }
        }
    }

    // Forward struct-level serde attributes.
    let serde_struct_attrs: Vec<&Attribute> = struct_attrs
        .iter()
        .filter(|a| a.path().is_ident("serde"))
        .collect();

    let expanded = quote! {
        #(#serde_struct_attrs)*
        #[doc(hidden)]
        #[derive(
            ::core::default::Default,
            ::core::clone::Clone,
            ::serde::Deserialize,
            ::core::fmt::Debug
        )]
        pub struct #config_name #generics #where_clause {
            #(#config_fields,)*
        }

        impl #impl_generics #config_name #ty_generics #where_clause {
            /// Applies the dynamic (non-`static`) config fields onto `target`.
            pub fn merge_into(self, target: &mut #struct_name #ty_generics) {
                #(#dynamic_merge)*
            }

            /// Applies only the `#[config(static)]` fields onto `target`. These are
            /// honored once at client construction (they cannot take effect at runtime).
            pub fn merge_static_into(self, target: &mut #struct_name #ty_generics) {
                #(#static_merge)*
            }

            /// Deserializes this config from a YAML string.
            pub fn from_yaml_str(s: &str) -> ::core::result::Result<Self, ::serde_yml::Error> {
                ::serde_yml::from_str(s)
            }
        }
    };

    TokenStream::from(expanded)
}
