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
                let _ = env_logger::Builder::from_env(
                    env_logger::Env::default().default_filter_or("error"),
                )
                .is_test(true)
                .try_init();

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
                let _ = env_logger::Builder::from_env(
                    env_logger::Env::default().default_filter_or("error"),
                )
                .is_test(true)
                .try_init();

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

// ===== RecordMapper derive ===================================================

#[derive(Default)]
struct RecordFieldAttrs {
    key: bool,
    generation: bool,
    skip: bool,
    bin: Option<String>,
}

fn parse_record_field_attrs(attrs: &[Attribute]) -> Result<RecordFieldAttrs, syn::Error> {
    let mut out = RecordFieldAttrs::default();
    for attr in attrs.iter().filter(|a| a.path().is_ident("record")) {
        let metas = attr.parse_args_with(Punctuated::<Meta, Comma>::parse_terminated)?;
        for meta in metas {
            match meta {
                Meta::Path(ref p) if p.is_ident("key") => out.key = true,
                Meta::Path(ref p) if p.is_ident("generation") => out.generation = true,
                Meta::Path(ref p) if p.is_ident("skip") => out.skip = true,
                Meta::NameValue(ref nv) if nv.path.is_ident("bin") => {
                    if let syn::Expr::Lit(syn::ExprLit {
                        lit: syn::Lit::Str(s),
                        ..
                    }) = &nv.value
                    {
                        out.bin = Some(s.value());
                    } else {
                        return Err(syn::Error::new_spanned(
                            &nv.value,
                            "record(bin) expects a string literal",
                        ));
                    }
                }
                other => {
                    return Err(syn::Error::new_spanned(
                        other,
                        "unsupported record(...) directive on a field (expected one of: key, \
                         generation, skip, bin = \"...\")",
                    ));
                }
            }
        }
    }
    Ok(out)
}

struct RecordStructAttrs {
    root: Path,
    serde: bool,
}

fn parse_record_struct_attrs(attrs: &[Attribute]) -> Result<RecordStructAttrs, syn::Error> {
    let mut root: Path = syn::parse_str("::aerospike_core").expect("valid default path");
    let mut serde = false;
    for attr in attrs.iter().filter(|a| a.path().is_ident("record")) {
        let metas = attr.parse_args_with(Punctuated::<Meta, Comma>::parse_terminated)?;
        for meta in metas {
            match meta {
                Meta::Path(ref p) if p.is_ident("serde") => serde = true,
                Meta::NameValue(ref nv) if nv.path.is_ident("crate") => {
                    if let syn::Expr::Lit(syn::ExprLit {
                        lit: syn::Lit::Str(s),
                        ..
                    }) = &nv.value
                    {
                        root = syn::parse_str(&s.value()).map_err(|_| {
                            syn::Error::new_spanned(
                                &nv.value,
                                "record(crate) expects a path string like \"aerospike_sdk\"",
                            )
                        })?;
                    } else {
                        return Err(syn::Error::new_spanned(
                            &nv.value,
                            "record(crate) expects a string literal",
                        ));
                    }
                }
                other => {
                    return Err(syn::Error::new_spanned(
                        other,
                        "unsupported record(...) directive on a struct (expected one of: serde, \
                         crate = \"...\")",
                    ));
                }
            }
        }
    }
    Ok(RecordStructAttrs { root, serde })
}

/// Derives Aerospike object mapping for a struct with named fields, in
/// one of two modes:
///
/// - **Entity mode** — the struct has a `#[record(key)]` field: derives
///   `RecordMapper` (bins ↔ struct, key extraction), making the type
///   usable with typed datasets and object write builders.
/// - **Value mode** — no key field: derives `ToValue`/`FromValue`,
///   encoding the struct as a map value so it can nest inside entity
///   fields (recursively).
///
/// Field attributes:
/// - `#[record(key)]` — the record's user key (entity mode, exactly
///   one). Not stored as a bin; `id()` reads it and `from_record`
///   recovers it from the record key (requires stored keys —
///   `send_key`).
/// - `#[record(bin = "name")]` — bin/map-entry name override (default:
///   the field name; bins must be at most 15 characters).
/// - `#[record(generation)]` — populated with the record generation on
///   reads, never written (entity mode only; field type must be `u32`).
/// - `#[record(skip)]` — neither written nor read; rebuilt with
///   `Default::default()`.
///
/// Struct attributes:
/// - `#[record(crate = "aerospike_sdk")]` — the crate path the generated
///   code resolves Aerospike types through (default `::aerospike_core`;
///   use the crate your project actually depends on).
#[proc_macro_derive(RecordMapper, attributes(record))]
pub fn derive_record_mapper(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let struct_attrs = match parse_record_struct_attrs(&input.attrs) {
        Ok(attrs) => attrs,
        Err(e) => return e.to_compile_error().into(),
    };
    let use_serde = struct_attrs.serde;
    let root = struct_attrs.root;
    let derive_mod = quote!(#root::mapping::__derive);

    let fields = match input.data {
        Data::Struct(data_struct) => match data_struct.fields {
            Fields::Named(fields_named) => fields_named.named,
            _ => {
                return syn::Error::new_spanned(
                    struct_name,
                    "RecordMapper can only be derived for structs with named fields",
                )
                .to_compile_error()
                .into()
            }
        },
        _ => {
            return syn::Error::new_spanned(
                struct_name,
                "RecordMapper can only be derived for structs",
            )
            .to_compile_error()
            .into()
        }
    };

    let mut key_field: Option<syn::Ident> = None;
    let mut generation_field: Option<syn::Ident> = None;
    let mut skip_fields: Vec<syn::Ident> = Vec::new();
    // (ident, type, bin/map-entry name)
    let mut regular_fields: Vec<(syn::Ident, Type, String)> = Vec::new();

    for field in &fields {
        let ident = field.ident.clone().expect("named field");
        let ty = field.ty.clone();
        let attrs = match parse_record_field_attrs(&field.attrs) {
            Ok(a) => a,
            Err(e) => return e.to_compile_error().into(),
        };

        let marker_count =
            usize::from(attrs.key) + usize::from(attrs.generation) + usize::from(attrs.skip);
        if marker_count > 1 {
            return syn::Error::new_spanned(
                &field.ident,
                "record(key), record(generation) and record(skip) are mutually exclusive",
            )
            .to_compile_error()
            .into();
        }
        if (attrs.key || attrs.generation || attrs.skip) && attrs.bin.is_some() {
            return syn::Error::new_spanned(
                &field.ident,
                "record(bin) does not apply to key, generation, or skipped fields",
            )
            .to_compile_error()
            .into();
        }
        if use_serde && attrs.bin.is_some() {
            return syn::Error::new_spanned(
                &field.ident,
                "under record(serde), rename bins with #[serde(rename = \"...\")]",
            )
            .to_compile_error()
            .into();
        }
        if use_serde && attrs.skip {
            return syn::Error::new_spanned(
                &field.ident,
                "under record(serde), skip fields with #[serde(skip)] (plus #[serde(default)])",
            )
            .to_compile_error()
            .into();
        }

        if attrs.key {
            if key_field.is_some() {
                return syn::Error::new_spanned(
                    &field.ident,
                    "only one field may be marked record(key)",
                )
                .to_compile_error()
                .into();
            }
            key_field = Some(ident);
            continue;
        }
        if attrs.generation {
            if generation_field.is_some() {
                return syn::Error::new_spanned(
                    &field.ident,
                    "only one field may be marked record(generation)",
                )
                .to_compile_error()
                .into();
            }
            generation_field = Some(ident);
            continue;
        }
        if attrs.skip {
            skip_fields.push(ident);
            continue;
        }

        let bin_name = attrs.bin.unwrap_or_else(|| ident.to_string());
        if bin_name.len() > 15 {
            return syn::Error::new_spanned(
                &field.ident,
                format!("bin name '{bin_name}' exceeds the 15-character server limit"),
            )
            .to_compile_error()
            .into();
        }
        regular_fields.push((ident, ty, bin_name));
    }

    let skip_inits = skip_fields.iter().map(|ident| {
        quote! { #ident: ::core::default::Default::default(), }
    });

    let expanded = if let Some(key_ident) = key_field {
        // ---- Entity mode: RecordMapper over the record's bins ----
        let key_ty = fields
            .iter()
            .find(|f| f.ident.as_ref() == Some(&key_ident))
            .map(|f| f.ty.clone())
            .expect("key field exists");

        if use_serde {
            // Serde engine: delegate conversion to Serialize/Deserialize,
            // stripping the key (and generation) on writes and injecting
            // them from the record on reads, by field name.
            let key_name = key_ident.to_string();
            let generation_strip = generation_field.as_ref().map(|ident| {
                let name = ident.to_string();
                quote! { bins.shift_remove(#name); }
            });
            let generation_inject = generation_field.as_ref().map(|ident| {
                let name = ident.to_string();
                quote! {
                    map.insert(
                        ::std::string::String::from(#name),
                        #derive_mod::Value::Int(i64::from(generation)),
                    );
                }
            });

            return TokenStream::from(quote! {
                impl #impl_generics #derive_mod::RecordMapper
                    for #struct_name #ty_generics #where_clause
                {
                    fn to_bins(
                        &self,
                    ) -> #derive_mod::Result<
                        #derive_mod::IndexMap<::std::string::String, #derive_mod::Value>,
                    > {
                        let mut bins = #derive_mod::serde_support::to_bins(self)?;
                        bins.shift_remove(#key_name);
                        #generation_strip
                        ::core::result::Result::Ok(bins)
                    }

                    fn from_record(
                        bins: &#derive_mod::IndexMap<::std::string::String, #derive_mod::Value>,
                        key: &#derive_mod::Key,
                        generation: u32,
                    ) -> #derive_mod::Result<Self> {
                        let _ = generation;
                        let mut map = bins.clone();
                        match &key.user_key {
                            ::core::option::Option::Some(user_key) => {
                                map.insert(
                                    ::std::string::String::from(#key_name),
                                    ::core::clone::Clone::clone(user_key),
                                );
                            }
                            ::core::option::Option::None => {
                                return ::core::result::Result::Err(
                                    #derive_mod::Error::invalid_argument(
                                        "record has no user key; write with send_key enabled \
                                         to map keyed entities from query results",
                                    ),
                                )
                            }
                        }
                        #generation_inject
                        #derive_mod::serde_support::from_bins(&map)
                    }

                    fn id(&self) -> #derive_mod::Value {
                        #derive_mod::ToValue::to_value(&self.#key_ident)
                            .expect("the record(key) field must convert to a Value")
                    }
                }
            });
        }

        let inserts = regular_fields.iter().map(|(ident, _ty, bin_name)| {
            quote! {
                bins.insert(
                    ::std::string::String::from(#bin_name),
                    #derive_mod::ToValue::to_value(&self.#ident).map_err(|e| {
                        #derive_mod::Error::invalid_argument(::std::format!(
                            "bin '{}': {}", #bin_name, e
                        ))
                    })?,
                );
            }
        });
        let reads = regular_fields.iter().map(|(ident, ty, bin_name)| {
            quote! {
                #ident: match bins.get(#bin_name) {
                    ::core::option::Option::Some(value) => {
                        <#ty as #derive_mod::FromValue>::from_value(value).map_err(|e| {
                            #derive_mod::Error::invalid_argument(::std::format!(
                                "bin '{}': {}", #bin_name, e
                            ))
                        })?
                    }
                    ::core::option::Option::None => {
                        <#ty as #derive_mod::FromValue>::from_missing().map_err(|e| {
                            #derive_mod::Error::invalid_argument(::std::format!(
                                "bin '{}': {}", #bin_name, e
                            ))
                        })?
                    }
                },
            }
        });
        let generation_init = generation_field.map(|ident| {
            quote! { #ident: generation, }
        });
        let skip_inits = skip_inits.clone();

        quote! {
            impl #impl_generics #derive_mod::RecordMapper for #struct_name #ty_generics #where_clause {
                fn to_bins(
                    &self,
                ) -> #derive_mod::Result<
                    #derive_mod::IndexMap<::std::string::String, #derive_mod::Value>,
                > {
                    let mut bins: #derive_mod::IndexMap<
                        ::std::string::String,
                        #derive_mod::Value,
                    > = ::core::default::Default::default();
                    #(#inserts)*
                    ::core::result::Result::Ok(bins)
                }

                fn from_record(
                    bins: &#derive_mod::IndexMap<::std::string::String, #derive_mod::Value>,
                    key: &#derive_mod::Key,
                    generation: u32,
                ) -> #derive_mod::Result<Self> {
                    let _ = generation;
                    ::core::result::Result::Ok(Self {
                        #key_ident: match &key.user_key {
                            ::core::option::Option::Some(user_key) => {
                                <#key_ty as #derive_mod::FromValue>::from_value(user_key)
                                    .map_err(|e| {
                                        #derive_mod::Error::invalid_argument(::std::format!(
                                            "key field '{}': {}",
                                            ::core::stringify!(#key_ident),
                                            e
                                        ))
                                    })?
                            }
                            ::core::option::Option::None => {
                                return ::core::result::Result::Err(
                                    #derive_mod::Error::invalid_argument(
                                        "record has no user key; write with send_key enabled \
                                         to map keyed entities from query results",
                                    ),
                                )
                            }
                        },
                        #generation_init
                        #(#skip_inits)*
                        #(#reads)*
                    })
                }

                fn id(&self) -> #derive_mod::Value {
                    #derive_mod::ToValue::to_value(&self.#key_ident)
                        .expect("the record(key) field must convert to a Value")
                }
            }
        }
    } else {
        // ---- Value mode: ToValue/FromValue over a map encoding ----
        if let Some(ident) = generation_field {
            return syn::Error::new_spanned(
                ident,
                "record(generation) requires a record context; add a #[record(key)] field",
            )
            .to_compile_error()
            .into();
        }

        if use_serde {
            return TokenStream::from(quote! {
                impl #impl_generics #derive_mod::ToValue
                    for #struct_name #ty_generics #where_clause
                {
                    fn to_value(&self) -> #derive_mod::Result<#derive_mod::Value> {
                        #derive_mod::serde_support::to_value(self)
                    }
                }

                impl #impl_generics #derive_mod::FromValue
                    for #struct_name #ty_generics #where_clause
                {
                    fn from_value(
                        value: &#derive_mod::Value,
                    ) -> #derive_mod::Result<Self> {
                        #derive_mod::serde_support::from_value(value)
                    }
                }
            });
        }

        let inserts = regular_fields.iter().map(|(ident, _ty, entry_name)| {
            quote! {
                map.insert(
                    #derive_mod::Value::String(::std::string::String::from(#entry_name)),
                    #derive_mod::ToValue::to_value(&self.#ident).map_err(|e| {
                        #derive_mod::Error::invalid_argument(::std::format!(
                            "field '{}': {}", #entry_name, e
                        ))
                    })?,
                );
            }
        });
        let reads = regular_fields.iter().map(|(ident, ty, entry_name)| {
            quote! {
                #ident: {
                    let entry_key =
                        #derive_mod::Value::String(::std::string::String::from(#entry_name));
                    let entry = match value {
                        #derive_mod::Value::HashMap(m) => m.get(&entry_key),
                        #derive_mod::Value::OrderedMap(m) => m.get(&entry_key),
                        #derive_mod::Value::SortedMap(m) => m.get(&entry_key),
                        other => {
                            return ::core::result::Result::Err(
                                #derive_mod::Error::invalid_argument(::std::format!(
                                    "expected a map for {}, got {:?}",
                                    ::core::stringify!(#struct_name),
                                    other
                                )),
                            )
                        }
                    };
                    match entry {
                        ::core::option::Option::Some(v) => {
                            <#ty as #derive_mod::FromValue>::from_value(v).map_err(|e| {
                                #derive_mod::Error::invalid_argument(::std::format!(
                                    "field '{}': {}", #entry_name, e
                                ))
                            })?
                        }
                        ::core::option::Option::None => {
                            <#ty as #derive_mod::FromValue>::from_missing().map_err(|e| {
                                #derive_mod::Error::invalid_argument(::std::format!(
                                    "field '{}': {}", #entry_name, e
                                ))
                            })?
                        }
                    }
                },
            }
        });
        let skip_inits = skip_inits.clone();

        quote! {
            impl #impl_generics #derive_mod::ToValue for #struct_name #ty_generics #where_clause {
                fn to_value(&self) -> #derive_mod::Result<#derive_mod::Value> {
                    let mut map: ::std::collections::HashMap<
                        #derive_mod::Value,
                        #derive_mod::Value,
                    > = ::std::collections::HashMap::new();
                    #(#inserts)*
                    ::core::result::Result::Ok(#derive_mod::Value::HashMap(map))
                }
            }

            impl #impl_generics #derive_mod::FromValue for #struct_name #ty_generics #where_clause {
                fn from_value(value: &#derive_mod::Value) -> #derive_mod::Result<Self> {
                    ::core::result::Result::Ok(Self {
                        #(#skip_inits)*
                        #(#reads)*
                    })
                }
            }
        }
    };

    TokenStream::from(expanded)
}
