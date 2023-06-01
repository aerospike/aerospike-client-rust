use proc_macro2::{TokenStream};
use quote::{quote, quote_spanned, ToTokens};
use syn::{Ident, Data, Fields, Expr, Field, ExprLit};
use syn::Expr::{Assign, Lit, Path};
use syn::spanned::Spanned;

pub struct WritableFieldAttributes<'a> {
    field: &'a Field,
    ident: &'a Option<Ident>,
    name: String,
    default: Option<syn::Lit>,
    skip: bool,
}

impl<'a> WritableFieldAttributes<'a> {
    pub fn default_write_value_token_stream(&self) -> (TokenStream, usize, u8) {
        // Unwarp is fine since this function can only get called if default is Some.
        let default = self.default.clone().unwrap();
        match default {
            syn::Lit::Str(s) => {
                let val = &s.value();
                return (quote! {
                    buffer.write_str(#val);
                },
                val.len(), 3)
            }
            syn::Lit::Int(i) => {
                if let Ok(val) = i.base10_parse::<i64>() {
                    return (quote! {
                        buffer.write_i64(#val);
                    }, 8, 1)
                } else {
                    panic!("Aerospike Default value could not be parsed as i64")
                }
            }
            syn::Lit::Float(f) => {
                if let Ok(val) = f.base10_parse::<f64>() {
                    return (quote! {
                        buffer.write_f64(#val);
                    }, 8, 2)
                } else {
                    panic!("Aerospike Default value could not be parsed as f64")
                }
            }
            syn::Lit::Bool(b) => {
                let val = b.value();
                return (quote! {
                    buffer.write_bool(#val);
                }, 1, 17)
            }
            _ => {panic!("Aerospike Default value is not supported for the value on {}", &self.name)}
        }
    }
}

fn writable_field_arguments(field: &Field) -> WritableFieldAttributes {
    // Collect initial Information
    let mut attributes = WritableFieldAttributes {
        field,
        ident: &field.ident,
        name: field.ident.clone().unwrap().to_string(),
        default: None,
        skip: false,
    };

    for a in &field.attrs {
        // Filter for aerospike() attributes
        if !a.path().is_ident("aerospike") {
            continue;
        }

        // Parse field attributes to Expression
        let expr: Expr = a.parse_args().unwrap();

        match expr {
            // Assign based Attributes like rename
            Assign(assign) => {
                match assign.left.as_ref() {
                    Path(path) => {
                        // Rename Attribute extraction
                        if path.path.is_ident("rename") {
                            if let Lit(lit) = *assign.right {
                                // Currently only accepts Strings as Field Name
                                if let syn::Lit::Str(ls) = lit.lit {
                                    attributes.name = ls.value();
                                }else{
                                    panic!("Invalid Aerospike Rename Value")
                                }
                            }else{
                                panic!("Invalid Aerospike Rename Value")
                            }
                        } else if path.path.is_ident("default") {
                            if let Lit(lit) = *assign.right {
                                attributes.default = Some(lit.lit);
                            }
                        }
                    }
                    _ => { panic!("Invalid Aerospike Derive Attribute") }
                }
            }
            // Path based Attributes that just serve as markers
            Path(path) => {
                if let Some(ident) = path.path.get_ident() {
                    match ident.to_string().as_ref() {
                        // Ignore Attribute with skip as alias
                        "ignore" | "skip" => attributes.skip = true,
                        _ => { panic!("Invalid Aerospike Derive Attribute") }
                    }
                }
            }
            _ => { panic!("Invalid Aerospike Derive Attribute") }
        }
    }

    attributes
}


pub (crate) fn build_writable(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => {
            match data.fields {
                Fields::Named(ref fields) => {
                    // Collect all the Field Info
                    let field_args = fields.named.iter().map(|f| {
                        writable_field_arguments(&f)
                    }).collect::<Vec<WritableFieldAttributes>>();

                    // Build the `write_as_bins` function
                    let writer_recurse = field_args.iter().map(|f| {
                        let name = f.ident;
                        let skip = f.skip;
                        let name_str = &f.name;
                        let has_default = f.default.is_some();
                        // Build the bin Token Stream.
                        if has_default {
                            let default = f.default_write_value_token_stream();
                            let default_writer = default.0;
                            let default_length = default.1;
                            let default_type = default.2;
                            quote_spanned! {f.field.span()=>
                                if !#skip {
                                    {
                                        let encodable = aerospike::WritableValue::writable_value_encodable(&self.#name);
                                        if encodable {
                                        buffer.write_i32((#name_str.len() + aerospike::WritableValue::write_as_value(&self.#name, &mut None) + 4) as i32);
                                        } else {
                                            buffer.write_i32((#name_str.len() + #default_length + 4) as i32);
                                        }
                                        buffer.write_u8(op_type);

                                        if encodable {
                                            buffer.write_u8(aerospike::WritableValue::writable_value_particle_type(&self.#name) as u8);
                                        } else {
                                            buffer.write_u8(#default_type);
                                        }
                                        buffer.write_u8(0);
                                        buffer.write_u8(#name_str.len() as u8);
                                        buffer.write_str(#name_str);
                                        if encodable {
                                            aerospike::WritableValue::write_as_value(&self.#name, &mut Some(buffer));
                                        } else {
                                            #default_writer
                                        }
                                    }
                                }
                            }

                        } else {
                            quote_spanned! {f.field.span()=>
                                if !#skip && aerospike::WritableValue::writable_value_encodable(&self.#name) {
                                    buffer.write_i32((#name_str.len() + aerospike::WritableValue::write_as_value(&self.#name, &mut None) + 4) as i32);
                                    buffer.write_u8(op_type);
                                    buffer.write_u8(aerospike::WritableValue::writable_value_particle_type(&self.#name) as u8);
                                    buffer.write_u8(0);
                                    buffer.write_u8(#name_str.len() as u8);
                                    buffer.write_str(#name_str);
                                    aerospike::WritableValue::write_as_value(&self.#name, &mut Some(buffer));
                                }
                            }
                        }

                    });

                    // Build the `writable_bins_size` function
                    let length_recurse = field_args.iter().map(|f| {
                        let name = f.ident;
                        let name_len = f.name.len();
                        let skip = f.skip;
                        let has_default = f.default.is_some();
                        // Build the bin Token Stream.
                        if has_default {
                            let default = f.default_write_value_token_stream();
                            let default_length = default.1;

                            quote_spanned! {f.field.span()=>
                                if !#skip {
                                    if aerospike::WritableValue::writable_value_encodable(&self.#name) {
                                        size += #name_len + aerospike::WritableValue::write_as_value(&self.#name, &mut None) + 8;
                                    } else {
                                        size += #name_len + #default_length + 8;
                                    }
                                }
                            }
                        } else {
                            quote_spanned! {f.field.span()=>
                                if !#skip && aerospike::WritableValue::writable_value_encodable(&self.#name) {
                                    size += #name_len + aerospike::WritableValue::write_as_value(&self.#name, &mut None) + 8;
                                }
                            }
                        }
                    });

                    // Build the `writable_bins_count` function
                    let op_count_recurse = field_args.iter().map(|f| {
                        let name = f.ident;
                        let skip = f.skip;
                        let has_default = f.default.is_some();
                        if has_default {
                            quote_spanned! {f.field.span()=>
                                if !#skip {
                                    count += 1;
                                }
                            }
                        } else {
                            quote_spanned! {f.field.span()=>
                                if !#skip && aerospike::WritableValue::writable_value_encodable(&self.#name) {
                                    count += 1;
                                }
                            }
                        }

                    });

                    // Build the final functions for the Trait impl
                    quote! {
                        fn write_as_bins(&self, buffer: &mut aerospike::Buffer, op_type: u8) -> aerospike::errors::Result<()>{
                            #(#writer_recurse)*
                            Ok(())
                        }

                        fn writable_bins_size(&self) -> usize {
                            let mut size: usize = 0;
                            #(#length_recurse)*
                            size
                        }

                        fn writable_bins_count(&self) -> usize {
                            let mut count: usize = 0;
                            #(#op_count_recurse)*
                            count
                        }

                    }
                }
                _ => unimplemented!()
            }
        }
        Data::Enum(_) | Data::Union(_) => unimplemented!(),
    }
}


// WritableValue
pub(crate) fn convert_writable_value_source(data: &Data) -> proc_macro2::TokenStream {
    match *data {
        Data::Struct(ref data) => {
            match data.fields {
                Fields::Named(ref fields) => {
                    let len = fields.named.len();
                    let recurse = fields.named.iter().map(|f| {
                        let name = &f.ident;
                        let name_str = &name.clone().unwrap().to_string();
                        // Build the bin Token Stream.
                        quote_spanned! {f.span()=>
                            size += aerospike::msgpack::encoder::pack_string(buffer, #name_str);
                            size += aerospike::WritableValue::write_as_cdt_value(&self.#name, buffer);
                        }
                    });
                    quote! {
                        size += aerospike::msgpack::encoder::pack_map_begin(buffer, #len);
                        #(#recurse)*
                    }
                }
                _ => unimplemented!()
            }
        }
        Data::Enum(_) | Data::Union(_) => unimplemented!(),
    }
}