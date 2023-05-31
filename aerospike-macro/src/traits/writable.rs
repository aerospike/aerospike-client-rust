use quote::{quote, quote_spanned};
use syn::{parse_macro_input, DeriveInput, Generics, GenericParam, parse_quote, Data, Fields};
use syn::spanned::Spanned;

pub(crate) fn convert_writable_source(data: &Data) -> proc_macro2::TokenStream {
    match *data {
        Data::Struct(ref data) => {
            match data.fields {
                Fields::Named(ref fields) => {
                    let recurse = fields.named.iter().map(|f| {
                        let name = &f.ident;
                        let name_str = &name.clone().unwrap().to_string();
                        // Build the bin Token Stream.
                        quote_spanned! {f.span()=>
                            buffer.write_i32((#name_str.len() + aerospike::WritableValue::write_as_value(&self.#name, &mut None) + 4) as i32);
                            buffer.write_u8(op_type);
                            buffer.write_u8(aerospike::WritableValue::writable_value_particle_type(&self.#name) as u8);
                            buffer.write_u8(0);
                            buffer.write_u8(#name_str.len() as u8);
                            buffer.write_str(#name_str);
                            aerospike::WritableValue::write_as_value(&self.#name, &mut Some(buffer));
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

pub(crate) fn writable_source_length(data: &Data) -> proc_macro2::TokenStream {
    match *data {
        Data::Struct(ref data) => {
            match data.fields {
                Fields::Named(ref fields) => {
                    let recurse = fields.named.iter().map(|f| {
                        let name = &f.ident;
                        let name_len = name.clone().unwrap().to_string().len();
                        // Build the bin Token Stream.
                        quote_spanned! {f.span()=>
                            size += #name_len + aerospike::WritableValue::write_as_value(&self.#name, &mut None) + 8;
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

pub(crate) fn writable_operation_count(data: &Data) -> usize {
    match *data {
        Data::Struct(ref data) => {
            match data.fields {
                Fields::Named(ref fields) => {
                    fields.named.len()
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