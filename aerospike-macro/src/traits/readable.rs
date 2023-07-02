use quote::{quote, quote_spanned};
use syn::spanned::Spanned;
use syn::Expr::{Assign, Lit, Path};
use syn::{Data, Expr, Field, Fields, Ident, Type};

pub struct ReadableFieldAttributes<'a> {
    field: &'a Field,
    ident: &'a Option<Ident>,
    name: String,
    ty: Type,
}

fn readable_field_arguments(field: &Field) -> ReadableFieldAttributes {
    // Collect initial Information
    let mut attributes = ReadableFieldAttributes {
        field,
        ident: &field.ident,
        name: field.ident.clone().unwrap().to_string(),
        ty: field.ty.clone(),
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
                                } else {
                                    panic!("Invalid Aerospike Rename Value")
                                }
                            } else {
                                panic!("Invalid Aerospike Rename Value")
                            }
                        }
                    }
                    _ => {
                        panic!("Invalid Aerospike Derive Attribute")
                    }
                }
            }
            // Path based Attributes that just serve as markers
            /*Path(path) => {
                if let Some(ident) = path.path.get_ident() {
                    match ident.to_string().as_ref() {
                        _ => { panic!("Invalid Aerospike Derive Attribute") }
                    }
                }
            }*/
            _ => {
                panic!("Invalid Aerospike Derive Attribute")
            }
        }
    }
    if attributes.name.len() > 15 {
        panic!("Aerospike Derive Bin Names can not be longer than 15 bytes!")
    }
    attributes
}

pub(crate) fn convert_readable_bins(data: &Data, name: &Ident) -> proc_macro2::TokenStream {
    match *data {
        Data::Struct(ref data) => {
            match data.fields {
                Fields::Named(ref fields) => {
                    let field_args = fields
                        .named
                        .iter()
                        .map(|f| readable_field_arguments(&f))
                        .collect::<Vec<ReadableFieldAttributes>>();

                    let field_recurse = field_args.iter().map(|f| {
                        let field_name = &f.name;
                        let name = f.ident;
                        quote_spanned! {f.field.span()=>
                            #name: aerospike::derive::readable::ReadableValue::read_value_from_bytes(data_points.get_mut(#field_name).unwrap_or(&mut aerospike::derive::readable::PreParsedBin {sub_values: vec![], value: aerospike::derive::readable::PreParsedValue { particle_type: 0, buffer: Default::default(), byte_length: 0 }}))?,
                        }
                    });
                    quote! {
                        // Build the final struct
                        Ok(#name {
                            #(#field_recurse)*
                        })
                    }
                }
                _ => unimplemented!(),
            }
        }
        Data::Enum(_) | Data::Union(_) => unimplemented!(),
    }
}

pub(crate) fn convert_readable_value_source(data: &Data, name: &Ident) -> proc_macro2::TokenStream {
    match *data {
        Data::Struct(ref data) => {
            match data.fields {
                Fields::Named(ref fields) => {
                    let field_args = fields
                        .named
                        .iter()
                        .map(|f| readable_field_arguments(&f))
                        .collect::<Vec<ReadableFieldAttributes>>();

                    let field_recurse = field_args.iter().map(|f| {
                        let field_name = &f.name;
                        let name = f.ident;
                        let ty = &f.ty;
                        quote_spanned! {f.field.span()=>
                            #name: aerospike::derive::readable::read_map_value_bytes(buff, keys.get(#field_name))?,
                        }
                    });
                    quote! {
                        fn read_value_from_bytes(data_point: &mut aerospike::derive::readable::PreParsedBin) -> aerospike::errors::Result<Self> {
                            let mut nbuf = aerospike::Buffer::new(1);
                            let mut op_count: u64 = 0;
                            let mut ndbuf = vec![];
                            let mut ndval = vec![&mut data_point.value];
                            ndval.extend(&mut data_point.sub_values);
                            for sv in &mut ndval {
                                let ltype = sv.buffer.peek();
                                let count: usize = match ltype {
                                    0x80..=0x8f => (ltype & 0x0f) as usize,
                                    0xde => sv.buffer.read_u16(None) as usize,
                                    0xdf => sv.buffer.read_u32(None) as usize,
                                    _ => {
                                        return Err(aerospike::ErrorKind::Derive("Invalid Data Type for derive Map (Struct) CDT Type".to_string()).into())
                                    }
                                };
                                op_count += count as u64;
                                ndbuf.extend(sv.buffer.data_buffer[1..].iter());
                            }
                            nbuf.resize_buffer(8)?;
                            nbuf.reset_offset();
                            nbuf.write_u64(op_count);
                            nbuf.data_buffer.extend(ndbuf.iter());
                            Self::parse_cdt_value(&mut nbuf)
                        }

                        fn parse_value(data_point: &mut aerospike::derive::readable::PreParsedValue) -> aerospike::errors::Result<Self> {
                            unreachable!()
                        }

                        fn parse_cdt_value(buff: &mut aerospike::Buffer) -> aerospike::errors::Result<Self> {
                            buff.reset_offset();
                            let count = buff.read_u64(None);

                            let mut keys: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
                            for _ in 0..count {
                                let len = buff.read_u8(None) as usize & 0x1f;
                                let vtype = buff.read_u8(None);
                                if(vtype != 3) {
                                   return Err(aerospike::ErrorKind::Derive("Only map keys of type string are allowed for Struct reading".to_string()).into())
                                }
                                let name = buff.read_str(len-1)?;
                                let ofs = buff.data_offset();
                                aerospike::derive::readable::skip_map_value_bytes(buff)?;
                                keys.insert(name.clone(), ofs);
                            }

                            Ok(#name {
                                #(#field_recurse)*
                            })
                        }
                    }
                }
                _ => unimplemented!(),
            }
        }
        Data::Enum(_) | Data::Union(_) => unimplemented!(),
    }
}
