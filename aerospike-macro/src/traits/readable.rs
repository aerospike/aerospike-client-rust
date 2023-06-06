use quote::{quote, quote_spanned};
use syn::spanned::Spanned;
use syn::Expr::{Assign, Lit, Path};
use syn::{Data, Expr, Field, Fields, Ident};

pub struct ReadableFieldAttributes<'a> {
    field: &'a Field,
    ident: &'a Option<Ident>,
    name: String,
}

fn readable_field_arguments(field: &Field) -> ReadableFieldAttributes {
    // Collect initial Information
    let mut attributes = ReadableFieldAttributes {
        field,
        ident: &field.ident,
        name: field.ident.clone().unwrap().to_string(),
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
                            #name: aerospike::ReadableValue::read_value_from_bytes(data_points.get_mut(#field_name).unwrap_or(&mut aerospike::PreParsedBin {sub_values: vec![], value: aerospike::PreParsedValue { particle_type: 0, buffer: Default::default(), byte_length: 0 }}))?,
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
