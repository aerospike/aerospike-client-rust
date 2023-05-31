use proc_macro2::{TokenStream};
use quote::{quote, quote_spanned};
use syn::{Ident, Data, Fields, Expr, Field};
use syn::Expr::{Assign, Lit, Path};
use syn::spanned::Spanned;

pub struct WritableFieldAttributes<'a> {
    field: &'a Field,
    ident: &'a Option<Ident>,
    name: String,
    skip: bool
}

fn writable_field_arguments(field: &Field) -> WritableFieldAttributes {
    // Collect initial Information
    let mut attributes = WritableFieldAttributes {
        field,
        ident: &field.ident,
        name: field.ident.clone().unwrap().to_string(),
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
                    let mut count = 0;

                    // Collect all the Field Info
                    let field_args = fields.named.iter().map(|f| {
                        writable_field_arguments(&f)
                    }).collect::<Vec<WritableFieldAttributes>>();

                    // Build the `write_as_bins` function
                    let writer_recurse = field_args.iter().map(|f| {
                        let name = f.ident;
                        let skip = f.skip;
                        let name_str = &f.name;
                        if !skip {count += 1}
                        // Build the bin Token Stream.
                        quote_spanned! {f.field.span()=>
                            if !#skip {
                                buffer.write_i32((#name_str.len() + aerospike::WritableValue::write_as_value(&self.#name, &mut None) + 4) as i32);
                                buffer.write_u8(op_type);
                                buffer.write_u8(aerospike::WritableValue::writable_value_particle_type(&self.#name) as u8);
                                buffer.write_u8(0);
                                buffer.write_u8(#name_str.len() as u8);
                                buffer.write_str(#name_str);
                                aerospike::WritableValue::write_as_value(&self.#name, &mut Some(buffer));
                            }
                        }
                    });

                    // Build the `writable_bins_size` function
                    let length_recurse = field_args.iter().map(|f| {
                        let name = f.ident;
                        let name_len = f.name.len();
                        let skip = f.skip;
                        // Build the bin Token Stream.
                        quote_spanned! {f.field.span()=>
                            if !#skip {
                                size += #name_len + aerospike::WritableValue::write_as_value(&self.#name, &mut None) + 8;
                            }
                        }
                    });

                    // Calculate the Operation Count
                    let op_count: usize = field_args.iter().fold(0, |a, b| {
                        if b.skip {
                            return a
                        }
                        a+1
                    });

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
                            #op_count
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