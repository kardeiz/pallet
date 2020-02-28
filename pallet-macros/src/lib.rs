extern crate proc_macro;

#[macro_use]
extern crate quote;

#[macro_use]
extern crate syn;

use proc_macro::TokenStream;

#[proc_macro_derive(DocumentLike, attributes(pallet))]
pub fn document_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as syn::DeriveInput);
    document_derive_inner(input).unwrap()
}

#[derive(Debug)]
struct FieldMeta {
    ident: syn::Ident,
    name: String,
    ty: syn::Type,
    opts: proc_macro2::TokenStream,
    is_default_search_field: bool,
}

fn handle_field(input: &syn::Field) -> Result<Option<FieldMeta>, Box<dyn std::error::Error>> {
    let pallet_path: syn::Path = parse_quote!(pallet);
    let index_field_name_path: syn::Path = parse_quote!(index_field_name);
    let skip_indexing_path: syn::Path = parse_quote!(skip_indexing);
    let index_field_type_path: syn::Path = parse_quote!(index_field_type);
    let index_field_options_path: syn::Path = parse_quote!(index_field_options);
    let default_search_field_path: syn::Path = parse_quote!(default_search_field);

    let ident = input.ident.as_ref().unwrap();

    let mut name = ident.to_string();

    let mut ty = input.ty.clone();

    let mut opts = quote!(std::option::Option::<()>::None);

    let l_attrs = input
        .attrs
        .iter()
        .flat_map(|x| x.parse_meta())
        .filter(|x| x.path() == &pallet_path)
        .filter_map(|x| match x {
            syn::Meta::List(ml) => Some(ml),
            _ => None,
        })
        .flat_map(|x| x.nested)
        .filter_map(|x| match x {
            syn::NestedMeta::Meta(m) => Some(m),
            _ => None,
        });

    if l_attrs.clone().any(|x| x.path() == &skip_indexing_path) {
        return Ok(None);
    }

    let is_default_search_field =
        l_attrs.clone().any(|x| x.path() == &default_search_field_path);

    if let Some(index_field_name) = l_attrs
        .clone()
        .filter_map(|x| match x {
            syn::Meta::NameValue(mnv) => Some(mnv),
            _ => None,
        })
        .filter(|x| x.path == index_field_name_path)
        .filter_map(|x| match x.lit {
            syn::Lit::Str(s) => Some(s.value()),
            _ => None,
        })
        .next()
    {
        name = index_field_name;
    }

    if let Some(user_ty) = l_attrs
        .clone()
        .filter_map(|x| match x {
            syn::Meta::NameValue(mnv) => Some(mnv),
            _ => None,
        })
        .filter(|x| x.path == index_field_type_path)
        .filter_map(|x| match x.lit {
            syn::Lit::Str(s) => syn::parse_str(&s.value()).ok(),
            _ => None,
        })
        .next()
    {
        ty = user_ty;
    }

    if let Some(index_fields_options) = l_attrs
        .clone()
        .filter_map(|x| match x {
            syn::Meta::NameValue(mnv) => Some(mnv),
            _ => None,
        })
        .filter(|x| x.path == index_field_options_path)
        .filter_map(|x| match x.lit {
            syn::Lit::Str(s) => syn::parse_str::<syn::Expr>(&s.value()).ok(),
            _ => None,
        })
        .map(|e| quote!(Some(#e)))
        .next()
    {
        opts = index_fields_options;
    }

    Ok(Some(FieldMeta {
        ident: ident.clone(),
        name,
        ty,
        opts,
        is_default_search_field,
    }))
}

fn document_derive_inner(
    input: syn::DeriveInput,
) -> Result<TokenStream, Box<dyn std::error::Error>> {
    let data = match input.data {
        syn::Data::Struct(ref x @ syn::DataStruct { fields: syn::Fields::Named(_), .. }) => x,
        _ => {
            return Err("`Document` can only be used on a `Struct` with named fields.".into());
        }
    };

    let name = &input.ident;
    let pallet_path: syn::Path = parse_quote!(pallet);
    let tree_name_path: syn::Path = parse_quote!(tree_name);

    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let l_attrs = input
        .attrs
        .iter()
        .flat_map(|x| x.parse_meta())
        .filter(|x| x.path() == &pallet_path)
        .filter_map(|x| match x {
            syn::Meta::List(ml) => Some(ml),
            _ => None,
        })
        .flat_map(|x| x.nested)
        .filter_map(|x| match x {
            syn::NestedMeta::Meta(m) => Some(m),
            _ => None,
        });

    let tree_name = l_attrs
        .clone()
        .filter_map(|x| match x {
            syn::Meta::NameValue(mnv) => Some(mnv),
            _ => None,
        })
        .filter(|x| x.path == tree_name_path)
        .filter_map(|x| match x.lit {
            syn::Lit::Str(s) => Some(s.value()),
            _ => None,
        })
        .next()
        .map(|s| quote!(Some(#s.into())))
        .unwrap_or_else(|| quote!(None));

    let field_metas = data
        .fields
        .iter()
        .map(handle_field)
        .filter_map(Result::transpose)
        .collect::<Result<Vec<_>, _>>()?;

    let index_fields = field_metas.iter()
        .map(|FieldMeta { name, ty, opts, .. }| quote!(schema_builder.add_field(<#ty as pallet::search::FieldValue>::field_entry(#name, #opts))))
        .collect::<Vec<_>>();

    let doc_fields = field_metas.iter().enumerate()
        .map(|(idx, FieldMeta { ident, ty, .. })| 
            quote! {
                if let Some(val) = <#ty as pallet::search::FieldValue>::into_value(self.#ident.clone().into()) {
                    doc.add(pallet::ext::tantivy::schema::FieldValue::new(fields[#idx], val));
                }
            })
        .collect::<Vec<_>>();

    let default_search_fields = field_metas
        .iter()
        .enumerate()
        .filter(|(_, FieldMeta { is_default_search_field, .. })| *is_default_search_field)
        .map(|(idx, _)| quote!(fields[#idx]))
        .collect::<Vec<_>>();

    let out = quote! {
        impl #impl_generics pallet::DocumentLike for #name #ty_generics #where_clause {

            type IndexFieldsType = pallet::search::FieldsContainer;

            fn tree_builder() -> pallet::db::TreeBuilder {
                let mut out = pallet::db::TreeBuilder::default();
                if let std::option::Option::<std::string::String>::Some(tree_name) = #tree_name {
                    out = out.with_tree_name(tree_name);
                }
                out
            }

            fn index_builder() -> pallet::search::IndexBuilder<Self::IndexFieldsType> {
                let out = pallet::search::IndexBuilder::default()
                    .with_fields_builder(|schema_builder| {
                        Ok(pallet::search::FieldsContainer(vec![#(#index_fields,)*]))
                    })
                    .with_default_search_fields_builder(|fields| {
                        let fields = &fields.0;
                        vec![#(#default_search_fields,)*]
                    });
                out
            }

            fn as_index_document(
                &self,
                index_fields: &Self::IndexFieldsType,
            ) -> pallet::err::Result<pallet::ext::tantivy::Document> {
                use pallet::search::FieldValue;

                let fields = &index_fields.0;

                let mut doc = pallet::ext::tantivy::Document::new();
                #(#doc_fields)*

                Ok(doc)
            }
        }
    };

    Ok(out.into())
}
