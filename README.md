# pallet

[![Docs](https://docs.rs/pallet/badge.svg)](https://docs.rs/crate/pallet/)
[![Crates.io](https://img.shields.io/crates/v/pallet.svg)](https://crates.io/crates/pallet)


A searchable document datastore built on [`sled`](https://docs.rs/sled) and [`tantivy`](https://docs.rs/tantivy).

Provides a typed-tree interface to a `sled` database, with standard datastore ops (`find`, `create`, `update`, `delete`),
but also Lucene/Elasticsearch style searching.

The included `pallet_macros` crate provides an easy way to derive `pallet::DocumentLike` for data structs.

## Usage

```rust
#[macro_use]
extern crate serde;

#[derive(Serialize, Deserialize, Debug, pallet::DocumentLike)]
#[pallet(tree_name = "books")]
pub struct Book {
    #[pallet(default_search_field)]
    title: String,
    #[pallet(default_search_field)]
    description: Option<String>,
    #[pallet(index_field_type = "u64")]
    rating: u8,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::TempDir::new_in(".")?;

    let db = sled::open(temp_dir.path().join("db"))?;

    let store = pallet::Store::builder().with_db(db).with_index_dir(temp_dir.path()).finish()?;

    let books = vec![
        Book {
            title: "The Old Man and the Sea".into(),
            description: Some(
                "He was an old man who fished alone in a skiff in \
            the Gulf Stream and he had gone eighty-four days \
            now without taking a fish."
                    .into(),
            ),
            rating: 10,
        },
        Book {
            title: "The Great Gatsby".into(),
            description: Some("About a man and some other stuff".into()),
            rating: 8,
        },
    ];

    let _ = store.create_multi(&books)?;

    let books = store.search("man AND rating:>8")?;

    println!("{:?}", books);

    Ok(())
}
```

## `pallet_macros`

See the example for usage. The following attributes can be used to customize the implementation:

* `tree_name`: A container level attribute to specify the `sled::Tree` name.
* `index_field_name`: Rename the field in the search schema.
* `index_field_type`: Set the index field type, must implement `Into<tantivy::schema::Value>`.
* `index_field_options`: Set the index field options. By default, the options for `String` is
`tantivy::schema::TEXT`, and the options for numeric types is `tantivy::schema::INDEXED`.
* `default_search_field`: Include this field in the list of default search fields.
* `skip_indexing`: Do not index this field.

## Changelog

### 0.4.0

* Add various builders.
* Split out `index` and `tree` functionality.
* Set up `search::Searcher` trait and other search helpers.

### 0.3.2

* Add some docs

### 0.3.0

* Add `pallet_macros` to derive `pallet::DocumentLike`


<hr/>

Current version: 0.4.0

License: MIT
