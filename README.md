# pallet

[![Docs](https://docs.rs/pallet/badge.svg)](https://docs.rs/crate/pallet/)
[![Crates.io](https://img.shields.io/crates/v/pallet.svg)](https://crates.io/crates/pallet)


A searchable document database built on [`sled`](https://docs.rs/sled) and [`tantivy`](https://docs.rs/tantivy).

Provides a "Typed Tree" interface to a `sled` database, with standard datastore ops (`find`, `create`, `update`, `delete`),
but also Lucene/Elastic style searching.

The included `pallet_macros` crate provides an easy way to derive `pallet::DocumentLike` for data structs.

## Usage

```rust
#[derive(serde::Serialize, serde::Deserialize, Debug, pallet::Document)]
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

    let store =
        pallet::Store::<Book>::builder().with_db(db.clone()).with_index_dir(temp_dir.path()).finish()?;

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

## Changelog

### 0.3.0

* Add `pallet_macros` to derive `pallet::DocumentLike`


<hr/>

Current version: 0.3.0

License: MIT
