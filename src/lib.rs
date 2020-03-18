/*!
A searchable document datastore built on [`sled`](https://docs.rs/sled) and [`tantivy`](https://docs.rs/tantivy).

Provides a typed-tree interface to a `sled` database, with standard datastore ops (`find`, `create`, `update`, `delete`),
but also Lucene/Elasticsearch style searching.

The included `pallet_macros` crate provides an easy way to derive `pallet::DocumentLike` for data structs.

# Usage

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

# `pallet_macros`

See the example for usage. The following attributes can be used to customize the implementation:

* `tree_name`: A container level attribute to specify the `sled::Tree` name.
* `index_field_name`: Rename the field in the search schema.
* `index_field_type`: Set the index field type, must implement `Into<tantivy::schema::Value>`.
* `index_field_options`: Set the index field options. By default, the options for `String` is
`tantivy::schema::TEXT`, and the options for numeric types is `tantivy::schema::INDEXED`.
* `default_search_field`: Include this field in the list of default search fields.
* `skip_indexing`: Do not index this field.

# Changelog

## 0.5.0

* Add `Deref` to inner type on `Document`
* Make index writer persistent

## 0.4.0

* Add various builders.
* Split out `index` and `tree` functionality.
* Set up `search::Searcher` trait and other search helpers.

## 0.3.2

* Add some docs

## 0.3.0

* Add `pallet_macros` to derive `pallet::DocumentLike`

*/

use std::convert::TryInto;
use std::marker::PhantomData;
use std::path::PathBuf;

/// Re-export the `pallet_macros` derive type.
pub use pallet_macros::DocumentLike;

/// Re-exports `tantivy` and `sled` for use by `pallet_macros` and convenience.
pub mod ext {
    pub use sled;
    pub use tantivy;
}

/// Error management
pub mod err {

    /// Error container
    #[derive(thiserror::Error, Debug)]
    pub enum Error {
        #[error("Search error: `{0}`")]
        Tantivy(tantivy::TantivyError),
        #[error("Database error: `{0}`")]
        Sled(#[from] sled::Error),
        #[error("De/serialization error: `{0}`")]
        Bincode(#[from] bincode::Error),
        #[error("Error: {0}")]
        Custom(Box<str>),
    }

    impl From<tantivy::TantivyError> for Error {
        fn from(t: tantivy::TantivyError) -> Self {
            Error::Tantivy(t)
        }
    }

    impl From<tantivy::query::QueryParserError> for Error {
        fn from(t: tantivy::query::QueryParserError) -> Self {
            Error::Tantivy(t.into())
        }
    }

    impl From<sled::TransactionError<Error>> for Error {
        fn from(t: sled::TransactionError<Error>) -> Self {
            match t {
                sled::TransactionError::Abort(t) => t,
                sled::TransactionError::Storage(t) => Error::Sled(t),
            }
        }
    }

    /// Create a custom error.
    pub fn custom<T: std::fmt::Display>(t: T) -> Error {
        Error::Custom(t.to_string().into_boxed_str())
    }

    pub type Result<T> = std::result::Result<T, Error>;
}

/// Items related to `sled` and data storage
pub mod db;

/// Items relating to `tantivy` and searching
pub mod search;

/// Persisted wrapper of the internal document, includes `id`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Document<T> {
    pub id: u64,
    pub inner: T,
}

impl<T> std::ops::Deref for Document<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> std::ops::DerefMut for Document<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}


/// The document store, contains the `sled::Tree` and `tantivy::Index`.
pub struct Store<T: DocumentLike> {
    tree: db::Tree,
    marker: PhantomData<fn(T)>,
    pub index: search::Index<T::IndexFieldsType>,
}

impl<T: DocumentLike> Store<T> {
    /// Create a new builder
    pub fn builder() -> StoreBuilder<T> {
        StoreBuilder::default()
    }

    /// Create a new `Document`, returns the persisted document's `id`.
    pub fn create(&self, inner: &T) -> err::Result<u64> {
        let id = self.tree.transaction(
            |tree| -> sled::ConflictableTransactionResult<u64, err::Error> {
                let mut index_writer =
                    self.index.writer.lock().map_err(err::custom).map_err(sled::ConflictableTransactionError::Abort)?;

                let id =
                    self.tree.generate_id().map_err(sled::ConflictableTransactionError::Abort)?;

                let serialized_inner = bincode::serialize(inner)
                    .map_err(err::Error::Bincode)
                    .map_err(sled::ConflictableTransactionError::Abort)?;

                let mut search_doc = inner
                    .as_index_document(&self.index.fields)
                    .map_err(sled::ConflictableTransactionError::Abort)?;

                search_doc.add_u64(self.index.id_field, id);

                index_writer.add_document(search_doc);

                tree.insert(&id.to_le_bytes(), serialized_inner)?;

                index_writer
                    .commit()
                    .map_err(err::Error::Tantivy)
                    .map_err(sled::ConflictableTransactionError::Abort)?;

                Ok(id)
            },
        )?;

        Ok(id)
    }

    /// Create new `Document`s, returns the persisted documents' `id`s.
    pub fn create_multi(&self, inners: &[T]) -> err::Result<Vec<u64>> {
        let ids = self.tree.transaction(
            |tree| -> sled::ConflictableTransactionResult<_, err::Error> {
                let mut out = Vec::with_capacity(inners.len());

                let mut index_writer =
                    self.index.writer.lock().map_err(err::custom).map_err(sled::ConflictableTransactionError::Abort)?;

                for inner in inners {
                    let id = self
                        .tree
                        .generate_id()
                        .map_err(sled::ConflictableTransactionError::Abort)?;

                    let serialized_inner = bincode::serialize(inner)
                        .map_err(err::Error::Bincode)
                        .map_err(sled::ConflictableTransactionError::Abort)?;

                    let mut search_doc = inner
                        .as_index_document(&self.index.fields)
                        .map_err(sled::ConflictableTransactionError::Abort)?;

                    search_doc.add_u64(self.index.id_field, id);

                    index_writer.add_document(search_doc);

                    tree.insert(&id.to_le_bytes(), serialized_inner)?;

                    out.push(id);
                }

                index_writer
                    .commit()
                    .map_err(err::Error::Tantivy)
                    .map_err(sled::ConflictableTransactionError::Abort)?;

                Ok(out)
            },
        )?;

        Ok(ids)
    }

    /// Update a given `Document`.
    pub fn update(&self, doc: &Document<T>) -> err::Result<()> {
        self.update_multi(std::slice::from_ref(doc))
    }

    /// Update given `Document`s.
    pub fn update_multi(&self, docs: &[Document<T>]) -> err::Result<()> {
        self.tree.transaction(|tree| -> sled::ConflictableTransactionResult<_, err::Error> {
            let mut index_writer =
                self.index.writer.lock().map_err(err::custom).map_err(sled::ConflictableTransactionError::Abort)?;

            for Document { id, inner } in docs {
                let serialized_inner = bincode::serialize(inner)
                    .map_err(err::Error::Bincode)
                    .map_err(sled::ConflictableTransactionError::Abort)?;

                let mut search_doc = inner
                    .as_index_document(&self.index.fields)
                    .map_err(sled::ConflictableTransactionError::Abort)?;

                search_doc.add_u64(self.index.id_field, *id);

                index_writer.delete_term(tantivy::Term::from_field_u64(self.index.id_field, *id));

                index_writer.add_document(search_doc);

                tree.insert(&id.to_le_bytes(), serialized_inner)?;
            }

            index_writer
                .commit()
                .map_err(err::Error::Tantivy)
                .map_err(sled::ConflictableTransactionError::Abort)?;

            Ok(())
        })?;

        Ok(())
    }

    /// Delete a `Document` by `id`.
    pub fn delete(&self, id: u64) -> err::Result<()> {
        self.delete_multi(&[id])
    }

    /// Delete `Document`s by `id`s.
    pub fn delete_multi(&self, ids: &[u64]) -> err::Result<()> {
        self.tree.transaction(|tree| -> sled::ConflictableTransactionResult<_, err::Error> {
            let mut index_writer =
                self.index.writer.lock().map_err(err::custom).map_err(sled::ConflictableTransactionError::Abort)?;

            for id in ids {
                index_writer.delete_term(tantivy::Term::from_field_u64(self.index.id_field, *id));

                tree.remove(&id.to_le_bytes())?;
            }

            index_writer
                .commit()
                .map_err(err::Error::Tantivy)
                .map_err(sled::ConflictableTransactionError::Abort)?;

            Ok(())
        })?;

        Ok(())
    }

    /// Search the datastore, using the query language provided by `tantivy`.
    pub fn search<I: search::Searcher<T>>(&self, searcher: I) -> Result<I::Item, I::Error> {
        searcher.search(self)
    }

    /// Get all `Documents` from the datastore. Does not use the search index.
    pub fn all(&self) -> err::Result<Vec<Document<T>>> {
        Ok(self
            .tree
            .iter()
            .flatten()
            .map(|(k, v)| {
                Ok(Document {
                    id: u64::from_le_bytes(k.as_ref().try_into().map_err(err::custom)?),
                    inner: bincode::deserialize(&v)?,
                })
            })
            .collect::<err::Result<Vec<_>>>()?)
    }

    /// Index (or re-index) all `Documents` in the datastore.
    pub fn index_all(&self) -> err::Result<()> {
        let docs = self.all()?;

        let mut index_writer = self.index.writer.lock().map_err(err::custom)?;

        for Document { id, inner } in docs {
            let mut search_doc = inner.as_index_document(&self.index.fields)?;

            search_doc.add_u64(self.index.id_field, id);

            index_writer.delete_term(tantivy::Term::from_field_u64(self.index.id_field, id));

            index_writer.add_document(search_doc);
        }

        index_writer.commit()?;

        Ok(())
    }

    /// Find a single `Document` by its `id`. Does not use the search index.
    pub fn find(&self, id: u64) -> err::Result<Option<Document<T>>> {
        Ok(self
            .tree
            .get(id.to_le_bytes())?
            .map(|bytes| bincode::deserialize(&bytes))
            .transpose()?
            .map(|inner| Document { id, inner }))
    }
}

/// Builder for `Store`
pub struct StoreBuilder<T: DocumentLike> {
    tree_builder: db::TreeBuilder,
    index_builder: search::IndexBuilder<T::IndexFieldsType>,
    marker: PhantomData<fn(T)>,
}

impl<T: DocumentLike> Default for StoreBuilder<T> {
    fn default() -> Self {
        StoreBuilder {
            tree_builder: db::TreeBuilder::default(),
            index_builder: search::IndexBuilder::default(),
            marker: PhantomData,
        }
    }
}

impl<T: DocumentLike> StoreBuilder<T> {
    /// Shortcut method to set the `sled::Db` for the `tree_builder`
    pub fn with_db(mut self, db: sled::Db) -> Self {
        self.tree_builder = self.tree_builder.with_db(db);
        self
    }

    /// Shortcut method to set the index dir for the `index_builder`
    pub fn with_index_dir<I: Into<PathBuf>>(mut self, index_dir: I) -> Self {
        self.index_builder = self.index_builder.with_index_dir(index_dir);
        self
    }

    /// Set the `tree_builder` to be used.
    pub fn with_tree_builder(mut self, tree_builder: db::TreeBuilder) -> Self {
        self.tree_builder = tree_builder;
        self
    }

    /// Set the `index_builder` to be used.
    pub fn with_index_builder(
        mut self,
        index_builder: search::IndexBuilder<T::IndexFieldsType>,
    ) -> Self {
        self.index_builder = index_builder;
        self
    }

    /// Convert into finished `Store`
    pub fn finish(self) -> err::Result<Store<T>> {
        let tree = self.tree_builder.merge(T::tree_builder()).finish()?;

        let index = self.index_builder.merge(T::index_builder()).finish()?;

        Ok(Store { tree, index, marker: PhantomData })
    }
}

/// Defines methods for building the index schema and creating a `tantivy::Document`.
///
/// `pallet_macros` provides a way to automatically derive this trait.
pub trait DocumentLike: serde::Serialize + serde::de::DeserializeOwned {
    /// The container for an index's fields.
    ///
    /// When using `pallet_macros`, this is a wrapped `Vec<tantivy::schema::Field>`.
    type IndexFieldsType;

    /// Given the specified document and fields container, returns a `tantivy::Document`.
    fn as_index_document(
        &self,
        index_fields: &Self::IndexFieldsType,
    ) -> err::Result<tantivy::Document>;

    /// Can be provided to set some or all of the `Tree` config.
    ///
    /// Will be merged with any configuration provided in `StoreBuilder::tree_builder`
    fn tree_builder() -> db::TreeBuilder {
        db::TreeBuilder::default()
    }

    /// Can be provided to set some or all of the `Index` config.
    ///
    /// Will be merged with any configuration provided in `StoreBuilder::index_builder`
    fn index_builder() -> search::IndexBuilder<Self::IndexFieldsType> {
        search::IndexBuilder::default()
    }
}
