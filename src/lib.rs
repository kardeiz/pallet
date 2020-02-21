/*!

A searchable document datastore built on [`sled`](https://docs.rs/sled) and [`tantivy`](https://docs.rs/tantivy).

Provides a typed-tree interface to a `sled` database, with standard datastore ops (`find`, `create`, `update`, `delete`),
but also Lucene/Elasticsearch style searching.

The included `pallet_macros` crate provides an easy way to derive `pallet::DocumentLike` for data structs.

# Usage

```rust,no_run
#[macro_use] extern crate serde;

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

    let store = pallet::Store::<Book>::builder()
        .with_db(db.clone())
        .with_index_dir(temp_dir.path())
        .finish()?;

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

## 0.3.2

* Add some docs

## 0.3.0

* Add `pallet_macros` to derive `pallet::DocumentLike`

*/

use std::convert::Into;
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
        #[error("Search engine error: `{0}`")]
        Tantivy(tantivy::TantivyError),
        #[error("Search query error: `{0}`")]
        TantivyQueryParser(tantivy::query::QueryParserError),
        #[error("Database error: `{0}`")]
        Sled(#[from] sled::Error),
        #[error("De/serialization error: `{0}`")]
        Bincode(#[from] bincode::Error),
        #[error("Error: {0}")]
        Custom(Box<str>),
    }

    impl Error {
        /// Create a custom error.
        pub fn custom<T: std::fmt::Display>(t: T) -> Self {
            Error::Custom(t.to_string().into_boxed_str())
        }
    }

    impl From<tantivy::TantivyError> for Error {
        fn from(t: tantivy::TantivyError) -> Self {
            Error::Tantivy(t)
        }
    }

    impl From<tantivy::query::QueryParserError> for Error {
        fn from(t: tantivy::query::QueryParserError) -> Self {
            Error::TantivyQueryParser(t)
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

    pub type Result<T> = std::result::Result<T, Error>;
}

mod field_value;

#[doc(hidden)]
// For use primarily by `pallet_macros`.
pub use field_value::FieldValue;

/// Like `tantivy`'s `TopDocs` collector, but without any limit.
#[derive(Default)]
pub struct ScoredDocs {
    size_hint: Option<usize>,
}

#[doc(hidden)]
// Used by the `ScoredDocs` collector.
pub struct ScoredDocsSegmentCollector {
    segment_local_id: tantivy::SegmentLocalId,
    buffer: Vec<(tantivy::Score, tantivy::DocAddress)>,
}

impl tantivy::collector::Collector for ScoredDocs {
    type Fruit = Vec<(tantivy::Score, tantivy::DocAddress)>;
    type Child = ScoredDocsSegmentCollector;

    fn for_segment(
        &self,
        segment_local_id: tantivy::SegmentLocalId,
        _: &tantivy::SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        Ok(ScoredDocsSegmentCollector {
            segment_local_id,
            buffer: self.size_hint.map(|size| Vec::with_capacity(size)).unwrap_or_else(Vec::new),
        })
    }

    fn requires_scoring(&self) -> bool {
        true
    }

    fn merge_fruits(&self, segment_fruits: Vec<Self::Fruit>) -> tantivy::Result<Self::Fruit> {
        let mut out = segment_fruits.into_iter().flat_map(|x| x).collect::<Vec<_>>();
        out.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(b.1.cmp(&a.1)));
        Ok(out)
    }
}

impl tantivy::collector::SegmentCollector for ScoredDocsSegmentCollector {
    type Fruit = Vec<(tantivy::Score, tantivy::DocAddress)>;

    fn collect(&mut self, doc: tantivy::DocId, score: tantivy::Score) {
        self.buffer.push((score, tantivy::DocAddress(self.segment_local_id, doc)));
    }

    fn harvest(self) -> Self::Fruit {
        self.buffer
    }
}

/// `Document` wrapper that includes the search query score.
#[derive(Debug, Clone)]
pub struct Hit<T> {
    pub score: f32,
    pub doc: Document<T>,
}

/// Search results container, contains the `count` of returned results.
#[derive(Debug, Clone)]
pub struct Results<T> {
    pub count: usize,
    pub hits: Vec<Hit<T>>,
}

/// Persisted wrapper of the internal document, includes `id`.
#[derive(Debug, Clone)]
pub struct Document<T> {
    pub id: u64,
    pub inner: T,
}

#[doc(hidden)]
// For use primarily by `pallet_macros`.
pub struct IndexFieldsVec(pub Vec<tantivy::schema::Field>);

/// The document store, contains the `sled::Tree` and `tantivy::Index`.
pub struct Store<T: DocumentLike> {
    generate_id: Box<dyn Fn() -> err::Result<u64> + Sync>,
    tree: sled::Tree,
    marker: PhantomData<fn(T)>,
    index_fields: T::IndexFieldsType,
    index: tantivy::Index,
    index_id_field: tantivy::schema::Field,
    index_writer_accessor:
        Box<dyn Fn(&tantivy::Index) -> tantivy::Result<tantivy::IndexWriter> + Sync>,
}

impl<T: DocumentLike> Store<T> {
    /// Builder for a new `Store`
    pub fn builder() -> StoreBuilder<T> {
        StoreBuilder {
            db: None,
            marker: PhantomData,
            schema_builder: tantivy::schema::SchemaBuilder::default(),
            index_dir: None,
            id_field_name: None,
            tree_name: None,
            index_writer_accessor: None,
            index_configuration: None,
        }
    }

    /// The fields that should be used as default search fields in a search.
    pub fn default_search_fields(&self) -> Vec<tantivy::schema::Field> {
        T::default_search_fields(&self.index_fields)
    }

    /// Get a `tantivy::IndexWriter` from the stored `tantivy::Index`.
    pub fn index_writer(&self) -> err::Result<tantivy::IndexWriter> {
        Ok((&self.index_writer_accessor)(&self.index)?)
    }

    /// Create a new `Document`, returns the persisted document's `id`.
    pub fn create(&self, inner: &T) -> err::Result<u64> {
        let id = self.tree.transaction(
            |tree| -> sled::ConflictableTransactionResult<u64, err::Error> {
                let mut index_writer =
                    self.index_writer().map_err(sled::ConflictableTransactionError::Abort)?;

                let id = (self.generate_id)().map_err(sled::ConflictableTransactionError::Abort)?;

                let serialized_inner = bincode::serialize(inner)
                    .map_err(err::Error::Bincode)
                    .map_err(sled::ConflictableTransactionError::Abort)?;

                let mut search_doc = inner
                    .as_search_document(&self.index_fields)
                    .map_err(sled::ConflictableTransactionError::Abort)?;

                search_doc.add_u64(self.index_id_field, id);

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
                    self.index_writer().map_err(sled::ConflictableTransactionError::Abort)?;

                for inner in inners {
                    let id =
                        (self.generate_id)().map_err(sled::ConflictableTransactionError::Abort)?;

                    let serialized_inner = bincode::serialize(inner)
                        .map_err(err::Error::Bincode)
                        .map_err(sled::ConflictableTransactionError::Abort)?;

                    let mut search_doc = inner
                        .as_search_document(&self.index_fields)
                        .map_err(sled::ConflictableTransactionError::Abort)?;

                    search_doc.add_u64(self.index_id_field, id);

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
                self.index_writer().map_err(sled::ConflictableTransactionError::Abort)?;

            for Document { id, inner } in docs {
                let serialized_inner = bincode::serialize(inner)
                    .map_err(err::Error::Bincode)
                    .map_err(sled::ConflictableTransactionError::Abort)?;

                let mut search_doc = inner
                    .as_search_document(&self.index_fields)
                    .map_err(sled::ConflictableTransactionError::Abort)?;

                search_doc.add_u64(self.index_id_field, *id);

                index_writer.delete_term(tantivy::Term::from_field_u64(self.index_id_field, *id));

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
                self.index_writer().map_err(sled::ConflictableTransactionError::Abort)?;

            for id in ids {
                index_writer.delete_term(tantivy::Term::from_field_u64(self.index_id_field, *id));

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
    pub fn search(&self, query_str: &str) -> err::Result<Results<T>> {
        use rayon::prelude::*;

        let reader = self.index.reader()?;

        let searcher = reader.searcher();

        let query_parser =
            tantivy::query::QueryParser::for_index(&self.index, self.default_search_fields());

        let query = query_parser.parse_query(query_str)?;

        let scored_docs_handle = ScoredDocs { size_hint: None };
        let count_handle = tantivy::collector::Count;

        let (count, scored_docs) = searcher.search(&query, &(count_handle, scored_docs_handle))?;

        let ids_and_scores = scored_docs
            .into_iter()
            .filter_map(|(score, addr)| {
                let opt_id = searcher
                    .segment_reader(addr.segment_ord())
                    .fast_fields()
                    .u64(self.index_id_field.clone())
                    .map(|ffr| ffr.get(addr.doc()));

                opt_id.map(|id| (id, score))
            })
            .collect::<Vec<_>>();

        let hits = ids_and_scores
            .into_par_iter()
            .map(|(id, score)| self.find(id).map(|opt_doc| opt_doc.map(|doc| Hit { doc, score })))
            .filter_map(Result::transpose)
            .collect::<err::Result<Vec<_>>>()?;

        Ok(Results { count, hits })
    }

    /// Get all `Documents` from the datastore. Does not use the search index.
    pub fn all(&self) -> err::Result<Vec<Document<T>>> {
        Ok(self
            .tree
            .iter()
            .flat_map(|x| x)
            .map(|(k, v)| {
                Ok(Document {
                    id: u64::from_le_bytes(k.as_ref().try_into().map_err(err::Error::custom)?),
                    inner: bincode::deserialize(&v)?,
                })
            })
            .collect::<err::Result<Vec<_>>>()?)
    }

    /// Index (or re-index) all `Documents` in the datastore.
    pub fn index_all(&self) -> err::Result<()> {
        let docs = self.all()?;

        let mut index_writer = self.index_writer()?;

        for Document { id, inner } in docs {
            let mut search_doc = inner.as_search_document(&self.index_fields)?;

            search_doc.add_u64(self.index_id_field, id);

            index_writer.delete_term(tantivy::Term::from_field_u64(self.index_id_field, id));

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

/// The `Store` builder struct
pub struct StoreBuilder<T> {
    db: Option<sled::Db>,
    marker: PhantomData<fn(T)>,
    schema_builder: tantivy::schema::SchemaBuilder,
    index_dir: Option<PathBuf>,
    id_field_name: Option<String>,
    tree_name: Option<String>,
    index_writer_accessor:
        Option<Box<dyn Fn(&tantivy::Index) -> tantivy::Result<tantivy::IndexWriter> + Sync>>,
    index_configuration: Option<Box<dyn Fn(&mut tantivy::Index) -> tantivy::Result<()>>>,
}

impl<T: DocumentLike> StoreBuilder<T> {
    /// Use the given `sled::Db` for this `Store`.
    pub fn with_db(mut self, db: sled::Db) -> Self {
        self.db = Some(db);
        self
    }

    /// Use the given directory (must exist) for the `tantivy::Index`.
    pub fn with_index_dir<I: Into<PathBuf>>(mut self, index_dir: I) -> Self {
        self.index_dir = Some(index_dir.into());
        self
    }

    /// Use the given name for the internal `sled::Tree`.
    ///
    /// This can be provided alternatively by implementing `DocumentLike::tree_name`
    /// for your document type.
    pub fn with_tree_name<I: Into<String>>(mut self, tree_name: I) -> Self {
        self.tree_name = Some(tree_name.into());
        self
    }

    /// Define a custom way to get the `tantivy::IndexWriter`.
    ///
    /// By default will use `tantivy_index.writer(128_000_000)`.
    pub fn with_index_writer_accessor<F>(mut self, index_writer_accessor: F) -> Self
    where
        F: Fn(&tantivy::Index) -> tantivy::Result<tantivy::IndexWriter> + Sync + 'static,
    {
        self.index_writer_accessor = Some(Box::new(index_writer_accessor));
        self
    }

    /// Custom configuration for the `tantivy::Index`.
    ///
    /// By default will use `tantivy_index.set_default_multithread_executor()?`.
    pub fn with_index_configuration<F>(mut self, index_configuration: F) -> Self
    where
        F: Fn(&mut tantivy::Index) -> tantivy::Result<()> + 'static,
    {
        self.index_configuration = Some(Box::new(index_configuration));
        self
    }

    /// Finish the builder and return the `Store`.
    pub fn finish(mut self) -> err::Result<Store<T>> {
        let db = self.db.ok_or_else(|| err::Error::custom("`db` not set"))?;

        let index_dir = self.index_dir.ok_or_else(|| err::Error::custom("`index_dir` not set"))?;

        let tree_name = self
            .tree_name
            .or_else(|| T::tree_name())
            .ok_or_else(|| err::Error::custom("`tree_name` not set"))?;

        let tree = db.open_tree(tree_name.as_bytes())?;

        let generate_id = Box::new(move || db.generate_id().map_err(err::Error::from));

        let index_fields = T::index_fields(&mut self.schema_builder)?;

        let index_id_field = match self.id_field_name.as_ref() {
            Some(id_field_name) => self
                .schema_builder
                .add_u64_field(id_field_name, tantivy::schema::INDEXED | tantivy::schema::FAST),
            None => self
                .schema_builder
                .add_u64_field("__id__", tantivy::schema::INDEXED | tantivy::schema::FAST),
        };

        let schema = self.schema_builder.build();

        let mmap_dir = tantivy::directory::MmapDirectory::open(&index_dir)
            .map_err(tantivy::TantivyError::from)?;

        let mut index = tantivy::Index::open_or_create(mmap_dir, schema)?;

        if let Some(index_configuration) = self.index_configuration {
            index_configuration(&mut index)?;
        } else {
            index.set_default_multithread_executor()?;
        }

        let index_writer_accessor =
            self.index_writer_accessor.unwrap_or_else(|| Box::new(|idx| idx.writer(128_000_000)));

        Ok(Store {
            tree,
            generate_id,
            marker: self.marker,
            index_fields,
            index,
            index_id_field,
            index_writer_accessor,
        })
    }
}

/// Defines methods for building the index schema and creating a `tantivy::Document`.
///
/// `pallet_macros` provides a way to automatically derive this trait.
pub trait DocumentLike: serde::Serialize + serde::de::DeserializeOwned + Send {
    /// The container for an index's fields.
    ///
    /// Using `pallet_macros`, this is a wrapped `Vec<tantivy::schema::Field>`.
    type IndexFieldsType: Sync;

    /// Given the specified fields container, return fields that should be used for the default search.
    fn default_search_fields(_index_fields: &Self::IndexFieldsType) -> Vec<tantivy::schema::Field> {
        Vec::new()
    }

    /// Alternative way to set the `sled::Tree` name.
    fn tree_name() -> Option<String> {
        None
    }

    /// Adds all fields to the given `tantivy::schema::SchemaBuilder` and returns the fields container.
    fn index_fields(
        schema_builder: &mut tantivy::schema::SchemaBuilder,
    ) -> err::Result<Self::IndexFieldsType>;

    /// Given the specified document and fields container, returns a `tantivy::Document`.
    fn as_search_document(
        &self,
        index_fields: &Self::IndexFieldsType,
    ) -> err::Result<tantivy::Document>;
}
