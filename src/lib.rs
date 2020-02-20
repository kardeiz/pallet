/*!

A searchable document database built on [`sled`](https://docs.rs/sled) and [`tantivy`](https://docs.rs/tantivy).

Provides a "Typed Tree" interface to a `sled` database, with standard datastore ops (`find`, `create`, `update`, `delete`),
but also Lucene/Elastic style searching.

The included `pallet_macros` crate provides an easy way to derive `pallet::DocumentLike` for data structs.

# Usage

```rust,no_run
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

# Changelog

## 0.3.0

* Add `pallet_macros` to derive `pallet::DocumentLike`

*/

use std::convert::Into;
use std::convert::TryInto;
use std::marker::PhantomData;
use std::path::PathBuf;

pub use pallet_macros::Document;

pub mod ext {
    pub use tantivy;
    pub use sled;
}

pub mod err {

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
        #[error("Unknown error")]
        Unknown,
        #[error("Store build error: {0}")]
        StoreBuilder(&'static str),
        #[error("Custom error: {0}")]
        Custom(Box<str>),
    }

    impl Error {
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
pub use field_value::FieldValue;

#[derive(Default)]
pub struct ScoredDocs {
    size_hint: Option<usize>,
}

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

#[derive(Debug, Clone)]
pub struct Hit<T> {
    pub score: f32,
    pub doc: Document<T>,
}

#[derive(Debug, Clone)]
pub struct Results<T> {
    pub count: usize,
    pub hits: Vec<Hit<T>>,
}

#[derive(Debug, Clone)]
pub struct Document<T> {
    pub id: u64,
    pub inner: T,
}

pub struct IndexFieldsVec(pub Vec<tantivy::schema::Field>);

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

    fn default_search_fields(&self) -> Vec<tantivy::schema::Field> {
        T::default_search_fields(&self.index_fields)
    }

    fn index_writer(&self) -> err::Result<tantivy::IndexWriter> {
        Ok((&self.index_writer_accessor)(&self.index)?)
    }

    pub fn create(&self, inner: &T) -> err::Result<u64> {

        let id =
            self.tree.transaction(|tree| -> sled::ConflictableTransactionResult<u64, err::Error> {
                let mut index_writer =
                    self.index_writer().map_err(sled::ConflictableTransactionError::Abort)?;

                let id = (self.generate_id)()
                    .map_err(sled::ConflictableTransactionError::Abort)?;

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
            })?;

        Ok(id)
    }

    pub fn create_multi(&self, inners: &[T]) -> err::Result<Vec<u64>> {

        let ids =
            self.tree.transaction(|tree| -> sled::ConflictableTransactionResult<_, err::Error> {
                let mut out = Vec::with_capacity(inners.len());

                let mut index_writer =
                    self.index_writer().map_err(sled::ConflictableTransactionError::Abort)?;

                for inner in inners {
                    let id = (self.generate_id)()
                        .map_err(sled::ConflictableTransactionError::Abort)?;

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
            })?;

        Ok(ids)
    }

    pub fn update(&self, doc: &Document<T>) -> err::Result<()> {
        self.update_multi(std::slice::from_ref(doc))
    }

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

    pub fn delete(&self, id: u64) -> err::Result<()> {
        self.delete_multi(&[id])
    }

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

    pub fn search(&self, query_str: &str) -> err::Result<Results<T>> {
        use rayon::prelude::*;

        let reader = self.index.reader()?;

        let searcher = reader.searcher();

        let query_parser =
            tantivy::query::QueryParser::for_index(&self.index, self.default_search_fields());

        let query = query_parser.parse_query(query_str)?;

        let top_docs_handle = ScoredDocs { size_hint: None };
        let count_handle = tantivy::collector::Count;

        let (count, top_docs) = searcher.search(&query, &(count_handle, top_docs_handle))?;

        let ids_and_scores = top_docs
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

    pub fn all(&self) -> err::Result<Vec<Document<T>>> {

        Ok(self.tree
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

    pub fn find(&self, id: u64) -> err::Result<Option<Document<T>>> {

        Ok(self.tree
            .get(id.to_le_bytes())?
            .map(|bytes| bincode::deserialize(&bytes))
            .transpose()?
            .map(|inner| Document { id, inner }))
    }
}

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
    pub fn with_db(mut self, db: sled::Db) -> Self {
        self.db = Some(db);
        self
    }

    pub fn with_index_dir<I: Into<PathBuf>>(mut self, index_dir: I) -> Self {
        self.index_dir = Some(index_dir.into());
        self
    }

    pub fn with_tree_name<I: Into<String>>(mut self, tree_name: I) -> Self {
        self.tree_name = Some(tree_name.into());
        self
    }

    pub fn with_index_writer_accessor<F>(mut self, index_writer_accessor: F) -> Self
    where
        F: Fn(&tantivy::Index) -> tantivy::Result<tantivy::IndexWriter> + Sync + 'static,
    {
        self.index_writer_accessor = Some(Box::new(index_writer_accessor));
        self
    }

    pub fn with_index_configuration<F>(mut self, index_configuration: F) -> Self
    where
        F: Fn(&mut tantivy::Index) -> tantivy::Result<()> + 'static,
    {
        self.index_configuration = Some(Box::new(index_configuration));
        self
    }

    pub fn finish(mut self) -> err::Result<Store<T>> {
        let db =
            self.db.ok_or_else(|| err::Error::StoreBuilder("`db` not set"))?;
        
        let index_dir =
            self.index_dir.ok_or_else(|| err::Error::StoreBuilder("`index_dir` not set"))?;
        
        let tree_name = self
            .tree_name
            .or_else(|| T::tree_name())
            .ok_or_else(|| err::Error::StoreBuilder("`tree_name` not set"))?;

        let tree = db.open_tree(tree_name.as_bytes())?;

        let generate_id = Box::new(move || db.generate_id().map_err(err::Error::from) );

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

pub trait DocumentLike: serde::Serialize + serde::de::DeserializeOwned + Send {
    type IndexFieldsType: Sync;

    fn default_search_fields(_index_fields: &Self::IndexFieldsType) -> Vec<tantivy::schema::Field> {
        Vec::new()
    }

    fn tree_name() -> Option<String> {
        None
    }

    fn index_fields(
        schema_builder: &mut tantivy::schema::SchemaBuilder,
    ) -> err::Result<Self::IndexFieldsType>;

    fn as_search_document(
        &self,
        index_fields: &Self::IndexFieldsType,
    ) -> err::Result<tantivy::Document>;
}
