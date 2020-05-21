use std::path::PathBuf;
use std::sync::Mutex;
use crate::{err, Document, DocumentLike, Store, CollectionStore};

mod as_query;
mod field_value;
mod params;
mod scored_ids;

pub use as_query::AsQuery;
pub use params::Params;
pub use scored_ids::{ScoredId, ScoredIds};

// For use primarily by `pallet_macros`.
#[doc(hidden)]
pub use field_value::FieldValue;

#[doc(hidden)]
// For use primarily by `pallet_macros`.
pub struct FieldsContainer(pub Vec<tantivy::schema::Field>);

/// Wrapper around `tantivy::Index`, with additional search data
pub struct Index<T> {
    pub id_field: tantivy::schema::Field,
    pub fields: T,
    default_search_fields: Vec<tantivy::schema::Field>,
    inner: tantivy::Index,
    pub(crate) writer: Mutex<tantivy::IndexWriter>,
}

impl<T> Index<T> {
    /// Create a new builder
    pub fn builder() -> IndexBuilder<T> {
        IndexBuilder::default()
    }
    /// Get the query parser associated with index and default search fields.
    pub fn query_parser(&self) -> tantivy::query::QueryParser {
        tantivy::query::QueryParser::for_index(&self.inner, self.default_search_fields.clone())
    }
}

/// Builder for an `Index`
pub struct IndexBuilder<T> {
    fields_builder: Option<Box<dyn Fn(&mut tantivy::schema::SchemaBuilder) -> err::Result<T>>>,
    default_search_fields_builder: Option<Box<dyn Fn(&T) -> Vec<tantivy::schema::Field>>>,
    writer_accessor:
        Option<Box<dyn Fn(&tantivy::Index) -> tantivy::Result<tantivy::IndexWriter>>>,
    index_dir: Option<PathBuf>,
    config: Option<Box<dyn Fn(&mut tantivy::Index) -> tantivy::Result<()>>>,
    id_field_name: Option<String>,
}

impl<T> Default for IndexBuilder<T> {
    fn default() -> Self {
        IndexBuilder {
            fields_builder: None,
            default_search_fields_builder: None,
            writer_accessor: None,
            index_dir: None,
            config: None,
            id_field_name: None,
        }
    }
}

impl<T> IndexBuilder<T> {
    pub(crate) fn merge(self, other: Self) -> Self {
        let IndexBuilder {
            fields_builder: a1,
            default_search_fields_builder: a2,
            writer_accessor: a3,
            index_dir: a4,
            config: a5,
            id_field_name: a6,
        } = self;

        let IndexBuilder {
            fields_builder: b1,
            default_search_fields_builder: b2,
            writer_accessor: b3,
            index_dir: b4,
            config: b5,
            id_field_name: b6,
        } = other;

        IndexBuilder {
            fields_builder: a1.or(b1),
            default_search_fields_builder: a2.or(b2),
            writer_accessor: a3.or(b3),
            index_dir: a4.or(b4),
            config: a5.or(b5),
            id_field_name: a6.or(b6),
        }
    }

    /// Use the given directory (must exist) for the `tantivy::Index`.
    pub fn with_index_dir<I: Into<PathBuf>>(mut self, index_dir: I) -> Self {
        self.index_dir = Some(index_dir.into());
        self
    }

    /// Define a custom way to get the `tantivy::IndexWriter`.
    ///
    /// By default will use `tantivy_index.writer(128_000_000)`.
    pub fn with_writer_accessor<F>(mut self, writer_accessor: F) -> Self
    where
        F: Fn(&tantivy::Index) -> tantivy::Result<tantivy::IndexWriter> + 'static,
    {
        self.writer_accessor = Some(Box::new(writer_accessor));
        self
    }

    /// Custom configuration for the `tantivy::Index`.
    ///
    /// By default will use `tantivy_index.set_default_multithread_executor()?`.
    pub fn with_config<F>(mut self, config: F) -> Self
    where
        F: Fn(&mut tantivy::Index) -> tantivy::Result<()> + 'static,
    {
        self.config = Some(Box::new(config));
        self
    }

    /// Set the field name to be used for the datastore `id`.
    ///
    /// By default will use `__id__`.
    pub fn with_id_field_name<I: Into<String>>(mut self, id_field_name: I) -> Self {
        self.id_field_name = Some(id_field_name.into());
        self
    }

    /// Handler that adds fields to a schema, and returns them in the fields container
    pub fn with_fields_builder<F>(mut self, fields_builder: F) -> Self
    where
        F: Fn(&mut tantivy::schema::SchemaBuilder) -> err::Result<T> + 'static,
    {
        self.fields_builder = Some(Box::new(fields_builder));
        self
    }

    /// Given the fields container, return fields that should be used in default search.
    pub fn with_default_search_fields_builder<F>(mut self, default_search_fields_builder: F) -> Self
    where
        F: Fn(&T) -> Vec<tantivy::schema::Field> + 'static,
    {
        self.default_search_fields_builder = Some(Box::new(default_search_fields_builder));
        self
    }

    /// Convert into finished `Index`
    pub fn finish(self) -> err::Result<Index<T>> {
        let fields_builder =
            self.fields_builder.ok_or_else(|| err::custom("`fields_builder` not set"))?;

        let index_dir = self.index_dir.ok_or_else(|| err::custom("`index_dir` not set"))?;

        let mut schema_builder = tantivy::schema::SchemaBuilder::default();

        let fields = fields_builder(&mut schema_builder)?;

        let id_field = match self.id_field_name.as_ref() {
            Some(id_field_name) => schema_builder
                .add_u64_field(id_field_name, tantivy::schema::INDEXED | tantivy::schema::FAST),
            None => schema_builder
                .add_u64_field("__id__", tantivy::schema::INDEXED | tantivy::schema::FAST),
        };

        let schema = schema_builder.build();

        let mmap_dir = tantivy::directory::MmapDirectory::open(&index_dir)
            .map_err(tantivy::TantivyError::from)?;

        let mut index = tantivy::Index::open_or_create(mmap_dir, schema)?;

        if let Some(config) = self.config {
            config(&mut index)?;
        } else {
            index.set_default_multithread_executor()?;
        }

        let writer_accessor =
            self.writer_accessor.unwrap_or_else(|| Box::new(|idx| idx.writer(128_000_000)));

        let default_search_fields =
            if let Some(default_search_fields_builder) = self.default_search_fields_builder {
                default_search_fields_builder(&fields)
            } else {
                Vec::new()
            };

        let writer = writer_accessor(&index)?;

        Ok(Index { 
            default_search_fields, 
            inner: index, 
            id_field, 
            fields, 
            // writer_accessor,
            writer: Mutex::new(writer)
        })
    }
}

/// `Document` wrapper that includes the search query score
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Hit<T> {
    pub score: f32,
    pub doc: Document<T>,
}

/// Search results container, contains the `count` of returned results
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Results<T> {
    pub count: usize,
    pub hits: Vec<Hit<T>>,
}

/// Items that function as search parameters
pub trait Searcher<T: DocumentLike> {
    type Item;
    type Error: From<err::Error>;
    fn search(&self, store: &Store<T>) -> Result<Self::Item, Self::Error>;
}

impl<Q, C, H, O, T, E> Searcher<T> for Params<Q, params::Collector<C>, params::Handler<H>>
where
    Q: AsQuery,
    E: From<err::Error>,
    C: tantivy::collector::Collector,
    H: Fn(C::Fruit) -> Result<O, E>,
    T: DocumentLike,
{
    type Item = O;
    type Error = E;

    fn search(&self, store: &Store<T>) -> Result<Self::Item, Self::Error> {
        let Self {
            query: ref query_like,
            collector: params::Collector(ref collector),
            handler: params::Handler(ref handler),
            ..
        } = self;

        let reader = store.index.inner.reader().map_err(err::Error::from)?;

        let searcher = reader.searcher();

        let query = query_like.as_query(&store.index.inner, &store.index.default_search_fields)?;

        let fruit = searcher.search(query.as_ref(), collector).map_err(err::Error::from)?;

        handler(fruit)
    }
}

impl<Q, T> Searcher<T> for Q
where
    Q: AsQuery,
    T: DocumentLike + Send,
    T::IndexFieldsType: Sync,
{
    type Item = Results<T>;
    type Error = err::Error;

    fn search(&self, store: &Store<T>) -> Result<Self::Item, Self::Error> {
        use rayon::prelude::*;

        let scored_ids_handle = ScoredIds { size_hint: None, id_field: store.index.id_field };
        let count_handle = tantivy::collector::Count;

        let query = self.as_query(&store.index.inner, &store.index.default_search_fields)?;

        let search_params = Params::default()
            .with_query(query)
            .with_collector((count_handle, scored_ids_handle))
            .with_handler(|(count, scored_ids)| -> Result<_, err::Error> {
                let hits = scored_ids
                    .into_par_iter()
                    .map(|ScoredId { id, score }| {
                        store.find(id).map(|opt_doc| opt_doc.map(|doc| Hit { doc, score }))
                    })
                    .filter_map(Result::transpose)
                    .collect::<err::Result<Vec<_>>>()?;

                Ok(Results { count, hits })
            });
        Ok(search_params.search(store)?)
    }
}
