use crate::{err, Document, DocumentLike, Store};
use std::marker::PhantomData;

mod field_value;

// For use primarily by `pallet_macros`.
#[doc(hidden)]
pub use field_value::FieldValue;

// // Internal repr for `AsQuery::as_query` output
// #[doc(hidden)]
// pub enum MaybeOwned<'a, T> {
//     Owned(Box<T>),
//     Borrowed(&'a T),
// }

pub enum QueryContainer<'a> {
    Boxed(Box<dyn tantivy::query::Query>),
    Ref(&'a dyn tantivy::query::Query)
}

impl<'a> AsRef<dyn tantivy::query::Query> for QueryContainer<'a> {
    fn as_ref(&self) -> &dyn tantivy::query::Query {
        match self {
            Self::Boxed(ref t) => t,
            Self::Ref(t) => *t,
        }
    }
}

/// Items that can be expressed as a query
pub trait AsQuery {
    fn as_query(
        &self,
        index: &tantivy::Index,
        default_search_fields: Vec<tantivy::schema::Field>,
    ) -> err::Result<QueryContainer>;
}

impl AsQuery for str {
    fn as_query(
        &self,
        index: &tantivy::Index,
        default_search_fields: Vec<tantivy::schema::Field>,
    ) -> err::Result<QueryContainer> {
        let query_parser =
            tantivy::query::QueryParser::for_index(index, default_search_fields);

        let query = query_parser.parse_query(self)?;

        Ok(QueryContainer::Boxed(query))
    }
}

impl<'a> AsQuery for QueryContainer<'a>
{
    fn as_query(
        &self,
        _index: &tantivy::Index,
        _default_search_fields: Vec<tantivy::schema::Field>,
    ) -> err::Result<QueryContainer> {
        Ok(QueryContainer::Ref(self.as_ref()))
    }
}

impl<U> AsQuery for U
where
    U: tantivy::query::Query,
{
    fn as_query(
        &self,
        index: &tantivy::Index,
        default_search_fields: Vec<tantivy::schema::Field>,
    ) -> err::Result<QueryContainer> {
        Ok(QueryContainer::Ref(self))
    }
}

#[doc(hidden)]
// For use primarily by `pallet_macros`.
pub struct FieldsContainer(pub Vec<tantivy::schema::Field>);

/// Wrapper around `tantivy::Index`, with additional search data
pub struct Index<F> {
    pub id_field: tantivy::schema::Field,
    pub fields: F,
    pub(crate) default_search_fields: Vec<tantivy::schema::Field>,
    pub(crate) inner: tantivy::Index,
    pub(crate) writer_accessor:
        Box<dyn Fn(&tantivy::Index) -> tantivy::Result<tantivy::IndexWriter> + Send + Sync>,
}

impl<F> Index<F> {
    /// Get a `tantivy::IndexWriter` from the stored `tantivy::Index`.
    pub fn writer(&self) -> err::Result<tantivy::IndexWriter> {
        Ok((self.writer_accessor)(&self.inner)?)
    }

    /// Get the query parser associated with index and default search fields.
    pub fn query_parser(&self) -> tantivy::query::QueryParser {
        tantivy::query::QueryParser::for_index(&self.inner, self.default_search_fields.clone())
    }
}

/// Datastore `id`, scored according to search performance
pub struct ScoredId {
    pub id: u64,
    pub score: f32,
}

/// Like `tantivy`'s `TopDocs` collector, but without any limit
pub struct ScoredIds {
    pub size_hint: Option<usize>,
    pub id_field: tantivy::schema::Field,
}

// Used by the `ScoredIds` collector.
#[doc(hidden)]
pub struct ScoredIdsSegmentCollector {
    id_field_reader: Option<tantivy::fastfield::FastFieldReader<u64>>,
    buffer: Vec<ScoredId>,
}

impl tantivy::collector::Collector for ScoredIds {
    type Fruit = Vec<ScoredId>;
    type Child = ScoredIdsSegmentCollector;

    fn for_segment(
        &self,
        _segment_local_id: tantivy::SegmentLocalId,
        segment: &tantivy::SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        Ok(ScoredIdsSegmentCollector {
            buffer: self.size_hint.map(Vec::with_capacity).unwrap_or_else(Vec::new),
            id_field_reader: segment.fast_fields().u64(self.id_field.clone()),
        })
    }

    fn requires_scoring(&self) -> bool {
        true
    }

    fn merge_fruits(&self, segment_fruits: Vec<Self::Fruit>) -> tantivy::Result<Self::Fruit> {
        let mut out = segment_fruits.into_iter().flatten().collect::<Vec<_>>();
        out.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or_else(|| a.id.cmp(&b.id)));
        Ok(out)
    }
}

impl tantivy::collector::SegmentCollector for ScoredIdsSegmentCollector {
    type Fruit = Vec<ScoredId>;

    fn collect(&mut self, doc: tantivy::DocId, score: tantivy::Score) {
        if let Some(ref id_field_reader) = self.id_field_reader {
            self.buffer.push(ScoredId { score, id: id_field_reader.get(doc) });
        }
    }

    fn harvest(self) -> Self::Fruit {
        self.buffer
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
pub struct SearchResults<T> {
    pub count: usize,
    pub hits: Vec<Hit<T>>,
}

// Internal helper for `SearchParams` builder
#[doc(hidden)]
pub struct Collector<T>(T);

// Internal helper for `SearchParams` builder
#[doc(hidden)]
pub struct Handler<T>(T);

/**
Builder to prepare search execution

## Usage:

```rust,ignore
let search_params = SearchParams::default()
    .with_query("my search terms here")
    .with_collector((tantivy::collector::Count, scored_ids_handle))
    .with_handler(|(count, scored_ids)| {
        let hits = scored_ids
            .into_par_iter()
            .map(|ScoredId { id, score }| {
                store.find(id).map(|opt_doc| opt_doc.map(|doc| Hit { doc, score }))
            })
            .filter_map(Result::transpose)
            .collect::<err::Result<Vec<_>>>()?;

        Ok(SearchResults { count, hits })
    });
Ok(search_params.search(store)?)
```
*/
pub struct SearchParams<Q, C, H> {
    query: Q,
    collector: C,
    handler: H,
}

impl Default for SearchParams<(), (), ()> {
    fn default() -> Self {
        SearchParams { query: (), collector: (), handler: () }
    }
}

impl<Q, C, H> SearchParams<Q, C, H> {
    pub fn with_query<N: AsQuery>(self, query: N) -> SearchParams<N, C, H> {
        let Self { collector, handler, .. } = self;
        SearchParams { query, collector, handler }
    }
}

impl<Q> SearchParams<Q, (), ()> {
    pub fn with_handler<O, C: tantivy::collector::Collector, E: From<err::Error>, N: Fn(C::Fruit) -> Result<O, E>>(
        self,
        handler: N,
    ) -> SearchParams<Q, PhantomData<fn(C)>, Handler<N>> {
        let Self { query, .. } = self;
        SearchParams { query, collector: PhantomData, handler: Handler(handler) }
    }
}

impl<Q, C: tantivy::collector::Collector> SearchParams<Q, Collector<C>, ()> {
    pub fn with_handler<O, E: From<err::Error>, N: Fn(C::Fruit) -> Result<O, E>>(
        self,
        handler: N,
    ) -> SearchParams<Q, Collector<C>, Handler<N>> {
        let Self { query, collector, .. } = self;
        SearchParams { query, collector, handler: Handler(handler) }
    }
}

impl<Q> SearchParams<Q, (), ()> {
    pub fn with_collector<N>(self, collector: N) -> SearchParams<Q, Collector<N>, ()> {
        let Self { query, handler, .. } = self;
        SearchParams { query, collector: Collector(collector), handler }
    }
}

impl<Q, N, H, O, E> SearchParams<Q, PhantomData<fn(N)>, Handler<H>>
where
    E: From<err::Error>,
    N: tantivy::collector::Collector,
    H: Fn(N::Fruit) -> Result<O, E>,
{
    pub fn with_collector(self, collector: N) -> SearchParams<Q, Collector<N>, Handler<H>> {
        let Self { query, handler, .. } = self;
        SearchParams { query, collector: Collector(collector), handler }
    }
}

/// Items that function as search parameters
pub trait Searcher<T: DocumentLike> {
    type Item;
    type Error: From<err::Error>;
    fn search(&self, store: &Store<T>) -> Result<Self::Item, Self::Error>;
}

impl<Q, C, H, O, T, E> Searcher<T> for SearchParams<Q, Collector<C>, Handler<H>>
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
            collector: Collector(ref collector),
            handler: Handler(ref handler),
            ..
        } = self;

        let reader = store.index.inner.reader().map_err(err::Error::from)?;

        let searcher = reader.searcher();

        let query = query_like.as_query(&store.index.inner, store.index.default_search_fields.clone())?;

        let fruit = searcher.search(query.as_ref(), collector).map_err(err::Error::from)?;

        handler(fruit)
    }
}

impl<Q, T> Searcher<T> for Q
where
    Q: AsQuery,
    T: DocumentLike,
    T::IndexFieldsType: Sync,
{
    type Item = SearchResults<T>;
    type Error = err::Error;

    fn search(&self, store: &Store<T>) -> Result<Self::Item, Self::Error> {
        use rayon::prelude::*;

        let scored_ids_handle = ScoredIds { size_hint: None, id_field: store.index.id_field };
        let count_handle = tantivy::collector::Count;

        let query = self.as_query(&store.index.inner, store.index.default_search_fields.clone())?;

        let search_params = SearchParams::default()
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

                Ok(SearchResults { count, hits })
            });
        Ok(search_params.search(store)?)
    }
}
