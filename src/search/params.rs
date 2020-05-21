use std::marker::PhantomData;

use crate::{err, search::AsQuery};

// Internal helper for `Params` builder
#[doc(hidden)]
pub struct Collector<T>(pub(crate) T);

// Internal helper for `Params` builder
#[doc(hidden)]
pub struct Handler<T>(pub(crate) T);

/**
* Builder to prepare search execution
*
* ## Usage:
*
* ```rust
* use pallet::{err, search, Store, DocumentLike, CollectionStore};
*
* fn search<T: DocumentLike>(store: &Store<T>, query: &str) -> err::Result<search::Results<T>> {
*     let scored_ids_handle = search::ScoredIds { size_hint: None, id_field: store.index.id_field };
*     let count_handle = tantivy::collector::Count;
*
*     let search_params = search::Params::default()
*         .with_query(query)
*         .with_collector((count_handle, scored_ids_handle))
*         .with_handler(|(count, scored_ids)| -> err::Result<_> {
*             let hits = scored_ids
*                 .into_iter()
*                 .map(|search::ScoredId { id, score }| {
*                     store.find(id).map(|opt_doc| opt_doc.map(|doc| search::Hit { doc, score }))
*                 })
*                 .filter_map(Result::transpose)
*                 .collect::<err::Result<Vec<_>>>()?;
*
*             Ok(search::Results { count, hits })
*         });
*     Ok(store.search(search_params)?)
* }
* ```
*/
pub struct Params<Q, C, H> {
    pub(crate) query: Q,
    pub(crate) collector: C,
    pub(crate) handler: H,
}

impl Default for Params<(), (), ()> {
    fn default() -> Self {
        Params { query: (), collector: (), handler: () }
    }
}

impl<Q, C, H> Params<Q, C, H> {
    pub fn with_query<N: AsQuery>(self, query: N) -> Params<N, C, H> {
        let Self { collector, handler, .. } = self;
        Params { query, collector, handler }
    }
}

impl<Q> Params<Q, (), ()> {
    pub fn with_handler<
        O,
        C: tantivy::collector::Collector,
        E: From<err::Error>,
        N: Fn(C::Fruit) -> Result<O, E>,
    >(
        self,
        handler: N,
    ) -> Params<Q, PhantomData<fn(C)>, Handler<N>> {
        let Self { query, .. } = self;
        Params { query, collector: PhantomData, handler: Handler(handler) }
    }
}

impl<Q, C: tantivy::collector::Collector> Params<Q, Collector<C>, ()> {
    pub fn with_handler<O, E: From<err::Error>, N: Fn(C::Fruit) -> Result<O, E>>(
        self,
        handler: N,
    ) -> Params<Q, Collector<C>, Handler<N>> {
        let Self { query, collector, .. } = self;
        Params { query, collector, handler: Handler(handler) }
    }
}

impl<Q> Params<Q, (), ()> {
    pub fn with_collector<N>(self, collector: N) -> Params<Q, Collector<N>, ()> {
        let Self { query, handler, .. } = self;
        Params { query, collector: Collector(collector), handler }
    }
}

impl<Q, N, H, O, E> Params<Q, PhantomData<fn(N)>, Handler<H>>
where
    E: From<err::Error>,
    N: tantivy::collector::Collector,
    H: Fn(N::Fruit) -> Result<O, E>,
{
    pub fn with_collector(self, collector: N) -> Params<Q, Collector<N>, Handler<H>> {
        let Self { query, handler, .. } = self;
        Params { query, collector: Collector(collector), handler }
    }
}
