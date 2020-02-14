use std::sync::Arc;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::convert::TryInto;

pub mod err {

    #[derive(thiserror::Error, Debug)]
    pub enum Error {
        #[error("Search engine error: `{0}`")]
        Tantivy(tantivy::Error),
        #[error("Search query error: `{0}`")]
        TantivyQueryParser(tantivy::query::QueryParserError),
        #[error("Database error: `{0}`")]
        Sled(#[from] sled::Error),
        // #[error("Database error: `{0}`")]
        // SledTransaction(#[from] sled::TransactionError<Box<Self>>),
         #[error("De/serialization error: `{0}`")]
        Bincode(#[from] bincode::Error),
        #[error("Unknown error")]
        Unknown,
        #[error("Store build error: {0}")]
        StoreBuilder(&'static str),
        #[error("Custom error: {0}")]
        Custom(Box<str>)
    }

    impl Error {
        pub fn custom<T: std::fmt::Display>(t: T) -> Self {
            Error::Custom(t.to_string().into_boxed_str())
        }
    }

    impl From<tantivy::Error> for Error {
        fn from(t: tantivy::Error) -> Self {
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

#[derive(Default)]
pub struct ScoredDocs { size_hint: Option<usize> }

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
        _: &tantivy::SegmentReader
    ) -> tantivy::Result<Self::Child> {
        Ok(ScoredDocsSegmentCollector { 
            segment_local_id, 
            buffer: self.size_hint.map(|size| Vec::with_capacity(size) ).unwrap_or_else(Vec::new)
        })
    }

    fn requires_scoring(&self) -> bool {
        true
    }

    fn merge_fruits(
        &self, 
        segment_fruits: Vec<Self::Fruit>
    ) -> tantivy::Result<Self::Fruit> {
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

pub struct Store<T> {
    db: Arc<sled::Db>,
    marker: PhantomData<fn(T)>,
    ordered_index_fields: Vec<tantivy::schema::Field>,
    index: tantivy::Index,
    index_id_field: tantivy::schema::Field,
    tree_name: String,
}

impl<T> Store<T> {
    pub fn builder() -> StoreBuilder<T> { 
        StoreBuilder {
            db: None,
            marker: PhantomData,
            schema_builder: tantivy::schema::SchemaBuilder::default(),
            index_dir: None,
            id_field_name: None,
            tree_name: None,
        }
    }
}

impl<T: DocumentLike> Store<T> {    

    pub fn create(&self, inner: &T) -> err::Result<u64> {        

        let tree = self.db.open_tree(self.tree_name.as_bytes())?;        

        let id = tree.transaction(|tree| -> sled::ConflictableTransactionResult<u64, err::Error> {

            let mut index_writer = self.index.writer(128_000_000)
                .map_err(err::Error::Tantivy)
                .map_err(sled::ConflictableTransactionError::Abort)?;

            let id = self.db.generate_id()?;            

            let serialized_inner = bincode::serialize(inner)
                .map_err(err::Error::Bincode)
                .map_err(sled::ConflictableTransactionError::Abort)?;

            let mut search_doc = inner.as_search_document(&self.ordered_index_fields)
                .map_err(sled::ConflictableTransactionError::Abort)?;

            search_doc.add_u64(self.index_id_field, id);

            index_writer.add_document(search_doc);

            tree.insert(&id.to_le_bytes(), serialized_inner)?;

            index_writer.commit().map_err(err::Error::Tantivy)
                .map_err(sled::ConflictableTransactionError::Abort)?;

            Ok(id)
        })?;

        Ok(id)
    }

    pub fn create_multi(&self, inners: &[&T]) -> err::Result<Vec<u64>> {        

        let tree = self.db.open_tree(self.tree_name.as_bytes())?;        

        let ids = tree.transaction(|tree| -> sled::ConflictableTransactionResult<_, err::Error> {

            let mut out = Vec::with_capacity(inners.len());

            let mut index_writer = self.index.writer(128_000_000)
                .map_err(err::Error::Tantivy)
                .map_err(sled::ConflictableTransactionError::Abort)?;

            for inner in inners {

                let id = self.db.generate_id()?;

                let serialized_inner = bincode::serialize(inner)
                    .map_err(err::Error::Bincode)
                    .map_err(sled::ConflictableTransactionError::Abort)?;

                let mut search_doc = inner.as_search_document(&self.ordered_index_fields)
                    .map_err(sled::ConflictableTransactionError::Abort)?;

                search_doc.add_u64(self.index_id_field, id);

                index_writer.add_document(search_doc);

                tree.insert(&id.to_le_bytes(), serialized_inner)?;

                out.push(id);
            }

            index_writer.commit().map_err(err::Error::Tantivy)
                .map_err(sled::ConflictableTransactionError::Abort)?;

            Ok(out)
        })?;

        Ok(ids)
    }

    pub fn update(&self, doc: &Document<T>) -> err::Result<()> {        
        self.update_multi(&[doc])
    }

    pub fn update_multi(&self, docs: &[&Document<T>]) -> err::Result<()> {        

        let tree = self.db.open_tree(self.tree_name.as_bytes())?;        

        tree.transaction(|tree| -> sled::ConflictableTransactionResult<_, err::Error> {

            let mut index_writer = self.index.writer(128_000_000)
                .map_err(err::Error::Tantivy)
                .map_err(sled::ConflictableTransactionError::Abort)?;

            for Document { id, inner } in docs {

                let serialized_inner = bincode::serialize(inner)
                    .map_err(err::Error::Bincode)
                    .map_err(sled::ConflictableTransactionError::Abort)?;

                let mut search_doc = inner.as_search_document(&self.ordered_index_fields)
                    .map_err(sled::ConflictableTransactionError::Abort)?;

                search_doc.add_u64(self.index_id_field, *id);

                index_writer.delete_term(tantivy::Term::from_field_u64(self.index_id_field, *id));

                index_writer.add_document(search_doc);

                tree.insert(&id.to_le_bytes(), serialized_inner)?;
            }

            index_writer.commit().map_err(err::Error::Tantivy)
                .map_err(sled::ConflictableTransactionError::Abort)?;

            Ok(())
        })?;

        Ok(())
    }

    pub fn delete(&self, id: u64) -> err::Result<()> {
        self.delete_multi(&[id])
    }

    pub fn delete_multi(&self, ids: &[u64]) -> err::Result<()> {        

        let tree = self.db.open_tree(self.tree_name.as_bytes())?;        

        tree.transaction(|tree| -> sled::ConflictableTransactionResult<_, err::Error> {

            let mut index_writer = self.index.writer(128_000_000)
                .map_err(err::Error::Tantivy)
                .map_err(sled::ConflictableTransactionError::Abort)?;

            for id in ids {

                index_writer.delete_term(tantivy::Term::from_field_u64(self.index_id_field, *id));

                tree.remove(&id.to_le_bytes())?;
            }

            index_writer.commit().map_err(err::Error::Tantivy)
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
            tantivy::query::QueryParser::for_index(&self.index, self.ordered_index_fields.clone());

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
            .map(|(id, score)| self.find(id).map(|opt_doc| opt_doc.map(|doc| Hit { doc, score })) )
            .filter_map(Result::transpose)
            .collect::<err::Result<Vec<_>>>()?;

        Ok(Results { count, hits })

    }

    pub fn all(&self) -> err::Result<Vec<Document<T>>> {
        let tree = self.db.open_tree(self.tree_name.as_bytes())?;

        Ok(tree.iter()
            .flat_map(|x| x)
            .map(|(k, v)| Ok(Document {
                id: u64::from_le_bytes(k.as_ref().try_into().map_err(err::Error::custom)?),
                inner: bincode::deserialize(&v)?
            }))
            .collect::<err::Result<Vec<_>>>()?)
    }

    pub fn index_all(&self) -> err::Result<()> {
        let docs = self.all()?;

        let mut index_writer = self.index.writer(128_000_000)?;

        for Document { id, inner } in docs {

            let mut search_doc = 
                inner.as_search_document(&self.ordered_index_fields)?;

            search_doc.add_u64(self.index_id_field, id);

            index_writer.delete_term(tantivy::Term::from_field_u64(self.index_id_field, id));

            index_writer.add_document(search_doc);
        }

        index_writer.commit()?;

        Ok(())
    }


    pub fn find(&self, id: u64) -> err::Result<Option<Document<T>>> {
        let tree = self.db.open_tree(self.tree_name.as_bytes())?;

        Ok(tree.get(id.to_le_bytes())?
            .map(|bytes| bincode::deserialize(&bytes) )
            .transpose()?
            .map(|inner| Document { id, inner }))
    }

}

#[derive(Default)]
pub struct StoreBuilder<T> {
    db: Option<Arc<sled::Db>>,
    marker: PhantomData<fn(T)>,
    schema_builder: tantivy::schema::SchemaBuilder,
    index_dir: Option<PathBuf>,
    id_field_name: Option<String>,
    tree_name: Option<String>,
}

impl<T: DocumentLike> StoreBuilder<T> {

    pub fn with_db(mut self, db: Arc<sled::Db>) -> Self {
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

    pub fn finish(mut self) -> err::Result<Store<T>> {
        let db = self.db.ok_or_else(|| err::Error::StoreBuilder("`DB` not set"))?;
        let index_dir = self.index_dir.ok_or_else(|| err::Error::StoreBuilder("`index_dir` not set"))?;
        let tree_name = self.tree_name
            .or_else(|| T::tree_name())
            .ok_or_else(|| err::Error::StoreBuilder("`tree_name` not set"))?;

        let ordered_index_fields = T::index_fields(&mut self.schema_builder)?;

        let index_id_field = match self.id_field_name.as_ref() {
            Some(id_field_name) => 
                self.schema_builder.add_u64_field(id_field_name, tantivy::schema::INDEXED | tantivy::schema::FAST),
            None =>
                self.schema_builder.add_u64_field("__id__", tantivy::schema::INDEXED | tantivy::schema::FAST)
        };

        let schema = self.schema_builder.build();

        let mmap_dir = tantivy::directory::MmapDirectory::open(&index_dir)
            .map_err(tantivy::Error::from)?;

        let index = tantivy::Index::open_or_create(mmap_dir, schema)?;

        Ok(Store { db, marker: self.marker, ordered_index_fields, index, index_id_field, tree_name })
    }

}



pub trait DocumentLike: serde::Serialize + serde::de::DeserializeOwned + Send {
    fn tree_name() -> Option<String> {
        None
    }

    fn index_fields(schema_builder: &mut tantivy::schema::SchemaBuilder) -> err::Result<Vec<tantivy::schema::Field>>;

    fn as_search_document(&self, fields: &[tantivy::schema::Field]) -> err::Result<tantivy::Document>;
}









#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
