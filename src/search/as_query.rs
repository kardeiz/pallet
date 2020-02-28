use crate::err;

pub enum QueryContainer<'a> {
    Boxed(Box<dyn tantivy::query::Query>),
    Ref(&'a dyn tantivy::query::Query),
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
        default_search_fields: &[tantivy::schema::Field],
    ) -> err::Result<QueryContainer>;
}

impl AsQuery for str {
    fn as_query(
        &self,
        index: &tantivy::Index,
        default_search_fields: &[tantivy::schema::Field],
    ) -> err::Result<QueryContainer> {
        let query_parser =
            tantivy::query::QueryParser::for_index(index, default_search_fields.into());

        let query = query_parser.parse_query(self)?;

        Ok(QueryContainer::Boxed(query))
    }
}

impl AsQuery for Box<dyn tantivy::query::Query> {
    fn as_query(
        &self,
        _index: &tantivy::Index,
        _default_search_fields: &[tantivy::schema::Field],
    ) -> err::Result<QueryContainer> {
        Ok(QueryContainer::Ref(self))
    }
}

impl<'a> AsQuery for QueryContainer<'a> {
    fn as_query(
        &self,
        _index: &tantivy::Index,
        _default_search_fields: &[tantivy::schema::Field],
    ) -> err::Result<QueryContainer> {
        Ok(QueryContainer::Ref(self.as_ref()))
    }
}

impl<U> AsQuery for &U
where
    U: AsQuery + ?Sized,
{
    fn as_query(
        &self,
        index: &tantivy::Index,
        default_search_fields: &[tantivy::schema::Field],
    ) -> err::Result<QueryContainer> {
        AsQuery::as_query(*self, index, default_search_fields)
    }
}
