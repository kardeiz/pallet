use crate::err;
use std::ops::Deref;

/// Wrapper for `sled::Tree` and its `sled::Db` (included for `id` generation)
pub struct Tree {
    inner: sled::Tree,
}

impl Deref for Tree {
    type Target = sled::Tree;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Tree {
    /// Create a new builder
    pub fn builder() -> TreeBuilder {
        TreeBuilder::default()
    }
}

/// Builder for `Tree`
#[derive(Default)]
pub struct TreeBuilder {
    tree_name: Option<String>,
    db: Option<sled::Db>,
}

impl TreeBuilder {
    pub(crate) fn merge(self, other: Self) -> Self {
        let TreeBuilder { tree_name: a1, db: a2 } = self;
        let TreeBuilder { tree_name: b1, db: b2 } = other;

        TreeBuilder { tree_name: a1.or(b1), db: a2.or(b2) }
    }

    /// Set the name for this `Tree`
    pub fn with_tree_name<I: Into<String>>(mut self, tree_name: I) -> Self {
        self.tree_name = Some(tree_name.into());
        self
    }

    /// Set the `sled::Db` for this `Tree`
    pub fn with_db(mut self, db: sled::Db) -> Self {
        self.db = Some(db);
        self
    }

    /// Convert into finished `Tree`
    pub fn finish(self) -> err::Result<Tree> {
        let db = self.db.ok_or_else(|| err::custom("`db` not set"))?;
        let tree_name = self.tree_name.ok_or_else(|| err::custom("`tree_name` not set"))?;

        let inner = db.open_tree(tree_name.as_bytes())?;

        Ok(Tree { inner })
    }
}
