
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct Foo {
    inner: String,
}

impl pallet::DocumentLike for Foo {

    fn tree_name() -> Option<String> {
        Some("foos".into())
    }

    fn index_fields(schema_builder: &mut tantivy::schema::SchemaBuilder) -> pallet::err::Result<Vec<tantivy::schema::Field>> {
        Ok(vec![schema_builder.add_text_field("inner", tantivy::schema::TEXT)])
    }

    fn as_search_document(&self, fields: &[tantivy::schema::Field]) -> pallet::err::Result<tantivy::Document> {
        let mut doc = tantivy::Document::new();
        doc.add_text(fields[0], &self.inner);

        Ok(doc)
    }
}


fn main() -> Result<(), Box<dyn std::error::Error>> {

    let temp_dir = tempfile::TempDir::new_in(".")?;

    let db = sled::open("db")?;
    let db = std::sync::Arc::new(db);

    // println!("{:?}", "WHAT1");

    let store = pallet::Store::<Foo>::builder()
        .with_db(db)
        .with_index_dir(temp_dir.path())
        // .with_index_dir("idx")
        .finish()?;

    // store.index_all()?;


    // println!("{:?}", &store.all());

    let foo = Foo { inner: "Jacob Brown".into() };
    let foo2 = Foo { inner: "Jacob Hohmann".into() };
    let foo3 = Foo { inner: "John Hohmann".into() };

    let _ = store.create_multi(&[&foo, &foo2, &foo3])?;

    let foos = store.search("Jacob Brown")?;

    println!("{:?}", foos);

    Ok(())
}