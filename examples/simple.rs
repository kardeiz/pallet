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

    let temp_dir_path = temp_dir.path();

    let db = sled::open(temp_dir_path.join("db"))?;

    let store = pallet::Store::builder().with_db(db).with_index_dir(temp_dir_path).finish()?;

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
        Book {
            title: "Cabin in the Woods".into(),
            description: None,
            rating: 9,
        },
    ];

    let _ = store.create_multi(&books)?;

    let books = store.search("man AND rating:>8")?;

    println!("{:?}", books);

    Ok(())
}
