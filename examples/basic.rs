use serde::{Deserialize, Serialize};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Creating a temporary sled database.
    // If you want to persist the data use sled::open instead.
    let db = sled::Config::new().temporary(true).open().unwrap();

    // The id is used by sled to identify which Tree in the database (db) to open
    let tree = typed_sled::Tree::<String, SomeValue>::open(&db, "unique_id");

    tree.insert(&"some_key".to_owned(), &SomeValue(10))?;

    assert_eq!(tree.get(&"some_key".to_owned())?, Some(SomeValue(10)));
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct SomeValue(u32);
