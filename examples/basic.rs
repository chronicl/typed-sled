use serde::{Deserialize, Serialize};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // If you want to persist the data use sled::open instead
    let config = sled::Config::new().temporary(true);
    let db = config.open().unwrap();

    // The id is used by sled to identify which Tree in the database (db) to open.
    let animals = typed_sled::Tree::<String, Animal>::open(&db, "unique_id");

    let larry = "Larry".to_string();
    animals.insert(&larry, &Animal::Dog)?;

    assert_eq!(animals.get(&larry)?, Some(Animal::Dog));
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum Animal {
    Dog,
    Cat,
}
