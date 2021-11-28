use rkyv::{Archive, Deserialize, Serialize};
use typed_sled::serialize::RkyvSerDe;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // If you want to persist the data use sled::open instead
    let config = sled::Config::new().temporary(true);
    let db = config.open().unwrap();

    // The id is used by sled to identify which Tree in the database (db) to open.
    let animals = typed_sled::RawTree::<String, Animal, RkyvSerDe>::open(&db, "unique_id");

    let larry = "Larry".to_string();
    animals.insert(&larry, &Animal::Dog)?;

    assert_eq!(animals.get(&larry)?.unwrap().value(), &ArchivedAnimal::Dog);

    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive, PartialEq)]
// Uncomment if you want to be able to compare ArchiveAnimal with Animal
// #[archive(compare(PartialEq))]
#[archive_attr(derive(PartialEq))]
#[archive_attr(derive(Debug))]
enum Animal {
    Dog,
    Cat,
}
