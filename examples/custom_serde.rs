use serde::{Deserialize, Serialize};
use typed_sled::custom_serde::{serialize::BincodeSerDeLazy, Tree};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Creating a temporary sled database.
    // If you want to persist the data use sled::open instead.
    let db = sled::Config::new().temporary(true).open().unwrap();

    // Notice that we are using &str, and SomeValue<'a> here which do not implement
    // serde::DeserializeOwned and thus could not be used with typed_sled::Tree.
    // However our custom lazy Deserializer contained in BincodeSerDeLazy allows us
    // to perform the deserialization lazily and only requires serde::Deserialize<'a>
    // for the lazy deserialization.
    let tree = Tree::<&str, SomeValue, BincodeSerDeLazy>::open(&db, "unique_id");

    tree.insert(&"some_key", &SomeValue("some_value"))?;

    assert_eq!(
        tree.get(&"some_key")?.unwrap().deserialize(),
        SomeValue("some_value")
    );
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct SomeValue<'a>(&'a str);
