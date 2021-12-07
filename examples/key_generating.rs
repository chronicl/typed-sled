use typed_sled::key_generating::CounterTree;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Creating a temporary sled database.
    // If you want to persist the data use sled::open instead.
    let db = sled::Config::new().temporary(true).open().unwrap();

    // The id is used by sled to identify which Tree in the database (db) to open.
    let tree = CounterTree::open(&db, "unique_id");

    let (first_key, _) = tree.insert(&5)?;
    let (second_key, _) = tree.insert(&6)?;

    assert_eq!(first_key, 0);
    assert_eq!(second_key, 1);
    assert_eq!(tree.get(&0)?, Some(5));
    assert_eq!(tree.get(&1)?, Some(6));
    Ok(())
}
