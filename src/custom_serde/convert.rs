//! Convert one typed [Tree][crate::Tree] into another.
//! # Example
//! ```
//! pub fn main() -> Result<(), Box<dyn std::error::Error>> {
//!  let db = sled::Config::new().temporary(true).open().unwrap();
//!
//!  {
//!      let old_tree: Tree<u32, u32> = Tree::open(&db, "test_tree");
//!
//!      old_tree.insert(&1, &2)?;
//!      old_tree.insert(&3, &4)?;
//!      old_tree.flush()?;
//!  }
//!
//!  convert::<u32, u32, u64, u64>(&db, "test_tree");
//!  let tree: Tree<u64, u64> = Tree::open(&db, "test_tree");
//!  assert_eq!(tree.get(&1)?.unwrap(), 2);
//!  assert_eq!(tree.get(&3)?.unwrap(), 4);
//! Ok(()) }
//! ```
use crate::custom_serde::serialize::{Key, Value};
use crate::custom_serde::Tree;
use std::convert::Into;

/// Convert `Tree<KOld, VOld, SerDeOld>` to `Tree<KNew, VNew, SerDeNew>`
pub fn convert<KOld, VOld, KNew, VNew, SerDeOld, SerDeNew>(db: &sled::Db, tree: &str)
where
    Key<KOld, VOld, SerDeOld>: Into<KNew>,
    Value<KOld, VOld, SerDeOld>: Into<VNew>,
    SerDeOld: crate::custom_serde::serialize::SerDe<KOld, VOld>,
    SerDeNew: crate::custom_serde::serialize::SerDe<KNew, VNew>,
{
    let mut kvs = Vec::new();

    {
        let tree: Tree<KOld, VOld, SerDeOld> = Tree::open(db, tree);

        for kv_pair in tree.iter() {
            kvs.push(kv_pair.unwrap());
        }
    }

    db.drop_tree(tree).unwrap();
    let tree: Tree<KNew, VNew, SerDeNew> = Tree::open(db, tree);

    for kv_pair in kvs.drain(..) {
        tree.insert(&kv_pair.0.into(), &kv_pair.1.into()).unwrap();
    }
}

#[test]
fn test_convert() {
    use super::serialize::BincodeSerDe;
    let config = sled::Config::new().temporary(true);
    let db = config.open().unwrap();

    {
        let old_tree: Tree<u32, u32, BincodeSerDe> = Tree::open(&db, "test_tree");

        old_tree.insert(&1, &2).unwrap();
        old_tree.insert(&3, &4).unwrap();
        old_tree.flush().unwrap();
    }

    convert::<u32, u32, u64, u64, BincodeSerDe, BincodeSerDe>(&db, "test_tree");
    let tree: Tree<u64, u64, BincodeSerDe> = Tree::open(&db, "test_tree");
    assert_eq!(tree.get(&1).unwrap().unwrap(), 2);
    assert_eq!(tree.get(&3).unwrap().unwrap(), 4);
}
