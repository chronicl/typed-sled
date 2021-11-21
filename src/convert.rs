use crate::Tree;
use serde::{de::DeserializeOwned, Serialize};
use std::convert::Into;

/// Convert one tree into another. The types of the old key and value need
/// to implement Into for the new key and value types respectively.
/// Right now this function is storing the entire old tree in memory
/// so if the old tree contains a large amount of data, the conversion
/// will not be successful. It's still a ToDo to fix this.
pub fn convert<KOld, VOld, KNew, VNew>(db: &sled::Db, tree: &str)
where
    KOld: Into<KNew>,
    VOld: Into<VNew>,
    KOld: DeserializeOwned + Serialize + Clone + Send + Sync,
    VOld: DeserializeOwned + Serialize + Clone + Send + Sync,
    KNew: DeserializeOwned + Serialize + Clone + Send + Sync,
    VNew: DeserializeOwned + Serialize + Clone + Send + Sync,
{
    let mut kvs = Vec::new();

    {
        let tree: Tree<KOld, VOld> = Tree::open(&db, tree);

        for kv_pair in tree.iter() {
            kvs.push(kv_pair.unwrap().to_owned());
        }
    }

    db.drop_tree(tree).unwrap();
    let tree: Tree<KNew, VNew> = Tree::open(&db, tree);

    for kv_pair in kvs.drain(..) {
        tree.insert(&kv_pair.0.into(), &kv_pair.1.into()).unwrap();
    }
}

#[test]
fn test_convert() {
    let config = sled::Config::new().temporary(true);
    let db = config.open().unwrap();

    {
        let old_tree: Tree<u32, u32> = Tree::open(&db, "test_tree");

        old_tree.insert(&1, &2).unwrap();
        old_tree.insert(&3, &4).unwrap();
        old_tree.flush().unwrap();
    }

    convert::<u32, u32, u64, u64>(&db, "test_tree");
    let tree: Tree<u64, u64> = Tree::open(&db, "test_tree");
    assert_eq!(tree.get(&1).unwrap().unwrap(), 2);
    assert_eq!(tree.get(&3).unwrap().unwrap(), 4);
}
