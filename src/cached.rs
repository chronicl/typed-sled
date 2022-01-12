use crate::{Result, Tree, KV};
use lru::LruCache;
use parking_lot::RwLock;
use std::cmp::Eq;
use std::hash::Hash;
use std::sync::Arc;

pub struct CachedTree<K: Hash, V> {
    tree: Tree<K, V>,
    cache: Arc<RwLock<LruCache<K, Arc<V>>>>,
}

impl<K: Hash + Eq, V> CachedTree<K, V> {
    /// Initialize a typed tree. The id identifies the tree to be opened from the db.
    /// In contrast to `open` a lru cache that can hold up to `cache_size` key value pairs is also
    /// initialized. However the cache is only used for simple `get` operations.
    pub fn new(tree: &Tree<K, V>, cache_size: usize) -> Self {
        Self {
            tree: tree.clone(),
            cache: Arc::new(RwLock::new(LruCache::new(cache_size))),
        }
    }

    pub fn get(&self, k: &K) -> Result<Option<Arc<V>>>
    where
        K: KV + Clone,
        V: KV,
    {
        let mut cache = self.cache.write();
        if let Some(v) = cache.get(k) {
            Ok(Some(Arc::clone(v)))
        } else {
            if let Some(v) = self.tree.get(k)? {
                let v = Arc::new(v);
                cache.put(k.clone(), Arc::clone(&v));
                Ok(Some(v))
            } else {
                Ok(None)
            }
        }
    }

    /// Insert a key to a new value, returning the last value if it was set.
    /// WARNING: This method does not guarantee that the key value pair is flushed
    /// to disk before being in the cache, i.e. this method is not ACID. To ensure that
    /// the value is flushed to disk before being in the cache use `insert_flush`.
    pub fn insert(&self, k: K, v: V) -> Result<Option<V>>
    where
        K: KV,
        V: KV,
    {
        // Locking the cache so no one can read it while we modify the value in the tree.
        let mut cache = self.cache.write();
        let res = self.tree.insert(&k, &v);
        cache.put(k, Arc::new(v));
        res
    }

    /// Insert a key to a new value, returning the last value if it was set.
    /// The new key value pair is flushed to disk before being inserted into the cache
    /// to ensure that the cache never contains a key value pair that is newer than
    /// what is contained in the tree.
    pub fn insert_flush(&self, k: K, v: V) -> Result<Option<V>>
    where
        K: KV,
        V: KV,
    {
        // Locking the cache so no one can read it while we modify the value in the tree.
        let mut cache = self.cache.write();
        let res = self.tree.insert(&k, &v);
        self.tree.flush()?;
        cache.put(k, Arc::new(v));
        res
    }
}

#[test]
fn test_cached_tree() {
    use crate::Config;
    let db = Config::new().temporary(true).open().unwrap();
    let tree: Tree<u8, u8> = Tree::open(&db, "test_tree");
    let cached = CachedTree::new(&tree, 100);
    cached.insert(0, 1).unwrap();
    tree.flush().unwrap();
    assert_eq!(cached.get(&0).unwrap(), Some(Arc::new(1)));
}
