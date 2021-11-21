use crate::{Batch, Tree};
use serde::{de::DeserializeOwned, Serialize};
use sled::Result;
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};

/// Wraps a type that implements KeyGenerating and uses it to
/// generate the keys for a typed_sled::Tree.
///
/// See CounterTree for a specific example of how to use this type.
#[derive(Clone, Debug)]
pub struct KeyGeneratingTree<KeyGenerator: KeyGenerating, V> {
    key_generator: KeyGenerator,
    inner: Tree<KeyGenerator::Key, V>,
}

impl<KeyGenerator: KeyGenerating, V: DeserializeOwned + Serialize + Send + Sync>
    KeyGeneratingTree<KeyGenerator, V>
{
    pub fn open<T: AsRef<str>>(db: &sled::Db, id: T) -> Self {
        let tree = Tree::open(db, id);
        let key_generator = KeyGenerator::initialize(&tree);

        Self {
            key_generator,
            inner: tree,
        }
    }

    /// Insert a generated key to a new value, returning the key and the last value if it was set.
    pub fn insert(&self, value: &V) -> Result<(KeyGenerator::Key, Option<V>)> {
        let key = self.key_generator.next_key();
        let res = self.inner.insert(&key, value);
        res.map(|opt_v| (key, opt_v))
    }

    pub fn key_generator(&self) -> &KeyGenerator {
        &self.key_generator
    }

    pub fn new_batch(&self) -> KeyGeneratingBatch<KeyGenerator, V> {
        KeyGeneratingBatch {
            key_generator: self.key_generator(),
            inner: Batch::default(),
        }
    }

    pub fn apply_batch(&self, batch: KeyGeneratingBatch<KeyGenerator, V>) -> Result<()> {
        self.inner.apply_batch(batch.inner)
    }
}

impl<KeyGenerator: KeyGenerating, V: DeserializeOwned + Serialize + Send + Sync> Deref
    for KeyGeneratingTree<KeyGenerator, V>
{
    type Target = Tree<KeyGenerator::Key, V>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Implement on a type that you want to use for key generation
/// for a typed_sled::Tree.
///
/// See CounterTree for a specific example of how to use this trait.
pub trait KeyGenerating {
    type Key: DeserializeOwned + Serialize + Send + Sync;

    fn initialize<V: DeserializeOwned + Serialize + Send + Sync>(tree: &Tree<Self::Key, V>)
        -> Self;

    fn next_key(&self) -> Self::Key;
}

#[derive(Clone, Debug)]
pub struct KeyGeneratingBatch<'a, KeyGenerator: KeyGenerating, V> {
    key_generator: &'a KeyGenerator,
    inner: Batch<KeyGenerator::Key, V>,
}

impl<
        'a,
        KeyGenerator: KeyGenerating<Key = K>,
        K: DeserializeOwned + Serialize + Send + Sync,
        V: DeserializeOwned + Serialize + Send + Sync,
    > KeyGeneratingBatch<'a, KeyGenerator, V>
{
    pub fn insert(&mut self, value: &V) {
        self.inner.insert(&self.key_generator.next_key(), value);
    }

    pub fn remove(&mut self, key: &K) {
        self.inner.remove(key)
    }
}

/// A typed_sled::Tree with automatically generated and continuously increasing u64 keys.
pub type CounterTree<V> = KeyGeneratingTree<Counter, V>;

#[derive(Debug)]
pub struct Counter(AtomicU64);

impl KeyGenerating for Counter {
    type Key = u64;

    fn initialize<Value: DeserializeOwned + Serialize + Send + Sync>(
        tree: &Tree<Self::Key, Value>,
    ) -> Self {
        if let Some((key, _)) = tree
            .last()
            .expect("KeyGenerating Counter failed to access sled Tree.")
        {
            Counter(AtomicU64::new(key + 1))
        } else {
            Counter(AtomicU64::new(0))
        }
    }

    fn next_key(&self) -> Self::Key {
        self.0.fetch_add(1, Ordering::Relaxed)
    }
}
