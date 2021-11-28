// use crate::{Batch, RawTree, KV};
// use sled::transaction::{ConflictableTransactionResult, TransactionResult};
// use sled::Result;
// use std::ops::Deref;
// use std::sync::atomic::{AtomicU64, Ordering};

// /// Wraps a type that implements KeyGenerating and uses it to
// /// generate the keys for a typed_sled::Tree.
// ///
// /// See CounterTree for a specific example of how to use this type.
// #[derive(Clone, Debug)]
// pub struct RawKeyGeneratingTree<KG: KeyGenerating, V, SerDe> {
//     key_generator: KG,
//     inner: RawTree<KG::Key, V, SerDe>,
// }

// impl<'de, KG: KeyGenerating, V: KV, SerDe: crate::serialize::SerDe<'de, KG::Key, V>>
//     RawKeyGeneratingTree<KG, V, SerDe>
// {
//     pub fn open<T: AsRef<str>>(db: &sled::Db, id: T) -> Self {
//         let tree = RawTree::open(db, id);
//         let key_generator = KG::initialize(&tree);

//         Self {
//             key_generator,
//             inner: tree,
//         }
//     }

//     /// Insert a generated key to a new value, returning the key and the last value if it was set.
//     pub fn insert(&self, value: &V) -> Result<(KG::Key, Option<V>)> {
//         let key = self.key_generator.next_key();
//         let res = self.inner.insert(&key, value);
//         res.map(|opt_v| (key, opt_v))
//     }

//     /// Insert a key to a new value, returning the last value if it was set.
//     /// Be careful not to insert a key that conflicts with the keys generated
//     /// by the key generator. If you need the generated key for construction of
//     /// the value, you can first use `next_key` and then use this method with
//     /// the generated key. Alternatively use `insert_fn`.
//     pub fn insert_with_key(&self, key: &KG::Key, value: &V) -> Result<Option<V>> {
//         self.inner.insert(&key, value)
//     }

//     /// Generate the next key using the key generator.
//     pub fn next_key(&self) -> KG::Key {
//         self.key_generator.next_key()
//     }

//     /// Insert a generated key to a new dynamically created value, returning the key and the last value if it was set.
//     /// The argument supplied to `f` is a reference to the key and the returned value is the value that will
//     /// be inserted at the key.
//     pub fn insert_fn(&self, f: impl Fn(&KG::Key) -> V) -> Result<(KG::Key, Option<V>)> {
//         let key = self.key_generator.next_key();
//         let value = f(&key);
//         let res = self.insert_with_key(&key, &value);
//         res.map(|opt_v| (key, opt_v))
//     }

//     pub fn transaction<F, A, E>(&self, f: F) -> TransactionResult<A, E>
//     where
//         F: Fn(&KeyGeneratingTransactionalTree<KG, V, SerDe>) -> ConflictableTransactionResult<A, E>,
//     {
//         self.inner.transaction(|transactional_tree| {
//             f(&KeyGeneratingTransactionalTree {
//                 key_generator: self.key_generator(),
//                 inner: transactional_tree,
//             })
//         })
//     }

//     pub fn key_generator(&self) -> &KG {
//         &self.key_generator
//     }

//     pub fn new_batch(&self) -> KeyGeneratingBatch<KG, V, SerDe> {
//         KeyGeneratingBatch {
//             key_generator: self.key_generator(),
//             inner: Batch::default(),
//         }
//     }

//     pub fn apply_batch(&self, batch: KeyGeneratingBatch<KG, V, SerDe>) -> Result<()> {
//         self.inner.apply_batch(batch.inner)
//     }
// }

// impl<'de, KG: KeyGenerating, V: KV, SerDe: crate::serialize::SerDe<'de, KG::Key, V>> Deref
//     for RawKeyGeneratingTree<KG, V, SerDe>
// {
//     type Target = RawTree<KG::Key, V, SerDe>;

//     fn deref(&self) -> &Self::Target {
//         &self.inner
//     }
// }

// /// Implement on a type that you want to use for key generation
// /// for a typed_sled::Tree.
// ///
// /// See CounterTree for a specific example of how to use this trait.
// pub trait KeyGenerating {
//     type Key: KV;

//     fn initialize<'de, V: KV, SerDe: crate::serialize::SerDe<'de, Self::Key, V>>(
//         tree: &RawTree<Self::Key, V, SerDe>,
//     ) -> Self;

//     fn next_key(&self) -> Self::Key;
// }

// #[derive(Clone, Debug)]
// pub struct KeyGeneratingBatch<'a, KG: KeyGenerating, V, SerDe> {
//     key_generator: &'a KG,
//     inner: Batch<KG::Key, V, SerDe>,
// }

// impl<
//         'a,
//         'de,
//         KG: KeyGenerating<Key = K>,
//         K: KV,
//         V: KV,
//         SerDe: crate::serialize::SerDe<'de, K, V>,
//     > KeyGeneratingBatch<'a, KG, V, SerDe>
// {
//     pub fn insert(&mut self, value: &V) {
//         self.inner.insert(&self.key_generator.next_key(), value);
//     }

//     pub fn remove(&mut self, key: &K) {
//         self.inner.remove(key)
//     }
// }

// /// A typed_sled::Tree with automatically generated and continuously increasing u64 keys.
// pub type CounterTree<V, SerDe> = RawKeyGeneratingTree<Counter, V, SerDe>;

// #[derive(Debug)]
// pub struct Counter(AtomicU64);

// impl KeyGenerating for Counter {
//     type Key = u64;

//     fn initialize<Value: KV, SerDe>(tree: &RawTree<Self::Key, Value, SerDe>) -> Self {
//         if let Some((key, _)) = tree
//             .last()
//             .expect("KeyGenerating Counter failed to access sled Tree.")
//         {
//             Counter(AtomicU64::new(key + 1))
//         } else {
//             Counter(AtomicU64::new(0))
//         }
//     }

//     fn next_key(&self) -> Self::Key {
//         self.0.fetch_add(1, Ordering::Relaxed)
//     }
// }

// pub struct KeyGeneratingTransactionalTree<'a, KG: KeyGenerating, V, SerDe> {
//     key_generator: &'a KG,
//     inner: &'a crate::TransactionalTree<'a, KG::Key, V, SerDe>,
// }

// impl<'a, 'de, KG: KeyGenerating, V: KV, SerDe: crate::serialize::SerDe<'de, KG::Key, V>>
//     KeyGeneratingTransactionalTree<'a, KG, V, SerDe>
// {
//     pub fn insert(
//         &self,
//         value: &V,
//     ) -> std::result::Result<Option<V>, sled::transaction::UnabortableTransactionError> {
//         self.inner.insert(&self.key_generator.next_key(), value)
//     }

//     pub fn apply_batch(
//         &self,
//         batch: &KeyGeneratingBatch<KG, V, SerDe>,
//     ) -> std::result::Result<(), sled::transaction::UnabortableTransactionError> {
//         self.inner.apply_batch(&batch.inner)
//     }
// }

// impl<'a, 'de, KG: KeyGenerating, V, SerDe: crate::serialize::SerDe<'de, KG::Key, V>> Deref
//     for KeyGeneratingTransactionalTree<'a, KG, V, SerDe>
// {
//     type Target = crate::TransactionalTree<'a, KG::Key, V, SerDe>;

//     fn deref(&self) -> &Self::Target {
//         &self.inner
//     }
// }
