use std::marker::PhantomData;

use sled::transaction::{ConflictableTransactionResult, TransactionResult};

use crate::{deserialize, serialize, Batch, Tree, KV};

pub struct TransactionalTree<'a, K, V> {
    inner: &'a sled::transaction::TransactionalTree,
    _key: PhantomData<fn() -> K>,
    _value: PhantomData<fn() -> V>,
}

impl<'a, K, V> TransactionalTree<'a, K, V> {
    pub(crate) fn new(sled: &'a sled::transaction::TransactionalTree) -> Self {
        Self {
            inner: sled,
            _key: PhantomData,
            _value: PhantomData,
        }
    }

    pub fn insert(
        &self,
        key: &K,
        value: &V,
    ) -> std::result::Result<Option<V>, sled::transaction::UnabortableTransactionError>
    where
        K: KV,
        V: KV,
    {
        self.inner
            .insert(serialize(key), serialize(value))
            .map(|opt| opt.map(|v| deserialize(&v)))
    }

    pub fn remove(
        &self,
        key: &K,
    ) -> std::result::Result<Option<V>, sled::transaction::UnabortableTransactionError>
    where
        K: KV,
        V: KV,
    {
        self.inner
            .remove(serialize(key))
            .map(|opt| opt.map(|v| deserialize(&v)))
    }

    pub fn get(
        &self,
        key: &K,
    ) -> std::result::Result<Option<V>, sled::transaction::UnabortableTransactionError>
    where
        K: KV,
        V: KV,
    {
        self.inner
            .get(serialize(key))
            .map(|opt| opt.map(|v| deserialize(&v)))
    }

    pub fn apply_batch(
        &self,
        batch: &Batch<K, V>,
    ) -> std::result::Result<(), sled::transaction::UnabortableTransactionError> {
        self.inner.apply_batch(&batch.inner)
    }

    pub fn flush(&self) {
        self.inner.flush()
    }

    pub fn generate_id(&self) -> sled::Result<u64> {
        self.inner.generate_id()
    }
}

pub trait Transactional<E = ()> {
    type View<'a>;

    fn transaction<F, A>(&self, f: F) -> TransactionResult<A, E>
    where
        F: for<'a> Fn(Self::View<'a>) -> ConflictableTransactionResult<A, E>;
}

macro_rules! impl_transactional {
  ($($k:ident, $v:ident, $i:tt),+) => {
      impl<E, $($k, $v),+> Transactional<E> for ($(&Tree<$k, $v>),+) {
          type View<'a> = (
              $(TransactionalTree<'a, $k, $v>),+
          );

          fn transaction<F, A>(&self, f: F) -> TransactionResult<A, E>
          where
              F: for<'a> Fn(Self::View<'a>) -> ConflictableTransactionResult<A, E>,
          {
              use sled::Transactional;

              ($(&self.$i.inner),+).transaction(|trees| {
                  f((
                      $(TransactionalTree::new(&trees.$i)),+
                  ))
              })
          }
      }
  };
}

impl_transactional!(K0, V0, 0, K1, V1, 1);
impl_transactional!(K0, V0, 0, K1, V1, 1, K2, V2, 2);
impl_transactional!(K0, V0, 0, K1, V1, 1, K2, V2, 2, K3, V3, 3);
impl_transactional!(K0, V0, 0, K1, V1, 1, K2, V2, 2, K3, V3, 3, K4, V4, 4);
impl_transactional!(K0, V0, 0, K1, V1, 1, K2, V2, 2, K3, V3, 3, K4, V4, 4, K5, V5, 5);
impl_transactional!(K0, V0, 0, K1, V1, 1, K2, V2, 2, K3, V3, 3, K4, V4, 4, K5, V5, 5, K6, V6, 6);
impl_transactional!(
    K0, V0, 0, K1, V1, 1, K2, V2, 2, K3, V3, 3, K4, V4, 4, K5, V5, 5, K6, V6, 6, K7, V7, 7
);
impl_transactional!(
    K0, V0, 0, K1, V1, 1, K2, V2, 2, K3, V3, 3, K4, V4, 4, K5, V5, 5, K6, V6, 6, K7, V7, 7, K8, V8,
    8
);
impl_transactional!(
    K0, V0, 0, K1, V1, 1, K2, V2, 2, K3, V3, 3, K4, V4, 4, K5, V5, 5, K6, V6, 6, K7, V7, 7, K8, V8,
    8, K9, V9, 9
);
impl_transactional!(
    K0, V0, 0, K1, V1, 1, K2, V2, 2, K3, V3, 3, K4, V4, 4, K5, V5, 5, K6, V6, 6, K7, V7, 7, K8, V8,
    8, K9, V9, 9, K10, V10, 10
);

#[test]
fn test_multiple_tree_transaction() {
    let db = sled::Config::new().temporary(true).open().unwrap();
    let tree0 = Tree::<u32, i32>::open(&db, "tree0");
    let tree1 = Tree::<u16, i16>::open(&db, "tree1");
    let tree2 = Tree::<u8, i8>::open(&db, "tree2");

    (&tree0, &tree1, &tree2)
        .transaction(|trees| {
            trees.0.insert(&0, &0)?;
            trees.1.insert(&0, &0)?;
            trees.2.insert(&0, &0)?;
            // Todo: E in ConflitableTransactionResult<A, E> is not inferred
            // automatically, although Transactional<E = ()> has default E = () type.
            Ok::<(), sled::transaction::ConflictableTransactionError<()>>(())
        })
        .unwrap();

    assert_eq!(tree0.get(&0), Ok(Some(0)));
    assert_eq!(tree1.get(&0), Ok(Some(0)));
}
