#![feature(generic_associated_types)]

//! Sled with Types instead of Bytes.
//! This crate builds on top of `sled` and handles the (de)serialization
//! of keys and values that are inserted into a sleSerDe::Tree for you.
//! It also includes a convert feature, which allows you to convert any Tree
//! into another Tree with different key and value types.
//! It hasn't been tested extensively.
//!
//! Some info about sled:
//! `sled` is a high-performance embedded database with
//! an API that is similar to a `BTreeMap<[u8], [u8]>`,
//! but with several additional capabilities for
//! assisting creators of stateful systems. It is fully thread-safe,
//! and all operations are atomic.
//!
//! # Example
use crate::serialize::{Deserializer, Serializer};
use bincode::deserialize;
/// ```
/// # #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
/// # enum Animal {
/// #     Dog,
/// #    Cat,
/// # }
///
/// # pub fn main() -> Result<(), Box<dyn stSerDe::error::Error>> {
/// let db: sleSerDe::Db = sleSerDe::open("db")?;
///
/// // The id is used by sled to identify which Tree in the database (db) to open.
/// let animals = typed_sleSerDe::Tree::<String, Animal>::open(&db, "unique_id");
///
/// let larry = "Larry".to_string();
/// animals.insert(&larry, &Animal::Dog)?;
/// assert_eq!(animals.get(&larry)?, Some(Animal::Dog));
/// # Ok(()) }
/// ```
use core::fmt;
use core::iter::{DoubleEndedIterator, Iterator};
use core::ops::{Bound, RangeBounds};
use serde::Deserialize;
use sled::{
    transaction::{ConflictableTransactionResult, TransactionResult},
    IVec, Result,
};
use std::marker::PhantomData;

pub use sled;

#[cfg(feature = "convert")]
pub mod convert;

#[cfg(feature = "key-generating")]
pub mod key_generating;

pub mod serialize;
use serialize::{RawValue, Value};

// pub trait Bin = DeserializeOwned + Serialize + Clone + Send + Sync;

/// A flash-sympathetic persistent lock-free B+ tree.
///
/// A `Tree` represents a single logical keyspacFe / namespace / bucket.
///
/// # Example
///
/// ```
/// # use serde::{Serialize, Deserialize};
/// # #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
/// # enum Animal {
/// #     Dog,
/// #    Cat,
/// # }
///
/// # pub fn main() -> Result<(), Box<dyn stSerDe::error::Error>> {
/// let db: sleSerDe::Db = sleSerDe::open("db")?;
/// let animals = typed_sleSerDe::Tree::<String, Animal>::open(&db, "animals");
/// animals.insert(&"Larry".to_string(), &Animal::Dog);
/// # Ok(()) }
/// ```
#[derive(Clone, Debug)]
pub struct RawTree<K, V, SerDe> {
    inner: sled::Tree,
    key: PhantomData<K>,
    value: PhantomData<V>,
    serde: PhantomData<SerDe>,
}

pub type Tree<K, V> = RawTree<K, V, serialize::BincodeSerDe>;

// type Tree<K, V> = RawTree<
//     K,
//     V,
//     serialize::SerDe<
//     serialize::BincodeSerializer,
//     serialize::BincodeSerializer,
//     serialize::BincodeDeserializer,
//     serialize::BincodeDeserializer,
// >;

/// Trait alias for bounds required on keys and values.
pub trait KV: Send + Sync {}

impl<T: Send + Sync> KV for T {}

/// Compare and swap error.
#[derive(Clone, Debug)]
pub struct CompareAndSwapError<V, DV> {
    /// The current value which caused your CAS to fail.
    pub current: Option<Value<V, DV>>,
    /// Returned value that was proposed unsuccessfully.
    pub proposed: Option<Value<V, DV>>,
}

impl<V, DV> fmt::Display for CompareAndSwapError<V, DV> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Compare and swap conflict")
    }
}

// implemented like this in the sled source
impl<V: std::fmt::Debug, DV: std::fmt::Debug> std::error::Error for CompareAndSwapError<V, DV> {}

impl<'de, K: KV, V: KV, SerDe: serialize::SerDe<'de, K, V> + Send + Sync> RawTree<K, V, SerDe> {
    /// Initialize a typed tree. The id identifies the tree to be opened from the db.
    /// # Example
    ///
    /// ```
    /// # use serde::{Serialize, Deserialize};
    /// # #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    /// # enum Animal {
    /// #     Dog,
    /// #    Cat,
    /// # }
    ///
    /// # pub fn main() -> Result<(), Box<dyn stSerDe::error::Error>> {
    /// let db: sleSerDe::Db = sleSerDe::open("db")?;
    /// let animals = typed_sleSerDe::Tree::<String, Animal>::open(&db, "animals");
    /// animals.insert(&"Larry".to_string(), &Animal::Dog)?;
    /// # Ok(()) }
    /// ```
    pub fn open<T: AsRef<str>>(db: &sled::Db, id: T) -> Self {
        Self {
            inner: db.open_tree(id.as_ref()).unwrap(),
            key: PhantomData,
            value: PhantomData,
            serde: PhantomData,
        }
    }

    /// Insert a key to a new value, returning the last value if it was set.
    pub fn insert(&self, key: &K, value: &V) -> Result<Option<Value<V, SerDe::DV>>> {
        self.inner
            .insert(
                SerDe::SK::serialize(key).as_ref(),
                SerDe::SV::serialize(value).as_ref(),
            )
            .map(|opt| opt.map(|old_value| Value::new(old_value)))
    }

    /// Perform a multi-key serializable transaction.
    pub fn transaction<F, A, E>(&self, f: F) -> TransactionResult<A, E>
    where
        F: Fn(&TransactionalTree<K, V, SerDe>) -> ConflictableTransactionResult<A, E>,
    {
        self.inner.transaction(|sled_transactional_tree| {
            f(&TransactionalTree {
                inner: sled_transactional_tree,
                phantom_key: PhantomData,
                phantom_value: PhantomData,
                serde: PhantomData,
            })
        })
    }

    /// Create a new batched update that can be atomically applied.
    ///
    /// It is possible to apply a Batch in a transaction as well, which is the way you can apply a Batch to multiple Trees atomically.
    pub fn apply_batch(&self, batch: Batch<K, V, SerDe>) -> Result<()> {
        self.inner.apply_batch(batch.inner)
    }

    /// Retrieve a value from the Tree if it exists.
    pub fn get(&self, key: &K) -> Result<Option<Value<V, SerDe::DV>>> {
        self.inner
            .get(SerDe::SK::serialize(key).as_ref())
            .map(|opt| opt.map(|v| Value::new(v)))
    }

    /// Delete a value, returning the old value if it existed.
    pub fn remove(&self, key: &K) -> Result<Option<Value<V, SerDe::DV>>> {
        self.inner
            .remove(SerDe::SK::serialize(key))
            .map(|opt| opt.map(|v| Value::new(v)))
    }

    /// Compare and swap. Capable of unique creation, conditional modification, or deletion. If old is None, this will only set the value if it doesn't exist yet. If new is None, will delete the value if old is correct. If both old and new are Some, will modify the value if old is correct.
    ///
    /// It returns Ok(Ok(())) if operation finishes successfully.
    ///
    /// If it fails it returns: - Ok(Err(CompareAndSwapError(current, proposed))) if operation failed to setup a new value. CompareAndSwapError contains current and proposed values. - Err(Error::Unsupported) if the database is opened in read-only mode.
    pub fn compare_and_swap(
        &self,
        key: &K,
        old: Option<&V>,
        new: Option<&V>,
    ) -> Result<core::result::Result<(), CompareAndSwapError<V, SerDe::DV>>> {
        if let Some(new) = new {
            self.inner
                .compare_and_swap(
                    SerDe::SK::serialize(key),
                    old.map(|old| SerDe::SV::serialize(old)),
                    Some(SerDe::SV::serialize(new).as_ref()),
                )
                .map(|cas_res| {
                    cas_res.map_err(|cas_err| CompareAndSwapError {
                        current: cas_err.current.map(|b| Value::new(b)),
                        proposed: cas_err.proposed.map(|b| Value::new(b)),
                    })
                })
        } else {
            self.inner
                .compare_and_swap::<_, _, &[u8]>(
                    SerDe::SK::serialize(key),
                    old.map(|old| SerDe::SV::serialize(old)),
                    None,
                )
                .map(|cas_res| {
                    cas_res.map_err(|cas_err| CompareAndSwapError {
                        current: cas_err.current.map(|b| Value::new(b)),
                        proposed: cas_err.proposed.map(|b| Value::new(b)),
                    })
                })
        }
    }

    /// Fetch the value, apply a function to it and return the result.
    // not sure if implemented correctly (different trait bound for F)
    pub fn update_and_fetch<F>(&self, key: &K, mut f: F) -> Result<Option<Value<V, SerDe::DV>>>
    where
        F: FnMut(Option<RawValue<V, &[u8], SerDe::DV>>) -> Option<V>,
    {
        self.inner
            .update_and_fetch(SerDe::SK::serialize(key), |opt_value| {
                f(opt_value.map(|v| RawValue::new(v))).map(|v| {
                    SerDe::SV::serialize(&v)
                        .as_ref()
                        .iter()
                        .map(|v| *v)
                        .collect::<Vec<u8>>()
                })
            })
            .map(|res| res.map(|v| Value::new(v)))
    }

    /// Fetch the value, apply a function to it and return the previous value.
    // not sure if implemented correctly (different trait bound for F)
    pub fn fetch_and_update<F>(&self, key: &K, mut f: F) -> Result<Option<Value<V, SerDe::DV>>>
    where
        F: FnMut(Option<RawValue<V, &[u8], SerDe::DV>>) -> Option<V>,
    {
        self.inner
            .fetch_and_update(SerDe::SK::serialize(key), |opt_value| {
                f(opt_value.map(|v| RawValue::new(v))).map(|v| {
                    SerDe::SV::serialize(&v)
                        .as_ref()
                        .iter()
                        .map(|v| *v)
                        .collect::<Vec<u8>>()
                })
            })
            .map(|res| res.map(|v| Value::new(v)))
    }

    /// Subscribe to `Event`s that happen to keys that have
    /// the specified prefix. Events for particular keys are
    /// guaranteed to be witnessed in the same order by all
    /// threads, but threads may witness different interleavings
    /// of `Event`s across different keys. If subscribers don't
    /// keep up with new writes, they will cause new writes
    /// to block. There is a buffer of 1024 items per
    /// `Subscriber`. This can be used to build reactive
    /// and replicated systems.
    pub fn watch_prefix(&self, prefix: &V) -> Subscriber<'de, K, V, SerDe> {
        Subscriber::from_sled(
            self.inner
                .watch_prefix(SerDe::SV::serialize(prefix).as_ref()),
        )
    }

    /// Synchronously flushes all dirty IO buffers and calls
    /// fsync. If this succeeds, it is guaranteed that all
    /// previous writes will be recovered if the system
    /// crashes. Returns the number of bytes flushed during
    /// this call.
    ///
    /// Flushing can take quite a lot of time, and you should
    /// measure the performance impact of using it on
    /// realistic sustained workloads running on realistic
    /// hardware.
    pub fn flush(&self) -> Result<usize> {
        self.inner.flush()
    }

    /// Asynchronously flushes all dirty IO buffers
    /// and calls fsync. If this succeeds, it is
    /// guaranteed that all previous writes will
    /// be recovered if the system crashes. Returns
    /// the number of bytes flushed during this call.
    ///
    /// Flushing can take quite a lot of time, and you
    /// should measure the performance impact of
    /// using it on realistic sustained workloads
    /// running on realistic hardware.
    pub async fn flush_async(&self) -> Result<usize> {
        self.inner.flush_async().await
    }

    /// Returns `true` if the `Tree` contains a value for
    /// the specified key.
    pub fn contains_key(&self, key: &K) -> Result<bool> {
        self.inner.contains_key(SerDe::SK::serialize(key).as_ref())
    }

    /// Retrieve the key and value before the provided key,
    /// if one exists.
    pub fn get_lt(&self, key: &K) -> Result<Option<(Value<K, SerDe::DK>, Value<V, SerDe::DV>)>> {
        self.inner
            .get_lt(SerDe::SK::serialize(key))
            .map(|res| res.map(|(k, v)| (Value::new(k), Value::new(v))))
    }

    /// Retrieve the next key and value from the `Tree` after the
    /// provided key.
    pub fn get_gt(&self, key: &K) -> Result<Option<(Value<K, SerDe::DK>, Value<V, SerDe::DV>)>> {
        self.inner
            .get_gt(SerDe::SK::serialize(key))
            .map(|res| res.map(|(k, v)| (Value::new(k), Value::new(v))))
    }

    /// Merge state directly into a given key's value using the
    /// configured merge operator. This allows state to be written
    /// into a value directly, without any read-modify-write steps.
    /// Merge operators can be used to implement arbitrary data
    /// structures.
    ///
    /// Calling `merge` will return an `Unsupported` error if it
    /// is called without first setting a merge operator function.
    ///
    /// Merge operators are shared by all instances of a particular
    /// `Tree`. Different merge operators may be set on different
    /// `Tree`s.
    pub fn merge(&self, key: &K, value: &V) -> Result<Option<Value<V, SerDe::DV>>> {
        self.inner
            .merge(SerDe::SK::serialize(key), SerDe::SV::serialize(value))
            .map(|res| res.map(|old_v| Value::new(old_v)))
    }

    // TODO: implement using own MergeOperator trait
    /// Sets a merge operator for use with the `merge` function.
    ///
    /// Merge state directly into a given key's value using the
    /// configured merge operator. This allows state to be written
    /// into a value directly, without any read-modify-write steps.
    /// Merge operators can be used to implement arbitrary data
    /// structures.
    ///
    /// # Panics
    ///
    /// Calling `merge` will panic if no merge operator has been
    /// configured.
    // pub fn set_merge_operator(
    //     &self,
    //     merge_operator: impl MergeOperator<'de, K, V, SerDe> + 'static,
    // ) {
    //     self.inner
    //         .set_merge_operator(move |key: &[u8], old_v: Option<&[u8]>, value: &[u8]| {
    //             let opt_v = merge_operator(
    //                 <SerDe::DK as Deserializer<'de, K>>::deserialize(key),
    //                 old_v.map(|v| RawValue::<V, _, SerDe::DV>::new(v).value()),
    //                 RawValue::<V, _, SerDe::DV>::new(value).value(),
    //             );
    //             opt_v.map(|v| {
    //                 SerDe::SV::serialize(&v)
    //                     .as_ref()
    //                     .iter()
    //                     .map(|v| *v)
    //                     .collect()
    //             })
    //         });
    // }

    /// Create a double-ended iterator over the tuples of keys and
    /// values in this tree.
    pub fn iter(&self) -> Iter<'de, K, V, SerDe> {
        Iter::from_sled(self.inner.iter())
    }

    /// Create a double-ended iterator over tuples of keys and values,
    /// where the keys fall within the specified range.
    pub fn range<R: RangeBounds<K>>(&self, range: R) -> Iter<'de, K, V, SerDe>
    where
        K: std::fmt::Debug,
    {
        match (range.start_bound(), range.end_bound()) {
            (Bound::Unbounded, Bound::Unbounded) => {
                return Iter::from_sled(self.inner.range::<&[u8], _>(..))
            }
            (Bound::Unbounded, Bound::Excluded(b)) => {
                return Iter::from_sled(self.inner.range(..SerDe::SK::serialize(b)))
            }
            (Bound::Unbounded, Bound::Included(b)) => {
                return Iter::from_sled(self.inner.range(..=SerDe::SK::serialize(b)))
            }
            // FIX: This is not excluding lower bound.
            (Bound::Excluded(b), Bound::Unbounded) => {
                return Iter::from_sled(self.inner.range(SerDe::SK::serialize(b)..))
            }
            (Bound::Excluded(b), Bound::Excluded(bb)) => {
                return Iter::from_sled(
                    self.inner
                        .range(SerDe::SK::serialize(b)..SerDe::SK::serialize(bb)),
                )
            }
            (Bound::Excluded(b), Bound::Included(bb)) => {
                return Iter::from_sled(
                    self.inner
                        .range(SerDe::SK::serialize(b)..=SerDe::SK::serialize(bb)),
                )
            }
            (Bound::Included(b), Bound::Unbounded) => {
                return Iter::from_sled(self.inner.range(SerDe::SK::serialize(b)..))
            }
            (Bound::Included(b), Bound::Excluded(bb)) => {
                return Iter::from_sled(
                    self.inner
                        .range(SerDe::SK::serialize(b)..SerDe::SK::serialize(bb)),
                )
            }
            (Bound::Included(b), Bound::Included(bb)) => {
                return Iter::from_sled(
                    self.inner
                        .range(SerDe::SK::serialize(b)..=SerDe::SK::serialize(bb)),
                )
            }
        }
    }

    /// Create an iterator over tuples of keys and values,
    /// where the all the keys starts with the given prefix.
    pub fn scan_prefix(&self, prefix: &V) -> Iter<'de, K, V, SerDe> {
        Iter::from_sled(self.inner.scan_prefix(SerDe::SV::serialize(prefix)))
    }

    /// Returns the first key and value in the `Tree`, or
    /// `None` if the `Tree` is empty.
    pub fn first(&self) -> Result<Option<(Value<K, SerDe::DK>, Value<V, SerDe::DV>)>> {
        self.inner
            .first()
            .map(|res| res.map(|(k, v)| (Value::new(k), Value::new(v))))
    }

    /// Returns the last key and value in the `Tree`, or
    /// `None` if the `Tree` is empty.
    pub fn last(&self) -> Result<Option<(Value<K, SerDe::DK>, Value<V, SerDe::DV>)>> {
        self.inner
            .last()
            .map(|res| res.map(|(k, v)| (Value::new(k), Value::new(v))))
    }

    /// Atomically removes the maximum item in the `Tree` instance.
    pub fn pop_max(&self) -> Result<Option<(Value<K, SerDe::DK>, Value<V, SerDe::DV>)>> {
        self.inner
            .pop_max()
            .map(|res| res.map(|(k, v)| (Value::new(k), Value::new(v))))
    }

    /// Atomically removes the minimum item in the `Tree` instance.
    pub fn pop_min(&self) -> Result<Option<(Value<K, SerDe::DK>, Value<V, SerDe::DV>)>> {
        self.inner
            .pop_min()
            .map(|res| res.map(|(k, v)| (Value::new(k), Value::new(v))))
    }

    /// Returns the number of elements in this tree.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns `true` if the `Tree` contains no elements.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Clears the `Tree`, removing all values.
    ///
    /// Note that this is not atomic.
    pub fn clear(&self) -> Result<()> {
        self.inner.clear()
    }

    /// Returns the name of the tree.
    pub fn name(&self) -> IVec {
        self.inner.name()
    }

    /// Returns the CRC32 of all keys and values
    /// in this Tree.
    ///
    /// This is O(N) and locks the underlying tree
    /// for the duration of the entire scan.
    pub fn checksum(&self) -> Result<u32> {
        self.inner.checksum()
    }
}

pub trait MergeOperator<'de, K, V, SerDe: serialize::SerDe<'de, K, V>>:
    Fn(
    <SerDe::DK as Deserializer<'de, K>>::DeserializedValue,
    Option<<SerDe::DV as Deserializer<'de, V>>::DeserializedValue>,
    V,
) -> Option<V>
{
}

pub struct TransactionalTree<'a, K, V, SerDe> {
    inner: &'a sled::transaction::TransactionalTree,
    phantom_key: PhantomData<K>,
    phantom_value: PhantomData<V>,
    serde: PhantomData<SerDe>,
}

impl<'a, 'de, K: KV, V: KV, SerDe: serialize::SerDe<'de, K, V>> TransactionalTree<'a, K, V, SerDe> {
    pub fn insert(
        &self,
        key: &K,
        value: &V,
    ) -> std::result::Result<
        Option<Value<V, SerDe::DV>>,
        sled::transaction::UnabortableTransactionError,
    > {
        self.inner
            .insert(
                SerDe::SK::serialize(key).as_ref(),
                SerDe::SV::serialize(value).as_ref(),
            )
            .map(|opt| opt.map(|v| Value::new(v)))
    }

    pub fn remove(
        &self,
        key: &K,
    ) -> std::result::Result<
        Option<Value<V, SerDe::DV>>,
        sled::transaction::UnabortableTransactionError,
    > {
        self.inner
            .remove(SerDe::SK::serialize(key).as_ref())
            .map(|opt| opt.map(|v| Value::new(v)))
    }

    pub fn get(
        &self,
        key: &K,
    ) -> std::result::Result<
        Option<Value<V, SerDe::DV>>,
        sled::transaction::UnabortableTransactionError,
    > {
        self.inner
            .get(SerDe::SK::serialize(key))
            .map(|opt| opt.map(|v| Value::new(v)))
    }

    pub fn apply_batch(
        &self,
        batch: &Batch<K, V, SerDe>,
    ) -> std::result::Result<(), sled::transaction::UnabortableTransactionError> {
        self.inner.apply_batch(&batch.inner)
    }

    pub fn flush(&self) {
        self.inner.flush()
    }

    pub fn generate_id(&self) -> Result<u64> {
        self.inner.generate_id()
    }
}

pub struct Iter<'de, K, V, SerDe> {
    inner: sled::Iter,
    phantom_key: PhantomData<K>,
    phantom_value: PhantomData<V>,
    serde: PhantomData<&'de SerDe>,
}

impl<'de, K: KV, V: KV, SerDe: serialize::SerDe<'de, K, V>> Iterator for Iter<'de, K, V, SerDe> {
    type Item = Result<(Value<K, SerDe::DK>, Value<V, SerDe::DV>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|res| res.map(|(k, v)| (Value::new(k), Value::new(v))))
    }

    fn last(mut self) -> Option<Self::Item> {
        self.inner
            .next_back()
            .map(|res| res.map(|(k, v)| (Value::new(k), Value::new(v))))
    }
}

impl<'de, K: KV, V: KV, SerDe: serialize::SerDe<'de, K, V>> DoubleEndedIterator
    for Iter<'de, K, V, SerDe>
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner
            .next_back()
            .map(|res| res.map(|(k, v)| (Value::new(k), Value::new(v))))
    }
}

impl<'de, K: KV, V: KV, SerDe: serialize::SerDe<'de, K, V> + Sync + Send> Iter<'de, K, V, SerDe> {
    pub fn from_sled(iter: sled::Iter) -> Self {
        Iter {
            inner: iter,
            phantom_key: PhantomData,
            phantom_value: PhantomData,
            serde: PhantomData,
        }
    }

    // pub fn keys(
    //     self,
    // ) -> impl DoubleEndedIterator<Item = Result<Value<K, SerDe::DK>>> + Send + Sync {
    //     self.map(|r| r.map(|(k, _v)| k))
    // }

    // /// Iterate over the values of this Tree
    // pub fn values(
    //     self,
    // ) -> impl DoubleEndedIterator<Item = Result<Value<V, SerDe::DV>>> + Send + Sync {
    //     self.map(|r| r.map(|(_k, v)| v))
    // }
}

#[derive(Clone, Debug)]
pub struct Batch<K, V, SerDe> {
    inner: sled::Batch,
    phantom_key: PhantomData<K>,
    phantom_value: PhantomData<V>,
    serde: PhantomData<SerDe>,
}

impl<'de, K: KV, V: KV, SerDe: serialize::SerDe<'de, K, V>> Batch<K, V, SerDe> {
    pub fn insert(&mut self, key: &K, value: &V) {
        self.inner.insert(
            SerDe::SK::serialize(key).as_ref(),
            SerDe::SV::serialize(value).as_ref(),
        );
    }

    pub fn remove(&mut self, key: &K) {
        self.inner.remove(SerDe::SK::serialize(key).as_ref())
    }
}

// Implementing Default manually to not require K and V to implement Default.
impl<K, V, SerDe> Default for Batch<K, V, SerDe> {
    fn default() -> Self {
        Self {
            inner: Default::default(),
            phantom_key: PhantomData,
            phantom_value: PhantomData,
            serde: PhantomData,
        }
    }
}

pub struct Subscriber<'de, K, V, SerDe> {
    inner: sled::Subscriber,
    phantom_key: PhantomData<K>,
    phantom_value: PhantomData<V>,
    serde: PhantomData<&'de SerDe>,
}

impl<'de, K: KV, V: KV, SerDe: serialize::SerDe<'de, K, V>> Subscriber<'de, K, V, SerDe> {
    pub fn next_timeout(
        &mut self,
        timeout: core::time::Duration,
    ) -> core::result::Result<
        Event<Value<K, SerDe::DK>, Value<V, SerDe::DV>, SerDe>,
        std::sync::mpsc::RecvTimeoutError,
    > {
        self.inner
            .next_timeout(timeout)
            .map(|e| Event::from_sled(&e))
    }

    pub fn from_sled(subscriber: sled::Subscriber) -> Self {
        Self {
            inner: subscriber,
            phantom_key: PhantomData,
            phantom_value: PhantomData,
            serde: PhantomData,
        }
    }
}

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
impl<'de, K: KV + Unpin, V: KV + Unpin, SerDe: serialize::SerDe<'de, K, V>> Future
    for Subscriber<'de, K, V, SerDe>
{
    type Output = Option<Event<Value<K, SerDe::DK>, Value<V, SerDe::DV>, SerDe>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().inner)
            .poll(cx)
            .map(|opt| opt.map(|e| Event::from_sled(&e)))
    }
}

impl<'de, K: KV, V: KV, SerDe: serialize::SerDe<'de, K, V>> Iterator
    for Subscriber<'de, K, V, SerDe>
{
    type Item = Event<Value<K, SerDe::DK>, Value<V, SerDe::DV>, SerDe>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|e| Event::from_sled(&e))
    }
}

pub enum Event<K: KV, V: KV, SerDe> {
    Insert { key: K, value: V },
    Remove { key: K },
    _Unreachable(std::convert::Infallible, std::marker::PhantomData<SerDe>),
}

impl<'de, K: KV, V: KV, SerDe: serialize::SerDe<'de, K, V>>
    Event<Value<K, SerDe::DK>, Value<V, SerDe::DV>, SerDe>
{
    pub fn key(&self) -> &Value<K, SerDe::DK> {
        match self {
            Self::Insert { key, .. } | Self::Remove { key } => key,
            Self::_Unreachable(..) => unreachable!(),
        }
    }

    pub fn from_sled(event: &sled::Event) -> Self {
        match event {
            sled::Event::Insert { key, value } => Self::Insert {
                key: Value::new(key.to_owned()),
                value: Value::new(value.to_owned()),
            },
            sled::Event::Remove { key } => Self::Remove {
                key: Value::new(key.to_owned()),
            },
        }
    }
}

// pub(crate) fn deserialize<'a, T>(bytes: &'a [u8]) -> T
// where
//     T: serde::de::Deserialize<'a>,
// {
//     bincode::deserialize(bytes).expect("deserialization failed, did the type serialized change?")
// }

// pub(crate) fn serialize<T>(value: &T) -> Vec<u8>
// where
//     T: serde::Serialize,
// {
//     bincode::serialize(value).expect("serialization failed, did the type serialized change?")
// }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range() {
        let config = sled::Config::new().temporary(true);
        let db = config.open().unwrap();

        let tree: Tree<u32, u32> = Tree::open(&db, "test_tree");

        tree.insert(&1, &2).unwrap();
        tree.insert(&3, &4).unwrap();
        tree.insert(&6, &2).unwrap();
        tree.insert(&10, &2).unwrap();
        tree.insert(&15, &2).unwrap();
        tree.flush().unwrap();

        let expect_results = [(6, 2), (10, 2)];

        for (i, result) in tree
            .range(6..11)
            .map(|v| v.map(|(k, v)| (k.value(), v.value())))
            .enumerate()
        {
            assert_eq!(result.unwrap(), expect_results[i]);
        }
    }

    #[test]
    fn test_cas() {
        let config = sled::Config::new().temporary(true);
        let db = config.open().unwrap();

        let tree: Tree<u32, u32> = Tree::open(&db, "test_tree");

        let current = 2;
        tree.insert(&1, &current).unwrap();
        let expected = 3;
        let proposed = 4;
        let res = tree
            .compare_and_swap(&1, Some(&expected), Some(&proposed))
            .expect("db failure");

        // assert_eq!(
        //     res,
        //     Err(CompareAndSwapError {
        //         current: Some(current),
        //         proposed: Some(proposed),
        //     }),
        // );
    }
}
