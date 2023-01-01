//! Create custom (de)serializers for key and value (de)serialization.
//!
//! The default `Tree` uses bincode for (de)serialization of types
//! that implement DeserializeOwned. However if you want to use
//! zero-copy deserialization, lazy deserialization or simply want
//! to support deserialization of types that don't implement DeserializeOwned
//! you need a different Deserializer. Implementing [SerDe] and
//! using it together with a [Tree][crate::custom_serde::Tree] allows you
//! to do just that.

// use rkyv::{archived_root, ser::Serializer as _, AlignedVec, Archive, Archived};
use serde::de::DeserializeOwned;
use std::convert::AsRef;

/// The default `Tree` uses bincode for (de)serialization of types
/// that implement DeserializeOwned. However if you want to use
/// zero-copy deserialization, lazy deserialization or simply want
/// to support deserialization of types that don't implement DeserializeOwned
/// you need a different Deserializer. Implementing this trait and
/// using it together with a [Tree][crate::custom_serde::Tree] allows you
/// to do just that.
pub trait SerDe<K, V> {
    /// Key Serializer
    type SK: Serializer<K>;
    /// Value Serializer
    type SV: Serializer<V>;
    /// Key Deserializer
    type DK: Deserializer<K>;
    /// Value Deserializer
    type DV: Deserializer<V>;
}

pub type Key<K, V, SD> = <<SD as SerDe<K, V>>::DK as Deserializer<K>>::DeserializedValue;
pub type Value<K, V, SD> = <<SD as SerDe<K, V>>::DV as Deserializer<V>>::DeserializedValue;

pub trait Serializer<T> {
    type Bytes: AsRef<[u8]>;

    fn serialize(value: &T) -> Self::Bytes;
}

pub trait Deserializer<T> {
    type DeserializedValue;

    fn deserialize(bytes: sled::IVec) -> Self::DeserializedValue;
}

/// (De)serializer using bincode.
#[derive(Debug)]
pub struct BincodeSerDe;
pub trait BincodeSerDeBounds: serde::Serialize + DeserializeOwned {}
impl<T> BincodeSerDeBounds for T where T: serde::Serialize + DeserializeOwned {}
#[derive(Debug)]
pub struct BincodeSerDeLazy;
#[derive(Debug)]
pub struct BincodeSerDeLazyK;
#[derive(Debug)]
pub struct BincodeSerDeLazyV;
#[derive(Debug)]
pub struct BincodeSerializer;
#[derive(Debug)]
pub struct BincodeDeserializer;
pub struct BincodeDeserializerLazy;

impl<K: serde::Serialize + DeserializeOwned, V: serde::Serialize + DeserializeOwned> SerDe<K, V>
    for BincodeSerDe
{
    type SK = BincodeSerializer;
    type SV = BincodeSerializer;
    type DK = BincodeDeserializer;
    type DV = BincodeDeserializer;
}

impl<K: serde::Serialize, V: serde::Serialize> SerDe<K, V> for BincodeSerDeLazy {
    type SK = BincodeSerializer;
    type SV = BincodeSerializer;
    type DK = BincodeDeserializerLazy;
    type DV = BincodeDeserializerLazy;
}

impl<K: serde::Serialize, V: serde::Serialize + DeserializeOwned> SerDe<K, V>
    for BincodeSerDeLazyK
{
    type SK = BincodeSerializer;
    type SV = BincodeSerializer;
    type DK = BincodeDeserializerLazy;
    type DV = BincodeDeserializer;
}

impl<K: serde::Serialize + DeserializeOwned, V: serde::Serialize> SerDe<K, V>
    for BincodeSerDeLazyV
{
    type SK = BincodeSerializer;
    type SV = BincodeSerializer;
    type DK = BincodeDeserializer;
    type DV = BincodeDeserializerLazy;
}

impl<T: serde::Serialize> Serializer<T> for BincodeSerializer {
    type Bytes = Vec<u8>;

    fn serialize(value: &T) -> Self::Bytes {
        bincode::serialize(value).expect("serialization failed, did the type serialized change?")
    }
}

impl<T: serde::de::DeserializeOwned> Deserializer<T> for BincodeDeserializer {
    type DeserializedValue = T;

    fn deserialize(bytes: sled::IVec) -> Self::DeserializedValue {
        bincode::deserialize(&bytes)
            .expect("deserialization failed, did the type serialized change?")
    }
}

impl<T> Deserializer<T> for BincodeDeserializerLazy {
    type DeserializedValue = Lazy<T>;

    fn deserialize(bytes: sled::IVec) -> Self::DeserializedValue {
        Lazy::new(bytes)
    }
}

pub struct Lazy<T> {
    v: sled::IVec,
    _t: std::marker::PhantomData<fn() -> T>,
}

impl<T> Lazy<T> {
    fn new(v: sled::IVec) -> Self {
        Self {
            v,
            _t: std::marker::PhantomData,
        }
    }
}

impl<T> Lazy<T> {
    pub fn deserialize<'de>(&'de self) -> T
    where
        T: serde::Deserialize<'de>,
    {
        bincode::deserialize(&self.v)
            .expect("deserialization failed, did the type serialized change?")
    }
}

#[test]
fn test_lazy() {
    let ref_str_bytes = sled::IVec::from(
        bincode::serialize::<&str>(&"hello there my darling how has your day been?").unwrap(),
    );
    let l = Lazy::<&str>::new(ref_str_bytes);
    l.deserialize();
}

// TODO: Implement (De)serializers for rkyv.
