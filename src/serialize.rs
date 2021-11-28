use bincode::deserialize;
use rkyv::{
    archived_root,
    ser::{serializers::AllocSerializer, Serializer as _},
    AlignedVec, Archive, Archived, Deserialize, DeserializeUnsized, Serialize,
};
use sled::IVec;
use std::{convert::AsRef, marker::PhantomData};

/// The default `Tree` uses bincode for (de)serialization, however if
/// faster (de)serializtion
pub trait SerDe<K, V> {
    /// Key Serializer
    type SK: Serializer<K> + Send + Sync;
    /// Value Serializer
    type SV: Serializer<V> + Send + Sync;
    /// Key Deserializer
    type DK: Deserializer<K> + Send + Sync;
    /// Value Deserializer
    type DV: Deserializer<V> + Send + Sync;
}

pub type Value<V, D> = RawValue<V, IVec, D>;

#[derive(Clone, Debug)]
pub struct RawValue<T, B, D> {
    pub bytes: B,
    value: PhantomData<T>,
    deserializer: PhantomData<D>,
}

impl<T, B, D> RawValue<T, B, D> {
    pub fn new(bytes: B) -> Self {
        Self {
            bytes,
            value: PhantomData,
            deserializer: PhantomData,
        }
    }

    pub fn value<'de>(&'de self) -> D::DeserializedValue<'de>
    where
        B: AsRef<[u8]> + 'static,
        D: Deserializer<T>,
    {
        D::deserialize(self.bytes.as_ref())
    }
}

pub trait Serializer<T> {
    type Bytes: AsRef<[u8]>;

    fn serialize(value: &T) -> Self::Bytes;
}

pub trait Deserializer<T> {
    type DeserializedValue<'de>;

    fn deserialize<'de>(bytes: &'de [u8]) -> Self::DeserializedValue<'de>;
}

/// (De)serializer using bincode.
#[derive(Debug)]
pub struct BincodeSerDe;
#[derive(Debug)]
pub struct BincodeSerializer;
#[derive(Debug)]
pub struct BincodeDeserializer;

impl<
        K: serde::Serialize + for<'de> serde::Deserialize<'de>,
        V: serde::Serialize + for<'de> serde::Deserialize<'de>,
    > SerDe<K, V> for BincodeSerDe
{
    type SK = BincodeSerializer;
    type SV = BincodeSerializer;
    type DK = BincodeDeserializer;
    type DV = BincodeDeserializer;
}

impl<T: serde::Serialize> Serializer<T> for BincodeSerializer {
    type Bytes = Vec<u8>;

    fn serialize(value: &T) -> Self::Bytes {
        bincode::serialize(value).expect("serialization failed, did the type serialized change?")
    }
}

impl<T: for<'a> serde::Deserialize<'a>> Deserializer<T> for BincodeDeserializer {
    type DeserializedValue<'de> = T;

    fn deserialize<'de>(bytes: &'de [u8]) -> Self::DeserializedValue<'de> {
        bincode::deserialize(bytes)
            .expect("deserialization failed, did the type serialized change?")
    }
}

/// (De)serializer using rkyv.
#[derive(Debug)]
pub struct RkyvSerDe;
#[derive(Debug)]
pub struct RkyvSerializer;
#[derive(Debug)]
pub struct RkyvDeserializer;

use rkyv::ser::serializers::AlignedSerializer;
impl<
        K: rkyv::Archive + rkyv::Serialize<AlignedSerializer<AlignedVec>>,
        V: rkyv::Archive + rkyv::Serialize<AlignedSerializer<AlignedVec>>,
    > SerDe<K, V> for RkyvSerDe
where
    // Deserialize not necessary here
    <K as Archive>::Archived: rkyv::Deserialize<K, rkyv::Infallible> + 'static,
    <V as Archive>::Archived: rkyv::Deserialize<V, rkyv::Infallible> + 'static,
{
    type SK = RkyvSerializer;
    type SV = RkyvSerializer;
    type DK = RkyvDeserializer;
    type DV = RkyvDeserializer;
}

impl<T: rkyv::Serialize<AlignedSerializer<AlignedVec>>> Serializer<T> for RkyvSerializer {
    type Bytes = AlignedVec;

    fn serialize(value: &T) -> Self::Bytes {
        let mut serializer = AlignedSerializer::<AlignedVec>::default();
        serializer.serialize_value(value).unwrap();
        serializer.into_inner()
    }
}

impl<T: rkyv::Archive> Deserializer<T> for RkyvDeserializer
where
    // would love to remove this 'static bound, but don't know how
    <T as Archive>::Archived: 'static,
{
    type DeserializedValue<'de> = &'de Archived<T>;

    fn deserialize<'de>(bytes: &'de [u8]) -> Self::DeserializedValue<'de> {
        unsafe { archived_root::<T>(bytes) }
    }
}
