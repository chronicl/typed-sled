use bincode::deserialize;
use rkyv::{
    archived_root,
    ser::{serializers::AllocSerializer, Serializer as _},
    AlignedVec, Archive, Archived, Deserialize, DeserializeUnsized, Serialize,
};
use sled::IVec;
use std::{convert::AsRef, marker::PhantomData};

pub trait SerDe<'de, K, V> {
    /// Key Serializer
    type SK: Serializer<K> + Send + Sync;
    /// Value Serializer
    type SV: Serializer<V> + Send + Sync;
    /// Key Deserializer
    type DK: Deserializer<'de, K> + Send + Sync;
    /// Value Deserializer
    type DV: Deserializer<'de, V> + Send + Sync;
}

pub type Value<V, D> = RawValue<V, IVec, D>;

#[derive(Clone, Debug)]
pub struct RawValue<V, B, D> {
    pub bytes: B,
    value: PhantomData<V>,
    deserializer: PhantomData<D>,
}

impl<V, B, D> RawValue<V, B, D> {
    pub fn new(bytes: B) -> Self {
        Self {
            bytes,
            value: PhantomData,
            deserializer: PhantomData,
        }
    }

    pub fn value<'de>(&'de self) -> D::DeserializedValue
    where
        B: AsRef<[u8]> + 'static,
        D: Deserializer<'de, V>,
    {
        D::deserialize(self.bytes.as_ref())
    }
}

pub trait Serializer<T> {
    type Bytes: AsRef<[u8]>;

    fn serialize(value: &T) -> Self::Bytes;
}

pub trait Deserializer<'de, T> {
    type DeserializedValue;

    fn deserialize(bytes: &'de [u8]) -> Self::DeserializedValue;

    fn deserialize_og(bytes: &'de [u8]) -> T;
}

/// (De)serializer using bincode.
pub struct BincodeSerDe;
pub struct BincodeSerializer;
pub struct BincodeDeserializer;

impl<
        'de,
        K: serde::Serialize + serde::Deserialize<'de>,
        V: serde::Serialize + serde::Deserialize<'de>,
    > SerDe<'de, K, V> for BincodeSerDe
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

impl<'de, T: serde::Deserialize<'de>> Deserializer<'de, T> for BincodeDeserializer {
    type DeserializedValue = T;

    fn deserialize(bytes: &'de [u8]) -> Self::DeserializedValue {
        bincode::deserialize(bytes)
            .expect("deserialization failed, did the type serialized change?")
    }

    fn deserialize_og(bytes: &'de [u8]) -> T {
        bincode::deserialize(bytes)
            .expect("deserialization failed, did the type serialized change?")
    }
}

/// (De)serializer using rkyv.
pub struct RkyvSerDe;
pub struct RkyvSerializer;
pub struct RkyvDeserializer;

use rkyv::ser::serializers::AlignedSerializer;
impl<
        'de,
        K: rkyv::Archive
            + 'de
            + rkyv::Serialize<AlignedSerializer<AlignedVec>>
            + rkyv::Deserialize<K, rkyv::Infallible>,
        V: rkyv::Archive
            + 'de
            + rkyv::Serialize<AlignedSerializer<AlignedVec>>
            + rkyv::Deserialize<V, rkyv::Infallible>,
    > SerDe<'de, K, V> for RkyvSerDe
where
    <K as Archive>::Archived: rkyv::Deserialize<K, rkyv::Infallible>,
    <V as Archive>::Archived: rkyv::Deserialize<V, rkyv::Infallible>,
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

impl<'de, S: rkyv::Fallible + ?Sized, T: rkyv::Archive + rkyv::Deserialize<T, S> + 'de>
    Deserializer<'de, T> for RkyvDeserializer
where
    <T as Archive>::Archived: rkyv::Deserialize<T, rkyv::Infallible>,
{
    type DeserializedValue = &'de Archived<T>;

    fn deserialize(bytes: &'de [u8]) -> Self::DeserializedValue {
        unsafe { archived_root::<T>(bytes) }
    }

    fn deserialize_og(bytes: &'de [u8]) -> T {
        let archived = unsafe { archived_root::<T>(bytes) };
        rkyv::Deserialize::<T, rkyv::Infallible>::deserialize(archived, &mut rkyv::Infallible)
            .unwrap()
    }
}
