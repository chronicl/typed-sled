use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rkyv::Deserialize;
use typed_sled::serialize::{
    BincodeDeserializer, BincodeSerializer, Deserializer, RkyvDeserializer, RkyvSerializer,
    Serializer,
};

#[derive(
    serde::Serialize, serde::Deserialize, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive,
)]
struct A {
    s1: String,
    s2: String,
    s3: String,
}

fn serialize_bincode<T: serde::Serialize>(a: &T) -> <BincodeSerializer as Serializer<T>>::Bytes {
    BincodeSerializer::serialize(a)
}

fn serialize_rkyv<
    T: rkyv::Serialize<rkyv::ser::serializers::AlignedSerializer<rkyv::AlignedVec>>,
>(
    a: &T,
) -> <RkyvSerializer as Serializer<T>>::Bytes {
    RkyvSerializer::serialize(a)
}

fn deserialize_bincode<'de, T: for<'a> serde::Deserialize<'a>>(a: &'de [u8]) {
    <BincodeDeserializer as Deserializer<T>>::deserialize(a);
}

fn deserialize_rkyv<T: rkyv::Archive>(a: &[u8])
where
    <T as rkyv::Archive>::Archived: rkyv::Deserialize<T, rkyv::Infallible> + 'static,
{
    let r = <RkyvDeserializer as Deserializer<T>>::deserialize(a);
    r.deserialize(&mut rkyv::Infallible).unwrap();
}

fn sled_get(tree: &typed_sled::Tree<String, A>, key: &String) {
    tree.get(key).unwrap().unwrap().value();
}

fn sled_insert(tree: &typed_sled::Tree<String, A>, key: &String, a: &A) {
    tree.insert(key, a).unwrap().unwrap().value();
}

fn criterion_benchmark(c: &mut Criterion) {
    let a = A {
        s1: "HGKAHsdgahs;ldkghasdslfjas".to_string(),
        s2: "sd;ghas;gha;sdhgas;gdhasdgas".to_string(),
        s3: "dasgahskgdhas;khgajg".to_string(),
    };
    let a_serialized_bincode = serialize_bincode(&a);
    let a_serialized_rkyv = serialize_rkyv(&a);

    // Bincode seems to win serialization by about 4x, which
    // makes sense since the Archived
    c.bench_function("Bincode: serialize struct A", |b| {
        b.iter(|| serialize_bincode(black_box(&a)))
    });

    c.bench_function("Rkyv: serialize struct A", |b| {
        b.iter(|| serialize_rkyv(black_box(&a)))
    });

    c.bench_function("Rkyv: serialize struct A", |b| {
        b.iter(|| serialize_rkyv(black_box(&a)))
    });

    // u64 could be a key
    c.bench_function("Bincode: serialize u64", |b| {
        b.iter(|| serialize_bincode(black_box(&50u64)))
    });

    c.bench_function("Rkyv: serialize u64", |b| {
        b.iter(|| serialize_rkyv(black_box(&50u64)))
    });

    c.bench_function("Bincode: deserialize struct A", |b| {
        b.iter(|| deserialize_bincode::<A>(black_box(&a_serialized_bincode)))
    });

    c.bench_function("Rkyv: deserialize struct A", |b| {
        b.iter(|| deserialize_rkyv::<A>(black_box(&a_serialized_rkyv)))
    });

    let config = sled::Config::new().temporary(true);
    let db = config.open().unwrap();

    // The id is used by sled to identify which Tree in the database (db) to open.
    let animals = typed_sled::Tree::<String, A>::open(&db, "unique_id");
    animals.insert(&"YESSS".to_string(), &a);

    c.bench_function("Tree: get", |b| {
        b.iter(|| sled_get(black_box(&animals), black_box(&"YESSS".to_string())))
    });

    c.bench_function("Tree: insert", |b| {
        b.iter(|| {
            sled_insert(
                black_box(&animals),
                black_box(&"YESSS".to_string()),
                black_box(&a),
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
