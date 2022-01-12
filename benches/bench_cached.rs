use criterion::{black_box, criterion_group, criterion_main, Criterion};
use sled::Config;
use std::collections::HashMap;
use typed_sled::{cached::CachedTree, Tree};

fn tree_get<V: typed_sled::KV>(tree: &Tree<u64, V>) {
    tree.get(&0).unwrap();
}

fn cached_tree_get<V: typed_sled::KV>(tree: &CachedTree<u64, V>) {
    tree.get(&0).unwrap();
}

fn criterion_benchmark(c: &mut Criterion) {
    let db = Config::new().temporary(true).open().unwrap();
    let tree: Tree<u64, HashMap<u64, String>> = Tree::open(&db, "bench_cached");
    let cached_tree = CachedTree::new(&tree, 10);

    let mut big_hashmap: HashMap<u64, String> = HashMap::new();
    for i in 0..100000 {
        big_hashmap.insert(
            i,
            "this is some not too short string, which won't be quite that easy to deserialize."
                .to_owned(),
        );
    }
    cached_tree.insert(0, big_hashmap).unwrap();

    c.bench_function("tree get", |b| b.iter(|| tree_get(black_box(&tree))));

    c.bench_function("cached tree get", |b| {
        b.iter(|| cached_tree_get(black_box(&cached_tree)))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
