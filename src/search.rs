use crate::{serialize, Event, Tree, KV};

use std::fs::create_dir;
use std::iter::Iterator;
use std::thread;
use std::{marker::PhantomData, path::Path};
use tantivy::collector::TopDocs;
use tantivy::DocAddress;
use tantivy::{
    collector::Collector,
    directory::MmapDirectory,
    query::{Query, QueryParser},
    schema::{BytesOptions, Field, SchemaBuilder, Value},
    Document, Index, IndexReader, Score, Term,
};

pub struct SearchEngine<K, V> {
    tree: Tree<K, V>,
    pub index: Index,
    index_reader: IndexReader,
    key_field: Field,
    phantom_key: PhantomData<K>,
    phantom_value: PhantomData<V>,
}

impl<K, V> SearchEngine<K, V> {
    pub fn open<P: AsRef<Path> + Clone, F>(
        path: P,
        tree: &Tree<K, V>,
        schema_builder: SchemaBuilder,
        f: F,
    ) -> Result<Self, SearchError>
    where
        F: Fn(&K, &V) -> Document + Send + Sync + 'static,
        K: KV + 'static,
        V: KV + 'static,
    {
        Self::open_with_options(Some(path), tree, schema_builder, f)
    }

    pub fn open_temp<F>(
        tree: &Tree<K, V>,
        schema_builder: SchemaBuilder,
        f: F,
    ) -> Result<Self, SearchError>
    where
        F: Fn(&K, &V) -> Document + Send + Sync + 'static,
        K: KV + 'static,
        V: KV + 'static,
    {
        Self::open_with_options::<&str, _>(None, tree, schema_builder, f)
    }

    // Change unwraps
    pub fn open_with_options<P: AsRef<Path> + Clone, F>(
        path: Option<P>,
        tree: &Tree<K, V>,
        mut schema_builder: SchemaBuilder,
        f: F,
    ) -> Result<Self, SearchError>
    where
        F: Fn(&K, &V) -> Document + Send + Sync + 'static,
        K: KV + 'static,
        V: KV + 'static,
    {
        let key_field = schema_builder.add_bytes_field(
            "_typed_sled_key",
            BytesOptions::default()
                .set_indexed()
                .set_stored()
                .set_fast(),
        );
        let schema = schema_builder.build();

        let f = move |k: &K, v: &V| {
            let mut document = f(k, v);
            document.add_bytes(key_field, serialize(k));
            document
        };

        let index = if let Some(path) = path {
            create_dir(path.clone()).ok();
            Index::open_or_create(
                MmapDirectory::open(path).expect("SearchEngine: failed to open directory"),
                schema,
            )?
        } else {
            Index::create_from_tempdir(schema)?
        };

        let mut index_writer = index.writer(100_000_000)?;
        for r in tree.iter() {
            let (k, v) = r?;
            index_writer.add_document(f(&k, &v));
        }
        index_writer.commit()?;
        drop(index_writer);

        let mut subscriber = tree.watch_all();
        let mut index_writer = index.writer(5_000_000)?;
        thread::spawn(move || {
            while let Some(e) = subscriber.next() {
                match e {
                    Event::Insert { key, value } => {
                        index_writer.add_document(f(&key, &value));
                        // How should this error be handled?
                        index_writer.commit();
                    }
                    Event::Remove { key } => {
                        index_writer
                            .delete_term(Term::from_field_bytes(key_field, &serialize(&key)));
                        // How should this error be handled?
                        index_writer.commit();
                    }
                }
            }
        });

        let index_reader = index.reader()?;
        Ok(SearchEngine {
            tree: tree.to_owned(),
            index,
            index_reader,
            key_field,
            phantom_key: PhantomData,
            phantom_value: PhantomData,
        })
    }

    // For more info on what queries can be parsed:
    // https://docs.rs/tantivy/latest/tantivy/query/struct.QueryParser.html
    pub fn search(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<Vec<(Score, Option<(K, V)>)>, SearchError>
    where
        K: KV,
        V: KV,
    {
        let query_parser = QueryParser::for_index(
            &self.index,
            self.index
                .schema()
                .fields()
                .map(|(field, _)| field)
                .collect(),
        );
        let query = query_parser.parse_query(query)?;

        self.search_with_query(&query, limit)
    }

    pub fn search_with_query(
        &self,
        query: &dyn Query,
        limit: usize,
    ) -> Result<Vec<(Score, Option<(K, V)>)>, SearchError>
    where
        K: KV,
        V: KV,
    {
        let searcher = self.index_reader.searcher();
        println!(
            "{}",
            searcher.search(query, &TopDocs::with_limit(limit))?.len()
        );

        let mut v = Vec::new();
        for (score, doc_addr) in self
            .search_with_query_and_collector(query, &TopDocs::with_limit(limit))?
            .iter()
        {
            let doc = searcher.doc(*doc_addr)?;

            let key_bytes = if let Value::Bytes(bytes) = doc
                .get_first(self.key_field)
                .ok_or(SearchError::DocDoesNotExist)?
            {
                bytes
            } else {
                panic!();
            };
            let kv = self.tree.get_kv_from_raw(key_bytes)?;
            v.push((*score, kv))
        }
        Ok(v)
    }

    pub fn search_with_collector<C: Collector>(
        &self,
        query: &str,
        collector: &C,
    ) -> Result<C::Fruit, SearchError> {
        let query_parser = QueryParser::for_index(
            &self.index,
            self.index
                .schema()
                .fields()
                .map(|(field, _)| field)
                .collect(),
        );
        let query = query_parser.parse_query(query)?;

        self.search_with_query_and_collector(&query, collector)
    }

    pub fn search_with_query_and_collector<C: Collector>(
        &self,
        query: &dyn Query,
        collector: &C,
    ) -> Result<C::Fruit, SearchError> {
        let searcher = self.index_reader.searcher();
        Ok(searcher.search(query, collector)?)
    }

    pub fn doc_address_to_kv(&self, doc_addr: DocAddress) -> Result<Option<(K, V)>, SearchError>
    where
        K: KV,
        V: KV,
    {
        let doc = self.index_reader.searcher().doc(doc_addr)?;

        let key_bytes = if let Value::Bytes(bytes) = doc
            .get_first(self.key_field)
            .ok_or(SearchError::DocDoesNotExist)?
        {
            bytes
        } else {
            panic!();
        };
        let kv = self.tree.get_kv_from_raw(key_bytes)?;
        Ok(kv)
    }

    pub fn doc_adresses_to_kvs<I: Iterator<Item = DocAddress>>(
        &self,
        doc_addrs: I,
    ) -> Result<Vec<Option<(K, V)>>, SearchError>
    where
        K: KV,
        V: KV,
    {
        let searcher = self.index_reader.searcher();
        let mut v = Vec::new();
        for doc_addr in doc_addrs {
            let doc = searcher.doc(doc_addr)?;
            let key_bytes = if let Value::Bytes(bytes) = doc
                .get_first(self.key_field)
                .ok_or(SearchError::DocDoesNotExist)?
            {
                bytes
            } else {
                panic!();
            };
            let kv = self.tree.get_kv_from_raw(key_bytes)?;
            v.push(kv);
        }
        Ok(v)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SearchError {
    #[error("Tree error: {0}")]
    Tree(#[from] sled::Error),
    #[error("Search error: {0}")]
    Search(#[from] tantivy::TantivyError),
    #[error("Error while parsing query: {0}")]
    QueryParserError(#[from] tantivy::query::QueryParserError),
    #[error("Document Address doesn't exist")]
    DocDoesNotExist,
}
