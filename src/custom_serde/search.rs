// //! A search engine for a single [Tree][crate::Tree], using [tantivy] under the hood.
// //! # Example
// //! ```
// //! use typed_sled::search::SearchEngine;
// //! use tantivy::{
// //!     doc,
// //!     schema::{Schema, TEXT},
// //! };
// //!
// //! #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
// //! struct BlogPost {
// //!     author: String,
// //!     title: String,
// //!     body: String,
// //! }
// //!
// //! # pub fn main() -> Result<(), Box<dyn std::error::Error>> {
// //! let config = sled::Config::new().temporary(true);
// //! let db = config.open().unwrap();
// //!
// //! let tree = typed_sled::Tree::<u64, BlogPost>::open(&db, "unique_id");
// //!
// //! let post = BlogPost {
// //!     author: "Mike".to_string(),
// //!     title: "The life of the disillusioned".to_string(),
// //!     body: "Long story short, he didn't have fun.".to_string(),
// //! };
// //! tree.insert(&0, &post)?;
// //!
// //! let mut schema_builder = Schema::builder();
// //! let author = schema_builder.add_text_field("author", TEXT);
// //! let title = schema_builder.add_text_field("title", TEXT);
// //! let body = schema_builder.add_text_field("body", TEXT);
// //!
// //! let post_to_document = move |_k, v| {
// //!     doc!(
// //!         author => v.author.to_owned(),
// //!         title => v.title.to_owned(),
// //!         body => v.body.to_owned()
// //!       )
// //!  };
// //! let search_engine = SearchEngine::open_temp(&tree, schema_builder, post_to_document);
// //! let search_result = search_engine.search("life", 10)?;
// //!
// //! for result in search_results.iter() {
// //!     println!("Found Blog Post with score {}:\n{:#?}", result.0, result.1);
// //! }
// //! # Ok(()) }
// //! ```
// //!
// //! [tantivy]: https://docs.rs/tantivy/latest/tantivy/
// use crate::custom_serde::{
//     serialize::{self, BincodeSerDe, BincodeSerDeBounds, BincodeSerializer, Serializer},
//     Event, Tree,
// };

// use std::fs::create_dir_all;
// use std::iter::Iterator;
// use std::thread;
// use std::{marker::PhantomData, path::Path};
// use tantivy::collector::TopDocs;
// use tantivy::DocAddress;
// use tantivy::{
//     collector::Collector,
//     directory::MmapDirectory,
//     query::{Query, QueryParser},
//     schema::{BytesOptions, Field, SchemaBuilder, Value},
//     Document, Index, IndexReader, Score, Term,
// };

// /// A search engine for a single tree, using `tantivy` under the hood.
// /// # Example
// /// ```
// /// use typed_sled::search::SearchEngine;
// /// use tantivy::{
// ///     doc,
// ///     schema::{Schema, TEXT},
// /// };
// /// use serde::{Deserialize, Serialize};
// ///
// /// #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
// /// struct BlogPost {
// ///     author: String,
// ///     title: String,
// ///     body: String,
// /// }
// ///
// /// # pub fn main() -> Result<(), Box<dyn std::error::Error>> {
// /// let config = sled::Config::new().temporary(true);
// /// let db = config.open().unwrap();
// ///
// /// let tree = typed_sled::Tree::<u64, BlogPost>::open(&db, "unique_id");
// ///
// /// let post = BlogPost {
// ///     author: "Mike".to_string(),
// ///     title: "The life of the disillusioned".to_string(),
// ///     body: "Long story short, he didn't have fun.".to_string(),
// /// };
// /// tree.insert(&0, &post)?;
// ///
// /// let mut schema_builder = Schema::builder();
// /// let author = schema_builder.add_text_field("author", TEXT);
// /// let title = schema_builder.add_text_field("title", TEXT);
// /// let body = schema_builder.add_text_field("body", TEXT);
// ///
// /// let search_engine = SearchEngine::new_temp(&tree, schema_builder, move |_k, v| {
// ///     doc!(
// ///         author => v.author.to_owned(),
// ///         title => v.title.to_owned(),
// ///         body => v.body.to_owned()
// ///       )
// ///  })?;
// /// let search_results = search_engine.search("life", 10)?;
// ///
// /// for result in search_results.iter() {
// ///     println!("Found Blog Post with score {}:\n{:#?}", result.0, result.1);
// /// }
// /// # Ok(()) }
// /// ```
// pub struct SearchEngine<K, V> {
//     tree: Tree<K, V>,
//     pub index: Index,
//     index_reader: IndexReader,
//     key_field: Field,
//     phantom_key: PhantomData<fn() -> K>,
//     phantom_value: PhantomData<fn() -> V>,
// }

// impl<K, V> SearchEngine<K, V> {
//     /// Create a new search engine or if the path already exists
//     /// open an existing search engine.
//     pub fn new<P: AsRef<Path> + Clone, F>(
//         path: P,
//         tree: &Tree<K, V>,
//         schema_builder: SchemaBuilder,
//         f: F,
//     ) -> Result<Self, SearchError>
//     where
//         F: Fn(&K, &V) -> Document + Send + Sync + 'static,
//         K: BincodeSerDeBounds + 'static,
//         V: BincodeSerDeBounds + 'static,
//     {
//         Self::new_with_options(Some(path), tree, schema_builder, f)
//     }

//     /// Create a new temporary search engine.
//     pub fn new_temp<F>(
//         tree: &Tree<K, V>,
//         schema_builder: SchemaBuilder,
//         f: F,
//     ) -> Result<Self, SearchError>
//     where
//         F: Fn(&K, &V) -> Document + Send + Sync + 'static,
//         K: BincodeSerDeBounds + 'static,
//         V: BincodeSerDeBounds + 'static,
//     {
//         Self::new_with_options::<&str, _>(None, tree, schema_builder, f)
//     }

//     // Change unwraps
//     /// Create a new search engine with more options.
//     fn new_with_options<P: AsRef<Path> + Clone, F>(
//         path: Option<P>,
//         tree: &Tree<K, V>,
//         mut schema_builder: SchemaBuilder,
//         f: F,
//     ) -> Result<Self, SearchError>
//     where
//         F: Fn(&K, &V) -> Document + Send + Sync + 'static,
//         K: BincodeSerDeBounds + 'static,
//         V: BincodeSerDeBounds + 'static,
//     {
//         let key_field = schema_builder.add_bytes_field(
//             "_typed_sled_key",
//             BytesOptions::default()
//                 .set_indexed()
//                 .set_stored()
//                 .set_fast(),
//         );
//         let schema = schema_builder.build();

//         let f = move |k: &K, v: &V| {
//             let mut document = f(k, v);
//             document.add_bytes(key_field, BincodeSerializer::serialize(k));
//             document
//         };

//         let mut from_new = false;
//         let index = if let Some(path) = path {
//             from_new = !create_dir_all(path.clone()).is_err();
//             Index::open_or_create(
//                 MmapDirectory::open(path).expect("SearchEngine: failed to open directory"),
//                 schema,
//             )?
//         } else {
//             from_new = true;
//             Index::create_from_tempdir(schema)?
//         };

//         if from_new {
//             let mut index_writer = index.writer(100_000_000)?;
//             for r in tree.iter() {
//                 let (k, v) = r?;
//                 index_writer.add_document(f(&k, &v));
//             }
//             index_writer.commit()?;
//         }

//         let mut subscriber = tree.watch_all();
//         let mut index_writer = index.writer(5_000_000)?;
//         thread::spawn(move || {
//             while let Some(e) = subscriber.next() {
//                 match e {
//                     Event::Insert { key, value } => {
//                         index_writer.add_document(f(&key, &value));
//                         // How should this error be handled?
//                         index_writer.commit();
//                     }
//                     Event::Remove { key } => {
//                         index_writer.delete_term(Term::from_field_bytes(
//                             key_field,
//                             &BincodeSerializer::serialize(&key),
//                         ));
//                         // How should this error be handled?
//                         index_writer.commit();
//                     }
//                 }
//             }
//         });

//         let index_reader = index.reader()?;
//         Ok(SearchEngine {
//             tree: tree.to_owned(),
//             index,
//             index_reader,
//             key_field,
//             phantom_key: PhantomData,
//             phantom_value: PhantomData,
//         })
//     }

//     /// Search for all key value pairs matching a query. The query will be
//     /// parsed by the `QueryParser` from `tantivy`. All fields of the schema
//     /// will be queried.
//     /// For more info on what queries can be used:
//     /// https://docs.rs/tantivy/latest/tantivy/query/struct.QueryParser.html
//     pub fn search(
//         &self,
//         query: &str,
//         limit: usize,
//     ) -> Result<Vec<(Score, Option<(K, V)>)>, SearchError>
//     where
//         K: BincodeSerDeBounds,
//         V: BincodeSerDeBounds,
//     {
//         let query_parser = QueryParser::for_index(
//             &self.index,
//             self.index
//                 .schema()
//                 .fields()
//                 .map(|(field, _)| field)
//                 .collect(),
//         );
//         let query = query_parser.parse_query(query)?;

//         self.search_with_query(&query, limit)
//     }

//     /// Search for all key value pairs matching a custom query.
//     /// See `tantivy` queries for what type of queries can be used.
//     pub fn search_with_query(
//         &self,
//         query: &dyn Query,
//         limit: usize,
//     ) -> Result<Vec<(Score, Option<(K, V)>)>, SearchError>
//     where
//         K: BincodeSerDeBounds,
//         V: BincodeSerDeBounds,
//     {
//         let searcher = self.index_reader.searcher();
//         println!(
//             "{}",
//             searcher.search(query, &TopDocs::with_limit(limit))?.len()
//         );

//         let mut v = Vec::new();
//         for (score, doc_addr) in self
//             .search_with_query_and_collector(query, &TopDocs::with_limit(limit))?
//             .iter()
//         {
//             let doc = searcher.doc(*doc_addr)?;

//             let key_bytes = if let Value::Bytes(bytes) = doc
//                 .get_first(self.key_field)
//                 .ok_or(SearchError::DocDoesNotExist)?
//             {
//                 bytes
//             } else {
//                 panic!();
//             };
//             let kv = self.tree.get_kv_from_raw(key_bytes)?;
//             v.push((*score, kv))
//         }
//         Ok(v)
//     }

//     /// Search for all key value pairs matching a query and collect them with a custom collector.
//     /// The query will be parsed by the `QueryParser` from `tantivy`. All fields of the schema
//     /// will be queried.
//     /// See `tantivy` for what type of collectors can be used.
//     pub fn search_with_collector<C: Collector>(
//         &self,
//         query: &str,
//         collector: &C,
//     ) -> Result<C::Fruit, SearchError> {
//         let query_parser = QueryParser::for_index(
//             &self.index,
//             self.index
//                 .schema()
//                 .fields()
//                 .map(|(field, _)| field)
//                 .collect(),
//         );
//         let query = query_parser.parse_query(query)?;

//         self.search_with_query_and_collector(&query, collector)
//     }

//     /// Search for all key value pairs matching a custom query and collect them with a custom collector.
//     /// See `tantivy` for what type of queries and collectors can be used.
//     pub fn search_with_query_and_collector<C: Collector>(
//         &self,
//         query: &dyn Query,
//         collector: &C,
//     ) -> Result<C::Fruit, SearchError> {
//         let searcher = self.index_reader.searcher();
//         Ok(searcher.search(query, collector)?)
//     }

//     /// Retrieve the key value pair corresponding to a `DocAdress`.
//     /// This can for example be used after using `search_with_collector` with
//     /// a custom collector which yields `DocAdress`es.
//     pub fn doc_address_to_kv(&self, doc_addr: DocAddress) -> Result<Option<(K, V)>, SearchError>
//     where
//         K: BincodeSerDeBounds,
//         V: BincodeSerDeBounds,
//     {
//         let doc = self.index_reader.searcher().doc(doc_addr)?;

//         let key_bytes = if let Value::Bytes(bytes) = doc
//             .get_first(self.key_field)
//             .ok_or(SearchError::DocDoesNotExist)?
//         {
//             bytes
//         } else {
//             panic!();
//         };
//         let kv = self.tree.get_kv_from_raw(key_bytes)?;
//         Ok(kv)
//     }

//     /// Retrieve the key value pair corresponding to an Iterator over `DocAdress`.
//     /// This can for example be used after using `search_with_collector` with
//     /// a custom collector which yields `DocAdress`es.
//     pub fn doc_adresses_to_kvs<I: Iterator<Item = DocAddress>>(
//         &self,
//         doc_addrs: I,
//     ) -> Result<Vec<Option<(K, V)>>, SearchError>
//     where
//         K: BincodeSerDeBounds,
//         V: BincodeSerDeBounds,
//     {
//         let searcher = self.index_reader.searcher();
//         let mut v = Vec::new();
//         for doc_addr in doc_addrs {
//             let doc = searcher.doc(doc_addr)?;
//             let key_bytes = if let Value::Bytes(bytes) = doc
//                 .get_first(self.key_field)
//                 .ok_or(SearchError::DocDoesNotExist)?
//             {
//                 bytes
//             } else {
//                 panic!();
//             };
//             let kv = self.tree.get_kv_from_raw(key_bytes)?;
//             v.push(kv);
//         }
//         Ok(v)
//     }

//     /// Get the index of the SearchEngine.
//     pub fn index(&self) -> &Index {
//         &self.index
//     }
// }

// #[derive(Debug, thiserror::Error)]
// pub enum SearchError {
//     #[error("Tree error: {0}")]
//     Tree(#[from] sled::Error),
//     #[error("Search error: {0}")]
//     Search(#[from] tantivy::TantivyError),
//     #[error("Error while parsing query: {0}")]
//     QueryParserError(#[from] tantivy::query::QueryParserError),
//     #[error("Document Address doesn't exist")]
//     DocDoesNotExist,
// }
