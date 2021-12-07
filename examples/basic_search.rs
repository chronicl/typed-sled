use serde::{Deserialize, Serialize};
use tantivy::{
    doc,
    schema::{Schema, TEXT},
};
use typed_sled::search::SearchEngine;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Creating a temporary sled database.
    // If you want to persist the data use sled::open instead.
    let db = sled::Config::new().temporary(true).open().unwrap();

    // The id is used by sled to identify which Tree in the database (db) to open
    let tree = typed_sled::key_generating::CounterTree::open(&db, "unique_id");

    // Dummy blog post
    let post1 = BlogPost {
        author: "Mike".to_string(),
        title: "The life of the disillusioned".to_string(),
        body: "Long story short, he didn't have fun.".to_string(),
    };

    // Inserting the first blog post before the search engine is created.
    tree.insert(&post1)?;

    // Building a schema which defines how our blog posts will be indexed.
    let mut schema_builder = Schema::builder();
    let author = schema_builder.add_text_field("author", TEXT);
    let title = schema_builder.add_text_field("title", TEXT);
    let body = schema_builder.add_text_field("body", TEXT);

    // Creating the search engine. Note that we never finished building the schema_builder
    // as the search engine needs to add one more field to it which maps search results to key and value
    // pairs from the tree.
    // To persist the search engine in a directory, use `new` instead of `new_temp`.
    let search_engine = SearchEngine::new_temp(&tree, schema_builder, move |_k, v| {
        doc!(
          author => v.author.to_owned(),
          title => v.title.to_owned(),
          body => v.body.to_owned()
        )
    })?;

    let post2 = BlogPost {
        author: "Anna".to_string(),
        title: "Living a happy life".to_string(),
        body: "Live health and stress free.".to_string(),
    };

    tree.insert(&post2)?;
    // Waiting so the new blog post is indexed.
    std::thread::sleep(std::time::Duration::from_secs(1));

    // Printing the results of the searches
    println!("Searching for Blog Posts matching the query \"life\"");
    let results = search_engine.search("life", 10)?;
    for result in results.iter() {
        println!("Found Blog Post with score {}:\n{:#?}", result.0, result.1);
    }

    println!("\nSearching for Blog Posts matching the query \"happy\"");
    let results = search_engine.search("happy", 10)?;
    for result in results.iter() {
        println!("Found Blog Post with score {}:\n{:#?}", result.0, result.1);
    }

    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct BlogPost {
    author: String,
    title: String,
    body: String,
}
