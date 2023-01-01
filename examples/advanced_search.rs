use serde::{Deserialize, Serialize};
use std::ops::Bound;
use tantivy::{
    doc,
    query::{BooleanQuery, Occur, QueryParser, RangeQuery},
    schema::{Schema, Type, TEXT},
    DateOptions, Term,
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
        date: chrono::Utc::now(),
        title: "The life of the disillusioned".to_string(),
        body: "Long story short, he didn't have fun.".to_string(),
    };

    // Inserting the first blog post before the search engine is created.
    tree.insert(&post1)?;

    // Building a schema which defines how our blog posts will be indexed.
    let mut schema_builder = Schema::builder();
    let date = schema_builder.add_date_field("date", DateOptions::default().set_indexed());
    let title = schema_builder.add_text_field("title", TEXT);
    let body = schema_builder.add_text_field("body", TEXT);

    // Creating the search engine. Note that we never finished building the schema_builder
    // as the search engine needs to add one more field to it which maps search results to key and value
    // pairs from the tree.
    // To persist the search engine in a directory, use `new` instead of `new_temp`.
    let search_engine = SearchEngine::new_temp(&tree, schema_builder, move |_k, v| {
        let d = tantivy_datetime(v.date);
        doc!(
          date => d,
          title => v.title.to_owned(),
          body => v.body.to_owned()
        )
    })?;

    // Waiting so the timestamp of the second blog post is more
    // than 1 seconds older.
    std::thread::sleep(std::time::Duration::from_secs(1));

    let post2 = BlogPost {
        date: chrono::Utc::now(),
        title: "Living a happy life".to_string(),
        body: "Live health and stress free.".to_string(),
    };

    tree.insert(&post2)?;
    // Waiting so the new blog post is indexed.
    std::thread::sleep(std::time::Duration::from_secs(1));

    // Creating queries for the search engine.
    // Creating a range query for dates is a little cumbersome.
    let date_bound = RangeQuery::new_term_bounds(
        date,
        Type::Date,
        &Bound::Included(Term::from_field_date(date, tantivy_datetime(post2.date))),
        &Bound::Unbounded,
    );
    let query_parser = QueryParser::for_index(&search_engine.index, vec![title, body]);
    let query1 = query_parser.parse_query("life")?;
    let query2 = query_parser.parse_query("happy")?;

    // BooleanQuery is used to combine multiple queries.
    let date_bound_query1 = BooleanQuery::new(vec![
        (Occur::Must, Box::new(date_bound.clone())),
        (Occur::Must, query1),
    ]);
    let date_bound_query2 = BooleanQuery::new(vec![
        (Occur::Must, Box::new(date_bound)),
        (Occur::Must, query2),
    ]);
    // Recreating query1 and query2 because they can't be cloned easily.
    let query1 = query_parser.parse_query("life")?;
    let query2 = query_parser.parse_query("happy")?;

    // Asserting the results of the searches
    assert_eq!(search_engine.search_with_query(&query1, 10)?.len(), 2);
    assert_eq!(search_engine.search_with_query(&query2, 10)?.len(), 1);
    assert_eq!(
        search_engine
            .search_with_query(&date_bound_query1, 10)?
            .len(),
        1
    );
    assert_eq!(
        search_engine
            .search_with_query(&date_bound_query2, 10)?
            .len(),
        1
    );
    assert_eq!(search_engine.search("life", 10)?.len(), 2);

    Ok(())
}

fn tantivy_datetime(date: chrono::DateTime<chrono::Utc>) -> tantivy::DateTime {
    tantivy::DateTime::from_timestamp_millis(date.timestamp_millis())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct BlogPost {
    date: chrono::DateTime<chrono::Utc>,
    title: String,
    body: String,
}
