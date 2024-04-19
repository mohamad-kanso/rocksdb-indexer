use actix_web::{web, App, HttpServer, Responder, HttpResponse};
use rocksdb::{Options, DB};
use serde_json::{json, Value};
use std::collections::HashMap;

async fn get_json_data() -> impl Responder {
    // Open RocksDB database
    let path = "/path/to/your/rocksdb";
    let db = DB::open_default(path).unwrap();

    // Iterate over entries
    let mut json_data = HashMap::new();
    let iter = db.iterator(rocksdb::IteratorMode::Start);
    for (key, value) in iter {
        let key_str = String::from_utf8_lossy(&key);
        let parts: Vec<&str> = key_str.split('.').collect();

        // Extract doc_id and column
        let doc_id = parts.get(1).unwrap_or(&"").to_string();
        let column = parts.get(2).unwrap_or(&"").to_string();

        // Parse JSON value
        let value_str = String::from_utf8_lossy(&value);
        match serde_json::from_str::<Value>(&value_str) {
            Ok(json_value) => {
                // Update JSON data
                let entry = json_data.entry(doc_id).or_insert_with(HashMap::new);
                entry.insert(column, json_value);
            }
            Err(err) => {
                eprintln!("Error parsing value for key '{}': {}", key_str, err);
                // You may choose to handle the error differently, like skipping this entry
            }
        }
    }

    // Convert HashMap to JSON
    let json_output = serde_json::to_string_pretty(&json_data).unwrap();

    // Return JSON response
    HttpResponse::Ok()
        .content_type("application/json")
        .body(json_output)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    HttpServer::new(|| {
        App::new()
            .route("/get_json_data", web::get().to(get_json_data))
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}
