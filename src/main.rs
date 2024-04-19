use std::{collections::HashMap, sync::Arc};
use actix_web::{get, web::{self, get, resource, scope, Data}, App, HttpResponse, HttpServer};
use rocksdb_indexer::create_database;
use rocksdb::DB;
use serde::Deserialize;
use serde_json::Value;

#[derive(Deserialize)]
struct IndexPair{
    column: String,
    query:String
}
#[get("/{column}/{query}")]
async fn search_index (index: web::Path<IndexPair>, db: Data<Arc<DB>>) -> HttpResponse{
    let (column,query) = (index.column.clone(),index.query.clone());
    let mut json_map: Vec<HashMap<String,Value>> = Vec::new();
    let search = format!("S.{}.{}",column,query);
    let iter = db.prefix_iterator(search.as_bytes());
    for item in iter{
        let (key,_value) = item.unwrap();
        if String::from_utf8(key.to_vec()).unwrap().starts_with(&search){
            let k_str: String = String::from_utf8(key.to_vec()).unwrap();
            let key_st: String = k_str.split('.').last().unwrap().to_string();
            let mut json_one_entry: HashMap<String, Value> = HashMap::new();
            let p_iter = db.prefix_iterator(format!("R.{}",key_st).as_bytes());
            for item in p_iter {
                let (k,v) = item.unwrap();
                if String::from_utf8(k.to_vec()).unwrap().starts_with(&format!("R.{}",key_st)){
                    let k_str = String::from_utf8(k.to_vec()).unwrap();
                    let parts: Vec<&str> = k_str.split('.').collect();
                    let column = parts.get(2).unwrap().to_string();
                    let value_str = String::from_utf8_lossy(&v).to_string();
                    println!("{:?}", value_str);    
                    json_one_entry.insert(column, serde_json::Value::String(value_str));
                }
            }
            json_map.push(json_one_entry);
        }
    }
    let json_entries = serde_json::to_string(&json_map).unwrap();
    HttpResponse::Ok().content_type("application/json").body(json_entries)
}

async fn get_entries(db: Data<Arc<DB>>) -> HttpResponse{
    let mut json_map:Vec<HashMap<String, Value>> = Vec::new();
    let iter = db.prefix_iterator(format!("R."));
    let mut processed_keys:Vec<String> = Vec::new();
    for item in iter {
        let (k,_v) = item.unwrap();
        if String::from_utf8(k.to_vec()).unwrap().starts_with("R."){
            let k_str = String::from_utf8(k.to_vec()).unwrap();
            let parts: Vec<&str> = k_str.split('.').collect();
            let mut json_one_entry = HashMap::new();
            let key_st = parts.get(1).unwrap().to_string();
            let p_iter = db.prefix_iterator(format!("R.{}",key_st).as_bytes());
            if processed_keys.contains(&key_st){
                continue;
            }
            else{
                for item in p_iter {
                    let (k,v) = item.unwrap();
                    if String::from_utf8(k.to_vec()).unwrap().starts_with(&format!("R.{}",key_st)){
                        let k_str = String::from_utf8(k.to_vec()).unwrap();
                        let parts: Vec<&str> = k_str.split('.').collect();
                        let column = parts.get(2).unwrap().to_string();
                        let value_str = String::from_utf8_lossy(&v).to_string();
                        println!("{:?}", value_str);    
                        json_one_entry.insert(column, serde_json::Value::String(value_str));
                    }
                    processed_keys.push(key_st.clone());
                }
            }
            json_map.push(json_one_entry);
        }
    }

    let json_entry: String = serde_json::to_string(&json_map).unwrap();
    HttpResponse::Ok().content_type("application/json").body(json_entry)
}

// async fn put_entry(db: Data<Arc<DB>>, body: web::Json::<Value>) -> HttpResponse{
//     if let Value::Array(entries) = &body.0{
//         for entry in entries{
//             if let Value::Object(map) = entry{
//                 let mut k = String::new();
//                 for (key,value) in map.into_iter(){
//                     if key == "id" {
//                         k=value.to_string();
//                         println!("id is {k}");
//                     }
//                     let db_key = format!("R.{}.{}",k,key.to_string());
//                     match value{
//                         Value::Null => {},
//                         Value::Bool(b) => {
//                             println!("got a bool");
//                             db.put(db_key.as_bytes(), b.to_string()).expect("failed to put bool");
//                             db.put(format!("S.{}.{}")
//                         },
//                         Value::Number(nb) => {
//                             println!("got a number");
//                             db.put(db_key.as_bytes(), f64::to_ne_bytes(nb.as_f64().expect("failed to transform into f64"))).expect("failed to save number to db")
//                         },
//                         Value::String(str) => {
//                             println!("got string");
//                             db.put(db_key.as_bytes(), str.to_string()).expect("failed to save string to db")
//                         },
//                         Value::Array(_) => todo!(),
//                         Value::Object(_) => todo!(),
//                     };
//                 }
//             }
//         }
//     }

//     HttpResponse::Ok().body("body")
// }

#[allow(deprecated)]
#[actix_rt::main]
async fn main() -> std::io::Result<()>{
    let db = create_database("./database");
    let db = Arc::new(db);

    HttpServer::new(move || {
        App::new()
            .data(db.clone())
            .service(
                scope("/api")
                .service(
                    resource("/")
                        .route(get().to(get_entries))
                        // .route(put().to(put_entry))
                        
                )
                .service(search_index)
            )
    })
    .bind("0.0.0.0:7878")?
    .run()
    .await
}
