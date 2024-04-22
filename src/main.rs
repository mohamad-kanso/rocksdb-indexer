use std::{collections::HashMap, sync::Arc};
use actix_web::{get, web::{self, get, put, resource, scope, Data}, App, HttpRequest, HttpResponse, HttpServer};
use rocksdb_indexer::create_database;
use rocksdb::DB;
use serde_json::Value;
use qstring::QString;

#[get("/search")]
async fn search_index (req: HttpRequest, db: Data<Arc<DB>>) -> HttpResponse{
    let query_str = req.query_string();
    let qs = QString::from(query_str);
    let q = qs.into_pairs();
    let mut json_map: Vec<HashMap<String,Value>> = Vec::new();
    let search = format!("S.{}.{}", q.get(0).unwrap().0,q.get(0).unwrap().1);
    let iter = db.prefix_iterator(search.as_bytes());
    for item in iter{
        let (key,_value) = item.unwrap();
        if String::from_utf8(key.to_vec()).unwrap().starts_with(&search){
            let k_str: String = String::from_utf8(key.to_vec()).unwrap();
            let key_st: String = k_str.split('.').last().unwrap().to_string();
            let json_entry = get_entry(key_st, &db);
            json_map.push(json_entry);
        }
    }
    let json_entries = serde_json::to_string(&json_map).unwrap();
    HttpResponse::Ok().content_type("application/json").body(json_entries)
}

fn get_entry (key: String, db: &Data<Arc<DB>>) -> HashMap<String,Value>{
    let mut json_one_entry: HashMap<String, Value> = HashMap::new();
        let p_iter = db.prefix_iterator(format!("R.{}",key).as_bytes());
        for item in p_iter {
            let (k,v) = item.unwrap();
            if String::from_utf8(k.to_vec()).unwrap().starts_with(&format!("R.{}",key)){
                let k_str = String::from_utf8(k.to_vec()).unwrap();
                let parts: Vec<&str> = k_str.split('.').collect();
                let column = parts.get(2).unwrap().to_string();
                let value_str = String::from_utf8_lossy(&v).to_string();
                json_one_entry.insert(column, serde_json::Value::String(value_str));
            }
        }
    json_one_entry
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

async fn put_entry(db: Data<Arc<DB>>, body: web::Json::<Value>) -> HttpResponse{
    let mut identifier = String::new();
    if let Value::Object(map) = &body.0{
        for (key,value) in map.into_iter(){
            if key == "identifier" {identifier = value.to_string();}
            println!("{identifier}");
        }
        println!("{}",&body.0);
        for (k,v) in map.into_iter(){
            println!("{k}");
            if k != "identifier"{
                if let Value::Object(object) = v{
                    for (obj_k,obj_v) in object.into_iter(){
                        println!("{obj_k}");
                        let db_key = format!("R.{}.{}",identifier,obj_k.to_string());
                        match obj_v{
                            Value::Null => {
                                println!("got null value");
                                db.put(db_key.as_bytes(), "".as_bytes()).expect("failed to put null value");
                            },
                            Value::Bool(b) => {
                                println!("got a bool");
                                db.put(db_key.as_bytes(), b.to_string()).expect("failed to put bool");
                                db.put(format!("S.{}.{}.{}",obj_k.to_string(),b.to_string(),identifier).as_bytes(),"".as_bytes())
                                    .expect("failed to put reverse bool index");
                            },
                            Value::Number(nb) => {
                                println!("got a number");
                                db.put(db_key.as_bytes(), f64::to_ne_bytes(nb.as_f64().expect("failed to transform into f64")))
                                    .expect("failed to save number to db");
                                db.put(format!("S.{}.{}.{}",obj_k.to_string(),nb.as_f64().expect("failed to convert f64 to byte"),identifier)
                                    .as_bytes(),"".as_bytes())
                                    .expect("failed to put reverse number index");
                            },
                            Value::String(str) => {
                                println!("got string");
                                db.put(db_key.as_bytes(), str.to_string()).expect("failed to save string to db");
                                db.put(format!("S.{}.{}.{}",obj_k.to_string(),str.to_string(),identifier).as_bytes(),"".as_bytes())
                                    .expect("failed to put reverse string index");
                            },
                            _ => {}
                        };
                    }
                }        
            }
        }
    }
    let j = get_entry(identifier, &db);
    HttpResponse::Ok().json(j)
}

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
                        .route(put().to(put_entry))
                        
                )
            )
            .service(search_index)
    })
    .bind("0.0.0.0:7878")?
    .run()
    .await
}
