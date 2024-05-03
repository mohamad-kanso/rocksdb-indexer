use std::{collections::HashMap, sync::{Arc, Mutex}, thread};
use actix_web::{delete, get, web::{self, get, put, resource, Data, Path}, middleware::Logger, App, HttpRequest, HttpResponse, HttpServer};
use rocksdb::DB;
use serde_json::Value;
use qstring::QString;

mod indexer;
use crate::indexer::{INDFunction, IndexError, Indexer};

#[get("/search")]
async fn search_index (req: HttpRequest, data: Data<Indexer>) -> HttpResponse{
    let query_str = req.query_string();
    let qs = QString::from(query_str);
    let q = qs.into_pairs();
    let (column,index) = q.get(0).unwrap().to_owned();
    let json_map: Arc<Mutex<Vec<HashMap<String,Value>>>> = Arc::new(Mutex::new(Vec::new()));
    let keys = data.search(column,index).unwrap();
    let num_workers = 4;
    let mut tasks = Vec::with_capacity(num_workers);
    let slice_size = keys.len()/num_workers;
    for i in 0..num_workers{
        let start = i * slice_size;
        let end = start + slice_size;
        let slice: Vec<String> = keys[start..end].to_vec();
        let data = data.clone();
        let json_map = json_map.clone();
        let task = thread::spawn(move ||{
            for key in slice{
                let j = data.get(key).unwrap();
                json_map.lock().unwrap().push(j)
            }
        });
        tasks.push(task); 
    }
    for task in tasks{
        task.join().unwrap();
    }
    let json_map = json_map.lock().unwrap().to_owned();
    let json_entries = serde_json::to_string(&json_map).unwrap();
    HttpResponse::Ok().content_type("application/json").body(json_entries)
}

#[get("/key/{key}")]
async fn get_by_key (key:Path<String>, data: Data<Indexer>) -> HttpResponse{
    let key = key.into_inner();
    match data.get(key){
        Ok(j) => HttpResponse::Ok().json(j),
        Err(IndexError::KeyNotFound) => HttpResponse::NotFound().content_type("application/json").finish(),
        Err(_) => HttpResponse::InternalServerError().content_type("application/json").finish()
    }
}

async fn get_entries(data: Data<Indexer>) -> HttpResponse{
    let json_map = data.get_all();
    let json_entry: String = serde_json::to_string(&json_map).unwrap();
    HttpResponse::Ok().content_type("application/json").body(json_entry)
}

async fn put_entry(data: Data<Indexer>, body: web::Json::<Value>) -> HttpResponse{
    let body = body.into_inner();
    match data.put(body){
        Ok(()) => HttpResponse::Ok().content_type("application/json").finish(),
        Err(_) => HttpResponse::InternalServerError().content_type("application/json").finish()
    }
}

#[delete("/remove/{key}")]
async fn delete_entry (key: Path<String>,data: Data<Indexer>) -> HttpResponse{
    let key = key.into_inner();
    data.delete(key);
    get_entries(data).await
}

#[allow(deprecated)]
#[actix_web::main]
async fn main() -> std::io::Result<()>{
    let db=DB::open_default("./data").unwrap();
    let data: indexer::Indexer = indexer::INDFunction::init(db);

    std::env::set_var("RUST_LOG", "actix_web=info,actix_server=info");
    env_logger::init();

    HttpServer::new(move || {
        App::new()
            .data(data.clone())
            .wrap(Logger::default())
            .service(
                resource("/")
                .route(get().to(get_entries))
                .route(put().to(put_entry))
            )
            .service(search_index)
            .service(delete_entry)
            .service(get_by_key)
    })
    .bind("0.0.0.0:7878")?
    .run()
    .await
}