use actix_web::{delete, get, middleware::Logger, post, web::{self, get, put, resource, Data, Path}, App, HttpResponse, HttpServer};
use rocksdb::DB;
use serde_json::Value;

mod indexer;
use crate::indexer::{INDFunction, IndexError, Indexer};

#[post("/search")]
async fn search_index (body: web::Json::<Value>, data: Data<Indexer>) -> HttpResponse{
    let query = body.into_inner();
    let (mut column,mut index) = (String::new(),String::new());
    if let Value::Object(map) = query{
        match map.get("columnName"){
            Some (c) => {column = c.to_string().replace('"', "");}
            None => return HttpResponse::InternalServerError().finish(),
        }
        match map.get("value"){
            Some (i) => {index = i.to_string().replace('"',"");}
            None => return HttpResponse::InternalServerError().finish(),
        }
    }
    match data.search(column, index){
        Ok(json_map) => {
            let json_entries = serde_json::to_string(&json_map).unwrap();
            HttpResponse::Ok().content_type("application/json").body(json_entries)
        },
        Err(_) => HttpResponse::InternalServerError().finish()
    }
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
        Ok(()) => HttpResponse::Ok().content_type("application/json").body("data entry was succesful"),
        Err(_) => HttpResponse::InternalServerError().content_type("application/json").finish()
    }
}

#[delete("/remove/{key}")]
async fn delete_entry (key: Path<String>,data: Data<Indexer>) -> HttpResponse{
    let key = key.into_inner();
    data.delete(key);
    get_entries(data).await
}

#[actix_web::main]
async fn main() -> std::io::Result<()>{
    let db=DB::open_default("./data").unwrap();
    let data: indexer::Indexer = indexer::INDFunction::init(db);

    std::env::set_var("RUST_LOG", "actix_web=info,actix_server=info");
    env_logger::init();

    HttpServer::new(move || {
        App::new()
            .app_data(Data::new(data.clone()))
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