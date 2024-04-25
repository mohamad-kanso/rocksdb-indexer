use std::collections::HashMap;
use actix_web::{delete, get, put, web::{self, get, resource, scope, Data, Path}, App, HttpRequest, HttpResponse, HttpServer};
use serde_json::Value;
use qstring::QString;
use tokio;


mod indexer;
use crate::indexer::{INDFunction,Indexer};

#[get("/search")]
async fn search_index (req: HttpRequest, data: Data<Indexer>) -> HttpResponse{
    let query_str = req.query_string();
    let qs = QString::from(query_str);
    let q = qs.into_pairs();
    let (column,index) = q.get(0).unwrap().to_owned();
    let mut json_map: Vec<HashMap<String,Value>> = Vec::new();
    let keys = data.search(column,index);
    for key in keys {
        let j = data.get(key);
        json_map.push(j)
    }
    let json_entries = serde_json::to_string(&json_map).unwrap();
    HttpResponse::Ok().content_type("application/json").body(json_entries)
}

#[get("/key/{key}")]
async fn get_by_key (key:Path<String>, data: Data<Indexer>) -> HttpResponse{
    let key = key.into_inner();
    let j = data.get(key);
    HttpResponse::Ok().json(j)
}

async fn get_entries(data: Data<Indexer>) -> HttpResponse{
    let json_map = data.get_all();
    let json_entry: String = serde_json::to_string(&json_map).unwrap();
    println!("Getting entries");
    HttpResponse::Ok().content_type("application/json").body(json_entry)
}

#[put("/")]
async fn put_entry(data: Data<Indexer>, body: web::Json::<Value>) -> HttpResponse{
    let body = body.into_inner();
    let key = data.put(body);
    let j = data.get(key);
    HttpResponse::Ok().json(j)
}

#[delete("/remove/{key}")]
async fn delete_entry (key: Path<String>,data: Data<Indexer>) -> HttpResponse{
    let key = key.into_inner();
    data.delete(key);
    get_entries(data).await
}


#[allow(deprecated)]
#[tokio::main]
async fn main() -> std::io::Result<()>{
    let db: indexer::Indexer = indexer::INDFunction::init("./data");

    HttpServer::new(move || {
        App::new()
            .data(db.clone())
            .service(
                scope("/api")
                .service(
                    resource("/")
                        .route(get().to(get_entries))
                )
            )
            .service(put_entry)
            .service(search_index)
            .service(delete_entry)
            .service(get_by_key)
    })
    .bind("0.0.0.0:7878")?
    .run()
    .await
}