use std::{collections::HashMap, sync::Arc};
use rocksdb::DB;
use serde_json::Value;


pub trait INDFunction{
   fn init (file_path: &str) -> Self;
   fn get(&self, key:String) -> HashMap<String,Value>;
   fn get_all(&self) -> Vec<HashMap<String,Value>>;
   fn put(&self, body:Value) -> String;
   fn delete(&self, key:String);
}

#[derive(Clone)]
pub struct Indexer{
   pub db: Arc<DB>,
}

impl INDFunction for Indexer{
   fn init (file_path: &str) -> Self {
      Indexer { db: Arc::new(DB::open_default(file_path).unwrap()) }
   }

   fn get (&self, key:String) -> HashMap<String,Value>{
      let index = format!("R.{}",key);
      let mut json_one_entry: HashMap<String,Value> = HashMap::new();
      let iter = self.db.prefix_iterator(index);
      for item in iter {
         let (k,v) = item.unwrap();
         if String::from_utf8(k.to_vec()).unwrap().starts_with(&format!("R.{}",key)){
            let k_str = String::from_utf8(k.to_vec()).unwrap();
            let parts: Vec<&str> = k_str.split('.').collect();
            let column = parts.get(2).unwrap().to_string();
            let kind = parts.get(3).unwrap();
            match kind {
               &"s" | &"k" => {
                  let value_str = String::from_utf8_lossy(&v).to_string();
                  json_one_entry.insert(column, serde_json::Value::String(value_str));
               },
               &"n" => {
                  let value_n: f64 = f64::from_ne_bytes(v.into_vec().try_into().unwrap());
                  json_one_entry.insert(column, value_n.into());
               },
               &"b" => {
                  let value_b = match String::from_utf8(v.to_vec()).unwrap() == "true"{
                        true => {true},
                        false => {false}
                  };
                  json_one_entry.insert(column, serde_json::Value::Bool(value_b));
               },
               _ => {println!("undefined type")}
            };
         }
      }
      json_one_entry
   }

   fn get_all(&self) -> Vec<HashMap<String,Value>> {
      let mut json_map:Vec<HashMap<String, Value>> = Vec::new();
      let iter = self.db.prefix_iterator(format!("R."));
      let mut processed_keys:Vec<String> = Vec::new();
      for item in iter {
         let (k,_v) = item.unwrap();
         if String::from_utf8(k.to_vec()).unwrap().starts_with("R."){
            let k_str = String::from_utf8(k.to_vec()).unwrap();
            let parts: Vec<&str> = k_str.split('.').collect();
            let key_st = parts.get(1).unwrap().to_string();
            if processed_keys.contains(&key_st){
               continue;
            }
            else{
               let json_one_entry = self.get(key_st.clone());
               processed_keys.push(key_st.clone());
               json_map.push(json_one_entry);
            }
         }
      }
      json_map
   }
   
   fn put(&self,body: Value) -> String {
      let mut key = String::new();
      let mut exist: bool= false;
      if let Value::Object(map) = body{
         key = map.get("key").unwrap().to_string();
         let iter = self.db.prefix_iterator(format!("R.{}",key));
         for item in iter {
            let (k,_v) = item.unwrap();
            if String::from_utf8(k.to_vec()).unwrap().starts_with(&format!("R.{}",key)){
               exist = true;
            }
         }
         if exist {
            let iter = self.db.prefix_iterator(format!("S."));
            for item in iter{
               let (k,_v) = item.unwrap();
               let k_str = String::from_utf8(k.to_vec()).unwrap();
               if k_str.ends_with(&key){
                     let _ = self.db.delete(k);
               }
            }
         }
         self.db.put(format!("R.{}.key.k",key).as_bytes(), key.as_bytes()).unwrap();
         if let Value::Object(object) =  map.get("value").unwrap(){
            for (obj_k,obj_v) in object.into_iter(){
               println!("{obj_k}");
               let db_key = format!("R.{}.{}",key,obj_k.to_string());
               match obj_v{
                     Value::Null => {
                        println!("got null value");
                        self.db.put(db_key.as_bytes(), "".as_bytes()).expect("failed to put null value");
                     },
                     Value::Bool(b) => {
                        println!("got a bool");
                        self.db.put(format!("{}.b",db_key).as_bytes(), b.to_string()).expect("failed to put bool");
                        self.db.put(format!("S.{}.{}.{}",obj_k.to_string(),b.to_string(),key).as_bytes(),"".as_bytes())
                           .expect("failed to put reverse bool index");
                     },
                     Value::Number(nb) => {
                        println!("got a number");
                        self.db.put(format!("{}.n",db_key).as_bytes(), f64::to_ne_bytes(nb.as_f64().expect("failed to transform into f64")))
                           .expect("failed to save number to db");
                        self.db.put(format!("S.{}.{}.{}",obj_k.to_string(),nb.as_f64().expect("failed to convert f64 to byte"),key)
                           .as_bytes(),"".as_bytes())
                           .expect("failed to put reverse number index");
                     },
                     Value::String(str) => {
                        println!("got string");
                        self.db.put(format!("{}.s",db_key).as_bytes(), str.to_string()).expect("failed to save string to db");
                        self.db.put(format!("S.{}.{}.{}",obj_k.to_string(),str.to_string(),key).as_bytes(),"".as_bytes())
                           .expect("failed to put reverse string index");
                     },
                     _ => {}
               };
            }        
         }
      }
      key
   }

   fn delete(&self, key:String){
      let del = format!("R.{}",key);
      let iter = self.db.prefix_iterator(del.as_bytes());
      for item in iter {
         let (k,_v) = item.unwrap();
         let k_str = String::from_utf8(k.to_vec()).unwrap();
         if k_str.starts_with(&del) || k_str.ends_with(&key){
            let _ = self.db.delete(k);
         }   
      }
   }
}