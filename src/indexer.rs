use std::{collections::HashMap, sync::Arc};
use rocksdb::DB;
use serde_json::Value;


pub trait INDFunction{
   fn init (data: DB) -> Self;
   fn get(&self, key:String) -> Result<HashMap<String,Value>,IndexError>;
   fn get_all(&self) -> Vec<HashMap<String,Value>>;
   fn put(&self, body:Value) -> Result<(),IndexError>;
   fn delete(&self, key:String);
   fn search(&self, column:String, index:String) -> Result<Vec<String>,IndexError>;
}

#[derive(Clone)]
pub struct Indexer{
   pub db: Arc<DB>,
}

#[derive(Debug,PartialEq)]
pub enum IndexError {
   InvalidInput,
   KeyNotFound,
}
 
impl INDFunction for Indexer{
   fn init (data: DB) -> Self{
      Indexer {db: Arc::new(data)}
   }

   fn get (&self, key:String) -> Result<HashMap<String,Value>,IndexError>{
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
      match json_one_entry.is_empty(){
         true => {return Err(IndexError::KeyNotFound)},
         false => {Ok(json_one_entry)}
      }
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
               let json_one_entry = self.get(key_st.clone()).unwrap();
               processed_keys.push(key_st.clone());
               json_map.push(json_one_entry);
            }
         }
      }
      json_map
   }
   
   #[allow(unused_assignments)]
   fn put(&self,body: Value) -> Result<(),IndexError> {
      let mut key = String::new();
      match body.to_string() == "{}"{
         true => return Err(IndexError::InvalidInput),
         false => {}
      }
      let mut exist: bool= false;
      if let Value::Object(map) = body{
         match map.get("key"){
            Some(k) => {key = k.to_string();},
            None => return Err(IndexError::InvalidInput) 
         }
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
         match map.get("value") {
            Some(body) => if let Value::Object(object) = body{
               for (obj_k,obj_v) in object.into_iter(){
                  let db_key = format!("R.{}.{}",key,obj_k.to_string());
                  match obj_v{
                        Value::Null => {
                           self.db.put(db_key.as_bytes(), "".as_bytes()).expect("failed to put null value");
                        },
                        Value::Bool(b) => {
                           self.db.put(format!("{}.b",db_key).as_bytes(), b.to_string()).expect("failed to put bool");
                           self.db.put(format!("S.{}.{}.{}",obj_k.to_string(),b.to_string(),key).as_bytes(),"".as_bytes())
                              .expect("failed to put reverse bool index");
                        },
                        Value::Number(nb) => {
                           self.db.put(format!("{}.n",db_key).as_bytes(), f64::to_ne_bytes(nb.as_f64().expect("failed to transform into f64")))
                              .expect("failed to save number to db");
                           self.db.put(format!("S.{}.{}.{}",obj_k.to_string(),nb.as_f64().expect("failed to convert f64 to byte"),key)
                              .as_bytes(),"".as_bytes())
                              .expect("failed to put reverse number index");
                        },
                        Value::String(str) => {
                           self.db.put(format!("{}.s",db_key).as_bytes(), str.to_string()).expect("failed to save string to db");
                           self.db.put(format!("S.{}.{}.{}",obj_k.to_string(),str.to_string(),key).as_bytes(),"".as_bytes())
                              .expect("failed to put reverse string index");
                        },
                        _ => {}
                  };
               }
            },
            None => return Err(IndexError::InvalidInput)
         }
      }
      Ok(())
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

   fn search(&self, column:String, index:String) -> Result<Vec<String>,IndexError> {
      let search = format!("S.{}.{}", column, index);
      let mut keys = vec![];
      let iter = self.db.prefix_iterator(search.as_bytes());
      for item in iter{
         let (key,_value) = item.unwrap();
         if String::from_utf8(key.to_vec()).unwrap().starts_with(&search){
            let k_str: String = String::from_utf8(key.to_vec()).unwrap();
            let key_st: String = k_str.split('.').last().unwrap().to_string();
            keys.push(key_st);
         }
      }
      match keys.is_empty(){
         true => return Err(IndexError::KeyNotFound),
         false => Ok(keys)
      }
   }
}

#[cfg(test)]
mod tests{
   use std::fs::File;
   use rstest::rstest;

   use crate::indexer;

   use super::*;

   fn initialize() -> DB{
      let db = DB::open_default("./tmp").unwrap();
      db
   }
 
   #[rstest]
   #[case("json_test_files/empty_json.json")]
   #[case("json_test_files/json_key_only.json")]
   #[case("json_test_files/no_key.json")]
   fn putting_invalid_json(#[case] p: String){
      let data: indexer::Indexer = indexer::INDFunction::init(initialize());
      let file = File::open(p).unwrap();
      let body: Value = serde_json::from_reader(file).unwrap();
      assert_eq!(data.put(body).unwrap_err(),IndexError::InvalidInput)
   }

   #[test]
   fn putting(){
      let data: indexer::Indexer = indexer::INDFunction::init(initialize());
      let file = File::open("json_test_files/example.json").unwrap();
      let body: Value = serde_json::from_reader(file).unwrap();
      assert!(data.put(body).is_ok());
      assert!(data.get(String::from("5")).is_ok())
   }

   #[test]
   fn getting_empty(){
      let data: indexer::Indexer = indexer::INDFunction::init(initialize());
      let file = File::open("json_test_files/example.json").unwrap();
      let body: Value = serde_json::from_reader(file).unwrap();
      let _ = data.put(body).unwrap();
      assert!(data.get("5".to_string()).is_ok());
      assert_eq!(data.get(String::from("999")).unwrap_err(),IndexError::KeyNotFound)
   }

   #[test]
   fn deleting(){
      let data: indexer::Indexer = indexer::INDFunction::init(initialize());
      let file = File::open("json_test_files/example.json").unwrap();
      let body: Value = serde_json::from_reader(file).unwrap();
      let _ = data.put(body).unwrap();
      assert!(data.get("5".to_string()).is_ok());
      let _ = data.delete("5".to_string());
      assert_eq!(data.get("5".to_string()).unwrap_err(),IndexError::KeyNotFound)
   }

   #[test]
   fn searcing(){
      let data: indexer::Indexer = indexer::INDFunction::init(initialize());
      let file = File::open("json_test_files/example.json").unwrap();
      let body: Value = serde_json::from_reader(file).unwrap();
      let _ = data.put(body).unwrap();
      assert!(data.get("5".to_string()).is_ok());
      assert!(data.search("name".to_string(), "Ali".to_string()).is_ok())
   }

   #[test]
   fn seaching_not_found(){
      let data: indexer::Indexer = indexer::INDFunction::init(initialize());
      let file = File::open("json_test_files/example.json").unwrap();
      let body: Value = serde_json::from_reader(file).unwrap();
      let _ = data.put(body).unwrap();
      assert!(data.get("5".to_string()).is_ok());
      assert_eq!(data.search("column".to_string(), "kkk".to_string()).unwrap_err(),IndexError::KeyNotFound)
   }
}