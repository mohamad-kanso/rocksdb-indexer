use rocksdb::DB;
use serde::{Serialize,Deserialize};
#[derive(Serialize, Deserialize)]
pub struct User{
   pub id: String,
   pub name:String,
   pub address:String
}
pub fn create_database(path:&str) -> DB {
   //Defining options
   let mut options = rocksdb::Options::default();
   options.set_error_if_exists(false);
   options.create_if_missing(true);
   options.create_missing_column_families(true);

   //opening database
   let cfs = rocksdb::DB::list_cf(&options, "./database").unwrap_or(vec![]);
   let data = rocksdb::DB::open_cf(&options, path, cfs.clone()).unwrap();
   
   let hasan = r#"{
       "id": "1",
       "name": "Nazeer Issa",
       "address": "Beirut"
   }"#;
   let mohamad_k = r#"{
       "id": "2",
       "name": "Mohamad Kanso",
       "address": "Baalbck"
   }"#;
   let hanin = r#"{
       "id": "3",
       "name": "Hanin Matar",
       "address":"Jbeil"
   }"#;
   let mohamad_a = r#"{
       "id": "4",
       "name": "Mohamad Ali",
       "address": "Beirut"
   }"#;
   let mut users: Vec<User> = Vec::new();
   let u1: User = serde_json::from_str(hasan).unwrap();
   let u2: User = serde_json::from_str(mohamad_k).unwrap();
   let u3: User = serde_json::from_str(hanin).unwrap();
   let u4: User = serde_json::from_str(mohamad_a).unwrap();
   users.insert(0, u1);
   users.insert(1, u2);
   users.insert(2, u3);
   users.insert(3, u4);
   //Row indexing
   for item in users{
       data.put(format!("R.{}.id",item.id),item.id.clone()).unwrap();
       data.put(format!("R.{}.name",item.id),item.name.clone()).unwrap();
       data.put(format!("R.{}.address",item.id),item.address.clone()).unwrap();
       data.put(format!("C.name.{}",item.id),item.name.clone()).unwrap();
       data.put(format!("S.name.{}.{}",item.name,item.id),"").unwrap();
       data.put(format!("S.address.{}.{}",item.address,item.id),"").unwrap();
       //println!("{}\t{}\t{}",item.id,item.name,item.age);
   }
   data
}
// pub fn display_ages(data: &DB){
//    let age_iter = data.iterator(rocksdb::IteratorMode::Start);
//    for item in age_iter{
//       let (key,_value) = item.unwrap();
//       if String::from_utf8(key.to_vec()).unwrap().starts_with("C.age"){
//          let retrieved_age_bytes = data
//             .get(key)
//             .unwrap().unwrap();
//          println!("{}",i32::from_ne_bytes(retrieved_age_bytes
//             .as_slice().try_into().unwrap()));
//       }
//    }
//    println!();
// }
pub fn display_names(data: &DB){
   let name_iter = data.iterator(rocksdb::IteratorMode::Start);
   for item in name_iter {
      let (key,value) = item.unwrap();
      if String::from_utf8(key.to_vec()).unwrap().starts_with("C.name"){
         println!("{:?}",String::from_utf8(value.to_vec()).unwrap());
      }
   }
   println!();
}
pub fn display_entries(data: &DB){
   let display = data.iterator(rocksdb::IteratorMode::Start);
   for item in display{
      let (key,value) = item.unwrap();
      if String::from_utf8(key.to_vec()).unwrap().starts_with("R."){
         if String::from_utf8(key.to_vec()).unwrap().contains("age"){
            let retrieved_age_bytes = data
               .get(key)
               .unwrap().unwrap();
            println!("{}",i32::from_ne_bytes(retrieved_age_bytes
               .as_slice().try_into().unwrap()));
         }
         else{println!("{:?}",String::from_utf8(value.to_vec()).unwrap());}
      }
   }
   println!();
}
pub fn display_entry(data: &DB,id:&str){
   let s_key = format!("R.{}",id);
   let display = data.iterator(rocksdb::IteratorMode::Start);
   for item in display{
      let (key,value) = item.unwrap();
      if String::from_utf8(key.to_vec()).unwrap().starts_with(&s_key){
         if String::from_utf8(key.to_vec()).unwrap().contains("age"){
            let retrieved_age_bytes = data
               .get(key)
               .unwrap().unwrap();
            println!("{}",i32::from_ne_bytes(retrieved_age_bytes
               .as_slice().try_into().unwrap()));
         }
         else{println!("{:?}",String::from_utf8(value.to_vec()).unwrap());}
      }
   }
   println!();
}
pub fn delete_entry (data: &DB,id:&str){
   let r_key = format!("R.{}",id); 
   let r_iter = data
      .iterator(rocksdb::IteratorMode::Start);
   for item in r_iter{
      let (key,_value) = item.unwrap();
      if String::from_utf8(key.to_vec()).unwrap().starts_with(&r_key) ||
         String::from_utf8(key.to_vec()).unwrap().ends_with(id){
            data.delete(key).unwrap();
      }
   }
}
pub fn update_age(data: &DB,id:&str,n_age:i32){
   data.put(format!("C.age.{}",id),&n_age.to_ne_bytes()).unwrap();
   let old_age = data.get(format!("C.age.{}",id)).unwrap().unwrap();
   data.delete(format!("S.age.{}.{}",String::from_utf8(old_age.to_vec()).unwrap(),id)).unwrap();
   data.put(format!("S.age.{}.{}",n_age,id), "").unwrap();
   data.put(format!("R.{}.age",id), n_age.to_ne_bytes()).unwrap();
}
pub fn search_name(data: &DB,name:&str){
   let iter = data.iterator(rocksdb::IteratorMode::Start);
   for item in iter{
      let (key,_value) = item.unwrap();
      if String::from_utf8(key.to_vec()).unwrap().starts_with(&format!("S.name.{}",name)){
         let key = String::from_utf8(key.to_vec()).unwrap();
         let key_st = key.split('.').last().unwrap();
         println!("indexed key for given name is {key_st}");    
         display_entry(data,key_st);
      }
   }
}
pub fn search_age(data: &DB,age:i32){
   let iter = data.iterator(rocksdb::IteratorMode::Start);
   for item in iter{
      let (key,_value) = item.unwrap();
      if String::from_utf8(key.to_vec()).unwrap().starts_with(&format!("S.age.{}",age)){
         let key = String::from_utf8(key.to_vec()).unwrap();
         let key_st = key.split('.').last().unwrap();
         println!("indexed key for given age is {key_st}");    
         display_entry(data, key_st);
      }
   }
}