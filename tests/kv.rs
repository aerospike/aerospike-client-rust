// Copyright 2015-2016 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

#[macro_use]
extern crate aerospike;
extern crate env_logger;

use std::collections::HashMap;

use aerospike::{Client, Host};
use aerospike::{ClientPolicy, ReadPolicy, WritePolicy};
use aerospike::{Key, Bin};
use aerospike::{Operation};
use aerospike::{UDFLang};
use aerospike::value::*;

// use log::LogLevel;
// use env_logger;

// use std::collections::{HashMap, VecDeque};
use std::sync::{RwLock, Arc, Mutex};
// use std::vec::Vec;
use std::thread;
use std::time::{Instant, Duration};


// #[test]
// fn connect() {
//     env_logger::init();

//     let cpolicy = ClientPolicy::default();
//     let client: Arc<Client> = Arc::new(Client::new(&cpolicy, &vec![Host::new("ubvm", 3000)]).unwrap());

//     // let t: i64 = 1;
//     // let key = Key::new("ns", "set", Value::from(t));
//     // let key = as_key!("ns", "set", t);
//     // let key = as_key!("ns", "set", &t);
//     // let key = as_key!("ns", "set", 1);
//     // let key = as_key!("ns", "set", &1);
//     // let key = as_key!("ns", "set", 1i8);
//     // let key = as_key!("ns", "set", &1i8);
//     // let key = as_key!("ns", "set", 1u8);
//     // let key = as_key!("ns", "set", &1u8);
//     // let key = as_key!("ns", "set", 1i16);
//     // let key = as_key!("ns", "set", &1i16);
//     // let key = as_key!("ns", "set", 1u16);
//     // let key = as_key!("ns", "set", &1u16);
//     // let key = as_key!("ns", "set", 1i32);
//     // let key = as_key!("ns", "set", &1i32);
//     // let key = as_key!("ns", "set", 1u32);
//     // let key = as_key!("ns", "set", &1u32);
//     // let key = as_key!("ns", "set", 1i64);
//     // let key = as_key!("ns", "set", &1i64);
//     // // let key = as_key!("ns", "set", 1.0f32);
//     // // let key = as_key!("ns", "set", &1.0f32);
//     // let key = as_key!("ns", "set", 1.0f64);
//     // let key = as_key!("ns", "set", &1.0f64);

//     // let key = as_key!("ns", "set", "haha");
//     // let key = as_key!("ns", "set", "haha".to_string());
//     // let key = as_key!("ns", "set", &"haha".to_string());


//     // let mut threads = vec![];
//     // let now = Instant::now();
//     // for _ in 0..1 {
//     // 	let client = client.clone();
// 	   //  let t = thread::spawn(move || {
// 		    let policy = ReadPolicy::default();

// 		    let wpolicy = WritePolicy::default();
// 		    let key = as_key!("test", "test", -1);
// 		    let wbin = as_bin!("bin999", "test string");
// 		    let wbin1 = as_bin!("bin vec![int]", as_list![1u32, 2u32, 3u32]);
// 		    let wbin2 = as_bin!("bin vec![u8]", as_blob!(vec![1u8, 2u8, 3u8]));
// 		    let wbin3 = as_bin!("bin map", as_map!(1 => 1, 2 => 2, 3 => "hi!"));
// 		    let wbin4 = as_bin!("bin f64", 1.64f64);
// 		    let wbin5 = as_bin!("bin Nil", None);
// 		    let wbin6 = as_bin!("bin Geo", as_geo!(format!("{{ \"type\": \"Point\", \"coordinates\": [{}, {}] }}", 17.119381, 19.45612)));
// 		    let bins = vec![&wbin, &wbin1, &wbin2, &wbin3, &wbin4, &wbin5, &wbin6];

// 			client.delete(&wpolicy, &key).unwrap();


// 			client.put(&wpolicy, &key, &bins).unwrap();
// 		    let rec = client.get(&policy, &key, None);
// 		    println!("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ {}", rec.unwrap());

// 			// client.touch(&wpolicy, &key).unwrap();
// 		 //    let rec = client.get(&policy, &key, None);
// 		 //    println!("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ {}", rec.unwrap());

// 		 //    let rec = client.get_header(&policy, &key);
// 		 //    println!("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@/// {}", rec.unwrap());

// 			// let exists = client.exists(&wpolicy, &key).unwrap();
// 			// println!("exists: {}", exists);

// 			// let ops = &vec![Operation::put(&wbin), Operation::get()];
// 			// let op_rec = client.operate(&wpolicy, &key, ops);
// 			// println!("operate: {}", op_rec.unwrap());

// 			// let existed = client.delete(&wpolicy, &key).unwrap();
// 			// println!("existed: {}", existed);

// 			// let existed = client.delete(&wpolicy, &key).unwrap();
// 			// println!("existed: {}", existed);
// 	// 	});
// 	// 	threads.push(t);
// 	// }

// 	// for t in threads {
// 	// 	t.join();
// 	// }
// 	// println!("total time: {:?}", now.elapsed());

// 	// struct T(i64);
// 	// struct TN{N: i64};

//  //    let now = Instant::now();
// 	// for _ in 0..10_000_000 {
// 	// 	    let wbin = 1;
// 	// }
// 	// println!("total time: {:?}", now.elapsed());

//  //    let now = Instant::now();
// 	// for _ in 0..10_000_000 {
// 	// 	    let wbin = T(1);
// 	// }
// 	// println!("total time: {:?}", now.elapsed());

//  //    let now = Instant::now();
// 	// for _ in 0..10_000_000 {
// 	// 	    let wbin = TN{N:1};
// 	// }
// 	// println!("total time: {:?}", now.elapsed());
// }

// #[test]
// fn perf() {
//     env_logger::init();

//     let cpolicy = ClientPolicy::default();
//     let client: Arc<Client> = Arc::new(Client::new(&cpolicy, &vec![Host::new("ubvm", 3000)]).unwrap());

//     let wpolicy = WritePolicy::default();
//     let key = as_key!("test", "test", 1);
//     let wbin = as_bin!("bin666", 1);
//     let bins = vec![&wbin];
// 	client.put(&wpolicy, &key, &bins).unwrap();

//     let policy = ReadPolicy::default();
//     let key = as_key!("test", "test", 1);
//     for i in 1..10_000 {
// 	    let rec = client.get(&policy, &key, None);
// 	}
// }

#[test]
fn udf() {
    env_logger::init();

    let cpolicy = ClientPolicy::default();
    let client: Arc<Client> = Arc::new(Client::new(&cpolicy, &vec![Host::new("ubvm", 3000)]).unwrap());

    let wpolicy = WritePolicy::default();
    let key = as_key!("test", "test", 1);
    let wbin = as_bin!("bin", 10);
    let bins = vec![&wbin];
	client.put(&wpolicy, &key, &bins).unwrap();

	let udf_body1 = "function func_div(rec, div)
	   local ret = map()

	   local x = rec['bin']

	   rec['bin2'] = math.floor(x / div)

	   aerospike:update(rec)

	   ret['status'] = 'OK'
	   ret['res'] = math.floor(x / div)
	   return ret
	end";

	let udf_body2 = "function echo(rec, val)
	   return val
	end";

	client.register_udf(&wpolicy, udf_body1.as_bytes(), "test_udf1.lua", UDFLang::Lua).unwrap();
	client.register_udf(&wpolicy, udf_body2.as_bytes(), "test_udf2.lua", UDFLang::Lua).unwrap();

	let res = client.execute_udf(&wpolicy, &key, "test_udf2", "echo", Some(&as_list!["ha ha..."]));
	println!("{:?}", res);

	let res = client.execute_udf(&wpolicy, &key, "test_udf1", "func_div", Some(&as_list![2]));
	println!("{:?}", res);
}
