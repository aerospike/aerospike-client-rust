// Copyright 2015-2017 Aerospike, Inc.
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

//! A pure-rust client for the Aerospike NoSQL database.
//!
//! ## Example
//!
//! The following is a very simple example of CRUD operations in an Aerospike database.
//!
//! ```rust
//! #[macro_use]
//! extern crate aerospike;
//!
//! use aerospike::*;
//! use std::env;
//! use std::sync::Arc;
//! use std::time::Instant;
//! use std::thread;
//!
//! fn main() {
//!     let cpolicy = ClientPolicy::default();
//!     let hosts = env::var("AEROSPIKE_HOSTS")
//!         .unwrap_or(String::from("127.0.0.1:3000"));
//!     let client = Client::new(&cpolicy, &hosts)
//!         .expect("Failed to connect to cluster");
//!     let client = Arc::new(client);
//!
//!     let mut threads = vec![];
//!     let now = Instant::now();
//!     for i in 0..2 {
//!         let client = client.clone();
//!         let t = thread::spawn(move || {
//!             let rpolicy = ReadPolicy::default();
//!             let wpolicy = WritePolicy::default();
//!             let key = as_key!("test", "test", i);
//!             let wbin = as_bin!("bin999", 1);
//!             let bins = vec![&wbin];
//!
//!             client.put(&wpolicy, &key, &bins).unwrap();
//!             let rec = client.get(&rpolicy, &key, None);
//!             println!("Record: {}", rec.unwrap());
//!
//!             client.touch(&wpolicy, &key).unwrap();
//!             let rec = client.get(&rpolicy, &key, None);
//!             println!("Record: {}", rec.unwrap());
//!
//!             let rec = client.get_header(&rpolicy, &key);
//!             println!("Record Header: {}", rec.unwrap());
//!
//!             let exists = client.exists(&wpolicy, &key).unwrap();
//!             println!("exists: {}", exists);
//!
//!             let ops = &vec![Operation::put(&wbin), Operation::get()];
//!             let op_rec = client.operate(&wpolicy, &key, ops);
//!             println!("operate: {}", op_rec.unwrap());
//!
//!             let existed = client.delete(&wpolicy, &key).unwrap();
//!             println!("existed (sould be true): {}", existed);
//!
//!             let existed = client.delete(&wpolicy, &key).unwrap();
//!             println!("existed (should be false): {}", existed);
//!         });
//!
//!         threads.push(t);
//!     }
//!
//!     for t in threads {
//!         t.join();
//!     }
//!
//!     println!("total time: {:?}", now.elapsed());
//! }
//! ```

// `error_chain` can recurse deeply
#![recursion_limit = "1024"]

#[macro_use]
extern crate log;
extern crate byteorder;
extern crate crypto;
extern crate rustc_serialize;
extern crate crossbeam;
extern crate rand;
extern crate threadpool;
extern crate pwhash;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate error_chain;

pub use errors::*;
pub use value::Value;
pub use policy::{Policy, ClientPolicy, ReadPolicy, WritePolicy, Priority, ConsistencyLevel,
                 CommitLevel, RecordExistsAction, GenerationPolicy, ScanPolicy, QueryPolicy};
pub use net::{Host, Connection};
pub use client::{Client, ResultCode};
pub use common::{Key, Bin, Operation, UDFLang, Recordset, Statement, Filter, IndexType,
                 CollectionIndexType, ParticleType, MapPolicy, MapReturnType};

#[macro_use]
pub mod errors;

mod command;
mod msgpack;

pub mod common;
pub mod value;
pub mod policy;
pub mod net;
pub mod cluster;
pub mod client;

#[cfg(test)]
extern crate hex;
