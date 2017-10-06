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

#![warn(missing_docs)]
#![doc(test(attr(allow(unused_variables), allow(unused_assignments), allow(unused_mut),
                 allow(unused_attributes), allow(dead_code), deny(warnings))))]

//! A pure-rust client for the Aerospike NoSQL database.
//!
//! Aerospike is an enterprise-class, NoSQL database solution for real-time operational
//! applications, delivering predictable performance at scale, superior uptime, and high
//! availability at the lowest TCO compared to first-generation NoSQL and relational databases. For
//! more information please refer to https://www.aerospike.com/.
//!
//! # Installation
//!
//! Add this to your `Cargo.toml`:
//!
//! ```text
//! [dependencies]
//! aerospike = "0.1.0"
//! ```
//!
//! # Examples
//!
//! The following is a very simple example of CRUD operations in an Aerospike database.
//!
//! ```rust
//! #[macro_use]
//! extern crate aerospike;
//!
//! use std::env;
//! use std::sync::Arc;
//! use std::time::Instant;
//! use std::thread;
//!
//! use aerospike::{Bins, Client, ClientPolicy, ReadPolicy, WritePolicy};
//! use aerospike::operations;
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
//!             let rec = client.get(&rpolicy, &key, Bins::All);
//!             println!("Record: {}", rec.unwrap());
//!
//!             client.touch(&wpolicy, &key).unwrap();
//!             let rec = client.get(&rpolicy, &key, Bins::All);
//!             println!("Record: {}", rec.unwrap());
//!
//!             let rec = client.get(&rpolicy, &key, Bins::None);
//!             println!("Record Header: {}", rec.unwrap());
//!
//!             let exists = client.exists(&wpolicy, &key).unwrap();
//!             println!("exists: {}", exists);
//!
//!             let ops = &vec![operations::put(&wbin), operations::get()];
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
//!         t.join().unwrap();
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
extern crate pwhash;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate error_chain;
extern crate parking_lot;
extern crate scoped_pool;

pub use batch::BatchRead;
pub use bin::{Bin, Bins};
pub use client::Client;
pub use errors::{Error, ErrorKind, Result};
pub use key::Key;
pub use net::Host;
pub use operations::{MapPolicy, MapReturnType, MapWriteMode};
pub use policy::{Policy, BatchPolicy, Concurrency, ClientPolicy, ReadPolicy, WritePolicy,
                 ScanPolicy, QueryPolicy, Priority, ConsistencyLevel, CommitLevel,
                 RecordExistsAction, GenerationPolicy, Expiration};
pub use query::{Statement, UDFLang, Recordset, IndexType, CollectionIndexType};
pub use record::Record;
pub use result_code::ResultCode;
pub use user::User;
pub use value::{Value, FloatValue};

#[macro_use]
pub mod errors;
#[macro_use]
mod value;
#[macro_use]
mod bin;
#[macro_use]
mod key;
mod batch;
mod client;
mod cluster;
mod commands;
mod msgpack;
mod net;
pub mod operations;
pub mod policy;
pub mod query;
mod record;
mod result_code;
mod user;

#[cfg(test)]
extern crate hex;
