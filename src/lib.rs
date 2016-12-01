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

//! ## Example
//!
//! The following is a very simple example of CRUD operations in an Aerospike database.
//!
//! ```rust,no_run
//! #[macro_use]
//! extern crate aerospike;
//!
//! use aerospike::*;
//! use std::sync::Arc;
//! use std::time::Instant;
//! use std::thread;
//!
//! fn main() {
//!     let cpolicy = ClientPolicy::default();
//!     let client: Arc<Client> = Arc::new(Client::new(&cpolicy, &vec![Host::new("127.0.0.1", 3000)]).unwrap());
//!
//!     let mut threads = vec![];
//!     let now = Instant::now();
//!     for i in 0..2 {
//!         let client = client.clone();
//!         let t = thread::spawn(move || {
//!             let policy = ReadPolicy::default();
//!             let wpolicy = WritePolicy::default();
//!             let key = as_key!("test", "test", i);
//!             let wbin = as_bin!("bin999", 1);
//!             let bins = vec![&wbin];
//!
//!             client.put(&wpolicy, &key, &bins).unwrap();
//!             let rec = client.get(&policy, &key, None);
//!             println!("Record: {}", rec.unwrap());
//!
//!             client.touch(&wpolicy, &key).unwrap();
//!             let rec = client.get(&policy, &key, None);
//!             println!("Record: {}", rec.unwrap());
//!
//!             let rec = client.get_header(&policy, &key);
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

pub use value::Value;
pub use policy::{Policy, ClientPolicy, ReadPolicy, WritePolicy, Priority, ConsistencyLevel,
                 CommitLevel, RecordExistsAction, GenerationPolicy, ScanPolicy, QueryPolicy};
pub use net::{Host, Connection};
pub use cluster::{Node, Cluster};
pub use error::{AerospikeError, AerospikeResult};
pub use client::{Client, ResultCode};
pub use common::{Key, Bin, Operation, UDFLang, Recordset, Statement, Filter, IndexType,
                 CollectionIndexType, ParticleType};
pub use common::{MapPolicy, MapReturnType};

mod command;
mod msgpack;

pub mod common;
pub mod value;
pub mod policy;
pub mod net;
pub mod cluster;
pub mod error;
pub mod client;
