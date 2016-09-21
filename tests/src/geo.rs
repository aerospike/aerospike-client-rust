// // Copyright 2015-2016 Aerospike, Inc.
// //
// // Portions may be licensed to Aerospike, Inc. under one or more contributor
// // license agreements.
// //
// // Licensed under the Apache License, Version 2.0 (the "License"); you may not
// // use this file except in compliance with the License. You may obtain a copy of
// // the License at http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing, software
// // distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// // WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// // License for the specific language governing permissions and limitations under
// // the License.

// use std::collections::HashMap;
// use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering};

// use aerospike::*;

// // use log::LogLevel;
// use env_logger;

// // use std::collections::{HashMap, VecDeque};
// use std::sync::{RwLock, Arc, Mutex};
// // use std::vec::Vec;
// use std::thread;
// use std::time::{Instant, Duration};

// use common1;

// #[test]
// fn geo_filter_macros() {
//     let _ = env_logger::init();

//     let geo_filter = as_within_region!("bin1", "{}".to_string());
//     let geo_filter = as_within_region_in_collection!("bin1", CollectionIndexType::MapValues, "{}");
//     let geo_filter = as_regions_containing_point!("bin1", "{}");
//     let geo_filter = as_regions_containing_point_in_collection!("bin1",
//                                                                 CollectionIndexType::MapValues,
//                                                                 "{}");
//     let geo_filter = as_within_radius!("bin1", 1, 3, 7);
//     let geo_filter = as_within_radius_in_collection!("bin1", CollectionIndexType::List, 1, 3, 7);
// }
