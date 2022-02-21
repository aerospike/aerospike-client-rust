// Copyright 2015-2020 Aerospike, Inc.
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

mod batch;
mod cdt_bitwise;
mod cdt_list;
mod cdt_map;
mod exp;
mod exp_bitwise;
mod exp_hll;
mod exp_list;
mod exp_map;
mod exp_op;
mod hll;
mod index;
mod kv;
mod query;
mod scan;
#[cfg(feature = "serialization")]
mod serialization;
mod task;
mod truncate;
mod udf;
