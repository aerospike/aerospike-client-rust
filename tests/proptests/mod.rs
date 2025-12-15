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

mod batch_operation;
mod batches;
mod batches_spam;
mod bins;
mod filter_expression;
mod key;
mod kv;
mod operation;
mod operations;
mod partition_filter;
mod policy;
mod queries;
mod scans;
mod value;

use aerospike::Value;
use std::collections::HashMap;

/// Returns a (deep!) clone of the value, but where all UInt values have
/// been mapped to Int values.
///
/// Applications for this method includes property testing where the
/// randomness of a strategy forbids carefully crafting a value to avoid
/// UInts.  This is a VERY EXPENSIVE operation.  Do not use this for
/// production code unless you know precisely what you are doing and
/// understand its performance impacts.
pub fn clone_safely(v: &Value) -> Value {
    match v {
        // Servers store integers as signed quantities at all times.
        // Sending requests with Ints can sometimes yield responses with
        // UInts, however.  For the sake of testing, this prevents us from
        // simply comparing Value trees using == or != operators.  This
        // mapping restores our ability to do this.
        Value::UInt(val) => Value::Int(*val as i64),

        // We need to recurse into maps and lists.
        Value::HashMap(m) => {
            let mut new_map = HashMap::new();
            for (key, value) in m.iter() {
                new_map.insert(clone_safely(key), clone_safely(value));
            }
            Value::HashMap(new_map)
        }
        Value::OrderedMap(v) => {
            let mut new_map = vec![];
            for (key, value) in v {
                new_map.push((clone_safely(key), clone_safely(value)));
            }
            Value::OrderedMap(new_map)
        }
        Value::List(l) => {
            let mut new_list = vec![];
            for item in l {
                new_list.push(clone_safely(item));
            }
            Value::List(new_list)
        }

        // For everything else, just clone it as-is.
        myself => myself.clone(),
    }
}
