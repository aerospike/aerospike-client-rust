// Copyright 2015-2026 Aerospike, Inc.
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

use aerospike::query::PartitionFilter;

use crate::proptests::key::*;
use proptest::prelude::*;

pub fn partition_filter(ns: String, set_name: String) -> impl Strategy<Value = PartitionFilter> {
    prop_oneof![
        Just(PartitionFilter::all()),
        (0usize..4095).prop_map(PartitionFilter::by_id),
        (0usize..4096 / 2, 1usize..4096 / 2)
            .prop_map(|(begin, count)| PartitionFilter::by_range(begin, count)),
        any_key(ns, set_name).prop_map(|key| { PartitionFilter::by_key(&key) }),
    ]
}
