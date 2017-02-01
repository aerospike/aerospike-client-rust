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

use policy::{BasePolicy, PolicyLike};

#[derive(Debug,Clone)]
pub struct ScanPolicy {
    pub base_policy: BasePolicy,

    pub scan_percent: u8, // = 100

    pub max_concurrent_nodes: usize, // 0, parallel all

    pub record_queue_size: usize, // = 1024

    pub include_bin_data: bool, // = true

    pub fail_on_cluster_change: bool, // = true
}


impl ScanPolicy {
    pub fn new() -> Self {
        ScanPolicy::default()
    }
}

impl Default for ScanPolicy {
    fn default() -> ScanPolicy {
        ScanPolicy {
            base_policy: BasePolicy::default(),

            scan_percent: 100,

            max_concurrent_nodes: 0,

            record_queue_size: 1024,

            include_bin_data: true,

            fail_on_cluster_change: true,
        }
    }
}

impl PolicyLike for ScanPolicy {
    fn base(&self) -> &BasePolicy {
        &self.base_policy
    }
}
