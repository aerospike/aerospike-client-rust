// Copyright 2015-2018 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
//
// Licensed under the Apache License version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

use aerospike_rt::time::{Duration, Instant};

pub trait StreamPolicy {
    fn max_records(&self) -> Option<u64>;
    fn sleep_between_retries(&self) -> Option<Duration>;
    fn deadline(&self) -> Option<Instant>;
    fn socket_timeout(&self) -> u32;
    fn total_timeout(&self) -> u32;
    fn replica(&self) -> crate::policy::Replica;
    fn max_retries(&self) -> usize;
}
