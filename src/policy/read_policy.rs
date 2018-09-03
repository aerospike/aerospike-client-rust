// Copyright 2015-2018 Aerospike, Inc.
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

use std::time::Duration;
use Priority;
use ConsistencyLevel;
use policy::BasePolicy;

/// `ReadPolicy` excapsulates parameters for transaction policy attributes
/// used in all database operation calls.
pub type ReadPolicy = BasePolicy;

impl Default for ReadPolicy {
    fn default() -> ReadPolicy {
        ReadPolicy {
            priority: Priority::Default,
            timeout: Some(Duration::new(30, 0)),
            max_retries: Some(2),
            sleep_between_retries: Some(Duration::new(0, 500000000)),
            consistency_level: ConsistencyLevel::ConsistencyOne,
        }
    }
}
