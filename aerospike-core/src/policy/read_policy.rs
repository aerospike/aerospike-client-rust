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

use crate::expressions::FilterExpression;
use crate::policy::BasePolicy;
use crate::ConsistencyLevel;
use aerospike_rt::time::Duration;

use super::{PolicyLike, Replica};

/// `ReadPolicy` excapsulates parameters for transaction policy attributes
/// used in all database operation calls.
#[derive(Debug, Default, Clone)]
pub struct ReadPolicy {
    /// Base policy instance
    pub base_policy: BasePolicy,

    /// Defines algorithm used to determine the target node for a command. The replica algorithm only affects single record and batch commands.
    pub replica: Replica,
}

impl Default for BasePolicy {
    fn default() -> BasePolicy {
        BasePolicy {
            total_timeout: Some(Duration::new(30, 0)),
            max_retries: Some(2),
            sleep_between_retries: Some(Duration::new(0, 500_000_000)),
            consistency_level: ConsistencyLevel::ConsistencyOne,
            read_touch_ttl: super::ReadTouchTTL::ServerDefault,
            filter_expression: None,
        }
    }
}

impl BasePolicy {
    /// Get the Optional Filter Expression
    pub const fn filter_expression(&self) -> &Option<FilterExpression> {
        &self.filter_expression
    }
}

impl PolicyLike for ReadPolicy {
    fn base(&self) -> &BasePolicy {
        &self.base_policy
    }
}
