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

mod priority;
mod consistency_level;
mod generation_policy;
mod record_exists_action;
mod commit_level;
mod client_policy;
mod read_policy;
mod write_policy;
mod scan_policy;
mod query_policy;
mod admin_policy;

pub use self::priority::Priority;
pub use self::consistency_level::ConsistencyLevel;
pub use self::generation_policy::GenerationPolicy;
pub use self::record_exists_action::RecordExistsAction;
pub use self::commit_level::CommitLevel;
pub use self::write_policy::Expiration;

pub use self::client_policy::ClientPolicy;
pub use self::read_policy::ReadPolicy;
pub use self::write_policy::WritePolicy;
pub use self::scan_policy::ScanPolicy;
pub use self::query_policy::QueryPolicy;
pub use self::admin_policy::AdminPolicy;

use std::time::{Duration, Instant};
use std::option::Option;

pub trait Policy {
    fn priority(&self) -> &Priority;
    fn deadline(&self) -> Option<Instant>;
    fn timeout(&self) -> Option<Duration>;
    fn max_retries(&self) -> Option<usize>;
    fn sleep_between_retries(&self) -> Option<Duration>;
    fn consistency_level(&self) -> &ConsistencyLevel;
}

pub trait PolicyLike {
    fn base(&self) -> &BasePolicy;
}

impl<T> Policy for T
    where T: PolicyLike
{
    fn priority(&self) -> &Priority {
        self.base().priority()
    }

    fn consistency_level(&self) -> &ConsistencyLevel {
        self.base().consistency_level()
    }

    fn deadline(&self) -> Option<Instant> {
        self.base().deadline()
    }

    fn timeout(&self) -> Option<Duration> {
        self.base().timeout()
    }

    fn max_retries(&self) -> Option<usize> {
        self.base().max_retries()
    }

    fn sleep_between_retries(&self) -> Option<Duration> {
        self.base().sleep_between_retries()
    }
}

#[derive(Debug,Clone)]
pub struct BasePolicy {
    // Priority of request relative to other transactions.
    // Currently, only used for scans.
    pub priority: Priority, // = Priority.DEFAULT;

    // How replicas should be consulted in a read operation to provide the desired
    // consistency guarantee. Default to allowing one replica to be used in the
    // read operation.
    pub consistency_level: ConsistencyLevel, // = CONSISTENCY_ONE

    // Timeout specifies transaction timeout.
    // This timeout is used to set the socket timeout and is also sent to the
    // server along with the transaction in the wire protocol.
    // Default to no timeout (0).
    pub timeout: Option<Duration>,

    // MaxRetries determines maximum number of retries before aborting the current transaction.
    // A retry is attempted when there is a network error other than timeout.
    // If maxRetries is exceeded, the abort will occur even if the timeout
    // has not yet been exceeded.
    pub max_retries: Option<usize>, // = 2;

    // SleepBetweenReplies determines duration to sleep between retries if a
    // transaction fails and the timeout was not exceeded.  Enter zero to skip sleep.
    pub sleep_between_retries: Option<Duration>, // = 500ms;
}

impl Policy for BasePolicy {
    fn priority(&self) -> &Priority {
        &self.priority
    }

    fn consistency_level(&self) -> &ConsistencyLevel {
        &self.consistency_level
    }

    // fn set_priority(&mut self, p: Priority) {
    //     self.priority = p
    // }

    fn deadline(&self) -> Option<Instant> {
        match self.timeout {
            Some(timeout) => Some(Instant::now() + timeout),
            None => None,
        }
    }

    fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    fn max_retries(&self) -> Option<usize> {
        self.max_retries
    }

    // fn set_max_retries(&mut self, r: Option<usize>) {
    //     self.max_retries = r
    // }

    fn sleep_between_retries(&self) -> Option<Duration> {
        self.sleep_between_retries
    }
}
