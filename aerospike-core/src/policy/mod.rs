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

//! Policy types encapsulate optional parameters for various client operations.
#![allow(clippy::missing_errors_doc)]

mod admin_policy;
mod batch_policy;
mod client_policy;
mod commit_level;
mod concurrency;
mod consistency_level;
mod expiration;
mod generation_policy;
mod query_duration;
mod query_policy;
mod read_policy;
mod read_touch_ttl_percent;
mod record_exists_action;
mod scan_policy;
mod stream_policy;
mod write_policy;

pub use self::admin_policy::AdminPolicy;
pub use self::batch_policy::BatchPolicy;
pub use self::client_policy::ClientPolicy;
pub use self::commit_level::CommitLevel;
pub use self::concurrency::Concurrency;
pub use self::consistency_level::ConsistencyLevel;
pub use self::expiration::Expiration;
pub use self::generation_policy::GenerationPolicy;
pub use self::query_duration::QueryDuration;
pub use self::query_policy::QueryPolicy;
pub use self::read_policy::ReadPolicy;
pub use self::read_touch_ttl_percent::ReadTouchTTL;
pub use self::record_exists_action::RecordExistsAction;
pub use self::scan_policy::ScanPolicy;
pub(crate) use self::stream_policy::StreamPolicy;
pub use self::write_policy::WritePolicy;

use crate::expressions::FilterExpression;
use aerospike_rt::time::{Duration, Instant};
use std::option::Option;

/// Trait implemented by most policy types; policies that implement this trait typically encompass
/// an instance of `BasePolicy`.
pub trait Policy {
    #[doc(hidden)]
    /// Deadline for current transaction based on specified timeout. For internal use only.
    fn deadline(&self) -> Option<Instant>;

    /// Total transaction timeout for both client and server. The timeout is tracked on the client
    /// and also sent to the server along with the transaction in the wire protocol. The client
    /// will most likely timeout first, but the server has the capability to timeout the
    /// transaction as well.
    ///
    /// The timeout is also used as a socket timeout. Default: 0 (no timeout).
    fn total_timeout(&self) -> Option<Duration>;

    /// Maximum number of retries before aborting the current transaction. A retry may be attempted
    /// when there is a network error. If `max_retries` is exceeded, the abort will occur even if
    /// the timeout has not yet been exceeded.
    fn max_retries(&self) -> Option<usize>;

    /// Time to sleep between retries. Set to zero to skip sleep. Default: 500ms.
    fn sleep_between_retries(&self) -> Option<Duration>;

    /// How replicas should be consulted in read operations to provide the desired consistency
    /// guarantee.
    fn consistency_level(&self) -> &ConsistencyLevel;
}

#[doc(hidden)]
/// Policy-like object that encapsulates a base policy instance.
pub trait PolicyLike {
    /// Retrieve a reference to the base policy.
    fn base(&self) -> &BasePolicy;
}

impl<T> Policy for T
where
    T: PolicyLike,
{
    fn consistency_level(&self) -> &ConsistencyLevel {
        self.base().consistency_level()
    }

    fn deadline(&self) -> Option<Instant> {
        self.base().deadline()
    }

    fn total_timeout(&self) -> Option<Duration> {
        self.base().total_timeout()
    }

    fn max_retries(&self) -> Option<usize> {
        self.base().max_retries()
    }

    fn sleep_between_retries(&self) -> Option<Duration> {
        self.base().sleep_between_retries()
    }
}

/// Defines algorithm used to determine the target node for a command. The replica algorithm only affects single record and batch commands.
#[derive(Debug, Copy, Clone)]
pub enum Replica {
    /// Use node containing key's master partition.
    Master,

    /// Try node containing master partition first.
    /// If connection fails, all commands try nodes containing replicated partitions.
    /// If socketTimeout is reached, reads also try nodes containing replicated partitions,
    /// but writes remain on master node.
    Sequence,

    /// Try node on the same rack as the client first.  If timeout or there are no nodes on the
    /// same rack, use SEQUENCE instead.
    ///
    /// {@link ClientPolicy#rackAware}, {@link ClientPolicy#rackId}, and server rack
    /// configuration must also be set to enable this functionality.
    PreferRack,
}

impl Default for Replica {
    fn default() -> Self {
        Replica::Sequence
    }
}

/// Common parameters shared by all policy types.
#[derive(Debug, Clone)]
pub struct BasePolicy {
    /// How replicas should be consulted in a read operation to provide the desired
    /// consistency guarantee. Default to allowing one replica to be used in the
    /// read operation.
    pub consistency_level: ConsistencyLevel,

    /// Timeout specifies transaction timeout.
    /// This timeout is used to set the socket timeout and is also sent to the
    /// server along with the transaction in the wire protocol.
    /// Default to no timeout (0).
    pub total_timeout: Option<Duration>,

    /// MaxRetries determines maximum number of retries before aborting the current transaction.
    /// A retry is attempted when there is a network error other than timeout.
    /// If maxRetries is exceeded, the abort will occur even if the timeout
    /// has not yet been exceeded.
    pub max_retries: Option<usize>,

    /// read_touch_ttl determines how record TTL (time to live) is affected on reads. When enabled, the server can
    /// efficiently operate as a read-based LRU cache where the least recently used records are expired.
    /// The value is expressed as a percentage of the TTL sent on the most recent write such that a read
    /// within this interval of the record’s end of life will generate a touch.
    ///
    /// For example, if the most recent write had a TTL of 10 hours and `read_touch_ttl` is set to
    /// 80, the next read within 8 hours of the record's end of life (equivalent to 2 hours after the most
    /// recent write) will result in a touch, resetting the TTL to another 10 hours.
    ///
    /// Supported in server v8+.
    ///
    /// Default: ReadTouchTTL::ServerDefault
    pub read_touch_ttl: ReadTouchTTL,

    /// SleepBetweenReplies determines duration to sleep between retries if a
    /// transaction fails and the timeout was not exceeded.  Enter zero to skip sleep.
    pub sleep_between_retries: Option<Duration>,

    /// Optional FilterExpression
    pub filter_expression: Option<FilterExpression>,
}

impl Policy for BasePolicy {
    fn deadline(&self) -> Option<Instant> {
        self.total_timeout.map(|timeout| Instant::now() + timeout)
    }

    fn total_timeout(&self) -> Option<Duration> {
        self.total_timeout
    }

    fn max_retries(&self) -> Option<usize> {
        self.max_retries
    }

    fn sleep_between_retries(&self) -> Option<Duration> {
        self.sleep_between_retries
    }

    fn consistency_level(&self) -> &ConsistencyLevel {
        &self.consistency_level
    }
}
