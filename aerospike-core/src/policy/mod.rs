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

use std::cmp::min;

mod admin_policy;
mod batch_policy;
mod client_policy;
mod commit_level;
mod concurrency;
mod expiration;
mod generation_policy;
mod query_duration;
mod query_policy;
mod read_mode_ap;
mod read_mode_sc;
mod read_policy;
mod read_touch_ttl_percent;
mod record_exists_action;
mod stream_policy;
mod write_policy;

pub use self::admin_policy::AdminPolicy;
pub use self::batch_policy::BatchPolicy;
pub use self::client_policy::AuthMode;
pub use self::client_policy::ClientPolicy;
pub use self::commit_level::CommitLevel;
pub use self::concurrency::Concurrency;
pub use self::expiration::Expiration;
pub use self::generation_policy::GenerationPolicy;
pub use self::query_duration::QueryDuration;
pub use self::query_policy::QueryPolicy;
pub use self::read_mode_ap::ReadModeAP;
pub use self::read_mode_sc::ReadModeSC;
pub use self::read_policy::ReadPolicy;
pub use self::read_touch_ttl_percent::ReadTouchTTL;
pub use self::record_exists_action::RecordExistsAction;
pub(crate) use self::stream_policy::StreamPolicy;
pub use self::write_policy::WritePolicy;

use crate::expressions::Expression;
use crate::txn::Txn;
use aerospike_rt::time::{Duration, Instant};
use std::option::Option;
use std::sync::Arc;

/// Trait implemented by most policy types; policies that implement this trait typically encompass
/// an instance of `BasePolicy`.
pub trait Policy {
    #[doc(hidden)]
    /// Deadline for current transaction based on specified timeout. For internal use only.
    fn deadline(&self) -> Option<Instant>;

    /// Server timeout.
    fn server_timeout(&self) -> u32;

    /// Socket timeout for client.
    fn socket_timeout(&self) -> u32;

    /// Total transaction timeout for both client and server. The timeout is tracked on the client
    /// and also sent to the server along with the transaction in the wire protocol. The client
    /// will most likely timeout first, but the server has the capability to timeout the
    /// transaction as well.
    fn total_timeout(&self) -> u32;

    /// Returns the value of `timeout_delay`.
    fn timeout_delay(&self) -> u32;

    /// Maximum number of retries before aborting the current transaction. A retry may be attempted
    /// when there is a network error. If `max_retries` is exceeded, the abort will occur even if
    /// the timeout has not yet been exceeded.
    fn max_retries(&self) -> usize;

    /// Time to sleep between retries. Set to zero to skip sleep. Default: 500ms.
    fn sleep_between_retries(&self) -> Option<Duration>;

    /// Whether to use zlib compression on command buffers.
    fn use_compression(&self) -> bool;

    /// Minimum command-buffer size at which compression actually fires.
    /// Buffers `<=` this value are sent uncompressed even when
    /// [`use_compression`](Self::use_compression) is true.
    fn compression_threshold(&self) -> usize;

    /// Read policy for AP (availability) namespaces.
    fn read_mode_ap(&self) -> ReadModeAP;

    /// Read policy for SC (strong consistency) namespaces.
    fn read_mode_sc(&self) -> ReadModeSC;
}

/// Policy-like object that encapsulates a base policy instance.
pub(crate) trait PolicyLike {
    /// Retrieve a reference to the base policy.
    fn base(&self) -> &BasePolicy;
}

impl<T> Policy for T
where
    T: PolicyLike,
{
    fn read_mode_ap(&self) -> ReadModeAP {
        self.base().read_mode_ap()
    }

    fn read_mode_sc(&self) -> ReadModeSC {
        self.base().read_mode_sc()
    }

    fn use_compression(&self) -> bool {
        self.base().use_compression()
    }

    fn compression_threshold(&self) -> usize {
        self.base().compression_threshold()
    }

    fn deadline(&self) -> Option<Instant> {
        self.base().deadline()
    }

    fn server_timeout(&self) -> u32 {
        self.base().server_timeout()
    }

    fn socket_timeout(&self) -> u32 {
        self.base().socket_timeout()
    }

    fn total_timeout(&self) -> u32 {
        self.base().total_timeout()
    }

    fn timeout_delay(&self) -> u32 {
        self.base().timeout_delay()
    }

    fn max_retries(&self) -> usize {
        self.base().max_retries()
    }

    fn sleep_between_retries(&self) -> Option<Duration> {
        self.base().sleep_between_retries()
    }
}

/// Defines algorithm used to determine the target node for a command. The replica algorithm only affects single record and batch commands.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
pub enum Replica {
    /// Use node containing key's master partition.
    Master,

    /// Distribute reads across nodes containing key's master and replicated partitions
    /// in round-robin fashion. Writes always use the master node.
    MasterProles,

    /// Distribute reads across all nodes in the cluster in round-robin fashion.
    /// This option is useful when the replication factor equals the number of nodes
    /// in the cluster and the overhead of requesting proles is not desired.
    /// Writes always use the master node.
    Random,

    /// Try node containing master partition first.
    /// If connection fails, all commands try nodes containing replicated partitions.
    /// If socketTimeout is reached, reads also try nodes containing replicated partitions,
    /// but writes remain on master node.
    #[default]
    Sequence,

    /// Try node on the same rack as the client first. If timeout or there are no nodes on the
    /// same rack, use SEQUENCE instead.
    ///
    /// `ClientPolicy::rack_ids` and server rack configuration must also be set to enable
    /// this functionality.
    PreferRack,
}

/// Common parameters shared by all policy types.
#[derive(Debug, Clone)]
pub struct BasePolicy {
    /// Read policy for AP (availability) namespaces.
    /// Indicates how duplicates should be consulted in a read operation.
    /// Only makes a difference during migrations and only applicable in AP mode.
    pub read_mode_ap: ReadModeAP,

    /// Read policy for SC (strong consistency) namespaces.
    /// Determines SC read consistency options.
    pub read_mode_sc: ReadModeSC,

    /// Socket idle timeout when processing a database command.
    ///
    /// If `socket_timeout` is zero and `total_timeout` is non-zero, then `socket_timeout` will be set
    /// to `total_timeout`. If both `socket_timeout` and `total_timeout` are non-zero and
    /// `socket_timeout` > `total_timeout`, then `socket_timeout` will be set to `total_timeout`. If both
    /// `socket_timeout` and `total_timeout` are zero, then there will be no socket idle limit.
    ///
    /// If `socket_timeout` is non-zero and the socket has been idle for at least `socket_timeout`,
    /// both `max_retries` and `total_timeout` are checked. If `max_retries` and `total_timeout` are not
    /// exceeded, the command is retried.
    pub socket_timeout: u32,

    /// Total command timeout.
    /// This timeout is used to set the socket timeout and is also sent to the
    /// server along with the command in the wire protocol.
    /// Default to no timeout (0).
    pub total_timeout: u32,

    /// Delay milliseconds after socket read timeout in an attempt to recover the socket
    /// in the background. Processing continues on the original command and the user
    /// is still notified at the original command timeout.
    ///
    /// When a command is stopped prematurely, the socket must be drained of all incoming
    /// data or closed to prevent unread socket data from corrupting the next command
    /// that would use that socket.
    ///
    /// If a socket read timeout occurs and `timeout_delay` is greater than zero, the socket
    /// will be drained until all data has been read or `timeout_delay` is reached. If all
    /// data has been read, the socket will be placed back into the connection pool. If
    /// `timeout_delay` is reached before the draining is complete, the socket will be closed.
    ///
    /// Many cloud providers encounter performance problems when sockets are closed by the
    /// client when the server still has data left to write (results in socket RST packet).
    /// If the socket is fully drained before closing, the socket RST performance penalty
    /// can be avoided on these cloud providers.
    ///
    /// The disadvantage of enabling `timeout_delay` is that extra processing is required
    /// to drain sockets and additional connections may still be needed for command retries.
    ///
    /// If `timeout_delay` were to be enabled, 3000ms would be a reasonable value.
    ///
    /// Default: 0 (no delay, connection closed on timeout)
    pub timeout_delay: u32,

    /// `max_retries` determines maximum number of retries before aborting the current transaction.
    /// A retry is attempted when there is a network error other than timeout.
    /// If maxRetries is exceeded, the abort will occur even if the timeout
    /// has not yet been exceeded.
    pub max_retries: usize,

    /// Determines how record TTL (time to live) is affected on reads. When enabled, the server can
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
    /// Default: `ReadTouchTTL::ServerDefault`
    pub read_touch_ttl: ReadTouchTTL,

    /// Duration to sleep between retries if a command fails and
    /// the timeout was not exceeded. Enter zero to skip sleep.
    ///
    /// Default: 500
    pub sleep_between_retries: u32,

    /// Use zlib compression on command buffers sent to the server and responses received
    /// from the server when the buffer size is greater than [`compression_threshold`](Self::compression_threshold).
    ///
    /// This option will increase cpu and memory usage (for extra compressed buffers), but
    /// decrease the size of data sent over the network.
    ///
    /// Valid for Aerospike Server Enterprise Edition only.
    ///
    /// Default: false
    pub use_compression: bool,

    /// Minimum command buffer size, in bytes, before compression is applied.
    /// Buffers smaller than or equal to this value are sent uncompressed
    /// even when [`use_compression`](Self::use_compression) is enabled —
    /// the per-command CPU cost of zlib outweighs the savings on small
    /// payloads.
    ///
    /// Defaults to `128`. Tune higher when most operations touch only a
    /// few small bins, lower when most payloads are large. Setting it to
    /// `0` makes every command go through zlib whenever `use_compression`
    /// is on.
    ///
    /// No effect when [`use_compression`](Self::use_compression) is `false`.
    pub compression_threshold: usize,

    /// Optional expression filter applied to each record **after** the server performs the
    /// primary operation (index lookup, scan, read, write, etc.). If the expression evaluates
    /// to `false` for a record, that record is excluded from the results (or the write is
    /// skipped).
    ///
    /// This is different from [`Filter::expression`](crate::query::Filter::expression), which
    /// selects which **expression-based secondary index** to use for a query. In contrast,
    /// `filter_expression` is a post-filter that narrows down the records returned by any
    /// operation.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use aerospike_core::QueryPolicy;
    /// # use aerospike_core::expressions::{eq, int_bin, int_val};
    /// let mut qpolicy = QueryPolicy::default();
    /// // Only return records where bin "status" equals 1
    /// qpolicy.base_policy.filter_expression
    ///     .replace(eq(int_bin("status".to_string()), int_val(1)));
    /// ```
    ///
    /// Default: `None`
    pub filter_expression: Option<Expression>,

    /// Optional Multi-Record Transaction (MRT). All commands in the transaction
    /// must use the same namespace.
    pub txn: Option<Arc<Txn>>,
}

impl Policy for BasePolicy {
    fn deadline(&self) -> Option<Instant> {
        if self.total_timeout > 0 {
            Some(Instant::now() + Duration::from_millis(u64::from(self.total_timeout)))
        } else {
            None
        }
    }

    fn server_timeout(&self) -> u32 {
        if self.total_timeout > 0 {
            self.socket_timeout()
        } else {
            0
        }
    }

    fn socket_timeout(&self) -> u32 {
        match (self.socket_timeout, self.total_timeout) {
            (0, 0) => 0,
            (d, 0) | (0, d) => d,
            (d1, d2) => min(d1, d2),
        }
    }

    fn total_timeout(&self) -> u32 {
        self.total_timeout
    }

    fn timeout_delay(&self) -> u32 {
        self.timeout_delay
    }

    fn max_retries(&self) -> usize {
        self.max_retries
    }

    fn sleep_between_retries(&self) -> Option<Duration> {
        if self.sleep_between_retries > 0 {
            Some(Duration::from_millis(u64::from(self.sleep_between_retries)))
        } else {
            None
        }
    }

    fn read_mode_ap(&self) -> ReadModeAP {
        self.read_mode_ap
    }

    fn read_mode_sc(&self) -> ReadModeSC {
        self.read_mode_sc
    }

    fn use_compression(&self) -> bool {
        self.use_compression
    }

    fn compression_threshold(&self) -> usize {
        self.compression_threshold
    }
}
