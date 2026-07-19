// Copyright 2015-2018 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub mod admin_command;
pub mod batch_attr;
pub mod batch_operate_command;
pub mod buffer;
pub mod delete_command;
pub mod execute_udf_command;
pub mod exists_command;
pub mod info_command;
pub mod operate_command;
pub mod particle_type;
pub mod query_command;
pub mod read_command;
pub mod server_command;
pub mod single_command;
pub mod stream_command;
pub mod touch_command;
pub mod txn_add_keys_command;
pub mod txn_close_command;
pub mod txn_mark_roll_forward_command;
pub mod txn_roll_command;
pub mod txn_verify_command;
pub mod write_command;

pub mod field_type;

use std::sync::Arc;

pub use self::batch_attr::BatchAttr;
pub use self::batch_operate_command::BatchOperateCommand;
pub use self::delete_command::DeleteCommand;
pub use self::execute_udf_command::ExecuteUDFCommand;
pub use self::exists_command::ExistsCommand;
pub use self::info_command::Message;
pub use self::operate_command::OperateCommand;
pub use self::particle_type::ParticleType;
pub use self::query_command::QueryCommand;
pub use self::read_command::ReadCommand;
pub use self::server_command::ServerCommand;
pub use self::single_command::SingleCommand;
pub use self::stream_command::StreamCommand;
pub use self::touch_command::TouchCommand;
pub use self::write_command::WriteCommand;

use crate::cluster::{Cluster, Node};
use crate::errors::{Error, Result};
use crate::metrics::CommandType;
use crate::net::Connection;
use crate::ResultCode;

// Command interface describes all commands available
#[async_trait::async_trait]
pub trait Command {
    fn hint(&self) -> u8;
    async fn write_timeout(&mut self, conn: &mut Connection) -> Result<()>;
    async fn prepare_buffer(&mut self, conn: &mut Connection) -> Result<()>;
    fn get_node(&mut self) -> Result<Arc<Node>>;
    async fn parse_result(&mut self, conn: &mut Connection) -> Result<()>;
    async fn write_buffer(&mut self, conn: &mut Connection) -> Result<()>;
    fn can_retry(&mut self) -> bool;
    fn can_recover_connection(&mut self) -> bool;
    /// True if this command performs a write (for `in_doubt` computation on failure).
    fn is_write(&self) -> bool {
        false
    }
    /// Prepare the partition for a retry by advancing the sequence number.
    fn prepare_retry(&mut self, _is_client_timeout: bool) {}
    /// Logical command type, used to attribute metrics. Defaults to
    /// [`CommandType::None`] (not recorded).
    fn command_type(&self) -> CommandType {
        CommandType::None
    }
    /// Namespace this command targets, used for detailed per-namespace
    /// metrics. `None` for multi-namespace or namespace-less commands.
    fn namespace(&self) -> Option<&str> {
        None
    }
    /// The cluster this command runs against, used by the retry loop to record
    /// cluster-wide counters (`exceeded-max-retries` / `exceeded-total-timeout`).
    /// Defaulted to `None` for commands that carry only a node handle
    /// (streaming / server-background commands).
    fn cluster(&self) -> Option<&Cluster> {
        None
    }
}

/// Pacing sleep between retries while the connection pool is empty and a
/// background task is opening a connection. Pool-empty waits do not consume
/// the command's retry budget (see the retry loops), so this bounds how hot
/// the wait loop spins; the wait itself is bounded by the command deadline
/// plus [`POOL_EMPTY_MAX_WAITS`].
pub(crate) const POOL_EMPTY_WAIT: std::time::Duration = std::time::Duration::from_millis(1);

/// Upper bound on consecutive pool-empty waits for commands without a total
/// timeout (`total_timeout == 0` means no deadline): ~5s at
/// [`POOL_EMPTY_WAIT`] pacing. Beyond this the pool-empty error is handled
/// like any other connection failure.
pub(crate) const POOL_EMPTY_MAX_WAITS: usize = 5_000;

/// Whether the connection may be returned to the pool after this error.
/// Client-side errors and the `SCAN_ABORT` / `QUERY_ABORTED` server codes
/// require the socket to be discarded (it may still have stream bytes
/// pending).
pub fn keep_connection(err: &Error) -> bool {
    err.keep_connection()
}

/// Client-initiated network error (broken connection or socket timeout).
pub fn is_network_error(err: &Error) -> bool {
    matches!(
        err.kind(),
        crate::ErrorKind::Connection | crate::ErrorKind::Timeout
    )
}

/// Server-reported result codes that are safe to retry on (TIMEOUT,
/// `DEVICE_OVERLOAD`, `KEY_BUSY`). We also treat `PartitionUnavailable` as
/// retriable so callers eventually see the partition recover from a
/// transitional state.
pub fn is_retriable_server_error(err: &Error) -> bool {
    match err.kind() {
        crate::ErrorKind::Server { rc, .. } | crate::ErrorKind::BatchRow { rc, .. } => matches!(
            rc,
            ResultCode::Timeout
                | ResultCode::DeviceOverload
                | ResultCode::KeyBusy
                | ResultCode::PartitionUnavailable
        ),
        _ => false,
    }
}

/// Overall retry gate: either a network failure or a retriable server error.
pub fn should_retry(err: &Error) -> bool {
    is_network_error(err) || is_retriable_server_error(err)
}

/// Contract tests for the three predicates that gate retry + socket reuse.
/// `Error::Connection` must be retriable 
/// `Error::Io` must NOT be (regression guard — if someone re-introduces
/// `Err(e.into())` at a socket site, the loopback tests catch the producer
/// regression while these pin the predicate semantics).
#[cfg(test)]
mod tests_retry_predicates {
    use super::*;
    use crate::ResultCode;

    fn conn_err() -> Error {
        Error::connection("read: early eof")
    }
    fn io_err() -> Error {
        Error::from(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "early eof",
        ))
    }
    fn timeout_err() -> Error {
        Error::timeout("Timeout reading from the network connection")
    }
    fn server_err(rc: ResultCode) -> Error {
        Error::server_error(rc, String::new(), None)
    }

    #[test]
    fn is_network_error_contract() {
        // (err, expected, label)
        let cases: &[(Error, bool, &str)] = &[
            (conn_err(),                              true,  "Error::Connection"),
            (timeout_err(),                           true,  "Error::Timeout"),
            (io_err(),                                false, "Error::Io — regression guard"),
            (server_err(ResultCode::DeviceOverload),  false, "server error is not a network error"),
        ];
        for (err, expected, label) in cases {
            assert_eq!(is_network_error(err), *expected, "{label}: err={err:?}");
        }
    }

    #[test]
    fn should_retry_contract() {
        let cases: &[(Error, bool, &str)] = &[
            (conn_err(),                                true,  "Connection retries"),
            (timeout_err(),                             true,  "Timeout retries"),
            (io_err(),                                  false, "Io does NOT retry"),
            (server_err(ResultCode::Timeout),           true,  "server TIMEOUT retries"),
            (server_err(ResultCode::DeviceOverload),    true,  "DEVICE_OVERLOAD retries"),
            (server_err(ResultCode::KeyBusy),           true,  "KEY_BUSY retries"),
            (server_err(ResultCode::PartitionUnavailable), true, "PARTITION_UNAVAILABLE retries"),
            (server_err(ResultCode::KeyNotFoundError),  false, "KEY_NOT_FOUND does NOT retry"),
            (server_err(ResultCode::ParameterError),    false, "PARAMETER_ERROR does NOT retry"),
        ];
        for (err, expected, label) in cases {
            assert_eq!(should_retry(err), *expected, "{label}: err={err:?}");
        }
    }

    #[test]
    fn keep_connection_contract() {
        // true = keep, false = drop (caller calls invalidate)
        let cases: &[(Error, bool, &str)] = &[
            (conn_err(),                               false, "Connection: socket broken — drop"),
            (timeout_err(),                            true,  "Timeout: deadline elapsed, socket may recover — keep"),
            (io_err(),                                 false, "Io: conservative drop"),
            (server_err(ResultCode::KeyNotFoundError), true,  "ordinary server error: response complete — keep"),
            (server_err(ResultCode::ScanAbort),        false, "ScanAbort: stream mid-frame — drop"),
        ];
        for (err, expected, label) in cases {
            assert_eq!(keep_connection(err), *expected, "{label}: err={err:?}");
        }
    }
}
