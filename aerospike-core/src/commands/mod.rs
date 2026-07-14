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
pub mod scan_command;
pub mod server_command;
pub mod single_command;
pub mod stream_command;
pub mod touch_command;
pub mod write_command;

mod field_type;

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
pub use self::scan_command::ScanCommand;
pub use self::server_command::ServerCommand;
pub use self::single_command::SingleCommand;
pub use self::stream_command::StreamCommand;
pub use self::touch_command::TouchCommand;
pub use self::write_command::WriteCommand;

use crate::cluster::Node;
use crate::errors::{Error, Result};
use crate::net::Connection;
use crate::ResultCode;

// Command interface describes all commands available
#[async_trait::async_trait]
pub trait Command {
    fn hint(&self) -> u8;
    async fn write_timeout(&mut self, conn: &mut Connection) -> Result<()>;
    async fn prepare_buffer(&mut self, conn: &mut Connection) -> Result<()>;
    async fn get_node(&mut self) -> Result<Arc<Node>>;
    async fn parse_result(&mut self, conn: &mut Connection) -> Result<()>;
    async fn write_buffer(&mut self, conn: &mut Connection) -> Result<()>;
    fn can_retry(&mut self) -> bool;
    fn can_recover_connection(&mut self) -> bool;
}

pub const fn keep_connection(err: &Error) -> bool {
    match err {
        Error::ServerError(rc, _, _)
        | Error::BatchError(_, rc, _, _)
        | Error::BatchLastError(_, rc, _, _) => {
            !matches!(rc, ResultCode::ScanAbort | ResultCode::QueryAborted)
        }
        Error::Timeout(_) => true,
        _ => false,
    }
}

pub const fn is_network_error(err: &Error) -> bool {
    matches!(err, Error::Connection(_) | Error::Timeout(_))
}

/// Server-reported result codes that are safe to retry on (TIMEOUT,
/// `DEVICE_OVERLOAD`, `KEY_BUSY`). We also treat `PartitionUnavailable` as
/// retriable so callers eventually see the partition recover from a
/// transitional state.
pub const fn is_retriable_server_error(err: &Error) -> bool {
    match err {
        Error::ServerError(rc, _, _)
        | Error::BatchError(_, rc, _, _)
        | Error::BatchLastError(_, rc, _, _) => matches!(
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
pub const fn should_retry(err: &Error) -> bool {
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
        Error::Connection("read: early eof".into())
    }
    fn io_err() -> Error {
        Error::Io(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "early eof",
        ))
    }
    fn timeout_err() -> Error {
        Error::Timeout("Timeout reading from the network connection".into())
    }
    fn server_err(rc: ResultCode) -> Error {
        Error::ServerError(rc, false, String::new())
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
