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
pub mod single_command;
pub mod stream_command;
pub mod touch_command;
pub mod write_command;

mod field_type;

use std::iter::Iterator;
use std::sync::Arc;

#[cfg(feature = "serialization")]
use serde::Serialize;

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
pub use self::single_command::SingleCommand;
pub use self::stream_command::StreamCommand;
pub use self::touch_command::TouchCommand;
pub use self::write_command::WriteCommand;

use crate::cluster::Node;
use crate::errors::{Error, Result};
use crate::net::Connection;

/// Used in metrics to count the number of executed commands.
#[derive(Debug, Eq, PartialEq, Hash, Clone, Copy)]
#[cfg_attr(feature = "serialization", derive(Serialize))]
#[cfg_attr(feature = "serialization", serde(rename_all = "snake_case"))]
pub enum CommandType {
    None,
    Get,
    GetHeader,
    Exists,
    Put,
    Delete,
    Operate,
    Query,
    Scan,
    Udf,
    BatchRead,
    BatchWrite,
}

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

    fn command_type(&self) -> CommandType;
}

pub trait NamespaceProvider {
    fn get_namespaces(&self) -> impl Iterator<Item = (&str, CommandType)>;
}

pub const fn keep_connection(err: &Error) -> bool {
    matches!(err, Error::ServerError(_, _, _) | Error::Timeout(_))
}

pub const fn is_network_error(err: &Error) -> bool {
    matches!(err, Error::Connection(_) | Error::Timeout(_))
}

#[macro_export]
macro_rules! report_latency {
    ($self:expr, $node:expr) => {{
        use crate::cluster::metrics::SingleCommandMetric;
        use crate::errors::Error;
        use crate::result_code::ResultCode;
        use aerospike_rt::time::Instant;

        let mut single_metric = SingleCommandMetric::new();

        let start = Instant::now();
        let res = SingleCommand::execute($self.policy, $self, &mut single_metric).await;
        let total_latency = start.elapsed().as_micros();
        single_metric.command_latency = total_latency;

        // record latencies
        if let Some(node) = $node.upgrade() {
            node.metrics
                .load()
                .apply_latency($self.command_type(), total_latency as u64);

            node.metrics
                .load()
                .update_metrics_for_namespace($self, &single_metric);

            match res {
                Ok(_) => (),
                Err(Error::ServerError(rc, _, _)) => node
                    .metrics
                    .load()
                    .update_result_code_for_namespace($self, rc),
                Err(_) => node
                    .metrics
                    .load()
                    .update_result_code_for_namespace($self, ResultCode::Ok),
            }
        }

        res
    }};
}

#[macro_export]
macro_rules! record_latency {
    ($node:ident, $timer:ident, $metric:expr) => {{
        if $node.metrics.load().metric_policy.metrics_per_namespace {
            $metric = $timer.elapsed().as_micros();
            $timer = Instant::now();
        }
    }};
}

#[macro_export]
macro_rules! record_bytes {
    ($node:ident, $conn:ident, $metric:expr) => {{
        if $node.metrics.load().metric_policy.metrics_per_namespace {
            $metric.bytes_sent = $conn.bytes_sent as u128;
            $metric.bytes_received = $conn.total_bytes_received as u128;
        }
    }};
}
