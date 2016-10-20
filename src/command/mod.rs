// Copyright 2013-2016 Aerospike, Inc.
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

pub mod info_command;
pub mod buffer;
pub mod single_command;
pub mod read_command;
pub mod write_command;
pub mod delete_command;
pub mod touch_command;
pub mod exists_command;
pub mod read_header_command;
pub mod operate_command;
pub mod execute_udf_command;
pub mod stream_command;
pub mod scan_command;
pub mod query_command;
pub mod admin_command;

use std::sync::Arc;
use std::time::{Instant, Duration};

use net::Connection;
use error::{AerospikeError, ResultCode, AerospikeResult};

use net::Host;
use cluster::node_validator::NodeValidator;
use cluster::partition_tokenizer::PartitionTokenizer;
use cluster::partition::Partition;
use cluster::{Node, Cluster};
use common::{Key, Record};
use policy::{ClientPolicy, ReadPolicy, Policy};
use command::buffer::Buffer;

// Command interface describes all commands available
pub trait Command {
    fn write_timeout(&mut self,
                     conn: &mut Connection,
                     timeout: Option<Duration>)
                     -> AerospikeResult<()>;
    fn prepare_buffer(&mut self, conn: &mut Connection) -> AerospikeResult<()>;
    fn get_node(&self) -> AerospikeResult<Arc<Node>>;
    fn parse_result(&mut self, conn: &mut Connection) -> AerospikeResult<()>;

    fn write_buffer(&mut self, conn: &mut Connection) -> AerospikeResult<()>;
}
