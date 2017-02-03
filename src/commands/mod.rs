// Copyright 2015-2017 Aerospike, Inc.
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
pub mod buffer;
pub mod particle_type;

mod field_type;

use std::sync::Arc;
use std::time::Duration;

pub use self::delete_command::DeleteCommand;
pub use self::execute_udf_command::ExecuteUDFCommand;
pub use self::exists_command::ExistsCommand;
pub use self::info_command::Message;
pub use self::operate_command::OperateCommand;
pub use self::query_command::QueryCommand;
pub use self::read_command::ReadCommand;
pub use self::read_header_command::ReadHeaderCommand;
pub use self::scan_command::ScanCommand;
pub use self::single_command::SingleCommand;
pub use self::stream_command::StreamCommand;
pub use self::touch_command::TouchCommand;
pub use self::write_command::WriteCommand;
pub use self::particle_type::ParticleType;

use errors::*;
use net::Connection;
use cluster::Node;

// Command interface describes all commands available
pub trait Command {
    fn write_timeout(&mut self,
                     conn: &mut Connection,
                     timeout: Option<Duration>)
                     -> Result<()>;
    fn prepare_buffer(&mut self, conn: &mut Connection) -> Result<()>;
    fn get_node(&self) -> Result<Arc<Node>>;
    fn parse_result(&mut self, conn: &mut Connection) -> Result<()>;
    fn write_buffer(&mut self, conn: &mut Connection) -> Result<()>;
}
