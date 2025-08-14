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
pub(crate) mod batch_attr;
pub(crate) mod batch_operate_command;
pub(crate) mod buffer;
pub(crate) mod delete_command;
pub(crate) mod execute_udf_command;
pub(crate) mod exists_command;
pub(crate) mod info_command;
pub(crate) mod operate_command;
pub(crate) mod particle_type;
pub(crate) mod query_command;
pub(crate) mod read_command;
pub(crate) mod scan_command;
pub(crate) mod single_command;
pub(crate) mod stream_command;
pub(crate) mod touch_command;
pub(crate) mod write_command;

mod field_type;

use std::sync::Arc;
use std::time::Duration;

pub(crate) use self::batch_attr::BatchAttr;
pub(crate) use self::batch_operate_command::BatchOperateCommand;
pub(crate) use self::delete_command::DeleteCommand;
pub(crate) use self::execute_udf_command::ExecuteUDFCommand;
pub(crate) use self::exists_command::ExistsCommand;
pub(crate) use self::info_command::Message;
pub(crate) use self::operate_command::OperateCommand;
pub(crate) use self::particle_type::ParticleType;
pub(crate) use self::query_command::QueryCommand;
pub(crate) use self::read_command::ReadCommand;
pub(crate) use self::scan_command::ScanCommand;
pub(crate) use self::single_command::SingleCommand;
pub(crate) use self::stream_command::StreamCommand;
pub(crate) use self::touch_command::TouchCommand;
pub(crate) use self::write_command::WriteCommand;

use crate::cluster::Node;
use crate::errors::{Error, Result};
use crate::net::Connection;
use crate::ResultCode;

// Command interface describes all commands available
#[async_trait::async_trait]
pub(crate) trait Command {
    async fn write_timeout(
        &mut self,
        conn: &mut Connection,
        timeout: Option<Duration>,
    ) -> Result<()>;
    async fn prepare_buffer(&mut self, conn: &mut Connection) -> Result<()>;
    async fn get_node(&mut self) -> Result<Arc<Node>>;
    async fn parse_result(&mut self, conn: &mut Connection) -> Result<()>;
    async fn write_buffer(&mut self, conn: &mut Connection) -> Result<()>;
}

pub(crate) const fn keep_connection(err: &Error) -> bool {
    matches!(err, Error::ServerError(ResultCode::KeyNotFoundError, _, _))
}
