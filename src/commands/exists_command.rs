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

use std::sync::Arc;
use std::time::Duration;

use cluster::{Cluster, Node};
use commands::buffer;
use commands::{Command, SingleCommand};
use errors::*;
use net::Connection;
use policy::WritePolicy;
use Key;
use ResultCode;

pub struct ExistsCommand<'a> {
    single_command: SingleCommand<'a>,
    policy: &'a WritePolicy,
    pub exists: bool,
}

impl<'a> ExistsCommand<'a> {
    pub fn new(policy: &'a WritePolicy, cluster: Arc<Cluster>, key: &'a Key) -> Self {
        ExistsCommand {
            single_command: SingleCommand::new(cluster, key),
            policy,
            exists: false,
        }
    }

    pub fn execute(&mut self) -> Result<()> {
        SingleCommand::execute(self.policy, self)
    }
}

impl<'a> Command for ExistsCommand<'a> {
    fn write_timeout(&mut self, conn: &mut Connection, timeout: Option<Duration>) -> Result<()> {
        conn.buffer.write_timeout(timeout);
        Ok(())
    }

    fn write_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.flush()
    }

    fn prepare_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.buffer.set_exists(self.policy, self.single_command.key)
    }

    fn get_node(&self) -> Result<Arc<Node>> {
        self.single_command.get_node()
    }

    fn parse_result(&mut self, conn: &mut Connection) -> Result<()> {
        // Read header.
        if let Err(err) = conn.read_buffer(buffer::MSG_TOTAL_HEADER_SIZE as usize) {
            warn!("Parse result error: {}", err);
            return Err(err);
        }

        conn.buffer.reset_offset()?;

        // A number of these are commented out because we just don't care enough to read
        // that section of the header. If we do care, uncomment and check!
        let result_code = ResultCode::from(conn.buffer.read_u8(Some(13))?);

        if result_code != ResultCode::Ok && result_code != ResultCode::KeyNotFoundError {
            bail!(ErrorKind::ServerError(result_code));
        }

        self.exists = result_code == ResultCode::Ok;

        SingleCommand::empty_socket(conn)
    }
}
