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

use std::sync::Arc;
use std::collections::HashMap;
use std::time::Duration;
use std::str;

use errors::*;
use Key;
use Record;
use ResultCode;
use cluster::{Node, Cluster};
use commands::buffer;
use commands::{Command, SingleCommand};
use net::Connection;
use policy::ReadPolicy;

pub struct ReadHeaderCommand<'a> {
    single_command: SingleCommand<'a>,
    policy: &'a ReadPolicy,
    pub record: Option<Record>,
}

impl<'a> ReadHeaderCommand<'a> {
    pub fn new(policy: &'a ReadPolicy,
               cluster: Arc<Cluster>,
               key: &'a Key)
               -> Self {
        ReadHeaderCommand {
            single_command: SingleCommand::new(cluster, key),
            policy: policy,
            record: None,
        }
    }

    pub fn execute(&mut self) -> Result<()> {
        SingleCommand::execute(self.policy, self)
    }
}

impl<'a> Command for ReadHeaderCommand<'a> {
    fn write_timeout(&mut self,
                     conn: &mut Connection,
                     timeout: Option<Duration>)
                     -> Result<()> {
        conn.buffer.write_timeout(timeout);
        Ok(())
    }

    fn write_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.flush()
    }

    fn prepare_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.buffer.set_read_header(self.policy, self.single_command.key)
    }

    fn get_node(&self) -> Result<Arc<Node>> {
        self.single_command.get_node()
    }

    fn parse_result(&mut self, conn: &mut Connection) -> Result<()> {
        if let Err(err) = conn.read_buffer(buffer::MSG_TOTAL_HEADER_SIZE as usize) {
            warn!("Parse result error: {}", err);
            bail!(err);
        }

        match ResultCode::from(conn.buffer.read_u8(Some(13))?) {
            ResultCode::Ok => {
                let generation = conn.buffer.read_u32(Some(14))?;
                let expiration = conn.buffer.read_u32(Some(18))?;
                self.record = Some(Record::new(None, HashMap::new(), generation, expiration));
                SingleCommand::empty_socket(conn)
            }
            rc @ _ => Err(ErrorKind::ServerError(rc).into()),
        }
    }
}
