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

use std::sync::Arc;
use std::time::Duration;

use net::Connection;
use error::AerospikeResult;

use cluster::Node;
use common::{Recordset, Statement};
use policy::QueryPolicy;
use command::Command;
use command::single_command::SingleCommand;
use command::stream_command::StreamCommand;

pub struct QueryCommand<'a> {
    stream_command: StreamCommand,

    policy: &'a QueryPolicy,
    statement: Arc<Statement>,
}

impl<'a> QueryCommand<'a> {
    pub fn new(policy: &'a QueryPolicy,
               node: Arc<Node>,
               statement: Arc<Statement>,
               recordset: Arc<Recordset>)
               -> AerospikeResult<Self> {
        Ok(QueryCommand {
            stream_command: try!(StreamCommand::new(node, recordset)),

            policy: policy,
            statement: statement,
        })
    }

    pub fn execute(&mut self) -> AerospikeResult<()> {
        SingleCommand::execute(self.policy, self)
    }
}

impl<'a> Command for QueryCommand<'a> {
    fn write_timeout(&mut self,
                     conn: &mut Connection,
                     timeout: Option<Duration>)
                     -> AerospikeResult<()> {
        conn.buffer.write_timeout(timeout);
        Ok(())
    }

    fn write_buffer(&mut self, conn: &mut Connection) -> AerospikeResult<()> {
        conn.flush()
    }

    fn prepare_buffer(&mut self, conn: &mut Connection) -> AerospikeResult<()> {
        conn.buffer.set_query(self.policy,
                              &self.statement,
                              false,
                              self.stream_command.recordset.task_id())
    }

    fn get_node(&self) -> AerospikeResult<Arc<Node>> {
        self.stream_command.get_node()
    }

    fn parse_result(&mut self, conn: &mut Connection) -> AerospikeResult<()> {
        StreamCommand::parse_result(&mut self.stream_command, conn)
    }
}
