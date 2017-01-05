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
use std::str;

use net::Connection;
use error::AerospikeResult;

use cluster::Node;
use common::Recordset;
use policy::ScanPolicy;
use command::Command;
use command::single_command::SingleCommand;
use command::stream_command::StreamCommand;

pub struct ScanCommand<'a> {
    stream_command: StreamCommand,
    policy: &'a ScanPolicy,
    namespace: &'a str,
    set_name: &'a str,
    bin_names: &'a Option<Vec<String>>,
}

impl<'a> ScanCommand<'a> {
    pub fn new(policy: &'a ScanPolicy,
               node: Arc<Node>,
               namespace: &'a str,
               set_name: &'a str,
               bin_names: &'a Option<Vec<String>>,
               recordset: Arc<Recordset>)
               -> Self {
        ScanCommand {
            stream_command: StreamCommand::new(node, recordset),
            policy: policy,
            namespace: namespace,
            set_name: set_name,
            bin_names: bin_names,
        }
    }

    pub fn execute(&mut self) -> AerospikeResult<()> {
        SingleCommand::execute(self.policy, self)
    }
}

impl<'a> Command for ScanCommand<'a> {
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
        conn.buffer.set_scan(self.policy,
                             self.namespace,
                             self.set_name,
                             self.bin_names,
                             self.stream_command.recordset.task_id())
    }

    fn get_node(&self) -> AerospikeResult<Arc<Node>> {
        self.stream_command.get_node()
    }

    fn parse_result(&mut self, conn: &mut Connection) -> AerospikeResult<()> {
        StreamCommand::parse_result(&mut self.stream_command, conn)
    }
}
