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
use value::Value;

use cluster::{Node, Cluster};
use common::Key;
use policy::WritePolicy;
use command::Command;
use command::single_command::SingleCommand;
use command::read_command::ReadCommand;

pub struct ExecuteUDFCommand<'a> {
    pub read_command: ReadCommand<'a>,

    policy: &'a WritePolicy,
    package_name: &'a str,
    function_name: &'a str,
    args: Option<&'a [Value]>,
}

impl<'a> ExecuteUDFCommand<'a> {
    pub fn new(policy: &'a WritePolicy,
               cluster: Arc<Cluster>,
               key: &'a Key,
               package_name: &'a str,
               function_name: &'a str,
               args: Option<&'a [Value]>)
               -> AerospikeResult<Self> {
        Ok(ExecuteUDFCommand {
            read_command: try!(ReadCommand::new(&policy.base_policy, cluster, key, None)),

            policy: policy,
            package_name: package_name,
            function_name: function_name,
            args: args,
        })
    }

    pub fn execute(&mut self) -> AerospikeResult<()> {
        SingleCommand::execute(self.policy, self)
    }
}

impl<'a> Command for ExecuteUDFCommand<'a> {
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
        conn.buffer.set_udf(self.policy,
                            self.read_command.single_command.key,
                            self.package_name,
                            self.function_name,
                            self.args)
    }

    fn get_node(&self) -> AerospikeResult<Arc<Node>> {
        self.read_command.get_node()
    }

    fn parse_result(&mut self, conn: &mut Connection) -> AerospikeResult<()> {
        self.read_command.parse_result(conn)
    }
}
