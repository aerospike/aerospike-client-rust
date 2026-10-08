// Copyright 2015-2026 Aerospike, Inc.
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

use crate::cluster::partition::Partition;
use crate::cluster::{Cluster, Node};
use crate::commands::{Command, ReadCommand, SingleCommand};
use crate::errors::Result;
use crate::net::Connection;
use crate::operations::Operation;
use crate::policy::{Policy, WritePolicy};
use crate::{Bins, Key};

pub struct OperateCommand<'a> {
    pub read_command: ReadCommand<'a>,
    policy: &'a WritePolicy,
    operations: &'a [Operation],
}

impl<'a> OperateCommand<'a> {
    pub fn new(
        policy: &'a WritePolicy,
        cluster: Arc<Cluster>,
        key: &'a Key,
        operations: &'a [Operation],
    ) -> Self {
        // An operate with no write operation is a read: the policy's replica
        // and SC read mode pick the node, and a transaction records it as a
        // read of the key. Only an operate that can mutate the record takes
        // the write routing.
        let has_write = operations.iter().any(Operation::is_write);
        let base = &policy.base_policy;
        let partition = if has_write {
            Partition::for_write(key, base.replica)
        } else {
            Partition::for_read(&cluster, key, base.replica, base.read_mode_sc)
        };
        let mut read_command =
            ReadCommand::new_with_partition(base, cluster, key, Bins::All, partition);
        read_command.is_write = has_write;
        read_command.wants_results = true;
        OperateCommand {
            read_command,
            policy,
            operations,
        }
    }

    pub async fn execute(&mut self) -> Result<()> {
        SingleCommand::execute(self.policy, self).await
    }
}

#[async_trait::async_trait]
impl Command for OperateCommand<'_> {
    fn cluster(&self) -> Option<&Cluster> {
        Some(self.read_command.single_command.cluster())
    }

    async fn write_timeout(&mut self, conn: &mut Connection) -> Result<()> {
        conn.buffer.write_timeout(self.policy.server_timeout());
        Ok(())
    }

    async fn write_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.flush().await
    }

    async fn prepare_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.buffer.set_operate(
            self.policy,
            self.read_command.single_command.key,
            self.operations,
        )
    }

    fn can_retry(&mut self) -> bool {
        true
    }

    fn can_recover_connection(&mut self) -> bool {
        true
    }

    fn is_write(&self) -> bool {
        // Decided once at construction, so routing, transaction bookkeeping
        // and `in_doubt` all agree on whether this operate can mutate the
        // record.
        self.read_command.is_write
    }

    fn get_node(&mut self) -> Result<Arc<Node>> {
        self.read_command.get_node()
    }

    fn hint(&self) -> u8 {
        self.read_command.single_command.hint()
    }

    fn command_type(&self) -> crate::metrics::CommandType {
        crate::metrics::CommandType::Operate
    }

    fn namespace(&self) -> Option<&str> {
        Some(&self.read_command.single_command.key.namespace)
    }

    fn prepare_retry(&mut self, is_client_timeout: bool) {
        self.read_command
            .single_command
            .prepare_retry(is_client_timeout);
    }

    async fn parse_result(&mut self, conn: &mut Connection) -> Result<()> {
        self.read_command.parse_result(conn).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::cluster::Partitions;
    use crate::operations;
    use crate::policy::{ClientPolicy, ReadModeSc, Replica};

    fn prefer_rack() -> WritePolicy {
        let mut policy = WritePolicy::default();
        policy.base_policy.replica = Replica::PreferRack;
        policy
    }

    #[test]
    fn read_only_operate_routes_by_the_policy_replica() {
        let cluster = Cluster::new_unconnected(ClientPolicy::default());
        let key = as_key!("test", "set", "k");
        let ops = [operations::get_bin("a"), operations::get_header()];
        let policy = prefer_rack();

        let cmd = OperateCommand::new(&policy, cluster, &key, &ops);

        let partition = &cmd.read_command.single_command.partition;
        assert_eq!(partition.replica, Replica::PreferRack);
        assert!(!partition.is_write);
        assert!(!cmd.is_write());
    }

    #[test]
    fn read_only_operate_applies_the_sc_read_mode() {
        let cluster = Cluster::new_unconnected(ClientPolicy::default());
        let sc = Partitions {
            sc_mode: true,
            ..Partitions::default()
        };
        cluster
            .partition_map
            .store(Arc::new(HashMap::from([("test".to_string(), sc)])));
        let key = as_key!("test", "set", "k");
        let ops = [operations::get_bin("a")];
        let mut policy = prefer_rack();
        policy.base_policy.read_mode_sc = ReadModeSc::Linearize;

        let cmd = OperateCommand::new(&policy, cluster, &key, &ops);

        let partition = &cmd.read_command.single_command.partition;
        assert_eq!(partition.replica, Replica::Sequence);
        assert!(partition.linearize);
    }

    #[test]
    fn operate_with_any_write_takes_the_write_routing() {
        let cluster = Cluster::new_unconnected(ClientPolicy::default());
        let key = as_key!("test", "set", "k");
        let ops = [
            operations::get_bin("a"),
            operations::put(&as_bin!("b", 1i64)),
        ];
        let policy = prefer_rack();

        let cmd = OperateCommand::new(&policy, cluster, &key, &ops);

        let partition = &cmd.read_command.single_command.partition;
        assert!(partition.is_write);
        assert!(!partition.linearize);
        assert!(cmd.is_write());
    }
}
