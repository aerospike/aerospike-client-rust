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

use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::sync::Arc;

use crate::cluster::partition::Partition;
use crate::cluster::{Cluster, Node};
use crate::commands::{Command, SingleCommand};
use crate::errors::{Error, Result};
use crate::net::Connection;
use crate::policy::{BasePolicy, Policy, ReadPolicy};
use crate::value::bytes_to_particle;
use crate::{Bins, Key, Record, ResultCode, Value};

pub struct ReadCommand<'a> {
    pub single_command: SingleCommand<'a>,
    pub record: Option<Record>,
    policy: &'a BasePolicy,
    bins: Bins,
    /// When true, txn notifications use `on_write` instead of `on_read`.
    pub(crate) is_write: bool,
}

impl<'a> ReadCommand<'a> {
    pub fn new(policy: &'a ReadPolicy, cluster: Arc<Cluster>, key: &'a Key, bins: Bins) -> Self {
        let partition = Partition::for_read(
            &cluster,
            key,
            policy.replica,
            policy.base_policy.read_mode_sc,
        );
        ReadCommand {
            single_command: SingleCommand::new(cluster, key, partition),
            bins,
            policy: &policy.base_policy,
            record: None,
            is_write: false,
        }
    }

    pub const fn new_with_partition(
        policy: &'a BasePolicy,
        cluster: Arc<Cluster>,
        key: &'a Key,
        bins: Bins,
        partition: Partition<'a>,
    ) -> Self {
        ReadCommand {
            single_command: SingleCommand::new(cluster, key, partition),
            bins,
            policy,
            record: None,
            is_write: false,
        }
    }

    pub async fn execute(&mut self) -> Result<()> {
        SingleCommand::execute(self.policy, self).await
    }

    fn parse_record(
        &self,
        conn: &mut Connection,
        op_count: usize,
        field_count: usize,
        generation: u32,
        expiration: u32,
    ) -> Result<(Record, Option<u64>)> {
        let mut bins: HashMap<String, Value> = HashMap::with_capacity(op_count);

        // Parse fields, extracting record version if present (used by MRT).
        let version = conn.buffer.parse_fields_for_version(field_count);

        for _ in 0..op_count {
            let op_size = conn.buffer.read_u32(None) as usize;
            conn.buffer.skip(1);
            let particle_type = conn.buffer.read_u8(None);
            conn.buffer.skip(1);
            let name_size = conn.buffer.read_u8(None) as usize;
            let name: String = conn.buffer.read_str(name_size)?;

            let particle_bytes_size = op_size - (4 + name_size);
            let value = bytes_to_particle(particle_type, &mut conn.buffer, particle_bytes_size)?;

            if !value.is_nil() {
                // list/map operations may return multiple values for the same bin.
                match bins.entry(name) {
                    Vacant(entry) => {
                        entry.insert(value);
                    }
                    Occupied(entry) => match *entry.into_mut() {
                        Value::MultiResult(ref mut list) => list.push(value),
                        ref mut prev => {
                            *prev = Value::MultiResult(vec![prev.clone(), value]);
                        }
                    },
                }
            }
        }

        Ok((Record::new(None, bins, generation, expiration), version))
    }
}

#[async_trait::async_trait]
impl Command for ReadCommand<'_> {
    async fn write_timeout(&mut self, conn: &mut Connection) -> Result<()> {
        conn.buffer.write_timeout(self.policy.server_timeout());
        Ok(())
    }

    async fn write_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.flush().await
    }

    async fn prepare_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.buffer
            .set_read(self.policy, self.single_command.key, &self.bins)
    }

    fn get_node(&mut self) -> Result<Arc<Node>> {
        self.single_command.get_node()
    }

    fn hint(&self) -> u8 {
        self.single_command.hint()
    }

    fn can_retry(&mut self) -> bool {
        true
    }

    fn can_recover_connection(&mut self) -> bool {
        true
    }

    fn prepare_retry(&mut self, is_client_timeout: bool) {
        self.single_command.prepare_retry(is_client_timeout);
    }

    async fn parse_result(&mut self, conn: &mut Connection) -> Result<()> {
        if let Err(err) = conn.read_header().await {
            warn!("Parse result error: {err}");
            return Err(err);
        }

        conn.buffer.reset_offset();
        let sz = conn.buffer.read_u64(Some(0));
        let header_length = conn.buffer.read_u8(Some(8));
        let result_code = conn.buffer.read_u8(Some(13));
        let generation = conn.buffer.read_u32(Some(14));
        let expiration = conn.buffer.read_u32(Some(18));
        let field_count = conn.buffer.read_u16(Some(26)) as usize; // almost certainly 0
        let op_count = conn.buffer.read_u16(Some(28)) as usize;
        let receive_size = ((sz & 0xFFFF_FFFF_FFFF) - u64::from(header_length)) as usize;

        // Read remaining message bytes
        if receive_size > 0 {
            if let Err(err) = conn.read_body(receive_size).await {
                warn!("Parse result error: {err}");
                return Err(err);
            }
        }

        let rc = ResultCode::from(result_code);
        match rc {
            ResultCode::Ok => {
                let (record, version) = if self.bins.is_none() {
                    let version = conn.buffer.parse_fields_for_version(field_count);
                    (
                        Record::new(None, HashMap::new(), generation, expiration),
                        version,
                    )
                } else {
                    self.parse_record(conn, op_count, field_count, generation, expiration)?
                };

                if let Some(txn) = &self.policy.txn {
                    if self.is_write {
                        txn.on_write(self.single_command.key, version, rc);
                    } else {
                        txn.on_read(self.single_command.key, version);
                    }
                }

                self.record = Some(record);
                Ok(())
            }
            ResultCode::UdfBadResponse => {
                // record bin "FAILURE" contains details about the UDF error
                let (record, _version) =
                    self.parse_record(conn, op_count, field_count, generation, expiration)?;
                let reason = record
                    .bins
                    .get("FAILURE")
                    .map_or_else(|| String::from("UDF Error"), ToString::to_string);
                Err(Error::UdfBadResponse(reason))
            }
            rc => Err(Error::ServerError(rc, false, conn.addr.clone())),
        }
    }
}
