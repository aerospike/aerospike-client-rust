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
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::time::Duration;
use std::str;

use errors::*;
use Key;
use Record;
use ResultCode;
use Value;
use cluster::{Node, Cluster};
use commands::buffer;
use commands::{Command, SingleCommand};
use net::Connection;
use policy::ReadPolicy;
use value::bytes_to_particle;

pub struct ReadCommand<'a> {
    pub single_command: SingleCommand<'a>,
    pub record: Option<Record>,
    policy: &'a ReadPolicy,
    bin_names: Option<&'a [&'a str]>,
}

impl<'a> ReadCommand<'a> {
    pub fn new(policy: &'a ReadPolicy,
               cluster: Arc<Cluster>,
               key: &'a Key,
               bin_names: Option<&'a [&'a str]>)
               -> Self {
        ReadCommand {
            single_command: SingleCommand::new(cluster, key),
            bin_names: bin_names,
            policy: policy,
            record: None,
        }
    }

    fn parse_record(&mut self,
                    conn: &mut Connection,
                    op_count: usize,
                    field_count: usize,
                    generation: u32,
                    expiration: u32)
                    -> Result<Record> {
        let mut bins: HashMap<String, Value> = HashMap::with_capacity(op_count);

        // There can be fields in the response (setname etc). For now, ignore them. Expose them to
        // the API if needed in the future.
        for _ in 0..field_count {
            let field_size = conn.buffer.read_u32(None)? as usize;
            conn.buffer.skip(4 + field_size)?;
        }

        for _ in 0..op_count {
            let op_size = try!(conn.buffer.read_u32(None)) as usize;
            try!(conn.buffer.skip(1));
            let particle_type = try!(conn.buffer.read_u8(None));
            try!(conn.buffer.skip(1));
            let name_size = try!(conn.buffer.read_u8(None)) as usize;
            let name: String = try!(conn.buffer.read_str(name_size));

            let particle_bytes_size = op_size - (4 + name_size);
            let value = bytes_to_particle(particle_type, &mut conn.buffer, particle_bytes_size)?;

            if !value.is_nil() {
                // list/map operations may return multiple values for the same bin.
                match bins.entry(name) {
                    Vacant(entry) => { entry.insert(value); () },
                    Occupied(entry) => match *entry.into_mut() {
                        Value::List(ref mut list) => list.push(value),
                        ref mut prev => { *prev = as_list!(prev.clone(), value); () },
                    },
                }
            }
        }

        Ok(Record::new(None, bins, generation, expiration))
    }

    pub fn execute(&mut self) -> Result<()> {
        SingleCommand::execute(self.policy, self)
    }
}

impl<'a> Command for ReadCommand<'a> {
    fn write_timeout(&mut self, conn: &mut Connection, timeout: Option<Duration>) -> Result<()> {
        conn.buffer.write_timeout(timeout);
        Ok(())
    }

    fn write_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.flush()
    }

    fn prepare_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.buffer.set_read(self.policy, self.single_command.key, self.bin_names)
    }

    fn get_node(&self) -> Result<Arc<Node>> {
        self.single_command.get_node()
    }

    fn parse_result(&mut self, conn: &mut Connection) -> Result<()> {
        if let Err(err) = conn.read_buffer(buffer::MSG_TOTAL_HEADER_SIZE as usize) {
            warn!("Parse result error: {}", err);
            bail!(err);
        }

        conn.buffer.reset_offset()?;
        let sz = conn.buffer.read_u64(Some(0))?;
        let header_length = conn.buffer.read_u8(Some(8))?;
        let result_code = conn.buffer.read_u8(Some(13))?;
        let generation = conn.buffer.read_u32(Some(14))?;
        let expiration = conn.buffer.read_u32(Some(18))?;
        let field_count = conn.buffer.read_u16(Some(26))? as usize; // almost certainly 0
        let op_count = conn.buffer.read_u16(Some(28))? as usize;
        let receive_size = ((sz & 0xFFFFFFFFFFFF) - header_length as u64) as usize;

        // Read remaining message bytes
        if receive_size > 0 {
            if let Err(err) = conn.read_buffer(receive_size) {
                warn!("Parse result error: {}", err);
                bail!(err);
            }
        }

        match ResultCode::from(result_code) {
            ResultCode::Ok => {
                let record =
                    self.parse_record(conn, op_count, field_count, generation, expiration)?;
                self.record = Some(record);
                Ok(())
            }
            ResultCode::UdfBadResponse => {
                // record bin "FAILURE" contains details about the UDF error
                let record =
                    self.parse_record(conn, op_count, field_count, generation, expiration)?;
                let reason =
                    record.bins.get("FAILURE").map_or(String::from("UDF Error"), |v| v.to_string());
                Err(ErrorKind::UdfBadResponse(reason).into())
            }
            rc => Err(ErrorKind::ServerError(rc).into()),
        }
    }
}
