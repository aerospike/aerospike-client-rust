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
use std::io::Write;
use std::collections::{HashMap};
use std::time::{Instant, Duration};
use std::str;

use byteorder::{NetworkEndian, ReadBytesExt, WriteBytesExt, ByteOrder};

use net::Connection;
use error::{AerospikeError, ResultCode, AerospikeResult};
use value::{Value};

use net::Host;
use cluster::node_validator::NodeValidator;
use cluster::partition_tokenizer::PartitionTokenizer;
use cluster::partition::Partition;
use cluster::{Node, Cluster};
use common::{Key, Record, OperationType, FieldType, ParticleType};
use policy::{ClientPolicy, ReadPolicy, Policy, ConsistencyLevel};
use common::operation;
use command::command::Command;
use command::single_command::SingleCommand;
use command::buffer;
use command::buffer::{Buffer};
use value::value;

pub struct ReadCommand<'a> {
    pub single_command: SingleCommand<'a>,

    policy: &'a ReadPolicy,
    bin_names: Option<&'a [&'a str]>,

    pub record: Option<Arc<Record<'a>>>,
}

impl<'a> ReadCommand<'a> {

    pub fn new(policy: &'a ReadPolicy, cluster: Arc<Cluster>, key: &'a Key<'a>, bin_names: Option<&'a [&'a str]>) -> AerospikeResult<Self> {
        Ok(ReadCommand {
            single_command: try!(SingleCommand::new(cluster, key)),

            bin_names: bin_names,
            policy: policy,
            record: None,
        })
    }

    fn handle_udf_error(&self, result_code: isize, bins: &HashMap<String, Value>) -> AerospikeError {
        if let Some(ret) = bins.get("FAILURE") {
            return AerospikeError::new(result_code, Some(ret.to_string()));
        }
        return AerospikeError::new(result_code, None);
    }

    fn parse_record(&mut self, conn: &mut Connection, op_count: usize, field_count: usize, generation: u32, expiration: u32) -> AerospikeResult<Arc<Record<'a>>> {
        let mut bins: HashMap<String, Value> = HashMap::with_capacity(op_count);

        // There can be fields in the response (setname etc).
        // But for now, ignore them. Expose them to the API if needed in the future.
        // Logger.Debug("field count: %d, databuffer: %v", field_count, conn.buffer)
        if field_count > 0 {
            // Just skip over all the fields
            for _ in 0..field_count {
                // debug!("Receive Offset: {}", receive_offset);
                let field_size = try!(conn.buffer.read_u32(None)) as usize;
                conn.buffer.skip_bytes(4 + field_size);
            }
        }

        for _ in 0..op_count {
            let op_size = try!(conn.buffer.read_u32(None)) as usize;
            conn.buffer.skip_bytes(1);
            let particle_type = try!(conn.buffer.read_u8(None));
            conn.buffer.skip_bytes(1);
            let name_size = try!(conn.buffer.read_u8(None)) as usize;
            let name: String = try!(conn.buffer.read_str(name_size));

            let particle_bytes_size = op_size - (4 + name_size);
            let value = try!(value::bytes_to_particle(particle_type, &mut conn.buffer, particle_bytes_size));

            if let Some(value) = value {
                // for operate list command results
                if bins.contains_key(&name) {
                    let prev = bins.get_mut(&name).unwrap();
                    match prev {
                        &mut Value::List(ref mut prev) => {
                            prev.push(value);
                        },
                        _ => {
                            *prev = Value::from(vec![prev.clone(), value]);
                        },

                    }
                } else {
                    bins.insert(name, value);
                }
            }
        }

        Ok(Arc::new(try!(Record::new(self.single_command.key, bins, generation, expiration))))
    }

    pub fn execute(&mut self) -> AerospikeResult<()> {
        SingleCommand::execute(self.policy, self)
    }

}

impl<'a> Command for ReadCommand<'a> {

    fn write_timeout(&mut self, conn: &mut Connection, timeout: Option<Duration>) -> AerospikeResult<()> {
        conn.buffer.write_timeout(timeout);
        Ok(())
    }

    fn write_buffer(&mut self, conn: &mut Connection) -> AerospikeResult<()> {
        conn.flush()
    }

    fn prepare_buffer(&mut self, conn: &mut Connection) -> AerospikeResult<()> {
        conn.buffer.set_read(self.policy, self.single_command.key, self.bin_names)
    }

    fn get_node(&self) -> AerospikeResult<Arc<Node>> {
        self.single_command.get_node()
    }

    fn parse_result(&mut self, conn: &mut Connection) -> AerospikeResult<()> {
        // Read header.
        if let Err(err) = conn.read_buffer(buffer::MSG_TOTAL_HEADER_SIZE as usize) {
            warn!("Parse result error: {}", err);
            return Err(err);
        }

        try!(conn.buffer.reset_offset());

        // A number of these are commented out because we just don't care enough to read
        // that section of the header. If we do care, uncomment and check!
        let sz = try!(conn.buffer.read_u64(Some(0)));
        let header_length = try!(conn.buffer.read_u8(Some(8)));
        let result_code = (try!(conn.buffer.read_u8(Some(13))) & 0xFF) as isize;
        let generation = try!(conn.buffer.read_u32(Some(14)));
        let expiration = try!(conn.buffer.read_u32(Some(18)));
        let field_count = try!(conn.buffer.read_u16(Some(26))) as usize; // almost certainly 0
        let op_count = try!(conn.buffer.read_u16(Some(28))) as usize;
        let receive_size = ((sz & 0xFFFFFFFFFFFF) - header_length as u64) as usize;

        // Read remaining message bytes.
        if receive_size > 0 {
            if let Err(err) = conn.read_buffer(receive_size) {
                warn!("Parse result error: {}", err);
                return Err(err);
            }
        }

        if result_code != 0 {
            if result_code == ResultCode::UDF_BAD_RESPONSE {
                let record = try!(self.parse_record(conn, op_count, field_count, generation, expiration));
                let err = self.handle_udf_error(result_code, &record.bins);
                warn!("UDF execution error: {}", err);
                return Err(err)
            }

            return Err(AerospikeError::new(result_code, None));
        }

        if op_count == 0 {
            // data Bin was not returned
            self.record = Some(Arc::new(try!(Record::new(self.single_command.key, HashMap::new(), generation, expiration))));
            return Ok(())
        }

        self.record = Some(try!(self.parse_record(conn, op_count, field_count, generation, expiration)));
        Ok(())
    }

}