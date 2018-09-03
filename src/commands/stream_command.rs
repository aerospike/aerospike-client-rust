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
use std::collections::HashMap;
use std::time::Duration;
use std::thread;

use errors::*;
use Key;
use Record;
use ResultCode;
use Value;
use cluster::Node;
use commands::Command;
use commands::buffer;
use commands::field_type::FieldType;
use net::Connection;
use query::Recordset;
use value::bytes_to_particle;

pub struct StreamCommand {
    node: Arc<Node>,
    pub recordset: Arc<Recordset>,
}

impl Drop for StreamCommand {
    fn drop(&mut self) {
        // signal_end
        self.recordset.signal_end();
    }
}

impl StreamCommand {
    pub fn new(node: Arc<Node>, recordset: Arc<Recordset>) -> Self {
        StreamCommand {
            node: node,
            recordset: recordset,
        }
    }

    fn parse_record(conn: &mut Connection, size: usize) -> Result<Option<Record>> {
        // A number of these are commented out because we just don't care enough to read
        // that section of the header. If we do care, uncomment and check!
        let result_code = ResultCode::from(try!(conn.buffer.read_u8(Some(5))) & 0xFF);
        if result_code != ResultCode::Ok {
            if result_code == ResultCode::KeyNotFoundError {
                if conn.bytes_read() < size {
                    let remaining = size - conn.bytes_read();
                    try!(conn.read_buffer(remaining));
                }
                return Ok(None);
            }

            bail!(ErrorKind::ServerError(result_code));
        }

        // if cmd is the end marker of the response, do not proceed further
        let info3 = try!(conn.buffer.read_u8(Some(3)));
        if info3 & buffer::INFO3_LAST == buffer::INFO3_LAST {
            return Ok(None);
        }

        try!(conn.buffer.skip(6));
        let generation = try!(conn.buffer.read_u32(None));
        let expiration = try!(conn.buffer.read_u32(None));
        try!(conn.buffer.skip(4));
        let field_count = try!(conn.buffer.read_u16(None)) as usize; // almost certainly 0
        let op_count = try!(conn.buffer.read_u16(None)) as usize;

        let key = try!(StreamCommand::parse_key(conn, field_count));


        let mut bins: HashMap<String, Value> = HashMap::with_capacity(op_count);

        for _ in 0..op_count {
            try!(conn.read_buffer(8));
            let op_size = try!(conn.buffer.read_u32(None)) as usize;
            try!(conn.buffer.skip(1));
            let particle_type = try!(conn.buffer.read_u8(None));
            try!(conn.buffer.skip(1));
            let name_size = try!(conn.buffer.read_u8(None)) as usize;
            try!(conn.read_buffer(name_size));
            let name: String = try!(conn.buffer.read_str(name_size));

            let particle_bytes_size = op_size - (4 + name_size);
            try!(conn.read_buffer(particle_bytes_size));
            let value = bytes_to_particle(particle_type, &mut conn.buffer, particle_bytes_size)?;

            bins.insert(name, value);
        }

        let record = Record::new(Some(key), bins, generation, expiration);
        Ok(Some(record))
    }

    fn parse_stream(&mut self, conn: &mut Connection, size: usize) -> Result<bool> {

        while self.recordset.is_active() && conn.bytes_read() < size {
            // Read header.
            if let Err(err) = conn.read_buffer(buffer::MSG_REMAINING_HEADER_SIZE as usize) {
                warn!("Parse result error: {}", err);
                return Err(err);
            }

            let res = StreamCommand::parse_record(conn, size);
            match res {
                Ok(Some(mut rec)) => {
                    loop {
                        let result = self.recordset.push(Ok(rec));
                        match result {
                            None => break,
                            Some(returned) => {
                                rec = try!(returned);
                                thread::yield_now();
                            }
                        }
                    }
                }
                Ok(None) => return Ok(false),
                Err(err) => {
                    self.recordset.push(Err(err));
                    return Ok(false);
                }
            };
        }

        Ok(true)
    }

    pub fn parse_key(conn: &mut Connection, field_count: usize) -> Result<Key> {
        let mut digest: [u8; 20] = [0; 20];
        let mut namespace: String = "".to_string();
        let mut set_name: String = "".to_string();
        let mut orig_key: Option<Value> = None;

        for _ in 0..field_count {
            try!(conn.read_buffer(4));
            let field_len = try!(conn.buffer.read_u32(None)) as usize;
            try!(conn.read_buffer(field_len));
            let field_type = try!(conn.buffer.read_u8(None));

            match field_type {
                x if x == FieldType::DigestRipe as u8 => {
                    digest.copy_from_slice(try!(conn.buffer.read_slice(field_len - 1)));
                }
                x if x == FieldType::Namespace as u8 => {
                    namespace = try!(conn.buffer.read_str(field_len - 1));
                }
                x if x == FieldType::Table as u8 => {
                    set_name = try!(conn.buffer.read_str(field_len - 1));
                }
                x if x == FieldType::Key as u8 => {
                    let particle_type = try!(conn.buffer.read_u8(None));
                    let particle_bytes_size = field_len - 2;
                    orig_key = Some(bytes_to_particle(particle_type,
                                                      &mut conn.buffer,
                                                      particle_bytes_size)?);
                }
                _ => unreachable!(),
            }
        }


        Ok(Key {
            namespace: namespace,
            set_name: set_name,
            user_key: orig_key,
            digest: digest,
        })
    }
}

impl Command for StreamCommand {
    fn write_timeout(&mut self, conn: &mut Connection, timeout: Option<Duration>) -> Result<()> {
        conn.buffer.write_timeout(timeout);
        Ok(())
    }

    fn write_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.flush()
    }

    #[allow(unused_variables)]
    fn prepare_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        // should be implemented downstream
        unreachable!()
    }

    fn get_node(&self) -> Result<Arc<Node>> {
        Ok(self.node.clone())
    }

    fn parse_result(&mut self, conn: &mut Connection) -> Result<()> {
        let mut status = true;

        while status {
            try!(conn.read_buffer(8));
            let size = try!(conn.buffer.read_msg_size(None));
            conn.bookmark();

            status = false;
            if size > 0 {
                status = try!(self.parse_stream(conn, size as usize));
            }
        }

        Ok(())
    }
}
