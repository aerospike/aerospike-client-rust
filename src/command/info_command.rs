// Copyright 2015-2016 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

use std::io::{Cursor, Read, Write};
use std::collections::HashMap;
use byteorder::{NetworkEndian, ReadBytesExt, WriteBytesExt};
use std::str;
use std::sync::{Arc, Mutex};

use net::Connection;
use error::{AerospikeError, ResultCode, AerospikeResult};

enum MessageType {
    Info = 1,
    Message = 3,
}


#[derive(Debug, Clone)]
pub struct Message {
    buf: Vec<u8>,
}

impl Message {
    pub fn info(conn: &mut Connection,
                commands: &[&str])
                -> AerospikeResult<HashMap<String, String>> {

        let cmd = commands.join("\n") + "\n";
        let mut msg = try!(Message::new(MessageType::Info, &cmd.into_bytes()));

        try!(msg.send(conn));
        Ok(try!(msg.parse_response()))
    }

    fn new(msg_type: MessageType, data: &[u8]) -> AerospikeResult<Self> {
        let mut len = Vec::with_capacity(8);
        len.write_u64::<NetworkEndian>(data.len() as u64).unwrap();

        let mut buf: Vec<u8> = Vec::with_capacity(1024);
        buf.push(2); // version
        buf.push(msg_type as u8); // msg_type
        try!(buf.write(&len[2..8]));
        try!(buf.write(&data));

        Ok(Message { buf: buf })
    }

    fn version(&self) -> isize {
        self.buf[0] as isize
    }

    fn msg_type(&self) -> MessageType {
        match self.buf[1] {
            1 => MessageType::Info,
            3 => MessageType::Message,
            _ => unreachable!(),
        }
    }

    fn data_len(&self) -> u64 {
        let mut lbuf: Vec<u8> = vec![0; 8];
        lbuf[2..8].clone_from_slice(&self.buf[2..8]);
        let mut rdr = Cursor::new(lbuf);
        rdr.read_u64::<NetworkEndian>().unwrap()
    }

    fn send(&mut self, conn: &mut Connection) -> AerospikeResult<()> {
        // send the message
        try!(conn.write(&self.buf));

        // read the header
        try!(conn.read(self.buf[..8].as_mut()));

        // figure our message size and grow the buffer if necessary
        let data_len = self.data_len() as usize;
        self.buf.resize(data_len, 0);

        // read the message content
        try!(conn.read(self.buf.as_mut()));

        Ok(())
    }

    fn parse_response(&self) -> AerospikeResult<HashMap<String, String>> {
        let response = try!(str::from_utf8(&self.buf));
        let response = response.trim_matches('\n');

        debug!("response from server for info command: {:?}", response);
        let mut result: HashMap<String, String> = HashMap::new();

        for tuple in response.split("\n") {
            let mut kv = tuple.split("\t");
            let key = kv.nth(0);
            let val = kv.nth(0);

            match (key, val) {
                (Some(key), Some(val)) => result.insert(key.to_string(), val.to_string()),
                (Some(key), None) => result.insert(key.to_string(), "".to_string()),
                _ => {
                    return Err(AerospikeError::new(ResultCode::PARSE_ERROR,
                                                   Some("parsing info command failed".to_string())))
                }
            };
        }

        Ok(result)
    }
}

// #[test]
// fn test_info() {
//     let data: Vec<u8> = vec![0];
//     let mut msg = Message::new(MessageType::Message, &data);
//     assert_eq!(1, msg.data_len());
// }
