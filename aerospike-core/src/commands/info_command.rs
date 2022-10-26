// Copyright 2015-2020 Aerospike, Inc.
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

use byteorder::{NetworkEndian, ReadBytesExt, WriteBytesExt};
use std::collections::HashMap;
use std::io::{Cursor, Write};
use std::str;

use crate::errors::Result;
use crate::net::Connection;

// MAX_BUFFER_SIZE protects against allocating massive memory blocks
// for buffers.
const MAX_BUFFER_SIZE: usize = 1024 * 1024 + 8; // 1 MB + header

#[derive(Debug, Clone)]
pub struct Message {
    buf: Vec<u8>,
}

impl Message {
    pub async fn info(conn: &mut Connection, commands: &[&str]) -> Result<HashMap<String, String>> {
        let cmd = commands.join("\n") + "\n";
        let mut msg = Message::new(&cmd.into_bytes())?;

        msg.send(conn).await?;
        msg.parse_response()
    }

    fn new(data: &[u8]) -> Result<Self> {
        let mut len = Vec::with_capacity(8);
        len.write_u64::<NetworkEndian>(data.len() as u64).unwrap();

        let mut buf: Vec<u8> = Vec::with_capacity(1024);
        buf.push(2); // version
        buf.push(1); // msg_type
        buf.write_all(&len[2..8])?;
        buf.write_all(data)?;

        Ok(Message { buf })
    }

    fn data_len(&self) -> u64 {
        let mut lbuf: Vec<u8> = vec![0; 8];
        lbuf[2..8].clone_from_slice(&self.buf[2..8]);
        let mut rdr = Cursor::new(lbuf);
        rdr.read_u64::<NetworkEndian>().unwrap()
    }

    async fn send(&mut self, conn: &mut Connection) -> Result<()> {
        conn.write(&self.buf).await?;

        // read the header
        conn.read(self.buf[..8].as_mut()).await?;

        // figure our message size and grow the buffer if necessary
        let data_len = self.data_len() as usize;

        // Corrupted data streams can result in a huge length.
        // Do a sanity check here.
        if data_len > MAX_BUFFER_SIZE {
            bail!("Invalid size for info command buffer: {}", data_len);
        }
        self.buf.resize(data_len, 0);

        // read the message content
        conn.read(self.buf.as_mut()).await?;

        Ok(())
    }

    fn parse_response(&self) -> Result<HashMap<String, String>> {
        let response = str::from_utf8(&self.buf)?;
        let response = response.trim_matches('\n');

        debug!("response from server for info command: {:?}", response);
        let mut result: HashMap<String, String> = HashMap::new();

        for tuple in response.split('\n') {
            let mut kv = tuple.split('\t');
            let key = kv.next();
            let val = kv.next();

            match (key, val) {
                (Some(key), Some(val)) => result.insert(key.to_string(), val.to_string()),
                (Some(key), None) => result.insert(key.to_string(), "".to_string()),
                _ => bail!("Parsing Info command failed"),
            };
        }

        Ok(result)
    }
}
