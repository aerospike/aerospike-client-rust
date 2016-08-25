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

use std::sync::{Mutex, Arc};
use std::io::prelude::*;
use std::net::TcpStream;
use std::time::{Instant, Duration};
use std::io::{Error, ErrorKind};
use std::net::Shutdown;
use std::ops::Add;
use std::default::Default;

use policy::client_policy::ClientPolicy;
use error::{AerospikeResult, ResultCode, AerospikeError};
use cluster::node::Node;
use Host;
use command::buffer::Buffer;

#[derive(Debug)]
pub struct Connection {
    timeout: Option<Duration>,

    // duration after which connection is considered idle
    idle_timeout: Option<Duration>,
    idle_deadline: Option<Instant>,

    // connection object
    conn: TcpStream,

    pub buffer: Buffer,
}

impl Connection {
    pub fn new_raw(host: &Host) -> AerospikeResult<Self> {
        let s: &str = &host.name;
        let stream = try!(TcpStream::connect((s, host.port)));

        let cpolicy: ClientPolicy = Default::default();

        let mut conn = Connection {
            // node: None,
            buffer: Buffer::new(),
            timeout: cpolicy.timeout,
            conn: stream,

            idle_timeout: cpolicy.idle_timeout,
            idle_deadline: match cpolicy.idle_timeout {
                None => None,
                Some(timeout) => Some(Instant::now() + timeout),
            },
        };

        conn.refresh();

        Ok(conn)
    }

    pub fn new(node: &Node) -> AerospikeResult<Self> {
        let nd = node;

        let stream = try!(TcpStream::connect(nd.address()));

        let cpolicy = node.client_policy();

        let mut conn = Connection {
            buffer: Buffer::new(),
            // node: Some(node),
            timeout: cpolicy.timeout,
            conn: stream,

            idle_timeout: cpolicy.idle_timeout,
            idle_deadline: match cpolicy.idle_timeout {
                None => None,
                Some(timeout) => Some(Instant::now() + timeout),
            },
        };

        conn.refresh();

        Ok(conn)
    }

    pub fn close(&mut self) {
        self.conn.shutdown(Shutdown::Both);
    }

    pub fn flush(&mut self) -> AerospikeResult<()> {
        try!(self.conn.write_all(&self.buffer.data_buffer));
        self.refresh();
        return Ok(());
    }


    pub fn read_buffer(&mut self, size: usize) -> AerospikeResult<()> {
        try!(self.buffer.resize_buffer(size));
        try!(self.conn.read_exact(&mut self.buffer.data_buffer));
        self.refresh();
        return Ok(());
    }


    pub fn write(&mut self, buf: &[u8]) -> AerospikeResult<()> {
        try!(self.conn.write_all(buf));
        self.refresh();
        return Ok(());
    }

    pub fn read(&mut self, buf: &mut [u8]) -> AerospikeResult<()> {
        try!(self.conn.read_exact(buf));
        self.refresh();
        return Ok(());
    }

    pub fn set_timeout(&self, timeout: Option<Duration>) -> AerospikeResult<()> {
        try!(self.conn.set_read_timeout(timeout));
        try!(self.conn.set_write_timeout(timeout));
        Ok(())
    }

    pub fn is_idle(&self) -> bool {
        if let Some(idle_dl) = self.idle_deadline {
            return Instant::now() >= idle_dl;
        };

        return false;
    }

    fn refresh(&mut self) {
        self.idle_deadline = None;
        if let Some(idle_to) = self.idle_timeout {
            self.idle_deadline = Some(Instant::now().add(idle_to))
        };
    }
}

// TODO: implement this
// impl Drop for Connection {
//     fn drop(&mut self) {
//         if let Some(node) = self.node {
//             node.dec_connections();
//         }
//         self.conn.shutdown(Shutdown::Both);
//     }
// }
