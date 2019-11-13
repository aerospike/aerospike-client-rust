// Copyright 2015-2018 Aerospike, Inc.
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

use std::io::prelude::*;
use std::net::{Shutdown, TcpStream, ToSocketAddrs};
use std::ops::Add;
use std::time::{Duration, Instant};

use commands::admin_command::AdminCommand;
use commands::buffer::Buffer;
use errors::*;
use policy::ClientPolicy;

#[derive(Debug)]
pub struct Connection {
    timeout: Option<Duration>,

    // duration after which connection is considered idle
    idle_timeout: Option<Duration>,
    idle_deadline: Option<Instant>,

    // connection object
    conn: TcpStream,

    bytes_read: usize,

    pub buffer: Buffer,
}

impl Connection {
    pub fn new<T: ToSocketAddrs>(addr: T, policy: &ClientPolicy) -> Result<Self> {
        let stream = TcpStream::connect(addr)?;
        let mut conn = Connection {
            buffer: Buffer::new(),
            bytes_read: 0,
            timeout: policy.timeout,
            conn: stream,
            idle_timeout: policy.idle_timeout,
            idle_deadline: match policy.idle_timeout {
                None => None,
                Some(timeout) => Some(Instant::now() + timeout),
            },
        };
        conn.authenticate(&policy.user_password)?;
        conn.refresh();
        Ok(conn)
    }

    pub fn close(&mut self) {
        let _ = self.conn.shutdown(Shutdown::Both);
    }

    pub fn flush(&mut self) -> Result<()> {
        self.conn.write_all(&self.buffer.data_buffer)?;
        self.refresh();
        Ok(())
    }

    pub fn read_buffer(&mut self, size: usize) -> Result<()> {
        self.buffer.resize_buffer(size)?;
        self.conn.read_exact(&mut self.buffer.data_buffer)?;
        self.bytes_read += size;
        self.buffer.reset_offset()?;
        self.refresh();
        Ok(())
    }

    pub fn write(&mut self, buf: &[u8]) -> Result<()> {
        self.conn.write_all(buf)?;
        self.refresh();
        Ok(())
    }

    pub fn read(&mut self, buf: &mut [u8]) -> Result<()> {
        self.conn.read_exact(buf)?;
        self.bytes_read += buf.len();
        self.refresh();
        Ok(())
    }

    pub fn set_timeout(&self, timeout: Option<Duration>) -> Result<()> {
        self.conn.set_read_timeout(timeout)?;
        self.conn.set_write_timeout(timeout)?;
        Ok(())
    }

    pub fn is_idle(&self) -> bool {
        if let Some(idle_dl) = self.idle_deadline {
            Instant::now() >= idle_dl
        } else {
            false
        }
    }

    fn refresh(&mut self) {
        self.idle_deadline = None;
        if let Some(idle_to) = self.idle_timeout {
            self.idle_deadline = Some(Instant::now().add(idle_to))
        };
    }

    fn authenticate(&mut self, user_password: &Option<(String, String)>) -> Result<()> {
        if let Some((ref user, ref password)) = *user_password {
            match AdminCommand::authenticate(self, user, password) {
                Ok(()) => {
                    return Ok(());
                }
                Err(err) => {
                    self.close();
                    return Err(err);
                }
            }
        }

        Ok(())
    }

    pub fn bookmark(&mut self) {
        self.bytes_read = 0;
    }

    pub fn bytes_read(&self) -> usize {
        self.bytes_read
    }
}
