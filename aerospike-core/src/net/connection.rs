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

use crate::commands::admin_command::AdminCommand;
use crate::commands::buffer::Buffer;
use crate::errors::{ErrorKind, Result};
use crate::policy::ClientPolicy;
use crate::derive::readable::{PreParsedBin, PreParsedValue};
#[cfg(all(any(feature = "rt-async-std"), not(feature = "rt-tokio")))]
use aerospike_rt::async_std::net::Shutdown;
#[cfg(all(any(feature = "rt-tokio"), not(feature = "rt-async-std")))]
use aerospike_rt::io::{AsyncReadExt, AsyncWriteExt};
use aerospike_rt::net::TcpStream;
use aerospike_rt::time::{Duration, Instant};
#[cfg(all(any(feature = "rt-async-std"), not(feature = "rt-tokio")))]
use futures::{AsyncReadExt, AsyncWriteExt};
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::ops::Add;

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
    pub async fn new(addr: &str, policy: &ClientPolicy) -> Result<Self> {
        let stream = aerospike_rt::timeout(Duration::from_secs(10), TcpStream::connect(addr)).await;
        if stream.is_err() {
            bail!(ErrorKind::Connection(
                "Could not open network connection".to_string()
            ));
        }
        let mut conn = Connection {
            buffer: Buffer::new(policy.buffer_reclaim_threshold),
            bytes_read: 0,
            timeout: policy.timeout,
            conn: stream.unwrap()?,
            idle_timeout: policy.idle_timeout,
            idle_deadline: policy.idle_timeout.map(|timeout| Instant::now() + timeout),
        };
        conn.authenticate(&policy.user_password).await?;
        conn.refresh();
        Ok(conn)
    }

    pub async fn close(&mut self) {
        #[cfg(all(any(feature = "rt-async-std"), not(feature = "rt-tokio")))]
        let _s = self.conn.shutdown(Shutdown::Both);
        #[cfg(all(any(feature = "rt-tokio"), not(feature = "rt-async-std")))]
        let _s = self.conn.shutdown().await;
    }

    pub async fn flush(&mut self) -> Result<()> {
        self.conn.write_all(&self.buffer.data_buffer).await?;
        self.refresh();
        Ok(())
    }

    pub async fn read_buffer(&mut self, size: usize) -> Result<()> {
        self.buffer.resize_buffer(size)?;
        self.conn.read_exact(&mut self.buffer.data_buffer).await?;
        self.bytes_read += size;
        self.buffer.reset_offset();
        self.refresh();
        Ok(())
    }

    pub async fn write(&mut self, buf: &[u8]) -> Result<()> {
        self.conn.write_all(buf).await?;
        self.refresh();
        Ok(())
    }

    pub async fn read(&mut self, buf: &mut [u8]) -> Result<()> {
        self.conn.read_exact(buf).await?;
        self.bytes_read += buf.len();
        self.refresh();
        Ok(())
    }

    pub fn is_idle(&self) -> bool {
        self.idle_deadline
            .map_or(false, |idle_dl| Instant::now() >= idle_dl)
    }

    fn refresh(&mut self) {
        self.idle_deadline = None;
        if let Some(idle_to) = self.idle_timeout {
            self.idle_deadline = Some(Instant::now().add(idle_to));
        };
    }

    async fn authenticate(&mut self, user_password: &Option<(String, String)>) -> Result<()> {
        if let Some((ref user, ref password)) = *user_password {
            return match AdminCommand::authenticate(self, user, password).await {
                Ok(()) => Ok(()),
                Err(err) => {
                    self.close().await;
                    Err(err)
                }
            };
        }

        Ok(())
    }

    pub fn bookmark(&mut self) {
        self.bytes_read = 0;
    }

    pub const fn bytes_read(&self) -> usize {
        self.bytes_read
    }

    pub(crate) async fn pre_parse_stream_bins(
        &mut self,
        op_count: usize,
    ) -> Result<HashMap<String, PreParsedBin>> {
        let mut data_points: HashMap<String, PreParsedBin> = HashMap::new();

        for _ in 0..op_count {
            self.read_buffer(8).await?;

            let op_size = self.buffer.read_u32(None) as usize;
            self.buffer.skip(1);
            let particle_type = self.buffer.read_u8(None);
            self.buffer.skip(1);
            let name_size = self.buffer.read_u8(None) as usize;
            self.read_buffer(name_size).await?;

            let name: String = self.buffer.read_str(name_size)?;
            let particle_bytes_size = op_size - (4 + name_size);
            self.read_buffer(particle_bytes_size).await?;

            if particle_type != 0 {
                let pre_parsed = PreParsedValue {
                    particle_type,
                    // Needs to be cloned since buffer will be changed in the meantime
                    buffer: self.buffer.clone(),
                    byte_length: particle_bytes_size,
                };

                match data_points.entry(name) {
                    Vacant(entry) => {
                        let pre_bin = PreParsedBin {
                            value: pre_parsed,
                            sub_values: Vec::new(),
                        };
                        entry.insert(pre_bin);
                    }
                    Occupied(entry) => {
                        let ent = entry.into_mut();
                        ent.sub_values.push(pre_parsed);
                    }
                }
            }

            // Value Data starts at current offset. We dont want to parse that now, so skip to let the loop continue at the next bin.
            self.buffer.skip(particle_bytes_size);
        }

        Ok(data_points)
    }
}
