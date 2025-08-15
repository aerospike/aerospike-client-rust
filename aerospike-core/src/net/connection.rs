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
use crate::commands::buffer::{Buffer, MAX_BUFFER_SIZE};
use crate::errors::{Error, Result};
use crate::policy::ClientPolicy;
use std::cmp::min;

#[cfg(all(any(feature = "rt-async-std"), not(feature = "rt-tokio")))]
use aerospike_rt::async_std::net::Shutdown;
#[cfg(all(any(feature = "rt-tokio"), not(feature = "rt-async-std")))]
use aerospike_rt::io::{AsyncReadExt, AsyncWriteExt};
use aerospike_rt::net::TcpStream;
use aerospike_rt::time::{Duration, Instant};
#[cfg(all(any(feature = "rt-async-std"), not(feature = "rt-tokio")))]
use futures::{AsyncReadExt, AsyncWriteExt};
use std::ops::Add;

#[derive(Debug)]
pub struct Connection {
    pub(crate) addr: String,
    // duration after which connection is considered idle
    idle_timeout: Option<Duration>,
    idle_deadline: Option<Instant>,

    // connection object
    pub(crate) conn: TcpStream,

    bytes_read: usize,

    pub buffer: Buffer,
}

impl Connection {
    pub async fn new(addr: &str, policy: &ClientPolicy) -> Result<Self> {
        let stream = aerospike_rt::timeout(Duration::from_secs(10), TcpStream::connect(addr)).await;
        if stream.is_err() {
            return Err(Error::Connection(
                "Could not open network connection".to_string(),
            ));
        }
        let mut conn = Connection {
            addr: addr.into(),
            buffer: Buffer::new(policy.buffer_reclaim_threshold),
            bytes_read: 0,
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

    pub async fn read_buffer(&mut self, size: usize) -> Result<usize> {
        self.buffer.resize_buffer(size)?;
        let size = self.conn.read_exact(&mut self.buffer.data_buffer).await?;
        self.bytes_read += size;
        self.buffer.reset_offset();
        self.refresh();
        Ok(size)
    }

    pub async fn read_buffer_at(&mut self, pos: usize, size: usize) -> Result<usize> {
        let size = self
            .conn
            .read_exact(&mut self.buffer.data_buffer[pos..pos + size])
            .await?;
        self.bytes_read += size;
        self.buffer.reset_offset();
        self.refresh();
        Ok(size)
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
}

// Holds data buffer for the command
#[derive(Debug)]
pub(crate) struct BufferedConn<'a> {
    pub(crate) conn: &'a mut Connection,

    cache: Vec<u8>,
    pos: usize,

    limit: usize,
    bytes_read: usize,
}

impl<'a> BufferedConn<'a> {
    pub fn new(conn: &'a mut Connection) -> Self {
        BufferedConn {
            conn: conn,
            cache: Vec::with_capacity(4 * 1024),
            limit: 0,
            pos: 0,
            bytes_read: 0,
        }
    }

    pub(crate) fn bookmark(&mut self) {
        self.bytes_read = 0;
        self.conn.bookmark();
    }

    #[inline]
    pub(crate) fn buffer(&mut self) -> &mut Buffer {
        &mut self.conn.buffer
    }

    #[inline]
    pub(crate) fn bytes_read(&mut self) -> usize {
        self.bytes_read
    }

    pub(crate) fn set_limit(&mut self, size: usize) {
        self.limit = size;
        self.pos = 0;
        self.bytes_read = 0;
        self.resize_cache(0).unwrap();
    }

    fn resize_cache(&mut self, size: usize) -> Result<()> {
        // Corrupted data streams can result in a huge length.
        // Do a sanity check here.
        if size > MAX_BUFFER_SIZE {
            return Err(Error::InvalidArgument(format!(
                "Invalid size for buffer: {size}"
            )));
        }

        self.cache.resize(size, 0);

        Ok(())
    }

    async fn fill_buffer(&mut self) -> Result<usize> {
        // fill_buffer fills the buffer from the beginning.
        // The buffer should have been completely consumed before calling this function
        if self.pos != self.cache.len() || self.limit <= 0 {
            return Ok(0);
        };

        let size = min(self.cache.capacity(), self.limit);
        self.resize_cache(size)?;

        let size = self.conn.conn.read_exact(&mut self.cache).await?;

        self.limit -= size;
        self.pos = 0;
        Ok(size)
    }

    #[inline]
    pub(crate) fn exhausted(&self) -> bool {
        self.limit <= 0 && self.empty()
    }

    #[inline]
    fn len(&self) -> usize {
        self.cache.len() - self.pos
    }

    #[inline]
    fn empty(&self) -> bool {
        self.len() == 0
    }

    async fn cached_read_rest(&mut self) -> Result<usize> {
        if !self.empty() {
            return self.cached_read(0, self.len()).await;
        }
        Ok(0)
    }

    async fn cached_read(&mut self, pos: usize, size: usize) -> Result<usize> {
        self.conn.buffer.data_buffer[pos..pos + size]
            .copy_from_slice(&self.cache[self.pos..self.pos + size]);

        self.pos += size;
        Ok(size)
    }

    // pub async fn read_buffer2(&mut self, size: usize) -> Result<usize> {
    //     let size: usize = self.conn.read_buffer(size).await?;
    //     // self.conn.buffer.dump_buffer();
    //     self.bytes_read += size;
    //     self.limit -= size;
    //     self.pos = self.cache.len(); // mark as empty
    //     Ok(size)
    // }

    pub async fn read_buffer(&mut self, size: usize) -> Result<usize> {
        self.conn.buffer.resize_buffer(size)?;

        if self.limit > 0 && self.empty() {
            self.fill_buffer().await?;
        }

        if size <= self.len() {
            self.cached_read(0, size).await?;
        } else if size > self.len() {
            // we have data left in the buffer, but we need more
            let cached = self.cached_read_rest().await?;
            let remaining = size - cached;
            if remaining > self.cache.capacity() / 2 {
                // read directly
                self.conn.read_buffer_at(cached, remaining).await?;
                self.limit -= remaining;
            } else {
                // fill the buffer and read the rest of requested bytes
                self.fill_buffer().await?;
                self.cached_read(cached, remaining).await?;
            }
        }

        self.bytes_read += size;

        self.conn.buffer.reset_offset();
        self.conn.refresh();

        Ok(size)
    }
}
