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

#[cfg(feature = "tls")]
use std::convert::TryFrom;
#[cfg(feature = "tls")]
use std::sync::Arc;

use crate::commands::admin_command::AdminCommand;
use crate::commands::buffer::{self, Buffer, MAX_BUFFER_SIZE};
use crate::errors::{Error, Result};
use crate::net::Host;
use crate::policy::{AuthMode, ClientPolicy};
#[cfg(feature = "rt-async-std")]
use aerospike_rt::async_std::net::Shutdown;
#[cfg(feature = "rt-tokio")]
use aerospike_rt::io::{AsyncReadExt, AsyncWriteExt};
use aerospike_rt::net::TcpStream;
use aerospike_rt::time::{Duration, Instant};
#[cfg(feature = "rt-async-std")]
use futures::{AsyncReadExt, AsyncWriteExt};
use std::cmp::min;
use std::ops::Add;

#[cfg(feature = "tls")]
use rustls::pki_types::ServerName;
#[cfg(feature = "tls")]
use tokio_rustls::{client::TlsStream, rustls, TlsConnector};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionState {
    Ready,
    Closed,
    Writing,
    ReadingHeader(usize),
    ReadingBody(usize),
}

#[derive(Debug)]
pub enum Netsocket {
    Tcp(TcpStream),
    #[cfg(feature = "tls")]
    Tls(TlsStream<TcpStream>),
}

#[derive(Debug)]
pub struct Connection {
    pub(crate) addr: String,
    socket_timeout: u32,
    deadline: Option<Instant>,
    timeout_delay: u32,
    // duration after which connection is considered idle
    idle_timeout: Option<Duration>,
    idle_deadline: Option<Instant>,

    // connection object
    pub(crate) conn: Netsocket,

    bytes_read: usize,

    pub buffer: Buffer,

    pub(crate) state: ConnectionState,
    can_recover_connection: bool,
}

impl Connection {
    #[cfg(feature = "tls")]
    async fn get_netsocket(
        stream: TcpStream,
        host: &Host,
        policy: &ClientPolicy,
    ) -> Result<Netsocket> {
        if let Some(tls_config) = policy.tls_config.clone() {
            let connector = TlsConnector::from(Arc::new(tls_config));
            let server_name = host
                .tls_name
                .clone()
                .unwrap_or(policy.cluster_name.clone().unwrap_or_default());
            let domain = ServerName::try_from(server_name.as_str())
                .map_err(|e| Error::ClientError(e.to_string()))?
                .to_owned();
            Ok(Netsocket::Tls(connector.connect(domain, stream).await?))
        } else {
            Ok(Netsocket::Tcp(stream))
        }
    }

    #[cfg(not(feature = "tls"))]
    async fn get_netsocket(
        stream: TcpStream,
        _host: &Host,
        _policy: &ClientPolicy,
    ) -> Result<Netsocket> {
        Ok(Netsocket::Tcp(stream))
    }

    pub async fn new(
        host: &Host,
        policy: &ClientPolicy,
        hashed_pass: Option<&String>,
    ) -> Result<Self> {
        let addr = host.address();
        let stream =
            aerospike_rt::timeout(policy.timeout(), TcpStream::connect(addr.clone())).await;
        if stream.is_err() {
            return Err(Error::Connection(
                "Could not open network connection".to_string(),
            ));
        }

        let stream = stream.unwrap()?;
        let stream = Self::get_netsocket(stream, host, policy).await?;

        let idle_timeout = if policy.idle_timeout > 0 {
            Some(Duration::from_millis(u64::from(policy.idle_timeout)))
        } else {
            None
        };

        let mut conn = Connection {
            addr,
            buffer: Buffer::new(policy.buffer_reclaim_threshold),
            bytes_read: 0,
            conn: stream,
            socket_timeout: policy.timeout().as_millis() as u32,
            timeout_delay: 0,
            deadline: None,
            idle_timeout,
            idle_deadline: idle_timeout.map(|timeout| Instant::now() + timeout),
            state: ConnectionState::Ready,
            can_recover_connection: false,
        };
        conn.authenticate(&policy.auth_mode, hashed_pass).await?;
        conn.refresh();
        Ok(conn)
    }

    pub fn close(&mut self) {
        self.state = ConnectionState::Closed;
        let () = match self.conn {
            Netsocket::Tcp(ref mut conn) => {
                #[cfg(feature = "rt-tokio")]
                let _ = conn.shutdown();
                #[cfg(feature = "rt-async-std")]
                let _ = conn.shutdown(Shutdown::Both);
            }
            #[cfg(feature = "tls")]
            Netsocket::Tls(ref mut conn) => {
                #[cfg(feature = "rt-tokio")]
                let _ = conn.shutdown();
                #[cfg(feature = "rt-async-std")]
                let _ = conn.shutdown(Shutdown::Both);
            }
        };
    }

    pub async fn flush(&mut self) -> Result<()> {
        self.state = ConnectionState::Writing;
        let timeout = self.deadline();
        let res = match self.conn {
            Netsocket::Tcp(ref mut conn) => {
                aerospike_rt::timeout(timeout, conn.write_all(&self.buffer.data_buffer)).await
            }
            #[cfg(feature = "tls")]
            Netsocket::Tls(ref mut conn) => {
                aerospike_rt::timeout(timeout, conn.write_all(&self.buffer.data_buffer)).await
            }
        };

        match res {
            Ok(Ok(())) => (),
            Ok(Err(e)) => return Err(e.into()),
            Err(_) => {
                return Err(Error::Timeout(
                    "Timeout writing to network connection".to_string(),
                ));
            }
        }

        self.refresh();
        Ok(())
    }

    pub(crate) const fn set_state(&mut self, state: ConnectionState) {
        self.state = state;
        self.bytes_read = 0;
    }

    /// Sets the timeout delay for the connection.
    pub(crate) const fn set_timeout_delay(
        &mut self,
        can_recover_connection: bool,
        timeout_delay: u32,
    ) {
        self.can_recover_connection = can_recover_connection;
        self.timeout_delay = timeout_delay;
    }

    /// Sets the timeout for the connection.
    pub const fn set_socket_timeout(&mut self, deadline: Option<Instant>, socket_timeout: u32) {
        self.deadline = deadline;
        if socket_timeout > 0 {
            self.socket_timeout = socket_timeout;
        } else {
            self.socket_timeout = 30_000; // 30 secs
        }
    }

    /// Reads the socket deadline for the connection.
    pub fn deadline(&self) -> Duration {
        let now = Instant::now();
        let socket_deadline = now + self.socket_timeout();

        let deadline = if let Some(deadline) = self.deadline {
            min(deadline, socket_deadline)
        } else {
            socket_deadline
        };

        deadline - now
    }

    /// Reads the socket timeout for the connection.
    /// If the timeout is zero, it will return the default (30 000 ms)
    pub fn socket_timeout(&self) -> Duration {
        if self.socket_timeout > 0 {
            Duration::from_millis(u64::from(self.socket_timeout))
        } else {
            Duration::from_millis(30_000) // 30 secs
        }
    }

    // This function validates the message header.
    pub(crate) fn validate_header(&self, header: u64) -> Result<()> {
        let msg_version = (header & 0xFF00000000000000) >> 56;
        if msg_version != 2 {
            return Err(Error::ClientError(format!(
                "Invalid Message Header: Expected version to be 2, but got {msg_version}"
            )));
        }

        let msg_type = (header & 0x00FF000000000000) >> 49;
        if !(msg_type == 1 || msg_type == 3 || msg_type == 4) {
            return Err(Error::ClientError(format!(
                "Invalid Message Header: Expected type to be 1, 3 or 4, but got {msg_type}"
            )));
        }

        Ok(())
    }

    // This function reads a standard header, setting the state correctly.
    pub(crate) async fn read_header(&mut self) -> Result<usize> {
        let header_size = buffer::MSG_TOTAL_HEADER_SIZE as usize;
        self.set_state(ConnectionState::ReadingHeader(header_size));
        let res = self.read_buffer(header_size).await?;
        self.set_state(ConnectionState::Ready);

        let proto = self.buffer.read_u64(Some(0));
        self.validate_header(proto)?;

        Ok(res)
    }

    // This function reads a standard header, setting the state correctly.
    pub(crate) async fn read_body(&mut self, receive_size: usize) -> Result<usize> {
        self.set_state(ConnectionState::ReadingBody(receive_size));
        let res = self.read_buffer(receive_size).await?;
        self.set_state(ConnectionState::Ready);
        Ok(res)
    }

    pub(crate) async fn read_buffer(&mut self, size: usize) -> Result<usize> {
        self.read_buffer_at(0, size).await
    }

    pub(crate) async fn read_buffer_at(&mut self, pos: usize, size: usize) -> Result<usize> {
        self.buffer.resize_buffer(size + pos)?;

        let deadline = self.deadline.unwrap_or(Instant::now() + self.deadline());

        let mut total_read: usize = 0;
        while total_read < size && Instant::now() < deadline {
            let read_result = match self.conn {
                Netsocket::Tcp(ref mut conn) => {
                    #[cfg(feature = "rt-tokio")]
                    conn.readable().await?;
                    conn.read(&mut self.buffer.data_buffer[pos + total_read..])
                        .await
                }

                #[cfg(feature = "tls")]
                Netsocket::Tls(ref mut conn) => {
                    conn.read(&mut self.buffer.data_buffer[pos + total_read..])
                        .await
                }
            };

            match read_result {
                Ok(0) => break,
                Ok(n) => {
                    total_read += n;
                    self.bytes_read += n;
                }
                Err(ref e) if e.kind() == aerospike_rt::io::ErrorKind::WouldBlock => {}
                Err(e) => Err(e)?,
            }
        }

        if total_read != size {
            return Err(Error::Timeout(
                "Timeout reading from the network connection".into(),
            ));
        }

        self.buffer.reset_offset();
        self.refresh();
        Ok(size)
    }

    /// Writes to the connection until done or timeout has been reached.
    pub async fn write_all(&mut self, buf: &[u8]) -> Result<()> {
        self.state = ConnectionState::Writing;

        let timeout = self.deadline();
        let res = match self.conn {
            Netsocket::Tcp(ref mut conn) => {
                aerospike_rt::timeout(timeout, conn.write_all(buf)).await
            }
            #[cfg(feature = "tls")]
            Netsocket::Tls(ref mut conn) => {
                aerospike_rt::timeout(timeout, conn.write_all(buf)).await
            }
        };

        match res {
            Ok(Ok(())) => (),
            Ok(Err(e)) => {
                return Err(e.into());
            }
            Err(e) => {
                return Err(Error::Timeout(format!(
                    "Timeout writing to the network connection: {e}"
                )));
            }
        }

        self.state = ConnectionState::Ready;
        self.refresh();
        Ok(())
    }

    /// Reads from the connection until the buffer is full or timeout has been reached.
    pub async fn read_all(&mut self, buf: &mut [u8]) -> Result<()> {
        self.state = ConnectionState::ReadingBody(buf.len());

        let timeout = self.deadline();
        let res = match self.conn {
            Netsocket::Tcp(ref mut conn) => {
                aerospike_rt::timeout(timeout, conn.read_exact(buf)).await
            }
            #[cfg(feature = "tls")]
            Netsocket::Tls(ref mut conn) => {
                aerospike_rt::timeout(timeout, conn.read_exact(buf)).await
            }
        };

        match res {
            Ok(Ok(_)) => (),
            Ok(Err(e)) => return Err(e.into()),
            Err(_) => {
                return Err(Error::Timeout(
                    "Timeout reading from the network connection".to_string(),
                ))
            }
        }

        self.state = ConnectionState::Ready;
        self.bytes_read += buf.len();
        self.refresh();
        Ok(())
    }

    pub fn is_idle(&self) -> bool {
        self.idle_deadline
            .is_some_and(|idle_dl| Instant::now() >= idle_dl)
    }

    fn refresh(&mut self) {
        self.idle_deadline = None;
        self.deadline = None;
        if let Some(idle_to) = self.idle_timeout {
            self.idle_deadline = Some(Instant::now().add(idle_to));
        }
    }

    async fn authenticate(
        &mut self,
        auth_mode: &AuthMode,
        hashed_pass: Option<&String>,
    ) -> Result<()> {
        self.state = ConnectionState::Writing;
        return match AdminCommand::authenticate(self, auth_mode, hashed_pass).await {
            Ok(()) => Ok(()),
            Err(err) => {
                self.close();
                Err(err)
            }
        };
    }

    pub const fn bookmark(&mut self) {
        self.bytes_read = 0;
    }

    pub const fn bytes_read(&self) -> usize {
        self.bytes_read
    }

    /// `recover_connection` will try to recover a connection in cases of timeout
    pub(crate) async fn recover_connection(&mut self) {
        if !self.can_recover_connection || self.timeout_delay <= 0 {
            return;
        }

        match self.state {
            ConnectionState::Ready | ConnectionState::Closed | ConnectionState::Writing => (),
            ConnectionState::ReadingHeader(total_size) => {
                self.set_socket_timeout(None, self.timeout_delay);

                if total_size > self.bytes_read {
                    // read the rest of the header
                    if let Err(_) = self
                        .read_buffer_at(self.bytes_read, total_size - self.bytes_read)
                        .await
                    {
                        // return early and don't update the connection state
                        return;
                    };
                }

                self.buffer.reset_offset();
                let sz = self.buffer.read_u64(Some(0));
                let header_length = self.buffer.read_u8(Some(8));
                let receive_size = ((sz & 0xFFFF_FFFF_FFFF) - u64::from(header_length)) as usize;
                self.set_state(ConnectionState::ReadingBody(receive_size));
                if let Err(_) = self
                    .drain(
                        receive_size,
                        Duration::from_millis(u64::from(self.timeout_delay)),
                    )
                    .await
                {
                    // return early and don't update the connection state
                    return;
                }

                assert!(self.bytes_read == receive_size);
                self.state = ConnectionState::Ready;
            }
            ConnectionState::ReadingBody(total_size) => {
                // read the rest of the body
                if let Err(_) = self
                    .drain(
                        total_size - self.bytes_read,
                        Duration::from_millis(u64::from(self.timeout_delay)),
                    )
                    .await
                {
                    // return early and don't update the connection state
                    return;
                }

                assert!(self.bytes_read == total_size);
                self.state = ConnectionState::Ready;
            }
        }
    }

    // reads the rest of the message to empty the connection buffer
    // before returning the connection back to the pool.
    async fn drain(&mut self, mut limit: usize, timeout: Duration) -> Result<()> {
        while limit > 0 {
            let count = match self.conn {
                Netsocket::Tcp(ref mut conn) => aerospike_rt::timeout(
                    timeout,
                    aerospike_rt::io::copy(
                        &mut conn.take(limit as u64),
                        &mut aerospike_rt::io::sink(),
                    ),
                )
                .await
                .map_err(|e| Error::Timeout(format!("Timeout draining the connection {e}")))?,

                #[cfg(feature = "tls")]
                Netsocket::Tls(ref mut conn) => aerospike_rt::timeout(
                    timeout,
                    aerospike_rt::io::copy(
                        &mut conn.take(limit as u64),
                        &mut aerospike_rt::io::sink(),
                    ),
                )
                .await
                .map_err(|e| Error::Timeout(format!("Timeout draining the connection {e}")))?,
            }?;

            limit -= count as usize;
            self.bytes_read += count as usize;
        }

        Ok(())
    }
}

/***********************************************************************************/
/*  Buffered Connection                                                            */
/***********************************************************************************/

// Holds data buffer for the command
#[derive(Debug)]
pub struct BufferedConn<'a> {
    pub(crate) conn: &'a mut Connection,

    cache: Vec<u8>,
    pos: usize,

    pub(crate) limit: usize,
    bytes_read: usize,
}

impl<'a> BufferedConn<'a> {
    pub fn new(conn: &'a mut Connection) -> Self {
        BufferedConn {
            conn,
            cache: Vec::with_capacity(4 * 1024),
            limit: 0,
            pos: 0,
            bytes_read: 0,
        }
    }

    pub(crate) const fn bookmark(&mut self) {
        self.bytes_read = 0;
        self.conn.bookmark();
    }

    #[inline]
    pub(crate) const fn buffer(&mut self) -> &mut Buffer {
        &mut self.conn.buffer
    }

    #[inline]
    pub(crate) const fn bytes_read(&self) -> usize {
        self.bytes_read
    }

    pub(crate) fn set_limit_header(&mut self, size: usize) -> Result<()> {
        self.conn.set_state(ConnectionState::ReadingHeader(size));
        self.set_limit(size)
    }

    pub(crate) fn set_limit_body(&mut self, size: usize) -> Result<()> {
        self.conn.set_state(ConnectionState::ReadingBody(size));
        self.set_limit(size)
    }

    fn set_limit(&mut self, size: usize) -> Result<()> {
        self.limit = size;
        self.pos = 0;
        self.bytes_read = 0;
        self.resize_cache(0)
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
        }

        let size = min(self.cache.capacity(), self.limit);
        self.resize_cache(size)?;

        let deadline = Instant::now() + self.conn.deadline();
        let mut total_read: usize = 0;
        while total_read < size && Instant::now() < deadline {
            let read_result = match self.conn.conn {
                Netsocket::Tcp(ref mut conn) => conn.read(&mut self.cache[total_read..]).await,

                #[cfg(feature = "tls")]
                Netsocket::Tls(ref mut conn) => conn.read(&mut self.cache[total_read..]).await,
            };

            match read_result {
                Ok(0) => break,
                Ok(n) => {
                    total_read += n;
                    self.limit -= n;
                    self.conn.bytes_read += n;
                }
                Err(e) => Err(e)?,
            }
        }

        if total_read != size {
            return Err(Error::Timeout(
                "Timeout reading from the network connection".into(),
            ));
        }

        self.pos = 0;
        Ok(size)
    }

    pub(crate) async fn drain(&mut self, timeout: Duration) -> Result<()> {
        while self.limit > 0 {
            let count = match self.conn.conn {
                Netsocket::Tcp(ref mut conn) => aerospike_rt::timeout(
                    timeout,
                    aerospike_rt::io::copy(
                        &mut conn.take(self.limit as u64),
                        &mut aerospike_rt::io::sink(),
                    ),
                )
                .await
                .map_err(|e| Error::Timeout(format!("Timeout draining the connection {e}")))?,
                #[cfg(feature = "tls")]
                Netsocket::Tls(ref mut conn) => aerospike_rt::timeout(
                    timeout,
                    aerospike_rt::io::copy(
                        &mut conn.take(self.limit as u64),
                        &mut aerospike_rt::io::sink(),
                    ),
                )
                .await
                .map_err(|e| Error::Timeout(format!("Timeout draining the connection {e}")))?,
            }?;

            self.limit -= count as usize;
            self.bytes_read += count as usize;
            self.conn.bytes_read += count as usize;
        }

        let _ = self.resize_cache(0);
        self.pos = 0;
        assert!(self.exhausted());

        self.conn.state = ConnectionState::Ready;

        Ok(())
    }

    #[inline]
    pub(crate) const fn exhausted(&self) -> bool {
        self.limit <= 0 && self.empty()
    }

    #[inline]
    const fn len(&self) -> usize {
        self.cache.len() - self.pos
    }

    #[inline]
    const fn empty(&self) -> bool {
        self.len() == 0
    }

    async fn cached_read_rest(&mut self) -> Result<usize> {
        if !self.empty() {
            return self.cached_read(0, self.len());
        }
        Ok(0)
    }

    fn cached_read(&mut self, pos: usize, size: usize) -> Result<usize> {
        self.conn.buffer.data_buffer[pos..pos + size]
            .copy_from_slice(&self.cache[self.pos..self.pos + size]);

        self.pos += size;
        Ok(size)
    }

    pub async fn read_buffer(&mut self, size: usize) -> Result<usize> {
        self.conn.buffer.resize_buffer(size)?;

        if self.limit > 0 && self.empty() {
            self.fill_buffer().await?;
        }

        if size <= self.len() {
            self.cached_read(0, size)?;
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
                self.cached_read(cached, remaining)?;
            }
        }

        self.bytes_read += size;

        self.conn.buffer.reset_offset();
        self.conn.refresh();

        Ok(size)
    }
}
