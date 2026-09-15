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

use std::io::Read;
#[cfg(feature = "rt-tokio")]
use std::pin::Pin;

use crate::commands::admin_command::AdminCommand;
use crate::commands::buffer::{self, Buffer, MAX_BUFFER_SIZE};
use crate::errors::{Error, Result};
use crate::net::Host;
use crate::policy::{AuthMode, ClientPolicy};
use crate::XorShift;
#[cfg(feature = "rt-async-std")]
use aerospike_rt::async_std::net::Shutdown;
#[cfg(feature = "rt-tokio")]
use aerospike_rt::io::{AsyncReadExt, AsyncWriteExt};
use aerospike_rt::net::TcpStream;
use aerospike_rt::time::{Duration, Instant};
use flate2::read::ZlibDecoder;
#[cfg(feature = "rt-async-std")]
use futures::{AsyncReadExt, AsyncWriteExt};
use std::cmp::min;
use std::ops::Add;

#[cfg(feature = "tls")]
use rustls::pki_types::ServerName;
#[cfg(feature = "tls")]
use tokio_rustls::{client::TlsStream, rustls, TlsConnector};

/// State of a connection in the wire protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionState {
    /// Connection is idle and ready for a command.
    Ready,
    /// Connection is closed.
    Closed,
    /// Writing request data.
    Writing,
    /// Reading response header (payload size in bytes).
    ReadingHeader(usize),
    /// Reading response body.
    ReadingBody(usize),
    /// Reading stream response header.
    ReadingStreamHeader(usize),
    /// Reading stream response body.
    ReadingStreamBody(usize),
}

/// Result of a pool-checkout liveness peek.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Liveness {
    /// Socket open with nothing pending.
    Alive,
    /// Socket open, but bytes are waiting that nobody asked for.
    PendingBytes,
    /// Peer closed the connection, or the socket is broken.
    Closed,
}

/// A pooled connection's position relative to its idle deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IdleStatus {
    /// No deadline armed (`idle_timeout = 0`), or plenty of time left.
    Fresh,
    /// The deadline falls inside the caller's expiry horizon.
    ExpiringSoon,
    /// The deadline has passed.
    Expired,
}

/// Underlying socket type for a connection (TCP or TLS).
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum Netsocket {
    /// Plain TCP stream.
    Tcp(TcpStream),
    /// TLS-wrapped TCP stream.
    #[cfg(feature = "tls")]
    Tls(TlsStream<TcpStream>),
    /// Test double (tests only).
    #[cfg(test)]
    TestDummy,
}

#[derive(Debug)]
#[allow(clippy::struct_field_names)]
pub struct Connection {
    pub(crate) addr: String,
    socket_timeout: u32,
    deadline: Option<Instant>,
    timeout_delay: u32,
    // duration after which connection is considered idle
    idle_timeout: Option<Duration>,
    idle_deadline: Option<Instant>,

    rnd: XorShift,

    // connection object
    pub(crate) conn: Netsocket,

    bytes_read: usize,

    pub buffer: Buffer,

    pub(crate) state: ConnectionState,
    can_recover_connection: bool,
    /// Set when a compressed response has been decompressed in-place.
    /// The full message body is already in the buffer; skip network reads.
    response_decompressed: bool,
    /// Tracks whether the current stream/batch message body being read is
    /// compressed (type 4). Set by `stream_command/batch_operate_command` when
    /// a compressed message is detected. Used by `ConnectionRecovery` to know
    /// that info3 inspection is invalid for the current body.
    pub(crate) compressed_stream_body: bool,
    /// Reusable per-IO timeout. Reset before each read/write and selected against the
    /// IO future; avoids the per-op `tokio::time::timeout` alloc + wheel register/remove.
    #[cfg(feature = "rt-tokio")]
    pub(crate) sleep: Pin<Box<aerospike_rt::tokio::time::Sleep>>,
}

/// Races an IO future against the connection's timeout, picking a runtime
/// appropriate mechanism.
///
/// `$holder` is the value owning the `sleep` field (`self` for [`Connection`],
/// `self.conn` for `BufferedConn`).
macro_rules! io_with_timeout {
    ($holder:expr, $timeout:expr, $io:expr) => {{
        #[cfg(feature = "rt-tokio")]
        {
            $holder
                .sleep
                .as_mut()
                .reset(aerospike_rt::tokio::time::Instant::now() + $timeout);
            let sleep = $holder.sleep.as_mut();
            aerospike_rt::tokio::select! {
                biased;
                r = $io => Ok::<_, ()>(r),
                _ = sleep => Err(()),
            }
        }
        #[cfg(feature = "rt-async-std")]
        {
            aerospike_rt::timeout($timeout, $io).await
        }
    }};
}

impl Connection {
    /// Switch this connection's command buffer to lease large allocations
    /// from the owning cluster's tiered buffer pool. Called by the
    /// connection pool right after the connection is created; short-lived
    /// connections (seed probes, node validation) never attach a pool.
    pub(crate) fn attach_buffer_pool(
        &mut self,
        pool: std::sync::Arc<crate::net::buffer_pool::TieredBufferPool>,
    ) {
        self.buffer =
            crate::commands::buffer::Buffer::with_pool(self.buffer.reclaim_threshold, pool);
    }

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
                .unwrap_or_else(|| policy.cluster_name.clone().unwrap_or_default());
            let domain = ServerName::try_from(server_name.as_str())
                .map_err(|e| Error::client_error(e.to_string()))?
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

    /// Sets `TCP_NODELAY` on a freshly opened socket (disables Nagle).
    ///
    /// Nagle withholds a small segment while earlier data is still unacked,
    /// which stalls against the peer's delayed ACK until Linux's
    /// `TCP_DELACK_MIN` (40ms) expires.
    ///
    /// Best-effort. A platform that refuses the option still yields a usable
    /// connection, so the error is dropped rather than failing the connect.
    fn set_nodelay(stream: &TcpStream) {
        let _ = stream.set_nodelay(true);
    }

    #[cfg(not(test))]
    pub async fn new(
        host: &Host,
        policy: &ClientPolicy,
        hashed_pass: Option<&String>,
    ) -> Result<Self> {
        Self::new_with_session(host, policy, hashed_pass, None)
            .await
            .map(|(conn, _session)| conn)
    }

    /// Like [`new`](Self::new) but optionally reuses a previously-issued
    /// session token to authenticate via `AUTHENTICATE` instead of `LOGIN`.
    /// On success returns the connection plus a fresh `SessionInfo` if the
    /// server issued one (i.e. when we fell back to a full login).
    #[cfg(not(test))]
    pub async fn new_with_session(
        host: &Host,
        policy: &ClientPolicy,
        hashed_pass: Option<&String>,
        session: Option<&crate::commands::admin_command::SessionInfo>,
    ) -> Result<(Self, Option<crate::commands::admin_command::SessionInfo>)> {
        let addr = host.address();
        let stream =
            aerospike_rt::timeout(policy.connect_timeout(), TcpStream::connect(addr.clone())).await;
        if stream.is_err() {
            return Err(Error::connection(
                "Could not open network connection".to_string(),
            ));
        }

        let stream = stream.unwrap()?;

        Self::set_nodelay(&stream);

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
            // Governs the login/authenticate I/O below (the only I/O before a
            // command runs); commands overwrite it via `set_socket_timeout`
            // before use.
            socket_timeout: policy.login_timeout().as_millis() as u32,
            timeout_delay: 0,
            deadline: None,
            idle_timeout,
            idle_deadline: idle_timeout.map(|timeout| Instant::now() + timeout),
            state: ConnectionState::Ready,
            can_recover_connection: false,
            response_decompressed: false,
            compressed_stream_body: false,
            rnd: XorShift::new(),
            // Far-future deadline; reset before each IO so this never fires first.
            #[cfg(feature = "rt-tokio")]
            sleep: Box::pin(aerospike_rt::tokio::time::sleep(
                aerospike_rt::time::Duration::from_secs(3600),
            )),
        };

        // Try the cheap session-token path first. On rejection (token
        // revoked / quick-restart), fall back to a fresh login. Mirrors
        // Java `Node.refresh`'s `if (! AdminCommand.authenticate(...)) login()`.
        let mut new_session: Option<crate::commands::admin_command::SessionInfo> = None;
        let used_session = match session {
            Some(s) if !s.is_expired() => conn
                .authenticate_with_session(&policy.auth_mode, &s.token)
                .await
                .unwrap_or(false),
            _ => false,
        };
        if !used_session {
            new_session = conn.authenticate(&policy.auth_mode, hashed_pass).await?;
        }
        conn.refresh();
        Ok((conn, new_session))
    }

    /// Test-mode shim that mirrors the production
    /// [`new_with_session`](Self::new_with_session) signature so call sites
    /// like `ConnectionPool::make_conn` link under `cfg(test)`. Always
    /// returns a fresh `(connection, None)` pair — the test build never
    /// goes near a real LOGIN, so the cached-session fast path is moot.
    #[cfg(test)]
    pub async fn new_with_session(
        host: &Host,
        policy: &ClientPolicy,
        hashed_pass: Option<&String>,
        _session: Option<&crate::commands::admin_command::SessionInfo>,
    ) -> Result<(Self, Option<crate::commands::admin_command::SessionInfo>)> {
        Self::new(host, policy, hashed_pass)
            .await
            .map(|c| (c, None))
    }

    #[cfg(test)]
    pub async fn new(
        host: &Host,
        policy: &ClientPolicy,
        _hashed_pass: Option<&String>,
    ) -> Result<Self> {
        let addr = host.address();
        let stream = Netsocket::TestDummy;
        let rnd = XorShift::new();

        let idle_timeout = if policy.idle_timeout > 0 {
            Some(Duration::from_millis(policy.idle_timeout as u64))
        } else {
            None
        };

        let mut conn = Connection {
            addr: addr.into(),
            buffer: Buffer::new(policy.buffer_reclaim_threshold),
            bytes_read: 0,
            conn: stream,
            socket_timeout: policy.login_timeout().as_millis() as u32,
            timeout_delay: 0,
            deadline: None,
            idle_timeout: idle_timeout,
            idle_deadline: idle_timeout.map(|timeout| Instant::now() + timeout),
            state: ConnectionState::Ready,
            can_recover_connection: false,
            response_decompressed: false,
            compressed_stream_body: false,
            rnd: rnd,
            // Far-future deadline; reset before each IO so this never fires first.
            #[cfg(feature = "rt-tokio")]
            sleep: Box::pin(aerospike_rt::tokio::time::sleep(
                aerospike_rt::time::Duration::from_secs(3600),
            )),
        };
        conn.refresh();
        Ok(conn)
    }

    /// Test-only: connection over a real TCP stream (`new`'s test shim
    /// has no socket).
    #[cfg(all(test, feature = "rt-tokio"))]
    pub(crate) fn test_from_tcp_stream(
        stream: aerospike_rt::net::TcpStream,
        policy: &ClientPolicy,
    ) -> Self {
        let idle_timeout = if policy.idle_timeout > 0 {
            Some(Duration::from_millis(u64::from(policy.idle_timeout)))
        } else {
            None
        };
        let mut conn = Connection {
            addr: "127.0.0.1:0".into(),
            buffer: Buffer::new(policy.buffer_reclaim_threshold),
            bytes_read: 0,
            conn: Netsocket::Tcp(stream),
            socket_timeout: 5_000,
            timeout_delay: 0,
            deadline: None,
            idle_timeout,
            idle_deadline: idle_timeout.map(|timeout| Instant::now() + timeout),
            state: ConnectionState::Ready,
            can_recover_connection: false,
            response_decompressed: false,
            compressed_stream_body: false,
            rnd: XorShift::new(),
            // Far-future deadline; reset before each IO so this never fires first.
            sleep: Box::pin(aerospike_rt::tokio::time::sleep(
                aerospike_rt::time::Duration::from_secs(3600),
            )),
        };
        conn.refresh();
        conn
    }

    /// Returns the connection's per-connection random generator, used by the
    /// metrics sampler to decide whether to record a command.
    pub(crate) const fn rng(&mut self) -> &mut XorShift {
        &mut self.rnd
    }

    pub fn close(&mut self) {
        self.state = ConnectionState::Closed;
        #[allow(clippy::let_underscore_future)]
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
            #[cfg(test)]
            _ => (),
        };
    }

    pub async fn flush(&mut self) -> Result<()> {
        self.state = ConnectionState::Writing;
        let timeout = self.deadline();
        let buf = &self.buffer.data_buffer;
        let res = match self.conn {
            Netsocket::Tcp(ref mut conn) => {
                io_with_timeout!(self, timeout, async {
                    conn.write_all(buf).await?;
                    conn.flush().await
                })
            }
            #[cfg(feature = "tls")]
            Netsocket::Tls(ref mut conn) => {
                // `write_all` alone is not enough on a TLS stream: when the
                // socket is not writable, tokio-rustls accepts the plaintext
                // into the session's outgoing buffer and reports success with
                // ciphertext still unsent. Nothing on the read path drives
                // those bytes out, so the command would wait for a reply to a
                // request the server never fully received. See the note on
                // `tokio_rustls::client::TlsStream::poll_write`.
                io_with_timeout!(self, timeout, async {
                    conn.write_all(buf).await?;
                    conn.flush().await
                })
            }
            #[cfg(test)]
            _ => unreachable!(),
        };

        match res {
            Ok(Ok(())) => (),
            // classify socket I/O errors as Connection err and hence command retries.
            Ok(Err(e)) => return Err(Error::connection(format!("flush: {e}"))),
            Err(_) => {
                return Err(Error::timeout(
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

    pub(crate) const fn reset_state(&mut self) {
        self.state = ConnectionState::Ready;
        self.bytes_read = 0;
        self.response_decompressed = false;
        self.compressed_stream_body = false;
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

    /// Per-IO deadline. Returns `Duration::ZERO` when the command deadline has
    /// already elapsed; the per-IO `Sleep::reset` then fires immediately and the
    /// IO returns `Err(Timeout)` (avoids `Instant::sub` panic in that case).
    pub fn deadline(&self) -> Duration {
        let now = Instant::now();
        let socket_deadline = now + self.socket_timeout();

        let deadline = self
            .deadline
            .map_or(socket_deadline, |deadline| min(deadline, socket_deadline));

        deadline
            .checked_duration_since(now)
            .unwrap_or(Duration::ZERO)
    }

    /// Reads the socket timeout for the connection.
    /// If the timeout is zero, it will return the default (30 000 ms)
    pub fn socket_timeout(&self) -> Duration {
        if self.socket_timeout > 0 {
            Duration::from_millis(u64::from(self.socket_timeout))
        } else {
            Duration::from_secs(30) // 30 secs
        }
    }

    // This function validates the message header.
    pub(crate) fn validate_header(&self, header: u64) -> Result<()> {
        let msg_version = (header & 0xFF00_0000_0000_0000) >> 56;
        if msg_version != 2 {
            return Err(Error::client_error(format!(
                "Invalid Message Header: Expected version to be 2, but got {msg_version}"
            )));
        }

        let msg_type = (header & 0x00FF_0000_0000_0000) >> 48;
        if !(msg_type == 1 || msg_type == 3 || msg_type == 4) {
            return Err(Error::client_error(format!(
                "Invalid Message Header: Expected type to be 1, 3 or 4, but got {msg_type}"
            )));
        }

        Ok(())
    }

    // This function reads a standard header, setting the state correctly.
    // If the response is compressed (msg type 4), reads the full compressed
    // payload, decompresses it, and replaces the buffer contents.
    pub(crate) async fn read_header(&mut self) -> Result<usize> {
        let header_size = buffer::MSG_TOTAL_HEADER_SIZE as usize;
        self.set_state(ConnectionState::ReadingHeader(header_size));
        let res = self.read_buffer(header_size).await?;
        self.set_state(ConnectionState::Ready);

        let proto = self.buffer.read_u64(Some(0));
        self.validate_header(proto)?;

        let msg_type = ((proto >> 48) & 0xFF) as u8;
        if msg_type == buffer::AS_MSG_TYPE_COMPRESSED {
            self.decompress_response(proto).await?;
        }

        Ok(res)
    }

    /// Read a compressed response, decompress it, and replace the buffer.
    /// After this call, the buffer contains the full decompressed message
    /// (including the inner 8-byte proto header and 22-byte command header).
    async fn decompress_response(&mut self, proto: u64) -> Result<()> {
        let compressed_size = (proto & 0x0000_FFFF_FFFF_FFFF) as usize;
        // compressed_size includes the 8-byte uncompressed size field
        if compressed_size < 8 {
            return Err(Error::client_error(
                "Invalid compressed response: size too small".to_string(),
            ));
        }

        // Read the 8-byte uncompressed size (already in the header buffer at offset 8..16,
        // but we already read 30 bytes; the uncompressed size is at bytes 8..16)
        let uncompressed_size = self.buffer.read_u64(Some(8)) as usize;

        // The remaining compressed data to read from the network:
        // We already read 30 bytes (MSG_TOTAL_HEADER_SIZE). The full message is
        // 8 (proto header) + compressed_size bytes. So remaining = 8 + compressed_size - 30.
        let total_message_size = 8 + compressed_size;
        let already_read = buffer::MSG_TOTAL_HEADER_SIZE as usize;

        if total_message_size <= already_read {
            // All compressed data fits within what we already read
            let compressed_data_start = 16; // after proto header + uncompressed size
            let compressed_data_end = total_message_size;
            let compressed_data =
                self.buffer.data_buffer[compressed_data_start..compressed_data_end].to_vec();

            return self.inflate(&compressed_data, uncompressed_size);
        }

        // Need to read more data from the network
        let remaining = total_message_size - already_read;

        // Save what we already have after the 16-byte compressed header
        let existing_compressed = self.buffer.data_buffer[16..already_read].to_vec();

        // Read remaining compressed bytes
        self.buffer.resize_buffer(remaining)?;
        self.set_state(ConnectionState::ReadingBody(remaining));
        self.read_buffer(remaining).await?;
        self.set_state(ConnectionState::Ready);

        // Assemble full compressed payload
        let mut compressed_data = existing_compressed;
        compressed_data.extend_from_slice(&self.buffer.data_buffer[..remaining]);

        self.inflate(&compressed_data, uncompressed_size)
    }

    /// Decompress zlib data and replace the buffer contents with the decompressed data.
    fn inflate(&mut self, compressed_data: &[u8], uncompressed_size: usize) -> Result<()> {
        let mut decoder = ZlibDecoder::new(compressed_data);
        let mut decompressed = vec![0u8; uncompressed_size];
        decoder
            .read_exact(&mut decompressed)
            .map_err(|e| Error::client_error(format!("Decompression error: {e}")))?;

        // Replace buffer with decompressed data (which includes the inner proto header)
        self.buffer.data_buffer = decompressed;
        self.buffer.data_offset = 0;
        self.response_decompressed = true;

        // Validate the inner header
        let inner_proto = self.buffer.read_u64(Some(0));
        self.validate_header(inner_proto)?;

        Ok(())
    }

    // This function reads a standard header, setting the state correctly.
    pub(crate) async fn read_body(&mut self, receive_size: usize) -> Result<usize> {
        if self.response_decompressed {
            // Body is already in the buffer from decompression; skip network read.
            // The decompressed buffer has: 8-byte proto + 22-byte header + body.
            // Callers expect the body to start at offset 0 in data_buffer, so
            // shift the body portion to the front.
            let body_start = buffer::MSG_TOTAL_HEADER_SIZE as usize;
            self.buffer
                .data_buffer
                .copy_within(body_start..body_start + receive_size, 0);
            self.buffer.data_buffer.truncate(receive_size);
            self.buffer.reset_offset();
            self.response_decompressed = false;
            return Ok(receive_size);
        }
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

        let timeout = self.deadline();
        let read_result = match self.conn {
            Netsocket::Tcp(ref mut conn) => {
                io_with_timeout!(
                    self,
                    timeout,
                    conn.read_exact(&mut self.buffer.data_buffer[pos..])
                )
            }
            #[cfg(feature = "tls")]
            Netsocket::Tls(ref mut conn) => {
                io_with_timeout!(
                    self,
                    timeout,
                    conn.read_exact(&mut self.buffer.data_buffer[pos..])
                )
            }
            #[cfg(test)]
            _ => unreachable!(),
        };

        match read_result {
            Ok(Ok(_)) => self.bytes_read += size,
            Ok(Err(e)) => return Err(Error::connection(format!("read: {e}"))),
            Err(_) => {
                return Err(Error::timeout(
                    "Timeout reading from the network connection",
                ))
            }
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
                io_with_timeout!(self, timeout, conn.write_all(buf))
            }
            #[cfg(feature = "tls")]
            Netsocket::Tls(ref mut conn) => {
                // See `flush`: a TLS write is only on the wire once flushed.
                io_with_timeout!(self, timeout, async {
                    conn.write_all(buf).await?;
                    conn.flush().await
                })
            }
            #[cfg(test)]
            _ => unreachable!(),
        };

        match res {
            Ok(Ok(())) => (),
            Ok(Err(e)) => {
                return Err(Error::connection(format!("write: {e}")));
            }
            Err(_) => {
                return Err(Error::timeout(
                    "Timeout writing to the network connection".to_string(),
                ));
            }
        }

        self.refresh();
        Ok(())
    }

    /// Reads from the connection until the buffer is full or timeout has been reached.
    pub async fn read_all(&mut self, buf: &mut [u8]) -> Result<()> {
        self.state = ConnectionState::ReadingBody(buf.len());

        let timeout = self.deadline();
        let res = match self.conn {
            Netsocket::Tcp(ref mut conn) => {
                io_with_timeout!(self, timeout, conn.read_exact(buf))
            }
            #[cfg(feature = "tls")]
            Netsocket::Tls(ref mut conn) => {
                io_with_timeout!(self, timeout, conn.read_exact(buf))
            }
            #[cfg(test)]
            _ => unreachable!(),
        };

        match res {
            Ok(Ok(_)) => (),
            Ok(Err(e)) => return Err(Error::connection(format!("read_all: {e}"))),
            Err(_) => {
                return Err(Error::timeout(
                    "Timeout reading from the network connection".to_string(),
                ))
            }
        }

        self.bytes_read += buf.len();
        self.refresh();
        Ok(())
    }

    pub fn is_idle(&self) -> bool {
        self.idle_deadline
            .is_some_and(|idle_dl| Instant::now() >= idle_dl)
    }

    /// Where this connection stands relative to its idle deadline. A deadline
    /// within `expiry_horizon` counts as expiring.
    pub(crate) fn idle_status(&self, now: Instant, expiry_horizon: Duration) -> IdleStatus {
        match self.idle_deadline {
            None => IdleStatus::Fresh,
            Some(deadline) if now >= deadline => IdleStatus::Expired,
            Some(deadline) if now + expiry_horizon >= deadline => IdleStatus::ExpiringSoon,
            Some(_) => IdleStatus::Fresh,
        }
    }

    /// What a one-byte peek says about a socket.
    fn peek_liveness(sock: &socket2::SockRef<'_>) -> Liveness {
        // MSG_PEEK, so nothing is consumed: a byte seen here is still there for
        // the command that follows. Both runtimes keep the fd non-blocking, so
        // this never waits.
        let mut probe = [std::mem::MaybeUninit::<u8>::uninit(); 1];
        match sock.peek(&mut probe) {
            // Nothing to read on an open socket: the healthy idle case.
            Err(ref e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                ) =>
            {
                Liveness::Alive
            }
            // A peer that sent FIN reads as end-of-stream; an RST, EBADF or
            // anything else is equally unusable.
            Ok(0) | Err(_) => Liveness::Closed,
            Ok(_) => Liveness::PendingBytes,
        }
    }

    /// Non-blocking one-byte peek for pool checkout, so a socket the peer closed
    /// while it sat in the pool is discarded instead of handed to a command that
    /// would fail on its first read with `early eof`.
    ///
    /// Deliberately independent of `idle_timeout`: a socket can die long before
    /// its idle deadline — a server restart kills sockets that were in use a
    /// millisecond earlier — and with the default `idle_timeout = 0` no
    /// connection is ever *idle*, so the tend-time reaper
    /// (`Node::reap_and_refresh_idle_connections`) has nothing to walk. This
    /// checkout probe is what covers that gap.
    pub(crate) fn is_alive(&self) -> bool {
        match self.conn {
            Netsocket::Tcp(ref s) => {
                // Unsolicited bytes on a plain connection mean the stream is out
                // of step with the protocol, which is not recoverable here.
                !matches!(
                    Self::peek_liveness(&socket2::SockRef::from(s)),
                    Liveness::Closed | Liveness::PendingBytes
                )
            }
            #[cfg(feature = "tls")]
            Netsocket::Tls(ref s) => {
                // Peek the TCP socket underneath the TLS session.
                //
                // Pending bytes are treated as ALIVE here, unlike the plain arm:
                // post-handshake TLS control records (a TLS 1.3
                // NewSessionTicket, a KeyUpdate) legitimately arrive while a
                // connection sits idle in the pool, and they are not application
                // data. Calling those dead would evict a healthy connection,
                // reconnect, receive a fresh ticket and evict again — churn
                // caused by the probe itself. Only a closed or broken socket is
                // fatal.
                let (tcp, _session) = s.get_ref();
                !matches!(
                    Self::peek_liveness(&socket2::SockRef::from(tcp)),
                    Liveness::Closed
                )
            }
            #[cfg(test)]
            _ => true,
        }
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
    ) -> Result<Option<crate::commands::admin_command::SessionInfo>> {
        self.state = ConnectionState::Writing;
        match AdminCommand::authenticate(self, auth_mode, hashed_pass).await {
            Ok(session) => {
                // Restore Ready so PooledConnection::Drop puts the conn back
                // in the pool instead of taking the non-recoverable close arm.
                self.set_state(ConnectionState::Ready);
                Ok(session)
            }
            Err(err) => {
                self.close();
                Err(err)
            }
        }
    }

    /// Authenticate against the server with an existing session token —
    /// used by the connection pool to skip the full credential exchange
    /// when a previous `LOGIN` round-trip on this pool produced a still-
    /// valid token. The token-authenticate path is tried before falling
    /// back to a fresh `login()`.
    async fn authenticate_with_session(
        &mut self,
        auth_mode: &AuthMode,
        token: &[u8],
    ) -> Result<bool> {
        self.state = ConnectionState::Writing;
        match AdminCommand::authenticate_session(self, auth_mode, token).await {
            Ok(ok) => {
                // Restore Ready so PooledConnection::Drop puts the conn back
                // in the pool instead of taking the non-recoverable close arm.
                self.set_state(ConnectionState::Ready);
                Ok(ok)
            }
            Err(err) => {
                self.close();
                Err(err)
            }
        }
    }

    pub const fn bookmark(&mut self) {
        self.bytes_read = 0;
    }

    pub const fn bytes_read(&self) -> usize {
        self.bytes_read
    }

    pub(crate) const fn should_attempt_recovery(&self) -> bool {
        self.can_recover_connection && self.timeout_delay > 0
    }

    // reads the rest of the message to empty the connection buffer
    // before returning the connection back to the pool.
    async fn drain(&mut self, mut limit: usize, timeout: Duration) -> Result<()> {
        while limit > 0 {
            let count = match self.conn {
                Netsocket::Tcp(ref mut conn) => {
                    let mut reader = conn.take(limit as u64);
                    let mut sink = aerospike_rt::io::sink();
                    io_with_timeout!(
                        self,
                        timeout,
                        aerospike_rt::io::copy(&mut reader, &mut sink)
                    )
                    .unwrap_or_else(|_| {
                        Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "Timeout draining the connection",
                        ))
                    })
                    .map_err(|e| Error::timeout(format!("Timeout draining the connection {e}")))?
                }

                #[cfg(feature = "tls")]
                Netsocket::Tls(ref mut conn) => {
                    let mut reader = conn.take(limit as u64);
                    let mut sink = aerospike_rt::io::sink();
                    io_with_timeout!(
                        self,
                        timeout,
                        aerospike_rt::io::copy(&mut reader, &mut sink)
                    )
                    .unwrap_or_else(|_| {
                        Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "Timeout draining the connection",
                        ))
                    })
                    .map_err(|e| Error::timeout(format!("Timeout draining the connection {e}")))?
                }
                #[cfg(test)]
                _ => unreachable!(),
            };

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

    /// When set, `fill_buffer` decompresses from this decoder instead of
    /// reading from the network.  The decoder wraps the compressed payload
    /// and is consumed incrementally as records are parsed.
    decoder: Option<ZlibDecoder<std::io::Cursor<Vec<u8>>>>,
    /// Total decompressed bytes remaining (used for `exhausted` check).
    decoder_remaining: usize,
}

impl<'a> BufferedConn<'a> {
    pub fn new(conn: &'a mut Connection) -> Self {
        BufferedConn {
            conn,
            cache: Vec::with_capacity(4 * 1024),
            limit: 0,
            pos: 0,
            bytes_read: 0,
            decoder: None,
            decoder_remaining: 0,
        }
    }

    /// Creates a `BufferedConn` that streams decompressed data from the
    /// given decoder on demand, avoiding a large decompressed allocation.
    /// `remaining` is the number of decompressed bytes left to read from
    /// the decoder (the caller may have already consumed some, e.g. a header).
    pub fn new_with_decoder(
        conn: &'a mut Connection,
        decoder: ZlibDecoder<std::io::Cursor<Vec<u8>>>,
        remaining: usize,
    ) -> Self {
        BufferedConn {
            conn,
            cache: Vec::with_capacity(4 * 1024),
            limit: 0,
            pos: 0,
            bytes_read: 0,
            decoder: Some(decoder),
            decoder_remaining: remaining,
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
        self.conn
            .set_state(ConnectionState::ReadingStreamHeader(size));
        self.set_limit(size)
    }

    pub(crate) fn set_limit_body(&mut self, size: usize) -> Result<()> {
        self.conn
            .set_state(ConnectionState::ReadingStreamBody(size));
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
            return Err(Error::invalid_argument(format!(
                "Invalid size for buffer: {size}"
            )));
        }

        self.cache.resize(size, 0);

        Ok(())
    }

    async fn fill_buffer(&mut self) -> Result<usize> {
        // fill_buffer fills the buffer from the beginning.
        // The buffer should have been completely consumed before calling this function
        if self.pos != self.cache.len() {
            return Ok(0);
        }

        // Streaming decompression path: read from the decoder instead of the network.
        if self.decoder.is_some() {
            if self.decoder_remaining == 0 {
                return Ok(0);
            }
            let size = min(self.cache.capacity(), self.decoder_remaining);
            self.resize_cache(size)?;
            self.decoder
                .as_mut()
                .unwrap()
                .read_exact(&mut self.cache)
                .map_err(|e| Error::client_error(format!("Decompression error: {e}")))?;
            self.decoder_remaining -= size;
            self.pos = 0;
            return Ok(size);
        }

        if self.limit <= 0 {
            return Ok(0);
        }

        let size = min(self.cache.capacity(), self.limit);
        self.resize_cache(size)?;

        let deadline = self.conn.deadline();
        let read_result = match self.conn.conn {
            Netsocket::Tcp(ref mut conn) => {
                io_with_timeout!(self.conn, deadline, conn.read_exact(&mut self.cache))
            }

            #[cfg(feature = "tls")]
            Netsocket::Tls(ref mut conn) => {
                io_with_timeout!(self.conn, deadline, conn.read_exact(&mut self.cache))
            }
            #[cfg(test)]
            _ => unreachable!(),
        };

        match read_result {
            Ok(Ok(_)) => {
                self.limit -= self.cache.len();
                self.conn.bytes_read += self.cache.len();
            }
            Ok(Err(e)) => return Err(Error::connection(format!("buffered_read: {e}"))),
            Err(_) => {
                return Err(Error::timeout(
                    "Timeout reading from the network connection",
                ))
            }
        }

        self.pos = 0;
        Ok(size)
    }

    pub(crate) async fn drain(&mut self, timeout: Duration) -> Result<()> {
        // Decoder mode: discard remaining decompressed bytes (no network I/O).
        if let Some(ref mut decoder) = self.decoder {
            while self.decoder_remaining > 0 {
                let chunk = min(4096, self.decoder_remaining);
                let mut sink = vec![0u8; chunk];
                decoder
                    .read_exact(&mut sink)
                    .map_err(|e| Error::client_error(format!("Decompression error: {e}")))?;
                self.decoder_remaining -= chunk;
            }

            let _ = self.resize_cache(0);
            self.pos = 0;
            assert!(self.exhausted());
            self.conn.state = ConnectionState::Ready;
            return Ok(());
        }

        while self.limit > 0 {
            let count = match self.conn.conn {
                Netsocket::Tcp(ref mut conn) => {
                    let mut reader = conn.take(self.limit as u64);
                    let mut sink = aerospike_rt::io::sink();
                    io_with_timeout!(
                        self.conn,
                        timeout,
                        aerospike_rt::io::copy(&mut reader, &mut sink)
                    )
                    .unwrap_or_else(|_| {
                        Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "Timeout draining the connection",
                        ))
                    })
                    .map_err(|e| Error::timeout(format!("Timeout draining the connection {e}")))?
                }
                #[cfg(feature = "tls")]
                Netsocket::Tls(ref mut conn) => {
                    let mut reader = conn.take(self.limit as u64);
                    let mut sink = aerospike_rt::io::sink();
                    io_with_timeout!(
                        self.conn,
                        timeout,
                        aerospike_rt::io::copy(&mut reader, &mut sink)
                    )
                    .unwrap_or_else(|_| {
                        Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "Timeout draining the connection",
                        ))
                    })
                    .map_err(|e| Error::timeout(format!("Timeout draining the connection {e}")))?
                }
                #[cfg(test)]
                _ => unreachable!(),
            };

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
        self.limit <= 0 && self.decoder_remaining == 0 && self.empty()
    }

    #[inline]
    const fn len(&self) -> usize {
        self.cache.len() - self.pos
    }

    #[inline]
    const fn empty(&self) -> bool {
        self.len() == 0
    }

    fn cached_read_rest(&mut self) -> usize {
        if !self.empty() {
            return self.cached_read(0, self.len());
        }
        0
    }

    fn cached_read(&mut self, pos: usize, size: usize) -> usize {
        self.conn.buffer.data_buffer[pos..pos + size]
            .copy_from_slice(&self.cache[self.pos..self.pos + size]);

        self.pos += size;
        size
    }

    pub async fn read_buffer(&mut self, size: usize) -> Result<usize> {
        self.conn.buffer.resize_buffer(size)?;

        if (self.limit > 0 || self.decoder.is_some()) && self.empty() {
            self.fill_buffer().await?;
        }

        if size <= self.len() {
            self.cached_read(0, size);
        } else if size > self.len() {
            // we have data left in the buffer, but we need more
            let cached = self.cached_read_rest();
            let remaining = size - cached;
            if self.decoder.is_some() {
                // Decoder mode: decompress directly into data_buffer
                let decoder = self.decoder.as_mut().unwrap();
                decoder
                    .read_exact(&mut self.conn.buffer.data_buffer[cached..cached + remaining])
                    .map_err(|e| Error::client_error(format!("Decompression error: {e}")))?;
                self.decoder_remaining -= remaining;
            } else if remaining > self.cache.capacity() / 2 {
                // read directly from network
                self.conn.read_buffer_at(cached, remaining).await?;
                self.limit -= remaining;
            } else {
                // fill the buffer and read the rest of requested bytes
                self.fill_buffer().await?;
                self.cached_read(cached, remaining);
            }
        }

        self.bytes_read += size;

        self.conn.buffer.reset_offset();
        self.conn.refresh();

        Ok(size)
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.close();
    }
}

pub struct ConnectionRecovery<'a> {
    conn: &'a mut Connection,
    /// Tracks whether the current message being recovered is compressed (type 4).
    compressed_msg: bool,
}

impl<'a> ConnectionRecovery<'a> {
    pub const fn new(conn: &'a mut Connection) -> Self {
        let compressed_msg = conn.compressed_stream_body;
        Self {
            conn,
            compressed_msg,
        }
    }

    pub async fn recover(&mut self) {
        if !self.conn.can_recover_connection || self.conn.timeout_delay == 0 {
            return;
        }

        self.conn.set_socket_timeout(None, self.conn.timeout_delay);
        match self.conn.state {
            ConnectionState::Ready | ConnectionState::Closed | ConnectionState::Writing => (),
            ConnectionState::ReadingHeader(total_size) => {
                let Ok(receive_size) = self.read_header(total_size).await else {
                    return;
                };

                self.conn
                    .set_state(ConnectionState::ReadingBody(receive_size));

                if self.read_body(receive_size).await.is_ok() {
                    self.conn.reset_state();
                }
            }

            ConnectionState::ReadingBody(total_size) => {
                if self.read_body(total_size).await.is_ok() {
                    self.conn.reset_state();
                }
            }

            ConnectionState::ReadingStreamHeader(total_size) => {
                let Ok(mut receive_size) = self.read_stream_header(total_size).await else {
                    return;
                };

                while receive_size > 0 {
                    self.conn
                        .set_state(ConnectionState::ReadingStreamBody(receive_size));
                    match self.read_stream_body(receive_size).await {
                        Ok(true) => {
                            self.conn.reset_state();
                            return;
                        }
                        Err(_) => return,
                        _ => (),
                    }

                    self.conn
                        .set_state(ConnectionState::ReadingStreamHeader(receive_size));
                    receive_size = match self.read_stream_header(total_size).await {
                        Ok(v) => v,
                        Err(_) => return,
                    };
                }
            }

            ConnectionState::ReadingStreamBody(mut receive_size) => {
                while receive_size > 0 {
                    match self.read_stream_body(receive_size).await {
                        Ok(true) => {
                            self.conn.reset_state();
                            return;
                        }
                        Err(_) => return,
                        _ => (),
                    }

                    self.conn.set_state(ConnectionState::ReadingStreamHeader(8));
                    receive_size = match self.read_stream_header(8).await {
                        Ok(v) => v,
                        Err(_) => return,
                    };

                    self.conn
                        .set_state(ConnectionState::ReadingStreamBody(receive_size));
                }
            }
        }
    }

    async fn read_header(&mut self, total_size: usize) -> Result<usize> {
        if total_size > self.conn.bytes_read {
            // read the rest of the header
            if let Err(cause) = self
                .conn
                .read_buffer_at(self.conn.bytes_read, total_size - self.conn.bytes_read)
                .await
            {
                // return early and don't update the connection state
                return Err(Error::stream_terminated(Some(cause)));
            };
        }

        self.conn.buffer.reset_offset();
        let proto = self.conn.buffer.read_u64(Some(0));
        let msg_type = ((proto >> 48) & 0xFF) as u8;
        let proto_size = (proto & 0xFFFF_FFFF_FFFF) as usize;

        if msg_type == buffer::AS_MSG_TYPE_COMPRESSED {
            // Compressed message: 8-byte proto + [8-byte uncompressed_size + compressed_data].
            // We already read `total_size` (30) bytes from the wire, consuming 22 bytes of
            // the payload (total_size - 8). The remaining body to drain is:
            let already_consumed = total_size - 8;
            let receive_size = proto_size.saturating_sub(already_consumed);
            Ok(receive_size)
        } else {
            let header_length = self.conn.buffer.read_u8(Some(8));
            let receive_size = proto_size - usize::from(header_length);
            Ok(receive_size)
        }
    }

    async fn read_body(&mut self, total_size: usize) -> Result<()> {
        if total_size > self.conn.bytes_read {
            // read the rest of the body
            if let Err(cause) = self
                .conn
                .drain(
                    total_size - self.conn.bytes_read,
                    Duration::from_millis(u64::from(self.conn.timeout_delay)),
                )
                .await
            {
                // return early and don't update the connection state
                return Err(Error::stream_terminated(Some(cause)));
            }
        }

        assert!(self.conn.bytes_read == total_size);
        Ok(())
    }

    async fn read_stream_header(&mut self, total_size: usize) -> Result<usize> {
        assert_eq!(total_size, 8);
        if total_size > self.conn.bytes_read {
            // read the rest of the header
            if let Err(cause) = self
                .conn
                .read_buffer_at(self.conn.bytes_read, total_size - self.conn.bytes_read)
                .await
            {
                // return early and don't update the connection state
                return Err(Error::stream_terminated(Some(cause)));
            };
        }

        let proto = self.conn.buffer.read_u64(Some(0));
        let msg_type = ((proto >> 48) & 0xFF) as u8;
        self.compressed_msg = msg_type == buffer::AS_MSG_TYPE_COMPRESSED;

        let receive_size = (proto & 0x0000_FFFF_FFFF_FFFF) as usize;
        Ok(receive_size)
    }

    async fn read_stream_body(&mut self, total_size: usize) -> Result<bool> {
        if self.compressed_msg {
            // Compressed stream message: the body is raw compressed data.
            // We cannot inspect info3 to detect the last record; just drain
            // the entire body and continue to the next message.
            if total_size > self.conn.bytes_read {
                if let Err(cause) = self
                    .conn
                    .drain(
                        total_size - self.conn.bytes_read,
                        Duration::from_millis(u64::from(self.conn.timeout_delay)),
                    )
                    .await
                {
                    return Err(Error::stream_terminated(Some(cause)));
                }
            }

            assert!(self.conn.bytes_read == total_size);
            self.compressed_msg = false;
            return Ok(false);
        }

        // The message has been bigger than a header only last part. Drain it straight away.
        if self.conn.bytes_read > usize::from(crate::commands::buffer::MSG_TOTAL_HEADER_SIZE) {
            // we are past the header portion, clearly not the last message.
            // We can safely drain the connection
            if total_size > self.conn.bytes_read {
                if let Err(cause) = self
                    .conn
                    .drain(
                        total_size - self.conn.bytes_read,
                        Duration::from_millis(u64::from(self.conn.timeout_delay)),
                    )
                    .await
                {
                    // return early and don't update the connection state
                    return Err(Error::stream_terminated(Some(cause)));
                }
            }

            assert!(self.conn.bytes_read == total_size);
            return Ok(false);
        }

        // Still the header portion, so we need to read the rest of it and
        // figure out if this is the last message in the stream.
        if usize::from(crate::commands::buffer::MSG_TOTAL_HEADER_SIZE) > self.conn.bytes_read {
            let remaining = min(
                total_size,
                usize::from(crate::commands::buffer::MSG_TOTAL_HEADER_SIZE) - self.conn.bytes_read,
            );
            if let Err(cause) = self
                .conn
                .read_buffer_at(self.conn.bytes_read, remaining)
                .await
            {
                // return early and don't update the connection state
                return Err(Error::stream_terminated(Some(cause)));
            }
        }

        let info3 = self.conn.buffer.read_u8(Some(3));
        let last_record =
            info3 & crate::commands::buffer::INFO3_LAST == crate::commands::buffer::INFO3_LAST;

        // read the rest of the body
        if total_size > self.conn.bytes_read {
            if let Err(cause) = self
                .conn
                .drain(
                    total_size - self.conn.bytes_read,
                    Duration::from_millis(u64::from(self.conn.timeout_delay)),
                )
                .await
            {
                // return early and don't update the connection state
                return Err(Error::stream_terminated(Some(cause)));
            }
        }

        assert!(self.conn.bytes_read == total_size);
        Ok(last_record)
    }
}

#[cfg(test)]
mod idle_status_tests {
    use super::*;

    async fn conn_with_deadline(deadline: Option<Duration>) -> Connection {
        let mut c = Connection::new(&Host::new("127.0.0.1", 0), &ClientPolicy::default(), None)
            .await
            .unwrap();
        c.idle_deadline = deadline.map(|d| Instant::now() + d);
        c
    }

    #[aerospike_macro::test]
    async fn no_deadline_is_fresh() {
        let c = conn_with_deadline(None).await;
        let now = Instant::now();
        assert_eq!(
            c.idle_status(now, Duration::from_secs(100)),
            IdleStatus::Fresh,
            "idle_timeout = 0 arms no deadline, so nothing ever expires"
        );
    }

    #[aerospike_macro::test]
    async fn deadline_far_away_is_fresh() {
        let c = conn_with_deadline(Some(Duration::from_secs(60))).await;
        assert_eq!(
            c.idle_status(Instant::now(), Duration::from_secs(2)),
            IdleStatus::Fresh
        );
    }

    #[aerospike_macro::test]
    async fn deadline_inside_window_is_expiring() {
        let c = conn_with_deadline(Some(Duration::from_secs(1))).await;
        assert_eq!(
            c.idle_status(Instant::now(), Duration::from_secs(2)),
            IdleStatus::ExpiringSoon
        );
    }

    #[aerospike_macro::test]
    async fn deadline_in_the_past_is_expired() {
        let c = conn_with_deadline(Some(Duration::from_secs(0))).await;
        aerospike_rt::sleep(std::time::Duration::from_millis(5)).await;
        assert_eq!(
            c.idle_status(Instant::now(), Duration::from_secs(2)),
            IdleStatus::Expired,
            "the loose predicate must classify a missed deadline as Expired"
        );
    }
}

///  socket-level liveness probe and the `Error::Connection` classification of socket I/O failures.
/// The pool-checkout liveness probe, on whichever runtime is compiled.
///
/// `tests_eof_loopback` below is tokio-only; these cases are runtime-agnostic on
/// purpose, because the async-std arm of [`Connection::is_alive`] has no other
/// coverage.
#[cfg(test)]
mod liveness_probe_tests {
    use super::*;
    use aerospike_rt::net::{TcpListener, TcpStream};

    /// Half-close the accepted socket, so the client side sees FIN.
    async fn spawn_finning_peer() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        aerospike_rt::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                #[cfg(feature = "rt-tokio")]
                {
                    use aerospike_rt::io::AsyncWriteExt;
                    let _ = sock.shutdown().await;
                }
                #[cfg(feature = "rt-async-std")]
                {
                    let _ = sock.shutdown(aerospike_rt::async_std::net::Shutdown::Both);
                }
                drop(sock);
            }
        });
        addr
    }

    /// Accept and hold the socket open, saying nothing.
    async fn spawn_quiet_peer() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        aerospike_rt::spawn(async move {
            if let Ok((sock, _)) = listener.accept().await {
                aerospike_rt::sleep(Duration::from_secs(30)).await;
                drop(sock);
            }
        });
        addr
    }

    /// `TCP_NODELAY` must be set on every socket the client opens. Asserted
    /// through the real getsockopt rather than by trusting the setter, so a
    /// platform that silently ignores the option fails here instead of in
    /// production.
    ///
    /// This covers [`Connection::set_nodelay`], not its call site: under
    /// `cfg(test)` `Connection::new` returns a `Netsocket::TestDummy` and never
    /// opens a socket, so the real connect path is unreachable from a unit
    /// test.
    #[aerospike_macro::test]
    async fn sockets_are_opened_with_tcp_nodelay() {
        let addr = spawn_quiet_peer().await;
        let stream = TcpStream::connect(&*addr).await.unwrap();

        assert!(
            !socket2::SockRef::from(&stream).tcp_nodelay().unwrap(),
            "a fresh socket is expected to start with TCP_NODELAY unset; if the \
             platform default changed, this test no longer proves anything"
        );

        Connection::set_nodelay(&stream);

        assert!(
            socket2::SockRef::from(&stream).tcp_nodelay().unwrap(),
            "TCP_NODELAY must be set: Nagle deadlocks against the peer's \
             delayed ACK for 40ms on any request written as more than one write"
        );
    }

    /// Build a `Connection` around a real socket, bypassing the handshake.
    fn conn_over(stream: TcpStream) -> Connection {
        let mut conn = Connection {
            addr: "127.0.0.1:0".into(),
            buffer: Buffer::new(0),
            bytes_read: 0,
            conn: Netsocket::Tcp(stream),
            socket_timeout: 5_000,
            timeout_delay: 0,
            deadline: None,
            idle_timeout: None,
            idle_deadline: None,
            state: ConnectionState::Ready,
            can_recover_connection: false,
            response_decompressed: false,
            compressed_stream_body: false,
            rnd: XorShift::new(),
            #[cfg(feature = "rt-tokio")]
            sleep: Box::pin(aerospike_rt::tokio::time::sleep(Duration::from_secs(3600))),
        };
        conn.refresh();
        conn
    }

    #[aerospike_macro::test]
    async fn probe_says_alive_for_an_open_idle_socket() {
        let addr = spawn_quiet_peer().await;
        let stream = TcpStream::connect(&*addr).await.unwrap();
        aerospike_rt::sleep(Duration::from_millis(20)).await;
        assert!(
            conn_over(stream).is_alive(),
            "an open socket with nothing pending must probe alive"
        );
    }

    #[aerospike_macro::test]
    async fn probe_says_dead_after_peer_fin() {
        let addr = spawn_finning_peer().await;
        let stream = TcpStream::connect(&*addr).await.unwrap();
        // Let the FIN land in our kernel before probing.
        aerospike_rt::sleep(Duration::from_millis(50)).await;
        assert!(
            !conn_over(stream).is_alive(),
            "a socket the peer closed must probe dead"
        );
    }

    /// The peek must not consume: probing twice gives the same answer, and the
    /// command that follows still sees the pending bytes.
    #[aerospike_macro::test]
    async fn probe_does_not_consume_pending_bytes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        aerospike_rt::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                #[cfg(feature = "rt-tokio")]
                {
                    use aerospike_rt::io::AsyncWriteExt;
                    let _ = sock.write_all(b"XY").await;
                }
                #[cfg(feature = "rt-async-std")]
                {
                    use futures::AsyncWriteExt;
                    let _ = sock.write_all(b"XY").await;
                }
                aerospike_rt::sleep(Duration::from_secs(30)).await;
            }
        });

        let stream = TcpStream::connect(&*addr).await.unwrap();
        aerospike_rt::sleep(Duration::from_millis(50)).await;
        let mut conn = conn_over(stream);

        // Unsolicited bytes on a plain connection: not usable, twice over.
        assert!(!conn.is_alive());
        assert!(!conn.is_alive(), "the verdict must be stable, not consumed");

        // And the bytes are still on the socket for whoever reads next.
        let mut buf = [0_u8; 2];
        conn.read_all(&mut buf).await.expect("bytes still readable");
        assert_eq!(&buf, b"XY", "MSG_PEEK must leave the data in place");
    }
}

#[cfg(all(test, feature = "rt-tokio"))]
mod tests_eof_loopback {
    use super::*;
    use crate::commands::is_network_error;
    use aerospike_rt::net::{TcpListener, TcpStream};
    use std::net::SocketAddr;

    /// Build a `Connection` over a real TCP stream. Local to this module —
    /// inaccessible from other code, no API surface added.
    fn conn_from_stream(stream: TcpStream) -> Connection {
        let mut conn = Connection {
            addr: "127.0.0.1:0".into(),
            buffer: Buffer::new(0),
            bytes_read: 0,
            conn: Netsocket::Tcp(stream),
            socket_timeout: 5_000,
            timeout_delay: 0,
            deadline: None,
            idle_timeout: None,
            idle_deadline: None,
            state: ConnectionState::Ready,
            can_recover_connection: false,
            response_decompressed: false,
            compressed_stream_body: false,
            rnd: XorShift::new(),
            // Far-future deadline; reset before each IO so this never fires first.
            sleep: Box::pin(aerospike_rt::tokio::time::sleep(
                aerospike_rt::time::Duration::from_secs(3600),
            )),
        };
        conn.refresh();
        conn
    }

    /// Spawn a one-shot peer that accepts and immediately half-closes the
    /// socket — simulates a server-side FIN like `asd` exiting.
    async fn spawn_fin_peer() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        aerospike_rt::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                use tokio::io::AsyncWriteExt;
                let _ = sock.shutdown().await;
                drop(sock);
            }
        });
        addr
    }

    /// Spawn a peer that pushes bytes at the client without being asked —
    /// exercises the "stray bytes pending" branch of the probe.
    async fn spawn_chatty_peer() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        aerospike_rt::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                use tokio::io::AsyncWriteExt;
                let _ = sock.write_all(b"unsolicited").await;
                aerospike_rt::sleep(std::time::Duration::from_secs(60)).await;
            }
        });
        addr
    }

    /// Spawn a peer that accepts and holds the socket open silently —
    /// the live-and-idle case the liveness probe must accept.
    async fn spawn_idle_peer() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        aerospike_rt::spawn(async move {
            if let Ok((sock, _)) = listener.accept().await {
                aerospike_rt::sleep(std::time::Duration::from_secs(60)).await;
                drop(sock);
            }
        });
        addr
    }

    // ─── Bug 1: socket I/O errors classified as Error::Connection ─────────

    #[tokio::test(flavor = "current_thread")]
    async fn read_header_after_peer_fin_yields_error_connection() {
        let addr = spawn_fin_peer().await;
        let stream = TcpStream::connect(addr).await.unwrap();
        let mut conn = conn_from_stream(stream);

        let err = conn
            .read_header()
            .await
            .expect_err("read on FIN'd socket must fail");

        assert!(
            matches!(err.kind(), crate::ErrorKind::Connection),
            "expected connection error on peer FIN, got: {:?}",
            err
        );
        assert!(
            is_network_error(&err),
            "is_network_error must accept this so the retry gate engages; err = {:?}",
            err
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn write_all_after_peer_fin_yields_error_connection() {
        let addr = spawn_fin_peer().await;
        let stream = TcpStream::connect(addr).await.unwrap();
        let mut conn = conn_from_stream(stream);

        // Wait for FIN to propagate so the next write surfaces ECONNRESET
        // before the kernel send-buffer can absorb a small write.
        aerospike_rt::sleep(std::time::Duration::from_millis(50)).await;

        // Force the failure with a write large enough that the kernel can't
        // hide it inside the send buffer.
        let mut last_err: Option<Error> = None;
        for _ in 0..10 {
            let big = vec![0u8; 256 * 1024];
            if let Err(e) = conn.write_all(&big).await {
                last_err = Some(e);
                break;
            }
        }
        let err = last_err.expect("a write must eventually fail after peer FIN");

        assert!(
            matches!(err.kind(), crate::ErrorKind::Connection),
            "expected connection error on peer-closed write, got: {:?}",
            err
        );
        assert!(
            is_network_error(&err),
            "is_network_error must accept this; err = {:?}",
            err
        );
    }

    // ─── Queue::get returns parked live connections ───────────────────────
    //
    // Lives here (rather than in connection_pool.rs) because the
    // `conn_from_stream` helper needs access to `Connection`'s private
    // fields, which are only visible from within `connection.rs`.

    #[tokio::test(flavor = "current_thread")]
    async fn is_alive_returns_true_for_idle_socket() {
        let addr = spawn_idle_peer().await;
        let stream = TcpStream::connect(addr).await.unwrap();
        aerospike_rt::sleep(std::time::Duration::from_millis(20)).await;
        let conn = conn_from_stream(stream);
        assert!(conn.is_alive(), "idle live socket must probe alive");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn is_alive_returns_false_after_peer_fin() {
        let addr = spawn_fin_peer().await;
        let stream = TcpStream::connect(addr).await.unwrap();
        // Give the peer's shutdown a moment to arrive at our kernel.
        aerospike_rt::sleep(std::time::Duration::from_millis(50)).await;
        let conn = conn_from_stream(stream);
        assert!(!conn.is_alive(), "FIN'd socket must probe dead");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn is_alive_returns_false_when_stray_bytes_pending() {
        let addr = spawn_chatty_peer().await;
        let stream = TcpStream::connect(addr).await.unwrap();
        aerospike_rt::sleep(std::time::Duration::from_millis(50)).await;
        let conn = conn_from_stream(stream);
        assert!(
            !conn.is_alive(),
            "a socket with unsolicited bytes is out of step with the protocol"
        );
    }

    /// The regression this whole probe exists for: a pooled socket the peer
    /// closed must not be handed to the next command.
    #[tokio::test(flavor = "current_thread")]
    async fn queue_get_evicts_peer_finned_socket() {
        use crate::net::connection_pool::Queue;
        use crate::net::Host;
        use crate::policy::ClientPolicy;

        let host = Host::new("127.0.0.1", 0);
        let policy = ClientPolicy::default();
        let q = Queue::with_capacity(1, host, policy, None, None);

        let addr = spawn_fin_peer().await;
        let stream = TcpStream::connect(addr).await.unwrap();
        aerospike_rt::sleep(std::time::Duration::from_millis(50)).await;

        let conn = conn_from_stream(stream);
        assert!(q.reserve_capacity());
        q.put_back(conn);

        // The dead socket is dropped rather than returned, so the queue reports
        // empty and the caller opens a fresh connection instead of failing on
        // its first read.
        let result = q.get();
        assert!(
            result.is_err(),
            "Queue::get() must evict a peer-FIN'd socket, not hand it out"
        );
        assert_eq!(
            q.num_conns(),
            0,
            "the dead socket must be gone from the queue"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn queue_get_returns_live_socket() {
        use crate::net::connection_pool::Queue;
        use crate::net::Host;
        use crate::policy::ClientPolicy;

        let host = Host::new("127.0.0.1", 0);
        let policy = ClientPolicy::default();
        let q = Queue::with_capacity(1, host, policy, None, None);

        let addr = spawn_idle_peer().await;
        let stream = TcpStream::connect(addr).await.unwrap();
        aerospike_rt::sleep(std::time::Duration::from_millis(50)).await;

        let conn = conn_from_stream(stream);
        assert!(q.reserve_capacity());
        q.put_back(conn);

        let result = q.get();
        assert!(
            result.is_ok(),
            "Queue::get() must return a live socket; got Err({:?})",
            result.err()
        );
    }
}

/// A TLS write only reaches the peer once the session's outgoing buffer has
/// been drained to the socket. `tokio-rustls` reports `write_all` as complete
/// while ciphertext is still held inside the session — its `poll_write` says so
/// outright: *"it does not guarantee the final data to be sent. To be cautious,
/// you must manually call `flush`"* — and nothing on the read path pushes those
/// bytes out. Without an explicit flush, a large request therefore stalls until
/// the socket timeout fires, while the server sits waiting for a request it
/// never fully received.
#[cfg(all(test, feature = "tls", feature = "rt-tokio"))]
mod tls_flush_tests {
    use super::*;
    use aerospike_rt::net::{TcpListener, TcpStream};
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use rustls::{RootCertStore, ServerConfig};
    use std::net::SocketAddr;
    use tokio::sync::oneshot;
    use tokio_rustls::TlsAcceptor;

    /// Comfortably past any socket buffer plus rustls' 64 KiB outgoing buffer,
    /// so the write is certain to meet a non-writable socket.
    const PAYLOAD: usize = 1024 * 1024;
    /// Small socket buffers keep the pipe saturated whatever the platform's
    /// autotuning would otherwise do.
    const SOCK_BUF: usize = 16 * 1024;
    /// How long the peer waits for bytes that may never come.
    const READ_STALL: Duration = Duration::from_secs(5);

    /// Self-signed `localhost` certificate for the loopback peer below, with a
    /// P-256 key, valid until 2126. Baked in rather than generated at test time:
    /// a certificate generator would be a build-time dependency whose own MSRV
    /// can drift past this crate's `rust-version`, breaking CI with no change
    /// on our side. These are test fixtures — this key secures nothing, is
    /// never presented off this machine, and exists only so two sockets in one
    /// process can complete a handshake. If regenerated, it must carry
    /// `basicConstraints=critical,CA:FALSE` -- webpki rejects a CA certificate
    /// presented as an end entity (`CaUsedAsEndEntity`).
    const TEST_CERT_PEM: &[u8] = b"\
-----BEGIN CERTIFICATE-----
MIIBkjCCATigAwIBAgIUBd8CSag9UwVN+CmSCt1fHb95AXowCgYIKoZIzj0EAwIw
FDESMBAGA1UEAwwJbG9jYWxob3N0MCAXDTI2MDgyMTAxMDcyOFoYDzIxMjYwNzI4
MDEwNzI4WjAUMRIwEAYDVQQDDAlsb2NhbGhvc3QwWTATBgcqhkjOPQIBBggqhkjO
PQMBBwNCAARaDG4MJdt4ujwjndx1baO6lEZF2JIggiXBCqFUdjj6IPPzkDZtMO1f
U3lfoCm6z5EGqRhWg8An6dxdhFCdc2AZo2YwZDAdBgNVHQ4EFgQUkmu2kSnGV8kL
9Fd/i3OqX64gxlowHwYDVR0jBBgwFoAUkmu2kSnGV8kL9Fd/i3OqX64gxlowFAYD
VR0RBA0wC4IJbG9jYWxob3N0MAwGA1UdEwEB/wQCMAAwCgYIKoZIzj0EAwIDSAAw
RQIgD7tXhh5Ldb6hp8VcfcmI8MZQf5TrJSJO2SEXAVOzcmECIQCfDq3M7/QhO7f/
+5kpUI6o5q5Lp53WCGOvmKjvMmEfKg==
-----END CERTIFICATE-----
";

    /// Private key for [`TEST_CERT_PEM`]. Test fixture only; see the note there.
    const TEST_KEY_PEM: &[u8] = b"\
-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgZcTGiz3ft6sc5Q+L
rHUGuKRhKz67vcrzXrqQFgLuGO+hRANCAARaDG4MJdt4ujwjndx1baO6lEZF2JIg
giXBCqFUdjj6IPPzkDZtMO1fU3lfoCm6z5EGqRhWg8An6dxdhFCdc2AZ
-----END PRIVATE KEY-----
";

    /// Server config for the loopback peer, plus a root store that trusts it.
    fn self_signed() -> (ServerConfig, RootCertStore) {
        let cert = CertificateDer::from_pem_slice(TEST_CERT_PEM).expect("test certificate");
        let signing_key = PrivateKeyDer::from_pem_slice(TEST_KEY_PEM).expect("test key");

        let server = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert.clone()], signing_key)
            .unwrap();

        let mut roots = RootCertStore::empty();
        roots.add(cert).unwrap();
        (server, roots)
    }

    /// A minimal but valid `AS_MSG` reply: proto version 2, message type 3 and
    /// a 22-byte remaining header — exactly what `read_header` validates.
    fn reply_header() -> Vec<u8> {
        let proto = u64::from(buffer::MSG_REMAINING_HEADER_SIZE) | (2_u64 << 56) | (3_u64 << 48);
        let mut msg = proto.to_be_bytes().to_vec();
        msg.resize(usize::from(buffer::MSG_TOTAL_HEADER_SIZE), 0);
        msg[8] = buffer::MSG_REMAINING_HEADER_SIZE;
        msg
    }

    /// A peer that completes the handshake and then drains the stream
    /// deliberately slowly, so the client's socket stays full for the whole
    /// write and the last `poll_write` is certain to leave ciphertext behind.
    /// Reports how many plaintext bytes it managed to read; a stalled read is
    /// reported as a short count rather than hanging the test.
    fn spawn_slow_reader(listener: TcpListener, acceptor: TlsAcceptor) -> oneshot::Receiver<usize> {
        let (tx, rx) = oneshot::channel();
        aerospike_rt::spawn(async move {
            let Ok((sock, _)) = listener.accept().await else {
                return;
            };
            let _ = socket2::SockRef::from(&sock).set_recv_buffer_size(SOCK_BUF);
            let Ok(mut tls) = acceptor.accept(sock).await else {
                return;
            };

            let mut got = 0;
            let mut chunk = vec![0_u8; 16 * 1024];
            while got < PAYLOAD {
                // Pace the drain: a peer that keeps up would let the kernel
                // swallow the tail and hide the missing flush.
                aerospike_rt::sleep(Duration::from_millis(2)).await;
                // Anything but fresh bytes — a stall, EOF, an error — means the
                // rest of the request is never arriving.
                match aerospike_rt::timeout(READ_STALL, tls.read(&mut chunk)).await {
                    Ok(Ok(n)) if n > 0 => got += n,
                    _ => break,
                }
            }

            // Only a request that arrived in full earns a reply. That is the
            // whole point: a client that stranded its tail is left waiting for
            // a response no server would ever send.
            if got == PAYLOAD {
                let _ = tls.write_all(&reply_header()).await;
                let _ = tls.flush().await;
            }
            let _ = tx.send(got);
            // Hold the socket open so a close cannot race the reply.
            aerospike_rt::sleep(READ_STALL).await;
        });
        rx
    }

    /// A `Connection` over a real TLS stream to a slow-reading peer.
    async fn tls_pair() -> (Connection, oneshot::Receiver<usize>) {
        let (server_config, roots) = self_signed();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let _ = socket2::SockRef::from(&listener).set_recv_buffer_size(SOCK_BUF);
        let addr: SocketAddr = listener.local_addr().unwrap();
        let reader = spawn_slow_reader(listener, TlsAcceptor::from(Arc::new(server_config)));

        let stream = TcpStream::connect(addr).await.unwrap();
        let _ = socket2::SockRef::from(&stream).set_send_buffer_size(SOCK_BUF);

        let config = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let tls = TlsConnector::from(Arc::new(config))
            .connect(ServerName::try_from("localhost").unwrap(), stream)
            .await
            .unwrap();

        let mut conn = Connection {
            addr: addr.to_string(),
            buffer: Buffer::new(0),
            bytes_read: 0,
            conn: Netsocket::Tls(tls),
            socket_timeout: 30_000,
            timeout_delay: 0,
            deadline: None,
            idle_timeout: None,
            idle_deadline: None,
            state: ConnectionState::Ready,
            can_recover_connection: false,
            response_decompressed: false,
            compressed_stream_body: false,
            rnd: XorShift::new(),
            sleep: Box::pin(aerospike_rt::tokio::time::sleep(Duration::from_secs(3600))),
        };
        conn.refresh();
        (conn, reader)
    }

    /// The invariant the flush exists for: nothing the peer still needs may be
    /// left inside the session once the write call has returned.
    fn assert_session_drained(conn: &Connection) {
        let Netsocket::Tls(ref tls) = conn.conn else {
            unreachable!("test builds a TLS connection")
        };
        assert!(
            !tls.get_ref().1.wants_write(),
            "the write returned with ciphertext still buffered in the TLS \
             session: the request is not on the wire, and since the read path \
             never drives a write, the command stalls until the socket timeout"
        );
    }

    async fn assert_peer_got_everything(reader: oneshot::Receiver<usize>) {
        let got = reader.await.expect("the peer task must report a count");
        assert_eq!(
            got, PAYLOAD,
            "peer received {got} of {PAYLOAD} bytes; the tail never left the client"
        );
    }

    /// `Connection::flush` — the path every command's request takes.
    #[tokio::test(flavor = "current_thread")]
    async fn flush_puts_the_whole_request_on_the_wire() {
        let (mut conn, reader) = tls_pair().await;
        conn.buffer.data_buffer = vec![0xAB; PAYLOAD];

        conn.flush().await.expect("flush must succeed");

        assert_session_drained(&conn);
        assert_peer_got_everything(reader).await;
    }

    /// `Connection::write_all` — the path info commands take.
    #[tokio::test(flavor = "current_thread")]
    async fn write_all_puts_the_whole_request_on_the_wire() {
        let (mut conn, reader) = tls_pair().await;
        let payload = vec![0xCD; PAYLOAD];

        conn.write_all(&payload).await.expect("write must succeed");

        assert_session_drained(&conn);
        assert_peer_got_everything(reader).await;
    }

    /// The whole round trip, as a command experiences it: request out, response
    /// header back. Without the flush the peer is still waiting for the tail of
    /// a request the client already called sent, so nothing ever answers and
    /// the command dies on the read deadline — the socket timeout reported from
    /// the field.
    #[tokio::test(flavor = "current_thread")]
    async fn a_flushed_request_earns_a_reply_instead_of_a_socket_timeout() {
        let (mut conn, _reader) = tls_pair().await;
        conn.buffer.data_buffer = vec![0xAB; PAYLOAD];

        conn.flush().await.expect("flush must succeed");

        // Tight read deadline: the peer only needs to drain what is already in
        // flight before it answers, so a request that did leave the client is
        // answered well inside this, and one that did not fails promptly.
        conn.set_socket_timeout(None, 2_000);
        let size = conn.read_header().await.unwrap_or_else(|err| {
            panic!("no reply to a request the client reported as sent: {err}")
        });

        assert_eq!(
            size,
            usize::from(buffer::MSG_TOTAL_HEADER_SIZE),
            "a full response header must have been read"
        );
    }
}
