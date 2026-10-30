// Copyright 2015-2026 Aerospike, Inc.
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

//! The DNS seed candidate provider.

use std::collections::HashMap;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use aerospike_rt::time::Instant;

use super::{SeedCandidateContext, SeedCandidateProvider, DNS_PROVIDER};
use crate::errors::{Error, Result};
use crate::net::Host;

/// Record type a [`DnsSeedCandidateProvider`] queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum RecordType {
    /// A and AAAA records. The default.
    #[default]
    A,
    /// SRV records. Targets are resolved with A, then AAAA, queries.
    Srv,
}

impl RecordType {
    /// The YAML spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            RecordType::A => "a",
            RecordType::Srv => "srv",
        }
    }

    /// The YAML spellings, for error messages.
    pub const VALUES: [&'static str; 2] = ["a", "srv"];
}

impl std::str::FromStr for RecordType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "a" => Ok(RecordType::A),
            "srv" => Ok(RecordType::Srv),
            other => Err(Error::invalid_argument(format!(
                "Invalid DNS seed provider record_type value '{other}': valid values: {}",
                Self::VALUES.join(", ")
            ))),
        }
    }
}

/// Default port set on hosts resolved from A and AAAA records.
pub const DEFAULT_PORT: u16 = 3000;
/// Default total time for one refresh, in milliseconds.
pub const DEFAULT_TIMEOUT_MS: u32 = 80;
const DNS_PORT: u16 = 53;

const TYPE_A: u16 = 1;
const TYPE_AAAA: u16 = 28;
const TYPE_SRV: u16 = 33;
const CLASS_IN: u16 = 1;
const RCODE_NXDOMAIN: u8 = 3;
const RCODE_SERVFAIL: u8 = 2;
const MAX_UDP: usize = 4096;

/// Seed candidate provider that resolves DNS names on every refresh and
/// returns the results as seed hosts. YAML: `provider: dns` with the `dns`
/// block.
///
/// Every returned host is an IP address, never a host name, so the client
/// does no further name resolution when it seeds from the result. Hosts are
/// deduplicated and returned in the same order for the same DNS answers.
///
/// The provider needs periodic refresh and runs on the tend task, so its
/// [`timeout`](Builder::timeout) must not exceed
/// [`ClientPolicy::discovery_tend_refresh_deadline`](crate::ClientPolicy::discovery_tend_refresh_deadline);
/// client construction fails otherwise. With the default deadline of 100 ms,
/// only a local resolver answers reliably in time.
///
/// Failure: a name that does not resolve is logged at warning level and the
/// others are returned. When every name fails, the refresh fails with
/// `DNS seed discovery failed for every name: ...`, which keeps the previous
/// seed list on a periodic refresh and, at construction, leaves the static
/// seeds.
///
/// ```rust,edition2021
/// use aerospike::discovery::dns::{DnsSeedCandidateProvider, RecordType};
/// use aerospike::discovery::SeedMergePolicy;
/// use aerospike::ClientPolicy;
/// use std::sync::Arc;
///
/// # fn main() -> aerospike::Result<()> {
/// let provider = DnsSeedCandidateProvider::builder()
///     .names(["_aerospike._tcp.client-net.example.com"])
///     .record_type(RecordType::Srv)
///     .timeout(50)
///     .servers(["10.20.0.2"])
///     .build()?;
/// let mut policy = ClientPolicy::default();
/// policy.seed_candidate_provider = Some(Arc::new(provider));
/// policy.discovery_merge_policy = SeedMergePolicy::DiscoveryOnly;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsSeedCandidateProvider {
    names: Vec<String>,
    record_type: RecordType,
    port: u16,
    tls_name: Option<String>,
    timeout: u32,
    servers: Vec<String>,
    resolvers: Vec<SocketAddr>,
}

impl DnsSeedCandidateProvider {
    /// Returns a builder.
    #[must_use]
    pub fn builder() -> Builder {
        Builder::default()
    }

    /// The names resolved on every refresh.
    #[must_use]
    pub fn names(&self) -> &[String] {
        &self.names
    }

    /// The record type queried.
    #[must_use]
    pub const fn record_type(&self) -> RecordType {
        self.record_type
    }

    /// The port set on hosts from A and AAAA records.
    #[must_use]
    pub const fn port(&self) -> u16 {
        self.port
    }

    /// The TLS name set on every returned host.
    #[must_use]
    pub fn tls_name(&self) -> Option<&str> {
        self.tls_name.as_deref()
    }

    /// Total time for one refresh, in milliseconds.
    #[must_use]
    pub const fn timeout(&self) -> u32 {
        self.timeout
    }

    /// The configured resolver addresses; empty for the system resolvers.
    #[must_use]
    pub fn servers(&self) -> &[String] {
        &self.servers
    }

    async fn resolve_a(&self, name: &str, deadline: Instant) -> std::result::Result<Vec<Host>, Reason> {
        let addresses = self.addresses(name, deadline, false).await?;
        Ok(addresses
            .into_iter()
            .map(|address| self.host(&address, self.port))
            .collect())
    }

    async fn resolve_srv(&self, name: &str, deadline: Instant) -> std::result::Result<Vec<Host>, Reason> {
        let mut records: Vec<Srv> = Vec::new();
        for value in self.query(name, TYPE_SRV, deadline).await? {
            match Srv::parse(&value) {
                Some(srv) if srv.target == "." => {}
                Some(srv) => records.push(srv),
                None => warn!("DNS seed name {name} returned an invalid SRV record: {value}"),
            }
        }
        if records.is_empty() {
            return Ok(Vec::new());
        }
        records.sort();

        let mut targets: HashMap<String, Option<String>> = HashMap::new();
        let mut list = Vec::with_capacity(records.len());
        for srv in &records {
            if !targets.contains_key(&srv.target) {
                let resolved = match self.addresses(&srv.target, deadline, true).await {
                    Ok(addresses) if addresses.is_empty() => Err(Reason::Other("empty answer".into())),
                    other => other,
                };
                let address = match resolved {
                    Ok(addresses) => addresses.into_iter().next(),
                    Err(reason) => {
                        warn!(
                            "DNS seed name {name} SRV target {} dropped: {reason}",
                            srv.target
                        );
                        None
                    }
                };
                targets.insert(srv.target.clone(), address);
            }
            if let Some(Some(address)) = targets.get(&srv.target) {
                list.push(self.host(address, srv.port));
            }
        }
        if list.is_empty() {
            return Err(Reason::Other("no SRV target resolved".into()));
        }
        Ok(list)
    }

    fn host(&self, address: &str, port: u16) -> Host {
        Host {
            name: address.to_string(),
            tls_name: self.tls_name.clone(),
            port,
        }
    }

    /// The A, then AAAA, addresses of `name`, each type sorted. With `first`,
    /// the AAAA query is skipped once an A record answered.
    async fn addresses(
        &self,
        name: &str,
        deadline: Instant,
        first: bool,
    ) -> std::result::Result<Vec<String>, Reason> {
        let mut addresses = Vec::new();
        let mut error = None;
        for qtype in [TYPE_A, TYPE_AAAA] {
            if first && !addresses.is_empty() {
                break;
            }
            match self.query(name, qtype, deadline).await {
                Ok(mut values) => {
                    values.sort();
                    addresses.extend(values);
                }
                Err(Reason::NxDomain) => return Err(Reason::NxDomain),
                Err(reason) => error = Some(reason),
            }
        }
        match error {
            Some(reason) if addresses.is_empty() => Err(reason),
            _ => Ok(addresses),
        }
    }

    /// One query against the resolvers, in order, each bounded by a share of
    /// the time left: a truncated UDP answer is retried over TCP to the same
    /// server with the same timeout again.
    async fn query(
        &self,
        name: &str,
        qtype: u16,
        deadline: Instant,
    ) -> std::result::Result<Vec<String>, Reason> {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(Reason::Timeout);
        }
        let per_query = remaining / (2 * self.resolvers.len() as u32);
        let per_query = per_query.max(Duration::from_millis(1));
        let mut last = Reason::Timeout;
        for resolver in &self.resolvers {
            match query_server(*resolver, name, qtype, per_query).await {
                Ok(values) => return Ok(values),
                Err(Reason::NxDomain) => return Err(Reason::NxDomain),
                Err(reason) => last = reason,
            }
        }
        Err(last)
    }
}

#[async_trait::async_trait]
impl SeedCandidateProvider for DnsSeedCandidateProvider {
    /// Resolves every name and returns the hosts, deduplicated by address and
    /// port, within [`timeout`](Self::timeout).
    async fn refresh_seed_candidates(&self, _ctx: &SeedCandidateContext) -> Result<Vec<Host>> {
        let deadline = Instant::now() + Duration::from_millis(u64::from(self.timeout));
        let mut hosts: Vec<Host> = Vec::new();
        let mut failures: Vec<(String, Reason)> = Vec::new();
        for name in &self.names {
            let result = match self.record_type {
                RecordType::Srv => self.resolve_srv(name, deadline).await,
                RecordType::A => self.resolve_a(name, deadline).await,
            };
            match result {
                Ok(list) => {
                    if list.is_empty() {
                        failures.push((name.clone(), Reason::Other("empty answer".into())));
                    }
                    for host in list {
                        if !hosts
                            .iter()
                            .any(|h| h.name == host.name && h.port == host.port)
                        {
                            hosts.push(host);
                        }
                    }
                }
                Err(reason) => failures.push((name.clone(), reason)),
            }
        }

        if hosts.is_empty() {
            let detail = failures
                .iter()
                .map(|(name, reason)| format!("{name}: {reason}"))
                .collect::<Vec<_>>()
                .join("; ");
            return Err(Error::client_error(format!(
                "DNS seed discovery failed for every name: {detail}"
            )));
        }
        for (name, reason) in &failures {
            warn!("DNS seed name {name} failed: {reason}");
        }
        debug!("DNS seed names {:?} resolved to {hosts:?}", self.names);
        Ok(hosts)
    }

    /// `true`: the answers can change.
    fn needs_periodic_refresh(&self) -> bool {
        true
    }

    /// `true`: all I/O of one refresh is bounded by [`timeout`](Self::timeout).
    fn supports_deadline(&self) -> bool {
        true
    }

    fn refresh_timeout(&self) -> Option<Duration> {
        Some(Duration::from_millis(u64::from(self.timeout)))
    }

    fn name(&self) -> String {
        DNS_PROVIDER.to_string()
    }
}

impl fmt::Display for DnsSeedCandidateProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "dns {:?} {:?}", self.names, self.record_type)
    }
}

/// Builder for [`DnsSeedCandidateProvider`]. Values are validated by
/// [`build`](Self::build).
#[derive(Debug, Clone, Default)]
pub struct Builder {
    names: Vec<String>,
    record_type: Option<RecordType>,
    port: Option<u16>,
    tls_name: Option<String>,
    timeout: Option<u32>,
    servers: Vec<String>,
}

impl Builder {
    /// DNS names to resolve. Required: one or more names, no duplicates
    /// (compared case-insensitively, trailing dot optional). A name is
    /// queried as an absolute name.
    #[must_use]
    pub fn names<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.names = names.into_iter().map(Into::into).collect();
        self
    }

    /// Record type queried for each name. Default: [`RecordType::A`].
    #[must_use]
    pub const fn record_type(mut self, record_type: RecordType) -> Self {
        self.record_type = Some(record_type);
        self
    }

    /// Port set on hosts resolved from A and AAAA records; ignored for SRV,
    /// which uses each record's port. Default: [`DEFAULT_PORT`].
    #[must_use]
    pub const fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    /// TLS name set on every returned host. Default: none, in which case the
    /// TLS name is defaulted as for configured seeds.
    #[must_use]
    pub fn tls_name(mut self, tls_name: impl Into<String>) -> Self {
        self.tls_name = Some(tls_name.into());
        self
    }

    /// Total time in milliseconds for one refresh, across every name, record
    /// type, SRV target and resolver. Must be greater than zero and must not
    /// exceed the discovery tend refresh deadline, which is checked at client
    /// construction. Default: [`DEFAULT_TIMEOUT_MS`].
    #[must_use]
    pub const fn timeout(mut self, timeout_ms: u32) -> Self {
        self.timeout = Some(timeout_ms);
        self
    }

    /// Resolver addresses, each an IP address with an optional port
    /// (`ip[:port]`, an IPv6 address in brackets when a port is given:
    /// `[2001:db8::1]:53`), tried in order. Default: the system resolvers,
    /// read once by [`build`](Self::build).
    #[must_use]
    pub fn servers<I, S>(mut self, servers: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.servers = servers.into_iter().map(Into::into).collect();
        self
    }

    /// Validates the settings and builds the provider.
    ///
    /// # Errors
    ///
    /// An invalid setting fails with a message naming it.
    pub fn build(self) -> Result<DnsSeedCandidateProvider> {
        if self.names.is_empty() {
            return Err(Error::invalid_argument(
                "Invalid DNS seed provider names: at least one name is required",
            ));
        }
        let mut seen: Vec<String> = Vec::new();
        for name in &self.names {
            if name.is_empty() || name == "." || name.contains(' ') {
                return Err(Error::invalid_argument(format!(
                    "Invalid DNS seed provider name: '{name}'"
                )));
            }
            let key = absolute(name).to_ascii_lowercase();
            if seen.contains(&key) {
                return Err(Error::invalid_argument(format!(
                    "Invalid DNS seed provider names: duplicate name '{name}'"
                )));
            }
            seen.push(key);
        }
        let port = self.port.unwrap_or(DEFAULT_PORT);
        if port == 0 {
            return Err(Error::invalid_argument(
                "Invalid DNS seed provider port: 0",
            ));
        }
        if self.tls_name.as_deref() == Some("") {
            return Err(Error::invalid_argument(
                "Invalid DNS seed provider tls_name: empty",
            ));
        }
        let timeout = self.timeout.unwrap_or(DEFAULT_TIMEOUT_MS);
        if timeout == 0 {
            return Err(Error::invalid_argument(
                "Invalid DNS seed provider timeout: 0",
            ));
        }
        let resolvers = if self.servers.is_empty() {
            system_resolvers()?
        } else {
            self.servers
                .iter()
                .map(|server| parse_server(server))
                .collect::<Result<Vec<_>>>()?
        };
        Ok(DnsSeedCandidateProvider {
            names: self.names,
            record_type: self.record_type.unwrap_or_default(),
            port,
            tls_name: self.tls_name,
            timeout,
            servers: self.servers,
            resolvers,
        })
    }
}

fn absolute(name: &str) -> String {
    if name.ends_with('.') {
        name.to_string()
    } else {
        format!("{name}.")
    }
}

/// Parses `ip[:port]` (`[ipv6]:port`) into a resolver address.
fn parse_server(server: &str) -> Result<SocketAddr> {
    let invalid = || Error::invalid_argument(format!("Invalid DNS seed provider server: '{server}'"));
    let (host, port) = if let Some(rest) = server.strip_prefix('[') {
        let end = rest.find(']').ok_or_else(invalid)?;
        let host = &rest[..end];
        let after = &rest[end + 1..];
        let port = if after.is_empty() {
            None
        } else {
            Some(after.strip_prefix(':').ok_or_else(invalid)?)
        };
        (host, port)
    } else if server.matches(':').count() > 1 {
        (server, None)
    } else if let Some((host, port)) = server.split_once(':') {
        (host, Some(port))
    } else {
        (server, None)
    };
    let ip: IpAddr = host.parse().map_err(|_| {
        Error::invalid_argument(format!(
            "Invalid DNS seed provider server: '{server}': expected an IP address"
        ))
    })?;
    let port = match port {
        None => DNS_PORT,
        Some(p) => match p.parse::<u16>() {
            Ok(p) if p > 0 => p,
            _ => {
                return Err(Error::invalid_argument(format!(
                    "Invalid DNS seed provider server: '{server}': invalid port"
                )))
            }
        },
    };
    Ok(SocketAddr::new(ip, port))
}

/// The `nameserver` entries of `/etc/resolv.conf`.
fn system_resolvers() -> Result<Vec<SocketAddr>> {
    let text = std::fs::read_to_string("/etc/resolv.conf").map_err(|e| {
        Error::client_error(format!(
            "DNS seed provider: failed to read system DNS servers from /etc/resolv.conf: {e}"
        ))
    })?;
    let resolvers: Vec<SocketAddr> = text
        .lines()
        .map(str::trim)
        .filter_map(|line| line.strip_prefix("nameserver"))
        .filter_map(|rest| rest.split_whitespace().next())
        .filter_map(|addr| {
            // A scoped IPv6 address (`fe80::1%en0`) is not a usable literal here.
            addr.split('%').next()?.parse::<IpAddr>().ok()
        })
        .map(|ip| SocketAddr::new(ip, DNS_PORT))
        .collect();
    if resolvers.is_empty() {
        return Err(Error::client_error(
            "DNS seed provider: no system DNS server found. Set servers",
        ));
    }
    Ok(resolvers)
}

/// Why a name did not resolve.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Reason {
    NxDomain,
    ServFail,
    Timeout,
    Other(String),
}

impl fmt::Display for Reason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Reason::NxDomain => f.write_str("NXDOMAIN"),
            Reason::ServFail => f.write_str("SERVFAIL"),
            Reason::Timeout => f.write_str("timeout"),
            Reason::Other(s) => f.write_str(s),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Srv {
    priority: u16,
    /// Stored negated so the derived order puts the highest weight first.
    neg_weight: i32,
    target: String,
    port: u16,
}

impl Srv {
    /// Parses `priority weight port target`.
    fn parse(value: &str) -> Option<Self> {
        let fields: Vec<&str> = value.split_whitespace().collect();
        if fields.len() != 4 {
            return None;
        }
        let priority: u16 = fields[0].parse().ok()?;
        let weight: u16 = fields[1].parse().ok()?;
        let port: u16 = fields[2].parse().ok()?;
        if port == 0 {
            return None;
        }
        Some(Srv {
            priority,
            neg_weight: -i32::from(weight),
            target: fields[3].to_ascii_lowercase(),
            port,
        })
    }
}

// ---------------------------------------------------------------------------
// Wire protocol
// ---------------------------------------------------------------------------

/// Sends one query to `server` and returns the answer values of `qtype`,
/// within `timeout`. A truncated UDP reply is retried over TCP.
async fn query_server(
    server: SocketAddr,
    name: &str,
    qtype: u16,
    timeout: Duration,
) -> std::result::Result<Vec<String>, Reason> {
    let id: u16 = rand::random();
    let request = encode_query(id, name, qtype).map_err(Reason::Other)?;

    let reply = aerospike_rt::timeout(timeout, udp_exchange(server, &request, id))
        .await
        .map_err(|_| Reason::Timeout)??;
    let decoded = decode_reply(&reply, id, qtype)?;
    if !decoded.truncated {
        return decoded.into_values();
    }
    let reply = aerospike_rt::timeout(timeout, tcp_exchange(server, &request))
        .await
        .map_err(|_| Reason::Timeout)??;
    decode_reply(&reply, id, qtype)?.into_values()
}

async fn udp_exchange(server: SocketAddr, request: &[u8], id: u16) -> std::result::Result<Vec<u8>, Reason> {
    use aerospike_rt::net::UdpSocket;

    let bind: SocketAddr = if server.is_ipv6() {
        (Ipv6Addr::UNSPECIFIED, 0).into()
    } else {
        (Ipv4Addr::UNSPECIFIED, 0).into()
    };
    let socket = UdpSocket::bind(bind)
        .await
        .map_err(|e| Reason::Other(format!("udp bind: {e}")))?;
    socket
        .send_to(request, server)
        .await
        .map_err(|e| Reason::Other(format!("udp send: {e}")))?;
    let mut buf = vec![0u8; MAX_UDP];
    loop {
        let (n, from) = socket
            .recv_from(&mut buf)
            .await
            .map_err(|e| Reason::Other(format!("udp receive: {e}")))?;
        // A stray datagram, or one answering another query, is ignored.
        if from.ip() != server.ip() || n < 2 || u16::from_be_bytes([buf[0], buf[1]]) != id {
            continue;
        }
        return Ok(buf[..n].to_vec());
    }
}

async fn tcp_exchange(server: SocketAddr, request: &[u8]) -> std::result::Result<Vec<u8>, Reason> {
    use aerospike_rt::net::TcpStream;
    #[cfg(feature = "rt-tokio")]
    use aerospike_rt::io::{AsyncReadExt, AsyncWriteExt};
    #[cfg(feature = "rt-async-std")]
    use futures::{AsyncReadExt, AsyncWriteExt};

    let mut stream = TcpStream::connect(server)
        .await
        .map_err(|e| Reason::Other(format!("tcp connect: {e}")))?;
    let mut framed = Vec::with_capacity(request.len() + 2);
    framed.extend_from_slice(&(request.len() as u16).to_be_bytes());
    framed.extend_from_slice(request);
    stream
        .write_all(&framed)
        .await
        .map_err(|e| Reason::Other(format!("tcp send: {e}")))?;
    let mut len = [0u8; 2];
    stream
        .read_exact(&mut len)
        .await
        .map_err(|e| Reason::Other(format!("tcp receive: {e}")))?;
    let mut reply = vec![0u8; usize::from(u16::from_be_bytes(len))];
    stream
        .read_exact(&mut reply)
        .await
        .map_err(|e| Reason::Other(format!("tcp receive: {e}")))?;
    Ok(reply)
}

/// A standard query with recursion desired.
fn encode_query(id: u16, name: &str, qtype: u16) -> std::result::Result<Vec<u8>, String> {
    let mut out = Vec::with_capacity(64);
    out.extend_from_slice(&id.to_be_bytes());
    out.extend_from_slice(&0x0100u16.to_be_bytes()); // RD
    out.extend_from_slice(&1u16.to_be_bytes()); // QDCOUNT
    out.extend_from_slice(&[0, 0, 0, 0, 0, 0]); // AN, NS, AR
    for label in name.trim_end_matches('.').split('.') {
        if label.is_empty() || label.len() > 63 {
            return Err(format!("invalid DNS name '{name}'"));
        }
        out.push(label.len() as u8);
        out.extend_from_slice(label.as_bytes());
    }
    out.push(0);
    out.extend_from_slice(&qtype.to_be_bytes());
    out.extend_from_slice(&CLASS_IN.to_be_bytes());
    Ok(out)
}

struct Reply {
    truncated: bool,
    rcode: u8,
    /// Answer-section records of the requested type, rendered as strings.
    values: Vec<String>,
}

impl Reply {
    fn into_values(self) -> std::result::Result<Vec<String>, Reason> {
        match self.rcode {
            0 => Ok(self.values),
            RCODE_NXDOMAIN => Err(Reason::NxDomain),
            RCODE_SERVFAIL => Err(Reason::ServFail),
            code => Err(Reason::Other(format!("rcode {code}"))),
        }
    }
}

struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl Cursor<'_> {
    fn u8(&mut self) -> std::result::Result<u8, Reason> {
        let b = *self.buf.get(self.pos).ok_or_else(truncated_message)?;
        self.pos += 1;
        Ok(b)
    }

    fn u16(&mut self) -> std::result::Result<u16, Reason> {
        Ok(u16::from_be_bytes([self.u8()?, self.u8()?]))
    }

    fn skip(&mut self, n: usize) -> std::result::Result<(), Reason> {
        if self.pos + n > self.buf.len() {
            return Err(truncated_message());
        }
        self.pos += n;
        Ok(())
    }

    /// Reads a possibly compressed name, advancing past it in the stream.
    fn name(&mut self) -> std::result::Result<String, Reason> {
        let mut labels = Vec::new();
        let mut pos = self.pos;
        let mut jumped = false;
        let mut hops = 0;
        loop {
            let len = *self.buf.get(pos).ok_or_else(truncated_message)?;
            pos += 1;
            if len == 0 {
                break;
            }
            if len & 0xC0 == 0xC0 {
                let low = *self.buf.get(pos).ok_or_else(truncated_message)?;
                pos += 1;
                if !jumped {
                    self.pos = pos;
                    jumped = true;
                }
                pos = usize::from(u16::from_be_bytes([len & 0x3F, low]));
                hops += 1;
                if hops > 64 {
                    return Err(Reason::Other("compression loop".into()));
                }
                continue;
            }
            let len = usize::from(len);
            let label = self.buf.get(pos..pos + len).ok_or_else(truncated_message)?;
            labels.push(String::from_utf8_lossy(label).into_owned());
            pos += len;
        }
        if !jumped {
            self.pos = pos;
        }
        let mut name = labels.join(".");
        name.push('.');
        Ok(name)
    }
}

fn truncated_message() -> Reason {
    Reason::Other("malformed DNS reply".into())
}

/// Decodes the answer section of a reply to query `id`.
fn decode_reply(buf: &[u8], id: u16, qtype: u16) -> std::result::Result<Reply, Reason> {
    let mut c = Cursor { buf, pos: 0 };
    if c.u16()? != id {
        return Err(Reason::Other("DNS reply id mismatch".into()));
    }
    let flags = c.u16()?;
    let qdcount = c.u16()?;
    let ancount = c.u16()?;
    let _nscount = c.u16()?;
    let _arcount = c.u16()?;
    let reply_header = Reply {
        truncated: flags & 0x0200 != 0,
        rcode: (flags & 0x000F) as u8,
        values: Vec::new(),
    };
    if reply_header.rcode != 0 || reply_header.truncated {
        return Ok(reply_header);
    }
    for _ in 0..qdcount {
        c.name()?;
        c.skip(4)?;
    }
    let mut values = Vec::new();
    for _ in 0..ancount {
        c.name()?;
        let rtype = c.u16()?;
        let _class = c.u16()?;
        let _ttl = c.u16()?;
        let _ttl_low = c.u16()?;
        let rdlength = usize::from(c.u16()?);
        let start = c.pos;
        if rtype == qtype {
            match rtype {
                TYPE_A if rdlength == 4 => {
                    let b = &buf[start..start + 4];
                    values.push(Ipv4Addr::new(b[0], b[1], b[2], b[3]).to_string());
                }
                TYPE_AAAA if rdlength == 16 => {
                    let mut octets = [0u8; 16];
                    octets.copy_from_slice(buf.get(start..start + 16).ok_or_else(truncated_message)?);
                    values.push(Ipv6Addr::from(octets).to_string());
                }
                TYPE_SRV => {
                    let priority = c.u16()?;
                    let weight = c.u16()?;
                    let port = c.u16()?;
                    let target = c.name()?;
                    values.push(format!("{priority} {weight} {port} {target}"));
                }
                _ => {}
            }
        }
        c.pos = start;
        c.skip(rdlength)?;
    }
    Ok(Reply {
        values,
        ..reply_header
    })
}

#[cfg(all(test, feature = "rt-tokio"))]
pub(crate) mod stub {
    //! A UDP/TCP DNS server answering from in-memory records, for tests.

    use std::collections::HashMap;
    use std::net::{IpAddr, SocketAddr};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    use tokio::net::{TcpListener, UdpSocket};

    use crate::locks::lock;

    pub const A: u16 = 1;
    pub const AAAA: u16 = 28;
    pub const SRV: u16 = 33;
    pub const SERVFAIL: u8 = 2;
    pub const NXDOMAIN: u8 = 3;

    #[derive(Default)]
    struct State {
        answers: HashMap<(String, u16), Vec<Vec<u8>>>,
        rcodes: HashMap<(String, u16), u8>,
        /// Queries received, as `name/type`.
        queries: Vec<String>,
    }

    pub struct StubDnsServer {
        addr: SocketAddr,
        state: Arc<Mutex<State>>,
        silent: Arc<AtomicBool>,
        truncate: Arc<AtomicBool>,
        tcp_silent: Arc<AtomicBool>,
    }

    fn key(name: &str, qtype: u16) -> (String, u16) {
        let mut name = name.trim_end_matches('.').to_ascii_lowercase();
        name.push('.');
        (name, qtype)
    }

    fn encode_name(name: &str) -> Vec<u8> {
        let mut out = Vec::new();
        for label in name.trim_end_matches('.').split('.') {
            out.push(label.len() as u8);
            out.extend_from_slice(label.as_bytes());
        }
        out.push(0);
        out
    }

    fn record(name: &str, qtype: u16, rdata: &[u8]) -> Vec<u8> {
        let mut out = encode_name(name);
        out.extend_from_slice(&qtype.to_be_bytes());
        out.extend_from_slice(&1u16.to_be_bytes());
        out.extend_from_slice(&60u32.to_be_bytes());
        out.extend_from_slice(&(rdata.len() as u16).to_be_bytes());
        out.extend_from_slice(rdata);
        out
    }

    impl StubDnsServer {
        pub async fn start() -> Self {
            let udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let addr = udp.local_addr().unwrap();
            let tcp = TcpListener::bind(addr).await.unwrap();
            let state = Arc::new(Mutex::new(State::default()));
            let silent = Arc::new(AtomicBool::new(false));
            let truncate = Arc::new(AtomicBool::new(false));
            let tcp_silent = Arc::new(AtomicBool::new(false));

            let (st, si, tr) = (state.clone(), silent.clone(), truncate.clone());
            tokio::spawn(async move {
                let mut buf = vec![0u8; 4096];
                loop {
                    let Ok((n, from)) = udp.recv_from(&mut buf).await else {
                        break;
                    };
                    if si.load(Ordering::SeqCst) {
                        continue;
                    }
                    if let Some(reply) = Self::reply(&st, &buf[..n], tr.load(Ordering::SeqCst)) {
                        let _ = udp.send_to(&reply, from).await;
                    }
                }
            });
            let (st, ts) = (state.clone(), tcp_silent.clone());
            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                loop {
                    let Ok((mut sock, _)) = tcp.accept().await else {
                        break;
                    };
                    if ts.load(Ordering::SeqCst) {
                        // Hold the connection open without answering.
                        tokio::spawn(async move {
                            let mut b = [0u8; 1];
                            let _ = sock.read(&mut b).await;
                            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                        });
                        continue;
                    }
                    let st = st.clone();
                    tokio::spawn(async move {
                        let mut len = [0u8; 2];
                        if sock.read_exact(&mut len).await.is_err() {
                            return;
                        }
                        let mut req = vec![0u8; usize::from(u16::from_be_bytes(len))];
                        if sock.read_exact(&mut req).await.is_err() {
                            return;
                        }
                        if let Some(reply) = Self::reply(&st, &req, false) {
                            let mut framed = (reply.len() as u16).to_be_bytes().to_vec();
                            framed.extend_from_slice(&reply);
                            let _ = sock.write_all(&framed).await;
                        }
                    });
                }
            });
            StubDnsServer {
                addr,
                state,
                silent,
                truncate,
                tcp_silent,
            }
        }

        pub fn address(&self) -> String {
            self.addr.to_string()
        }

        pub fn set_silent(&self, silent: bool) {
            self.silent.store(silent, Ordering::SeqCst);
        }

        /// Answer every UDP query truncated and empty, so the client retries over TCP.
        pub fn truncate(&self) {
            self.truncate.store(true, Ordering::SeqCst);
        }

        /// Accept TCP connections and never answer.
        pub fn tcp_silent(&self) {
            self.tcp_silent.store(true, Ordering::SeqCst);
        }

        pub fn a(&self, name: &str, addresses: &[&str]) -> &Self {
            for address in addresses {
                let ip: IpAddr = address.parse().unwrap();
                let (t, rdata) = match ip {
                    IpAddr::V4(v4) => (A, v4.octets().to_vec()),
                    IpAddr::V6(v6) => (AAAA, v6.octets().to_vec()),
                };
                lock(&self.state)
                    .answers
                    .entry(key(name, t))
                    .or_default()
                    .push(record(name, t, &rdata));
            }
            self
        }

        pub fn srv(&self, name: &str, priority: u16, weight: u16, port: u16, target: &str) -> &Self {
            let mut rdata = Vec::new();
            rdata.extend_from_slice(&priority.to_be_bytes());
            rdata.extend_from_slice(&weight.to_be_bytes());
            rdata.extend_from_slice(&port.to_be_bytes());
            rdata.extend_from_slice(&encode_name(target));
            lock(&self.state)
                .answers
                .entry(key(name, SRV))
                .or_default()
                .push(record(name, SRV, &rdata));
            self
        }

        pub fn rcode(&self, name: &str, qtype: u16, rcode: u8) -> &Self {
            lock(&self.state).rcodes.insert(key(name, qtype), rcode);
            self
        }

        pub fn queries(&self) -> Vec<String> {
            lock(&self.state).queries.clone()
        }

        fn reply(state: &Mutex<State>, req: &[u8], truncate: bool) -> Option<Vec<u8>> {
            if req.len() < 12 {
                return None;
            }
            // Parse the question name.
            let mut pos = 12;
            let mut labels = Vec::new();
            loop {
                let len = usize::from(*req.get(pos)?);
                pos += 1;
                if len == 0 {
                    break;
                }
                labels.push(String::from_utf8_lossy(req.get(pos..pos + len)?).into_owned());
                pos += len;
            }
            let qtype = u16::from_be_bytes([*req.get(pos)?, *req.get(pos + 1)?]);
            let question = &req[12..pos + 4];
            let mut name = labels.join(".");
            name.push('.');
            let k = key(&name, qtype);

            let mut st = lock(state);
            st.queries.push(format!("{name}/{qtype}"));
            let answers = st.answers.get(&k).cloned().unwrap_or_default();
            let rcode = st.rcodes.get(&k).copied().unwrap_or_else(|| {
                // Unknown names get NXDOMAIN; a known name with no record of
                // this type gets an empty NOERROR answer.
                let known = st.answers.keys().any(|(n, _)| *n == k.0);
                if known {
                    0
                } else {
                    NXDOMAIN
                }
            });
            drop(st);

            let mut out = Vec::new();
            out.extend_from_slice(&req[..2]);
            let flags: u16 = 0x8000
                | 0x0400
                | (u16::from(req[2]) & 0x01) << 8
                | 0x0080
                | u16::from(rcode)
                | if truncate { 0x0200 } else { 0 };
            out.extend_from_slice(&flags.to_be_bytes());
            out.extend_from_slice(&1u16.to_be_bytes());
            let ancount = if rcode != 0 || truncate { 0 } else { answers.len() as u16 };
            out.extend_from_slice(&ancount.to_be_bytes());
            out.extend_from_slice(&[0, 0, 0, 0]);
            out.extend_from_slice(question);
            if ancount > 0 {
                for answer in answers {
                    out.extend_from_slice(&answer);
                }
            }
            Some(out)
        }
    }
}

#[cfg(all(test, feature = "rt-tokio"))]
mod tests {
    use super::stub::{StubDnsServer, SERVFAIL};
    use super::*;

    fn ctx() -> SeedCandidateContext {
        SeedCandidateContext::new(None, Vec::new(), 0)
    }

    fn builder(dns: &StubDnsServer, names: &[&str]) -> Builder {
        DnsSeedCandidateProvider::builder()
            .names(names.iter().copied())
            .servers([dns.address()])
            .timeout(2000)
    }

    fn h(name: &str, port: u16) -> Host {
        Host::new(name, port)
    }

    #[aerospike_macro::test]
    async fn a_and_aaaa_records_sorted_per_type_with_the_port() {
        let dns = StubDnsServer::start().await;
        dns.a("seeds.example.com", &["10.0.0.9", "10.0.0.2", "2001:db8::1"]);
        let provider = builder(&dns, &["seeds.example.com"]).port(3100).build().unwrap();
        let hosts = provider.refresh_seed_candidates(&ctx()).await.unwrap();
        assert_eq!(hosts, vec![h("10.0.0.2", 3100), h("10.0.0.9", 3100), h("2001:db8::1", 3100)]);
        assert_eq!(hosts, provider.refresh_seed_candidates(&ctx()).await.unwrap());
    }

    #[aerospike_macro::test]
    async fn defaults_are_port_3000_type_a_and_80ms() {
        let dns = StubDnsServer::start().await;
        dns.a("seeds.example.com", &["10.0.0.1"]);
        let provider = DnsSeedCandidateProvider::builder()
            .names(["seeds.example.com"])
            .servers([dns.address()])
            .build()
            .unwrap();
        assert_eq!(provider.refresh_seed_candidates(&ctx()).await.unwrap(), vec![h("10.0.0.1", 3000)]);
        assert_eq!(provider.record_type(), RecordType::A);
        assert_eq!(provider.port(), DEFAULT_PORT);
        assert_eq!(provider.timeout(), DEFAULT_TIMEOUT_MS);
        assert_eq!(provider.tls_name(), None);
        assert!(provider.needs_periodic_refresh());
        assert!(provider.supports_deadline());
        assert_eq!(provider.refresh_timeout(), Some(Duration::from_millis(80)));
        assert_eq!(provider.name(), "dns");
    }

    #[aerospike_macro::test]
    async fn multiple_names_in_order_and_deduplicated() {
        let dns = StubDnsServer::start().await;
        dns.a("b.example.com", &["10.0.0.2", "10.0.0.3"]);
        dns.a("a.example.com", &["10.0.0.1", "10.0.0.2"]);
        let provider = builder(&dns, &["b.example.com", "a.example.com."]).build().unwrap();
        let hosts = provider.refresh_seed_candidates(&ctx()).await.unwrap();
        assert_eq!(hosts, vec![h("10.0.0.2", 3000), h("10.0.0.3", 3000), h("10.0.0.1", 3000)]);
    }

    #[aerospike_macro::test]
    async fn srv_records_ordered_by_priority_then_weight_with_targets_resolved() {
        let dns = StubDnsServer::start().await;
        dns.srv("_aerospike._tcp.example.com", 20, 5, 3002, "node2.example.com")
            .srv("_aerospike._tcp.example.com", 10, 1, 3001, "node1.example.com")
            .srv("_aerospike._tcp.example.com", 10, 9, 3003, "node3.example.com")
            .a("node1.example.com", &["10.0.1.1"])
            .a("node2.example.com", &["10.0.1.2"])
            .a("node3.example.com", &["2001:db8::3"]);
        let provider = builder(&dns, &["_aerospike._tcp.example.com"])
            .record_type(RecordType::Srv)
            .port(4000)
            .build()
            .unwrap();
        let hosts = provider.refresh_seed_candidates(&ctx()).await.unwrap();
        assert_eq!(hosts, vec![h("2001:db8::3", 3003), h("10.0.1.1", 3001), h("10.0.1.2", 3002)]);
        let queries = dns.queries();
        assert!(queries.contains(&"node1.example.com./1".to_string()), "{queries:?}");
        // node3 has no A record: the AAAA follow-up query resolved it.
        assert!(queries.contains(&"node3.example.com./28".to_string()), "{queries:?}");
    }

    #[aerospike_macro::test]
    async fn srv_target_that_does_not_resolve_is_dropped_and_root_target_ignored() {
        let dns = StubDnsServer::start().await;
        dns.srv("_aerospike._tcp.example.com", 10, 1, 3001, "node1.example.com")
            .srv("_aerospike._tcp.example.com", 10, 1, 3002, "gone.example.com")
            .srv("_aerospike._tcp.example.com", 10, 1, 3003, ".")
            .a("node1.example.com", &["10.0.1.1"]);
        let provider = builder(&dns, &["_aerospike._tcp.example.com"])
            .record_type(RecordType::Srv)
            .build()
            .unwrap();
        let hosts = provider.refresh_seed_candidates(&ctx()).await.unwrap();
        assert_eq!(hosts, vec![h("10.0.1.1", 3001)]);
    }

    #[aerospike_macro::test]
    async fn partial_failure_returns_the_resolved_names() {
        let dns = StubDnsServer::start().await;
        dns.a("good.example.com", &["10.0.0.1"])
            .rcode("bad.example.com", stub::A, SERVFAIL)
            .rcode("bad.example.com", stub::AAAA, SERVFAIL);
        let provider = builder(&dns, &["missing.example.com", "good.example.com", "bad.example.com"])
            .build()
            .unwrap();
        let hosts = provider.refresh_seed_candidates(&ctx()).await.unwrap();
        assert_eq!(hosts, vec![h("10.0.0.1", 3000)]);
    }

    #[aerospike_macro::test]
    async fn every_name_failing_fails_naming_each() {
        let dns = StubDnsServer::start().await;
        dns.rcode("bad.example.com", stub::A, SERVFAIL)
            .rcode("bad.example.com", stub::AAAA, SERVFAIL)
            .srv("other.example.com", 1, 1, 1, "x.example.com");
        let provider = builder(&dns, &["missing.example.com", "bad.example.com", "other.example.com"])
            .build()
            .unwrap();
        let err = provider.refresh_seed_candidates(&ctx()).await.unwrap_err();
        assert_eq!(
            err.base_message(),
            "DNS seed discovery failed for every name: missing.example.com: NXDOMAIN; \
             bad.example.com: SERVFAIL; other.example.com: empty answer"
        );
    }

    #[aerospike_macro::test]
    async fn unanswered_query_fails_within_the_timeout() {
        let dns = StubDnsServer::start().await;
        dns.set_silent(true);
        let provider = builder(&dns, &["seeds.example.com"]).timeout(100).build().unwrap();
        let started = Instant::now();
        let err = provider.refresh_seed_candidates(&ctx()).await.unwrap_err();
        assert!(err.to_string().contains("seeds.example.com: timeout"), "{err}");
        assert!(started.elapsed() < Duration::from_millis(400), "{:?}", started.elapsed());
    }

    #[aerospike_macro::test]
    async fn truncated_udp_answer_is_retried_over_tcp() {
        let dns = StubDnsServer::start().await;
        dns.a("seeds.example.com", &["10.0.0.1"]);
        dns.truncate();
        let provider = builder(&dns, &["seeds.example.com"]).build().unwrap();
        let hosts = provider.refresh_seed_candidates(&ctx()).await.unwrap();
        assert_eq!(hosts, vec![h("10.0.0.1", 3000)]);

        // When the TCP retry never answers, the refresh still returns in time.
        dns.tcp_silent();
        let provider = builder(&dns, &["seeds.example.com"]).timeout(100).build().unwrap();
        let started = Instant::now();
        assert!(provider.refresh_seed_candidates(&ctx()).await.is_err());
        assert!(started.elapsed() < Duration::from_millis(400), "{:?}", started.elapsed());
    }

    #[aerospike_macro::test]
    async fn tls_name_is_carried_on_every_host() {
        let dns = StubDnsServer::start().await;
        dns.a("seeds.example.com", &["10.0.0.1", "10.0.0.2"]);
        let provider = builder(&dns, &["seeds.example.com"]).tls_name("tls-seed").build().unwrap();
        let hosts = provider.refresh_seed_candidates(&ctx()).await.unwrap();
        assert_eq!(hosts, vec![Host::new_tls("10.0.0.1", "tls-seed", 3000), Host::new_tls("10.0.0.2", "tls-seed", 3000)]);
    }

    #[test]
    fn invalid_config_is_rejected_at_construction() {
        let b = || DnsSeedCandidateProvider::builder().servers(["127.0.0.1:1"]);
        let cases: Vec<(Builder, &str)> = vec![
            (b(), "at least one name"),
            (b().names([""]), "Invalid DNS seed provider name: ''"),
            (b().names(["a b"]), "Invalid DNS seed provider name"),
            (b().names(["a.example.com", "A.example.com."]), "duplicate name"),
            (b().names(["a"]).port(0), "port: 0"),
            (b().names(["a"]).tls_name(""), "tls_name: empty"),
            (b().names(["a"]).timeout(0), "timeout: 0"),
            (b().names(["a"]).servers(["dns.example.com"]), "expected an IP address"),
            (b().names(["a"]).servers(["10.0.0.1:x"]), "invalid port"),
            (b().names(["a"]).servers(["[::1"]), "Invalid DNS seed provider server"),
        ];
        for (builder, expected) in cases {
            let err = builder.build().unwrap_err();
            assert!(err.to_string().contains(expected), "{err} should contain {expected}");
        }
        // Valid server spellings.
        let p = b().names(["a"]).servers(["10.0.0.1", "10.0.0.2:5353", "::1", "[::1]:5353"]).build().unwrap();
        assert_eq!(
            p.resolvers,
            vec![
                "10.0.0.1:53".parse().unwrap(),
                "10.0.0.2:5353".parse().unwrap(),
                "[::1]:53".parse().unwrap(),
                "[::1]:5353".parse().unwrap()
            ]
        );
        assert_eq!(p.servers().len(), 4);
    }

    #[test]
    fn srv_order_is_priority_then_weight_descending() {
        let mut records = [
            Srv::parse("20 5 3002 node2.example.com.").unwrap(),
            Srv::parse("10 1 3001 node1.example.com.").unwrap(),
            Srv::parse("10 9 3003 NODE3.example.com.").unwrap(),
        ];
        records.sort();
        let targets: Vec<&str> = records.iter().map(|s| s.target.as_str()).collect();
        assert_eq!(targets, vec!["node3.example.com.", "node1.example.com.", "node2.example.com."]);
        assert!(Srv::parse("1 2 0 x").is_none());
        assert!(Srv::parse("1 2 3").is_none());
    }

    #[test]
    fn wire_round_trip_decodes_compressed_names() {
        let request = encode_query(7, "seeds.example.com", TYPE_A).unwrap();
        assert_eq!(&request[..2], &[0, 7]);
        // A reply with a compressed owner name pointing at the question.
        let mut reply = Vec::new();
        reply.extend_from_slice(&[0, 7, 0x81, 0x80, 0, 1, 0, 1, 0, 0, 0, 0]);
        reply.extend_from_slice(&request[12..]);
        reply.extend_from_slice(&[0xC0, 0x0C, 0, 1, 0, 1, 0, 0, 0, 60, 0, 4, 10, 0, 0, 1]);
        let decoded = decode_reply(&reply, 7, TYPE_A).unwrap();
        assert_eq!(decoded.into_values().unwrap(), vec!["10.0.0.1"]);
        assert!(decode_reply(&reply, 8, TYPE_A).is_err());
    }
}
