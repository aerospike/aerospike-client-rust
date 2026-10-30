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

//! A minimal HTTP/1.1 client with every connect, handshake and read bounded
//! by one deadline. One connection per request, closed when the response is
//! read: nothing is pooled and no task is started.

#![allow(clippy::redundant_pub_crate)]

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use aerospike_rt::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use aerospike_rt::net::TcpStream;
use aerospike_rt::time::Instant;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;

pub(crate) const MAX_BODY: usize = 16 * 1024 * 1024;

/// A parsed `http[s]://host[:port][/path][?query]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Url {
    pub tls: bool,
    pub host: String,
    pub port: Option<u16>,
    pub path: String,
    pub query: Option<String>,
}

impl Url {
    /// Parses `text`; the scheme must be `http` or `https` and a host must be
    /// present.
    pub(crate) fn parse(text: &str) -> Result<Url, String> {
        let (scheme, rest) = text
            .split_once("://")
            .ok_or_else(|| format!("'{text}' is not a URL"))?;
        let tls = match scheme.to_ascii_lowercase().as_str() {
            "https" => true,
            "http" => false,
            _ => return Err(format!("'{text}' is not an http or https URL")),
        };
        let (rest, fragment) = match rest.split_once('#') {
            Some((r, f)) => (r, Some(f)),
            None => (rest, None),
        };
        if fragment.is_some() {
            return Err(format!("'{text}' has a fragment"));
        }
        let (rest, query) = match rest.split_once('?') {
            Some((r, q)) => (r, Some(q.to_string())),
            None => (rest, None),
        };
        let (authority, path) = match rest.find('/') {
            Some(i) => (&rest[..i], rest[i..].to_string()),
            None => (rest, "/".to_string()),
        };
        let (host, port) = if let Some(after) = authority.strip_prefix('[') {
            let end = after.find(']').ok_or_else(|| format!("'{text}' has an invalid host"))?;
            let port = after[end + 1..].strip_prefix(':');
            (after[..end].to_string(), port)
        } else {
            match authority.rsplit_once(':') {
                Some((h, p)) => (h.to_string(), Some(p)),
                None => (authority.to_string(), None),
            }
        };
        if host.is_empty() {
            return Err(format!("'{text}' has no host"));
        }
        let port = match port {
            None | Some("") => None,
            Some(p) => Some(
                p.parse::<u16>()
                    .map_err(|_| format!("'{text}' has an invalid port"))?,
            ),
        };
        Ok(Url {
            tls,
            host,
            port,
            path: if path.is_empty() { "/".into() } else { path },
            query,
        })
    }

    /// The `Host` header value.
    pub(crate) fn host_header(&self) -> String {
        match self.port {
            Some(port) => format!("{}:{port}", self.host),
            None => self.host.clone(),
        }
    }

    fn connect_port(&self) -> u16 {
        self.port.unwrap_or(if self.tls { 443 } else { 80 })
    }
}

#[derive(Debug)]
pub(crate) struct Response {
    pub status: u16,
    /// Header names lower-cased.
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
}

impl Response {
    pub(crate) fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

#[derive(Debug)]
pub(crate) enum HttpError {
    /// The deadline passed.
    Timeout,
    /// Any other I/O or protocol failure.
    Io(String),
}

impl fmt::Display for HttpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HttpError::Timeout => f.write_str("timed out"),
            HttpError::Io(s) => f.write_str(s),
        }
    }
}

impl From<std::io::Error> for HttpError {
    fn from(e: std::io::Error) -> Self {
        HttpError::Io(e.to_string())
    }
}

trait Stream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Stream for T {}

static TLS_CONFIG: LazyLock<Arc<ClientConfig>> = LazyLock::new(|| {
    let roots = RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.into(),
    };
    Arc::new(
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    )
});

/// The time left before `deadline`.
pub(crate) fn remaining(deadline: Instant) -> Result<Duration, HttpError> {
    let left = deadline.saturating_duration_since(Instant::now());
    if left.is_zero() {
        return Err(HttpError::Timeout);
    }
    Ok(left)
}

async fn bounded<T, F>(deadline: Instant, future: F) -> Result<T, HttpError>
where
    F: std::future::Future<Output = Result<T, HttpError>>,
{
    aerospike_rt::timeout(remaining(deadline)?, future)
        .await
        .map_err(|_| HttpError::Timeout)?
}

/// Sends one request and reads the response. Header names must be lower
/// case; `host` is added when absent.
pub(crate) async fn request(
    method: &str,
    url: &Url,
    headers: &BTreeMap<String, String>,
    body: Option<&[u8]>,
    deadline: Instant,
) -> Result<Response, HttpError> {
    use std::fmt::Write as _;

    let address = format!("{}:{}", url.host, url.connect_port());
    let tcp = bounded(deadline, async {
        TcpStream::connect(address).await.map_err(HttpError::from)
    })
    .await?;
    let mut stream: Box<dyn Stream> = if url.tls {
        let name = ServerName::try_from(url.host.clone())
            .map_err(|e| HttpError::Io(format!("invalid TLS server name: {e}")))?;
        let connector = TlsConnector::from(TLS_CONFIG.clone());
        let tls = bounded(deadline, async {
            connector.connect(name, tcp).await.map_err(HttpError::from)
        })
        .await?;
        Box::new(tls)
    } else {
        Box::new(tcp)
    };

    let mut target = url.path.clone();
    if let Some(query) = &url.query {
        target.push('?');
        target.push_str(query);
    }
    let mut head = format!("{method} {target} HTTP/1.1\r\n");
    if !headers.contains_key("host") {
        let _ = write!(head, "host: {}\r\n", url.host_header());
    }
    for (name, value) in headers {
        let _ = write!(head, "{name}: {value}\r\n");
    }
    if let Some(body) = body {
        let _ = write!(head, "content-length: {}\r\n", body.len());
    }
    head.push_str("connection: close\r\n\r\n");

    bounded(deadline, async {
        stream.write_all(head.as_bytes()).await?;
        if let Some(body) = body {
            stream.write_all(body).await?;
        }
        stream.flush().await?;
        Ok(())
    })
    .await?;

    let mut reader = Reader {
        stream,
        buffer: vec![0u8; 8192],
        offset: 0,
        limit: 0,
        total: 0,
    };
    bounded(deadline, read_response(&mut reader)).await
}

async fn read_response(reader: &mut Reader) -> Result<Response, HttpError> {
    let line = reader.line().await?;
    let mut fields = line.splitn(3, ' ');
    let version = fields.next().unwrap_or_default();
    let status = fields
        .next()
        .and_then(|s| s.parse::<u16>().ok())
        .filter(|_| version.starts_with("HTTP/"))
        .ok_or_else(|| HttpError::Io("invalid HTTP status line".into()))?;

    let mut headers = HashMap::new();
    loop {
        let line = reader.line().await?;
        if line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
        }
    }

    let mut body = Vec::new();
    let chunked = headers
        .get("transfer-encoding")
        .is_some_and(|v| v.eq_ignore_ascii_case("chunked"));
    if chunked {
        loop {
            let size_line = reader.line().await?;
            let size = size_line.split(';').next().unwrap_or_default().trim();
            let n = usize::from_str_radix(size, 16)
                .map_err(|_| HttpError::Io("invalid HTTP chunk size".into()))?;
            if n == 0 {
                while !reader.line().await?.is_empty() {}
                break;
            }
            reader.copy(&mut body, Some(n)).await?;
            reader.line().await?;
        }
    } else if let Some(length) = headers.get("content-length") {
        let n = length
            .parse::<usize>()
            .map_err(|_| HttpError::Io("invalid HTTP content-length".into()))?;
        reader.copy(&mut body, Some(n)).await?;
    } else if status >= 200 && status != 204 && status != 304 {
        reader.copy(&mut body, None).await?;
    }
    Ok(Response {
        status,
        headers,
        body,
    })
}

struct Reader {
    stream: Box<dyn Stream>,
    buffer: Vec<u8>,
    offset: usize,
    limit: usize,
    total: usize,
}

impl Reader {
    /// Refills the buffer; `false` at end of stream.
    async fn fill(&mut self) -> Result<bool, HttpError> {
        let n = self.stream.read(&mut self.buffer).await?;
        if n == 0 {
            return Ok(false);
        }
        self.total += n;
        if self.total > MAX_BODY {
            return Err(HttpError::Io(format!("HTTP response exceeds {MAX_BODY} bytes")));
        }
        self.offset = 0;
        self.limit = n;
        Ok(true)
    }

    async fn line(&mut self) -> Result<String, HttpError> {
        let mut out = Vec::with_capacity(128);
        loop {
            if self.offset == self.limit && !self.fill().await? {
                return Err(HttpError::Io("HTTP connection closed".into()));
            }
            let b = self.buffer[self.offset];
            self.offset += 1;
            if b == b'\n' {
                break;
            }
            if b != b'\r' {
                out.push(b);
            }
        }
        Ok(String::from_utf8_lossy(&out).into_owned())
    }

    /// Copies `length` bytes, or everything to the end of the stream.
    async fn copy(&mut self, out: &mut Vec<u8>, length: Option<usize>) -> Result<(), HttpError> {
        let mut left = length;
        while left != Some(0) {
            if self.offset == self.limit && !self.fill().await? {
                if left.is_none() {
                    return Ok(());
                }
                return Err(HttpError::Io("HTTP connection closed".into()));
            }
            let mut n = self.limit - self.offset;
            if let Some(l) = left {
                n = n.min(l);
            }
            out.extend_from_slice(&self.buffer[self.offset..self.offset + n]);
            self.offset += n;
            if let Some(l) = left {
                left = Some(l - n);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod stub {
    //! A plain-HTTP server for the AWS tests: one handler closure decides the
    //! response from the request line, headers and body.

    use std::fmt::Write as _;
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use crate::locks::lock;

    #[derive(Debug, Clone)]
    pub(crate) struct Request {
        pub method: String,
        pub target: String,
        pub headers: Vec<(String, String)>,
        pub body: String,
    }

    impl Request {
        pub(crate) fn header(&self, name: &str) -> Option<&str> {
            self.headers
                .iter()
                .find(|(n, _)| n.eq_ignore_ascii_case(name))
                .map(|(_, v)| v.as_str())
        }
    }

    pub(crate) type Handler = Arc<dyn Fn(&Request) -> (u16, Vec<(String, String)>, Vec<u8>) + Send + Sync>;

    pub(crate) struct StubHttpServer {
        addr: SocketAddr,
        requests: Arc<Mutex<Vec<Request>>>,
        silent: Arc<std::sync::atomic::AtomicBool>,
    }

    impl StubHttpServer {
        pub(crate) async fn start(handler: Handler) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let requests = Arc::new(Mutex::new(Vec::new()));
            let silent = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let (reqs, si) = (requests.clone(), silent.clone());
            tokio::spawn(async move {
                loop {
                    let Ok((mut sock, _)) = listener.accept().await else {
                        break;
                    };
                    let handler = handler.clone();
                    let reqs = reqs.clone();
                    let silent = si.load(std::sync::atomic::Ordering::SeqCst);
                    tokio::spawn(async move {
                        let mut buf = Vec::new();
                        let mut tmp = [0u8; 4096];
                        let (head_end, body_len);
                        loop {
                            let n = match sock.read(&mut tmp).await {
                                Ok(0) | Err(_) => return,
                                Ok(n) => n,
                            };
                            buf.extend_from_slice(&tmp[..n]);
                            if let Some(i) = find(&buf, b"\r\n\r\n") {
                                let head = String::from_utf8_lossy(&buf[..i]).into_owned();
                                let len = head
                                    .lines()
                                    .find_map(|l| {
                                        let (n, v) = l.split_once(':')?;
                                        n.trim().eq_ignore_ascii_case("content-length").then(|| v.trim().parse::<usize>().ok())?
                                    })
                                    .unwrap_or(0);
                                head_end = i + 4;
                                body_len = len;
                                break;
                            }
                        }
                        while buf.len() < head_end + body_len {
                            let n = match sock.read(&mut tmp).await {
                                Ok(0) | Err(_) => return,
                                Ok(n) => n,
                            };
                            buf.extend_from_slice(&tmp[..n]);
                        }
                        let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
                        let mut lines = head.lines();
                        let request_line = lines.next().unwrap_or_default();
                        let mut parts = request_line.split(' ');
                        let request = Request {
                            method: parts.next().unwrap_or_default().to_string(),
                            target: parts.next().unwrap_or_default().to_string(),
                            headers: lines
                                .filter_map(|l| l.split_once(':'))
                                .map(|(n, v)| (n.trim().to_ascii_lowercase(), v.trim().to_string()))
                                .collect(),
                            body: String::from_utf8_lossy(&buf[head_end..head_end + body_len]).into_owned(),
                        };
                        lock(&reqs).push(request.clone());
                        if silent {
                            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                            return;
                        }
                        let (status, headers, body) = handler(&request);
                        let mut out = format!("HTTP/1.1 {status} X\r\n");
                        for (n, v) in headers {
                            let _ = write!(out, "{n}: {v}\r\n");
                        }
                        let _ = write!(out, "content-length: {}\r\nconnection: close\r\n\r\n", body.len());
                        let mut bytes = out.into_bytes();
                        bytes.extend_from_slice(&body);
                        let _ = sock.write_all(&bytes).await;
                        let _ = sock.shutdown().await;
                    });
                }
            });
            StubHttpServer {
                addr,
                requests,
                silent,
            }
        }

        pub(crate) fn url(&self) -> String {
            format!("http://{}", self.addr)
        }

        pub(crate) fn requests(&self) -> Vec<Request> {
            lock(&self.requests).clone()
        }

        /// Accept requests and never answer them.
        pub(crate) fn set_silent(&self, silent: bool) {
            self.silent.store(silent, std::sync::atomic::Ordering::SeqCst);
        }
    }

    fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        haystack.windows(needle.len()).position(|w| w == needle)
    }

    pub(crate) fn ok(body: &str) -> (u16, Vec<(String, String)>, Vec<u8>) {
        (200, vec![], body.as_bytes().to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::stub::{ok, StubHttpServer};
    use super::*;

    #[test]
    fn url_parsing() {
        let u = Url::parse("https://ec2.us-east-1.amazonaws.com").unwrap();
        assert!(u.tls);
        assert_eq!(u.host, "ec2.us-east-1.amazonaws.com");
        assert_eq!(u.port, None);
        assert_eq!(u.path, "/");
        assert_eq!(u.host_header(), "ec2.us-east-1.amazonaws.com");
        assert_eq!(u.connect_port(), 443);

        let u = Url::parse("http://127.0.0.1:8080/latest/api/token?x=1").unwrap();
        assert!(!u.tls);
        assert_eq!(u.port, Some(8080));
        assert_eq!(u.path, "/latest/api/token");
        assert_eq!(u.query.as_deref(), Some("x=1"));
        assert_eq!(u.host_header(), "127.0.0.1:8080");

        let u = Url::parse("http://[::1]:99/p").unwrap();
        assert_eq!(u.host, "::1");
        assert_eq!(u.port, Some(99));

        assert!(Url::parse("ftp://x").is_err());
        assert!(Url::parse("http:///path").is_err());
        assert!(Url::parse("http://host:x").is_err());
        assert!(Url::parse("http://host/#frag").is_err());
        assert!(Url::parse("no-scheme").is_err());
    }

    #[aerospike_macro::test]
    async fn request_round_trip_and_chunked_bodies() {
        let server = StubHttpServer::start(Arc::new(|req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.target, "/p?q=1");
            assert_eq!(req.header("x-test"), Some("v"));
            assert_eq!(req.body, "hello");
            ok("world")
        }))
        .await;
        let url = Url::parse(&format!("{}/p?q=1", server.url())).unwrap();
        let mut headers = BTreeMap::new();
        headers.insert("x-test".to_string(), "v".to_string());
        let deadline = Instant::now() + Duration::from_secs(5);
        let response = request("POST", &url, &headers, Some(b"hello"), deadline).await.unwrap();
        assert_eq!(response.status, 200);
        assert_eq!(response.text(), "world");
        assert_eq!(server.requests()[0].header("host").unwrap(), url.host_header());

        // A silent server fails within the deadline.
        server.set_silent(true);
        let started = Instant::now();
        let err = request("GET", &url, &BTreeMap::new(), None, Instant::now() + Duration::from_millis(100))
            .await
            .unwrap_err();
        assert!(matches!(err, HttpError::Timeout), "{err}");
        assert!(started.elapsed() < Duration::from_millis(500));
    }

    #[aerospike_macro::test]
    async fn chunked_response_is_reassembled() {
        use tokio::io::AsyncWriteExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = tokio::io::AsyncReadExt::read(&mut sock, &mut buf).await;
            sock.write_all(b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n3\r\nabc\r\n2;ext\r\nde\r\n0\r\n\r\n")
                .await
                .unwrap();
        });
        let url = Url::parse(&format!("http://{addr}/")).unwrap();
        let response = request("GET", &url, &BTreeMap::new(), None, Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(response.text(), "abcde");
    }
}
