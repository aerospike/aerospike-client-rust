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

//! Config Pigeon: a dynamic-configuration provider that fetches the client's
//! configuration from a REST service instead of a file.
//!
//! The built-in `YamlFileProvider` watches a file on disk. A [`ConfigProvider`]
//! can be anything that returns a [`ConfigDocument`], though, and this one is a
//! carrier pigeon: on every poll it flies to an HTTP endpoint, and it only
//! brings the document home when the service's answer changed since the last
//! trip, so an unchanged configuration costs the client nothing.
//!
//! The service speaks JSON. The document has the same shape as the YAML file
//! (`version`, `static`, `dynamic`), because `ConfigDocument` is a plain serde
//! type; any format serde reads will do.
//!
//! To keep the example self-contained it starts a tiny HTTP service of its own
//! on a random local port, serves a document that turns client metrics on,
//! flips it to off while the client is running, and shows the client follow
//! both changes. Point `PigeonProvider` at a real service in your own code.
//!
//! Requires the `dynamic-config` feature (on by default):
//!
//! ```bash
//! cargo run --example config_pigeon
//! ```

use std::env;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use aerospike::config::{register_provider, ConfigDocument, ConfigProvider};
use aerospike::{Client, ClientPolicy, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Fetches the configuration document from `url` (`pigeon://host:port/path`).
///
/// `load` is called by the client's watcher every `config_interval`. It returns
/// `Ok(None)` when the service answered with the same body as last time, so the
/// client re-applies nothing; a changed body is parsed and handed over.
/// Failures are logged and reported as `Ok(None)` too: a configuration service
/// that is down must never take the database client down with it.
#[derive(Debug)]
pub struct PigeonProvider {
    host: String,
    path: String,
    last_body: Mutex<Option<String>>,
}

impl PigeonProvider {
    /// `url` is `pigeon://host:port/path` (the registered scheme) or
    /// `http://host:port/path`; only plain HTTP is spoken here.
    pub fn new(url: &str) -> Self {
        let rest = url
            .strip_prefix("pigeon://")
            .or_else(|| url.strip_prefix("http://"))
            .unwrap_or(url);
        let (host, path) = rest.split_once('/').unwrap_or((rest, ""));
        PigeonProvider {
            host: host.to_string(),
            path: format!("/{path}"),
            last_body: Mutex::new(None),
        }
    }

    /// One HTTP/1.1 GET over a plain socket: enough for a JSON document, and
    /// no HTTP client dependency. A real provider would use its HTTP client of
    /// choice and send `If-None-Match` with the last ETag.
    async fn fetch(&self) -> std::io::Result<String> {
        let mut stream = TcpStream::connect(&self.host).await?;
        let request = format!(
            "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
            self.path, self.host
        );
        stream.write_all(request.as_bytes()).await?;
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await?;
        let response = String::from_utf8_lossy(&response);
        let (head, body) = response
            .split_once("\r\n\r\n")
            .ok_or_else(|| std::io::Error::other("malformed HTTP response"))?;
        let status = head.lines().next().unwrap_or_default();
        if !status.contains(" 200 ") {
            return Err(std::io::Error::other(format!("config service answered: {status}")));
        }
        Ok(body.to_string())
    }
}

#[async_trait::async_trait]
impl ConfigProvider for PigeonProvider {
    async fn load(&self) -> Result<Option<ConfigDocument>> {
        let body = match self.fetch().await {
            Ok(body) => body,
            Err(err) => {
                log::warn!("config pigeon: {}{} unreachable: {err}", self.host, self.path);
                return Ok(None);
            }
        };
        {
            let mut last = self.last_body.lock().expect("config pigeon lock");
            if last.as_deref() == Some(body.as_str()) {
                return Ok(None);
            }
            *last = Some(body.clone());
        }
        match serde_json::from_str::<ConfigDocument>(&body) {
            Ok(doc) if doc.version.is_some() => Ok(Some(doc)),
            Ok(_) => {
                log::warn!("config pigeon: document without a `version`; ignoring it");
                Ok(None)
            }
            Err(err) => {
                log::warn!("config pigeon: cannot parse the document: {err}");
                Ok(None)
            }
        }
    }
}

/// Make `AEROSPIKE_CLIENT_CONFIG_URL=pigeon://host:port/path` build a
/// `PigeonProvider`, the way `file://` builds the YAML provider. The scheme
/// carries the provider's name: a generic `http://` would collide with any
/// other provider that also talks to a web service. The factory receives the
/// DSN without its scheme.
pub fn register_pigeon() {
    register_provider(
        "pigeon://",
        Box::new(|rest| Arc::new(PigeonProvider::new(&format!("pigeon://{rest}")))),
    );
}

/// The configuration the service serves: client metrics on or off, reloaded
/// every second (`config_interval` is in seconds and clamps to one).
fn document(metrics_enabled: bool) -> String {
    format!(
        r#"{{
  "version": "1.0.0",
  "static": {{ "client": {{ "config_interval": 1 }} }},
  "dynamic": {{ "metrics": {{ "enabled": {metrics_enabled} }} }}
}}"#
    )
}

/// A stand-in for the real REST service: one JSON document at `/config`,
/// swapped by the example while the client runs.
async fn serve(listener: TcpListener, body: Arc<Mutex<String>>) {
    loop {
        let Ok((mut socket, _)) = listener.accept().await else {
            return;
        };
        let body = body.clone();
        tokio::spawn(async move {
            let mut request = [0u8; 1024];
            let _ = socket.read(&mut request).await;
            let payload = body.lock().expect("service lock").clone();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{payload}",
                payload.len()
            );
            let _ = socket.write_all(response.as_bytes()).await;
        });
    }
}

/// Polls `client.metrics_enabled()` until it reads `expected`, which is how a
/// reload becomes visible from outside: the served document controls it.
async fn wait_for_metrics(client: &Client, expected: bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if client.metrics_enabled() == expected {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    false
}

#[tokio::main]
async fn main() {
    run().await
}

/// Example body. Standalone via `cargo run --example`, and also driven by
/// the integration test suite (`tests/src/examples.rs`).
pub async fn run() {
    // ---- The configuration service ----
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("pigeon://{}/config", listener.local_addr().expect("addr"));
    let served = Arc::new(Mutex::new(document(true)));
    tokio::spawn(serve(listener, served.clone()));
    println!("config service at {url}");

    // ---- The client, configured by the pigeon ----
    register_pigeon(); // AEROSPIKE_CLIENT_CONFIG_URL=pigeon://... now works too
    let provider = Arc::new(PigeonProvider::new(&url));

    let mut cpolicy = ClientPolicy::default();
    cpolicy.use_services_alternate = env::var("AEROSPIKE_USE_SERVICES_ALTERNATE")
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false);
    let hosts = env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| String::from("127.0.0.1:3000"));
    let client = Client::new_with_config(&cpolicy, &hosts, provider)
        .await
        .expect("Failed to connect to cluster");

    // The document applied at construction turned metrics on.
    assert!(
        wait_for_metrics(&client, true).await,
        "the served configuration did not reach the client"
    );
    println!("metrics enabled by the served configuration: {}", client.metrics_enabled());

    // ---- A change on the service side reaches the running client ----
    *served.lock().expect("service lock") = document(false);
    assert!(
        wait_for_metrics(&client, false).await,
        "the changed configuration did not reach the client"
    );
    println!("metrics after the service changed its answer: {}", client.metrics_enabled());

    client.close().await.expect("close");
}
