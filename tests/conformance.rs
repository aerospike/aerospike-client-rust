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

//! The shared L1 conformance fixtures of `aerospike-client-conformance`, run
//! against this client: address translation (`translate`), the seed merge
//! policy (`merge`), the `dns` and `aws-tags` seed providers against the
//! fixture server the repository ships, and server certificate name matching
//! (`tls`). Every row is compared with the result `tests/conformance/
//! rust-declared.yaml` declares for it; a row with no declaration is declared
//! `pass` (`blocked` when the fixture blocks it). The run writes
//! `conformance-report.json` in the shape of the repository's report schema.
//!
//! `CONFORMANCE_DIR` points at a checkout of the fixture set at the version
//! `FIXTURE_SET_VERSION` names; without it the test skips. `CONFORMANCE_REPORT`
//! overrides the report path, `target/conformance-report.json` by default.
//!
//! ```sh
//! CONFORMANCE_DIR=../aerospike-client-conformance \
//!   cargo test --test conformance --no-default-features \
//!   --features async,serialization,rt-tokio,tls,discovery-aws -- --nocapture
//! ```

use std::collections::{BTreeMap, HashMap};
use std::env;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{IpAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rustls::client::verify_server_name;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName};
use rustls::server::ParsedCertificate;
use serde_norway::Value;

use aerospike::discovery::aws::{AddressField, AwsEc2TagSeedCandidateProvider, Credentials};
use aerospike::discovery::conformance::{translate_with_policy, Refresher};
use aerospike::discovery::dns::{DnsSeedCandidateProvider, RecordType};
use aerospike::discovery::{
    Endpoint, PassThroughAddressTranslator, PortMapAddressTranslator, SeedCandidateContext,
    SeedCandidateProvider, SeedMergePolicy, SourceList, StaticMapAddressTranslator,
    TranslationMissMode,
};
use aerospike::{Client, ClientPolicy, ErrorKind, Host};

const RUNTIME: &str = "rust";
const FIXTURE_SET_VERSION: &str = "1.1.0";
const FIXTURE_ROOTS: [&str; 2] = ["discovery", "tls"];
const FIXTURE_KINDS: [&str; 5] = ["translate", "merge", "dns", "aws-tags", "tls"];
const DECLARED: &str = "tests/conformance/rust-declared.yaml";

const PASS: &str = "pass";
const FAIL: &str = "fail";
const UNSUPPORTED: &str = "unsupported";
const BLOCKED: &str = "blocked";
const RESULTS: [&str; 4] = [PASS, FAIL, UNSUPPORTED, BLOCKED];

/// Timeout given to a provider whose row sets none: generous for a loopback
/// server.
const LOOPBACK_TIMEOUT_MS: u32 = 2000;

/// Credentials the fixture server accepts: it checks the signature's shape,
/// not its value, so any uppercase alphanumeric key id works.
const FIXTURE_ACCESS_KEY_ID: &str = "AKIAFIXTURE0000000000";
const FIXTURE_SECRET: &str = "fixture-secret-access-key";

#[test]
fn conformance() {
    let Some(dir) = env::var_os("CONFORMANCE_DIR") else {
        println!(
            "CONFORMANCE-SKIP: CONFORMANCE_DIR is not set. Check out \
             aerospike-client-conformance v{FIXTURE_SET_VERSION} and point CONFORMANCE_DIR at it"
        );
        return;
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    runtime.block_on(run(PathBuf::from(dir)));
}

async fn run(dir: PathBuf) {
    let dir = dir.canonicalize().unwrap_or_else(|e| panic!("Conformance directory {dir:?}: {e}"));
    let version_file = dir.join("VERSION");
    assert!(
        version_file.is_file(),
        "Not a conformance fixture set, VERSION not found: {}",
        dir.display()
    );
    let fixture_set_version = fs::read_to_string(&version_file)
        .expect("VERSION readable")
        .trim()
        .to_string();
    assert_eq!(
        fixture_set_version, FIXTURE_SET_VERSION,
        "Conformance fixture set {fixture_set_version} found, {FIXTURE_SET_VERSION} expected: \
         check out that release or update the declarations"
    );

    let mut rows = load_rows(&dir);
    apply_declarations(&mut rows);

    let mut server: Option<FixtureServer> = None;
    let mut report_rows = Vec::new();
    let mut mismatches = Vec::new();

    for row in &rows {
        let actual = if let Some(blocked_by) = &row.blocked_by {
            Outcome::new(BLOCKED, Some(blocked_by.clone()))
        } else {
            match row.kind.as_str() {
                "translate" => translate_row(row),
                "merge" => merge_row(row).await,
                "tls" => tls_row(row),
                "dns" | "aws-tags" => {
                    if server.is_none() {
                        server = Some(FixtureServer::start(&dir));
                    }
                    provider_row(row, server.as_ref().expect("started above")).await
                }
                other => panic!("{row}: unknown kind {other}"),
            }
        };

        let declared = &row.declared;
        let reason = if declared.reason.is_some() && declared.result == actual.result {
            declared.reason.clone()
        } else {
            actual.reason.clone()
        };
        let mut entry = serde_json::Map::new();
        entry.insert("file".into(), row.file.clone().into());
        entry.insert("case".into(), row.case.clone().into());
        entry.insert("result".into(), actual.result.into());
        if let Some(reason) = &reason {
            entry.insert("reason".into(), reason.clone().into());
        }
        if let Some(detail) = &actual.reason {
            if Some(detail) != reason.as_ref() {
                entry.insert("detail".into(), detail.clone().into());
            }
        }
        report_rows.push(entry);

        println!(
            "CONFORMANCE {row} {}{}",
            actual.result,
            actual.reason.as_deref().map(|r| format!(": {r}")).unwrap_or_default()
        );
        if declared.result != actual.result {
            mismatches.push(format!(
                "{row} declared {}, actual {}{}",
                declared.result,
                actual.result,
                actual.reason.as_deref().map(|r| format!(": {r}")).unwrap_or_default()
            ));
        }
    }
    drop(server);

    write_report(&fixture_set_version, report_rows);
    assert!(
        mismatches.is_empty(),
        "{} conformance row(s) differ from {DECLARED}:\n  {}",
        mismatches.len(),
        mismatches.join("\n  ")
    );
}

// ---------------------------------------------------------------------------
// Rows
// ---------------------------------------------------------------------------

struct Row {
    file: String,
    kind: String,
    case: String,
    blocked_by: Option<String>,
    data: Value,
    /// The `tls` kind: the certificate of the row's shape.
    certificate: Option<CertificateDer<'static>>,
    declared: Declaration,
}

impl std::fmt::Display for Row {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.file, self.case)
    }
}

#[derive(Clone)]
struct Declaration {
    result: &'static str,
    reason: Option<String>,
}

struct Outcome {
    result: &'static str,
    reason: Option<String>,
}

impl Outcome {
    fn new(result: &'static str, reason: Option<String>) -> Self {
        Outcome { result, reason }
    }

    fn pass() -> Self {
        Self::new(PASS, None)
    }

    fn fail(reason: impl Into<String>) -> Self {
        Self::new(FAIL, Some(reason.into()))
    }
}

/// Every fixture file under the fixture roots, as the report collector reads
/// them: a `kind` the set knows and its `rows`, sorted by path.
fn load_rows(dir: &Path) -> Vec<Row> {
    let mut files = Vec::new();
    for root in FIXTURE_ROOTS {
        walk(&dir.join(root), &mut files);
    }
    files.sort();

    let mut rows = Vec::new();
    for path in files {
        let file = path
            .strip_prefix(dir)
            .expect("under the fixture directory")
            .to_string_lossy()
            .replace('\\', "/");
        let text = fs::read_to_string(&path).unwrap_or_else(|e| panic!("{file}: {e}"));
        let top: Value = serde_norway::from_str(&text).unwrap_or_else(|e| panic!("{file}: {e}"));
        let Some(kind) = get(&top, "kind").and_then(Value::as_str) else {
            continue;
        };
        if !FIXTURE_KINDS.contains(&kind) {
            continue;
        }
        let mut certificates: HashMap<String, CertificateDer<'static>> = HashMap::new();
        for item in get(&top, "rows").and_then(Value::as_sequence).into_iter().flatten() {
            let case = str_of(item, "case").to_string();
            let certificate = if kind == "tls" {
                let shape = str_of(get(item, "input").expect("input"), "shape").to_string();
                Some(
                    certificates
                        .entry(shape.clone())
                        .or_insert_with(|| {
                            let pem = path.with_file_name("certs").join(format!("{shape}.pem"));
                            CertificateDer::from_pem_file(&pem)
                                .unwrap_or_else(|e| panic!("Cannot read certificate {pem:?}: {e}"))
                        })
                        .clone(),
                )
            } else {
                None
            };
            let blocked_by = if get(item, "state").and_then(Value::as_str) == Some(BLOCKED) {
                Some(str_of(item, "blockedBy").to_string())
            } else {
                None
            };
            let declared = Declaration {
                result: if blocked_by.is_some() { BLOCKED } else { PASS },
                reason: None,
            };
            rows.push(Row {
                file: file.clone(),
                kind: kind.to_string(),
                case,
                blocked_by,
                data: item.clone(),
                certificate,
                declared,
            });
        }
    }
    assert!(!rows.is_empty(), "no fixture cases under {}", dir.display());
    rows
}

fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with('.') {
            continue;
        }
        if path.is_dir() {
            walk(&path, out);
        } else if name.ends_with(".yaml") {
            out.push(path);
        }
    }
}

/// `tests/conformance/rust-declared.yaml`: fixture file to case to
/// `{result, reason}`. Validated the way the Java runner validates its file,
/// so a stale declaration fails the run instead of hiding a row.
fn apply_declarations(rows: &mut [Row]) {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(DECLARED);
    let text = fs::read_to_string(&path).unwrap_or_else(|e| panic!("Declarations not found: {path:?}: {e}"));
    let doc: Value = serde_norway::from_str(&text).unwrap_or_else(|e| panic!("{DECLARED}: {e}"));
    if doc.is_null() {
        return;
    }
    let files = doc
        .as_mapping()
        .unwrap_or_else(|| panic!("{DECLARED}: must be a map"));
    for (file, cases) in files {
        let file = file.as_str().unwrap_or_else(|| panic!("{DECLARED}: file names must be strings"));
        assert!(
            rows.iter().any(|r| r.file == file),
            "{DECLARED}: unknown fixture file {file}"
        );
        let cases = cases
            .as_mapping()
            .unwrap_or_else(|| panic!("{DECLARED}: {file} must be a map"));
        for (case, decl) in cases {
            let case = case.as_str().unwrap_or_else(|| panic!("{DECLARED}: {file}: case names must be strings"));
            let where_ = format!("{DECLARED}: {file}:{case}");
            let row = rows
                .iter_mut()
                .find(|r| r.file == file && r.case == case)
                .unwrap_or_else(|| panic!("{where_}: unknown case"));
            let decl = decl
                .as_mapping()
                .unwrap_or_else(|| panic!("{where_} must be a map"));
            for key in decl.keys() {
                let key = key.as_str().unwrap_or_default();
                assert!(key == "result" || key == "reason", "{where_}: unknown key {key}");
            }
            let result = get(&Value::Mapping(decl.clone()), "result")
                .and_then(Value::as_str)
                .and_then(|r| RESULTS.iter().find(|known| **known == r).copied())
                .unwrap_or_else(|| panic!("{where_}: invalid result"));
            let reason = get(&Value::Mapping(decl.clone()), "reason")
                .and_then(Value::as_str)
                .map(str::to_string);
            assert!(
                result == PASS || reason.as_deref().is_some_and(|r| !r.is_empty()),
                "{where_}: a {result} declaration needs a reason"
            );
            row.declared = Declaration { result, reason };
        }
    }
}

// ---------------------------------------------------------------------------
// translate
// ---------------------------------------------------------------------------

fn translate_row(row: &Row) -> Outcome {
    let config = get(&row.data, "config").cloned().unwrap_or(Value::Null);
    let input = get(&row.data, "input").expect("input");
    let expect = get(&row.data, "expect").expect("expect");

    let mut policy = ClientPolicy::default();
    match get(&config, "translation") {
        None => policy.ip_map = get(&config, "ip_map").map(string_map),
        Some(translation) => {
            if let Some(on_miss) = get(translation, "on_miss").and_then(Value::as_str) {
                policy.translation_on_miss = match on_miss.parse::<TranslationMissMode>() {
                    Ok(mode) => mode,
                    Err(e) => return Outcome::fail(format!("on_miss: {e}")),
                };
            }
            let provider = str_of(translation, "provider");
            policy.address_translator = Some(match provider {
                "static-map" => Arc::new(StaticMapAddressTranslator::new(
                    get(translation, "static_map").map(string_map),
                )),
                "pass-through" => Arc::new(PassThroughAddressTranslator),
                "port-map" => {
                    let map = get(translation, "port_map").map(string_map).unwrap_or_default();
                    match PortMapAddressTranslator::new(map) {
                        Ok(t) => Arc::new(t),
                        Err(e) => return Outcome::fail(format!("port-map: {e}")),
                    }
                }
                other => {
                    return Outcome::new(
                        UNSUPPORTED,
                        Some(format!("translator {other} is not implemented")),
                    )
                }
            });
        }
    }

    let source_list = if str_of(input, "sourceList") == "alternate" {
        SourceList::Alternate
    } else {
        SourceList::Standard
    };
    let advertised = Endpoint::new(
        str_of(input, "nodeName").to_string(),
        get(input, "tlsName").and_then(Value::as_str).map(str::to_string),
        str_of(input, "host").to_string(),
        port_of(input, "port"),
        source_list,
    );
    let error = get(expect, "error");

    match translate_with_policy(&policy, &advertised) {
        Err(e) if matches!(e.kind(), ErrorKind::TranslationMiss) => {
            let Some(error) = error else {
                return Outcome::fail(format!("threw {e}"));
            };
            if str_of(error, "category") != "translation-miss" {
                return Outcome::fail(format!("threw {e}"));
            }
            let message = e.to_string();
            for value in get(error, "messageContains")
                .and_then(Value::as_mapping)
                .into_iter()
                .flat_map(|m| m.values())
            {
                let value = scalar(value);
                if !message.contains(&value) {
                    return Outcome::fail(format!("message does not contain {value}: {message}"));
                }
            }
            Outcome::pass()
        }
        Err(e) => Outcome::fail(format!("threw {e}")),
        Ok(actual) => {
            if let Some(error) = error {
                return Outcome::fail(format!(
                    "expected {} error, got {}",
                    str_of(error, "category"),
                    endpoint_string(&actual.name, actual.port, actual.tls_name.as_deref())
                ));
            }
            let expected = endpoint_string(
                str_of(expect, "host"),
                port_of(expect, "port"),
                get(expect, "tlsName").and_then(Value::as_str),
            );
            let got = endpoint_string(&actual.name, actual.port, actual.tls_name.as_deref());
            if expected != got {
                return Outcome::fail(format!("expected {expected}, got {got}"));
            }
            Outcome::pass()
        }
    }
}

fn endpoint_string(host: &str, port: u16, tls_name: Option<&str>) -> String {
    format!("host={host} port={port} tlsName={}", tls_name.unwrap_or("null"))
}

// ---------------------------------------------------------------------------
// merge
// ---------------------------------------------------------------------------

/// A provider answering the fixture's discovery results in order.
struct Scripted {
    results: Mutex<std::vec::IntoIter<Vec<Host>>>,
}

#[async_trait::async_trait]
impl SeedCandidateProvider for Scripted {
    async fn refresh_seed_candidates(&self, _ctx: &SeedCandidateContext) -> aerospike::Result<Vec<Host>> {
        Ok(self
            .results
            .lock()
            .unwrap()
            .next()
            .expect("one discovery result per refresh"))
    }

    fn needs_periodic_refresh(&self) -> bool {
        true
    }

    fn supports_deadline(&self) -> bool {
        true
    }
}

async fn merge_row(row: &Row) -> Outcome {
    let config = get(&row.data, "config").expect("config");
    let mut policy = ClientPolicy::default();
    if let Some(merge) = get(config, "discovery")
        .and_then(|d| get(d, "merge_policy"))
        .and_then(Value::as_str)
    {
        policy.discovery_merge_policy = match merge.parse::<SeedMergePolicy>() {
            Ok(m) => m,
            Err(e) => return Outcome::fail(format!("merge_policy: {e}")),
        };
    }
    let static_seeds = hosts_of(get(config, "seeds"));
    let results: Vec<Vec<Host>> = get(get(&row.data, "input").expect("input"), "discoveryResults")
        .and_then(Value::as_sequence)
        .map(|s| s.iter().map(|r| hosts_of(Some(r))).collect())
        .unwrap_or_default();
    let count = results.len();
    let provider = Arc::new(Scripted {
        results: Mutex::new(results.into_iter()),
    });

    let mut refresher = Refresher::new(&policy, provider, static_seeds);
    let mut snapshots = Vec::with_capacity(count);
    snapshots.push(refresher.initial().await.1);
    for _ in 1..count {
        snapshots.push(refresher.refresh().await.1);
    }

    let expected = get(get(&row.data, "expect").expect("expect"), "snapshots")
        .and_then(Value::as_sequence)
        .cloned()
        .unwrap_or_default();
    for (i, exp) in expected.iter().enumerate() {
        let exp = seeds_string(&hosts_of(Some(exp)));
        let got = snapshots.get(i).map(|s| seeds_string(s)).unwrap_or_default();
        if exp != got {
            return Outcome::fail(format!("snapshot {i}: expected {exp}, got {got}"));
        }
    }
    Outcome::pass()
}

fn seeds_string(hosts: &[Host]) -> String {
    let items: Vec<String> = hosts.iter().map(|h| format!("{} {}", h.name, h.port)).collect();
    format!("[{}]", items.join(", "))
}

// ---------------------------------------------------------------------------
// tls
// ---------------------------------------------------------------------------

/// Name matching exactly as the client's TLS handshake does it: rustls
/// verifies the reference identifier against the certificate's subject
/// alternative names, with no CN fallback.
fn tls_row(row: &Row) -> Outcome {
    let tls_name = str_of(get(&row.data, "input").expect("input"), "tlsName").to_string();
    let expected = str_of(get(&row.data, "expect").expect("expect"), "outcome");
    let certificate = row.certificate.as_ref().expect("tls row has a certificate");

    let parsed = match ParsedCertificate::try_from(certificate) {
        Ok(parsed) => parsed,
        Err(e) => return Outcome::fail(format!("certificate does not parse: {e}")),
    };
    let got = match ServerName::try_from(tls_name.as_str()) {
        // A tlsName that is not a DNS name or an IP address can match nothing.
        Err(_) => "reject",
        Ok(name) => match verify_server_name(&parsed, &name) {
            Ok(()) => "accept",
            Err(_) => "reject",
        },
    };
    if expected != got {
        return Outcome::fail(format!("expected {expected}, got {got}"));
    }
    Outcome::pass()
}

// ---------------------------------------------------------------------------
// dns and aws-tags
// ---------------------------------------------------------------------------

/// `discovery/providers/fixture-server.py`: a DNS server and an EC2 endpoint
/// answering what the selected refresh says.
struct FixtureServer {
    child: Child,
    http: u16,
    dns: u16,
}

impl FixtureServer {
    fn start(dir: &Path) -> Self {
        let script = dir.join("discovery/providers/fixture-server.py");
        let mut child = Command::new("python3")
            .arg("-I")
            .arg(&script)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap_or_else(|e| {
                panic!(
                    "python3 is required to run the dns and aws-tags fixtures ({}): {e}",
                    script.display()
                )
            });
        let stdout = child.stdout.take().expect("piped stdout");
        let mut line = String::new();
        BufReader::new(stdout)
            .read_line(&mut line)
            .expect("fixture server prints its ports");
        let ports: serde_json::Value =
            serde_json::from_str(line.trim()).unwrap_or_else(|e| panic!("fixture server ports {line:?}: {e}"));
        let port = |name: &str| {
            ports[name]
                .as_u64()
                .and_then(|p| u16::try_from(p).ok())
                .unwrap_or_else(|| panic!("fixture server printed no {name} port: {line}"))
        };
        let server = FixtureServer {
            http: port("http"),
            dns: port("dns"),
            child,
        };
        println!(
            "CONFORMANCE fixture server: http 127.0.0.1:{} dns 127.0.0.1:{}",
            server.http, server.dns
        );
        server
    }

    /// Selects the refresh the server answers next.
    fn select(&self, refresh: &Value, region: Option<&str>) {
        let body = serde_json::json!({
            "refresh": serde_json::to_value(refresh).expect("yaml to json"),
            "region": region,
        });
        let (status, text) = http_post(self.http, "/fixture/select", &body.to_string());
        assert_eq!(status, 200, "fixture select failed: {text}");
    }
}

impl Drop for FixtureServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// One HTTP/1.1 POST to the fixture server, returning the status and body.
fn http_post(port: u16, path: &str, body: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("fixture server reachable");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("read timeout");
    write!(
        stream,
        "POST {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .expect("request written");
    let mut response = Vec::new();
    stream.read_to_end(&mut response).expect("response read");
    let text = String::from_utf8_lossy(&response).into_owned();
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let body = text.split_once("\r\n\r\n").map(|(_, b)| b.to_string()).unwrap_or_default();
    (status, body)
}

async fn provider_row(row: &Row, server: &FixtureServer) -> Outcome {
    let discovery = get(get(&row.data, "config").expect("config"), "discovery").expect("discovery");
    let provider_name = str_of(discovery, "provider");
    let region = get(discovery, "aws")
        .and_then(|a| get(a, "region"))
        .and_then(Value::as_str)
        .map(str::to_string);

    let provider: Arc<dyn SeedCandidateProvider> = match (row.kind.as_str(), provider_name) {
        ("dns", "dns") => match dns_provider(get(discovery, "dns").expect("dns block"), server.dns) {
            Ok(p) => Arc::new(p),
            Err(e) => return Outcome::fail(format!("dns provider: {e}")),
        },
        ("aws-tags", "aws-tags") => {
            match aws_provider(get(discovery, "aws").expect("aws block"), server.http) {
                Ok(p) => Arc::new(p),
                Err(e) => return Outcome::fail(format!("aws-tags provider: {e}")),
            }
        }
        (kind, other) => {
            return Outcome::new(
                UNSUPPORTED,
                Some(format!("provider {other} is not implemented for kind {kind}")),
            )
        }
    };

    let mut policy = ClientPolicy::default();
    policy.discovery_merge_policy = SeedMergePolicy::DiscoveryOnly;
    let mut refresher = Refresher::new(&policy, provider, Vec::new());

    let inputs = get(get(&row.data, "input").expect("input"), "refreshes")
        .and_then(Value::as_sequence)
        .cloned()
        .unwrap_or_default();
    let expected = get(get(&row.data, "expect").expect("expect"), "refreshes")
        .and_then(Value::as_sequence)
        .cloned()
        .unwrap_or_default();

    for (i, (input, expect)) in inputs.iter().zip(expected.iter()).enumerate() {
        server.select(input, region.as_deref());
        let (result, seeds) = if i == 0 {
            refresher.initial().await
        } else {
            refresher.refresh().await
        };

        match (get(expect, "hosts"), get(expect, "error")) {
            (Some(hosts), _) => match &result {
                Ok(got) => {
                    let exp = host_set(&hosts_of(Some(hosts)));
                    let got = host_set(got);
                    if exp != got {
                        return Outcome::fail(format!("refresh {i}: expected hosts {exp:?}, got {got:?}"));
                    }
                }
                Err(e) => return Outcome::fail(format!("refresh {i}: expected hosts, got error {e}")),
            },
            (None, Some(error)) => {
                let Err(e) = &result else {
                    return Outcome::fail(format!(
                        "refresh {i}: expected {} error, got hosts {:?}",
                        str_of(error, "category"),
                        result.as_ref().map(|h| host_set(h)).unwrap_or_default()
                    ));
                };
                let message = e.to_string();
                for fact in get(error, "messageContains")
                    .and_then(Value::as_sequence)
                    .into_iter()
                    .flatten()
                {
                    let fact = scalar(fact);
                    if !message.contains(&fact) {
                        return Outcome::fail(format!(
                            "refresh {i}: message does not contain {fact}: {message}"
                        ));
                    }
                }
            }
            (None, None) => panic!("{row}: refresh {i} expects neither hosts nor error"),
        }

        let exp = host_set(&hosts_of(get(expect, "seeds")));
        let got = host_set(&seeds);
        if exp != got {
            return Outcome::fail(format!("refresh {i}: expected seeds {exp:?}, got {got:?}"));
        }
    }
    Outcome::pass()
}

fn dns_provider(block: &Value, dns_port: u16) -> aerospike::Result<DnsSeedCandidateProvider> {
    let names: Vec<String> = get(block, "names")
        .and_then(Value::as_sequence)
        .map(|s| s.iter().map(scalar).collect())
        .unwrap_or_default();
    let mut builder = DnsSeedCandidateProvider::builder()
        .names(names)
        .servers([format!("127.0.0.1:{dns_port}")])
        .timeout(timeout_of(block));
    if let Some(record_type) = get(block, "record_type").and_then(Value::as_str) {
        builder = builder.record_type(match record_type {
            "srv" => RecordType::Srv,
            _ => RecordType::A,
        });
    }
    if let Some(port) = get(block, "port").and_then(Value::as_u64) {
        builder = builder.port(u16::try_from(port).expect("port"));
    }
    if let Some(tls_name) = get(block, "tls_name").and_then(Value::as_str) {
        builder = builder.tls_name(tls_name);
    }
    builder.build()
}

fn aws_provider(block: &Value, http_port: u16) -> aerospike::Result<AwsEc2TagSeedCandidateProvider> {
    let timeout = timeout_of(block);
    let mut builder = AwsEc2TagSeedCandidateProvider::builder()
        .region(str_of(block, "region"))
        .endpoint(format!("http://127.0.0.1:{http_port}"))
        .credentials(Credentials::new(FIXTURE_ACCESS_KEY_ID, FIXTURE_SECRET, None)?)
        .timeout(timeout)
        .init_timeout(timeout);
    for filter in get(block, "filters").and_then(Value::as_sequence).into_iter().flatten() {
        let values: Vec<String> = get(filter, "values")
            .and_then(Value::as_sequence)
            .map(|s| s.iter().map(scalar).collect())
            .unwrap_or_default();
        builder = builder.filter(str_of(filter, "name"), values);
    }
    if let Some(field) = get(block, "address_field").and_then(Value::as_str) {
        builder = builder.address_field(match field {
            "public_ip" => AddressField::PublicIp,
            "private_dns" => AddressField::PrivateDns,
            "public_dns" => AddressField::PublicDns,
            _ => AddressField::PrivateIp,
        });
    }
    if let Some(port) = get(block, "port").and_then(Value::as_u64) {
        builder = builder.port(u16::try_from(port).expect("port"));
    }
    if let Some(tls_name) = get(block, "tls_name").and_then(Value::as_str) {
        builder = builder.tls_name(tls_name);
    }
    builder.build()
}

fn timeout_of(block: &Value) -> u32 {
    get(block, "timeout_ms")
        .and_then(Value::as_u64)
        .and_then(|t| u32::try_from(t).ok())
        .unwrap_or(LOOPBACK_TIMEOUT_MS)
}

/// Hosts as the provider fixtures compare them: by host, port and tlsName in
/// any order, addresses compared as addresses.
fn host_set(hosts: &[Host]) -> Vec<(String, u16, Option<String>)> {
    let mut set: Vec<(String, u16, Option<String>)> = hosts
        .iter()
        .map(|h| (canonical_host(&h.name), h.port, h.tls_name.clone()))
        .collect();
    set.sort();
    set
}

fn canonical_host(name: &str) -> String {
    name.parse::<IpAddr>()
        .map(|ip| ip.to_string())
        .unwrap_or_else(|_| name.to_ascii_lowercase())
}

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

fn write_report(fixture_set_version: &str, mut rows: Vec<serde_json::Map<String, serde_json::Value>>) {
    rows.sort_by(|a, b| {
        let key = |m: &serde_json::Map<String, serde_json::Value>| {
            format!("{}:{}", m["file"].as_str().unwrap_or(""), m["case"].as_str().unwrap_or(""))
        };
        key(a).cmp(&key(b))
    });
    let mut counts: BTreeMap<&str, usize> = RESULTS.iter().map(|r| (*r, 0)).collect();
    for row in &rows {
        if let Some(result) = row["result"].as_str() {
            *counts.entry(RESULTS.iter().find(|r| **r == result).copied().unwrap_or(FAIL)).or_default() += 1;
        }
    }
    let report = serde_json::json!({
        "runtime": RUNTIME,
        "clientVersion": Client::client_version(),
        "fixtureSetVersion": fixture_set_version,
        "counts": counts,
        "rows": rows,
    });
    let path = env::var_os("CONFORMANCE_REPORT")
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(env!("CARGO_MANIFEST_DIR")).join("target/conformance-report.json"));
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("report directory");
    }
    let mut text = serde_json::to_string_pretty(&report).expect("report serializes");
    text.push('\n');
    fs::write(&path, text).unwrap_or_else(|e| panic!("cannot write {path:?}: {e}"));
    println!(
        "CONFORMANCE report {} ({})",
        path.display(),
        counts
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(" ")
    );
}

// ---------------------------------------------------------------------------
// YAML helpers
// ---------------------------------------------------------------------------

fn get<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    value.as_mapping().and_then(|m| m.get(Value::String(key.to_string())))
}

fn str_of<'a>(value: &'a Value, key: &str) -> &'a str {
    get(value, key)
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("fixture row has no string {key}: {value:?}"))
}

fn port_of(value: &Value, key: &str) -> u16 {
    get(value, key)
        .and_then(Value::as_u64)
        .and_then(|p| u16::try_from(p).ok())
        .unwrap_or_else(|| panic!("fixture row has no port {key}: {value:?}"))
}

/// A scalar as the string a message would contain.
fn scalar(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "null".to_string(),
        other => serde_norway::to_string(other).unwrap_or_default().trim().to_string(),
    }
}

fn string_map(value: &Value) -> HashMap<String, String> {
    value
        .as_mapping()
        .map(|m| m.iter().map(|(k, v)| (scalar(k), scalar(v))).collect())
        .unwrap_or_default()
}

fn hosts_of(value: Option<&Value>) -> Vec<Host> {
    value
        .and_then(Value::as_sequence)
        .map(|s| {
            s.iter()
                .map(|seed| {
                    let mut host = Host::new(str_of(seed, "host"), port_of(seed, "port"));
                    host.tls_name = get(seed, "tlsName").and_then(Value::as_str).map(str::to_string);
                    host
                })
                .collect()
        })
        .unwrap_or_default()
}
