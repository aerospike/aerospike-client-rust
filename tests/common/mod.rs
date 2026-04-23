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

#![allow(dead_code)]

use std::env;

use aerospike::CollectionIndexType;
use aerospike::Task;

use rand;
use rand::distr::Alphanumeric;
use rand::RngExt;

use tokio::sync::OnceCell;

use aerospike::{AdminPolicy, AuthMode, Client, ClientPolicy};

#[cfg(feature = "tls")]
use rustls::pki_types::pem::PemObject;
#[cfg(feature = "tls")]
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
#[cfg(feature = "tls")]
use rustls::RootCertStore;

lazy_static! {
    static ref AEROSPIKE_HOSTS: String =
        // env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| String::from("127.0.0.1:tls1:3111"));
        env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| String::from("127.0.0.1:3000"));
    static ref AEROSPIKE_NAMESPACE: String =
        env::var("AEROSPIKE_NAMESPACE").unwrap_or_else(|_| String::from("test"));
    static ref AEROSPIKE_PROP_SET_NAME: String =
        env::var("AEROSPIKE_PROP_SET_NAME").unwrap_or_else(|_| String::from("test"));
    static ref AEROSPIKE_CLUSTER: Option<String> = env::var("AEROSPIKE_CLUSTER").ok();
    static ref AEROSPIKE_USE_SERVICES_ALTERNATE: bool =
        env::var("AEROSPIKE_USE_SERVICES_ALTERNATE").map(|v| v.trim().eq_ignore_ascii_case("true") || v.trim().eq_ignore_ascii_case("1")).unwrap_or(true);
}

#[cfg(all(any(feature = "rt-tokio"), not(feature = "rt-async-std")))]
lazy_static! {
    pub static ref RUNTIME: tokio::runtime::Runtime = {
        use tokio::runtime;
        runtime::Builder::new_current_thread()
        // runtime::Builder::new_multi_thread()
        //     .worker_threads(10)
            .enable_all()
            .build()
            .unwrap()
    };
}

#[cfg(not(feature = "tls"))]
lazy_static! {
    static ref GLOBAL_CLIENT_POLICY: ClientPolicy = {
        let mut policy = ClientPolicy::default();
        if let Ok(user) = env::var("AEROSPIKE_USER") {
            let password = env::var("AEROSPIKE_PASSWORD").unwrap_or_default();
            policy
                .set_auth_mode(AuthMode::Internal(user, password))
                .unwrap();
        }
        policy.cluster_name = AEROSPIKE_CLUSTER.clone();
        policy.use_services_alternate = AEROSPIKE_USE_SERVICES_ALTERNATE.clone();
        policy
    };
}

#[cfg(feature = "tls")]
lazy_static! {
    static ref GLOBAL_CLIENT_POLICY: ClientPolicy = {
        let mut policy = ClientPolicy::default();
        if let Ok(user) = env::var("AEROSPIKE_USER") {
            let password = env::var("AEROSPIKE_PASSWORD").unwrap_or_default();
            policy
                .set_auth_mode(AuthMode::Internal(user, password))
                .unwrap();
            policy.cluster_name = AEROSPIKE_CLUSTER.clone();
        }
        policy.use_services_alternate = AEROSPIKE_USE_SERVICES_ALTERNATE.clone();
        if !no_tls() {
            policy.tls_config = Some(tls_config_no_client_auth());
        }
        policy
    };
    static ref AEROSPIKE_CACERT_FILE: String =
        env::var("AEROSPIKE_CACERT_FILE").unwrap_or_default();
    static ref AEROSPIKE_KEY_FILE: String = env::var("AEROSPIKE_KEY_FILE").unwrap_or_default();
}

#[cfg(feature = "tls")]
pub fn no_tls() -> bool {
    AEROSPIKE_CACERT_FILE.is_empty() || AEROSPIKE_KEY_FILE.is_empty()
}

pub fn hosts() -> &'static str {
    &*AEROSPIKE_HOSTS
}

#[cfg(feature = "tls")]
pub fn tls_config() -> rustls::ClientConfig {
    let mut root_store = RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.into(),
    };

    root_store.add_parsable_certificates(
        CertificateDer::pem_file_iter(tls_cacert_file())
            .expect("Cannot open CA file")
            .map(|result| result.unwrap()),
    );

    let client_ca = CertificateDer::from_pem_file(tls_cacert_file()).expect("Cannot open CA file");
    let client_key = PrivateKeyDer::from_pem_file(tls_key_file()).expect("Cannot open Key file");

    rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_client_auth_cert(vec![client_ca], client_key)
        .unwrap()
}

#[cfg(feature = "tls")]
pub fn tls_config_no_client_auth() -> rustls::ClientConfig {
    let mut root_store = RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.into(),
    };

    root_store.add_parsable_certificates(
        CertificateDer::pem_file_iter(tls_cacert_file())
            .expect("Cannot open CA file")
            .map(|result| result.unwrap()),
    );

    rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth()
}

#[cfg(feature = "tls")]
pub fn tls_cacert_file() -> &'static str {
    &*AEROSPIKE_CACERT_FILE
}

#[cfg(feature = "tls")]
pub fn tls_key_file() -> &'static str {
    &*AEROSPIKE_KEY_FILE
}

pub fn namespace() -> &'static str {
    &*AEROSPIKE_NAMESPACE
}

pub fn prop_setname() -> &'static str {
    &*AEROSPIKE_PROP_SET_NAME
}

pub fn prop_setname_multi() -> String {
    format!("{}-multi", prop_setname())
}

pub fn client_policy() -> &'static ClientPolicy {
    &*GLOBAL_CLIENT_POLICY
}

pub async fn client() -> Client {
    Client::new(&GLOBAL_CLIENT_POLICY, &*AEROSPIKE_HOSTS)
        .await
        .unwrap_or_else(|e| {
            panic!(
                "integration tests: could not connect to AEROSPIKE_HOSTS={}: {}\n\
                 Start a node or set AEROSPIKE_HOSTS. For Docker/advertised addresses try \
                 AEROSPIKE_USE_SERVICES_ALTERNATE=true (default in this repo's test common).",
                hosts(),
                e
            );
        })
}

pub async fn singleton_client() -> &'static Client {
    static SHARED_CLIENT: OnceCell<Client> = OnceCell::const_new();
    SHARED_CLIENT
        .get_or_init(|| async {
            // std::panic::set_hook(Box::new(|info| {
            //     //let stacktrace = Backtrace::capture();
            //     let stacktrace = std::backtrace::Backtrace::force_capture();
            //     println!("Got panic. @info:{}\n@stackTrace:{}", info, stacktrace);
            //     std::process::abort();
            // }));
            // console_subscriber::init();
            insert_bins(namespace(), &prop_setname_multi(), 1000)
                .await
                .unwrap();
            let client = Client::new(&GLOBAL_CLIENT_POLICY, &*AEROSPIKE_HOSTS)
                .await
                .unwrap_or_else(|e| {
                    panic!(
                        "singleton_client: could not connect to AEROSPIKE_HOSTS={}: {}\n\
                         Start a node or set AEROSPIKE_HOSTS; see also AEROSPIKE_USE_SERVICES_ALTERNATE.",
                        hosts(),
                        e
                    );
                });

            client
        })
        .await
}

pub fn rand_str(sz: usize) -> String {
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(sz)
        .map(char::from)
        .collect()
}

pub async fn enterprise_edition() -> bool {
    let client = client().await;
    let node = client.cluster.get_random_node();
    if let Err(_) = node {
        return false;
    }

    let node = node.unwrap();
    let edition = node.info(&AdminPolicy::default(), &vec!["edition"]).await;
    if let Err(_) = edition {
        return false;
    }

    if let Some(edition) = edition.unwrap().get("edition") {
        return edition.to_lowercase().contains("enterprise");
    }

    false
}

/// Check whether the given namespace is configured with strong-consistency.
/// Returns `false` on any communication error so tests that key off this
/// default to the AP-compatible path.
pub async fn namespace_is_sc(client: &aerospike::Client, ns: &str) -> bool {
    let node = match client.cluster.get_random_node() {
        Ok(n) => n,
        Err(_) => return false,
    };
    let info_key = format!("namespace/{ns}");
    match node.info(&AdminPolicy::default(), &[&info_key]).await {
        Ok(map) => map
            .get(&info_key)
            .map(|info| {
                info.contains("strong-consistency=true")
                    || info.contains("strong-consistency-allow-expunge=true")
            })
            .unwrap_or(false),
        Err(_) => false,
    }
}

/// Namespace and host behavior for integration tests. Call [`ServerCapabilities::detect`] once at
/// the start of a test (or module setup), then branch or `return` early instead of scattering
/// probes. Strong-consistency vs AP affects several server features; explicit client TTL is
/// orthogonal but commonly disallowed when nsup/TTL rules reject `Expiration::Seconds` writes.
#[derive(Clone, Copy, Debug)]
pub struct ServerCapabilities {
    pub namespace_strong_consistency: bool,
    pub explicit_record_ttl_allowed: bool,
}

impl ServerCapabilities {
    pub async fn detect(client: &aerospike::Client) -> Self {
        let ns = namespace();
        Self {
            namespace_strong_consistency: namespace_is_sc(client, ns).await,
            explicit_record_ttl_allowed: explicit_record_ttl_probe(client).await,
        }
    }
}

async fn explicit_record_ttl_probe(client: &aerospike::Client) -> bool {
    let ns = namespace();
    let set_name = format!("ttlchk{}", rand_str(6));
    let key = aerospike::as_key!(ns, &set_name, 0i64);
    let wpolicy = aerospike::WritePolicy::new(0, aerospike::Expiration::Seconds(60));
    let bins = vec![aerospike::as_bin!("bin", 0i64)];
    match client.put(&wpolicy, &key, &bins).await {
        Ok(()) => {
            let _ = client
                .delete(&aerospike::WritePolicy::default(), &key)
                .await;
            true
        }
        Err(aerospike::Error::ServerError(
            aerospike::ResultCode::FailForbidden,
            _,
            _,
        )) => false,
        Err(e) => panic!("explicit TTL probe put: {}", e),
    }
}

pub async fn security_enabled() -> bool {
    if !enterprise_edition().await {
        return false;
    }

    let client = client().await;
    let roles = client.query_users(&AdminPolicy::default(), None).await;
    if let Err(_) = roles {
        return false;
    }

    true
}

pub async fn insert_bins(ns: &str, set_name: &str, num_recs: u32) -> aerospike::Result<()> {
    let client = crate::common::client().await;
    let wp = aerospike::WritePolicy::default();
    let ap = aerospike::AdminPolicy::default();

    client
        .truncate(&AdminPolicy::default(), ns, set_name, 0)
        .await
        .expect("should truncate the set");

    let udf_body = r#"
function echo(rec, val)
  return val
end
"#;

    let task = client
        .register_udf(
            &ap,
            udf_body.as_bytes(),
            "test_udf_proptests1.lua",
            aerospike::UDFLang::Lua,
        )
        .await
        .unwrap();
    task.wait_till_complete(None).await.unwrap();

    for i in 0..num_recs {
        let key = aerospike::as_key!(ns, set_name, i);
        let bins = vec![
            aerospike::as_bin!("bin_i", i),
            aerospike::as_bin!("bin_s", rand_str(3)),
        ];

        client
            .put(&wp, &key, &bins)
            .await
            .expect("Initial put failed");
    }

    let apolicy = AdminPolicy::default();
    let task = client
        .create_index_on_bin(
            &apolicy,
            ns,
            set_name,
            "bin_i",
            &format!("{}_{}_{}", ns, set_name, "bin_i"),
            aerospike::IndexType::Numeric,
            CollectionIndexType::Default,
            None,
        )
        .await
        .expect("Failed to create index for bin_i");
    task.wait_till_complete(None).await.unwrap();

    let task = client
        .create_index_on_bin(
            &apolicy,
            ns,
            set_name,
            "bin_s",
            &format!("{}_{}_{}", ns, set_name, "bin_s"),
            aerospike::IndexType::String,
            CollectionIndexType::Default,
            None,
        )
        .await
        .expect("Failed to create index for bin_s");
    task.wait_till_complete(None).await.unwrap();

    Ok(())
}
