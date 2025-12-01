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
use rand::distributions::Alphanumeric;
use rand::Rng;

use tokio::sync::OnceCell;

use aerospike::{AuthMode, Client, ClientPolicy};

#[cfg(feature = "tls")]
use rustls::pki_types::pem::PemObject;
#[cfg(feature = "tls")]
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
#[cfg(feature = "tls")]
use rustls::RootCertStore;

lazy_static! {
    static ref AEROSPIKE_HOSTS: String =
        // env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| String::from("127.0.0.1:tls1:3111"));
        env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| String::from("127.0.0.1:3100"));
    static ref AEROSPIKE_NAMESPACE: String =
        env::var("AEROSPIKE_NAMESPACE").unwrap_or_else(|_| String::from("test"));
    static ref AEROSPIKE_CLUSTER: Option<String> = env::var("AEROSPIKE_CLUSTER").ok();
    static ref AEROSPIKE_USE_SERVICES_ALTERNATE: bool =
        env::var("AEROSPIKE_USE_SERVICES_ALTERNATE").is_ok();
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

pub fn client_policy() -> &'static ClientPolicy {
    &*GLOBAL_CLIENT_POLICY
}

pub async fn client() -> Client {
    Client::new(&GLOBAL_CLIENT_POLICY, &*AEROSPIKE_HOSTS)
        .await
        .unwrap()
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
            // insert_bins(namespace(), "test").await.unwrap();
            let client = Client::new(&GLOBAL_CLIENT_POLICY, &*AEROSPIKE_HOSTS)
                .await
                .unwrap();

            client
        })
        .await
}

pub fn rand_str(sz: usize) -> String {
    let rng = rand::thread_rng();
    rng.sample_iter(&Alphanumeric).take(sz).collect()
}

pub async fn enterprise_edition() -> bool {
    let client = client().await;
    let node = client.cluster.get_random_node().await;
    if let Err(_) = node {
        return false;
    }

    let node = node.unwrap();
    let edition = node.info(&vec!["edition"]).await;
    if let Err(_) = edition {
        return false;
    }

    if let Some(edition) = edition.unwrap().get("edition") {
        return edition.to_lowercase().contains("enterprise");
    }

    false
}

pub async fn security_enabled() -> bool {
    if !enterprise_edition().await {
        return false;
    }

    let client = client().await;
    let roles = client.query_users(None).await;
    if let Err(_) = roles {
        return false;
    }

    true
}

pub async fn insert_bins(ns: &str, set_name: &str) -> aerospike::Result<()> {
    let client = crate::common::client().await;
    let wp = aerospike::WritePolicy::default();

    for i in 0u32..3000 {
        let key = aerospike::as_key!(ns, set_name, i);
        let bins = vec![
            aerospike::as_bin!("bin_i", i),
            aerospike::as_bin!("bin_s", rand_str(3)),
        ];

        client.put(&wp, &key, &bins).await?;
    }

    let task = client
        .create_index_on_bin(
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
