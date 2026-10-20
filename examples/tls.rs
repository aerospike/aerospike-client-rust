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

//! TLS and authentication: an encrypted, authenticated connection.
//!
//! The client speaks TLS through rustls. You build a `rustls::ClientConfig`
//! (which roots to trust, and optionally a client certificate), wrap it in a
//! `TlsPolicy`, and put that on the `ClientPolicy`. Host strings may carry
//! the TLS name the server certificate must match: `host:tls-name:port`.
//!
//! Authentication is separate: `AuthMode::Internal` sends a user name and a
//! hashed password, `External` passes the clear password to an external
//! directory (LDAP) over TLS, and `Pki` presents no password at all, since the
//! client certificate's subject *is* the user.
//!
//! `TlsPolicy::with_login_only(true)` encrypts only the login and runs the
//! data plane in cleartext, for clusters that terminate TLS at the edge.
//!
//! The example needs a TLS-enabled server and skips otherwise:
//!
//! ```bash
//! AEROSPIKE_HOSTS=localhost:tls1:4333 \
//! AEROSPIKE_CACERT_FILE=/path/to/ca.pem \
//! AEROSPIKE_USER=admin AEROSPIKE_PASSWORD=admin \
//! cargo run --example tls --features tls
//! ```
//!
//! Set `AEROSPIKE_KEY_FILE` as well to present `AEROSPIKE_CACERT_FILE` as a
//! client certificate for mutual TLS.

use std::env;

use aerospike::{as_bin, as_key, AuthMode, Bins, Client, ClientPolicy, ReadPolicy, TlsPolicy, WritePolicy};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::RootCertStore;

const SET: &str = "tls_demo";

/// Trust the public roots plus the cluster's own CA; present a client
/// certificate when a key file is given.
fn tls_config(ca_file: &str, key_file: Option<&str>) -> rustls::ClientConfig {
    let mut roots = RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.into(),
    };
    roots.add_parsable_certificates(
        CertificateDer::pem_file_iter(ca_file)
            .expect("cannot open the CA file")
            .map(|cert| cert.expect("malformed certificate in the CA file")),
    );
    let builder = rustls::ClientConfig::builder().with_root_certificates(roots);
    match key_file {
        Some(key_file) => {
            let cert = CertificateDer::from_pem_file(ca_file).expect("cannot read the client certificate");
            let key = PrivateKeyDer::from_pem_file(key_file).expect("cannot read the client key");
            builder
                .with_client_auth_cert(vec![cert], key)
                .expect("client certificate and key do not match")
        }
        None => builder.with_no_client_auth(),
    }
}

#[tokio::main]
async fn main() {
    run().await;
}

/// Example body. Standalone via `cargo run --example`, and also driven by
/// the integration test suite (`tests/src/examples.rs`).
pub async fn run() {
    let Ok(ca_file) = env::var("AEROSPIKE_CACERT_FILE") else {
        println!("AEROSPIKE_CACERT_FILE is not set: no TLS server to talk to; skipping.");
        return;
    };
    let key_file = env::var("AEROSPIKE_KEY_FILE").ok();

    let mut cpolicy = ClientPolicy::default();
    cpolicy.use_services_alternate = env::var("AEROSPIKE_USE_SERVICES_ALTERNATE")
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false);

    // ---- TLS ----
    cpolicy.tls_policy = Some(TlsPolicy::new(tls_config(&ca_file, key_file.as_deref())));
    // For login-only TLS, add `.with_login_only(true)`: the credentials still
    // travel encrypted, every later connection is plain TCP.

    // ---- Authentication ----
    cpolicy.set_auth_mode(match env::var("AEROSPIKE_USER") {
        Ok(user) => {
            let password = env::var("AEROSPIKE_PASSWORD").unwrap_or_default();
            AuthMode::Internal(user, password)
        }
        // With a client certificate and no user, let the certificate be the
        // identity; without either, connect unauthenticated.
        Err(_) if key_file.is_some() => AuthMode::Pki,
        Err(_) => AuthMode::None,
    });

    // `host:tls-name:port` names the certificate the node must present.
    let hosts = env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| String::from("127.0.0.1:tls1:4333"));
    let client = Client::new(&cpolicy, &hosts)
        .await
        .expect("Failed to connect to cluster over TLS");
    println!("connected over TLS to {:?}", client.node_names());

    let wpolicy = WritePolicy::default();
    let key = as_key!("test", SET, "secret");
    client.put(&wpolicy, &key, &[as_bin!("v", "encrypted in flight")]).await.unwrap();
    let rec = client.get(&ReadPolicy::default(), &key, Bins::All).await.unwrap();
    println!("round trip: {:?}", rec.bins.get("v"));

    let _ = client.delete(&wpolicy, &key).await;
    client.close().await.unwrap();
}
