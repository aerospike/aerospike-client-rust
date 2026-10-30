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

//! Testing strategy row N-17 against the `cn-only` profile, `ee` only (Java
//! `TestL2CnOnlyCertificate`). The topology is N-6's, but the server
//! certificate carries the tls-name only as its subject CN, with no matching
//! dNSName SAN. In its own profile so that a failing N-17 does not fail the
//! other rows.

#[cfg(feature = "tls")]
use super::rows;
#[cfg(feature = "tls")]
use super::topology::{self as topo};
#[cfg(feature = "tls")]
use super::{capture, run, skip_unless};

/// A TLS policy trusting the CA the topology mounts at `AEROSPIKE_CA_CERT`.
#[cfg(feature = "tls")]
pub fn tls_policy() -> aerospike::TlsPolicy {
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::CertificateDer;

    let ca = &topo::t().ca_cert;
    assert!(!ca.is_empty(), "AEROSPIKE_CA_CERT is not set; the ee topology mounts the server CA");
    let mut roots = rustls::RootCertStore::empty();
    roots.add_parsable_certificates(
        CertificateDer::pem_file_iter(ca)
            .unwrap_or_else(|e| panic!("Cannot open CA file {ca}: {e}"))
            .map(|c| c.expect("CA certificate")),
    );
    aerospike::TlsPolicy::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    )
}

/// N-17: a certificate that carries the tls-name only as its subject CN is
/// rejected. The TLS handshake with the seed fails on the tls-name and no
/// node is created.
#[cfg(feature = "tls")]
#[test]
fn n17_cn_only_certificate_is_rejected() {
    run("N-17", || async {
        let t = topo::t();
        const PROFILE: &str = " (run_l2.sh DISCOVERY_SUBSET=ee L2_PROFILE=cn-only)";
        if !skip_unless(
            t.subset == "ee",
            &format!("N-17 is ee only: it needs server TLS and TLS listeners{PROFILE}; subset is {}", t.subset),
        ) {
            return;
        }
        if !skip_unless(
            t.tls_cert_shape == "cn-only",
            &format!(
                "N-17 needs DISCOVERY_TLS_CERT_SHAPE=cn-only, a server certificate with the tls-name only \
                 as its CN{PROFILE}; shape is {}",
                t.tls_cert_shape
            ),
        ) {
            return;
        }

        let nodes = topo::nodes().await;
        let first = &nodes[0];
        assert!(first.tls_port > 0, "No TLS listener for {first}");
        let tls_name = first.tls_name.clone().unwrap_or_else(|| panic!("No tls-name for {first}"));

        let mut policy = topo::policy();
        policy.tls_policy = Some(tls_policy());
        policy.address_translator = Some(rows::port_map(topo::tls_port_map(&nodes)));
        capture::clear();

        match aerospike::Client::new(&policy, &vec![aerospike::Host::new_tls(&t.host, &tls_name, first.tls_port)]).await
        {
            Ok(client) => {
                let names = topo::client_node_names(&client);
                client.close().await.expect("close");
                println!(
                    "L2-N17-ACCEPTED nodes={names:?} cnFallbackWarnings={:?}",
                    rows::cn_fallback_warnings()
                );
                panic!(
                    "N-17: TLS handshake accepted a certificate that carries {tls_name} only as its CN; nodes={names:?}"
                );
            }
            Err(e) => {
                let message = topo::messages(&e);
                println!("L2-N17-ERROR {message}");
                assert!(
                    message.contains("NotValidForName") || message.to_lowercase().contains("certificate"),
                    "{message}"
                );
            }
        }
    });
}
