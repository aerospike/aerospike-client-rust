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

//! Security administration: users, roles, privileges, allowlists and quotas.
//!
//! On an Enterprise cluster with security enabled, a client authenticated as
//! an administrator can manage who may do what. Privileges are scoped: a
//! data privilege (`Read`, `ReadWrite`, `Write`, `Truncate`, …) can be
//! limited to a namespace or a set, while administrative ones (`UserAdmin`,
//! `SysAdmin`, …) always apply cluster-wide. A role bundles privileges with
//! an optional IP allowlist and read/write quotas (records per second,
//! 0 = unlimited). Users then get roles.
//!
//! Also here: an XDR filter expression, which tells a datacenter link which
//! records to ship. The example runs the call and reports the outcome, since
//! most development clusters have no XDR datacenter configured.
//!
//! The example connects with `AEROSPIKE_USER` / `AEROSPIKE_PASSWORD` when
//! they are set and skips when the cluster has no security enabled.
//!
//! ```bash
//! AEROSPIKE_USER=admin AEROSPIKE_PASSWORD=admin cargo run --example security
//! ```

use std::env;
use std::time::Duration;

use aerospike::expressions as exp;
use aerospike::{
    AdminPolicy, AuthMode, Client, ClientPolicy, Privilege, PrivilegeCode, ResultCode,
};

const USER: &str = "example_user";
const ROLE: &str = "example_role";

#[tokio::main]
async fn main() {
    run().await;
}

/// Example body. Standalone via `cargo run --example`, and also driven by
/// the integration test suite (`tests/src/examples.rs`).
pub async fn run() {
    let mut cpolicy = ClientPolicy::default();
    cpolicy.use_services_alternate = env::var("AEROSPIKE_USE_SERVICES_ALTERNATE")
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false);
    if let Ok(user) = env::var("AEROSPIKE_USER") {
        let password = env::var("AEROSPIKE_PASSWORD").unwrap_or_default();
        cpolicy.set_auth_mode(AuthMode::Internal(user, password));
    }
    let hosts = env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| String::from("127.0.0.1:3000"));
    let client = Client::new(&cpolicy, &hosts)
        .await
        .expect("Failed to connect to cluster");

    let apolicy = AdminPolicy::default();

    // ---- Is security on? ----
    match client.query_users(&apolicy, None).await {
        Ok(users) => println!("security is enabled; {} users defined", users.len()),
        Err(e) if e.matches(&[ResultCode::SecurityNotEnabled, ResultCode::SecurityNotSupported]) => {
            println!("Security is not enabled on this cluster ({e}); skipping.");
            client.close().await.unwrap();
            return;
        }
        Err(e) => panic!("query_users failed: {e}"),
    }

    // Start clean: these may linger from an earlier interrupted run.
    let _ = client.drop_user(&apolicy, USER).await;
    let _ = client.drop_user(&apolicy, "example_pki_user").await;
    let _ = client.drop_role(&apolicy, ROLE).await;

    // ---- A role: read-write on one set, read everywhere in the namespace ----
    let privileges = [
        Privilege {
            code: PrivilegeCode::ReadWrite,
            namespace: Some("test".into()),
            set_name: Some("orders".into()),
        },
        Privilege {
            code: PrivilegeCode::Read,
            namespace: Some("test".into()),
            set_name: None,
        },
    ];
    // No allowlist, 1000 reads/s, 100 writes/s.
    client
        .create_role(&apolicy, ROLE, &privileges, &[], 1_000, 100)
        .await
        .unwrap();
    println!("created role {ROLE}");

    // Add a privilege later, take one away, and tighten the quotas.
    client
        .grant_privileges(
            &apolicy,
            ROLE,
            &[Privilege { code: PrivilegeCode::Truncate, namespace: Some("test".into()), set_name: Some("orders".into()) }],
        )
        .await
        .unwrap();
    client
        .revoke_privileges(
            &apolicy,
            ROLE,
            &[Privilege { code: PrivilegeCode::Read, namespace: Some("test".into()), set_name: None }],
        )
        .await
        .unwrap();
    client.set_quotas(&apolicy, ROLE, 500, 50).await.unwrap();
    // Restrict the role to a network; an empty list clears the restriction.
    client.set_allowlist(&apolicy, ROLE, &["10.0.0.0/8", "192.168.1.0/24"]).await.unwrap();

    // ---- A user with that role ----
    client
        .create_user(&apolicy, USER, "s3cret-pa55word", &[ROLE])
        .await
        .unwrap();
    client.change_password(&apolicy, USER, "even-m0re-secret").await.unwrap();
    client.grant_roles(&apolicy, USER, &["read"]).await.unwrap();
    client.revoke_roles(&apolicy, USER, &["read"]).await.unwrap();
    println!("created user {USER}");

    // A PKI user has no password: it authenticates with a client certificate
    // whose subject name is the user name (`AuthMode::Pki` over TLS).
    match client.create_pki_user(&apolicy, "example_pki_user", &[ROLE]).await {
        Ok(()) => println!("created PKI user example_pki_user"),
        Err(e) => println!("PKI users not available here: {e}"),
    }

    // ---- Read it all back ----
    // Security changes propagate through the cluster asynchronously.
    aerospike_rt::sleep(Duration::from_secs(1)).await;
    for role in client.query_roles(&apolicy, Some(ROLE)).await.unwrap() {
        println!("role {}:", role.name);
        for p in &role.privileges {
            println!("  {:?} on {:?} / {:?}", p.code, p.namespace, p.set_name);
        }
        println!("  allowlist {:?}", role.allowlist);
        println!("  quotas read {} / write {} per second", role.read_quota, role.write_quota);
    }
    for user in client.query_users(&apolicy, Some(USER)).await.unwrap() {
        println!("user {} has roles {:?}", user.user, user.roles);
    }

    // ---- XDR filter: ship only orders over 1000 to datacenter `dc1` ----
    // Pass `None` to clear a filter. The server rejects an unknown DC.
    let filter = exp::gt(exp::int_bin("amount"), exp::int_val(1000));
    match client.set_xdr_filter(&apolicy, "dc1", "test", Some(&filter)).await {
        Ok(()) => println!("XDR filter set on dc1"),
        Err(e) => println!("XDR filter not applied (no such datacenter here?): {e}"),
    }

    // ---- Clean up ----
    let _ = client.drop_user(&apolicy, "example_pki_user").await;
    client.drop_user(&apolicy, USER).await.unwrap();
    client.drop_role(&apolicy, ROLE).await.unwrap();
    println!("dropped user and role");

    client.close().await.unwrap();
}
