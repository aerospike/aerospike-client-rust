// Copyright 2015-2025 Aerospike, Inc.
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

use aerospike::*;

use env_logger;

use crate::common;
use aerospike::{Privilege, PrivilegeCode, Role, User};
use aerospike_rt::sleep;
use aerospike_rt::time::Duration;

#[aerospike_macro::test]
async fn user_management() {
    const USER_NAME: &str = "test_user";
    const ROLE: &str = "user-admin";

    let _ = env_logger::try_init();

    let client = common::client().await;

    // drop the user if it potentially exists
    let _ = client.drop_user(USER_NAME).await;

    /* CREATE USER */
    client
        .create_user(USER_NAME, "something", &vec![ROLE])
        .await
        .unwrap();

    sleep(Duration::from_secs(1)).await;

    let users = client.query_users(None).await.unwrap();
    let user = users.iter().find(|u| u.user == USER_NAME).unwrap();
    assert_eq!(user.roles, vec![ROLE]);

    let users = client.query_users(Some(USER_NAME)).await.unwrap();
    let user = users.iter().find(|u| u.user == USER_NAME).unwrap();
    assert_eq!(user.roles, vec![ROLE]);

    /* GRANT ROLES */
    client.grant_roles(USER_NAME, &vec![ROLE]).await.unwrap();

    sleep(Duration::from_secs(1)).await;

    let users = client.query_users(Some(USER_NAME)).await.unwrap();
    let user = users.iter().find(|u| u.user == USER_NAME).unwrap();
    assert_eq!(user.roles, vec![ROLE]);

    /* REVOKE ROLES */
    client.revoke_roles(USER_NAME, &vec![ROLE]).await.unwrap();

    sleep(Duration::from_secs(1)).await;

    let users = client.query_users(Some(USER_NAME)).await.unwrap();
    let user = users.iter().find(|u| u.user == USER_NAME).unwrap();
    assert_eq!(user.roles.len(), 0);

    /* DROP USER */
    client.drop_user(USER_NAME).await.unwrap();

    sleep(Duration::from_secs(1)).await;

    let users = client.query_users(None).await.unwrap();
    let user = users.iter().find(|u| u.user == USER_NAME);
    assert_eq!(user.is_none(), true);
}

#[aerospike_macro::test]
async fn role_management() {
    let namespace: &str = common::namespace();
    let set_name = "test";

    const ROLE: &str = "test-role";

    let privileges = vec![Privilege::new(
        PrivilegeCode::Read,
        Some(namespace.into()),
        Some(set_name.into()),
    )];

    let _ = env_logger::try_init();

    let client = common::client().await;

    let _ = client.drop_role(ROLE).await;
    sleep(Duration::from_secs(1)).await;

    /* CREATE ROLE */
    client
        .create_role(ROLE, &privileges, &vec![], 1000, 5000)
        .await
        .unwrap();

    sleep(Duration::from_secs(1)).await;

    let roles = client.query_roles(None).await.unwrap();
    let role = roles.iter().find(|r| r.name == ROLE).unwrap();
    assert_eq!(role.privileges, privileges);
    assert_eq!(role.allowlist.len(), 0);
    assert_eq!(role.read_quota, 1000);
    assert_eq!(role.write_quota, 5000);

    let wpriv = Privilege::new(
        PrivilegeCode::Write,
        Some(namespace.into()),
        Some(set_name.into()),
    );

    /* GRANT PRIVILEGES */
    client
        .grant_privileges(ROLE, &vec![wpriv.clone()])
        .await
        .unwrap();

    sleep(Duration::from_secs(1)).await;

    let roles = client.query_roles(None).await.unwrap();
    let role = roles.iter().find(|r| r.name == ROLE).unwrap();
    assert_eq!(role.privileges, vec![privileges[0].clone(), wpriv.clone()]);
    assert_eq!(role.allowlist.len(), 0);
    assert_eq!(role.read_quota, 1000);
    assert_eq!(role.write_quota, 5000);

    /* REVOKE PRIVILEGES */
    client
        .revoke_privileges(ROLE, &vec![wpriv.clone()])
        .await
        .unwrap();

    sleep(Duration::from_secs(1)).await;

    let roles = client.query_roles(None).await.unwrap();
    let role = roles.iter().find(|r| r.name == ROLE).unwrap();
    assert_eq!(role.privileges, privileges);
    assert_eq!(role.allowlist.len(), 0);
    assert_eq!(role.read_quota, 1000);
    assert_eq!(role.write_quota, 5000);

    /* REVOKE PRIVILEGES */
    client.drop_role(ROLE).await.unwrap();
    sleep(Duration::from_secs(1)).await;

    let roles = client.query_roles(None).await.unwrap();
    let role = roles.iter().find(|r| r.name == ROLE);
    assert_eq!(role.is_none(), true)
}
