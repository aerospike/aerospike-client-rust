// Copyright 2015-2018 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(dead_code)]

use std::convert::TryInto;
use std::str;

use pwhash::bcrypt::{self, BcryptSetup, BcryptVariant};

use crate::cluster::Cluster;
use crate::errors::{Error, Result};
use crate::net::Connection;
use crate::net::PooledConnection;
use crate::policy::AuthMode;
use crate::ResultCode;
use crate::Role;
use crate::User;
use crate::{AdminPolicy, Privilege};

// Commands
const AUTHENTICATE: u8 = 0;
const CREATE_USER: u8 = 1;
const DROP_USER: u8 = 2;
const SET_PASSWORD: u8 = 3;
const CHANGE_PASSWORD: u8 = 4;
const GRANT_ROLES: u8 = 5;
const REVOKE_ROLES: u8 = 6;
const QUERY_USERS: u8 = 9;
const CREATE_ROLE: u8 = 10;
const DROP_ROLE: u8 = 11;
const GRANT_PRIVILEGES: u8 = 12;
const REVOKE_PRIVILEGES: u8 = 13;
const SET_ALLOWLIST: u8 = 14;
const SET_QUOTAS: u8 = 15;
const QUERY_ROLES: u8 = 16;
const LOGIN: u8 = 20;

// Field IDs
const USER: u8 = 0;
const PASSWORD: u8 = 1;
const OLD_PASSWORD: u8 = 2;
const CREDENTIAL: u8 = 3;
const CLEAR_PASSWORD: u8 = 4;
const SESSION_TOKEN: u8 = 5;
const SESSION_TTL: u8 = 6;
const ROLES: u8 = 10;
const ROLE: u8 = 11;
const PRIVILEGES: u8 = 12;
const WHITELIST: u8 = 13;
const READ_QUOTA: u8 = 14;
const WRITE_QUOTA: u8 = 15;
const READ_INFO: u8 = 16;
const WRITE_INFO: u8 = 17;
const CONNECTIONS: u8 = 18;

// Misc
const MSG_VERSION: i64 = 2;
const MSG_TYPE: i64 = 2;

const HEADER_SIZE: usize = 24;
const HEADER_REMAINING: usize = 16;
const RESULT_CODE: usize = 9;
const FIELD_COUNT_OFFSET: usize = 11;
const QUERY_END: usize = 50;

/// Session credentials returned by a successful `LOGIN`. Mirrors Java's
/// `LoginCommand.{sessionToken, sessionExpiration}` — subsequent
/// connections can authenticate via `AUTHENTICATE` with this token instead
/// of re-running the full login round-trip.
#[derive(Debug, Clone)]
pub struct SessionInfo {
    pub token: Vec<u8>,
    /// Wall-clock instant (monotonic) at which the token must be considered
    /// expired. We pre-subtract 60 s from the server-reported TTL so the
    /// client retires its copy a minute before the server does (matches
    /// Java's `LoginCommand.login` rule).
    pub expiration: aerospike_rt::time::Instant,
}

impl SessionInfo {
    pub fn is_expired(&self) -> bool {
        aerospike_rt::time::Instant::now() >= self.expiration
    }
}

pub struct AdminCommand {}

impl AdminCommand {
    pub const fn new() -> Self {
        AdminCommand {}
    }

    async fn execute(policy: &AdminPolicy, mut conn: PooledConnection) -> Result<()> {
        // Write the message header
        conn.buffer.size_buffer()?;
        let size = conn.buffer.data_offset;
        conn.buffer.reset_offset();
        AdminCommand::write_size(&mut conn, size as i64);

        conn.set_socket_timeout(None, policy.timeout());

        // Send command.
        if let Err(err) = conn.flush().await {
            conn.invalidate();
            return Err(err);
        }

        // read header
        if let Err(err) = conn.read_buffer(HEADER_SIZE).await {
            conn.invalidate();
            return Err(err);
        }

        let result_code = conn.buffer.read_u8(Some(RESULT_CODE));
        let result_code = ResultCode::from(result_code);
        if result_code != ResultCode::Ok {
            return Err(Error::ServerError(result_code, false, conn.addr.clone()));
        }

        conn.reset_state();

        Ok(())
    }

    async fn read_users(policy: &AdminPolicy, mut conn: PooledConnection) -> Result<Vec<User>> {
        // Write the message header
        conn.buffer.size_buffer()?;
        conn.set_socket_timeout(None, policy.timeout());
        let size = conn.buffer.data_offset;
        conn.buffer.reset_offset();
        AdminCommand::write_size(&mut conn, size as i64);

        // Send command.
        if let Err(err) = conn.flush().await {
            conn.invalidate();
            return Err(err);
        }

        let res = AdminCommand::read_user_blocks(&mut conn).await;
        if let Err(err) = res {
            conn.invalidate();
            return Err(err);
        }

        conn.reset_state();
        res
    }

    async fn read_user_blocks(conn: &mut Connection) -> Result<Vec<User>> {
        let mut users = vec![];

        let mut status = 0;
        while status == 0 {
            conn.read_buffer(8).await?;

            let sz = conn.buffer.read_u64(None);
            let receive_size = sz & 0xFFFF_FFFF_FFFF;
            if receive_size > 0 {
                conn.read_buffer(receive_size as usize).await?;

                let (res_status, mut list) = AdminCommand::parse_users(conn, receive_size)?;
                users.append(&mut list);
                status = res_status;
            } else {
                break;
            }
        }

        Ok(users)
    }

    fn parse_users(conn: &mut Connection, receive_size: u64) -> Result<(i8, Vec<User>)> {
        let mut users = vec![];

        while (conn.buffer.data_offset as u64) < receive_size {
            conn.buffer.skip(1);
            let result_code = conn.buffer.read_u8(None);
            if result_code != 0 {
                if result_code == QUERY_END as u8 {
                    return Ok((-1, users));
                }
                return Err(Error::ServerError(
                    ResultCode::from(result_code),
                    false,
                    conn.addr.clone(),
                ));
            }

            let mut user_name = String::new();
            let mut roles = vec![];
            let mut read_info = vec![];
            let mut write_info = vec![];
            let mut conns_in_use = 0;

            conn.buffer.skip(1);
            let field_count = conn.buffer.read_u8(None);
            conn.buffer.skip(HEADER_REMAINING - 4);
            for _ in 0..field_count {
                let len = conn.buffer.read_u32(None) as usize - 1;
                let id = conn.buffer.read_u8(None);
                match id {
                    USER => user_name = conn.buffer.read_str(len)?,
                    ROLES => roles = AdminCommand::parse_roles(conn)?,
                    READ_INFO => read_info = AdminCommand::parse_info(conn),
                    WRITE_INFO => write_info = AdminCommand::parse_info(conn),
                    CONNECTIONS => conns_in_use = conn.buffer.read_u32(None),
                    _ => conn.buffer.data_offset += len,
                }
            }

            if user_name.is_empty() && roles.is_empty() {
                continue;
            }

            let user = User {
                user: user_name,
                roles,
                read_info,
                write_info,
                conns_in_use,
            };

            users.push(user);
        }

        Ok((0, users))
    }

    async fn read_roles(policy: &AdminPolicy, mut conn: PooledConnection) -> Result<Vec<Role>> {
        // Write the message header
        conn.buffer.size_buffer()?;
        conn.set_socket_timeout(None, policy.timeout());
        let size = conn.buffer.data_offset();
        conn.buffer.reset_offset();
        AdminCommand::write_size(&mut conn, size as i64);

        // Send command.
        if let Err(err) = conn.flush().await {
            conn.invalidate();
            return Err(err);
        }

        let res = AdminCommand::read_role_blocks(&mut conn).await;
        if let Err(err) = res {
            conn.invalidate();
            return Err(err);
        }

        conn.reset_state();

        res
    }

    async fn read_role_blocks(conn: &mut Connection) -> Result<Vec<Role>> {
        let mut roles = vec![];

        let mut status = 0;
        while status == 0 {
            conn.read_buffer(8).await?;

            let sz = conn.buffer.read_u64(None);
            let receive_size = sz & 0xFFFF_FFFF_FFFF;
            if receive_size > 0 {
                conn.read_buffer(receive_size as usize).await?;
                let (res_status, mut list) = AdminCommand::parse_roles_full(conn, receive_size)?;
                roles.append(&mut list);
                status = res_status;
            } else {
                break;
            }
        }
        Ok(roles)
    }

    fn parse_roles_full(conn: &mut Connection, receive_size: u64) -> Result<(i8, Vec<Role>)> {
        let mut roles = vec![];

        while (conn.buffer.data_offset as u64) < receive_size {
            conn.buffer.skip(1);
            let result_code = conn.buffer.read_u8(None);

            if result_code != 0 {
                if result_code == QUERY_END as u8 {
                    return Ok((-1, roles));
                }
                return Err(Error::ServerError(
                    ResultCode::from(result_code),
                    false,
                    conn.addr.clone(),
                ));
            }

            let mut name = String::new();
            let mut privileges = vec![];
            let mut allowlist = vec![];
            let mut read_quota = 0;
            let mut write_quota = 0;

            conn.buffer.skip(1);
            let field_count = conn.buffer.read_u8(None);
            conn.buffer.skip(HEADER_REMAINING - 4);
            for _ in 0..field_count {
                let len = conn.buffer.read_u32(None) as usize - 1;
                let id = conn.buffer.read_u8(None);
                match id {
                    ROLE => name = conn.buffer.read_str(len)?,
                    PRIVILEGES => privileges = AdminCommand::parse_privileges(conn)?,
                    WHITELIST => allowlist = AdminCommand::parse_allowlist(conn, len)?,
                    READ_QUOTA => read_quota = conn.buffer.read_u32(None),
                    WRITE_QUOTA => write_quota = conn.buffer.read_u32(None),
                    _ => conn.buffer.data_offset += len,
                }
            }

            if name.is_empty() && privileges.is_empty() {
                continue;
            }

            let role = Role {
                name,
                privileges,
                allowlist,
                read_quota,
                write_quota,
            };

            roles.push(role);
        }

        Ok((0, roles))
    }

    pub(crate) fn parse_roles(conn: &mut Connection) -> Result<Vec<String>> {
        let mut roles = vec![];

        let size = conn.buffer.read_u8(None);
        for _ in 0..size {
            let len = conn.buffer.read_u8(None) as usize;
            let role = conn.buffer.read_str(len)?;
            roles.push(role);
        }

        Ok(roles)
    }

    pub(crate) fn parse_privileges(conn: &mut Connection) -> Result<Vec<Privilege>> {
        let mut privileges = vec![];

        let size = conn.buffer.read_u8(None);
        for _ in 0..size {
            let code = conn.buffer.read_u8(None).try_into()?;
            let mut privilege = Privilege {
                code,
                namespace: None,
                set_name: None,
            };

            if privilege.code.can_scope() {
                let len = conn.buffer.read_u8(None) as usize;
                privilege.namespace = Some(conn.buffer.read_str(len)?);
                let len = conn.buffer.read_u8(None) as usize;
                privilege.set_name = Some(conn.buffer.read_str(len)?);
            }

            privileges.push(privilege);
        }

        Ok(privileges)
    }

    pub(crate) fn parse_allowlist(conn: &mut Connection, length: usize) -> Result<Vec<String>> {
        let mut list = vec![];
        let max = conn.buffer.data_offset() + length;

        while conn.buffer.data_offset() < max {
            let item = conn.buffer.read_str_until(b',', max)?;
            list.push(item);
        }

        Ok(list)
    }

    pub(crate) fn parse_info(conn: &mut Connection) -> Vec<u32> {
        let size = conn.buffer.read_u8(None) as usize;
        let mut list = Vec::with_capacity(size);

        for _ in 0..size {
            let val = conn.buffer.read_u32(None);
            list.push(val);
        }

        list
    }

    /// Run a `LOGIN` round-trip and, on success, return the server-issued
    /// session token + expiration. Subsequent connections in the same
    /// pool can then use [`authenticate_session`](Self::authenticate_session)
    /// to skip the full credential exchange. Returns `None` only when
    /// `auth_mode` is `None` (security disabled at the policy level) or
    /// when the server reports `SecurityNotEnabled`.
    pub(crate) async fn authenticate(
        conn: &mut Connection,
        auth_mode: &AuthMode,
        hashed_pass: Option<&String>,
    ) -> Result<Option<SessionInfo>> {
        conn.buffer.resize_buffer(1024)?;
        conn.buffer.reset_offset();
        match auth_mode {
            AuthMode::Internal(ref user, _) => {
                AdminCommand::write_header(conn, LOGIN, 2);
                AdminCommand::write_field_str(conn, USER, user);
                AdminCommand::write_field_bytes(conn, CREDENTIAL, hashed_pass.unwrap().as_bytes());
            }
            AuthMode::External(ref user, ref password) => {
                AdminCommand::write_header(conn, LOGIN, 3);
                AdminCommand::write_field_str(conn, USER, user);
                AdminCommand::write_field_bytes(conn, CREDENTIAL, hashed_pass.unwrap().as_bytes());
                AdminCommand::write_field_str(conn, CLEAR_PASSWORD, password);
            }
            AuthMode::PKI => AdminCommand::write_header(conn, LOGIN, 0),
            AuthMode::None => return Ok(None),
        }

        conn.buffer.size_buffer()?;
        let size = conn.buffer.data_offset;
        conn.buffer.reset_offset();
        AdminCommand::write_size(conn, size as i64);

        conn.flush().await?;
        conn.read_buffer(HEADER_SIZE).await?;
        let result_code = conn.buffer.read_u8(Some(RESULT_CODE));
        let field_count = conn.buffer.read_u8(Some(FIELD_COUNT_OFFSET)) as usize;
        let result_code = ResultCode::from(result_code);
        if result_code == ResultCode::SecurityNotEnabled {
            // Drain any trailing body the server still queued, then bail —
            // there's no token to extract on a security-disabled cluster.
            let sz = conn.buffer.read_u64(Some(0));
            let receive_size = (sz & 0xFFFF_FFFF_FFFF) as i64 - HEADER_REMAINING as i64;
            if receive_size > 0 {
                conn.read_buffer(receive_size as usize).await?;
            }
            return Ok(None);
        }
        if result_code != ResultCode::Ok {
            return Err(Error::ServerError(result_code, false, conn.addr.clone()));
        }

        // Parse the body: each field is `len(u32) | id(u8) | bytes(len-1)`.
        let sz = conn.buffer.read_u64(Some(0));
        let receive_size = (sz & 0xFFFF_FFFF_FFFF) as i64 - HEADER_REMAINING as i64;
        if receive_size <= 0 || field_count == 0 {
            return Err(Error::ServerError(
                result_code,
                false,
                "Failed to retrieve session token".to_string(),
            ));
        }
        conn.read_buffer(receive_size as usize).await?;
        conn.buffer.reset_offset();

        let mut session_token: Option<Vec<u8>> = None;
        let mut session_expiration: Option<aerospike_rt::time::Instant> = None;
        for _ in 0..field_count {
            let raw_len = conn.buffer.read_u32(None) as usize;
            if raw_len == 0 {
                break;
            }
            let id = conn.buffer.read_u8(None);
            let len = raw_len - 1;
            match id {
                SESSION_TOKEN => {
                    let mut bytes = vec![0u8; len];
                    for byte in &mut bytes {
                        *byte = conn.buffer.read_u8(None);
                    }
                    session_token = Some(bytes);
                }
                SESSION_TTL => {
                    // 4-byte big-endian unsigned seconds. Subtract 60s so
                    // the client-side TTL expires just before the server
                    // does — same fudge factor as Java.
                    let seconds_raw = conn.buffer.read_u32(None) as i64;
                    let seconds = seconds_raw - 60;
                    if seconds > 0 {
                        session_expiration = Some(
                            aerospike_rt::time::Instant::now()
                                + std::time::Duration::from_secs(seconds as u64),
                        );
                    }
                    // Skip any remaining bytes from this field.
                    if len > 4 {
                        conn.buffer.data_offset += len - 4;
                    }
                }
                _ => {
                    conn.buffer.data_offset += len;
                }
            }
        }

        match (session_token, session_expiration) {
            (Some(token), Some(expiration)) => Ok(Some(SessionInfo { token, expiration })),
            // PKI / hashed_pass-only setups won't return a TTL on every
            // build; fall back to a short default so we at least retry
            // login soon rather than caching a permanent token.
            (Some(token), None) => Ok(Some(SessionInfo {
                token,
                expiration: aerospike_rt::time::Instant::now()
                    + std::time::Duration::from_secs(60 * 60), // 1h default
            })),
            _ => Err(Error::ServerError(
                result_code,
                false,
                "Failed to retrieve session token".to_string(),
            )),
        }
    }

    /// Run an `AUTHENTICATE` round-trip using a previously-obtained session
    /// token. Mirrors Java `AdminCommand.authenticate(cluster, conn,
    /// sessionToken)` — much cheaper than a fresh login because no
    /// credentials are exchanged. Returns `Ok(true)` on success, `Ok(false)`
    /// when the server rejects the token (caller should fall back to a
    /// full login), or `Err` on transport / parse errors.
    pub(crate) async fn authenticate_session(
        conn: &mut Connection,
        auth_mode: &AuthMode,
        token: &[u8],
    ) -> Result<bool> {
        conn.buffer.resize_buffer(1024)?;
        conn.buffer.reset_offset();
        match auth_mode {
            AuthMode::Internal(ref user, _) | AuthMode::External(ref user, _) => {
                AdminCommand::write_header(conn, AUTHENTICATE, 2);
                AdminCommand::write_field_str(conn, USER, user);
                AdminCommand::write_field_bytes(conn, SESSION_TOKEN, token);
            }
            AuthMode::PKI => {
                AdminCommand::write_header(conn, AUTHENTICATE, 1);
                AdminCommand::write_field_bytes(conn, SESSION_TOKEN, token);
            }
            AuthMode::None => return Ok(true),
        }

        conn.buffer.size_buffer()?;
        let size = conn.buffer.data_offset;
        conn.buffer.reset_offset();
        AdminCommand::write_size(conn, size as i64);

        conn.flush().await?;
        conn.read_buffer(HEADER_SIZE).await?;
        let result_code = conn.buffer.read_u8(Some(RESULT_CODE));
        let result_code = ResultCode::from(result_code);
        Ok(result_code == ResultCode::Ok || result_code == ResultCode::SecurityNotEnabled)
    }

    pub(crate) async fn create_user(
        policy: &AdminPolicy,
        cluster: &Cluster,
        user: &str,
        password: &str,
        roles: &[&str],
    ) -> Result<()> {
        let node = cluster.get_random_node()?;
        let mut conn = node.get_connection(0).await?;

        conn.buffer.resize_buffer(1024)?;
        conn.buffer.reset_offset();
        AdminCommand::write_header(&mut conn, CREATE_USER, 3);
        AdminCommand::write_field_str(&mut conn, USER, user);
        AdminCommand::write_field_str(&mut conn, PASSWORD, &AdminCommand::hash_password(password)?);
        AdminCommand::write_roles(&mut conn, roles);

        AdminCommand::execute(policy, conn).await
    }

    pub(crate) async fn drop_user(
        policy: &AdminPolicy,
        cluster: &Cluster,
        user: &str,
    ) -> Result<()> {
        let node = cluster.get_random_node()?;
        let mut conn = node.get_connection(0).await?;

        conn.buffer.resize_buffer(1024)?;
        conn.buffer.reset_offset();
        AdminCommand::write_header(&mut conn, DROP_USER, 1);
        AdminCommand::write_field_str(&mut conn, USER, user);

        AdminCommand::execute(policy, conn).await
    }

    pub(crate) async fn set_password(
        policy: &AdminPolicy,
        cluster: &Cluster,
        user: &str,
        password: &str,
    ) -> Result<()> {
        let node = cluster.get_random_node()?;
        let mut conn = node.get_connection(0).await?;

        conn.buffer.resize_buffer(1024)?;
        conn.buffer.reset_offset();
        AdminCommand::write_header(&mut conn, SET_PASSWORD, 2);
        AdminCommand::write_field_str(&mut conn, USER, user);
        AdminCommand::write_field_str(&mut conn, PASSWORD, &AdminCommand::hash_password(password)?);

        AdminCommand::execute(policy, conn).await
    }

    pub(crate) async fn change_password(
        policy: &AdminPolicy,
        cluster: &Cluster,
        user: &str,
        password: &str,
    ) -> Result<()> {
        let node = cluster.get_random_node()?;
        let mut conn = node.get_connection(0).await?;

        conn.buffer.resize_buffer(1024)?;
        conn.buffer.reset_offset();
        AdminCommand::write_header(&mut conn, CHANGE_PASSWORD, 3);
        AdminCommand::write_field_str(&mut conn, USER, user);
        match cluster.client_policy().auth_mode {
            AuthMode::Internal(_, ref password) | AuthMode::External(_, ref password) => {
                AdminCommand::write_field_str(
                    &mut conn,
                    OLD_PASSWORD,
                    &AdminCommand::hash_password(password)?,
                );
            }

            AuthMode::PKI => {
                return Err(Error::ClientError(
                    "Can't change PKI user's password".into(),
                ))
            }
            AuthMode::None => AdminCommand::write_field_str(&mut conn, OLD_PASSWORD, ""),
        }

        AdminCommand::write_field_str(&mut conn, PASSWORD, &AdminCommand::hash_password(password)?);

        AdminCommand::execute(policy, conn).await?;
        cluster.update_password(user, password)
    }

    pub(crate) async fn create_role(
        policy: &AdminPolicy,
        cluster: &Cluster,
        role_name: &str,
        privileges: &[Privilege],
        allowlist: &[&str],
        read_quota: u32,
        write_quota: u32,
    ) -> Result<()> {
        let node = cluster.get_random_node()?;
        let mut conn = node.get_connection(0).await?;

        let mut field_count = 1;
        if !privileges.is_empty() {
            field_count += 1;
        }

        if !allowlist.is_empty() {
            field_count += 1;
        }

        if read_quota > 0 {
            field_count += 1;
        }

        if write_quota > 0 {
            field_count += 1;
        }

        conn.buffer.resize_buffer(1024)?;
        conn.buffer.reset_offset();
        AdminCommand::write_header(&mut conn, CREATE_ROLE, field_count);
        AdminCommand::write_field_str(&mut conn, ROLE, role_name);

        if !privileges.is_empty() {
            AdminCommand::write_privileges(&mut conn, privileges)?;
        }

        if !allowlist.is_empty() {
            AdminCommand::write_allowlist(&mut conn, allowlist);
        }

        if read_quota > 0 {
            AdminCommand::write_field_u32(&mut conn, READ_QUOTA, read_quota);
        }

        if write_quota > 0 {
            AdminCommand::write_field_u32(&mut conn, WRITE_QUOTA, write_quota);
        }

        AdminCommand::execute(policy, conn).await
    }

    pub(crate) async fn drop_role(
        policy: &AdminPolicy,
        cluster: &Cluster,
        role_name: &str,
    ) -> Result<()> {
        let node = cluster.get_random_node()?;
        let mut conn = node.get_connection(0).await?;

        conn.buffer.resize_buffer(1024)?;
        conn.buffer.reset_offset();
        AdminCommand::write_header(&mut conn, DROP_ROLE, 1);
        AdminCommand::write_field_str(&mut conn, ROLE, role_name);

        AdminCommand::execute(policy, conn).await
    }

    pub(crate) async fn grant_privileges(
        policy: &AdminPolicy,
        cluster: &Cluster,
        role_name: &str,
        privileges: &[Privilege],
    ) -> Result<()> {
        let node = cluster.get_random_node()?;
        let mut conn = node.get_connection(0).await?;

        conn.buffer.resize_buffer(1024)?;
        conn.buffer.reset_offset();
        AdminCommand::write_header(&mut conn, GRANT_PRIVILEGES, 2);
        AdminCommand::write_field_str(&mut conn, ROLE, role_name);
        AdminCommand::write_privileges(&mut conn, privileges)?;

        AdminCommand::execute(policy, conn).await
    }

    pub(crate) async fn revoke_privileges(
        policy: &AdminPolicy,
        cluster: &Cluster,
        role_name: &str,
        privileges: &[Privilege],
    ) -> Result<()> {
        let node = cluster.get_random_node()?;
        let mut conn = node.get_connection(0).await?;

        conn.buffer.resize_buffer(1024)?;
        conn.buffer.reset_offset();
        AdminCommand::write_header(&mut conn, REVOKE_PRIVILEGES, 2);
        AdminCommand::write_field_str(&mut conn, ROLE, role_name);
        AdminCommand::write_privileges(&mut conn, privileges)?;

        AdminCommand::execute(policy, conn).await
    }

    pub(crate) async fn set_allowlist(
        policy: &AdminPolicy,
        cluster: &Cluster,
        role_name: &str,
        allowlist: &[&str],
    ) -> Result<()> {
        let node = cluster.get_random_node()?;
        let mut conn = node.get_connection(0).await?;

        conn.buffer.resize_buffer(1024)?;
        conn.buffer.reset_offset();
        AdminCommand::write_header(&mut conn, SET_ALLOWLIST, 2);
        AdminCommand::write_field_str(&mut conn, ROLE, role_name);
        AdminCommand::write_allowlist(&mut conn, allowlist);

        AdminCommand::execute(policy, conn).await
    }

    pub(crate) async fn set_quotas(
        policy: &AdminPolicy,
        cluster: &Cluster,
        role_name: &str,
        read_quota: u32,
        write_quota: u32,
    ) -> Result<()> {
        let node = cluster.get_random_node()?;
        let mut conn = node.get_connection(0).await?;

        conn.buffer.resize_buffer(1024)?;
        conn.buffer.reset_offset();
        AdminCommand::write_header(&mut conn, SET_QUOTAS, 3);
        AdminCommand::write_field_str(&mut conn, ROLE, role_name);
        AdminCommand::write_field_u32(&mut conn, READ_QUOTA, read_quota);
        AdminCommand::write_field_u32(&mut conn, WRITE_QUOTA, write_quota);

        AdminCommand::execute(policy, conn).await
    }

    pub(crate) async fn grant_roles(
        policy: &AdminPolicy,
        cluster: &Cluster,
        user: &str,
        roles: &[&str],
    ) -> Result<()> {
        let node = cluster.get_random_node()?;
        let mut conn = node.get_connection(0).await?;

        conn.buffer.resize_buffer(1024)?;
        conn.buffer.reset_offset();
        AdminCommand::write_header(&mut conn, GRANT_ROLES, 2);
        AdminCommand::write_field_str(&mut conn, USER, user);
        AdminCommand::write_roles(&mut conn, roles);

        AdminCommand::execute(policy, conn).await
    }

    pub(crate) async fn revoke_roles(
        policy: &AdminPolicy,
        cluster: &Cluster,
        user: &str,
        roles: &[&str],
    ) -> Result<()> {
        let node = cluster.get_random_node()?;
        let mut conn = node.get_connection(0).await?;

        conn.buffer.resize_buffer(1024)?;
        conn.buffer.reset_offset();
        AdminCommand::write_header(&mut conn, REVOKE_ROLES, 2);
        AdminCommand::write_field_str(&mut conn, USER, user);
        AdminCommand::write_roles(&mut conn, roles);

        AdminCommand::execute(policy, conn).await
    }

    pub(crate) async fn query_users(
        policy: &AdminPolicy,
        cluster: &Cluster,
        user: Option<&str>,
    ) -> Result<Vec<User>> {
        let node = cluster.get_random_node()?;
        let mut conn = node.get_connection(0).await?;

        conn.buffer.resize_buffer(1024)?;
        conn.buffer.reset_offset();

        if let Some(user) = user {
            AdminCommand::write_header(&mut conn, QUERY_USERS, 1);
            AdminCommand::write_field_str(&mut conn, USER, user);
        } else {
            AdminCommand::write_header(&mut conn, QUERY_USERS, 0);
        }

        AdminCommand::read_users(policy, conn).await
    }

    pub(crate) async fn query_roles(
        policy: &AdminPolicy,
        cluster: &Cluster,
        role: Option<&str>,
    ) -> Result<Vec<Role>> {
        let node = cluster.get_random_node()?;
        let mut conn = node.get_connection(0).await?;

        conn.buffer.resize_buffer(1024)?;
        conn.buffer.reset_offset();

        if let Some(role) = role {
            AdminCommand::write_header(&mut conn, QUERY_ROLES, 1);
            AdminCommand::write_field_str(&mut conn, ROLE, role);
        } else {
            AdminCommand::write_header(&mut conn, QUERY_ROLES, 0);
        }

        AdminCommand::read_roles(policy, conn).await
    }

    // Utility methods

    fn write_size(conn: &mut Connection, size: i64) {
        // Write total size of message which is the current offset.
        let size = (size - 8) | (MSG_VERSION << 56) | (MSG_TYPE << 48);
        conn.buffer.write_u64(size as u64);
    }

    fn write_header(conn: &mut Connection, command: u8, field_count: u8) {
        conn.buffer.data_offset = 8;
        conn.buffer.write_u8(0);
        conn.buffer.write_u8(0);
        conn.buffer.write_u8(command);
        conn.buffer.write_u8(field_count);

        // Authenticate header is almost all zeros
        for _ in 0..(16 - 4) {
            conn.buffer.write_u8(0);
        }
    }

    fn write_field_header(conn: &mut Connection, id: u8, size: usize) {
        conn.buffer.write_u32(size as u32 + 1);
        conn.buffer.write_u8(id);
    }

    fn write_field_str(conn: &mut Connection, id: u8, s: &str) {
        AdminCommand::write_field_header(conn, id, s.len());
        conn.buffer.write_str(s);
    }

    fn write_field_u32(conn: &mut Connection, id: u8, v: u32) {
        AdminCommand::write_field_header(conn, id, 4);
        conn.buffer.write_u32(v);
    }

    fn write_field_bytes(conn: &mut Connection, id: u8, b: &[u8]) {
        AdminCommand::write_field_header(conn, id, b.len());
        conn.buffer.write_bytes(b);
    }

    fn write_roles(conn: &mut Connection, roles: &[&str]) {
        let mut size = 1;
        for role in roles {
            size += role.len() + 1; // size + len
        }

        AdminCommand::write_field_header(conn, ROLES, size);
        conn.buffer.write_u8(roles.len() as u8);
        for role in roles {
            conn.buffer.write_u8(role.len() as u8);
            conn.buffer.write_str(role);
        }
    }

    fn write_privileges(conn: &mut Connection, privileges: &[Privilege]) -> Result<()> {
        let mut size = 1; // privileges.len()
        for prev in privileges {
            let code = &prev.code;
            size += 1; // code
            if code.can_scope() {
                if prev.set_name.as_ref().map_or(0, |s| s.trim().len()) > 0
                    && prev.namespace.as_ref().map_or(0, |s| s.trim().len()) == 0
                {
                    return Err(Error::ClientError(format!(
                        "admin privilege '{code}' has a set scope with an empty namespace."
                    )));
                }

                // Calculate size: namespace first, then set_name (matching Java client order)
                // Always include both fields (even if empty) - server expects them
                size += 1; // namespace length byte (always present)
                if let Some(ref namespace) = prev.namespace {
                    size += namespace.len();
                }
                size += 1; // set_name length byte (always present)
                if let Some(ref set_name) = prev.set_name {
                    size += set_name.len();
                }
            } else if prev.namespace.as_ref().map_or(0, std::string::String::len)
                + prev.set_name.as_ref().map_or(0, std::string::String::len)
                > 0
            {
                return Err(Error::ClientError(format!(
                    "admin global privilege '{code}' can't have a namespace or set"
                )));
            }
        }

        AdminCommand::write_field_header(conn, PRIVILEGES, size);
        conn.buffer.write_u8(privileges.len() as u8);

        for prev in privileges {
            let code = &prev.code;
            conn.buffer.write_u8(u8::from(code));
            if code.can_scope() {
                // Write namespace first, then set_name (matching Java client order)
                // Always write both fields (even if empty) - server expects them
                if let Some(ref namespace) = prev.namespace {
                    conn.buffer.write_u8(namespace.len() as u8);
                    conn.buffer.write_str(namespace);
                } else {
                    conn.buffer.write_u8(0); // Empty namespace
                }
                if let Some(ref set_name) = prev.set_name {
                    conn.buffer.write_u8(set_name.len() as u8);
                    conn.buffer.write_str(set_name);
                } else {
                    conn.buffer.write_u8(0); // Empty set_name
                }
            }
        }
        Ok(())
    }

    fn write_allowlist(conn: &mut Connection, allowlist: &[&str]) {
        let mut size = 1; // privileges.len()
        let mut comma = false;
        for address in allowlist {
            if comma {
                size += 1;
            } else {
                comma = true;
            }
            size += address.len();
        }

        AdminCommand::write_field_header(conn, SET_ALLOWLIST, size);

        for address in allowlist {
            if comma {
                conn.buffer.write_u8(b',');
            } else {
                comma = true;
            }
            conn.buffer.write_str(address);
        }
    }

    /// Hash and return the password.
    pub fn hash_password(password: &str) -> Result<String> {
        bcrypt::hash_with(
            BcryptSetup {
                salt: Some("7EqJtq98hPqEX7fNZaFWoO"),
                cost: Some(10),
                variant: Some(BcryptVariant::V2a),
            },
            password,
        )
        .map_err(std::convert::Into::into)
    }
}
