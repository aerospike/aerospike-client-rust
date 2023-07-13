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

use std::str;

use pwhash::bcrypt::{self, BcryptSetup, BcryptVariant};

use crate::cluster::Cluster;
use crate::errors::{ErrorKind, Result};
use crate::net::Connection;
use crate::net::PooledConnection;
use crate::privilege::{self, Privilege};
use crate::ResultCode;

// Commands
const AUTHENTICATE: u8 = 0;
const CREATE_USER: u8 = 1;
const DROP_USER: u8 = 2;
const SET_PASSWORD: u8 = 3;
const CHANGE_PASSWORD: u8 = 4;
const GRANT_ROLES: u8 = 5;
const REVOKE_ROLES: u8 = 6;
const REPLACE_ROLES: u8 = 7;
const QUERY_USERS: u8 = 9;
const CREATE_ROLE: u8 = 10;
const DROP_ROLE: u8 = 11;
const GRANT_PRIVILEGES: u8 = 12;
const REVOKE_PRIVILEGES: u8 = 13;
const SET_WHITELIST: u8 = 14;
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
const ALLOW_LIST: u8 = 13;
const READ_QUOTA: u8 = 14;
const WRITE_QUOTA: u8 = 15;
const READ_INFO: u8 = 16;
const WRITE_INFO: u8 = 17;
const CONNECTIONS: u8 = 18;

// Misc
const MSG_VERSION: i64 = 0;
const MSG_TYPE: i64 = 2;

const HEADER_SIZE: usize = 24;
const HEADER_REMAINING: usize = 16;
const RESULT_CODE: usize = 9;
const QUERY_END: usize = 50;

pub struct AdminCommand {}

impl AdminCommand {
    pub const fn new() -> Self {
        AdminCommand {}
    }

    async fn execute(mut conn: PooledConnection) -> Result<()> {
        // Write the message header
        conn.buffer.size_buffer()?;
        let size = conn.buffer.data_offset;
        conn.buffer.reset_offset();
        AdminCommand::write_size(&mut conn, size as i64);

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
            bail!(ErrorKind::ServerError(result_code));
        }

        Ok(())
    }

    pub async fn authenticate(conn: &mut Connection, user: &str, password: &str) -> Result<()> {
        conn.buffer.resize_buffer(1024)?;
        conn.buffer.reset_offset();
        AdminCommand::write_header(conn, LOGIN, 2);
        AdminCommand::write_field_str(conn, USER, user);
        AdminCommand::write_field_bytes(conn, CREDENTIAL, password.as_bytes());
        conn.buffer.size_buffer()?;
        let size = conn.buffer.data_offset;
        conn.buffer.reset_offset();
        AdminCommand::write_size(conn, size as i64);

        conn.flush().await?;
        conn.read_buffer(HEADER_SIZE).await?;
        let result_code = conn.buffer.read_u8(Some(RESULT_CODE));
        let result_code = ResultCode::from(result_code);
        if ResultCode::SecurityNotEnabled != result_code && ResultCode::Ok != result_code {
            bail!(ErrorKind::ServerError(result_code));
        }

        // consume the rest of the buffer
        let sz = conn.buffer.read_u64(Some(0));
        let receive_size = (sz & 0xFFFF_FFFF_FFFF) - HEADER_REMAINING as u64;
        conn.read_buffer(receive_size as usize).await?;

        Ok(())
    }

    pub async fn create_user(
        cluster: &Cluster,
        user: &str,
        password: &str,
        roles: &[&str],
    ) -> Result<()> {
        let node = cluster.get_random_node().await?;
        let mut conn = node.get_connection().await?;

        conn.buffer.resize_buffer(1024)?;
        conn.buffer.reset_offset();
        AdminCommand::write_header(&mut conn, CREATE_USER, 3);
        AdminCommand::write_field_str(&mut conn, USER, user);
        AdminCommand::write_field_str(&mut conn, PASSWORD, &AdminCommand::hash_password(password)?);
        AdminCommand::write_roles(&mut conn, roles);

        AdminCommand::execute(conn).await
    }

    pub async fn drop_user(cluster: &Cluster, user: &str) -> Result<()> {
        let node = cluster.get_random_node().await?;
        let mut conn = node.get_connection().await?;

        conn.buffer.resize_buffer(1024)?;
        conn.buffer.reset_offset();
        AdminCommand::write_header(&mut conn, DROP_USER, 1);
        AdminCommand::write_field_str(&mut conn, USER, user);

        AdminCommand::execute(conn).await
    }

    pub async fn set_password(cluster: &Cluster, user: &str, password: &str) -> Result<()> {
        let node = cluster.get_random_node().await?;
        let mut conn = node.get_connection().await?;

        conn.buffer.resize_buffer(1024)?;
        conn.buffer.reset_offset();
        AdminCommand::write_header(&mut conn, SET_PASSWORD, 2);
        AdminCommand::write_field_str(&mut conn, USER, user);
        AdminCommand::write_field_str(&mut conn, PASSWORD, &AdminCommand::hash_password(password)?);

        AdminCommand::execute(conn).await
    }

    pub async fn change_password(cluster: &Cluster, user: &str, password: &str) -> Result<()> {
        let node = cluster.get_random_node().await?;
        let mut conn = node.get_connection().await?;

        conn.buffer.resize_buffer(1024)?;
        conn.buffer.reset_offset();
        AdminCommand::write_header(&mut conn, CHANGE_PASSWORD, 3);
        AdminCommand::write_field_str(&mut conn, USER, user);
        match cluster.client_policy().user_password {
            Some((_, ref password)) => {
                AdminCommand::write_field_str(
                    &mut conn,
                    OLD_PASSWORD,
                    &AdminCommand::hash_password(password)?,
                );
            }

            None => AdminCommand::write_field_str(&mut conn, OLD_PASSWORD, ""),
        };

        AdminCommand::write_field_str(&mut conn, PASSWORD, &AdminCommand::hash_password(password)?);

        AdminCommand::execute(conn).await
    }

    pub async fn grant_roles(cluster: &Cluster, user: &str, roles: &[&str]) -> Result<()> {
        let node = cluster.get_random_node().await?;
        let mut conn = node.get_connection().await?;

        conn.buffer.resize_buffer(1024)?;
        conn.buffer.reset_offset();
        AdminCommand::write_header(&mut conn, GRANT_ROLES, 2);
        AdminCommand::write_field_str(&mut conn, USER, user);
        AdminCommand::write_roles(&mut conn, roles);

        AdminCommand::execute(conn).await
    }

    pub async fn revoke_roles(cluster: &Cluster, user: &str, roles: &[&str]) -> Result<()> {
        let node = cluster.get_random_node().await?;
        let mut conn = node.get_connection().await?;

        conn.buffer.resize_buffer(1024)?;
        conn.buffer.reset_offset();
        AdminCommand::write_header(&mut conn, REVOKE_ROLES, 2);
        AdminCommand::write_field_str(&mut conn, USER, user);
        AdminCommand::write_roles(&mut conn, roles);

        AdminCommand::execute(conn).await
    }

    // pub async fn create_role(
    //     cluster: &Cluster,
    //     user: &str,
    //     role_name: &str,
    //     privileges: &[Privilege],
    //     allow_list: &[&str],
    //     read_quota: Option<u32>,
    //     write_quota: Option<u32>,
    // ) -> Result<()> {
    //     let node = cluster.get_random_node().await?;
    //     let mut conn = node.get_connection().await?;

    //     let mut field_count = 1;
    //     if privileges.len() > 1 {
    //         field_count += 1;
    //     }

    //     if allow_list.len() > 1 {
    //         field_count += 1;
    //     }

    //     match read_quota {
    //         Some(rq) if rq > 0 => field_count += 1,
    //         _ => (),
    //     }

    //     match write_quota {
    //         Some(wq) if wq > 0 => field_count += 1,
    //         _ => (),
    //     }

    //     conn.buffer.resize_buffer(1024)?;
    //     conn.buffer.reset_offset();

    //     AdminCommand::write_header(&mut conn, CREATE_ROLE, field_count);
    //     AdminCommand::write_field_str(&mut conn, ROLE, role_name);

    //     if privileges.len() > 0 {
    //         AdminCommand::write_privileges(&mut conn, privileges);
    //     }

    //     if allow_list.len() > 0 {
    //         AdminCommand::write_allow_list(&mut conn, allow_list);
    //     }

    //     match read_quota {
    //         Some(rq) if rq > 0 => AdminCommand::write_field_u32(&mut conn, READ_QUOTA, rq),
    //         _ => (),
    //     }

    //     match write_quota {
    //         Some(wq) if wq > 0 => AdminCommand::write_field_u32(&mut conn, WRITE_QUOTA, wq),
    //         _ => (),
    //     }

    //     AdminCommand::execute(conn).await
    // }

    // Utility methods

    fn write_size(conn: &mut Connection, size: i64) {
        // Write total size of message which is the current offset.
        let size = (size - 8) | (MSG_VERSION << 56) | (MSG_TYPE << 48);
        conn.buffer.write_i64(size);
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

    fn write_field_bytes(conn: &mut Connection, id: u8, b: &[u8]) {
        AdminCommand::write_field_header(conn, id, b.len());
        conn.buffer.write_bytes(b);
    }

    fn write_field_u32(conn: &mut Connection, id: u8, v: u32) {
        AdminCommand::write_field_header(conn, id, 4);
        conn.buffer.write_u32(v);
    }

    fn write_roles(conn: &mut Connection, roles: &[&str]) {
        let mut size = 0;
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

    fn write_allow_list(conn: &mut Connection, l: &[&str]) {
        let mut size = 0;
        l.iter().enumerate().for_each(|(i, item)| {
            size += item.len(); // size + len
            if i > 0 {
                size += 1; // comma
            }
        });

        AdminCommand::write_field_header(conn, ALLOW_LIST, size);
        l.iter().enumerate().for_each(|(i, item)| {
            conn.buffer.write_str(item);
            conn.buffer.write_str(",");
        });
    }

    // fn write_privileges(conn: &mut Connection, p: &[Privilege]) -> Result<()> {
    //     let mut size = 0;

    //     p.iter().enumerate().for_each(|(i, item)| {
    //         conn.buffer.write_u8(item as u8);
    //         if item.can_scope() {
    //             match (item.set_name, item.namespace) {
    //                 (Some(set_name), None) if set_name.len() > 0 => bail!(format!(
    //                     "Admin privilege {} has a set scope with an empty namespace",
    //                     item
    //                 )),
    //             }
    //         }
    //     });

    //     AdminCommand::write_field_header(conn, ALLOW_LIST, size);
    //     p.iter().enumerate().for_each(|(i, item)| {
    //         conn.buffer.write_str(item);
    //         conn.buffer.write_str(",");
    //     });
    // }

    pub fn hash_password(password: &str) -> Result<String> {
        bcrypt::hash_with(
            BcryptSetup {
                salt: Some("7EqJtq98hPqEX7fNZaFWoO"),
                cost: Some(10),
                variant: Some(BcryptVariant::V2a),
            },
            &password,
        )
        .map_err(|e| e.into())
    }
}
