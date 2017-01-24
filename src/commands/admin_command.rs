// Copyright 2015-2017 Aerospike, Inc.
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

use std::sync::Arc;
use std::str;

use pwhash::bcrypt;
use pwhash::bcrypt::{BcryptVariant, BcryptSetup};

use errors::*;
use ResultCode;
use cluster::{Node, Cluster};
use net::Connection;
use policy::AdminPolicy;

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

// Field IDs
const USER: u8 = 0;
const PASSWORD: u8 = 1;
const OLD_PASSWORD: u8 = 2;
const CREDENTIAL: u8 = 3;
const ROLES: u8 = 10;

// Misc
const MSG_VERSION: i64 = 0;
const MSG_TYPE: i64 = 2;

const HEADER_SIZE: usize = 24;
const HEADER_REMAINING: usize = 16;
const RESULT_CODE: usize = 9;
const QUERY_END: usize = 50;


pub struct AdminCommand {
}

impl AdminCommand {
    pub fn new() -> Self {
        AdminCommand {}
    }

    pub fn execute(node: Arc<Node>, mut conn: Connection) -> Result<()> {
        // Write the message header
        try!(conn.buffer.size_buffer());
        let size = conn.buffer.data_offset;
        try!(conn.buffer.reset_offset());
        try!(AdminCommand::write_size(&mut conn, size as i64));

        // Send command.
        if let Err(err) = conn.flush() {
            node.invalidate_connection(&mut conn);
            return Err(err);
        }

        // read header
        if let Err(err) = conn.read_buffer(HEADER_SIZE) {
            node.invalidate_connection(&mut conn);
            return Err(err);
        }

        let result_code = match conn.buffer.read_u8(Some(RESULT_CODE)) {
            Ok(v) => {
                node.put_connection(conn);
                v
            }
            Err(err) => {
                node.put_connection(conn);
                return Err(err);
            }
        };
        let result_code = ResultCode::from(result_code);

        if result_code != ResultCode::Ok {
            bail!(ErrorKind::ServerError(result_code));
        }

        Ok(())
    }

    pub fn authenticate(conn: &mut Connection, user: &str, password: &str) -> Result<()> {

        try!(AdminCommand::set_authenticate(conn, user, password));
        try!(conn.flush());
        try!(conn.read_buffer(HEADER_SIZE));
        let result_code = try!(conn.buffer.read_u8(Some(RESULT_CODE)));
        let result_code = ResultCode::from(result_code);
        if result_code != ResultCode::Ok {
            bail!(ErrorKind::ServerError(result_code));
        }

        Ok(())
    }

    fn set_authenticate(conn: &mut Connection, user: &str, password: &str) -> Result<()> {

        try!(conn.buffer.resize_buffer(1024));
        try!(conn.buffer.reset_offset());
        try!(AdminCommand::write_header(conn, AUTHENTICATE, 2));
        try!(AdminCommand::write_field_str(conn, USER, user));
        try!(AdminCommand::write_field_bytes(conn, CREDENTIAL, password.as_bytes()));
        try!(conn.buffer.size_buffer());
        let size = conn.buffer.data_offset;
        try!(conn.buffer.reset_offset());
        try!(AdminCommand::write_size(conn, size as i64));

        Ok(())
    }


    pub fn create_user(cluster: &Cluster,
                       policy: &AdminPolicy,
                       user: &str,
                       password: &str,
                       roles: &[&str])
                       -> Result<()> {

        let node = try!(cluster.get_random_node());
        let mut conn = try!(node.get_connection(Some(policy.timeout)));

        try!(conn.buffer.resize_buffer(1024));
        try!(conn.buffer.reset_offset());
        try!(AdminCommand::write_header(&mut conn, CREATE_USER, 3));
        try!(AdminCommand::write_field_str(&mut conn, USER, user));
        try!(AdminCommand::write_field_str(&mut conn,
                                           PASSWORD,
                                           &try!(AdminCommand::hash_password(password))));
        try!(AdminCommand::write_roles(&mut conn, roles));

        AdminCommand::execute(node, conn)
    }

    pub fn drop_user(cluster: &Cluster, policy: &AdminPolicy, user: &str) -> Result<()> {

        let node = try!(cluster.get_random_node());
        let mut conn = try!(node.get_connection(Some(policy.timeout)));

        try!(conn.buffer.resize_buffer(1024));
        try!(conn.buffer.reset_offset());
        try!(AdminCommand::write_header(&mut conn, DROP_USER, 1));
        try!(AdminCommand::write_field_str(&mut conn, USER, user));

        AdminCommand::execute(node, conn)
    }

    pub fn set_password(cluster: &Cluster,
                        policy: &AdminPolicy,
                        user: &str,
                        password: &str)
                        -> Result<()> {

        let node = try!(cluster.get_random_node());
        let mut conn = try!(node.get_connection(Some(policy.timeout)));

        try!(conn.buffer.resize_buffer(1024));
        try!(conn.buffer.reset_offset());
        try!(AdminCommand::write_header(&mut conn, SET_PASSWORD, 2));
        try!(AdminCommand::write_field_str(&mut conn, USER, user));
        try!(AdminCommand::write_field_str(&mut conn,
                                           PASSWORD,
                                           &try!(AdminCommand::hash_password(password))));

        AdminCommand::execute(node, conn)
    }

    pub fn change_password(cluster: &Cluster,
                           policy: &AdminPolicy,
                           user: &str,
                           password: &str)
                           -> Result<()> {

        let node = try!(cluster.get_random_node());
        let mut conn = try!(node.get_connection(Some(policy.timeout)));

        try!(conn.buffer.resize_buffer(1024));
        try!(conn.buffer.reset_offset());
        try!(AdminCommand::write_header(&mut conn, CHANGE_PASSWORD, 3));
        try!(AdminCommand::write_field_str(&mut conn, USER, user));
        match cluster.client_policy().user_password {
            Some((_, ref password)) => {
                try!(AdminCommand::write_field_str(&mut conn,
                                                   OLD_PASSWORD,
                                                   &try!(AdminCommand::hash_password(password))));
            }

            None => try!(AdminCommand::write_field_str(&mut conn, OLD_PASSWORD, "")),
        };

        try!(AdminCommand::write_field_str(&mut conn,
                                           PASSWORD,
                                           &try!(AdminCommand::hash_password(password))));

        AdminCommand::execute(node, conn)
    }

    pub fn grant_roles(cluster: &Cluster,
                       policy: &AdminPolicy,
                       user: &str,
                       roles: &[&str])
                       -> Result<()> {

        let node = try!(cluster.get_random_node());
        let mut conn = try!(node.get_connection(Some(policy.timeout)));

        try!(conn.buffer.resize_buffer(1024));
        try!(conn.buffer.reset_offset());
        try!(AdminCommand::write_header(&mut conn, GRANT_ROLES, 2));
        try!(AdminCommand::write_field_str(&mut conn, USER, user));
        try!(AdminCommand::write_roles(&mut conn, roles));

        AdminCommand::execute(node, conn)
    }

    pub fn revoke_roles(cluster: &Cluster,
                        policy: &AdminPolicy,
                        user: &str,
                        roles: &[&str])
                        -> Result<()> {

        let node = try!(cluster.get_random_node());
        let mut conn = try!(node.get_connection(Some(policy.timeout)));

        try!(conn.buffer.resize_buffer(1024));
        try!(conn.buffer.reset_offset());
        try!(AdminCommand::write_header(&mut conn, REVOKE_ROLES, 2));
        try!(AdminCommand::write_field_str(&mut conn, USER, user));
        try!(AdminCommand::write_roles(&mut conn, roles));

        AdminCommand::execute(node, conn)
    }

    // Utility methods

    fn write_size(conn: &mut Connection, size: i64) -> Result<()> {
        // Write total size of message which is the current offset.
        let size = (size - 8) | (MSG_VERSION << 56) | (MSG_TYPE << 48);
        try!(conn.buffer.write_i64(size));

        Ok(())
    }

    fn write_header(conn: &mut Connection, command: u8, field_count: u8) -> Result<()> {
        conn.buffer.data_offset = 8;
        try!(conn.buffer.write_u8(0));
        try!(conn.buffer.write_u8(0));
        try!(conn.buffer.write_u8(command));
        try!(conn.buffer.write_u8(field_count));

        // Authenticate header is almost all zeros
        for _ in 0..(16 - 4) {
            try!(conn.buffer.write_u8(0));
        }

        Ok(())
    }

    fn write_field_header(conn: &mut Connection, id: u8, size: usize) -> Result<()> {
        try!(conn.buffer.write_u32(size as u32 + 1));
        try!(conn.buffer.write_u8(id));
        Ok(())
    }

    fn write_field_str(conn: &mut Connection, id: u8, s: &str) -> Result<()> {
        try!(AdminCommand::write_field_header(conn, id, s.len()));
        try!(conn.buffer.write_str(s));
        Ok(())
    }

    fn write_field_bytes(conn: &mut Connection, id: u8, b: &[u8]) -> Result<()> {
        try!(AdminCommand::write_field_header(conn, id, b.len()));
        try!(conn.buffer.write_bytes(b));
        Ok(())
    }

    fn write_roles(conn: &mut Connection, roles: &[&str]) -> Result<()> {
        let mut size = 0;
        for role in roles {
            size += role.len() + 1; // size + len
        }

        try!(AdminCommand::write_field_header(conn, ROLES, size));
        try!(conn.buffer.write_u8(roles.len() as u8));
        for role in roles {
            try!(conn.buffer.write_u8(role.len() as u8));
            try!(conn.buffer.write_str(role));
        }

        Ok(())
    }

    pub fn hash_password(password: &str) -> Result<String> {
        let password_hash = try!(bcrypt::hash_with(BcryptSetup {
                                                       salt: Some("7EqJtq98hPqEX7fNZaFWoO"),
                                                       cost: Some(10),
                                                       variant: Some(BcryptVariant::V2a),
                                                   },
                                                   &password));
        Ok(password_hash)
    }
}
