// Copyright 2015-2023 Aerospike, Inc.
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

//! Fundamental privileges used for user management in Roles.

use std::convert::TryFrom;

/// Pre-defined user roles.
#[derive(Debug)]
pub enum PrivilegeCode {
    /// Allows to manages users and their roles.
    UserAdmin = 0,

    /// Allows to manage indexes, user defined functions and server configuration.
    SysAdmin = 1,

    /// Allows to manage indicies and user defined functions.
    DataAdmin = 2,

    /// Allows to manage user defined functions.
    UDFAdmin = 3,

    /// Allows to manage indicies.
    SIndexAdmin = 4,

    /// Allows read, write and UDF transactions with the database.
    ReadWriteUDF = 10,

    /// Allows read and write transactions with the database.
    ReadWrite = 11,

    /// Allows read transactions with the database.
    Read = 12,

    /// Allows write transactions with the database.
    Write = 13,

    /// Allow issuing truncate commands.
    Truncate = 14,
}

impl TryFrom<PrivilegeCode> for &str {
    type Error = String;
    fn try_from(val: PrivilegeCode) -> std::result::Result<Self, Self::Error> {
        match val {
            PrivilegeCode::UserAdmin => Ok("user-admin"),
            PrivilegeCode::SysAdmin => Ok("sys-admin"),
            PrivilegeCode::DataAdmin => Ok("data-admin"),
            PrivilegeCode::UDFAdmin => Ok("udf-admin"),
            PrivilegeCode::SIndexAdmin => Ok("sindex-admin"),
            PrivilegeCode::Read => Ok("read"),
            PrivilegeCode::ReadWrite => Ok("read-write"),
            PrivilegeCode::ReadWriteUDF => Ok("read-write-udf"),
            PrivilegeCode::Write => Ok("write"),
            PrivilegeCode::Truncate => Ok("truncate"),
        }
    }
}

impl TryFrom<&str> for PrivilegeCode {
    type Error = String;
    fn try_from(val: &str) -> std::result::Result<Self, Self::Error> {
        match val {
            "user-admin" => Ok(PrivilegeCode::UserAdmin),
            "sys-admin" => Ok(PrivilegeCode::SysAdmin),
            "data-admin" => Ok(PrivilegeCode::DataAdmin),
            "udf-admin" => Ok(PrivilegeCode::UDFAdmin),
            "sindex-admin" => Ok(PrivilegeCode::SIndexAdmin),
            "read" => Ok(PrivilegeCode::Read),
            "read-write" => Ok(PrivilegeCode::ReadWrite),
            "read-write-udf" => Ok(PrivilegeCode::ReadWriteUDF),
            "write" => Ok(PrivilegeCode::Write),
            "truncate" => Ok(PrivilegeCode::Truncate),
            _ => bail!(format!(
                "Invalid type conversion from {} to {}",
                val,
                std::any::type_name::<Self>()
            )),
        }
    }
}
