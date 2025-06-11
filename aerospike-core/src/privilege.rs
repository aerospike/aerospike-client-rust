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

use std::fmt;

/// Default privileges defined on the server.
#[derive(Clone, Debug, PartialEq)]
pub enum PrivilegeCode {
    /// User can edit/remove other users.  Global scope only.
    UserAdmin = 0,

    /// User can perform systems administration functions on a database that do not involve user
    /// administration.  Examples include server configuration.
    /// Global scope only.
    SysAdmin = 1,

    /// User can perform UDF and SINDEX administration actions. Global scope only.
    DataAdmin = 2,

    /// User can perform user defined function(UDF) administration actions.
    /// Examples include create/drop UDF. Global scope only.
    /// Requires server version 6+
    UDFAdmin = 3,

    /// User can perform secondary index administration actions.
    /// Examples include create/drop index. Global scope only.
    /// Requires server version 6+
    SIndexAdmin = 4,

    /// User can read data only.
    Read = 10,

    /// User can read and write data.
    ReadWrite = 11,

    /// User can read and write data through user defined functions.
    ReadWriteUDF = 12,

    /// User can read and write data through user defined functions.
    Write = 13,

    /// User can truncate data only.
    /// Requires server version 6+
    Truncate = 14,
}

impl PrivilegeCode {
    pub(crate) fn can_scope(&self) -> bool {
        match self {
            PrivilegeCode::Read
            | PrivilegeCode::ReadWrite
            | PrivilegeCode::ReadWriteUDF
            | PrivilegeCode::Write
            | PrivilegeCode::Truncate => true,
            _ => false,
        }
    }
}

impl fmt::Display for PrivilegeCode {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", String::from(self))
    }
}

/// Privilege determines user access granularity.
#[derive(Clone, Debug, PartialEq)]
pub struct Privilege {
    /// Role
    pub code: PrivilegeCode,

    /// Namespace determines namespace scope. Apply permission to this namespace only.
    /// If namespace is None, the privilege applies to all namespaces.
    pub namespace: Option<String>,

    /// Set name scope. Apply permission to this set within namespace only.
    /// If set is None, the privilege applies to all sets within namespace.
    pub set_name: Option<String>,
}

impl Privilege {
    /// Initialize a new privilege
    pub fn new(code: PrivilegeCode, namespace: Option<String>, set_name: Option<String>) -> Self {
        Privilege {
            code: code,
            namespace: namespace,
            set_name: set_name,
        }
    }
}

impl From<u8> for PrivilegeCode {
    fn from(pc: u8) -> PrivilegeCode {
        match pc {
            0 => PrivilegeCode::UserAdmin,
            1 => PrivilegeCode::SysAdmin,
            2 => PrivilegeCode::DataAdmin,
            3 => PrivilegeCode::UDFAdmin,
            4 => PrivilegeCode::SIndexAdmin,
            10 => PrivilegeCode::Read,
            11 => PrivilegeCode::ReadWrite,
            12 => PrivilegeCode::ReadWriteUDF,
            13 => PrivilegeCode::Write,
            14 => PrivilegeCode::Truncate,
            _ => panic!("invalid privilege code {}", pc),
        }
    }
}

impl From<&PrivilegeCode> for u8 {
    fn from(pc: &PrivilegeCode) -> u8 {
        match pc {
            PrivilegeCode::UserAdmin => 0,
            PrivilegeCode::SysAdmin => 1,
            PrivilegeCode::DataAdmin => 2,
            PrivilegeCode::UDFAdmin => 3,
            PrivilegeCode::SIndexAdmin => 4,
            PrivilegeCode::Read => 10,
            PrivilegeCode::ReadWrite => 11,
            PrivilegeCode::ReadWriteUDF => 12,
            PrivilegeCode::Write => 13,
            PrivilegeCode::Truncate => 14,
        }
    }
}

impl From<&PrivilegeCode> for String {
    fn from(pc: &PrivilegeCode) -> String {
        match pc {
            &PrivilegeCode::UserAdmin => "user-admin".into(),
            &PrivilegeCode::SysAdmin => "sys-admin".into(),
            &PrivilegeCode::DataAdmin => "data-admin".into(),
            &PrivilegeCode::UDFAdmin => "udf-admin".into(),
            &PrivilegeCode::SIndexAdmin => "sindex-admin".into(),
            &PrivilegeCode::Read => "read".into(),
            &PrivilegeCode::ReadWrite => "read-write".into(),
            &PrivilegeCode::ReadWriteUDF => "read-write-udf".into(),
            &PrivilegeCode::Write => "write".into(),
            &PrivilegeCode::Truncate => "truncate".into(),
        }
    }
}

impl From<&str> for PrivilegeCode {
    fn from(pc: &str) -> PrivilegeCode {
        match pc {
            "user-admin" => PrivilegeCode::UserAdmin,
            "sys-admin" => PrivilegeCode::SysAdmin,
            "data-admin" => PrivilegeCode::DataAdmin,
            "udf-admin" => PrivilegeCode::UDFAdmin,
            "sindex-admin" => PrivilegeCode::SIndexAdmin,
            "read" => PrivilegeCode::Read,
            "read-write" => PrivilegeCode::ReadWrite,
            "read-write-udf" => PrivilegeCode::ReadWriteUDF,
            "write" => PrivilegeCode::Write,
            "truncate" => PrivilegeCode::Truncate,
            _ => panic!("invalid privilege code {}", pc),
        }
    }
}
