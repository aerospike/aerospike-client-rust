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

use std::convert::TryFrom;
use std::fmt;

use crate::errors::Error;

/// Default privileges defined on the server.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd)]
pub enum PrivilegeCode {
    /// User can edit/remove other users. Global scope only.
    UserAdmin = 0,

    /// User can perform systems administration functions on a database that do not involve user
    /// administration. Examples include server configuration.
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

    /// User can perform data masking administration actions.
    /// Global scope only.
    MaskingAdmin = 15,

    /// User can read masked data only.
    ReadMasked = 16,

    /// User can write masked data only.
    WriteMasked = 17,
}

impl PrivilegeCode {
    pub(crate) fn can_scope(&self) -> bool {
        *self >= Self::Read
    }
}

impl fmt::Display for PrivilegeCode {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", String::from(self))
    }
}

/// Privilege determines user access granularity.
#[derive(Clone, Debug, PartialEq, Eq)]
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
    pub const fn new(
        code: PrivilegeCode,
        namespace: Option<String>,
        set_name: Option<String>,
    ) -> Self {
        Privilege {
            code,
            namespace,
            set_name,
        }
    }
}

impl TryFrom<u8> for PrivilegeCode {
    type Error = Error;
    fn try_from(pc: u8) -> std::result::Result<Self, Self::Error> {
        match pc {
            0 => Ok(PrivilegeCode::UserAdmin),
            1 => Ok(PrivilegeCode::SysAdmin),
            2 => Ok(PrivilegeCode::DataAdmin),
            3 => Ok(PrivilegeCode::UDFAdmin),
            4 => Ok(PrivilegeCode::SIndexAdmin),
            10 => Ok(PrivilegeCode::Read),
            11 => Ok(PrivilegeCode::ReadWrite),
            12 => Ok(PrivilegeCode::ReadWriteUDF),
            13 => Ok(PrivilegeCode::Write),
            14 => Ok(PrivilegeCode::Truncate),
            15 => Ok(PrivilegeCode::MaskingAdmin),
            16 => Ok(PrivilegeCode::ReadMasked),
            17 => Ok(PrivilegeCode::WriteMasked),
            _ => Err(Error::BadResponse(format!("invalid privilege code {pc}"))),
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
            PrivilegeCode::MaskingAdmin => 15,
            PrivilegeCode::ReadMasked => 16,
            PrivilegeCode::WriteMasked => 17,
        }
    }
}

impl From<&PrivilegeCode> for String {
    fn from(pc: &PrivilegeCode) -> String {
        match *pc {
            PrivilegeCode::UserAdmin => "user-admin".into(),
            PrivilegeCode::SysAdmin => "sys-admin".into(),
            PrivilegeCode::DataAdmin => "data-admin".into(),
            PrivilegeCode::UDFAdmin => "udf-admin".into(),
            PrivilegeCode::SIndexAdmin => "sindex-admin".into(),
            PrivilegeCode::Read => "read".into(),
            PrivilegeCode::ReadWrite => "read-write".into(),
            PrivilegeCode::ReadWriteUDF => "read-write-udf".into(),
            PrivilegeCode::Write => "write".into(),
            PrivilegeCode::Truncate => "truncate".into(),
            PrivilegeCode::MaskingAdmin => "masking-admin".into(),
            PrivilegeCode::ReadMasked => "read-masked".into(),
            PrivilegeCode::WriteMasked => "write-masked".into(),
        }
    }
}

impl TryFrom<&str> for PrivilegeCode {
    type Error = Error;
    fn try_from(pc: &str) -> std::result::Result<Self, Self::Error> {
        match pc {
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
            "masking-admin" => Ok(PrivilegeCode::MaskingAdmin),
            "read-masked" => Ok(PrivilegeCode::ReadMasked),
            "write-masked" => Ok(PrivilegeCode::WriteMasked),
            _ => Err(Error::BadResponse(format!("invalid privilege code {pc}"))),
        }
    }
}
