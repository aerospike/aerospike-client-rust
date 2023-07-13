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

//! Privileges used for user management in Roles.

use crate::privilege_code::PrivilegeCode;

/// Privilege determines user access granularity.
#[derive(Debug)]
pub struct Privilege {
    /// Role
    pub code: PrivilegeCode,

    /// Namespace scope. Apply permission to this namespace only.
    /// If namespace is zero value, the privilege applies to all namespaces.
    pub namespace: Option<String>,

    /// Set name scope. Apply permission to this set within namespace only.
    /// If set is zero value, the privilege applies to all sets within namespace.
    pub set_name: Option<String>,
}

// impl Privilege {
//     pub(crate) fn can_scope(&self) -> bool {
//         (self.code as isize) >= (PrivilegeCode::ReadWriteUDF as isize)
//     }
// }
