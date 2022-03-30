// Copyright 2015-2018 Aerospike, Inc.
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

use std::fmt;
use std::result::Result as StdResult;

/// Database operation error codes. The error codes are defined in the server-side file proto.h.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ResultCode {
    /// OperationType was successful.
    Ok,

    /// Unknown server failure.
    ServerError,

    /// On retrieving, touching or replacing a record that doesn't exist.
    KeyNotFoundError,

    /// On modifying a record with unexpected generation.
    GenerationError,

    /// Bad parameter(s) were passed in database operation call.
    ParameterError,

    /// On create-only (write unique) operations on a record that already exists.
    KeyExistsError,

    /// On create-only (write unique) operations on a bin that already exists.
    BinExistsError,

    /// Expected cluster Id was not received.
    ClusterKeyMismatch,

    /// Server has run out of memory.
    ServerMemError,

    /// Client or server has timed out.
    Timeout,

    /// Xds product is not available.
    NoXds,

    /// Server is not accepting requests.
    ServerNotAvailable,

    /// OperationType is not supported with configured bin type (single-bin or multi-bin).
    BinTypeError,

    /// Record size exceeds limit.
    RecordTooBig,

    /// Too many concurrent operations on the same record.
    KeyBusy,

    /// Scan aborted by server.
    ScanAbort,

    /// Unsupported Server Feature (e.g. Scan + Udf)
    UnsupportedFeature,

    /// Specified bin name does not exist in record.
    BinNotFound,

    /// Specified bin name does not exist in record.
    DeviceOverload,

    /// Key type mismatch.
    KeyMismatch,

    /// Invalid namespace.
    InvalidNamespace,

    /// Bin name length greater than 14 characters.
    BinNameTooLong,

    /// OperationType not allowed at this time.
    FailForbidden,

    /// Returned by Map put and put_items operations when policy is REPLACE but key was not found.
    ElementNotFound,

    /// Returned by Map put and put_items operations when policy is CREATE_ONLY but key already
    /// exists.
    ElementExists,

    /// Enterprise-only feature not supported by the community edition
    EnterpriseOnly,

    /// There are no more records left for query.
    QueryEnd,

    /// Security type not supported by connected server.
    SecurityNotSupported,

    /// Administration command is invalid.
    SecurityNotEnabled,

    /// Administration field is invalid.
    SecuritySchemeNotSupported,

    /// Administration command is invalid.
    InvalidCommand,

    /// Administration field is invalid.
    InvalidField,

    /// Security protocol not followed.
    IllegalState,

    /// User name is invalid.
    InvalidUser,

    /// User was previously created.
    UserAlreadyExists,

    /// Password is invalid.
    InvalidPassword,

    /// Security credential is invalid.
    ExpiredPassword,

    /// Forbidden password (e.g. recently used)
    ForbiddenPassword,

    /// Security credential is invalid.
    InvalidCredential,

    /// Role name is invalid.
    InvalidRole,

    /// Role already exists.
    RoleAlreadyExists,

    /// Privilege is invalid.
    InvalidPrivilege,

    /// User must be authentication before performing database operations.
    NotAuthenticated,

    /// User does not posses the required role to perform the database operation.
    RoleViolation,

    /// A user defined function returned an error code.
    UdfBadResponse,

    /// The requested item in a large collection was not found.
    LargeItemNotFound,

    /// Batch functionality has been disabled.
    BatchDisabled,

    /// Batch max requests have been exceeded.
    BatchMaxRequestsExceeded,

    /// All batch queues are full.
    BatchQueuesFull,

    /// Secondary index already exists.
    IndexFound,

    /// Requested secondary index does not exist.
    IndexNotFound,

    /// Secondary index memory space exceeded.
    IndexOom,

    /// Secondary index not available.
    IndexNotReadable,

    /// Generic secondary index error.
    IndexGeneric,

    /// Index name maximum length exceeded.
    IndexNameMaxLen,

    /// Maximum number of indicies exceeded.
    IndexMaxCount,

    /// Secondary index query aborted.
    QueryAborted,

    /// Secondary index queue full.
    QueryQueueFull,

    /// Secondary index query timed out on server.
    QueryTimeout,

    /// Generic query error.
    QueryGeneric,

    /// Query NetIo error on server
    QueryNetioErr,

    /// Duplicate TaskId sent for the statement
    QueryDuplicate,

    /// Unknown server result code
    Unknown(u8),
}

impl ResultCode {
    /// Convert the result code from the server response.
    #[doc(hidden)]
    pub const fn from_u8(n: u8) -> ResultCode {
        match n {
            0 => ResultCode::Ok,
            1 => ResultCode::ServerError,
            2 => ResultCode::KeyNotFoundError,
            3 => ResultCode::GenerationError,
            4 => ResultCode::ParameterError,
            5 => ResultCode::KeyExistsError,
            6 => ResultCode::BinExistsError,
            7 => ResultCode::ClusterKeyMismatch,
            8 => ResultCode::ServerMemError,
            9 => ResultCode::Timeout,
            10 => ResultCode::NoXds,
            11 => ResultCode::ServerNotAvailable,
            12 => ResultCode::BinTypeError,
            13 => ResultCode::RecordTooBig,
            14 => ResultCode::KeyBusy,
            15 => ResultCode::ScanAbort,
            16 => ResultCode::UnsupportedFeature,
            17 => ResultCode::BinNotFound,
            18 => ResultCode::DeviceOverload,
            19 => ResultCode::KeyMismatch,
            20 => ResultCode::InvalidNamespace,
            21 => ResultCode::BinNameTooLong,
            22 => ResultCode::FailForbidden,
            23 => ResultCode::ElementNotFound,
            24 => ResultCode::ElementExists,
            25 => ResultCode::EnterpriseOnly,
            50 => ResultCode::QueryEnd,
            51 => ResultCode::SecurityNotSupported,
            52 => ResultCode::SecurityNotEnabled,
            53 => ResultCode::SecuritySchemeNotSupported,
            54 => ResultCode::InvalidCommand,
            55 => ResultCode::InvalidField,
            56 => ResultCode::IllegalState,
            60 => ResultCode::InvalidUser,
            61 => ResultCode::UserAlreadyExists,
            62 => ResultCode::InvalidPassword,
            63 => ResultCode::ExpiredPassword,
            64 => ResultCode::ForbiddenPassword,
            65 => ResultCode::InvalidCredential,
            70 => ResultCode::InvalidRole,
            71 => ResultCode::RoleAlreadyExists,
            72 => ResultCode::InvalidPrivilege,
            80 => ResultCode::NotAuthenticated,
            81 => ResultCode::RoleViolation,
            100 => ResultCode::UdfBadResponse,
            125 => ResultCode::LargeItemNotFound,
            150 => ResultCode::BatchDisabled,
            151 => ResultCode::BatchMaxRequestsExceeded,
            152 => ResultCode::BatchQueuesFull,
            200 => ResultCode::IndexFound,
            201 => ResultCode::IndexNotFound,
            202 => ResultCode::IndexOom,
            203 => ResultCode::IndexNotReadable,
            204 => ResultCode::IndexGeneric,
            205 => ResultCode::IndexNameMaxLen,
            206 => ResultCode::IndexMaxCount,
            210 => ResultCode::QueryAborted,
            211 => ResultCode::QueryQueueFull,
            212 => ResultCode::QueryTimeout,
            213 => ResultCode::QueryGeneric,
            214 => ResultCode::QueryNetioErr,
            215 => ResultCode::QueryDuplicate,
            code => ResultCode::Unknown(code),
        }
    }

    /// Convert a result code into an string.
    pub fn into_string(self) -> String {
        match self {
            ResultCode::Ok => String::from("ok"),
            ResultCode::ServerError => String::from("Server error"),
            ResultCode::KeyNotFoundError => String::from("Key not found"),
            ResultCode::GenerationError => String::from("Generation error"),
            ResultCode::ParameterError => String::from("Parameter error"),
            ResultCode::KeyExistsError => String::from("Key already exists"),
            ResultCode::BinExistsError => String::from("Bin already exists"),
            ResultCode::ClusterKeyMismatch => String::from("Cluster key mismatch"),
            ResultCode::ServerMemError => String::from("Server memory error"),
            ResultCode::Timeout => String::from("Timeout"),
            ResultCode::NoXds => String::from("Xds not available"),
            ResultCode::ServerNotAvailable => String::from("Server not available"),
            ResultCode::BinTypeError => String::from("Bin type error"),
            ResultCode::RecordTooBig => String::from("Record too big"),
            ResultCode::KeyBusy => String::from("Hot key"),
            ResultCode::ScanAbort => String::from("Scan aborted"),
            ResultCode::UnsupportedFeature => String::from("Unsupported Server Feature"),
            ResultCode::BinNotFound => String::from("Bin not found"),
            ResultCode::DeviceOverload => String::from("Device overload"),
            ResultCode::KeyMismatch => String::from("Key mismatch"),
            ResultCode::InvalidNamespace => String::from("Namespace not found"),
            ResultCode::BinNameTooLong => {
                String::from("Bin name length greater than 14 characters")
            }
            ResultCode::FailForbidden => String::from("OperationType not allowed at this time"),
            ResultCode::ElementNotFound => String::from("Element not found"),
            ResultCode::ElementExists => String::from("Element already exists"),
            ResultCode::EnterpriseOnly => {
                String::from("Enterprise-only feature not supported by community edition")
            }
            ResultCode::QueryEnd => String::from("Query end"),
            ResultCode::SecurityNotSupported => String::from("Security not supported"),
            ResultCode::SecurityNotEnabled => String::from("Security not enabled"),
            ResultCode::SecuritySchemeNotSupported => String::from("Security scheme not supported"),
            ResultCode::InvalidCommand => String::from("Invalid command"),
            ResultCode::InvalidField => String::from("Invalid field"),
            ResultCode::IllegalState => String::from("Illegal state"),
            ResultCode::InvalidUser => String::from("Invalid user"),
            ResultCode::UserAlreadyExists => String::from("User already exists"),
            ResultCode::InvalidPassword => String::from("Invalid password"),
            ResultCode::ExpiredPassword => String::from("Expired password"),
            ResultCode::ForbiddenPassword => String::from("Forbidden password"),
            ResultCode::InvalidCredential => String::from("Invalid credential"),
            ResultCode::InvalidRole => String::from("Invalid role"),
            ResultCode::RoleAlreadyExists => String::from("Role already exists"),
            ResultCode::InvalidPrivilege => String::from("Invalid privilege"),
            ResultCode::NotAuthenticated => String::from("Not authenticated"),
            ResultCode::RoleViolation => String::from("Role violation"),
            ResultCode::UdfBadResponse => String::from("Udf returned error"),
            ResultCode::LargeItemNotFound => String::from("Large collection item not found"),
            ResultCode::BatchDisabled => String::from("Batch functionality has been disabled"),
            ResultCode::BatchMaxRequestsExceeded => {
                String::from("Batch max requests have been exceeded")
            }
            ResultCode::BatchQueuesFull => String::from("All batch queues are full"),
            ResultCode::IndexFound => String::from("Index already exists"),
            ResultCode::IndexNotFound => String::from("Index not found"),
            ResultCode::IndexOom => String::from("Index out of memory"),
            ResultCode::IndexNotReadable => String::from("Index not readable"),
            ResultCode::IndexGeneric => String::from("Index error"),
            ResultCode::IndexNameMaxLen => String::from("Index name max length exceeded"),
            ResultCode::IndexMaxCount => String::from("Index count exceeds max"),
            ResultCode::QueryAborted => String::from("Query aborted"),
            ResultCode::QueryQueueFull => String::from("Query queue full"),
            ResultCode::QueryTimeout => String::from("Query timeout"),
            ResultCode::QueryGeneric => String::from("Query error"),
            ResultCode::QueryNetioErr => String::from("Query NetIo error on server"),
            ResultCode::QueryDuplicate => String::from("Duplicate TaskId sent for the statement"),
            ResultCode::Unknown(code) => format!("Unknown server error code: {}", code),
        }
    }
}

impl From<u8> for ResultCode {
    fn from(val: u8) -> ResultCode {
        ResultCode::from_u8(val)
    }
}

impl From<ResultCode> for String {
    fn from(code: ResultCode) -> String {
        code.into_string()
    }
}

impl fmt::Display for ResultCode {
    fn fmt(&self, f: &mut fmt::Formatter) -> StdResult<(), fmt::Error> {
        write!(f, "{}", self.into_string())
    }
}

#[cfg(test)]
mod tests {
    use super::ResultCode;

    #[test]
    fn from_result_code() {
        assert_eq!(ResultCode::KeyNotFoundError, ResultCode::from(2u8));
    }

    #[test]
    fn from_unknown_result_code() {
        assert_eq!(ResultCode::Unknown(234), ResultCode::from(234u8));
    }

    #[test]
    fn into_string() {
        let result: String = ResultCode::KeyNotFoundError.into();
        assert_eq!("Key not found", result);
    }

    #[test]
    fn unknown_into_string() {
        let result: String = ResultCode::Unknown(234).into();
        assert_eq!("Unknown server error code: 234", result);
    }
}
