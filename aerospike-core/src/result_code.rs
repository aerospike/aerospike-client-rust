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

#[cfg(feature = "serialization")]
use serde::Serialize;

/// Key and bin names used in batch read commands where variable bins are needed for each key.
#[cfg_attr(feature = "serialization", derive(Serialize))]
/// Database operation error codes. The error codes are defined in the server-side file proto.h.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ResultCode {
    /// `OperationType` was successful.
    Ok = 0,

    /// Unknown server failure.
    ServerError = 1,

    /// On retrieving, touching or replacing a record that doesn't exist.
    KeyNotFoundError = 2,

    /// On modifying a record with unexpected generation.
    GenerationError = 3,

    /// Bad parameter(s) were passed in database operation call.
    ParameterError = 4,

    /// On create-only (write unique) operations on a record that already exists.
    KeyExistsError = 5,

    /// On create-only (write unique) operations on a bin that already exists.
    BinExistsError = 6,

    /// Expected cluster Id was not received.
    ClusterKeyMismatch = 7,

    /// Server has run out of memory.
    ServerMemError = 8,

    /// Client or server has timed out.
    Timeout = 9,

    /// Operation not allowed in current configuration.
    AlwaysForbidden = 10,

    /// Partition is unavailable.
    PartitionUnavailable = 11,

    /// Operation type is not supported with configured bin type (single-bin or multi-bin).
    BinTypeError = 12,

    /// Record size exceeds limit.
    RecordTooBig = 13,

    /// Too many concurrent operations on the same record.
    KeyBusy = 14,

    /// Scan aborted by server.
    ScanAbort = 15,

    /// Unsupported Server Feature (e.g. Scan + Udf)
    UnsupportedFeature = 16,

    /// Specified bin name does not exist in record.
    BinNotFound = 17,

    /// Specified bin name does not exist in record.
    DeviceOverload = 18,

    /// Key type mismatch.
    KeyMismatch = 19,

    /// Invalid namespace.
    InvalidNamespace = 20,

    /// Bin name length greater than 14 characters.
    BinNameTooLong = 21,

    /// `OperationType` not allowed at this time.
    FailForbidden = 22,

    /// Returned by Map put and `put_items` operations when policy is REPLACE but key was not found.
    ElementNotFound = 23,

    /// Returned by Map put and `put_items` operations when policy is `CREATE_ONLY` but key already
    /// exists.
    ElementExists = 24,

    /// Enterprise-only feature not supported by the community edition.
    EnterpriseOnly = 25,

    /// The operation cannot be applied to the current bin value on the server.
    OpNotApplicable = 26,

    /// The command was not performed because the filter was false.
    FilteredOut = 27,

    /// Write command loses conflict to XDR.
    LostConflict = 28,

    /// Write can't complete until XDR finishes shipping.
    XDRKeyBusy = 32,

    /// There are no more records left for query.
    QueryEnd = 50,

    /// Security type not supported by connected server.
    SecurityNotSupported = 51,

    /// Administration command is invalid.
    SecurityNotEnabled = 52,

    /// Administration field is invalid.
    SecuritySchemeNotSupported = 53,

    /// Administration command is invalid.
    InvalidCommand = 54,

    /// Administration field is invalid.
    InvalidField = 55,

    /// Security protocol not followed.
    IllegalState = 56,

    /// User name is invalid.
    InvalidUser = 60,

    /// User was previously created.
    UserAlreadyExists = 61,

    /// Password is invalid.
    InvalidPassword = 62,

    /// Security credential is invalid.
    ExpiredPassword = 63,

    /// Forbidden password (e.g. recently used)
    ForbiddenPassword = 64,

    /// Security credential is invalid.
    InvalidCredential = 65,

    /// Login session expired.
    ExpiredSession = 66,

    /// Role name is invalid.
    InvalidRole = 70,

    /// Role already exists.
    RoleAlreadyExists = 71,

    /// Privilege is invalid.
    InvalidPrivilege = 72,

    /// Invalid IP address allowlist.
    InvalidAllowlist = 73,

    /// Quotas not enabled on server.
    QuotasNotEnabled = 74,

    /// Invalid quota value.
    InvalidQuota = 75,

    /// User must be authentication before performing database operations.
    NotAuthenticated = 80,

    /// User does not posses the required role to perform the database operation.
    RoleViolation = 81,

    /// Command not allowed because sender IP address not allowlisted.
    NotAllowlisted = 82,

    /// Quota exceeded.
    QuotaExceeded = 83,

    /// A user defined function returned an error code.
    UdfBadResponse = 100,

    /// Transaction record blocked by a different transaction.
    MrtBlocked = 120,

    /// Transaction read version mismatch identified during commit.
    /// Some other command changed the record outside of the transaction.
    MrtVersionMismatch = 121,

    /// Transaction deadline reached without a successful commit or abort.
    MrtExpired = 122,

    /// Transaction write command limit (4096) exceeded.
    MrtTooManyWrites = 123,

    /// Transaction was already committed.
    MrtCommitted = 124,

    /// Transaction was already aborted.
    MrtAborted = 125,

    /// This record has been locked by a previous update in this transaction.
    MrtAlreadyLocked = 126,

    /// This transaction has already started. Writing to the same transaction with independent goroutines is unsafe.
    MrtMonitorExists = 127,

    /// Batch functionality has been disabled.
    BatchDisabled = 150,

    /// Batch max requests have been exceeded.
    BatchMaxRequestsExceeded = 151,

    /// All batch queues are full.
    BatchQueuesFull = 152,

    /// Invalid `GeoJSON` on insert/update
    InvalidGeojson = 160,

    /// Secondary index already exists.
    IndexFound = 200,

    /// Requested secondary index does not exist.
    IndexNotFound = 201,

    /// Secondary index memory space exceeded.
    IndexOom = 202,

    /// Secondary index not available.
    IndexNotReadable = 203,

    /// Generic secondary index error.
    IndexGeneric = 204,

    /// Index name maximum length exceeded.
    IndexNameMaxLen = 205,

    /// Maximum number of indicies exceeded.
    IndexMaxCount = 206,

    /// Secondary index query aborted.
    QueryAborted = 210,

    /// Secondary index queue full.
    QueryQueueFull = 211,

    /// Secondary index query timed out on server.
    QueryTimeout = 212,

    /// Generic query error.
    QueryGeneric = 213,

    /// Query `NetIo` error on server
    QueryNetioErr = 214,

    /// Duplicate `TaskId` sent for the statement
    QueryDuplicate = 215,

    /// Unknown server result code
    Unknown(u8),
}

impl ResultCode {
    /// Convert the result code from the server response.
    pub(crate) const fn from_u8(n: u8) -> ResultCode {
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
            10 => ResultCode::AlwaysForbidden,
            11 => ResultCode::PartitionUnavailable,
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
            26 => ResultCode::OpNotApplicable,
            27 => ResultCode::FilteredOut,
            28 => ResultCode::LostConflict,
            32 => ResultCode::XDRKeyBusy,
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
            66 => ResultCode::ExpiredSession,
            70 => ResultCode::InvalidRole,
            71 => ResultCode::RoleAlreadyExists,
            72 => ResultCode::InvalidPrivilege,
            73 => ResultCode::InvalidAllowlist,
            74 => ResultCode::QuotasNotEnabled,
            75 => ResultCode::InvalidQuota,
            80 => ResultCode::NotAuthenticated,
            81 => ResultCode::RoleViolation,
            82 => ResultCode::NotAllowlisted,
            83 => ResultCode::QuotaExceeded,
            100 => ResultCode::UdfBadResponse,
            120 => ResultCode::MrtBlocked,
            121 => ResultCode::MrtVersionMismatch,
            122 => ResultCode::MrtExpired,
            123 => ResultCode::MrtTooManyWrites,
            124 => ResultCode::MrtCommitted,
            125 => ResultCode::MrtAborted,
            126 => ResultCode::MrtAlreadyLocked,
            127 => ResultCode::MrtMonitorExists,
            150 => ResultCode::BatchDisabled,
            151 => ResultCode::BatchMaxRequestsExceeded,
            152 => ResultCode::BatchQueuesFull,
            160 => ResultCode::InvalidGeojson,
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
            ResultCode::AlwaysForbidden => String::from("Xds not available"),
            ResultCode::PartitionUnavailable => String::from("Server not available"),
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
            ResultCode::OpNotApplicable => String::from("Operation not applicable"),
            ResultCode::FilteredOut => String::from("Transaction filtered out"),
            ResultCode::LostConflict => String::from("Write command loses conflict to XDR"),
            ResultCode::XDRKeyBusy => {
                String::from("Write can't complete until XDR finishes shipping")
            }
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
            ResultCode::ExpiredSession => String::from("Login session expired"),
            ResultCode::InvalidRole => String::from("Invalid role"),
            ResultCode::RoleAlreadyExists => String::from("Role already exists"),
            ResultCode::InvalidPrivilege => String::from("Invalid privilege"),
            ResultCode::InvalidAllowlist => String::from("Invalid whitelist"),
            ResultCode::QuotasNotEnabled => String::from("Quotas not enabled"),
            ResultCode::InvalidQuota => String::from("Invalid quota"),
            ResultCode::NotAuthenticated => String::from("Not authenticated"),
            ResultCode::RoleViolation => String::from("Role violation"),
            ResultCode::NotAllowlisted => String::from("Command not whitelisted"),
            ResultCode::QuotaExceeded => String::from("Quota exceeded"),
            ResultCode::UdfBadResponse => String::from("Udf returned error"),
            ResultCode::MrtBlocked => String::from("Transaction record blocked by a different transaction"),
            ResultCode::MrtVersionMismatch => String::from("Transaction read version mismatch identified during commit. Some other command changed the record outside of the transaction"),
            ResultCode::MrtExpired => String::from("Transaction deadline reached without a successful commit or abort"),
            ResultCode::MrtTooManyWrites => String::from("Transaction write command limit (4096) exceeded"),
            ResultCode::MrtCommitted => String::from("Transaction was already committed"),
            ResultCode::MrtAborted => String::from("Transaction was already aborted"),
            ResultCode::MrtAlreadyLocked => String::from("This record has been locked by a previous update in this transaction"),
            ResultCode::MrtMonitorExists => String::from("This transaction has already started. Writing to the same transaction with independent goroutines is unsafe"),
            ResultCode::BatchDisabled => String::from("Batch functionality has been disabled"),
            ResultCode::BatchMaxRequestsExceeded => {
                String::from("Batch max requests have been exceeded")
            }
            ResultCode::BatchQueuesFull => String::from("All batch queues are full"),
            ResultCode::InvalidGeojson => String::from("Invalid GeoJSON on insert/update"),
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
            ResultCode::Unknown(code) => format!("Unknown server error code: {code}"),
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

impl From<ResultCode> for u8 {
    fn from(rc: ResultCode) -> u8 {
        match rc {
            ResultCode::Ok => 0,
            ResultCode::ServerError => 1,
            ResultCode::KeyNotFoundError => 2,
            ResultCode::GenerationError => 3,
            ResultCode::ParameterError => 4,
            ResultCode::KeyExistsError => 5,
            ResultCode::BinExistsError => 6,
            ResultCode::ClusterKeyMismatch => 7,
            ResultCode::ServerMemError => 8,
            ResultCode::Timeout => 9,
            ResultCode::AlwaysForbidden => 10,
            ResultCode::PartitionUnavailable => 11,
            ResultCode::BinTypeError => 12,
            ResultCode::RecordTooBig => 13,
            ResultCode::KeyBusy => 14,
            ResultCode::ScanAbort => 15,
            ResultCode::UnsupportedFeature => 16,
            ResultCode::BinNotFound => 17,
            ResultCode::DeviceOverload => 18,
            ResultCode::KeyMismatch => 19,
            ResultCode::InvalidNamespace => 20,
            ResultCode::BinNameTooLong => 21,
            ResultCode::FailForbidden => 22,
            ResultCode::ElementNotFound => 23,
            ResultCode::ElementExists => 24,
            ResultCode::OpNotApplicable => 26,
            ResultCode::FilteredOut => 27,
            ResultCode::LostConflict => 28,
            ResultCode::XDRKeyBusy => 32,
            ResultCode::EnterpriseOnly => 25,
            ResultCode::QueryEnd => 50,
            ResultCode::SecurityNotSupported => 51,
            ResultCode::SecurityNotEnabled => 52,
            ResultCode::SecuritySchemeNotSupported => 53,
            ResultCode::InvalidCommand => 54,
            ResultCode::InvalidField => 55,
            ResultCode::IllegalState => 56,
            ResultCode::InvalidUser => 60,
            ResultCode::UserAlreadyExists => 61,
            ResultCode::InvalidPassword => 62,
            ResultCode::ExpiredPassword => 63,
            ResultCode::ForbiddenPassword => 64,
            ResultCode::InvalidCredential => 65,
            ResultCode::ExpiredSession => 66,
            ResultCode::InvalidRole => 70,
            ResultCode::RoleAlreadyExists => 71,
            ResultCode::InvalidPrivilege => 72,
            ResultCode::InvalidAllowlist => 73,
            ResultCode::QuotasNotEnabled => 74,
            ResultCode::InvalidQuota => 75,
            ResultCode::NotAuthenticated => 80,
            ResultCode::RoleViolation => 81,
            ResultCode::NotAllowlisted => 82,
            ResultCode::QuotaExceeded => 83,
            ResultCode::UdfBadResponse => 100,
            ResultCode::MrtBlocked => 120,
            ResultCode::MrtVersionMismatch => 121,
            ResultCode::MrtExpired => 122,
            ResultCode::MrtTooManyWrites => 123,
            ResultCode::MrtCommitted => 124,
            ResultCode::MrtAborted => 125,
            ResultCode::MrtAlreadyLocked => 126,
            ResultCode::MrtMonitorExists => 127,
            ResultCode::BatchDisabled => 150,
            ResultCode::BatchMaxRequestsExceeded => 151,
            ResultCode::BatchQueuesFull => 152,
            ResultCode::InvalidGeojson => 160,
            ResultCode::IndexFound => 200,
            ResultCode::IndexNotFound => 201,
            ResultCode::IndexOom => 202,
            ResultCode::IndexNotReadable => 203,
            ResultCode::IndexGeneric => 204,
            ResultCode::IndexNameMaxLen => 205,
            ResultCode::IndexMaxCount => 206,
            ResultCode::QueryAborted => 210,
            ResultCode::QueryQueueFull => 211,
            ResultCode::QueryTimeout => 212,
            ResultCode::QueryGeneric => 213,
            ResultCode::QueryNetioErr => 214,
            ResultCode::QueryDuplicate => 215,
            ResultCode::Unknown(code) => code,
        }
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
