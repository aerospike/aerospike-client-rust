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

/// Database operation error codes. The error codes are defined in the server-side file proto.h.
#[cfg_attr(feature = "serialization", derive(Serialize))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResultCode {
    /// `OperationType` was successful.
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

    /// Operation not allowed in current configuration.
    AlwaysForbidden,

    /// Partition is unavailable.
    PartitionUnavailable,

    /// Operation type is not supported with configured bin type (single-bin or multi-bin).
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

    /// `OperationType` not allowed at this time.
    FailForbidden,

    /// Returned by Map put and `put_items` operations when policy is REPLACE but key was not found.
    ElementNotFound,

    /// Returned by Map put and `put_items` operations when policy is `CREATE_ONLY` but key already
    /// exists.
    ElementExists,

    /// Enterprise-only feature not supported by the community edition.
    EnterpriseOnly,

    /// The operation cannot be applied to the current bin value on the server.
    OpNotApplicable,

    /// The command was not performed because the filter was false.
    FilteredOut,

    /// Write command loses conflict to XDR.
    LostConflict,

    /// Write can't complete until XDR finishes shipping.
    XDRKeyBusy,

    /// Transaction record blocked by another transaction.
    MrtBlocked,

    /// Expected transaction version mismatch.
    MrtVersionMismatch,

    /// Transaction expired.
    MrtExpired,

    /// Transaction write command limit (4096) exceeded.
    MrtTooManyWrites,

    /// Transaction was already committed.
    MrtCommitted,

    /// Transaction was already aborted.
    MrtAborted,

    /// Record already locked by this transaction.
    MrtAlreadyLocked,

    /// Transaction monitor record already exists.
    MrtMonitorExists,

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

    /// Login session expired.
    ExpiredSession,

    /// Role name is invalid.
    InvalidRole,

    /// Role already exists.
    RoleAlreadyExists,

    /// Privilege is invalid.
    InvalidPrivilege,

    /// Invalid IP address allowlist.
    InvalidAllowlist,

    /// Quotas not enabled on server.
    QuotasNotEnabled,

    /// Invalid quota value.
    InvalidQuota,

    /// User must be authentication before performing database operations.
    NotAuthenticated,

    /// User does not posses the required role to perform the database operation.
    RoleViolation,

    /// Command not allowed because sender IP address not allowlisted.
    NotAllowlisted,

    /// Quota exceeded.
    QuotaExceeded,

    /// A user defined function returned an error code.
    UdfBadResponse,

    /// Batch functionality has been disabled.
    BatchDisabled,

    /// Batch max requests have been exceeded.
    BatchMaxRequestsExceeded,

    /// All batch queues are full.
    BatchQueuesFull,

    /// Invalid `GeoJSON` on insert/update
    InvalidGeojson,

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

    /// Query `NetIo` error on server
    QueryNetioErr,

    /// Duplicate `TaskId` sent for the statement
    QueryDuplicate,

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
            120 => ResultCode::MrtBlocked,
            121 => ResultCode::MrtVersionMismatch,
            122 => ResultCode::MrtExpired,
            123 => ResultCode::MrtTooManyWrites,
            124 => ResultCode::MrtCommitted,
            125 => ResultCode::MrtAborted,
            126 => ResultCode::MrtAlreadyLocked,
            127 => ResultCode::MrtMonitorExists,
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
            ResultCode::MrtBlocked => {
                String::from("Transaction record blocked by another transaction")
            }
            ResultCode::MrtVersionMismatch => String::from("Expected transaction version mismatch"),
            ResultCode::MrtExpired => String::from("Transaction expired"),
            ResultCode::MrtTooManyWrites => {
                String::from("Transaction write command limit (4096) exceeded")
            }
            ResultCode::MrtCommitted => String::from("Transaction was already committed"),
            ResultCode::MrtAborted => String::from("Transaction was already aborted"),
            ResultCode::MrtAlreadyLocked => {
                String::from("Record already locked by this transaction")
            }
            ResultCode::MrtMonitorExists => {
                String::from("Transaction monitor record already exists")
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

impl From<ResultCode> for u8 {
    /// Wire value of the result code (inverse of [`ResultCode::from_u8`]).
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
            ResultCode::MrtBlocked => 120,
            ResultCode::MrtVersionMismatch => 121,
            ResultCode::MrtExpired => 122,
            ResultCode::MrtTooManyWrites => 123,
            ResultCode::MrtCommitted => 124,
            ResultCode::MrtAborted => 125,
            ResultCode::MrtAlreadyLocked => 126,
            ResultCode::MrtMonitorExists => 127,
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


/// Client-side error codes, mirroring the Java client's negative `ResultCode`
/// constants.
///
/// Server failures carry a [`ResultCode`] (non-negative wire
/// value); failures generated on the client carry one of these, so downstream
/// bindings can map every [`Error`](crate::errors::Error) to a number via
/// [`Error::result_code`](crate::errors::Error::result_code) and to a typed
/// code via
/// [`Error::client_result_code`](crate::errors::Error::client_result_code).
///
/// Note: client-side *timeouts* use the server `TIMEOUT` (9) code, matching
/// the Java client, and therefore do not appear here.
#[cfg_attr(feature = "serialization", derive(Serialize))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ClientResultCode {
    /// Transaction has already been aborted.
    TxnAlreadyAborted,

    /// Transaction has already been committed.
    TxnAlreadyCommitted,

    /// Transaction failed.
    TxnFailed,

    /// One or more keys failed in a batch.
    BatchFailed,

    /// No response received from server.
    NoResponse,

    /// Max errors limit reached (per-node circuit breaker).
    MaxErrorRate,

    /// Max retries limit reached.
    MaxRetriesExceeded,

    /// Client serialization error.
    SerializeError,

    /// Asynchronous delay queue is full. Reserved for Java compatibility;
    /// not produced by this client.
    AsyncQueueFull,

    /// Server is not accepting requests (connection failure).
    ServerNotAvailable,

    /// Max. number of connections per node would be exceeded.
    NoMoreConnections,

    /// Query was terminated prematurely.
    QueryTerminated,

    /// Scan was terminated prematurely.
    ScanTerminated,

    /// Chosen node is not currently active.
    InvalidNodeError,

    /// Client parse error.
    ParseError,

    /// Generic client error.
    ClientError,

    /// Unknown client error code.
    Unknown(i32),
}

impl ClientResultCode {
    /// Convert a Java-compatible numeric client code into the enum.
    pub(crate) const fn from_i32(n: i32) -> ClientResultCode {
        match n {
            -19 => ClientResultCode::TxnAlreadyAborted,
            -18 => ClientResultCode::TxnAlreadyCommitted,
            -17 => ClientResultCode::TxnFailed,
            -16 => ClientResultCode::BatchFailed,
            -15 => ClientResultCode::NoResponse,
            -12 => ClientResultCode::MaxErrorRate,
            -11 => ClientResultCode::MaxRetriesExceeded,
            -10 => ClientResultCode::SerializeError,
            -9 => ClientResultCode::AsyncQueueFull,
            -8 => ClientResultCode::ServerNotAvailable,
            -7 => ClientResultCode::NoMoreConnections,
            -5 => ClientResultCode::QueryTerminated,
            -4 => ClientResultCode::ScanTerminated,
            -3 => ClientResultCode::InvalidNodeError,
            -2 => ClientResultCode::ParseError,
            -1 => ClientResultCode::ClientError,
            code => ClientResultCode::Unknown(code),
        }
    }

    /// Convert a client result code into a string.
    pub fn into_string(self) -> String {
        match self {
            ClientResultCode::TxnAlreadyAborted => {
                String::from("Transaction already aborted")
            }
            ClientResultCode::TxnAlreadyCommitted => {
                String::from("Transaction already committed")
            }
            ClientResultCode::TxnFailed => String::from("Transaction failed"),
            ClientResultCode::BatchFailed => {
                String::from("One or more keys failed in a batch")
            }
            ClientResultCode::NoResponse => {
                String::from("No response received from server")
            }
            ClientResultCode::MaxErrorRate => String::from("Max errors limit reached"),
            ClientResultCode::MaxRetriesExceeded => String::from("Max retries exceeded"),
            ClientResultCode::SerializeError => String::from("Serialize error"),
            ClientResultCode::AsyncQueueFull => {
                String::from("Async delay queue is full")
            }
            ClientResultCode::ServerNotAvailable => {
                String::from("Server is not accepting requests")
            }
            ClientResultCode::NoMoreConnections => {
                String::from("No more available connections")
            }
            ClientResultCode::QueryTerminated => String::from("Query was terminated"),
            ClientResultCode::ScanTerminated => String::from("Scan was terminated"),
            ClientResultCode::InvalidNodeError => String::from("Invalid node"),
            ClientResultCode::ParseError => String::from("Parse error"),
            ClientResultCode::ClientError => String::from("Client error"),
            ClientResultCode::Unknown(code) => {
                format!("Unknown client error code: {code}")
            }
        }
    }
}

impl From<i32> for ClientResultCode {
    fn from(val: i32) -> ClientResultCode {
        ClientResultCode::from_i32(val)
    }
}

impl From<ClientResultCode> for i32 {
    /// Java-compatible numeric value (inverse of [`ClientResultCode::from_i32`]).
    fn from(rc: ClientResultCode) -> i32 {
        match rc {
            ClientResultCode::TxnAlreadyAborted => -19,
            ClientResultCode::TxnAlreadyCommitted => -18,
            ClientResultCode::TxnFailed => -17,
            ClientResultCode::BatchFailed => -16,
            ClientResultCode::NoResponse => -15,
            ClientResultCode::MaxErrorRate => -12,
            ClientResultCode::MaxRetriesExceeded => -11,
            ClientResultCode::SerializeError => -10,
            ClientResultCode::AsyncQueueFull => -9,
            ClientResultCode::ServerNotAvailable => -8,
            ClientResultCode::NoMoreConnections => -7,
            ClientResultCode::QueryTerminated => -5,
            ClientResultCode::ScanTerminated => -4,
            ClientResultCode::InvalidNodeError => -3,
            ClientResultCode::ParseError => -2,
            ClientResultCode::ClientError => -1,
            ClientResultCode::Unknown(code) => code,
        }
    }
}

impl From<ClientResultCode> for String {
    fn from(code: ClientResultCode) -> String {
        code.into_string()
    }
}

impl fmt::Display for ClientResultCode {
    fn fmt(&self, f: &mut fmt::Formatter) -> StdResult<(), fmt::Error> {
        write!(f, "{}", self.into_string())
    }
}

#[cfg(test)]
mod tests {
    use super::{ClientResultCode, ResultCode};

    #[test]
    fn client_code_i32_round_trip() {
        // Every known negative code maps back to itself; gaps and positives
        // fall through to Unknown but still round-trip numerically.
        for n in -25i32..=0 {
            let rc = ClientResultCode::from(n);
            assert_eq!(i32::from(rc), n, "round trip failed for {n}");
        }
    }

    #[test]
    fn client_code_into_string() {
        let result: String = ClientResultCode::MaxRetriesExceeded.into();
        assert_eq!("Max retries exceeded", result);
        assert_eq!(
            "Unknown client error code: -42",
            ClientResultCode::Unknown(-42).to_string()
        );
        assert_eq!("Client error", format!("{}", ClientResultCode::ClientError));
    }

    #[test]
    fn u8_round_trip() {
        // Every wire value maps back to itself through the inverse impl.
        for n in 0u8..=255 {
            let rc = ResultCode::from(n);
            assert_eq!(u8::from(rc), n, "round trip failed for {n}");
        }
    }

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
