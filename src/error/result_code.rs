// Copyright 2015-2016 Aerospike, Inc.
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

// ResultCode signifies the database operation error codes.
// The positive numbers align with the server side file proto.h.
pub mod ResultCode {

    use error::error::{AerospikeError, ErrorType};

    // IO Error
    pub const IO_ERROR: isize = -9;

    // There were no connections available to the node in the pool, and the pool was limited
    pub const NO_AVAILABLE_CONNECTIONS_TO_NODE: isize = -8;

    // Asynchronous max concurrent database commands have been exceeded and therefore rejected.
    pub const TYPE_NOT_SUPPORTED: isize = -7;

    // Asynchronous max concurrent database commands have been exceeded and therefore rejected.
    pub const COMMAND_REJECTED: isize = -6;

    // Query was terminated by user.
    pub const QUERY_TERMINATED: isize = -5;

    // Scan was terminated by user.
    pub const SCAN_TERMINATED: isize = -4;

    // Chosen node is not currently active.
    pub const INVALID_NODE_ERROR: isize = -3;

    // Client parse error.
    pub const PARSE_ERROR: isize = -2;

    // Client serialization error.
    pub const SERIALIZE_ERROR: isize = -1;

    // OperationType was successful.
    pub const OK: isize = 0;

    // Unknown server failure.
    pub const SERVER_ERROR: isize = 1;

    // On retrieving, touching or replacing a record that doesn't exist.
    pub const KEY_NOT_FOUND_ERROR: isize = 2;

    // On modifying a record with unexpected generation.
    pub const GENERATION_ERROR: isize = 3;

    // Bad parameter(s) were passed in database operation call.
    pub const PARAMETER_ERROR: isize = 4;

    // On create-only (write unique) operations on a record that already
    // exists.
    pub const KEY_EXISTS_ERROR: isize = 5;

    // On create-only (write unique) operations on a bin that already
    // exists.
    pub const BIN_EXISTS_ERROR: isize = 6;

    // Expected cluster ID was not received.
    pub const CLUSTER_KEY_MISMATCH: isize = 7;

    // Server has run out of memory.
    pub const SERVER_MEM_ERROR: isize = 8;

    // Client or server has timed out.
    pub const TIMEOUT: isize = 9;

    // XDS product is not available.
    pub const NO_XDS: isize = 10;

    // Server is not accepting requests.
    pub const SERVER_NOT_AVAILABLE: isize = 11;

    // OperationType is not supported with configured bin type (single-bin or
    // multi-bin).
    pub const BIN_TYPE_ERROR: isize = 12;

    // Record size exceeds limit.
    pub const RECORD_TOO_BIG: isize = 13;

    // Too many concurrent operations on the same record.
    pub const KEY_BUSY: isize = 14;

    // Scan aborted by server.
    pub const SCAN_ABORT: isize = 15;

    // Unsupported Server Feature (e.g. Scan + UDF)
    pub const UNSUPPORTED_FEATURE: isize = 16;

    // Specified bin name does not exist in record.
    pub const BIN_NOT_FOUND: isize = 17;

    // Specified bin name does not exist in record.
    pub const DEVICE_OVERLOAD: isize = 18;

    // Key type mismatch.
    pub const KEY_MISMATCH: isize = 19;

    // Invalid namespace.
    pub const INVALID_NAMESPACE: isize = 20;

    // Bin name length greater than 14 characters.
    pub const BIN_NAME_TOO_LONG: isize = 21;

    // OperationType not allowed at this time.
    pub const FAIL_FORBIDDEN: isize = 22;

    // There are no more records left for query.
    pub const QUERY_END: isize = 50;

    // Security type not supported by connected server.
    pub const SECURITY_NOT_SUPPORTED: isize = 51;

    // Administration command is invalid.
    pub const SECURITY_NOT_ENABLED: isize = 52;

    // Administration field is invalid.
    pub const SECURITY_SCHEME_NOT_SUPPORTED: isize = 53;

    // Administration command is invalid.
    pub const INVALID_COMMAND: isize = 54;

    // Administration field is invalid.
    pub const INVALID_FIELD: isize = 55;

    // Security protocol not followed.
    pub const ILLEGAL_STATE: isize = 56;

    // User name is invalid.
    pub const INVALID_USER: isize = 60;

    // User was previously created.
    pub const USER_ALREADY_EXISTS: isize = 61;

    // Password is invalid.
    pub const INVALID_PASSWORD: isize = 62;

    // Security credential is invalid.
    pub const EXPIRED_PASSWORD: isize = 63;

    // Forbidden password (e.g. recently used)
    pub const FORBIDDEN_PASSWORD: isize = 64;

    // Security credential is invalid.
    pub const INVALID_CREDENTIAL: isize = 65;

    // Role name is invalid.
    pub const INVALID_ROLE: isize = 70;

    // Role already exists.
    pub const ROLE_ALREADY_EXISTS: isize = 71;

    // Privilege is invalid.
    pub const INVALID_PRIVILEGE: isize = 72;

    // User must be authentication before performing database operations.
    pub const NOT_AUTHENTICATED: isize = 80;

    // User does not posses the required role to perform the database operation.
    pub const ROLE_VIOLATION: isize = 81;

    // A user defined function returned an error code.
    pub const UDF_BAD_RESPONSE: isize = 100;

    // The requested item in a large collection was not found.
    pub const LARGE_ITEM_NOT_FOUND: isize = 125;

    // Batch functionality has been disabled.
    pub const BATCH_DISABLED: isize = 150;

    // Batch max requests have been exceeded.
    pub const BATCH_MAX_REQUESTS_EXCEEDED: isize = 151;

    // All batch queues are full.
    pub const BATCH_QUEUES_FULL: isize = 152;

    // Secondary index already exists.
    pub const INDEX_FOUND: isize = 200;

    // Requested secondary index does not exist.
    pub const INDEX_NOTFOUND: isize = 201;

    // Secondary index memory space exceeded.
    pub const INDEX_OOM: isize = 202;

    // Secondary index not available.
    pub const INDEX_NOTREADABLE: isize = 203;

    // Generic secondary index error.
    pub const INDEX_GENERIC: isize = 204;

    // Index name maximum length exceeded.
    pub const INDEX_NAME_MAXLEN: isize = 205;

    // Maximum number of indicies exceeded.
    pub const INDEX_MAXCOUNT: isize = 206;

    // Secondary index query aborted.
    pub const QUERY_ABORTED: isize = 210;

    // Secondary index queue full.
    pub const QUERY_QUEUEFULL: isize = 211;

    // Secondary index query timed out on server.
    pub const QUERY_TIMEOUT: isize = 212;

    // Generic query error.
    pub const QUERY_GENERIC: isize = 213;

    // Query NetIO error on server
    pub const QUERY_NETIO_ERR: isize = 214;

    // Duplicate TaskId sent for the statement
    pub const QUERY_DUPLICATE: isize = 215;

    // UDF does not exist.
    pub const AEROSPIKE_ERR_UDF_NOT_FOUND: isize = 1301;

    // LUA file does not exist.
    pub const AEROSPIKE_ERR_LUA_FILE_NOT_FOUND: isize = 1302;

    // Should connection be put back into pool.
    pub fn keep_connection(err: &AerospikeError) -> bool {
        match err.err {
            ErrorType::WithDescription(result_code, _) => match result_code {
                KEY_NOT_FOUND_ERROR =>  true,
                _ => false,
            },
        _ => false,
        }
    }


    // Return result code as a string.
    pub fn to_string(code: isize) -> String {
        match code {
            IO_ERROR => "IO Error".to_string(),
            NO_AVAILABLE_CONNECTIONS_TO_NODE => {
                "No available connections to the node. Connection Pool was empty, and limited to \
                 certain number of connections."
                    .to_string()
            }
            TYPE_NOT_SUPPORTED => "Type cannot be converted to Value Type.".to_string(),
            COMMAND_REJECTED => "command rejected".to_string(),
            QUERY_TERMINATED => "Query terminated".to_string(),
            SCAN_TERMINATED => "Scan terminated".to_string(),
            INVALID_NODE_ERROR => "Invalid node".to_string(),
            PARSE_ERROR => "Parse error".to_string(),
            SERIALIZE_ERROR => "Serialize error".to_string(),
            OK => "ok".to_string(),
            SERVER_ERROR => "Server error".to_string(),
            KEY_NOT_FOUND_ERROR => "Key not found".to_string(),
            GENERATION_ERROR => "Generation error".to_string(),
            PARAMETER_ERROR => "Parameter error".to_string(),
            KEY_EXISTS_ERROR => "Key already exists".to_string(),
            BIN_EXISTS_ERROR => "Bin already exists".to_string(),
            CLUSTER_KEY_MISMATCH => "Cluster key mismatch".to_string(),
            SERVER_MEM_ERROR => "Server memory error".to_string(),
            TIMEOUT => "Timeout".to_string(),
            NO_XDS => "XDS not available".to_string(),
            SERVER_NOT_AVAILABLE => "Server not available".to_string(),
            BIN_TYPE_ERROR => "Bin type error".to_string(),
            RECORD_TOO_BIG => "Record too big".to_string(),
            KEY_BUSY => "Hot key".to_string(),
            SCAN_ABORT => "Scan aborted".to_string(),
            UNSUPPORTED_FEATURE => "Unsupported Server Feature".to_string(),
            BIN_NOT_FOUND => "Bin not found".to_string(),
            DEVICE_OVERLOAD => "Device overload".to_string(),
            KEY_MISMATCH => "Key mismatch".to_string(),
            INVALID_NAMESPACE => "Namespace not found".to_string(),
            BIN_NAME_TOO_LONG => "Bin name length greater than 14 characters".to_string(),
            FAIL_FORBIDDEN => "OperationType not allowed at this time".to_string(),
            QUERY_END => "Query end".to_string(),
            SECURITY_NOT_SUPPORTED => "Security not supported".to_string(),
            SECURITY_NOT_ENABLED => "Security not enabled".to_string(),
            SECURITY_SCHEME_NOT_SUPPORTED => "Security scheme not supported".to_string(),
            INVALID_COMMAND => "Invalid command".to_string(),
            INVALID_FIELD => "Invalid field".to_string(),
            ILLEGAL_STATE => "Illegal state".to_string(),
            INVALID_USER => "Invalid user".to_string(),
            USER_ALREADY_EXISTS => "User already exists".to_string(),
            INVALID_PASSWORD => "Invalid password".to_string(),
            EXPIRED_PASSWORD => "Expired password".to_string(),
            FORBIDDEN_PASSWORD => "Forbidden password".to_string(),
            INVALID_CREDENTIAL => "Invalid credential".to_string(),
            INVALID_ROLE => "Invalid role".to_string(),
            ROLE_ALREADY_EXISTS => "Role already exists".to_string(),
            INVALID_PRIVILEGE => "Invalid privilege".to_string(),
            NOT_AUTHENTICATED => "Not authenticated".to_string(),
            ROLE_VIOLATION => "Role violation".to_string(),
            UDF_BAD_RESPONSE => "UDF returned error".to_string(),
            LARGE_ITEM_NOT_FOUND => "Large collection item not found".to_string(),
            BATCH_DISABLED => "Batch functionality has been disabled".to_string(),
            BATCH_MAX_REQUESTS_EXCEEDED => "Batch max requests have been exceeded".to_string(),
            BATCH_QUEUES_FULL => "All batch queues are full".to_string(),
            INDEX_FOUND => "Index already exists".to_string(),
            INDEX_NOTFOUND => "Index not found".to_string(),
            INDEX_OOM => "Index out of memory".to_string(),
            INDEX_NOTREADABLE => "Index not readable".to_string(),
            INDEX_GENERIC => "Index error".to_string(),
            INDEX_NAME_MAXLEN => "Index name max length exceeded".to_string(),
            INDEX_MAXCOUNT => "Index count exceeds max".to_string(),
            QUERY_ABORTED => "Query aborted".to_string(),
            QUERY_QUEUEFULL => "Query queue full".to_string(),
            QUERY_TIMEOUT => "Query timeout".to_string(),
            QUERY_GENERIC => "Query error".to_string(),
            QUERY_NETIO_ERR => "Query NetIO error on server".to_string(),
            QUERY_DUPLICATE => "Duplicate TaskId sent for the statement".to_string(),
            AEROSPIKE_ERR_UDF_NOT_FOUND => "UDF does not exist.".to_string(),
            AEROSPIKE_ERR_LUA_FILE_NOT_FOUND => "LUA package/file does not exist.".to_string(),

            _ => {
                format!("Result code '{}' not recognized on client-side. Please file an issue on \
                         Github.",
                        code)
            }
        }
    }
}

