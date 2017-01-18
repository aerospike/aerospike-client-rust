// Copyright 2015-2017 Aerospike, Inc.
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

use client::ResultCode;

error_chain! {

    // Automatic conversions between this error chain and other error types not defined by the
    // `error_chain!`.
    foreign_links {
        Base64(::rustc_serialize::base64::FromBase64Error);
        InvalidUtf8(::std::str::Utf8Error);
        Io(::std::io::Error);
        MpscRecv(::std::sync::mpsc::RecvError);
        ParseAddr(::std::net::AddrParseError);
        ParseInt(::std::num::ParseIntError);
        PwHash(::pwhash::error::Error);
    }

    // Additional `ErrorKind` variants.
    errors {
        /// The client was not able to complete a command within the timeout duration specified
        /// within the client policy.
        Timeout {
            description("Command Timeout")
            display("Command execution timed out")
        }

        /// Error returned when executing a User-Defined Function (UDF) resulted in an error.
        UdfBadResponse(response: String) {
            description("UDF Bad Response")
            display("UDF Bad Response: '{}'", response)
        }

        /// An error occurred during the cluster tend process.
        ClusterTendError(msg: String) {
            description("Cluster Tend Error")
            display("Error during cluster tend: {}", msg)
        }

        /// A query statement is invalid.
        InvalidStatement(msg: String) {
            description("Invalid Query Statement")
            display("Invalid query statement: {}", msg)
        }

        /// Server responded with a responde code indicating an error condition.
        ServerError(rc: ResultCode) {
            description("Server Error")
            display("Server error: {}", rc.into_string())
        }
    }
}
