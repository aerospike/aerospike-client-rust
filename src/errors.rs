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

//! Error and Result types for the Aerospike client.

#![allow(missing_docs)]

use ResultCode;

error_chain! {

    // Automatic conversions between this error chain and other error types not defined by the
    // `error_chain!`.
    foreign_links {
        Base64(::rustc_serialize::base64::FromBase64Error) #[doc = "Error deserializing a Base64 encoded value"];
        InvalidUtf8(::std::str::Utf8Error) #[doc = "Error interpreting a sequence of u8 as a UTF-8 encoded string."];
        Io(::std::io::Error) #[doc = "Error during an I/O operation"];
        MpscRecv(::std::sync::mpsc::RecvError) #[doc = "Error returned from the `recv` function on an MPSC `Receiver`"];
        ParseAddr(::std::net::AddrParseError) #[doc = "Error parsing an IP or socket address"];
        ParseInt(::std::num::ParseIntError) #[doc = "Error parsing an integer"];
        PwHash(::pwhash::error::Error) #[doc = "Error returned while hashing a password for user authentication"];
    }

    // Additional `ErrorKind` variants.
    errors {

        /// The client received a server response that it was not able to process.
        BadResponse(details: String) {
            description("Bad Server Response")
            display("Bad Server Response: {}", details)
        }

        /// The client was not able to communicate with the cluster due to some issue with the
        /// network connection.
        Connection(details: String) {
            description("Network Connection Issue")
            display("Unable to communicate with server cluster: {}", details)
        }

        /// One or more of the arguments passed to the client are invalid.
        InvalidArgument(details: String) {
            description("Invalid Argument")
            display("Invalid argument: {}", details)
        }

        /// Server responded with a response code indicating an error condition.
        ServerError(rc: ResultCode) {
            description("Server Error")
            display("Server error: {}", rc.into_string())
        }

        /// Error returned when executing a User-Defined Function (UDF) resulted in an error.
        UdfBadResponse(details: String) {
            description("UDF Bad Response")
            display("UDF Bad Response: {}", details)
        }
    }
}

macro_rules! log_error_chain {
    ($err:expr, $($arg:tt)*) => {
        error!($($arg)*);
        error!("Error: {}", $err);
        for e in $err.iter().skip(1) {
            error!("caused by: {}", e);
        }
        if let Some(backtrace) = $err.backtrace() {
            error!("backtrace: {:?}", backtrace);
        }
    };
}
