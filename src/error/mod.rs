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

extern crate rustc_serialize;

pub mod result_code;

pub use self::result_code::ResultCode;

use std::error;
use std::string::FromUtf8Error;
use std::str::Utf8Error;

use std::fmt;
use std::io;
use std::net;
use std::num;
use std::sync::mpsc;

use rustc_serialize::base64;
use pwhash;

pub type AerospikeResult<T> = Result<T, AerospikeError>;

#[derive(Debug)]
pub struct AerospikeError {
    pub err: ErrorType,
}

impl AerospikeError {
    pub fn new(code: isize, detail: Option<String>) -> AerospikeError {
        AerospikeError {
            err: ErrorType::WithDescription(code,
                                            match detail {
                                                Some(x) => x,
                                                None => ResultCode::to_string(code).to_owned(),
                                            }),
        }
    }

    pub fn err_record_not_found() -> AerospikeError {
        AerospikeError::new(ResultCode::KEY_NOT_FOUND_ERROR,
                            Some("Record not found.".to_string()))
    }

    pub fn err_connection_pool_empty() -> AerospikeError {
        AerospikeError::new(ResultCode::NO_AVAILABLE_CONNECTIONS_TO_NODE,
                            Some("Connection pool is empty.".to_string()))
    }

    pub fn err_skip_msg_pack_header() -> AerospikeError {
        AerospikeError::new(ResultCode::OK,
                            Some("Msgpack header skipped. You should not see this message"
                                .to_string()))
    }

    pub fn err_serialize() -> AerospikeError {
        AerospikeError::new(ResultCode::SERIALIZE_ERROR,
                            Some("Serialization Error".to_string()))
    }
}

#[derive(Debug)]
pub enum ErrorType {
    WithDescription(isize, String),
    IoError(io::Error),
    AddrParseError(net::AddrParseError),
}

impl PartialEq for AerospikeError {
    fn eq(&self, other: &AerospikeError) -> bool {
        match (&self.err, &other.err) {
            (&ErrorType::WithDescription(kind_a, _), &ErrorType::WithDescription(kind_b, _)) => {
                kind_a == kind_b
            }
            _ => false,
        }
    }
}

impl From<mpsc::RecvError> for AerospikeError {
    fn from(err: mpsc::RecvError) -> AerospikeError {
        AerospikeError { err: ErrorType::WithDescription(ResultCode::IO_ERROR, format!("{}", err)) }
    }
}

impl From<num::ParseIntError> for AerospikeError {
    fn from(err: num::ParseIntError) -> AerospikeError {
        AerospikeError {
            err: ErrorType::WithDescription(ResultCode::PARSE_ERROR,
                                            format!("Invalid Int: {}", err)),
        }
    }
}

impl From<base64::FromBase64Error> for AerospikeError {
    fn from(err: base64::FromBase64Error) -> AerospikeError {
        AerospikeError {
            err: ErrorType::WithDescription(ResultCode::PARSE_ERROR, format!("{}", err)),
        }
    }
}

impl From<io::Error> for AerospikeError {
    fn from(ioerr: io::Error) -> AerospikeError {
        AerospikeError { err: ErrorType::IoError(ioerr) }
    }
}

impl From<net::AddrParseError> for AerospikeError {
    fn from(neterr: net::AddrParseError) -> AerospikeError {
        AerospikeError { err: ErrorType::AddrParseError(neterr) }
    }
}

impl From<Utf8Error> for AerospikeError {
    fn from(_: Utf8Error) -> AerospikeError {
        AerospikeError {
            err: ErrorType::WithDescription(ResultCode::PARSE_ERROR, "Invalid UTF-8".to_string()),
        }
    }
}

impl From<FromUtf8Error> for AerospikeError {
    fn from(_: FromUtf8Error) -> AerospikeError {
        AerospikeError {
            err: ErrorType::WithDescription(ResultCode::PARSE_ERROR, "Invalid UTF-8".to_string()),
        }
    }
}

impl From<(isize, String)> for AerospikeError {
    fn from((kind, desc): (isize, String)) -> AerospikeError {
        AerospikeError { err: ErrorType::WithDescription(kind, desc) }
    }
}

impl From<pwhash::error::Error> for AerospikeError {
    fn from(err: pwhash::error::Error) -> AerospikeError {
        AerospikeError {
            err: ErrorType::WithDescription(ResultCode::PARAMETER_ERROR, format!("{}", err)),
        }
    }
}


impl error::Error for AerospikeError {
    fn description(&self) -> &str {
        match self.err {
            ErrorType::WithDescription(_, ref desc) => desc,
            ErrorType::IoError(ref err) => err.description(),
            ErrorType::AddrParseError(ref err) => err.description(),
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match self.err {
            ErrorType::IoError(ref err) => Some(err as &error::Error),
            ErrorType::AddrParseError(ref err) => Some(err as &error::Error),
            _ => None,
        }
    }
}

impl fmt::Display for AerospikeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self.err {
            ErrorType::WithDescription(_, ref desc) => desc.fmt(f),
            ErrorType::IoError(ref err) => err.fmt(f),
            ErrorType::AddrParseError(ref err) => err.fmt(f),
        }
    }
}
