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

use net::parser::Parser;
use error::{AerospikeResult, AerospikeError};
use client::ResultCode;
use std::fmt;

// Host name/port of database server.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct Host {
    // Host name or IP address of database server.
    pub name: String,

    // Port of database server.
    pub port: u16,
}

impl Host {
    pub fn new(name: &str, port: u16) -> Self {
        Host {
            name: name.to_string(),
            port: port,
        }
    }
}

impl fmt::Display for Host {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "({}, {})", self.name, self.port)
    }
}

/// A trait for objects which can be converted to one or more `Host` values.
pub trait ToHosts {
    /// Converts this object into a list of `Host`s.
    ///
    /// # Errors
    ///
    /// Any errors encountered during conversion will be returned as an `Err`.
    fn to_hosts(&self) -> AerospikeResult<Vec<Host>>;
}

impl ToHosts for Vec<Host> {
    fn to_hosts(&self) -> AerospikeResult<Vec<Host>> {
        Ok(self.clone())
    }
}

impl ToHosts for String {
    fn to_hosts(&self) -> AerospikeResult<Vec<Host>> {
        let mut parser = Parser::new(self, 3000);
        match parser.read_hosts() {
            Ok(hosts) => return Ok(hosts),
            Err(err) => return Err(AerospikeError::new(ResultCode::ParseError,
                                                       Some(format!("Invalid hosts list: {}", err))))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_hosts() {
        assert_eq!(Ok(vec![Host::new("foo", 3000)]), String::from("foo").to_hosts());
        assert_eq!(Ok(vec![Host::new("foo", 1234)]), String::from("foo:1234").to_hosts());
        assert_eq!(Ok(vec![Host::new("foo", 1234), Host::new("bar", 1234)]), String::from("foo:1234,bar:1234").to_hosts());
    }
}
