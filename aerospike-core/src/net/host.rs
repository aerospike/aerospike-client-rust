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
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::vec::IntoIter;

use crate::errors::{Error, Result};
use crate::net::parser::Parser;

/// Host name/port of database server.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct Host {
    /// Host name or IP address of database server.
    pub name: String,

    /// TLS certificate name used for secure connections.
    pub tls_name: Option<String>,

    /// Port of database server.
    pub port: u16,
}

impl Host {
    /// Creates a new host instance given a hostname/IP and a port number.
    pub fn new(name: &str, port: u16) -> Self {
        Host {
            name: name.to_string(),
            tls_name: None,
            port,
        }
    }

    /// Creates a new tls host instance given a hostname/IP and a port number.
    pub fn new_tls(name: &str, tls_name: &str, port: u16) -> Self {
        let tls_name = match tls_name.trim().len() {
            0 => None,
            _ => Some(tls_name.into()),
        };

        Host {
            name: name.to_string(),
            tls_name,
            port,
        }
    }

    /// Returns a string representation of the host's address.
    pub fn address(&self) -> String {
        format!("{}:{}", self.name, self.port)
    }
}

impl ToSocketAddrs for Host {
    type Iter = IntoIter<SocketAddr>;
    fn to_socket_addrs(&self) -> io::Result<IntoIter<SocketAddr>> {
        (self.name.as_str(), self.port).to_socket_addrs()
    }
}

impl fmt::Display for Host {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}:{}", self.name, self.port)
    }
}

/// A trait for objects which can be converted to one or more `Host` values.
pub trait ToHosts {
    /// Converts this object into a list of `Host`s.
    ///
    /// # Errors
    ///
    /// Any errors encountered during conversion will be returned as an `Err`.
    fn to_hosts(&self) -> Result<Vec<Host>>;
}

impl ToHosts for Vec<Host> {
    fn to_hosts(&self) -> Result<Vec<Host>> {
        Ok(self.clone())
    }
}

impl ToHosts for String {
    fn to_hosts(&self) -> Result<Vec<Host>> {
        let parser = Parser::new(self, 3000);
        parser.read_hosts().map_err(|e| {
            e.wrap(Error::InvalidArgument(format!(
                "Invalid hosts list: '{self}'"
            )))
        })
    }
}

impl ToHosts for &str {
    fn to_hosts(&self) -> Result<Vec<Host>> {
        (*self).to_string().to_hosts()
    }
}

#[cfg(test)]
mod tests {
    use super::{Host, ToHosts};

    const CLOUD_TLS_NAME: &str = "6072bb5c-b902-4cbd-888c-cc63e71192f7.amstest.internal";

    fn assert_tls_host(seed: &str, expected_name: &str, expected_tls: &str, expected_port: u16) {
        let hosts = seed.to_hosts().unwrap_or_else(|e| {
            panic!(
                "expected `{}` to parse as host:tls_name:port, got: {:?}",
                seed, e
            )
        });
        assert_eq!(
            hosts,
            vec![Host::new_tls(expected_name, expected_tls, expected_port)],
            "unexpected parse for `{seed}`"
        );
    }

    /// Control: letter-first `tls_name` already parses (QE-1025 TEST 2).
    #[test]
    fn tls_name_letter_first_parses_host_tls_port() {
        assert_tls_host(
            "node.example.com:abc.example.com:4000",
            "node.example.com",
            "abc.example.com",
            4000,
        );
    }

    /// QE-1025 TEST 1: digit-first `tls_name` must not be mistaken for a port.
    #[test]
    fn tls_name_digit_first_parses_host_tls_port() {
        assert_tls_host(
            "node.example.com:6abc.example.com:4000",
            "node.example.com",
            "6abc.example.com",
            4000,
        );
    }

    /// QE-1025 TEST 3: Aerospike Cloud UUID-style `tls_name` (`host` == `tls_name`).
    #[test]
    fn tls_name_cloud_uuid_hostname_parses_host_tls_port() {
        let seed = format!("{CLOUD_TLS_NAME}:{CLOUD_TLS_NAME}:4000");
        assert_tls_host(&seed, CLOUD_TLS_NAME, CLOUD_TLS_NAME, 4000);
    }

    /// Bare host:port must still parse when the port starts with a digit.
    #[test]
    fn host_port_without_tls_name_still_parses() {
        let hosts = "node.example.com:4000".to_hosts().unwrap();
        assert_eq!(hosts, vec![Host::new("node.example.com", 4000)]);
    }

    #[test]
    fn to_hosts() {
        assert_eq!(
            vec![Host::new("foo", 3000)],
            String::from("foo").to_hosts().unwrap()
        );
        assert_eq!(vec![Host::new("foo", 3000)], "foo".to_hosts().unwrap());
        assert_eq!(vec![Host::new("foo", 1234)], "foo:1234".to_hosts().unwrap());
        assert_eq!(
            vec![Host::new("foo", 1234), Host::new("bar", 1234)],
            "foo:1234,bar:1234".to_hosts().unwrap()
        );
    }
}
