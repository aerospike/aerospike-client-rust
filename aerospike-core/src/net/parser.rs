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

use crate::errors::{Error, Result};
use crate::Host;
use regex::Regex;
use std::sync::LazyLock;

#[derive(Debug)]
pub struct Parser<'a> {
    s: &'a str,
    default_port: u16,
}

impl<'a> Parser<'a> {
    pub const fn new(s: &'a str, default_port: u16) -> Self {
        Parser { s, default_port }
    }

    pub fn read_hosts(&self) -> Result<Vec<Host>> {
        static RE: LazyLock<Regex> = LazyLock::new(|| {
            Regex::new(r"^(((\[(?P<ipv6>(\S+))\])|(?P<ipv4>((\d{1,3})(\.\d{1,3}){3}))|(?P<host>([\S--:]+)))(:(?P<tls>[\S--[\d:]][\S--:]*))?(:(?P<port>(\d{1,5})))?)$")
            .unwrap()
        });

        let mut hosts = Vec::new();

        let sne = self.s.split(',');
        for s in sne {
            if !RE.is_match(s) {
                return Err(Error::ClientError(format!("Could not parse `{s}`")));
            }

            // 'm' is a 'Match', and 'as_str()' returns the matching part of the haystack.
            let addrs: (String, Option<String>, String) = RE
                .captures(s)
                .map(|caps| {
                    let host = match (caps.name("ipv6"), caps.name("ipv4"), caps.name("host")) {
                        (Some(ipv6), None, None) => ipv6,
                        (None, Some(ipv4), None) => ipv4,
                        (None, None, Some(host)) => host,
                        _ => unreachable!(),
                    };
                    let host = host.as_str().into();
                    let tls = caps.name("tls").map(|tls| tls.as_str().into());
                    let port = caps
                        .name("port")
                        .map_or_else(|| format!("{}", self.default_port), |p| p.as_str().into());
                    (host, tls, port)
                })
                .unwrap();

            let (host, tls_name, port) = addrs;
            let (host, tls_name, port) = (host, tls_name, port.parse::<u16>()?);

            match tls_name {
                Some(tls_name) => hosts.push(Host::new_tls(&host, &tls_name, port)),
                _ => hosts.push(Host::new(&host, port)),
            }
        }
        Ok(hosts)
    }
}

#[cfg(test)]
mod tests {
    use super::{Host, Parser};

    #[test]
    fn read_hosts() {
        assert_eq!(
            vec![Host::new("foo", 3000)],
            Parser::new("foo", 3000).read_hosts().unwrap()
        );
        assert_eq!(
            vec![Host::new_tls("foo", "bar", 3000)],
            Parser::new("foo:bar", 3000).read_hosts().unwrap()
        );
        assert_eq!(
            vec![Host::new("foo", 1234)],
            Parser::new("foo:1234", 3000).read_hosts().unwrap()
        );
        assert_eq!(
            vec![Host::new_tls("foo", "bar", 1234)],
            Parser::new("foo:bar:1234", 3000).read_hosts().unwrap()
        );
        assert_eq!(
            vec![Host::new("foo", 1234), Host::new("bar", 1234)],
            Parser::new("foo:1234,bar:1234", 3000).read_hosts().unwrap()
        );
        assert!(Parser::new("", 3000).read_hosts().is_err());
        assert!(Parser::new(",", 3000).read_hosts().is_err());
        assert!(Parser::new("foo,", 3000).read_hosts().is_err());
        assert!(Parser::new(":", 3000).read_hosts().is_err());
        assert!(Parser::new("foo:", 3000).read_hosts().is_err());
        assert!(Parser::new("foo:bar:bar", 3000).read_hosts().is_err());
        assert!(Parser::new("foo:bar:1234:1234", 3000).read_hosts().is_err());
    }
}
