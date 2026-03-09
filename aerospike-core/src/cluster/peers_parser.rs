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
use std::iter::Peekable;
use std::str::Chars;

pub struct PeersParser<'a> {
    s: Peekable<Chars<'a>>,
}

impl<'a> PeersParser<'a> {
    pub fn new(s: &'a str) -> Self {
        PeersParser {
            s: s.chars().peekable(),
        }
    }

    pub fn parse(&mut self) -> Result<(u64, Vec<Host>)> {
        let mut hosts = vec![];

        let gen = self.read_generation()?;
        self.expect(",")?;
        let default_port = self.read_port()?;
        loop {
            match self.peek() {
                Some(&c) if c == ',' => self.next_char(),
                _ => break,
            };
            hosts.append(&mut self.parse_hosts(default_port)?);
        }

        Ok((gen, hosts))
    }

    pub fn parse_hosts(&mut self, default_port: u16) -> Result<Vec<Host>> {
        let mut hosts = Vec::new();

        self.expect("[")?;
        // hosts may be empty
        if self.peek_is_one_of("]") {
            self.expect("]")?;
            return Ok(hosts);
        }

        let _node_name = self.read_until(",");
        self.expect(",")?;
        let tls_name = self.read_until(",");

        while self.peek_is_one_of(",") {
            self.expect(",")?;
            self.expect("[")?;
            if !self.peek_is_one_of("]") {
                let mut host = self.read_hosts(&tls_name, default_port)?;
                hosts.append(&mut host);
            }
            self.expect("]")?;
        }

        self.expect("]")?;

        Ok(hosts)
    }

    pub fn read_generation(&mut self) -> Result<u64> {
        let gen = self.read_until(",");
        if gen.is_empty() {
            return Err(Error::ParsePeersError("generation not specified".into()));
        }

        gen.parse::<u64>()
            .map_err(|_| Error::ParsePeersError(format!("expected generation but found {gen}")))
    }

    pub fn read_port(&mut self) -> Result<u16> {
        let port = self.read_until("],");
        if port.is_empty() {
            return Err(Error::ParsePeersError("port not specified".into()));
        }

        port.parse::<u16>()
            .map_err(|_| Error::ParsePeersError(format!("expected port but found {port}")))
    }

    pub fn read_hosts(&mut self, tls_name: &str, default_port: u16) -> Result<Vec<Host>> {
        let mut hosts = Vec::new();
        loop {
            let (addr, port) = self.read_addr_tuple()?;
            hosts.push(Host::new_tls(&addr, tls_name, port.unwrap_or(default_port)));

            match self.peek() {
                Some(&c) if c == ',' => self.expect(",")?,
                _ => break,
            }
        }

        Ok(hosts)
    }

    fn read_addr_tuple(&mut self) -> Result<(String, Option<u16>)> {
        let addr = self.read_addr_part()?;
        let port = match self.peek() {
            Some(&c) if c == ':' => {
                self.expect(":")?;
                Some(self.read_port()?)
            }
            _ => None,
        };
        Ok((addr, port))
    }

    fn read_addr_part(&mut self) -> Result<String> {
        let addr = match self.peek() {
            // ipv6 or dns
            Some(&c) if c == '[' => {
                self.expect("[")?;
                let res = self.read_until("]");
                self.expect("]")?;
                res
            }
            // ipv4
            _ => self.read_until(":,]"),
        };

        if addr.is_empty() {
            return Err(Error::ParsePeersError(
                "Empty address string for peer".into(),
            ));
        }

        Ok(addr)
    }

    pub fn read_until(&mut self, until: &str) -> String {
        let mut substr = String::new();
        loop {
            match self.peek() {
                Some(&c) if !until.contains(c) => {
                    substr.push(c);
                    self.next_char();
                }
                _ => {
                    return substr;
                }
            }
        }
    }

    fn peek(&mut self) -> Option<&char> {
        self.s.peek()
    }

    fn peek_is_one_of(&mut self, sc: &str) -> bool {
        if let Some(c) = self.s.peek() {
            return sc.contains(*c);
        }

        false
    }

    fn expect(&mut self, sc: &str) -> Result<()> {
        if let Some(c) = self.s.next() {
            if !sc.contains(c) {
                return Err(Error::InvalidArgument(format!(
                    "Expected one of `{sc}` but found {c:?}"
                )));
            }
            return Ok(());
        }

        Err(Error::InvalidArgument(format!(
            "Expected one of `{sc}` but EOF"
        )))
    }

    fn next_char(&mut self) -> Option<char> {
        self.s.next()
    }
}

#[cfg(test)]
mod tests {
    use super::{Host, PeersParser};

    #[test]
    fn parse_peers() {
        let (gen, hosts) = PeersParser::new("1234567,3000,[n1,t1,[192.168.4.10,192.168.3.10]],[n2,t2,[[2018::0002],[2018::0001]:4000]],[n3,t3,[foo1.aerocluster.com,foo2.aerocluster.new:3100]],[n4,t4,[foo2.aerocluster.com:5000]]")
        .parse()
        .expect("Error parsing peer_string");

        assert_eq!(gen, 1234567);
        assert_eq!(
            hosts,
            vec![
                Host::new_tls("192.168.4.10", "t1", 3000),
                Host::new_tls("192.168.3.10", "t1", 3000),
                Host::new_tls("2018::0002", "t2", 3000),
                Host::new_tls("2018::0001", "t2", 4000),
                Host::new_tls("foo1.aerocluster.com", "t3", 3000),
                Host::new_tls("foo2.aerocluster.new", "t3", 3100),
                Host::new_tls("foo2.aerocluster.com", "t4", 5000),
            ]
        );

        let (gen, hosts) = PeersParser::new("12,3010,[n1,t1,[192.168.4.10,192.168.3.10:3100]],[n2,t2,[[2018::0001]]],[n3,t3,[foo1.aerocluster.com:3100]],[n4,t4,[]]")
        .parse()
        .expect("Error parsing peer_string");

        assert_eq!(gen, 12);
        assert_eq!(
            hosts,
            vec![
                Host::new_tls("192.168.4.10", "t1", 3010),
                Host::new_tls("192.168.3.10", "t1", 3100),
                Host::new_tls("2018::0001", "t2", 3010),
                Host::new_tls("foo1.aerocluster.com", "t3", 3100),
            ]
        );

        let (gen, hosts) = PeersParser::new(
            "7,3000,[[BB924A0A129825A,,[127.0.0.1:3109]],[BB9A14D609EE096,,[127.0.0.1:3110]],[BB9A14D609EE099,t1,[127.0.0.1]]]",
        )
        .parse()
        .expect("Error parsing peer_string");

        assert_eq!(gen, 7);
        assert_eq!(
            hosts,
            vec![
                Host::new("127.0.0.1", 3109),
                Host::new("127.0.0.1", 3110),
                Host::new_tls("127.0.0.1", "t1", 3000)
            ]
        );

        let (gen, hosts) = PeersParser::new("0,3000,[]")
            .parse()
            .expect("Error parsing peer_string");

        assert_eq!(gen, 0);
        assert_eq!(hosts, vec![]);
    }

    #[test]
    fn read_addr_part() {
        assert_eq!(
            "foo".to_string(),
            PeersParser::new("foo:bar").read_addr_part().unwrap()
        );
        assert_eq!(
            "foo".to_string(),
            PeersParser::new("foo,bar").read_addr_part().unwrap()
        );
        assert_eq!(
            "foo".to_string(),
            PeersParser::new("foo").read_addr_part().unwrap()
        );
        assert_eq!(
            "foo.com".to_string(),
            PeersParser::new("foo.com").read_addr_part().unwrap()
        );
        assert!(PeersParser::new("").read_addr_part().is_err());
        assert!(PeersParser::new(",").read_addr_part().is_err());
        assert!(PeersParser::new(":").read_addr_part().is_err());
    }

    #[test]
    fn read_addr_tuple() {
        assert_eq!(
            ("foo".to_string(), None),
            PeersParser::new("foo").read_addr_tuple().unwrap()
        );
        assert_eq!(
            ("foo".to_string(), Some(3000)),
            PeersParser::new("foo:3000,").read_addr_tuple().unwrap()
        );
        assert_eq!(
            ("foo.com".to_string(), Some(3000)),
            PeersParser::new("foo.com:3000,").read_addr_tuple().unwrap()
        );
        assert_eq!(
            ("::1".to_string(), Some(3000)),
            PeersParser::new("[::1]:3000,").read_addr_tuple().unwrap()
        );
        assert!(PeersParser::new("").read_addr_tuple().is_err());
        assert!(PeersParser::new(",").read_addr_tuple().is_err());
        assert!(PeersParser::new(":").read_addr_tuple().is_err());
        assert!(PeersParser::new("foo:").read_addr_tuple().is_err());
    }
}
