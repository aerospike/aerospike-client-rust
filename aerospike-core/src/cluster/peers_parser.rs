// Copyright 2015-2024 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::cluster::peers::Peer;
use crate::errors::{Error, Result};
use crate::Host;
use std::collections::HashMap;
use std::iter::Peekable;
use std::str::Chars;

pub struct PeersParser<'a> {
    s: Peekable<Chars<'a>>,
    ip_map: Option<&'a HashMap<String, String>>,
}

/// Result of parsing a peers response: generation number and list of peers.
pub struct PeersParseResult {
    pub generation: u64,
    pub peers: Vec<Peer>,
}

impl<'a> PeersParser<'a> {
    pub fn new(s: &'a str) -> Self {
        PeersParser {
            s: s.chars().peekable(),
            ip_map: None,
        }
    }

    /// Apply the `client_policy.ip_map` substitution to every host parsed
    /// out of the response — rewrites hosts at parse time (not later in
    /// the validator).
    pub fn with_ip_map(mut self, ip_map: Option<&'a HashMap<String, String>>) -> Self {
        self.ip_map = ip_map;
        self
    }

    /// Parses the peers response string and returns generation + peer list.
    ///
    /// Format: `generation,defaultPort,[peer1],[peer2],...`
    /// Where each peer is: `[nodeName,tlsName,[host1:port,host2:port,...]]`
    pub fn parse(&mut self) -> Result<PeersParseResult> {
        let gen = self.read_generation()?;
        self.expect(",")?;
        let default_port = self.read_port()?;

        let mut peers = vec![];

        // Server format: gen,port,[[peer1],[peer2],...]
        // The peer list is wrapped in outer brackets.
        match self.peek() {
            Some(&',') => {}
            _ => {
                return Ok(PeersParseResult {
                    generation: gen,
                    peers,
                })
            }
        }
        self.expect(",")?;
        self.expect("[")?;

        // Read peers until closing outer bracket
        loop {
            if self.peek_is_one_of("]") {
                break;
            }
            if let Some(peer) = self.parse_peer(default_port)? {
                peers.push(peer);
            }
            if !self.peek_is_one_of(",") {
                break;
            }
            self.expect(",")?;
        }

        self.expect("]")?;

        Ok(PeersParseResult {
            generation: gen,
            peers,
        })
    }

    /// Parses a single peer entry: `[nodeName,tlsName,[host1,host2,...]]`
    fn parse_peer(&mut self, default_port: u16) -> Result<Option<Peer>> {
        self.expect("[")?;
        // Peer may be empty: `[]`
        if self.peek_is_one_of("]") {
            self.expect("]")?;
            return Ok(None);
        }

        let node_name = self.read_until(",");
        self.expect(",")?;
        let tls_name = self.read_until(",");

        let mut hosts = Vec::new();
        while self.peek_is_one_of(",") {
            self.expect(",")?;
            self.expect("[")?;
            if !self.peek_is_one_of("]") {
                let mut h = self.read_hosts(&tls_name, default_port)?;
                hosts.append(&mut h);
            }
            self.expect("]")?;
        }

        self.expect("]")?;

        Ok(Some(Peer {
            node_name,
            tls_name,
            hosts,
            replace_node: None,
            from_node_name: None,
        }))
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
                Some(&',') => self.expect(",")?,
                _ => break,
            }
        }

        Ok(hosts)
    }

    fn read_addr_tuple(&mut self) -> Result<(String, Option<u16>)> {
        let addr = self.read_addr_part()?;
        let port = match self.peek() {
            Some(&':') => {
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
            Some(&'[') => {
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

        // Apply ip_map substitution at parse time, matching Java's
        // PeerParser.parseHost. Done here so every host (regardless of how
        // it's later consumed) sees a consistent address.
        if let Some(map) = self.ip_map {
            if let Some(mapped) = map.get(&addr) {
                return Ok(mapped.clone());
            }
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
    use super::*;

    #[test]
    fn parse_peers() {
        // Server format: gen,port,[[peer1],[peer2],...]
        let result = PeersParser::new("1234567,3000,[[n1,t1,[192.168.4.10,192.168.3.10]],[n2,t2,[[2018::0002],[2018::0001]:4000]],[n3,t3,[foo1.aerocluster.com,foo2.aerocluster.new:3100]],[n4,t4,[foo2.aerocluster.com:5000]]]")
        .parse()
        .expect("Error parsing peer_string");

        assert_eq!(result.generation, 1234567);
        assert_eq!(result.peers.len(), 4);

        assert_eq!(result.peers[0].node_name, "n1");
        assert_eq!(result.peers[0].tls_name, "t1");
        assert_eq!(
            result.peers[0].hosts,
            vec![
                Host::new_tls("192.168.4.10", "t1", 3000),
                Host::new_tls("192.168.3.10", "t1", 3000),
            ]
        );

        assert_eq!(result.peers[1].node_name, "n2");
        assert_eq!(
            result.peers[1].hosts,
            vec![
                Host::new_tls("2018::0002", "t2", 3000),
                Host::new_tls("2018::0001", "t2", 4000),
            ]
        );

        assert_eq!(result.peers[2].node_name, "n3");
        assert_eq!(
            result.peers[2].hosts,
            vec![
                Host::new_tls("foo1.aerocluster.com", "t3", 3000),
                Host::new_tls("foo2.aerocluster.new", "t3", 3100),
            ]
        );

        assert_eq!(result.peers[3].node_name, "n4");
        assert_eq!(
            result.peers[3].hosts,
            vec![Host::new_tls("foo2.aerocluster.com", "t4", 5000)]
        );

        // Second test case
        let result = PeersParser::new("12,3010,[[n1,t1,[192.168.4.10,192.168.3.10:3100]],[n2,t2,[[2018::0001]]],[n3,t3,[foo1.aerocluster.com:3100]],[n4,t4,[]]]")
        .parse()
        .expect("Error parsing peer_string");

        assert_eq!(result.generation, 12);
        assert_eq!(result.peers.len(), 4);
        assert_eq!(result.peers[0].hosts.len(), 2);
        assert_eq!(result.peers[1].hosts.len(), 1);
        assert_eq!(result.peers[2].hosts.len(), 1);
        assert_eq!(result.peers[3].hosts.len(), 0); // n4 has empty host list

        // Third test case
        let result = PeersParser::new(
            "7,3000,[[BB924A0A129825A,,[127.0.0.1:3109]],[BB9A14D609EE096,,[127.0.0.1:3110]],[BB9A14D609EE099,t1,[127.0.0.1]]]",
        )
        .parse()
        .expect("Error parsing peer_string");

        assert_eq!(result.generation, 7);
        assert_eq!(result.peers.len(), 3);
        assert_eq!(result.peers[0].node_name, "BB924A0A129825A");
        assert_eq!(result.peers[0].tls_name, "");
        assert_eq!(result.peers[0].hosts, vec![Host::new("127.0.0.1", 3109)]);
        assert_eq!(result.peers[1].node_name, "BB9A14D609EE096");
        assert_eq!(result.peers[1].hosts, vec![Host::new("127.0.0.1", 3110)]);
        assert_eq!(result.peers[2].node_name, "BB9A14D609EE099");
        assert_eq!(result.peers[2].tls_name, "t1");
        assert_eq!(
            result.peers[2].hosts,
            vec![Host::new_tls("127.0.0.1", "t1", 3000)]
        );

        // Empty peers list
        let result = PeersParser::new("0,3000,[]")
            .parse()
            .expect("Error parsing peer_string");

        assert_eq!(result.generation, 0);
        assert_eq!(result.peers.len(), 0);
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
