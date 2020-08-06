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

use crate::errors::{ErrorKind, Result};
use crate::Host;
use std::iter::Peekable;
use std::str::Chars;

pub struct Parser<'a> {
    s: Peekable<Chars<'a>>,
    default_port: u16,
}

impl<'a> Parser<'a> {
    pub fn new(s: &'a str, default_port: u16) -> Self {
        Parser {
            s: s.chars().peekable(),
            default_port,
        }
    }

    pub fn read_hosts(&mut self) -> Result<Vec<Host>> {
        let mut hosts = Vec::new();
        loop {
            let addr = self.read_addr_tuple()?;
            let (host, _tls_name, port) = match addr.len() {
                3 => (addr[0].clone(), Some(addr[1].clone()), addr[2].parse()?),
                2 => {
                    if let Ok(port) = addr[1].parse() {
                        (addr[0].clone(), None, port)
                    } else {
                        (addr[0].clone(), Some(addr[1].clone()), self.default_port)
                    }
                }
                1 => (addr[0].clone(), None, self.default_port),
                _ => bail!(ErrorKind::InvalidArgument(
                    "Invalid address string".to_string()
                )),
            };
            // TODO: add TLS name
            hosts.push(Host::new(&host, port));

            match self.peek() {
                Some(&c) if c == ',' => self.next_char(),
                _ => break,
            };
        }

        Ok(hosts)
    }

    fn read_addr_tuple(&mut self) -> Result<Vec<String>> {
        let mut parts = Vec::new();
        loop {
            let part = self.read_addr_part()?;
            parts.push(part);
            match self.peek() {
                Some(&c) if c == ':' => self.next_char(),
                _ => break,
            };
        }
        Ok(parts)
    }

    fn read_addr_part(&mut self) -> Result<String> {
        let mut substr = String::new();
        loop {
            match self.peek() {
                Some(&c) if c != ':' && c != ',' => {
                    substr.push(c);
                    self.next_char();
                }
                _ => {
                    return if substr.is_empty() {
                        bail!(ErrorKind::InvalidArgument(
                            "Invalid address string".to_string()
                        ))
                    } else {
                        Ok(substr)
                    }
                }
            }
        }
    }

    fn peek(&mut self) -> Option<&char> {
        self.s.peek()
    }

    fn next_char(&mut self) -> Option<char> {
        self.s.next()
    }
}

#[cfg(test)]
mod tests {
    use super::{Host, Parser};

    #[test]
    fn read_addr_part() {
        assert_eq!(
            "foo".to_string(),
            Parser::new("foo:bar", 3000).read_addr_part().unwrap()
        );
        assert_eq!(
            "foo".to_string(),
            Parser::new("foo,bar", 3000).read_addr_part().unwrap()
        );
        assert_eq!(
            "foo".to_string(),
            Parser::new("foo", 3000).read_addr_part().unwrap()
        );
        assert!(Parser::new("", 3000).read_addr_part().is_err());
        assert!(Parser::new(",", 3000).read_addr_part().is_err());
        assert!(Parser::new(":", 3000).read_addr_part().is_err());
    }

    #[test]
    fn read_addr_tuple() {
        assert_eq!(
            vec!["foo".to_string()],
            Parser::new("foo", 3000).read_addr_tuple().unwrap()
        );
        assert_eq!(
            vec!["foo".to_string(), "bar".to_string()],
            Parser::new("foo:bar", 3000).read_addr_tuple().unwrap()
        );
        assert_eq!(
            vec!["foo".to_string()],
            Parser::new("foo,", 3000).read_addr_tuple().unwrap()
        );
        assert!(Parser::new("", 3000).read_addr_tuple().is_err());
        assert!(Parser::new(",", 3000).read_addr_tuple().is_err());
        assert!(Parser::new(":", 3000).read_addr_tuple().is_err());
        assert!(Parser::new("foo:", 3000).read_addr_tuple().is_err());
    }

    #[test]
    fn read_hosts() {
        assert_eq!(
            vec![Host::new("foo", 3000)],
            Parser::new("foo", 3000).read_hosts().unwrap()
        );
        assert_eq!(
            vec![Host::new("foo", 3000)],
            Parser::new("foo:bar", 3000).read_hosts().unwrap()
        );
        assert_eq!(
            vec![Host::new("foo", 1234)],
            Parser::new("foo:1234", 3000).read_hosts().unwrap()
        );
        assert_eq!(
            vec![Host::new("foo", 1234)],
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
