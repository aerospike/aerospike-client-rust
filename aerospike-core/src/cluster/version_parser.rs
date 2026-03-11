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
use regex::Regex;
use std::sync::LazyLock;

#[derive(Debug, Default, PartialEq, Eq, PartialOrd, Clone)]
/// Holds version numbers.
pub struct Version {
    /// Major value.
    pub major: u64,
    /// Minor value.
    pub minor: u64,
    /// Patch value.
    pub patch: u64,
    /// Build value.
    pub build: u64,
}

impl Version {
    /// Initializes a version number.
    pub const fn new(major: u64, minor: u64, patch: u64, build: u64) -> Self {
        Version {
            major,
            minor,
            patch,
            build,
        }
    }
}

impl Version {
    /// Server supports partition scans.
    pub fn supports_partition_scan(&self) -> bool {
        self >= &Version::new(4, 9, 0, 3)
    }

    /// Server supports query-show command.
    pub fn supports_query_show(&self) -> bool {
        self >= &Version::new(5, 7, 0, 0)
    }

    /// Server supports batch-index commands.
    pub fn supports_batch_any(&self) -> bool {
        self >= &Version::new(6, 0, 0, 0)
    }

    /// Server supports partition queries.
    pub fn supports_partition_query(&self) -> bool {
        self >= &Version::new(6, 0, 0, 0)
    }

    /// Server supports app-id.
    pub fn supports_app_id(&self) -> bool {
        self >= &Version::new(8, 1, 0, 0)
    }
}

#[derive(Debug)]
pub struct VersionParser<'a> {
    s: &'a str,
}

impl<'a> VersionParser<'a> {
    pub const fn new(s: &'a str) -> Self {
        VersionParser { s }
    }

    pub fn parse(&self) -> Result<Version> {
        static RE: LazyLock<Regex> = LazyLock::new(|| {
            Regex::new(r"^(?P<major>(\d+))\.(?P<minor>(\d+))\.(?P<patch>(\d+))\.(?P<build>(\d+))")
                .unwrap()
        });

        if !RE.is_match(self.s) {
            return Err(Error::ClientError(format!(
                "Could not parse node version string `{}`",
                self.s
            )));
        }

        // 'm' is a 'Match', and 'as_str()' returns the matching part of the haystack.
        let (major, minor, patch, build) = RE
            .captures(self.s)
            .map(|caps| {
                (
                    caps.name("major").unwrap().as_str(),
                    caps.name("minor").unwrap().as_str(),
                    caps.name("patch").unwrap().as_str(),
                    caps.name("build").unwrap().as_str(),
                )
            })
            .unwrap();

        let (major, minor, patch, build) = (
            major.parse::<u64>().unwrap(),
            minor.parse::<u64>().unwrap(),
            patch.parse::<u64>().unwrap(),
            build.parse::<u64>().unwrap(),
        );

        Ok(Version {
            major,
            minor,
            patch,
            build,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{Version, VersionParser};

    #[test]
    fn comparisons() {
        assert!(Version::new(1, 0, 0, 0) < Version::new(1, 0, 0, 1));
        assert!(Version::new(1, 0, 0, 0) <= Version::new(1, 0, 0, 1));

        assert!(Version::new(2, 1, 3, 2) > Version::new(2, 1, 3, 1));
        assert!(Version::new(2, 1, 3, 2) >= Version::new(2, 1, 3, 1));

        assert!(Version::new(4, 3, 2, 1) == Version::new(4, 3, 2, 1));
        assert!(Version::new(4, 3, 2, 1) <= Version::new(4, 3, 2, 1));
        assert!(Version::new(4, 3, 2, 1) >= Version::new(4, 3, 2, 1));
    }

    #[test]
    fn parse() {
        assert_eq!(
            Version::new(1, 2, 3, 4),
            VersionParser::new("1.2.3.4").parse().unwrap()
        );

        assert_eq!(
            Version::new(1, 2, 3, 4),
            VersionParser::new("1.2.3.4-asdfasdf").parse().unwrap()
        );

        assert_eq!(
            Version::new(1111, 2222, 333, 14),
            VersionParser::new("1111.2222.333.14").parse().unwrap()
        );

        assert_eq!(
            Version::new(1111, 2222, 333, 14),
            VersionParser::new("1111.2222.333.14.adtrwadfdsfk")
                .parse()
                .unwrap()
        );

        assert_eq!(
            Version::new(1111, 2222, 333, 14),
            VersionParser::new("1111.2222.333.14adtrwadfdsfk")
                .parse()
                .unwrap()
        );

        let invalid = vec![
            ".1.2.3.4",
            "1.2.3-asdfasdf",
            "1111..333.14",
            "1111.a2222.333.14-asdfasdf",
        ];

        for iv in invalid {
            assert!(VersionParser::new(iv).parse().is_err());
        }
    }
}
