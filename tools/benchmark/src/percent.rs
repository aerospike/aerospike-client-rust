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

use std::cmp::Ordering;
use std::str::FromStr;

use rand::distributions::{Distribution, Standard};
use rand::Rng;

#[derive(Clone, Copy, Eq, PartialEq, PartialOrd, Debug)]
pub struct Percent(u8);

impl Percent {
    pub fn new(value: u8) -> Percent {
        Percent(value)
    }
}

impl FromStr for Percent {
    type Err = String;

    fn from_str(s: &str) -> Result<Percent, String> {
        if let Ok(pct) = u8::from_str(s) {
            if pct <= 100 {
                return Ok(Percent(pct));
            }
        }
        Err("Invalid percent value".into())
    }
}

impl Ord for Percent {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl Distribution<Percent> for Standard {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> Percent {
        let r: u32 = rng.gen();
        let pct = r % 101;
        Percent(pct as u8)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_percent_from_str() {
        assert_eq!(Percent::from_str("42"), Ok(Percent::new(42)));
        assert!(Percent::from_str("0.5").is_err());
        assert!(Percent::from_str("120").is_err());
        assert!(Percent::from_str("abc").is_err());
    }
}
