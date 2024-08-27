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

use crate::value::Value;
#[cfg(feature = "serialization")]
use serde::Serialize;
use std::convert::From;

/// Container object for a record bin, comprising a name and a value.
pub struct Bin {
    /// Bin name
    pub name: String,

    /// Bin value
    pub value: Value,
}

impl Bin {
    /// Construct a new bin given a name and a value.
    pub fn new(name: &str, val: Value) -> Self {
        Bin { name: name.to_string(), value: val }
    }
}

impl AsRef<Bin> for Bin {
    fn as_ref(&self) -> &Self {
        self
    }
}

/// Construct a new bin from a name and an optional value (defaults to the empty value `nil`).
#[macro_export]
macro_rules! as_bin {
    ($bin_name:expr, None) => {{
        $crate::Bin::new($bin_name, $crate::Value::Nil)
    }};
    ($bin_name:expr, $val:expr) => {{
        $crate::Bin::new($bin_name, $crate::Value::from($val))
    }};
}

/// Specify which, if any, bins to return in read operations.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serialization", derive(Serialize))]
pub enum Bins {
    /// Read all bins.
    All,

    /// Read record header (generation, expiration) only.
    None,

    /// Read specified bin names only.
    Some(Vec<String>),
}

impl Bins {
    /// Returns `true` if the bins selector is an `All` value.
    pub const fn is_all(&self) -> bool {
        matches!(*self, Bins::All)
    }

    /// Returns `true` if the bins selector is a `None` value.
    pub const fn is_none(&self) -> bool {
        matches!(*self, Bins::None)
    }
}

impl From<&[&str]> for Bins {
    fn from(bins: &[&str]) -> Self {
        let bins = bins.iter().cloned().map(String::from).collect();
        Bins::Some(bins)
    }
}

impl From<[&str; 1]> for Bins {
    fn from(bins: [&str; 1]) -> Self {
        let bins = bins.iter().cloned().map(String::from).collect();
        Bins::Some(bins)
    }
}

impl From<[&str; 2]> for Bins {
    fn from(bins: [&str; 2]) -> Self {
        let bins = bins.iter().cloned().map(String::from).collect();
        Bins::Some(bins)
    }
}

impl From<[&str; 3]> for Bins {
    fn from(bins: [&str; 3]) -> Self {
        let bins = bins.iter().cloned().map(String::from).collect();
        Bins::Some(bins)
    }
}

impl From<[&str; 4]> for Bins {
    fn from(bins: [&str; 4]) -> Self {
        let bins = bins.iter().cloned().map(String::from).collect();
        Bins::Some(bins)
    }
}

impl From<[&str; 5]> for Bins {
    fn from(bins: [&str; 5]) -> Self {
        let bins = bins.iter().cloned().map(String::from).collect();
        Bins::Some(bins)
    }
}

impl From<[&str; 6]> for Bins {
    fn from(bins: [&str; 6]) -> Self {
        let bins = bins.iter().cloned().map(String::from).collect();
        Bins::Some(bins)
    }
}

#[cfg(test)]
mod tests {
    use super::{Bins, From};

    #[test]
    fn into_bins() {
        let bin_names = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let expected = Bins::Some(bin_names);

        assert_eq!(expected, Bins::from(["a", "b", "c"]));
    }
}
