// Copyright 2015-2017 Aerospike, Inc.
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

use std::convert::From;

use value::Value;

/// Container object for a record bin, comprising a name and a value.
pub struct Bin<'a> {
    /// Bin name
    pub name: &'a str,

    /// Bin value
    pub value: Value,
}


impl<'a> Bin<'a> {
    /// Construct a new bin given a name and a value.
    pub fn new(name: &'a str, val: Value) -> Self {
        Bin {
            name: name,
            value: val,
        }
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
pub enum Bins<'a> {
    /// Read all bins.
    All,

    /// Read record header (generation, expiration) only.
    None,

    /// Read specified bin names only.
    Some(&'a [&'a str]),
}

impl<'a> Bins<'a> {
    /// Returns `true` if the bins selector is an `All` value.
    pub fn is_all(&self) -> bool {
        match *self {
            Bins::All => true,
            _ => false,
        }
    }

    /// Returns `true` if the bins selector is a `None` value.
    pub fn is_none(&self) -> bool {
        match *self {
            Bins::None => true,
            _ => false,
        }
    }
}

impl<'a> From<&'a [&'a str]> for Bins<'a> {
    fn from(bins: &'a [&'a str]) -> Self {
        Bins::Some(bins)
    }
}

impl<'a> From<&'a Vec<&'a str>> for Bins<'a> {
    fn from(bins: &'a Vec<&'a str>) -> Self {
        Bins::Some(bins.as_slice())
    }
}

impl<'a> From<&'a [&'a str; 1]> for Bins<'a> {
    fn from(bins: &'a [&'a str; 1]) -> Self {
        Bins::Some(bins)
    }
}

impl<'a> From<&'a [&'a str; 2]> for Bins<'a> {
    fn from(bins: &'a [&'a str; 2]) -> Self {
        Bins::Some(bins)
    }
}

impl<'a> From<&'a [&'a str; 3]> for Bins<'a> {
    fn from(bins: &'a [&'a str; 3]) -> Self {
        Bins::Some(bins)
    }
}

impl<'a> From<&'a [&'a str; 4]> for Bins<'a> {
    fn from(bins: &'a [&'a str; 4]) -> Self {
        Bins::Some(bins)
    }
}

impl<'a> From<&'a [&'a str; 5]> for Bins<'a> {
    fn from(bins: &'a [&'a str; 5]) -> Self {
        Bins::Some(bins)
    }
}

impl<'a> From<&'a [&'a str; 6]> for Bins<'a> {
    fn from(bins: &'a [&'a str; 6]) -> Self {
        Bins::Some(bins)
    }
}
