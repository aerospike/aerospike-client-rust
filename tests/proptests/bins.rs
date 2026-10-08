// Copyright 2015-2026 Aerospike, Inc.
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

use crate::proptests::value::value_any;

use aerospike::{Bin, Bins};

use proptest::prelude::*;

pub fn valid_bin_name() -> impl Strategy<Value = String> {
    prop::string::string_regex("[\\w\\d]{1,3}")
        .unwrap()
        .prop_filter("max 14 bytes", |s| s.len() <= 14)
}

pub fn latin_bin_name() -> impl Strategy<Value = String> {
    prop::string::string_regex("[A-Za-z0-9]{1,14}")
        .unwrap()
        .prop_filter("max 14 bytes", |s| s.len() <= 14)
}

pub fn long_latin_bin_name() -> impl Strategy<Value = String> {
    prop::string::string_regex("[A-Za-z0-9]{8,10}")
        .unwrap()
        .prop_filter("max 14 bytes", |s| s.len() <= 14)
}

prop_compose! {
    pub fn bin_names(n: u8)(vec in prop::collection::vec(valid_bin_name(), 1..n as usize)) -> Vec<String> {
       vec
   }
}

prop_compose! {
    pub fn latin_bin_names(n: u8)(vec in prop::collection::vec(latin_bin_name(), 1..n as usize)) -> Vec<String> {
       vec
   }
}

pub fn bins(n: u8) -> impl Strategy<Value = Bins> {
    prop_oneof![
        Just(Bins::None),
        Just(Bins::All),
        bin_names(n).prop_map(Bins::Some),
    ]
}

pub fn latin_bins(n: u8) -> impl Strategy<Value = Bins> {
    prop_oneof![
        // Just(Bins::None),
        Just(Bins::All),
        latin_bin_names(n).prop_map(Bins::Some),
    ]
}

prop_compose! {
    pub fn bin()(name in valid_bin_name(), val in value_any()) -> Bin {
        Bin::new(name, val)
    }
}

prop_compose! {
    pub fn unique_bin()(name in long_latin_bin_name(), val in value_any()) -> Bin {
        Bin::new(name, val)
    }
}

prop_compose! {
    pub fn boxed_bin()(name in valid_bin_name(), val in value_any()) -> Box<Bin> {
        Box::new(Bin::new(name, val))
    }
}

prop_compose! {
    pub fn bin_latin()(name in latin_bin_name(), val in value_any()) -> Bin {
        Bin::new(name, val)
    }
}

prop_compose! {
    pub fn many_bins(n: u8)(vec in prop::collection::vec(bin(), 1..n as usize)) -> Vec<Bin> {
       vec
   }
}

prop_compose! {
    pub fn many_unique_bins(n: u8)(vec in prop::collection::vec(unique_bin(), 1..n as usize)) -> Vec<Bin> {
       vec
   }
}
