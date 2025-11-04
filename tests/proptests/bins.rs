use crate::proptests::value::value_any;

use aerospike::{Bin, Bins};

use proptest::prelude::*;

pub fn valid_bin_name() -> impl Strategy<Value = String> {
    prop::string::string_regex("[\\w\\d]{1,3}")
        .unwrap()
        .prop_filter("max 14 bytes", |s| s.bytes().len() <= 14)
}

pub fn latin_bin_name() -> impl Strategy<Value = String> {
    prop::string::string_regex("[A-Za-z0-9]{1,14}")
        .unwrap()
        .prop_filter("max 14 bytes", |s| s.bytes().len() <= 14)
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
        bin_names(n).prop_map(|bins| Bins::Some(bins)),
    ]
}

pub fn latin_bins(n: u8) -> impl Strategy<Value = Bins> {
    prop_oneof![
        // Just(Bins::None),
        Just(Bins::All),
        latin_bin_names(n).prop_map(|bins| Bins::Some(bins)),
    ]
}

prop_compose! {
    pub fn bin()(name in valid_bin_name(), val in value_any()) -> Bin {
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
