use aerospike::{as_key, Key};

use crate::proptests::value::*;
use proptest::prelude::*;

prop_compose! {
    pub fn any_key(ns: String, set_name: String)(value in value_for_key()) -> Key {
        as_key!(ns.clone(), set_name.clone(), value)
    }
}
