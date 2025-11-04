use aerospike::as_val;

use proptest::prelude::*;

prop_compose! {
    pub fn value_i64()(i in any::<i64>()) -> aerospike::Value {
        aerospike::Value::Int(i)
    }
}

prop_compose! {
    pub fn value_f64()(f in any::<f64>()) -> aerospike::Value {
        as_val!(f)
    }
}

prop_compose! {
    pub fn value_bool()(b in any::<bool>()) -> aerospike::Value {
        aerospike::Value::Bool(b)
    }
}

prop_compose! {
    pub fn value_string()(s in ".{1,100}") -> aerospike::Value {
        aerospike::Value::String(s)
    }
}

prop_compose! {
    pub fn value_string_latin()(s in "[A-Za-z0-9]{1,100}") -> aerospike::Value {
        aerospike::Value::String(s)
    }
}

prop_compose! {
    pub fn value_blob(n: usize)(b in prop::collection::vec(any::<u8>(), 1..n)) -> aerospike::Value {
        as_val!(b)
    }
}

prop_compose! {
    pub fn value_list(n: usize)(l in prop::collection::vec(value_any(), 1..n)) -> aerospike::Value {
        as_val!(l)
    }
}

prop_compose! {
    pub fn value_map(n: usize)(m in prop::collection::hash_map(value_map_key(), value_any(), 1..n)) -> aerospike::Value {
        as_val!(m)
    }
}

// pub fn value_wildcard() -> impl Strategy<Value = aerospike::Value> {
//     Just(aerospike::Value::Wildcard)
// }

// pub fn value_infinity() -> impl Strategy<Value = aerospike::Value> {
//     Just(aerospike::Value::Infinity)
// }

pub fn value_map_key() -> impl Strategy<Value = aerospike::Value> {
    prop_oneof![value_string(), value_i64(),]
}

pub fn value_for_key() -> impl Strategy<Value = aerospike::Value> {
    prop_oneof![value_string(), value_i64(), value_blob(1000)]
}

pub fn value_any() -> impl Strategy<Value = aerospike::Value> {
    prop_oneof![
        value_i64(),
        value_f64(),
        value_bool(),
        value_string(),
        value_blob(10),
        value_complex(),
    ]
}

pub fn value_for_range_filter() -> impl Strategy<Value = (aerospike::Value, aerospike::Value)> {
    prop_oneof![
        (0..i64::MAX / 2, 0..i64::MAX / 2,).prop_map(|(v1, v2)| (as_val!(v1), as_val!(v1 + v2))),
        (value_string_latin(), value_string_latin())
            .prop_map(|(s1, s2)| (as_val!(s1.clone()), as_val!(format!("{}{}", s1, s2)))),
    ]
}

pub fn value_for_eq_filter() -> impl Strategy<Value = aerospike::Value> {
    prop_oneof![value_i64(), value_string_latin(), value_blob(10)]
}

pub fn value_simple() -> impl Strategy<Value = aerospike::Value> {
    prop_oneof![
        value_i64(),
        value_f64(),
        value_bool(),
        value_string(),
        value_blob(10),
    ]
}

pub fn value_complex() -> impl Strategy<Value = aerospike::Value> {
    let leaf = prop_oneof![
        value_i64(),
        value_f64(),
        value_bool(),
        value_string(),
        value_blob(10),
    ];
    leaf.prop_recursive(
        2, // 2 levels deep
        5, // Shoot for maximum size of 256 nodes
        1, // We put up to 2 items per collection
        |inner| {
            prop_oneof![
                // Take the inner strategy and make the two recursive cases.
                prop::collection::vec(inner.clone(), 0..2).prop_map(aerospike::Value::List),
                prop::collection::hash_map(value_map_key(), inner, 0..3)
                    .prop_map(aerospike::Value::HashMap),
            ]
        },
    )
}
