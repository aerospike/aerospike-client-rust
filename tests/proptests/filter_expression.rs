use crate::proptests::bins::*;
use crate::proptests::value::*;

use aerospike_core::expressions::*;

use proptest::prelude::*;

pub fn true_or_false_filter_expression() -> impl Strategy<Value = Option<Expression>> {
    prop_oneof![true_filter_expression(), false_filter_expression(),]
}

pub fn true_filter_expression() -> impl Strategy<Value = Option<Expression>> {
    exp_short_true_with().prop_map(|fe| Some(fe))
}

pub fn false_filter_expression() -> impl Strategy<Value = Option<Expression>> {
    exp_short_false_with().prop_map(|fe| Some(fe))
}

pub fn filter_expression() -> impl Strategy<Value = Option<Expression>> {
    prop_oneof![Just(None), exp_tree().prop_map(|fe| Some(fe))]
}

pub fn exp_simple() -> impl Strategy<Value = Expression> {
    prop_oneof![
        exp_int_bin_eq(),
        exp_float_bin_eq(),
        exp_str_bin_eq(),
        exp_blob_bin_eq(100),
        exp_list_bin_eq(5),
        exp_map_bin_eq(5),
        exp_int_bin_ne(),
        exp_float_bin_ne(),
        exp_str_bin_ne(),
        exp_blob_bin_ne(100),
        exp_list_bin_ne(5),
        exp_map_bin_ne(5),
    ]
}

pub fn exp_tree() -> impl Strategy<Value = Expression> {
    let leaf = exp_simple();
    leaf.prop_recursive(
        5, // 2 levels deep
        5, // Shoot for maximum size of 256 nodes
        3, // We put up to 2 items per collection
        |inner| {
            prop_oneof![
                inner.clone().prop_map(|m| exp_or2(m.clone(), m.clone())),
                inner.clone().prop_map(|m| exp_and2(m.clone(), m.clone())),
            ]
        },
    )
}

pub fn exp_or2(l: Expression, r: Expression) -> Expression {
    or(vec![l, r])
}

pub fn exp_and2(l: Expression, r: Expression) -> Expression {
    and(vec![l, r])
}

prop_compose! {
    pub fn exp_short_false_with()(e in exp_tree()) -> Expression {
        and(vec![bool_val(false), e])
    }
}

prop_compose! {
    pub fn exp_short_true_with()(e in exp_tree()) -> Expression {
        or(vec![bool_val(true), e])
    }
}

prop_compose! {
    pub fn exp_int_bin_eq()(name in valid_bin_name(), val in any::<i64>()) -> Expression {
        eq(int_bin(name), int_val(val))
    }
}

prop_compose! {
    pub fn exp_float_bin_eq()(name in valid_bin_name(), val in any::<f64>()) -> Expression {
        eq(float_bin(name), float_val(val))
    }
}

prop_compose! {
    pub fn exp_str_bin_eq()(name in valid_bin_name(), val in ".{1,100}") -> Expression {
        eq(string_bin(name), string_val(val))
    }
}

prop_compose! {
    pub fn exp_blob_bin_eq(n: usize)(name in valid_bin_name(), val in prop::collection::vec(any::<u8>(), 1..n)) -> Expression {
        eq(blob_bin(name), blob_val(val))
    }
}

prop_compose! {
    pub fn exp_list_bin_eq(n: usize)(name in valid_bin_name(), val in prop::collection::vec(value_simple(), 1..n)) -> Expression {
        eq(list_bin(name), list_val(val))
    }
}

prop_compose! {
    pub fn exp_map_bin_eq(n: usize)(name in valid_bin_name(), val in prop::collection::hash_map(value_map_key(), value_simple(), 1..n)) -> Expression {
        eq(map_bin(name), map_val(val))
    }
}

prop_compose! {
    pub fn exp_int_bin_ne()(name in valid_bin_name(), val in any::<i64>()) -> Expression {
        ne(int_bin(name), int_val(val))
    }
}

prop_compose! {
    pub fn exp_float_bin_ne()(name in valid_bin_name(), val in any::<f64>()) -> Expression {
        ne(float_bin(name), float_val(val))
    }
}

prop_compose! {
    pub fn exp_str_bin_ne()(name in valid_bin_name(), val in ".{1,100}") -> Expression {
        ne(string_bin(name), string_val(val))
    }
}

prop_compose! {
    pub fn exp_blob_bin_ne(n: usize)(name in valid_bin_name(), val in prop::collection::vec(any::<u8>(), 1..n)) -> Expression {
        ne(blob_bin(name), blob_val(val))
    }
}

prop_compose! {
    pub fn exp_list_bin_ne(n: usize)(name in valid_bin_name(), val in prop::collection::vec(value_simple(), 1..n)) -> Expression {
        ne(list_bin(name), list_val(val))
    }
}

prop_compose! {
    pub fn exp_map_bin_ne(n: usize)(name in valid_bin_name(), val in prop::collection::hash_map(value_map_key(), value_simple(), 1..n)) -> Expression {
        ne(map_bin(name), map_val(val))
    }
}
