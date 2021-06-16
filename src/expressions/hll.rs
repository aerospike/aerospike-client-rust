// Copyright 2015-2020 Aerospike, Inc.
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

//! HLL Aerospike Filter Expressions.

use crate::expressions::{int_val, ExpOp, ExpType, ExpressionArgument, FilterExpression, MODIFY};
use crate::operations::hll::HLLPolicy;
use crate::Value;

const MODULE: i64 = 2;

#[doc(hidden)]
pub enum HllExpOp {
    Init = 0,
    Add = 1,
    Count = 50,
    Union = 51,
    UnionCount = 52,
    IntersectCount = 53,
    Similarity = 54,
    Describe = 55,
    MayContain = 56,
}

/// Create expression that creates a new HLL or resets an existing HLL.
pub fn init(
    policy: HLLPolicy,
    index_bit_count: FilterExpression,
    bin: FilterExpression,
) -> FilterExpression {
    init_with_min_hash(policy, index_bit_count, int_val(-1), bin)
}

/// Create expression that creates a new HLL or resets an existing HLL with minhash bits.
pub fn init_with_min_hash(
    policy: HLLPolicy,
    index_bit_count: FilterExpression,
    min_hash_count: FilterExpression,
    bin: FilterExpression,
) -> FilterExpression {
    add_write(
        bin,
        vec![
            ExpressionArgument::Value(Value::from(HllExpOp::Init as i64)),
            ExpressionArgument::FilterExpression(index_bit_count),
            ExpressionArgument::FilterExpression(min_hash_count),
            ExpressionArgument::Value(Value::from(policy.flags as i64)),
        ],
    )
}

/// Create expression that adds list values to a HLL set and returns HLL set.
/// The function assumes HLL bin already exists.
/// ```
/// use aerospike::operations::hll::HLLPolicy;
/// use aerospike::Value;
/// use aerospike::expressions::{gt, list_val, hll_bin, int_val};
/// use aerospike::expressions::hll::add;
///
/// // Add values to HLL bin "a" and check count > 7
/// let list = vec![Value::from(1)];
/// gt(add(HLLPolicy::default(), list_val(list), hll_bin("a".to_string())), int_val(7));
/// ```
pub fn add(policy: HLLPolicy, list: FilterExpression, bin: FilterExpression) -> FilterExpression {
    add_with_index_and_min_hash(policy, list, int_val(-1), int_val(-1), bin)
}

/// Create expression that adds values to a HLL set and returns HLL set.
/// If HLL bin does not exist, use `indexBitCount` to create HLL bin.
/// ```
/// use aerospike::operations::hll::HLLPolicy;
/// use aerospike::Value;
/// use aerospike::expressions::{gt, list_val, int_val, hll_bin};
/// use aerospike::expressions::hll::add_with_index;
///
/// // Add values to HLL bin "a" and check count > 7
/// let list = vec![Value::from(1)];
/// gt(add_with_index(HLLPolicy::default(), list_val(list), int_val(10), hll_bin("a".to_string())), int_val(7));
/// ```
pub fn add_with_index(
    policy: HLLPolicy,
    list: FilterExpression,
    index_bit_count: FilterExpression,
    bin: FilterExpression,
) -> FilterExpression {
    add_with_index_and_min_hash(policy, list, index_bit_count, int_val(-1), bin)
}

/// Create expression that adds values to a HLL set and returns HLL set. If HLL bin does not
/// exist, use `indexBitCount` and `minHashBitCount` to create HLL set.
/// ```
/// use aerospike::expressions::{gt, list_val, int_val, hll_bin};
/// use aerospike::operations::hll::HLLPolicy;
/// use aerospike::Value;
/// use aerospike::expressions::hll::add_with_index_and_min_hash;
///
/// // Add values to HLL bin "a" and check count > 7
/// let list = vec![Value::from(1)];
/// gt(add_with_index_and_min_hash(HLLPolicy::default(), list_val(list), int_val(10), int_val(20), hll_bin("a".to_string())), int_val(7));
/// ```
pub fn add_with_index_and_min_hash(
    policy: HLLPolicy,
    list: FilterExpression,
    index_bit_count: FilterExpression,
    min_hash_count: FilterExpression,
    bin: FilterExpression,
) -> FilterExpression {
    add_write(
        bin,
        vec![
            ExpressionArgument::Value(Value::from(HllExpOp::Add as i64)),
            ExpressionArgument::FilterExpression(list),
            ExpressionArgument::FilterExpression(index_bit_count),
            ExpressionArgument::FilterExpression(min_hash_count),
            ExpressionArgument::Value(Value::from(policy.flags as i64)),
        ],
    )
}

/// Create expression that returns estimated number of elements in the HLL bin.
///
/// ```
/// // HLL bin "a" count > 7
/// use aerospike::expressions::{gt, hll_bin, int_val};
/// use aerospike::expressions::hll::get_count;
/// gt(get_count(hll_bin("a".to_string())), int_val(7));
/// ```
pub fn get_count(bin: FilterExpression) -> FilterExpression {
    add_read(
        bin,
        ExpType::INT,
        vec![ExpressionArgument::Value(Value::from(
            HllExpOp::Count as i64,
        ))],
    )
}

/// Create expression that returns a HLL object that is the union of all specified HLL objects
/// in the list with the HLL bin.
///
/// ```
/// use aerospike::expressions::hll::get_union;
/// use aerospike::expressions::{hll_bin, blob_val};
///
/// // Union of HLL bins "a" and "b"
/// get_union(hll_bin("a".to_string()), hll_bin("b".to_string()));
///
/// // Union of local HLL list with bin "b"
/// let blob: Vec<u8> = vec![];
/// get_union(hll_bin("b".to_string()), blob_val(blob));
/// ```
pub fn get_union(list: FilterExpression, bin: FilterExpression) -> FilterExpression {
    add_read(
        bin,
        ExpType::HLL,
        vec![
            ExpressionArgument::Value(Value::from(HllExpOp::Union as i64)),
            ExpressionArgument::FilterExpression(list),
        ],
    )
}

/// Create expression that returns estimated number of elements that would be contained by
/// the union of these HLL objects.
///
/// ```
/// use aerospike::expressions::hll::get_union_count;
/// use aerospike::expressions::{hll_bin, blob_val};
///
/// // Union count of HLL bins "a" and "b"
/// get_union_count(hll_bin("a".to_string()), hll_bin("b".to_string()));
///
/// // Union count of local HLL list with bin "b"
/// let blob: Vec<u8> = vec![];
/// get_union_count(hll_bin("b".to_string()), blob_val(blob));
/// ```
pub fn get_union_count(list: FilterExpression, bin: FilterExpression) -> FilterExpression {
    add_read(
        bin,
        ExpType::INT,
        vec![
            ExpressionArgument::Value(Value::from(HllExpOp::UnionCount as i64)),
            ExpressionArgument::FilterExpression(list),
        ],
    )
}

/// Create expression that returns estimated number of elements that would be contained by
/// the intersection of these HLL objects.
///
/// ```
/// use aerospike::expressions::{hll_bin, blob_val};
/// use aerospike::expressions::hll::get_union_count;
///
/// // Intersect count of HLL bins "a" and "b"
/// get_union_count(hll_bin("a".to_string()), hll_bin("b".to_string()));
///
/// // Intersect count of local HLL list with bin "b"
/// let blob: Vec<u8> = vec![];
/// get_union_count(hll_bin("b".to_string()), blob_val(blob));
/// ```
pub fn get_intersect_count(list: FilterExpression, bin: FilterExpression) -> FilterExpression {
    add_read(
        bin,
        ExpType::INT,
        vec![
            ExpressionArgument::Value(Value::from(HllExpOp::IntersectCount as i64)),
            ExpressionArgument::FilterExpression(list),
        ],
    )
}

/// Create expression that returns estimated similarity of these HLL objects as a 64 bit float.
///
/// ```
/// use aerospike::expressions::{hll_bin, ge, float_val};
/// use aerospike::expressions::hll::get_similarity;
///
/// // Similarity of HLL bins "a" and "b" >= 0.75
/// ge(get_similarity(hll_bin("a".to_string()), hll_bin("b".to_string())), float_val(0.75));
/// ```
pub fn get_similarity(list: FilterExpression, bin: FilterExpression) -> FilterExpression {
    add_read(
        bin,
        ExpType::FLOAT,
        vec![
            ExpressionArgument::Value(Value::from(HllExpOp::Similarity as i64)),
            ExpressionArgument::FilterExpression(list),
        ],
    )
}

/// Create expression that returns `indexBitCount` and `minHashBitCount` used to create HLL bin
/// in a list of longs. `list[0]` is `indexBitCount` and `list[1]` is `minHashBitCount`.
///
/// ```
/// use aerospike::expressions::{ExpType, lt, int_val, hll_bin};
/// use aerospike::expressions::lists::{get_by_index};
/// use aerospike::operations::lists::ListReturnType;
/// use aerospike::expressions::hll::describe;
///
/// // Bin "a" `indexBitCount` < 10
/// lt(get_by_index(ListReturnType::Values, ExpType::INT, int_val(0), describe(hll_bin("a".to_string())), &[]), int_val(10));
/// ```
pub fn describe(bin: FilterExpression) -> FilterExpression {
    add_read(
        bin,
        ExpType::LIST,
        vec![ExpressionArgument::Value(Value::from(
            HllExpOp::Describe as i64,
        ))],
    )
}

/// Create expression that returns one if HLL bin may contain all items in the list.
///
/// ```
/// use aerospike::Value;
/// use aerospike::expressions::{eq, list_val, hll_bin, int_val};
/// use aerospike::expressions::hll::may_contain;
/// let list: Vec<Value> = vec![Value::from("x")];
///
/// // Bin "a" may contain value "x"
/// eq(may_contain(list_val(list), hll_bin("a".to_string())), int_val(1));
/// ```
pub fn may_contain(list: FilterExpression, bin: FilterExpression) -> FilterExpression {
    add_read(
        bin,
        ExpType::INT,
        vec![
            ExpressionArgument::Value(Value::from(HllExpOp::MayContain as i64)),
            ExpressionArgument::FilterExpression(list),
        ],
    )
}

#[doc(hidden)]
fn add_read(
    bin: FilterExpression,
    return_type: ExpType,
    arguments: Vec<ExpressionArgument>,
) -> FilterExpression {
    FilterExpression {
        cmd: Some(ExpOp::Call),
        val: None,
        bin: Some(Box::new(bin)),
        flags: Some(MODULE),
        module: Some(return_type),
        exps: None,
        arguments: Some(arguments),
    }
}

#[doc(hidden)]
fn add_write(bin: FilterExpression, arguments: Vec<ExpressionArgument>) -> FilterExpression {
    FilterExpression {
        cmd: Some(ExpOp::Call),
        val: None,
        bin: Some(Box::new(bin)),
        flags: Some(MODULE | MODIFY),
        module: Some(ExpType::HLL),
        exps: None,
        arguments: Some(arguments),
    }
}
