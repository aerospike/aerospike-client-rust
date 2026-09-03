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

//! Types for `ORDER BY <bin> LIMIT k` ("Top-K") queries.
//!
//! See `Statement::set_order_by`/`Statement::set_top_k` for the client API.
//! The server supports wire-level pushdown; this client reduces results
//! client-side instead (TODO: use pushdown).
//!
/// Scalar comparator type for a Top-K order-by key.
///
/// The order-by value type must be declared explicitly.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum OrderByType {
    /// 64-bit signed integer order key.
    Integer = 1,
    /// 64-bit float order key.
    Double = 2,
    /// String order key.
    String = 3,
    /// Raw bytes order key, compared lexicographically.
    Bytes = 4,
}

/// Sort direction for a Top-K query.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Order {
    /// Ascending: smallest order-key value first.
    Asc = 0,
    /// Descending: largest order-key value first.
    Desc = 1,
}

/// Optional per-type modifiers for `order_by`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum OrderByFlags {
    /// No flags.
    #[default]
    None,
    /// Case-insensitive string comparison. Only valid with `OrderByType::String`.
    CaseInsensitive,
}

/// The order-by clause of a Top-K query: the order key's bin name, scalar
/// type, sort direction, and optional flags.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrderBy {
    /// Name of the bin (as it appears in the *returned* record — a physical
    /// bin or one produced by a read-op/read-expression projection) to sort by.
    pub bin_name: String,
    /// Scalar type of the order-key bin.
    pub order_type: OrderByType,
    /// Sort direction.
    pub direction: Order,
    /// Optional modifiers (currently only `CaseInsensitive`, for `String`).
    pub flags: OrderByFlags,
}
