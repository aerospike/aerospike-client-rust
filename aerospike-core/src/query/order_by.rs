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
//!
/// Scalar comparator type for a Top-K order-by key.
///
/// Aerospike has no schema, so the type of the order-by bin must be declared
/// explicitly. Values mirror the server's `AS_ORDER_BY_TYPE_*` wire
/// constants, so a future wire-encode path is a trivial `as u8` cast.
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

/// Sort direction for a Top-K query. Values mirror the server's
/// `AS_ORDER_BY_DIRECTION_*` wire constants.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Order {
    /// Ascending: smallest order-key value first.
    Asc = 0,
    /// Descending: largest order-key value first.
    Desc = 1,
}

/// Optional per-type modifiers for `order_by`.
///
/// Modeled as a plain enum (rather than a bitflags type) since exactly one
/// flag bit is currently defined on the wire
/// (`AS_ORDER_BY_FLAG_CASE_INSENSITIVE`); revisit if the server adds more
/// flag bits later.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum OrderByFlags {
    /// No flags.
    #[default]
    None,
    /// Case-insensitive string comparison. Only valid with `OrderByType::String`.
    CaseInsensitive,
}

impl OrderByFlags {
    /// The wire's bitmask representation (`AS_ORDER_BY_FLAG_*`). Deliberately
    /// not the enum's `as u64` discriminant: the wire field is a bitmask,
    /// while this type models it as a plain enum for API ergonomics (see the
    /// type-level docs), so the two representations aren't guaranteed to
    /// stay numerically identical as more flag bits are added.
    pub(crate) const fn to_wire_bits(self) -> u64 {
        match self {
            OrderByFlags::None => 0,
            OrderByFlags::CaseInsensitive => 1 << 0,
        }
    }
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
