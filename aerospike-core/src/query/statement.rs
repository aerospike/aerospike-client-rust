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

use crate::errors::{Error, Result};
use crate::operations::Operation;
use crate::query::order_by::{Order, OrderBy, OrderByFlags, OrderByType};
use crate::query::Filter;
use crate::Bins;
use crate::Value;

/// Maximum length, in bytes, of an order-by bin name.
const MAX_ORDER_BY_BIN_NAME_LEN: usize = 15;

/// How many order keys a Top-K query takes.
const MAX_ORDER_KEYS: usize = 2;

/// Inclusive bounds for `Statement::set_top_k`'s `k`.
const TOP_K_MIN: u32 = 1;
const TOP_K_MAX: u32 = 1000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Aggregation {
    pub package_name: String,
    pub function_name: String,
    pub function_args: Vec<Value>,
}

/// Query statement parameters.
#[derive(Clone, Debug, PartialEq)]
pub struct Statement {
    /// Namespace
    pub namespace: String,

    /// Set name. If left empty, all the sets within the namespace will be scanned.
    pub set_name: String,

    /// Optional list of bin names to return in query.
    pub bins: Bins,

    /// Optional secondary-index filter. The server accepts one filter per
    /// query; without one the statement scans the whole set.
    pub filter: Option<Filter>,

    /// Lua aggregation function parameters, set by the client's
    /// aggregate and background-UDF methods.
    pub(crate) aggregation: Option<Aggregation>,

    /// Optional ops projection. When set, the server returns the result
    /// of these operations for each matching record instead of the full
    /// bin set selected by `bins`. Mutually exclusive with `bins` —
    /// setting both makes the server use `operations` and ignore `bins`.
    ///
    /// On a foreground query (`Client::query`) only read operations are
    /// allowed. Server versions before 8.1.2 only accept the basic
    /// `Read` op here; 8.1.2+ accepts CDT, expression, bit, and HLL
    /// reads as well.
    pub operations: Option<Vec<Operation>>,

    /// Top-K order keys, at most two, compared in order; empty when the query
    /// is not ranked. Set with [`Self::set_order_by`] and
    /// [`Self::then_order_by`].
    pub order_by: Vec<OrderBy>,

    /// Top-K limit (`k`), in `[1, 1000]`. Requires an order key.
    pub top_k: Option<u32>,
}

impl Statement {
    /// Creates a new query statement with the given namespace, set name and optional list of bin
    /// names.
    ///
    /// # Examples
    ///
    /// Creates a new statement to query the namespace "foo" and set "bar" and return the "name" and
    /// "age" bins for each matching record.
    ///
    /// ```rust
    /// # use aerospike::*;
    ///
    /// let stmt = Statement::new("foo", "bar", Bins::from(["name", "age"]));
    /// ```
    pub fn new(namespace: impl Into<String>, set_name: impl Into<String>, bins: Bins) -> Self {
        Statement {
            namespace: namespace.into(),
            set_name: set_name.into(),
            bins,
            aggregation: None,
            filter: None,
            operations: None,
            order_by: Vec::new(),
            top_k: None,
        }
    }

    /// Attach operations to the statement. On a foreground query
    /// ([`Client::query`](crate::Client::query)) the server returns the
    /// result of these operations for each matching record instead of the
    /// bins selected by `bins`; mutually exclusive with `bins` (the server
    /// uses `operations` if both are set). On a background job
    /// ([`Client::query_operate`](crate::Client::query_operate)) the server
    /// applies them to each matching record.
    ///
    /// Foreground queries accept only read ops, and server versions before
    /// 8.1.2 only accept the basic `Read` op; background jobs accept only
    /// write ops.
    pub fn set_operations(&mut self, operations: impl Into<Vec<Operation>>) {
        self.operations = Some(operations.into());
    }

    /// Set the statement's secondary-index filter, replacing any previous one.
    /// The server accepts one filter per query.
    ///
    /// # Example
    ///
    /// This example uses a numeric index on bin _baz_ in namespace _foo_ within set _bar_ to find
    /// all records using a filter with the range 0 to 100 inclusive:
    ///
    /// ```rust
    /// # use aerospike::*;
    /// # use aerospike::query::Filter;
    ///
    /// let mut stmt = Statement::new("foo", "bar", Bins::from(["name", "age"]));
    /// stmt.set_filter(Filter::range("baz", 0, 100));
    /// ```
    pub fn set_filter(&mut self, filter: Filter) {
        self.filter = Some(filter);
    }

    /// The secondary-index filter this statement queries by, or `None` for a
    /// scan. An empty `filters` list is a scan, the same as `None`.
    pub(crate) const fn filter(&self) -> Option<&Filter> {
        self.filter.as_ref()
    }

    /// Lua aggregation function parameters, set by
    /// [`Client::query_aggregate`](crate::Client::query_aggregate) and
    /// [`Client::query_execute_udf`](crate::Client::query_execute_udf).
    ///
    /// Hidden: not part of the documented API. Language bindings that carry
    /// the parameters on the statement call it; Rust code passes them to the
    /// client methods instead.
    #[doc(hidden)]
    pub fn set_aggregate_function(
        &mut self,
        package_name: impl Into<String>,
        function_name: impl Into<String>,
        function_args: &[Value],
    ) {
        let agg = Aggregation {
            package_name: package_name.into(),
            function_name: function_name.into(),
            function_args: function_args.to_vec(),
        };
        self.aggregation = Some(agg);
    }

    /// Sets the Top-K order key and direction, replacing any keys set before.
    ///
    /// The key must appear in the returned record. Pair it with
    /// [`Self::set_top_k`]: an order key sent without `k` is refused by the
    /// server.
    pub fn set_order_by(&mut self, bin_name: &str, order_type: OrderByType, direction: Order) {
        self.set_order_by_with_flags(bin_name, order_type, direction, OrderByFlags::None);
    }

    /// Like [`Self::set_order_by`], with optional modifiers (currently only
    /// `OrderByFlags::CaseInsensitive`, valid only with `OrderByType::String`).
    pub fn set_order_by_with_flags(
        &mut self,
        bin_name: &str,
        order_type: OrderByType,
        direction: Order,
        flags: OrderByFlags,
    ) {
        self.order_by.clear();
        self.then_order_by_with_flags(bin_name, order_type, direction, flags);
    }

    /// Adds a second order key, consulted when the first compares equal. It
    /// must name a different bin from the first key; a Top-K query takes at
    /// most two keys.
    pub fn then_order_by(&mut self, bin_name: &str, order_type: OrderByType, direction: Order) {
        self.then_order_by_with_flags(bin_name, order_type, direction, OrderByFlags::None);
    }

    /// Like [`Self::then_order_by`], with optional modifiers.
    pub fn then_order_by_with_flags(
        &mut self,
        bin_name: &str,
        order_type: OrderByType,
        direction: Order,
        flags: OrderByFlags,
    ) {
        self.order_by.push(OrderBy {
            bin_name: bin_name.to_owned(),
            order_type,
            direction,
            flags,
        });
    }

    /// Sets the Top-K limit. Requires an order key, set in any order.
    pub const fn set_top_k(&mut self, k: u32) {
        self.top_k = Some(k);
    }

    /// Whether this is a Top-K query: a limit is set.
    pub const fn has_top_k(&self) -> bool {
        self.top_k.is_some()
    }

    /// Whether an order key is set, with or without a limit.
    pub const fn has_order_by(&self) -> bool {
        !self.order_by.is_empty()
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if let Some(ref agg) = self.aggregation {
            if agg.package_name.is_empty() {
                return Err(Error::invalid_argument(
                    "Empty UDF package name".to_string(),
                ));
            }

            if agg.function_name.is_empty() {
                return Err(Error::invalid_argument(
                    "Empty UDF function name".to_string(),
                ));
            }
        }

        self.validate_top_k()?;

        Ok(())
    }

    /// Validates the Top-K options.
    ///
    /// Only a statement with `top_k` is checked. An order key without `k` is
    /// sent as it is and refused by the server, which keeps the two clients'
    /// behaviour the same; `k` without an order key is refused here.
    fn validate_top_k(&self) -> Result<()> {
        let Some(k) = self.top_k else {
            return Ok(());
        };

        if self.order_by.is_empty() {
            return Err(Error::invalid_argument(
                "Top-K requires an order key; call set_order_by()".to_string(),
            ));
        }

        if self.order_by.len() > MAX_ORDER_KEYS {
            return Err(Error::invalid_argument(format!(
                "Top-K takes at most {MAX_ORDER_KEYS} order keys"
            )));
        }

        if !(TOP_K_MIN..=TOP_K_MAX).contains(&k) {
            return Err(Error::invalid_argument(format!(
                "Top-K limit must be in [{TOP_K_MIN}, {TOP_K_MAX}]; got {k}"
            )));
        }

        if self.aggregation.is_some() {
            return Err(Error::invalid_argument(
                "Top-K is only valid for foreground queries; it cannot be combined with an \
                 aggregation UDF"
                    .to_string(),
            ));
        }

        for key in &self.order_by {
            self.validate_order_key(key)?;
        }

        if self.order_by.len() > 1 && self.order_by[0].bin_name == self.order_by[1].bin_name {
            return Err(Error::invalid_argument(
                "Top-K order keys must use different bins".to_string(),
            ));
        }

        Ok(())
    }

    fn validate_order_key(&self, key: &OrderBy) -> Result<()> {
        let len = key.bin_name.len();
        if len == 0 || len > MAX_ORDER_BY_BIN_NAME_LEN || key.bin_name.contains('\0') {
            return Err(Error::invalid_argument(format!(
                "Top-K order-by bin name must be 1-{MAX_ORDER_BY_BIN_NAME_LEN} UTF-8 bytes without \
                 NUL"
            )));
        }

        if key.flags != OrderByFlags::None && key.order_type != OrderByType::String {
            return Err(Error::invalid_argument(
                "Top-K order-by flags are only valid for STRING (CASE_INSENSITIVE on another type)"
                    .to_string(),
            ));
        }

        // The order-by bin must come back with the record: produced by exactly
        // one projection operation, or named in the bin projection.
        if let Some(ref operations) = self.operations {
            let count = operations
                .iter()
                .filter(|op| {
                    matches!(&op.bin, crate::operations::OperationBin::Name(name) if name == &key.bin_name)
                })
                .count();
            if count == 0 {
                return Err(Error::invalid_argument(format!(
                    "Top-K order-by bin '{}' must be included in the operations projection",
                    key.bin_name
                )));
            }
            if count > 1 {
                return Err(Error::invalid_argument(format!(
                    "Top-K order-by bin '{}' must be produced by only one projection operation",
                    key.bin_name
                )));
            }
            return Ok(());
        }

        if let Bins::Some(ref bins) = self.bins {
            if !bins.iter().any(|b| b == &key.bin_name) {
                return Err(Error::invalid_argument(format!(
                    "Top-K order-by bin '{}' must be included in the bin projection",
                    key.bin_name
                )));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operations;

    fn base_statement() -> Statement {
        Statement::new("ns", "set", Bins::All)
    }

    #[test]
    fn no_order_by_or_top_k_is_valid() {
        assert!(base_statement().validate().is_ok());
    }

    #[test]
    fn order_by_without_top_k_is_left_for_the_server() {
        let mut stmt = base_statement();
        stmt.set_order_by("score", OrderByType::Integer, Order::Desc);
        assert!(stmt.validate().is_ok());
        assert!(stmt.has_order_by());
        assert!(!stmt.has_top_k());
    }

    #[test]
    fn top_k_without_order_by_is_rejected() {
        let mut stmt = base_statement();
        stmt.set_top_k(10);
        let err = stmt.validate().unwrap_err();
        assert!(err.to_string().contains("requires an order key"));
    }

    #[test]
    fn setters_accept_any_order() {
        let mut stmt = base_statement();
        stmt.set_top_k(10);
        stmt.set_order_by("score", OrderByType::Integer, Order::Desc);
        assert!(stmt.validate().is_ok());
    }

    #[test]
    fn set_order_by_replaces_and_then_order_by_appends() {
        let mut stmt = base_statement();
        stmt.set_order_by("a", OrderByType::Integer, Order::Desc);
        stmt.then_order_by("b", OrderByType::String, Order::Asc);
        assert_eq!(stmt.order_by.len(), 2);
        assert_eq!(stmt.order_by[1].bin_name, "b");

        stmt.set_order_by("c", OrderByType::Integer, Order::Asc);
        assert_eq!(stmt.order_by.len(), 1);
        assert_eq!(stmt.order_by[0].bin_name, "c");
    }

    #[test]
    fn second_order_key_is_valid_and_checked_like_the_first() {
        let mut stmt = Statement::new("ns", "set", Bins::from(["a", "b"]));
        stmt.set_order_by("a", OrderByType::Integer, Order::Desc);
        stmt.then_order_by("b", OrderByType::String, Order::Asc);
        stmt.set_top_k(10);
        assert!(stmt.validate().is_ok());

        stmt.then_order_by_with_flags(
            "b",
            OrderByType::Integer,
            Order::Asc,
            OrderByFlags::CaseInsensitive,
        );
        let err = stmt.validate().unwrap_err();
        assert!(err.to_string().contains("at most 2 order keys"), "{err}");

        let mut stmt = Statement::new("ns", "set", Bins::from(["a"]));
        stmt.set_order_by("a", OrderByType::Integer, Order::Desc);
        stmt.then_order_by("b", OrderByType::String, Order::Asc);
        stmt.set_top_k(10);
        let err = stmt.validate().unwrap_err();
        assert!(err.to_string().contains("bin projection"), "{err}");
    }

    #[test]
    fn order_keys_on_the_same_bin_are_rejected() {
        let mut stmt = base_statement();
        stmt.set_order_by("a", OrderByType::Integer, Order::Desc);
        stmt.then_order_by("a", OrderByType::Integer, Order::Asc);
        stmt.set_top_k(10);
        let err = stmt.validate().unwrap_err();
        assert!(err.to_string().contains("different bins"), "{err}");
    }

    #[test]
    fn order_by_bin_produced_by_two_operations_is_rejected() {
        let mut stmt = base_statement();
        stmt.set_operations(vec![operations::get_bin("dist"), operations::get_bin("dist")]);
        stmt.set_order_by("dist", OrderByType::Double, Order::Asc);
        stmt.set_top_k(10);
        let err = stmt.validate().unwrap_err();
        assert!(err.to_string().contains("only one projection operation"), "{err}");
    }

    #[test]
    fn top_k_zero_is_rejected() {
        let mut stmt = base_statement();
        stmt.set_order_by("score", OrderByType::Integer, Order::Desc);
        stmt.set_top_k(0);
        assert!(stmt.validate().is_err());
    }

    #[test]
    fn top_k_over_1000_is_rejected() {
        let mut stmt = base_statement();
        stmt.set_order_by("score", OrderByType::Integer, Order::Desc);
        stmt.set_top_k(1001);
        assert!(stmt.validate().is_err());
    }

    #[test]
    fn top_k_in_range_is_valid() {
        for k in [1, 500, 1000] {
            let mut stmt = base_statement();
            stmt.set_order_by("score", OrderByType::Integer, Order::Desc);
            stmt.set_top_k(k);
            assert!(stmt.validate().is_ok(), "k={k} should be valid");
        }
    }

    #[test]
    fn order_by_incompatible_with_aggregation() {
        let mut stmt = base_statement();
        stmt.set_order_by("score", OrderByType::Integer, Order::Desc);
        stmt.set_top_k(10);
        stmt.set_aggregate_function("pkg", "func", &[]);
        let err = stmt.validate().unwrap_err();
        assert!(err.to_string().contains("aggregation UDF"));
    }

    #[test]
    fn case_insensitive_flag_requires_string_type() {
        let mut stmt = base_statement();
        stmt.set_order_by_with_flags(
            "score",
            OrderByType::Integer,
            Order::Desc,
            OrderByFlags::CaseInsensitive,
        );
        stmt.set_top_k(10);
        let err = stmt.validate().unwrap_err();
        assert!(err.to_string().contains("CASE_INSENSITIVE"));
    }

    #[test]
    fn case_insensitive_flag_with_string_type_is_valid() {
        let mut stmt = base_statement();
        stmt.set_order_by_with_flags(
            "name",
            OrderByType::String,
            Order::Asc,
            OrderByFlags::CaseInsensitive,
        );
        stmt.set_top_k(10);
        assert!(stmt.validate().is_ok());
    }

    #[test]
    fn order_by_bin_name_too_long_is_rejected() {
        let mut stmt = base_statement();
        stmt.set_order_by(
            "this_bin_name_is_too_long",
            OrderByType::Integer,
            Order::Desc,
        );
        stmt.set_top_k(10);
        let err = stmt.validate().unwrap_err();
        assert!(err.to_string().contains("1-15 UTF-8 bytes"));
    }

    #[test]
    fn fifteen_byte_order_by_bin_name_is_valid() {
        let mut stmt = base_statement();
        stmt.set_order_by("fifteen-byte!!!", OrderByType::Integer, Order::Desc);
        stmt.set_top_k(10);
        assert!(stmt.validate().is_ok());
    }

    #[test]
    fn order_by_bin_name_with_nul_is_rejected() {
        let mut stmt = base_statement();
        stmt.set_order_by("bad\0name", OrderByType::Integer, Order::Desc);
        stmt.set_top_k(10);
        let err = stmt.validate().unwrap_err();
        assert!(err.to_string().contains("without NUL"));
    }

    #[test]
    fn order_by_bin_missing_from_bins_projection_is_rejected() {
        let mut stmt = Statement::new("ns", "set", Bins::from(["a", "b"]));
        stmt.set_order_by("c", OrderByType::Integer, Order::Desc);
        stmt.set_top_k(10);
        let err = stmt.validate().unwrap_err();
        assert!(err.to_string().contains("bin projection"));
    }

    #[test]
    fn order_by_bin_present_in_bins_projection_is_valid() {
        let mut stmt = Statement::new("ns", "set", Bins::from(["a", "b"]));
        stmt.set_order_by("b", OrderByType::Integer, Order::Desc);
        stmt.set_top_k(10);
        assert!(stmt.validate().is_ok());
    }

    #[test]
    fn order_by_bin_missing_from_operations_projection_is_rejected() {
        let mut stmt = base_statement();
        stmt.set_operations(vec![operations::get_bin("a")]);
        stmt.set_order_by("dist", OrderByType::Double, Order::Asc);
        stmt.set_top_k(10);
        let err = stmt.validate().unwrap_err();
        assert!(err.to_string().contains("operations projection"));
    }

    #[test]
    fn order_by_bin_present_in_operations_projection_is_valid() {
        let mut stmt = base_statement();
        stmt.set_operations(vec![operations::get_bin("dist")]);
        stmt.set_order_by("dist", OrderByType::Double, Order::Asc);
        stmt.set_top_k(10);
        assert!(stmt.validate().is_ok());
    }

    #[test]
    fn order_by_with_bins_all_projection_is_valid() {
        // `Bins::All` means "no explicit projection" — any order-by bin is
        // allowed since the full record (whatever it contains) comes back.
        let mut stmt = Statement::new("ns", "set", Bins::All);
        stmt.set_order_by("anything", OrderByType::Integer, Order::Desc);
        stmt.set_top_k(10);
        assert!(stmt.validate().is_ok());
    }
}
