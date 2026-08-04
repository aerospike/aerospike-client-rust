// Copyright 2015-2018 Aerospike, Inc.
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

/// Maximum length, in bytes, of an order-by bin name (`AS_BIN_NAME_MAX_SZ - 1`
/// on the server).
const MAX_ORDER_BY_BIN_NAME_LEN: usize = 14;

/// Inclusive bounds for `Statement::set_top_k`'s `k` (`TOP_K_MAX` on the server).
const TOP_K_MIN: u32 = 1;
const TOP_K_MAX: u32 = 1000;

#[derive(Clone, Debug)]
pub struct Aggregation {
    pub package_name: String,
    pub function_name: String,
    pub function_args: Option<Vec<Value>>,
}

/// Query statement parameters.
#[derive(Clone, Debug)]
pub struct Statement {
    /// Namespace
    pub namespace: String,

    /// Set name. If left empty, all the sets within the namespace will be scanned.
    pub set_name: String,

    /// Optional list of bin names to return in query.
    pub bins: Bins,

    /// Optional list of query filters. Currently, only one filter is allowed by the server on a
    /// secondary index lookup.
    pub filters: Option<Vec<Filter>>,

    /// Optional Lua aggregation function parameters.
    pub aggregation: Option<Aggregation>,

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

    /// Top-K order-by clause. Set via `set_order_by`/`set_order_by_with_flags`.
    ///
    /// # Work in progress
    ///
    /// This client validates and models Top-K statements, but does not yet
    /// send them over the wire — see [`crate::query::order_by`].
    pub order_by: Option<OrderBy>,

    /// Top-K limit (`k`), in `[1, 1000]`. Must be preceded by `set_order_by`.
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
    pub fn new(namespace: &str, set_name: &str, bins: Bins) -> Self {
        Statement {
            namespace: namespace.to_owned(),
            set_name: set_name.to_owned(),
            bins,
            aggregation: None,
            filters: None,
            operations: None,
            order_by: None,
            top_k: None,
        }
    }

    /// Attach an ops projection to the statement. On a foreground query
    /// the server returns the result of these operations for each
    /// matching record instead of the bins selected by `bins`. Mutually
    /// exclusive with `bins` (server uses `operations` if both are set).
    ///
    /// Foreground queries (`Client::query`) accept only read ops; server
    /// versions before 8.1.2 only accept the basic `Read` op here.
    pub fn set_operations(&mut self, operations: Vec<Operation>) {
        self.operations = Some(operations);
    }

    /// Add a query filter to the statement. Currently, only one filter is allowed by the server on
    /// a secondary index lookup.
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
    /// stmt.add_filter(Filter::range("baz", 0, 100));
    /// ```
    pub fn add_filter(&mut self, filter: Filter) {
        if let Some(ref mut filters) = self.filters {
            filters.push(filter);
        } else {
            let filters = vec![filter];
            self.filters = Some(filters);
        }
    }

    /// Set Lua aggregation function parameters.
    pub fn set_aggregate_function(
        &mut self,
        package_name: &str,
        function_name: &str,
        function_args: Option<&[Value]>,
    ) {
        let agg = Aggregation {
            package_name: package_name.to_owned(),
            function_name: function_name.to_owned(),
            function_args: function_args.map(<[Value]>::to_vec),
        };
        self.aggregation = Some(agg);
    }

    /// Sets the Top-K order-by clause: the order key's bin name (as it
    /// appears in the *returned* record), its scalar type, and sort
    /// direction. Equivalent to calling
    /// `set_order_by_with_flags(bin_name, order_type, direction, OrderByFlags::None)`.
    ///
    /// Must be set before `set_top_k`.
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
        self.order_by = Some(OrderBy {
            bin_name: bin_name.to_owned(),
            order_type,
            direction,
            flags,
        });
    }

    /// Sets the Top-K limit. `k` must be in `[1, 1000]`. Must be preceded by
    /// `set_order_by`/`set_order_by_with_flags` (checked by `validate()`, not
    /// here, so builder calls can happen in either order relative to other
    /// statement setup as long as `order_by` is set before the statement is
    /// executed).
    pub const fn set_top_k(&mut self, k: u32) {
        self.top_k = Some(k);
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if let Some(ref filters) = self.filters {
            if filters.len() > 1 {
                return Err(Error::invalid_argument(
                    "Too many filter expressions".to_string(),
                ));
            }
        }

        if let Some(ref agg) = self.aggregation {
            if agg.package_name.is_empty() {
                return Err(Error::invalid_argument("Empty UDF package name".to_string()));
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

    /// Validation rules specific to `order_by`/`top_k`, including which
    /// rules the server also enforces as defense-in-depth.
    fn validate_top_k(&self) -> Result<()> {
        if self.order_by.is_none() && self.top_k.is_none() {
            return Ok(());
        }

        if self.aggregation.is_some() {
            return Err(Error::invalid_argument(
                "orderBy/topK is incompatible with aggregate UDFs; use the UDF's own reduce stage"
                    .to_string(),
            ));
        }

        if let Some(ref order_by) = self.order_by {
            if order_by.bin_name.is_empty() {
                return Err(Error::invalid_argument(
                    "orderBy bin name must not be empty".to_string(),
                ));
            }

            if order_by.bin_name.len() > MAX_ORDER_BY_BIN_NAME_LEN {
                return Err(Error::invalid_argument(format!(
                    "orderBy bin name '{}' exceeds the {}-character limit",
                    order_by.bin_name, MAX_ORDER_BY_BIN_NAME_LEN
                )));
            }

            if order_by.flags == OrderByFlags::CaseInsensitive
                && order_by.order_type != OrderByType::String
            {
                return Err(Error::invalid_argument(
                    "orderBy flag CASE_INSENSITIVE is only valid with type STRING".to_string(),
                ));
            }

            // Order-by bin must be one of the projected bins, if a projection is set.
            if let Some(ref operations) = self.operations {
                let projected = operations.iter().any(|op| {
                    matches!(&op.bin, crate::operations::OperationBin::Name(name) if name == &order_by.bin_name)
                });
                if !projected {
                    return Err(Error::invalid_argument(format!(
                        "orderBy bin '{}' is not in projection; add it to setOperations or remove projection",
                        order_by.bin_name
                    )));
                }
            } else if let Bins::Some(ref bins) = self.bins {
                if !bins.iter().any(|b| b == &order_by.bin_name) {
                    return Err(Error::invalid_argument(format!(
                        "orderBy bin '{}' is not in projection; add it to setBinNames or remove projection",
                        order_by.bin_name
                    )));
                }
            }
        } else if self.top_k.is_some() {
            return Err(Error::invalid_argument(
                "topK requires orderBy to be set".to_string(),
            ));
        }

        if let Some(k) = self.top_k {
            if !(TOP_K_MIN..=TOP_K_MAX).contains(&k) {
                return Err(Error::invalid_argument(format!(
                    "topK must be in [{TOP_K_MIN}, {TOP_K_MAX}]; got {k}"
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
    fn order_by_alone_is_valid() {
        let mut stmt = base_statement();
        stmt.set_order_by("score", OrderByType::Integer, Order::Desc);
        assert!(stmt.validate().is_ok());
    }

    #[test]
    fn top_k_without_order_by_is_rejected() {
        let mut stmt = base_statement();
        stmt.set_top_k(10);
        let err = stmt.validate().unwrap_err();
        assert!(err.to_string().contains("topK requires orderBy"));
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
        stmt.set_aggregate_function("pkg", "func", None);
        let err = stmt.validate().unwrap_err();
        assert!(err.to_string().contains("aggregate UDF"));
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
        assert!(stmt.validate().is_ok());
    }

    #[test]
    fn order_by_bin_name_too_long_is_rejected() {
        let mut stmt = base_statement();
        stmt.set_order_by("this_bin_name_is_too_long", OrderByType::Integer, Order::Desc);
        let err = stmt.validate().unwrap_err();
        assert!(err.to_string().contains("14-character limit"));
    }

    #[test]
    fn order_by_bin_missing_from_bins_projection_is_rejected() {
        let mut stmt = Statement::new("ns", "set", Bins::from(["a", "b"]));
        stmt.set_order_by("c", OrderByType::Integer, Order::Desc);
        let err = stmt.validate().unwrap_err();
        assert!(err.to_string().contains("is not in projection"));
    }

    #[test]
    fn order_by_bin_present_in_bins_projection_is_valid() {
        let mut stmt = Statement::new("ns", "set", Bins::from(["a", "b"]));
        stmt.set_order_by("b", OrderByType::Integer, Order::Desc);
        assert!(stmt.validate().is_ok());
    }

    #[test]
    fn order_by_bin_missing_from_operations_projection_is_rejected() {
        let mut stmt = base_statement();
        stmt.set_operations(vec![operations::get_bin("a")]);
        stmt.set_order_by("dist", OrderByType::Double, Order::Asc);
        let err = stmt.validate().unwrap_err();
        assert!(err.to_string().contains("is not in projection"));
    }

    #[test]
    fn order_by_bin_present_in_operations_projection_is_valid() {
        let mut stmt = base_statement();
        stmt.set_operations(vec![operations::get_bin("dist")]);
        stmt.set_order_by("dist", OrderByType::Double, Order::Asc);
        assert!(stmt.validate().is_ok());
    }

    #[test]
    fn order_by_with_bins_all_projection_is_valid() {
        // `Bins::All` means "no explicit projection" — any order-by bin is
        // allowed since the full record (whatever it contains) comes back.
        let mut stmt = Statement::new("ns", "set", Bins::All);
        stmt.set_order_by("anything", OrderByType::Integer, Order::Desc);
        assert!(stmt.validate().is_ok());
    }
}
