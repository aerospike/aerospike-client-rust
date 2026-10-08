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
use crate::query::Filter;
use crate::Bins;
use crate::Value;

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

        Ok(())
    }
}
