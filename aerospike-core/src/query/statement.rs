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
use crate::query::Filter;
use crate::Bins;
use crate::Value;

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

    pub(crate) fn validate(&self) -> Result<()> {
        if let Some(ref filters) = self.filters {
            if filters.len() > 1 {
                return Err(Error::InvalidArgument(
                    "Too many filter expressions".to_string(),
                ));
            }
        }

        if let Some(ref agg) = self.aggregation {
            if agg.package_name.is_empty() {
                return Err(Error::InvalidArgument("Empty UDF package name".to_string()));
            }

            if agg.function_name.is_empty() {
                return Err(Error::InvalidArgument(
                    "Empty UDF function name".to_string(),
                ));
            }
        }

        Ok(())
    }
}
