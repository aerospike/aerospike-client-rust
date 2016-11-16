// Copyright 2015-2016 Aerospike, Inc.
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

use std::sync::Arc;

use value::Value;
use common::filter::Filter;
use error::{AerospikeError, AerospikeResult};
use client::ResultCode;

#[derive(Clone)]
pub struct Aggregation {
    pub package_name: String,
    pub function_name: String,
    pub function_args: Option<Vec<Value>>,
}

#[derive(Clone)]
pub struct Statement {
    pub namespace: String,
    pub set_name: String,
    pub index_name: Option<String>,
    pub bin_names: Option<Vec<String>>,

    pub filters: Option<Vec<Arc<Filter>>>,
    pub aggregation: Option<Aggregation>,
}

impl Statement {
    pub fn new(namespace: &str,
               set_name: &str,
               bin_names: Option<&[&str]>)
               -> Self {

        let bin_names = match bin_names {
            None => None,
            Some(bin_names) => {
                let bin_names: Vec<_> = bin_names.iter().cloned().map(String::from).collect();
                Some(bin_names)
            }
        };

        Statement {
            namespace: namespace.to_owned(),
            set_name: set_name.to_owned(),
            bin_names: bin_names,

            index_name: None,
            aggregation: None,
            filters: None,
        }
    }

    pub fn add_filter(&mut self, filter: Arc<Filter>) {
        match self.filters {
            Some(ref mut filters) => {
                filters.push(filter.to_owned());
            }
            None => {
                let mut filters = vec![];
                filters.push(filter.to_owned());
                self.filters = Some(filters);
            }
        }
    }

    pub fn is_scan(&self) -> bool {
        if let Some(ref filters) = self.filters {
            return filters.len() == 0;
        }
        return true;
    }

    pub fn set_aggregate_function(&mut self,
                                  package_name: &str,
                                  function_name: &str,
                                  function_args: Option<&[Value]>)
                                  -> AerospikeResult<()> {
        let agg = Aggregation {
            package_name: package_name.to_owned(),
            function_name: function_name.to_owned(),
            function_args: match function_args {
                Some(args) => Some(args.to_vec()),
                None => None,
            },
        };

        self.aggregation = Some(agg);

        Ok(())
    }

    pub fn validate(&self) -> AerospikeResult<()> {
        if let Some(ref filters) = self.filters {
            if filters.len() > 1 {
                return Err(AerospikeError::new(ResultCode::ParameterError,
                                               Some("Too many filters set in the statement. \
                                                     Aerospike server supports only one filter \
                                                     per query ."
                                                   .to_string())));
            }
        }

        if self.set_name == "" {
            return Err(AerospikeError::new(ResultCode::ParameterError,
                                           Some("Set name cannot be empty in the statement."
                                               .to_string())));
        }

        if let Some(ref index_name) = self.index_name {
            if index_name == "" {
                return Err(AerospikeError::new(ResultCode::ParameterError,
                                               Some("Index name cannot be empty in the \
                                                     statement."
                                                   .to_string())));
            }
        }

        if let Some(ref agg) = self.aggregation {
            if agg.package_name == "" {
                return Err(AerospikeError::new(ResultCode::ParameterError,
                                               Some("Package name cannot be empty in the \
                                                     statement."
                                                   .to_string())));
            }

            if agg.function_name == "" {
                return Err(AerospikeError::new(ResultCode::ParameterError,
                                               Some("Function name cannot be empty in the \
                                                     statement."
                                                   .to_string())));
            }

        }

        Ok(())

    }
}
