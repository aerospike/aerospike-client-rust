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

use aerospike::{Bin, Key, Value};
use rand::{rngs::StdRng};

use crate::db_object_spec::DBObjectSpec;

#[derive(Debug, Clone)]
pub struct Args {
    pub n_bins: usize,
    pub bin_name_base: String,
    pub object_specs: Vec<DBObjectSpec>,
}

#[derive(Debug, Default)]
pub struct ArgBuilder {
    n_bins: Option<usize>,
    bin_name_base: Option<String>,
    object_specs: Option<Vec<DBObjectSpec>>,
}

impl ArgBuilder {

    pub fn bin_name_base(mut self, bin_name_base: String) -> Self {
        self.bin_name_base = Some(bin_name_base);
        self
    }

    pub fn n_bins(mut self, bins: usize) -> Self {
        self.n_bins = Some(bins);
        self
    }

    pub fn object_specs(mut self, object_specs: Vec<DBObjectSpec>) -> Self {
        self.object_specs = Some(object_specs);
        self
    }

    pub fn build(self) -> Result<Args, String> {
        let n_bins = self.n_bins.unwrap_or(1);
        let bin_name_base = self.bin_name_base.unwrap_or_else(|| "testBin".to_string());
        let object_specs = self.object_specs.unwrap_or_else(|| vec![DBObjectSpec::default()]);
        Ok(Args {
            n_bins,
            bin_name_base,
            object_specs
        })
        
    }

}


impl Args {
    pub fn builder() -> ArgBuilder {
        ArgBuilder::default()
    }

    pub fn build_bins(&self, key: &Key, rng: &mut StdRng) -> Vec<Bin> {
        let mut bins = Vec::with_capacity(self.n_bins);
        let n_specs = self.object_specs.len();
        let seed = match key.user_key.as_ref() {
                Some(Value::Int(k)) => Some(*k),
                _ => None,
        };
        for i in 0..self.n_bins {
            let spec = &self.object_specs[i % n_specs];
            let value = if i == 0 {
                spec.gen_value(rng, seed)
            } else {
                spec.gen_value(rng, None)
            };
            let name = format!("{}_{}", self.bin_name_base, i + 1);
            bins.push(Bin::new(name, value));
        }
        bins
    }
}