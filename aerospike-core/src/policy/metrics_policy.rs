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

#[cfg(feature = "serialization")]
use serde::Serialize;

use crate::cluster::histogram::Type;

#[derive(Clone, Debug, PartialEq)]
/// Determines how the metrics are gathered.
/// Normal client statistics out of hot path are always gathered.
/// Command statistics are gathered per this policy.
#[cfg_attr(feature = "serialization", derive(Serialize))]
pub struct MetricsPolicy {
    /// Histogram type specifies if the histogram should be [histogram.Linear] or [histogram.Logarithmic].
    ///
    /// Default: [histogram.Logarithmic]    
    pub histogram_type: Type,

    /// LatencyColumns defines the number of elapsed time range buckets in latency histograms.
    ///
    /// Default: 24
    pub latency_columns: usize,

    /// Depending on the type of histogram:
    ///
    /// For logarithmic histograms, the buckets are: <base^1 <base^2 <base^3 ... >=base^(columns-1)
    ///
    ///  LatencyColumns=5 latencyBase=8
    ///  <8µs <64µs <512µs <4096µs >=4096
    ///
    ///  LatencyColumns=7 LatencyBase=4
    ///  <4µs <16µs <64µs <256µs <1024µs <4096 >=4096µs
    ///
    /// For linear histograms, the buckets are: <base <base*2 <base*3 ... >=base*(column-1)
    ///
    ///  LatencyColumns=5 latencyBase=15
    ///  <15µs <30µs <45µs <60µs >=60µs
    ///
    ///  LatencyColumns=7 LatencyBase=5
    ///  <5µs <10µs <15µs <20µs <25µs <30µs >=30µs
    ///
    /// Default: 2
    pub latency_base: usize,

    /// Gather error code metrics per command type. Expensive.
    pub error_metrics_per_latency: bool,

    /// Gather latency metrics per command type. Expensive.
    pub latency_metrics: bool,

    /// Group various detailed metrics per namespace. Expensive.
    pub metrics_per_namespace: bool,
}

//////////////////////////////////////////////////////////////////
// Default Implementations
//////////////////////////////////////////////////////////////////

impl Default for MetricsPolicy {
    fn default() -> Self {
        Self {
            histogram_type: Type::Logarithmic,
            latency_columns: 24,
            latency_base: 2,
            error_metrics_per_latency: false,
            latency_metrics: true,
            metrics_per_namespace: true,
        }
    }
}
