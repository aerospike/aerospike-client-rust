// Copyright 2014-2024 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Configuration for the periodic metrics subsystem.

use std::collections::HashMap;

use super::histogram::HistogramType;
use crate::sampler::Sampler;

#[cfg(feature = "serialization")]
use serde::Serialize;

/// User-provided labels appended to metrics on export.
///
/// Each entry is a set of `key: value` pairs. Downstream metrics aggregators
/// use these to group/identify metrics collected by the client.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serialization", derive(Serialize))]
pub struct Labels(pub Vec<HashMap<String, String>>);

impl Labels {
    /// Creates an empty set of labels.
    #[must_use]
    pub fn new() -> Self {
        Labels(Vec::new())
    }

    /// Creates labels from the provided non-empty maps. Empty maps are skipped.
    #[must_use]
    pub fn with_pairs(pairs: Vec<HashMap<String, String>>) -> Self {
        Labels(pairs.into_iter().filter(|m| !m.is_empty()).collect())
    }

    /// Appends a label set.
    pub fn push(&mut self, entry: HashMap<String, String>) {
        if !entry.is_empty() {
            self.0.push(entry);
        }
    }

    /// Returns the label sets.
    #[must_use]
    pub fn entries(&self) -> &[HashMap<String, String>] {
        &self.0
    }
}

/// Default number of latency histogram columns (elapsed-time range buckets).
pub const DEFAULT_LATENCY_COLUMNS: usize = 7;
/// Default histogram base.
pub const DEFAULT_LATENCY_BASE: usize = 2;

/// Specifies client periodic metrics configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "dynamic-config", derive(aerospike_macro::Config))]
pub struct MetricsPolicy {
    /// Histogram bucket layout. Default: [`HistogramType::Logarithmic`].
    #[cfg_attr(feature = "dynamic-config", config(skip))]
    pub histogram_type: HistogramType,

    /// Number of elapsed-time range buckets in latency histograms. Bucket
    /// units are **milliseconds** (matching the Java client): with the default
    /// 7 columns and base 2, the logarithmic buckets are
    /// `<1ms <2ms <4ms <8ms <16ms <32ms >=32ms`.
    ///
    /// Default: 7 (matching the Java client's `latencyColumns`).
    pub latency_columns: usize,

    /// Histogram base.
    ///
    /// For logarithmic histograms the buckets are
    /// `<base^1 <base^2 ... >=base^(columns-1)`; for linear histograms they are
    /// `<base <base*2 ... >=base*(columns-1)`.
    ///
    /// Default: 2 — equivalent to the Java client's `latencyShift = 1`
    /// (`base = 2^shift`). In dynamic-config files this is the `latency_base`
    /// key (a direct multiplier), matching the Aerospike Go client.
    pub latency_base: usize,

    /// User-provided labels appended to metrics on export.
    #[cfg_attr(feature = "dynamic-config", config(skip))]
    pub labels: Labels,

    /// Decides, per command, whether it is sampled while metrics are enabled.
    ///
    /// A [`Sampler`] whose `range == threshold` records every command; a
    /// `threshold` of `0` ([`Sampler::never`]) records nothing; otherwise it
    /// records a `threshold / range` fraction. Defaults to [`Sampler::all`],
    /// so enabling metrics records every command unless a sampler is set.
    #[cfg_attr(feature = "dynamic-config", config(skip))]
    pub sampler: Sampler,
}

impl Default for MetricsPolicy {
    fn default() -> Self {
        MetricsPolicy {
            histogram_type: HistogramType::Logarithmic,
            latency_columns: DEFAULT_LATENCY_COLUMNS,
            latency_base: DEFAULT_LATENCY_BASE,
            labels: Labels::new(),
            sampler: Sampler::all(),
        }
    }
}

impl MetricsPolicy {
    /// Creates a default policy carrying the provided labels.
    #[must_use]
    pub fn default_with_labels(pairs: Vec<HashMap<String, String>>) -> Self {
        MetricsPolicy {
            labels: Labels::with_pairs(pairs),
            ..MetricsPolicy::default()
        }
    }

    /// Histogram base as a `u64` (the type histograms are built with).
    #[must_use]
    pub(crate) fn base(&self) -> u64 {
        self.latency_base as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_policy_matches_java_defaults() {
        // Pinned to _temp/aerospike-client-java MetricsPolicy: latencyColumns=7,
        // latencyShift=1 (== base 2). Note this deliberately diverges from the
        // Go client's 24-column default.
        let p = MetricsPolicy::default();
        assert_eq!(p.histogram_type, HistogramType::Logarithmic);
        assert_eq!(p.latency_columns, 7);
        assert_eq!(p.latency_base, 2);
        assert!(p.labels.entries().is_empty());
    }

    #[test]
    fn default_with_labels_carries_labels_and_skips_empties() {
        let mut a = HashMap::new();
        a.insert("dc".to_string(), "us-east".to_string());
        let empty = HashMap::new();
        let p = MetricsPolicy::default_with_labels(vec![a, empty]);
        // Defaults preserved, the empty map dropped.
        assert_eq!(p.latency_columns, DEFAULT_LATENCY_COLUMNS);
        assert_eq!(p.labels.entries().len(), 1);
        assert_eq!(p.labels.entries()[0].get("dc").unwrap(), "us-east");
    }

    #[test]
    fn labels_push_skips_empty() {
        let mut labels = Labels::new();
        labels.push(HashMap::new());
        assert!(labels.entries().is_empty());
        let mut m = HashMap::new();
        m.insert("k".to_string(), "v".to_string());
        labels.push(m);
        assert_eq!(labels.entries().len(), 1);
    }
}
