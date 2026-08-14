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
use std::time::Duration;

use super::histogram::HistogramType;
use crate::sampler::Sampler;

#[cfg(feature = "dynamic-config")]
use serde::Deserialize;
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

/// Resolution in which elapsed-time metrics are measured and bucketed.
///
/// This is the unit of every *time* value in the latency histograms — total
/// command latency, connection-acquire time and parse time. Size histograms
/// (bytes sent/received) are unaffected.
///
/// It is also the axis on which the Aerospike clients differ: the Go client
/// records microseconds with 24 columns, the Java client milliseconds with 7.
/// Pick one with [`MetricsPolicy::micros`] or [`MetricsPolicy::millis`].
///
/// Serialized in metrics snapshots, and read from config files, as `"us"` /
/// `"ms"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serialization", derive(Serialize))]
#[cfg_attr(feature = "dynamic-config", derive(Deserialize))]
pub enum LatencyUnit {
    /// Microseconds. The default. With the default base 2 and 24 columns the
    /// logarithmic buckets are `<1µs <2µs <4µs ... >=8.4s`.
    #[cfg_attr(
        any(feature = "serialization", feature = "dynamic-config"),
        serde(rename = "us")
    )]
    #[default]
    Microseconds,
    /// Milliseconds. With 7 columns and base 2 the buckets are
    /// `<1ms <2ms <4ms <8ms <16ms <32ms >=32ms`, matching the Java client.
    #[cfg_attr(
        any(feature = "serialization", feature = "dynamic-config"),
        serde(rename = "ms")
    )]
    Milliseconds,
}

impl LatencyUnit {
    /// Short name used in serialized metrics and in config files.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            LatencyUnit::Microseconds => "us",
            LatencyUnit::Milliseconds => "ms",
        }
    }

    /// Converts an elapsed duration into a histogram value in this unit.
    ///
    /// The only place a [`Duration`] becomes a metric value: every recorder
    /// funnels through here, so no call site can disagree about the unit.
    /// Truncates towards zero (a 999µs phase is `0` in millisecond mode) and
    /// saturates rather than wrapping.
    #[must_use]
    pub(crate) fn value(self, elapsed: Duration) -> u64 {
        let ticks = match self {
            LatencyUnit::Microseconds => elapsed.as_micros(),
            LatencyUnit::Milliseconds => elapsed.as_millis(),
        };
        u64::try_from(ticks).unwrap_or(u64::MAX)
    }

    /// Encodes the unit for lock-free storage (see `NodeMetrics::latency_unit`).
    #[must_use]
    pub(crate) const fn to_code(self) -> u8 {
        match self {
            LatencyUnit::Microseconds => 0,
            LatencyUnit::Milliseconds => 1,
        }
    }

    /// Decodes [`Self::to_code`]. Any unknown code reads as the default.
    #[must_use]
    pub(crate) const fn from_code(code: u8) -> Self {
        match code {
            1 => LatencyUnit::Milliseconds,
            _ => LatencyUnit::Microseconds,
        }
    }
}

impl std::fmt::Display for LatencyUnit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Default number of latency histogram columns (elapsed-time range buckets),
/// paired with the default [`LatencyUnit::Microseconds`] — Go-client parity.
pub const DEFAULT_LATENCY_COLUMNS: usize = 24;
/// Latency columns that pair with [`LatencyUnit::Milliseconds`] — Java-client
/// parity. Used by [`MetricsPolicy::millis`].
pub const MILLIS_LATENCY_COLUMNS: usize = 7;
/// Default histogram base.
pub const DEFAULT_LATENCY_BASE: usize = 2;

/// Specifies client periodic metrics configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "dynamic-config", derive(aerospike_macro::Config))]
pub struct MetricsPolicy {
    /// Histogram bucket layout. Default: [`HistogramType::Logarithmic`].
    #[cfg_attr(feature = "dynamic-config", config(skip))]
    pub histogram_type: HistogramType,

    /// Resolution in which elapsed times are measured and bucketed.
    ///
    /// Set in code — usually via [`MetricsPolicy::micros`] /
    /// [`MetricsPolicy::millis`], which also pick the matching column count — or
    /// through the config file's `dynamic.metrics.latency_unit` key (`us` /
    /// `ms`), alongside `latency_columns` and `latency_base`.
    ///
    /// Changing it discards the latency samples collected so far: they were
    /// measured in the other unit and cannot share buckets with the new one.
    /// That is the same thing a `latency_columns` change does.
    ///
    /// Default: [`LatencyUnit::Microseconds`].
    pub latency_unit: LatencyUnit,

    /// Number of elapsed-time range buckets in latency histograms. Bucket
    /// units are whatever [`latency_unit`](Self::latency_unit) says, so the two
    /// have to be chosen together: 7 columns of microseconds tops out at
    /// `>=64µs` and puts nearly everything in the last bucket. The
    /// [`micros`](Self::micros) and [`millis`](Self::millis) presets pair them
    /// correctly.
    ///
    /// Default: 24, matching the Go client's microsecond histograms
    /// (`<1µs ... >=8.4s`).
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
    /// The [`micros`](MetricsPolicy::micros) preset.
    fn default() -> Self {
        MetricsPolicy::micros()
    }
}

impl MetricsPolicy {
    /// Microsecond-resolution latency histograms with 24 columns — Go-client
    /// parity, and the default.
    ///
    /// Buckets: `<1µs <2µs <4µs ... >=8.4s`.
    #[must_use]
    pub fn micros() -> Self {
        MetricsPolicy {
            histogram_type: HistogramType::Logarithmic,
            latency_unit: LatencyUnit::Microseconds,
            latency_columns: DEFAULT_LATENCY_COLUMNS,
            latency_base: DEFAULT_LATENCY_BASE,
            labels: Labels::new(),
            sampler: Sampler::all(),
        }
    }

    /// Millisecond-resolution latency histograms with 7 columns — Java-client
    /// parity, and what this client recorded before the unit was configurable.
    ///
    /// Buckets: `<1ms <2ms <4ms <8ms <16ms <32ms >=32ms`. Sub-millisecond
    /// phases record `0` and land in the first bucket.
    #[must_use]
    pub fn millis() -> Self {
        MetricsPolicy {
            latency_unit: LatencyUnit::Milliseconds,
            latency_columns: MILLIS_LATENCY_COLUMNS,
            ..MetricsPolicy::micros()
        }
    }

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
    fn default_policy_is_the_micros_preset() {
        // Pinned to the Go client (metrics_policy.go): microseconds,
        // LatencyColumns=24, base 2.
        let p = MetricsPolicy::default();
        assert_eq!(p, MetricsPolicy::micros());
        assert_eq!(p.latency_unit, LatencyUnit::Microseconds);
        assert_eq!(p.histogram_type, HistogramType::Logarithmic);
        assert_eq!(p.latency_columns, 24);
        assert_eq!(p.latency_base, 2);
        assert!(p.labels.entries().is_empty());
    }

    #[test]
    fn millis_preset_matches_java_defaults() {
        // Pinned to _temp/aerospike-client-java MetricsPolicy: latencyColumns=7,
        // latencyShift=1 (== base 2), bucket units milliseconds.
        let p = MetricsPolicy::millis();
        assert_eq!(p.latency_unit, LatencyUnit::Milliseconds);
        assert_eq!(p.latency_columns, 7);
        assert_eq!(p.latency_base, 2);
        assert_eq!(p.histogram_type, HistogramType::Logarithmic);
    }

    #[test]
    fn unit_converts_and_truncates_towards_zero() {
        let us = LatencyUnit::Microseconds;
        let ms = LatencyUnit::Milliseconds;

        // A sub-millisecond phase is visible in microseconds and 0 in millis -
        // the whole point of the knob.
        assert_eq!(us.value(Duration::from_micros(999)), 999);
        assert_eq!(ms.value(Duration::from_micros(999)), 0);

        assert_eq!(us.value(Duration::from_millis(3)), 3_000);
        assert_eq!(ms.value(Duration::from_millis(3)), 3);

        // Sub-unit remainders truncate rather than round.
        assert_eq!(ms.value(Duration::from_micros(1_999)), 1);
        assert_eq!(us.value(Duration::from_nanos(1_999)), 1);
        assert_eq!(us.value(Duration::ZERO), 0);
    }

    #[test]
    fn unit_value_saturates_instead_of_wrapping() {
        // as_micros() is u128, so a long enough duration overflows u64.
        let huge = Duration::from_secs(u64::MAX);
        assert_eq!(LatencyUnit::Microseconds.value(huge), u64::MAX);
        assert_eq!(LatencyUnit::Milliseconds.value(huge), u64::MAX);
    }

    #[test]
    fn unit_code_round_trips() {
        for unit in [LatencyUnit::Microseconds, LatencyUnit::Milliseconds] {
            assert_eq!(LatencyUnit::from_code(unit.to_code()), unit);
        }
        // Unknown codes decode as the default rather than panicking.
        assert_eq!(LatencyUnit::from_code(200), LatencyUnit::default());
    }

    #[test]
    fn unit_names_are_us_and_ms() {
        assert_eq!(LatencyUnit::Microseconds.as_str(), "us");
        assert_eq!(LatencyUnit::Milliseconds.as_str(), "ms");
        assert_eq!(LatencyUnit::Milliseconds.to_string(), "ms");
    }

    #[cfg(feature = "serialization")]
    #[test]
    fn unit_serializes_as_us_and_ms() {
        assert_eq!(
            serde_json::to_value(LatencyUnit::Microseconds).unwrap(),
            serde_json::json!("us")
        );
        assert_eq!(
            serde_json::to_value(LatencyUnit::Milliseconds).unwrap(),
            serde_json::json!("ms")
        );
    }

    #[cfg(feature = "dynamic-config")]
    #[test]
    fn unit_deserializes_from_us_and_ms() {
        assert_eq!(
            serde_yml::from_str::<LatencyUnit>("us").unwrap(),
            LatencyUnit::Microseconds
        );
        assert_eq!(
            serde_yml::from_str::<LatencyUnit>("ms").unwrap(),
            LatencyUnit::Milliseconds
        );
        // Anything else is a config error, not a silent default.
        assert!(serde_yml::from_str::<LatencyUnit>("seconds").is_err());
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
