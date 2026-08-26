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
/// The v3 default is milliseconds (7 columns). [`MetricsPolicy::micros`]
/// selects microseconds with 24 columns when sub-millisecond resolution
/// matters.
///
/// Serialized in metrics snapshots, and read from config files, as `"us"` /
/// `"ms"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serialization", derive(Serialize))]
#[cfg_attr(feature = "dynamic-config", derive(Deserialize))]
pub enum LatencyUnit {
    /// Microseconds. With 24 columns and shift 1 the range-layout buckets are
    /// `<=1µs >1µs >2µs ... >=2^22 µs`.
    #[cfg_attr(
        any(feature = "serialization", feature = "dynamic-config"),
        serde(rename = "us", alias = "microseconds")
    )]
    Microseconds,
    /// Milliseconds. The default. With 7 columns and shift 1 the buckets are
    /// `<=1ms >1ms >2ms >4ms >8ms >16ms >32ms`.
    #[cfg_attr(
        any(feature = "serialization", feature = "dynamic-config"),
        serde(rename = "ms", alias = "milliseconds")
    )]
    #[default]
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

    /// Decodes [`Self::to_code`]. Unknown codes read as microseconds (code 0).
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

/// Latency columns that pair with [`LatencyUnit::Microseconds`].
///
/// A deliberate deviation from the spec's `latency_columns: 7` default,
/// which is sized for milliseconds: 7 microsecond columns top out at
/// `>32µs` and put nearly every sample in the last bucket. 24 columns at
/// shift 1 span `<=1µs` through `>4.2s`, covering the microsecond range up
/// to where the millisecond layout's ceiling lands (and matching the
/// original 24-column microsecond histograms this client shipped with).
pub const MICROS_LATENCY_COLUMNS: usize = 24;
/// Default number of latency histogram columns — millisecond range layout.
pub const DEFAULT_LATENCY_COLUMNS: usize = 7;
/// Latency columns that pair with [`LatencyUnit::Milliseconds`].
pub const MILLIS_LATENCY_COLUMNS: usize = DEFAULT_LATENCY_COLUMNS;
/// Default histogram multiplier (`2^DEFAULT_LATENCY_SHIFT`).
pub const DEFAULT_LATENCY_BASE: usize = 2;
/// Default range-layout shift. Boundaries after the first multiply by
/// `2^latency_shift`. `1` means no skipped powers of two.
pub const DEFAULT_LATENCY_SHIFT: usize = 1;

/// Specifies client periodic metrics configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "dynamic-config", derive(aerospike_macro::Config))]
pub struct MetricsPolicy {
    /// Resolution in which elapsed times are measured and bucketed.
    ///
    /// Set in code — usually via [`MetricsPolicy::millis`] /
    /// [`MetricsPolicy::micros`], which also pick the matching column count — or
    /// through the config file (`dynamic.metrics.extended.operational.latency_unit`
    /// or the flat `dynamic.metrics.latency_unit` alias). YAML accepts `ms` /
    /// `us` and `milliseconds` / `microseconds`.
    ///
    /// Changing it discards the latency samples collected so far: they were
    /// measured in the other unit and cannot share buckets with the new one.
    /// That is the same thing a `latency_columns` change does.
    ///
    /// Default: [`LatencyUnit::Milliseconds`].
    pub latency_unit: LatencyUnit,

    /// Number of elapsed-time range buckets in latency histograms. Bucket
    /// units are whatever [`latency_unit`](Self::latency_unit) says, so the two
    /// have to be chosen together: 7 columns of microseconds tops out at
    /// `>32µs` and puts nearly everything in the last bucket. The
    /// [`micros`](Self::micros) and [`millis`](Self::millis) presets pair them
    /// correctly.
    ///
    /// Default: 7.
    pub latency_columns: usize,

    /// Range-layout shift: each boundary after the first two (`<=1`, `>1`)
    /// multiplies by `2^latency_shift`. Default 1 (no skipped powers).
    ///
    /// The shift is the only stored parameter of the range layout — the
    /// multiplier is always computed as `1 << latency_shift`, so the two can
    /// never disagree. Set it through
    /// [`set_latency_shift`](Self::set_latency_shift) and read it through
    /// [`latency_shift`](Self::latency_shift).
    ///
    /// Canonical YAML key: `dynamic.metrics.extended.operational.latency_shift`.
    /// The flat `dynamic.metrics.latency_shift` and legacy `latency_base`
    /// aliases are still accepted.
    #[cfg_attr(feature = "dynamic-config", config(skip))]
    pub(crate) latency_shift: usize,

    /// When `true`, enabling metrics records per-command operational data
    /// (latency, bytes, errors). When `false` — the default — only Tier 0
    /// lifecycle counters update. Independent of
    /// [`usage_enabled`](Self::usage_enabled).
    ///
    /// Off by default so that enabling metrics does not silently add
    /// per-command timing to the hot path; opt in here or through
    /// `extended.operational.enabled`.
    #[cfg_attr(feature = "dynamic-config", config(skip))]
    pub operational_enabled: bool,

    /// When `true`, [`crate::Client::record_usage`] increments feature
    /// counters. Default `false`. Independent of
    /// [`operational_enabled`](Self::operational_enabled); both require
    /// metrics to be enabled on the cluster.
    #[cfg_attr(feature = "dynamic-config", config(skip))]
    pub usage_enabled: bool,

    /// User-provided labels appended to metrics on export.
    #[cfg_attr(feature = "dynamic-config", config(skip))]
    pub labels: Labels,

    /// Decides, per user API call (not per retry), whether operational
    /// metrics are recorded while metrics are enabled and
    /// [`operational_enabled`](Self::operational_enabled) is true.
    ///
    /// A [`Sampler`] whose `range == threshold` records every call; a
    /// `threshold` of `0` ([`Sampler::never`]) records nothing; otherwise it
    /// records a `threshold / range` fraction. Defaults to [`Sampler::all`].
    ///
    /// YAML: `dynamic.metrics.extended.operational.sampler` (`range`, `threshold`).
    #[cfg_attr(feature = "dynamic-config", config(skip))]
    pub sampler: Sampler,

    /// When `true`, operational TLS/auth handshake counters are recorded.
    /// Ignored while [`operational_enabled`](Self::operational_enabled) is
    /// false. Default `false`.
    #[cfg_attr(feature = "dynamic-config", config(skip))]
    pub tls_metrics_enabled: bool,
}

impl Default for MetricsPolicy {
    /// The [`millis`](MetricsPolicy::millis) preset.
    fn default() -> Self {
        MetricsPolicy::millis()
    }
}

impl MetricsPolicy {
    /// Millisecond-resolution latency histograms with 7 columns — the v3
    /// default, matching the Java-client / `asadm` range layout.
    ///
    /// Buckets: `<=1ms >1ms >2ms >4ms >8ms >16ms >32ms`. Sub-millisecond
    /// phases record `0` and land in the first bucket.
    #[must_use]
    pub fn millis() -> Self {
        MetricsPolicy {
            latency_unit: LatencyUnit::Milliseconds,
            latency_columns: MILLIS_LATENCY_COLUMNS,
            latency_shift: DEFAULT_LATENCY_SHIFT,
            operational_enabled: false,
            usage_enabled: false,
            labels: Labels::new(),
            sampler: Sampler::all(),
            tls_metrics_enabled: false,
        }
    }

    /// Microsecond-resolution latency histograms with 24 columns.
    ///
    /// Buckets: `<=1µs >1µs >2µs ...` with shift 1.
    #[must_use]
    pub fn micros() -> Self {
        MetricsPolicy {
            latency_unit: LatencyUnit::Microseconds,
            latency_columns: MICROS_LATENCY_COLUMNS,
            ..MetricsPolicy::millis()
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

    /// Sets the range-layout shift, which the histogram multiplier
    /// (`2^shift`) is computed from. `shift` is clamped to `1..=63`; a shift
    /// of `0` would give every boundary the same limit and leave all but the
    /// first and last columns unreachable.
    pub fn set_latency_shift(&mut self, shift: usize) {
        self.latency_shift = shift.clamp(1, 63);
    }

    /// Range-layout shift: each histogram boundary after the first two
    /// multiplies by `2^latency_shift`.
    #[must_use]
    pub fn latency_shift(&self) -> usize {
        self.latency_shift
    }

    /// Histogram multiplier, always `2^latency_shift`.
    #[must_use]
    pub fn latency_base(&self) -> usize {
        1usize
            .checked_shl(self.latency_shift as u32)
            .unwrap_or(usize::MAX)
    }

    /// Histogram multiplier as a `u64` (the type histograms are built with).
    #[must_use]
    pub(crate) fn base(&self) -> u64 {
        1u64.checked_shl(self.latency_shift as u32)
            .unwrap_or(u64::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_policy_is_the_millis_preset() {
        let p = MetricsPolicy::default();
        assert_eq!(p, MetricsPolicy::millis());
        assert_eq!(p.latency_unit, LatencyUnit::Milliseconds);
        assert_eq!(p.latency_columns, 7);
        assert_eq!(p.latency_shift, 1);
        assert_eq!(p.latency_base(), 2);
        assert!(!p.operational_enabled);
        assert!(!p.usage_enabled);
        assert!(!p.tls_metrics_enabled);
        assert!(p.labels.entries().is_empty());
    }

    #[test]
    fn millis_preset_matches_java_defaults() {
        let p = MetricsPolicy::millis();
        assert_eq!(p.latency_unit, LatencyUnit::Milliseconds);
        assert_eq!(p.latency_columns, 7);
        assert_eq!(p.latency_shift, 1);
        assert_eq!(p.latency_base(), 2);
    }

    #[test]
    fn micros_preset_keeps_24_columns() {
        let p = MetricsPolicy::micros();
        assert_eq!(p.latency_unit, LatencyUnit::Microseconds);
        assert_eq!(p.latency_columns, MICROS_LATENCY_COLUMNS);
        assert_eq!(p.latency_shift, 1);
        assert_eq!(p.latency_base(), 2);
    }

    #[test]
    fn set_latency_shift_drives_the_multiplier() {
        let mut p = MetricsPolicy::default();
        p.set_latency_shift(3);
        assert_eq!(p.latency_shift, 3);
        assert_eq!(p.latency_base(), 8);
        assert_eq!(p.base(), 8);
    }

    #[test]
    fn set_latency_shift_clamps_away_the_degenerate_layout() {
        // Shift 0 would leave every boundary at 1, so only the first and last
        // columns could ever be reached.
        let mut p = MetricsPolicy::default();
        p.set_latency_shift(0);
        assert_eq!(p.latency_shift, 1);
        assert_eq!(p.latency_base(), 2);

        p.set_latency_shift(usize::MAX);
        assert_eq!(p.latency_shift, 63);
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
        // Unknown codes decode as microseconds (wire code 0), not as the
        // enum's Default (milliseconds).
        assert_eq!(LatencyUnit::from_code(200), LatencyUnit::Microseconds);
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
        assert_eq!(
            serde_yml::from_str::<LatencyUnit>("microseconds").unwrap(),
            LatencyUnit::Microseconds
        );
        assert_eq!(
            serde_yml::from_str::<LatencyUnit>("milliseconds").unwrap(),
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
