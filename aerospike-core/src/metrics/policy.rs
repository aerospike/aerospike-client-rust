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
/// Milliseconds with 7 columns is the cross-client default (the Java client
/// and the `learn-metrics` log format); microseconds with 24 columns is what
/// the Go client records. Pick one with [`MetricsPolicy::millis`] or
/// [`MetricsPolicy::micros`].
///
/// Serialized in metrics snapshots, and read from config files, as `"us"` /
/// `"ms"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serialization", derive(Serialize))]
#[cfg_attr(feature = "dynamic-config", derive(Deserialize))]
pub enum LatencyUnit {
    /// Microseconds. With the default shift 1 and 24 columns the buckets are
    /// `<=1µs >1µs >2µs ... >4.2s`.
    #[cfg_attr(
        any(feature = "serialization", feature = "dynamic-config"),
        serde(rename = "us")
    )]
    Microseconds,
    /// Milliseconds. The default. With 7 columns and shift 1 the buckets are
    /// `<=1ms >1ms >2ms >4ms >8ms >16ms >32ms`, matching the Java client.
    #[cfg_attr(
        any(feature = "serialization", feature = "dynamic-config"),
        serde(rename = "ms")
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

    /// Decodes [`Self::to_code`]. Any unknown code reads as the default.
    #[must_use]
    pub(crate) const fn from_code(code: u8) -> Self {
        match code {
            0 => LatencyUnit::Microseconds,
            _ => LatencyUnit::Milliseconds,
        }
    }
}

impl std::fmt::Display for LatencyUnit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Default number of latency histogram columns (elapsed-time range buckets),
/// paired with the default [`LatencyUnit::Milliseconds`] — the cross-client
/// default (`metrics.md` §5.3, Java-client parity).
pub const DEFAULT_LATENCY_COLUMNS: usize = 7;
/// Latency columns that pair with [`LatencyUnit::Milliseconds`]. Same value as
/// [`DEFAULT_LATENCY_COLUMNS`]; used by [`MetricsPolicy::millis`].
pub const MILLIS_LATENCY_COLUMNS: usize = DEFAULT_LATENCY_COLUMNS;
/// Latency columns that pair with [`LatencyUnit::Microseconds`] — Go-client
/// parity. Used by [`MetricsPolicy::micros`].
pub const MICROS_LATENCY_COLUMNS: usize = 24;
/// Default histogram boundary spacing exponent: boundaries multiply by
/// `2^shift`, so `1` means every power of two (`>1 >2 >4 >8 ...`).
pub const DEFAULT_LATENCY_SHIFT: u32 = 1;

/// Specifies client periodic metrics configuration.
///
/// Collection is layered in two tiers (`metrics.md` §3):
///
/// - **Tier 0 (standard)** is on whenever metrics are enabled
///   ([`crate::Client::enable_metrics`]): pool gauges read at snapshot time,
///   connection opened/closed counts, tend and node add/remove counts. Nothing
///   on the command hot path.
/// - **Tier 1 (operational)** is opt-in through
///   [`operational`](Self::operational): per-command latency histograms,
///   bytes, result codes, retry/error counters, connection failure and
///   close-reason counters. Subject to the [`sampler`](Self::sampler) and the
///   `latency_*` histogram settings, which are ignored while it is off.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "dynamic-config", derive(aerospike_macro::Config))]
pub struct MetricsPolicy {
    /// Enables the Tier 1 **operational** group: latency histograms, bytes,
    /// result codes, command retry/error counters and connection failure /
    /// close-reason counters. Off, only the always-on Tier 0 instruments
    /// (pool gauges, opened/closed, tend and node counts) are recorded.
    ///
    /// In dynamic-config files this is
    /// `dynamic.metrics.extended.operational.enabled`; the `latency_*` keys
    /// live in the same block.
    ///
    /// Default: `false`.
    #[cfg_attr(feature = "dynamic-config", config(skip))]
    pub operational: bool,

    /// Resolution in which elapsed times are measured and bucketed.
    ///
    /// Set in code — usually via [`MetricsPolicy::millis`] /
    /// [`MetricsPolicy::micros`], which also pick the matching column count — or
    /// through the config file's `dynamic.metrics.extended.operational.latency_unit`
    /// key (`ms` / `us`), alongside `latency_columns` and `latency_shift`.
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
    /// [`millis`](Self::millis) and [`micros`](Self::micros) presets pair them
    /// correctly.
    ///
    /// Default: 7 (`<=1ms >1ms >2ms >4ms >8ms >16ms >32ms`).
    pub latency_columns: usize,

    /// Histogram boundary spacing exponent: after the `<=1` bucket every
    /// boundary is the previous one multiplied by `2^latency_shift`. `1` is
    /// every power of two (`>1 >2 >4 >8 ...`); `3` skips two powers at a time
    /// (`>1 >8 >64 ...`). Same semantics as the Java client's `latencyShift`,
    /// `asadm` and `asloglatency`. Values below 1 are treated as 1.
    ///
    /// Default: 1.
    pub latency_shift: u32,

    /// User-provided labels appended to metrics on export.
    #[cfg_attr(feature = "dynamic-config", config(skip))]
    pub labels: Labels,

    /// Decides, per command, whether its operational metrics are recorded.
    ///
    /// A [`Sampler`] whose `range == threshold` records every command; a
    /// `threshold` of `0` ([`Sampler::never`]) records nothing; otherwise it
    /// records a `threshold / range` fraction. The decision is made once per
    /// user call (before any retry) and covers everything that call records.
    /// Defaults to [`Sampler::all`].
    #[cfg_attr(feature = "dynamic-config", config(skip))]
    pub sampler: Sampler,
}

impl Default for MetricsPolicy {
    /// The [`millis`](MetricsPolicy::millis) preset with the operational tier
    /// off.
    fn default() -> Self {
        MetricsPolicy::millis()
    }
}

impl MetricsPolicy {
    /// Millisecond-resolution latency histograms with 7 columns — the
    /// cross-client default (`metrics.md` §5.3, Java-client parity).
    ///
    /// Buckets: `<=1ms >1ms >2ms >4ms >8ms >16ms >32ms`. Sub-millisecond
    /// phases record `0` and land in the first bucket. The operational tier
    /// is off; turn it on with [`with_operational`](Self::with_operational).
    #[must_use]
    pub fn millis() -> Self {
        MetricsPolicy {
            operational: false,
            latency_unit: LatencyUnit::Milliseconds,
            latency_columns: MILLIS_LATENCY_COLUMNS,
            latency_shift: DEFAULT_LATENCY_SHIFT,
            labels: Labels::new(),
            sampler: Sampler::all(),
        }
    }

    /// Microsecond-resolution latency histograms with 24 columns — Go-client
    /// parity.
    ///
    /// Buckets: `<=1µs >1µs >2µs ... >4.2s`.
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

    /// Returns this policy with the Tier 1 operational group turned on or
    /// off. Chainable with the presets:
    /// `MetricsPolicy::micros().with_operational(true)`.
    #[must_use]
    pub const fn with_operational(mut self, on: bool) -> Self {
        self.operational = on;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_policy_is_the_millis_preset_with_operational_off() {
        // Pinned to metrics.md §5.3 / §7: milliseconds, 7 columns, shift 1,
        // `extended.operational.enabled: false`.
        let p = MetricsPolicy::default();
        assert_eq!(p, MetricsPolicy::millis());
        assert!(!p.operational);
        assert_eq!(p.latency_unit, LatencyUnit::Milliseconds);
        assert_eq!(p.latency_columns, 7);
        assert_eq!(p.latency_shift, 1);
        assert_eq!(p.sampler, Sampler::all());
        assert!(p.labels.entries().is_empty());
    }

    #[test]
    fn micros_preset_matches_go_defaults() {
        // Pinned to the Go client (metrics_policy.go): microseconds,
        // LatencyColumns=24, base 2 (== shift 1).
        let p = MetricsPolicy::micros();
        assert_eq!(p.latency_unit, LatencyUnit::Microseconds);
        assert_eq!(p.latency_columns, 24);
        assert_eq!(p.latency_shift, 1);
        assert!(!p.operational);
    }

    #[test]
    fn with_operational_flips_only_the_tier_flag() {
        let p = MetricsPolicy::micros().with_operational(true);
        assert!(p.operational);
        assert_eq!(p.latency_columns, 24);
        assert!(!p.with_operational(false).operational);
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
        assert_eq!(LatencyUnit::default(), LatencyUnit::Milliseconds);
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
