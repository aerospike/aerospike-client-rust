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

//! Dynamic configuration.
//!
//! Loads policy overrides from a pluggable [`ConfigProvider`] (a YAML file out of
//! the box) and applies them to client policies at runtime. The config document
//! has a `static` section (applied once, at client construction) and a `dynamic`
//! section (re-applied every refresh).
//!
//! Enable the `dynamic-config` cargo feature, then either inject a provider with
//! [`Client::new_with_config`](crate::Client::new_with_config) or set the
//! [`AEROSPIKE_CLIENT_CONFIG_URL`](registry::CONFIG_URL_ENV) environment variable
//! (e.g. `file:///etc/aerospike/config.yaml`) and use
//! [`Client::new`](crate::Client::new).
//!
//! Config keys and value units match the cross-client Aerospike config spec, so a
//! single file can be shared across Aerospike clients. Sections or keys that
//! this client does not (yet) support are ignored, not errors.

mod dyn_config;
mod provider;
mod registry;
mod yaml;

pub(crate) use dyn_config::DynConfig;
pub use provider::ConfigProvider;
pub use registry::{register_provider, ProviderFactory, CONFIG_URL_ENV};
pub(crate) use registry::provider_from_env;
pub use yaml::YamlFileProvider;

use serde::Deserialize;

use crate::batch::{BatchDeletePolicyConfig, BatchUDFPolicyConfig};
use crate::metrics::MetricsPolicyConfig;
use crate::policy::{
    BatchPolicyConfig, ClientPolicyConfig, QueryPolicyConfig, ReadPolicyConfig, TxnRollPolicyConfig,
    TxnVerifyPolicyConfig, WritePolicyConfig,
};

/// Converts a seconds value (the unit used for a few keys in config files) into
/// milliseconds (the unit of the corresponding policy fields). Saturates rather
/// than overflowing. Referenced by `#[config(with = ...)]` on those fields.
#[must_use]
pub(crate) const fn secs_to_ms(secs: u32) -> u32 {
    secs.saturating_mul(1000)
}

/// Top-level dynamic-configuration document, as deserialized from a source.
#[derive(Debug, Default, Clone, Deserialize)]
pub struct ConfigDocument {
    /// Schema version string. Providers require it to be present before applying.
    pub version: Option<String>,

    /// Settings honored once, at client construction (cannot take effect at runtime).
    #[serde(rename = "static")]
    pub static_config: Option<StaticConfig>,

    /// Settings re-applied to client policies on every refresh.
    pub dynamic: Option<DynamicConfig>,
}

/// The `static` section. Only client-level fields can be static.
#[derive(Debug, Default, Clone, Deserialize)]
pub struct StaticConfig {
    /// Static client-policy overrides (connection pool sizing, config interval).
    pub client: Option<ClientPolicyConfig>,
}

/// The `dynamic` section: per-policy override groups, each re-applied on refresh.
#[derive(Debug, Default, Clone, Deserialize)]
pub struct DynamicConfig {
    /// Dynamic client-policy overrides (timeouts, error rates, rack ids, …).
    pub client: Option<ClientPolicyConfig>,
    /// Overrides for read policies (`get`, `exists`, …).
    pub read: Option<ReadPolicyConfig>,
    /// Overrides for write policies (`put`, `delete`, `operate`, …).
    pub write: Option<WritePolicyConfig>,
    /// Overrides for query policies.
    pub query: Option<QueryPolicyConfig>,
    /// Overrides for the parent batch policy (`client.batch`).
    pub batch: Option<BatchPolicyConfig>,
    /// Overrides for batch *read*. Carries the per-record read fields (read
    /// modes, timeouts, replica — like [`read`](Self::read)) plus the
    /// batch-command wire flags (`allow_inline`, `allow_inline_ssd`,
    /// `respond_all_keys`). Matches the Go client, which applies those flags to
    /// the parent batch policy from the `batch_read` section.
    pub batch_read: Option<BatchReadSectionConfig>,
    /// Overrides for per-record batch *write* sub-policies. Carries the same
    /// fields as [`write`](Self::write) (timeouts, `send_key`, `durable_delete`).
    pub batch_write: Option<WritePolicyConfig>,
    /// Overrides for per-record batch *delete* sub-policies (`send_key`,
    /// `durable_delete`).
    pub batch_delete: Option<BatchDeletePolicyConfig>,
    /// Overrides for per-record batch *UDF* sub-policies (`send_key`,
    /// `durable_delete`).
    pub batch_udf: Option<BatchUDFPolicyConfig>,
    /// Overrides for the multi-record-transaction *verify* policy (applied by
    /// `commit`). Carries the `BasePolicy` knobs (timeouts, retries, read
    /// modes); Go's batch-only txn keys have no per-key analogue here.
    pub txn_verify: Option<TxnVerifyPolicyConfig>,
    /// Overrides for the multi-record-transaction *roll* policy (applied by
    /// `commit` and `abort`). Carries the `BasePolicy` knobs.
    pub txn_roll: Option<TxnRollPolicyConfig>,
    /// Metrics enable flag plus metrics-policy overrides.
    pub metrics: Option<MetricsConfig>,
}

/// The `dynamic.batch_read` section.
///
/// The flattened [`ReadPolicyConfig`] drives each batch read's per-record read
/// policy; the three batch-command wire flags are applied to the parent batch
/// policy (matching the Go client's `batch_read` handling).
/// `max_concurrent_thread` is intentionally omitted — it is dead in the Go
/// client too.
#[derive(Debug, Default, Clone, Deserialize)]
pub struct BatchReadSectionConfig {
    /// Per-record read overrides (read modes, timeouts, retries, replica).
    #[serde(flatten)]
    pub read: ReadPolicyConfig,
    /// Allow the server to process the batch inline in the receiving thread.
    pub allow_inline: Option<bool>,
    /// Allow inline batch processing for SSD namespaces.
    pub allow_inline_ssd: Option<bool>,
    /// Attempt every key regardless of prior key-specific errors.
    pub respond_all_keys: Option<bool>,
}

/// The `dynamic.metrics` section. Carries the `enable` toggle (which is not a
/// [`MetricsPolicy`](crate::metrics::MetricsPolicy) field) alongside the
/// macro-generated policy overrides.
#[derive(Debug, Default, Clone, Deserialize)]
pub struct MetricsConfig {
    /// Turn metrics collection on/off. `None` leaves the current state unchanged.
    pub enable: Option<bool>,

    /// Custom metadata labels shipped with metrics, as a flat key/value map
    /// (the cross-client `metrics.labels` schema). Applied as a single label
    /// entry on the metrics policy. `None` leaves existing labels intact; an
    /// empty map is treated the same as no labels.
    pub labels: Option<std::collections::HashMap<String, String>>,

    /// Metrics-policy field overrides (latency histogram shape).
    #[serde(flatten)]
    pub policy: MetricsPolicyConfig,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::{BatchDeletePolicy, BatchUDFPolicy};
    use crate::metrics::MetricsPolicy;
    use crate::policy::{
        BatchPolicy, ClientPolicy, QueryDuration, QueryPolicy, ReadModeAP, ReadModeSC, ReadPolicy,
        Replica, TxnRollPolicy, TxnVerifyPolicy, WritePolicy,
    };

    /// Parses a single YAML scalar into a config type `T`.
    fn parse<T: for<'de> serde::Deserialize<'de>>(yaml: &str) -> Result<T, serde_yml::Error> {
        serde_yml::from_str(yaml)
    }

    const SAMPLE: &str = r#"
version: "1.0.0"
static:
  client:
    config_interval: 5
    max_connections_per_node: 512
    min_connections_per_node: 10
dynamic:
  client:
    timeout: 2500
    max_socket_idle: 55
    use_service_alternate: true
    app_id: "billing"
  read:
    read_mode_ap: ALL
    socket_timeout: 1234
    total_timeout: 5678
    max_retries: 9
    replica: PREFER_RACK
  write:
    send_key: true
    durable_delete: true
    socket_timeout: 4321
  metrics:
    enable: true
    latency_columns: 9
    latency_base: 3
"#;

    fn document() -> ConfigDocument {
        serde_yml::from_str(SAMPLE).expect("sample config should parse")
    }

    #[test]
    fn parses_version_and_sections() {
        let doc = document();
        assert_eq!(doc.version.as_deref(), Some("1.0.0"));
        assert!(doc.static_config.is_some());
        let dynamic = doc.dynamic.unwrap();
        assert!(dynamic.client.is_some());
        assert!(dynamic.read.is_some());
        assert!(dynamic.write.is_some());
        assert!(dynamic.metrics.is_some());
    }

    #[test]
    fn read_section_merges_base_and_replica() {
        // Flattened base-policy keys + the read-specific replica key both apply.
        let read = document().dynamic.unwrap().read.unwrap();
        let mut policy = ReadPolicy::default();
        read.merge_into(&mut policy);
        assert_eq!(policy.base_policy.read_mode_ap, ReadModeAP::All);
        assert_eq!(policy.base_policy.socket_timeout, 1234);
        assert_eq!(policy.base_policy.total_timeout, 5678);
        assert_eq!(policy.base_policy.max_retries, 9);
        assert_eq!(policy.replica, Replica::PreferRack);
    }

    #[test]
    fn write_section_only_overrides_present_keys() {
        let write = document().dynamic.unwrap().write.unwrap();
        let mut policy = WritePolicy::default();
        let untouched_total = policy.base_policy.total_timeout;
        write.merge_into(&mut policy);
        assert!(policy.send_key);
        assert!(policy.durable_delete);
        assert_eq!(policy.base_policy.socket_timeout, 4321);
        // `total_timeout` is absent from the YAML, so it must be left as-is.
        assert_eq!(policy.base_policy.total_timeout, untouched_total);
    }

    #[test]
    fn client_static_and_dynamic_split_with_unit_conversion() {
        let doc = document();
        let static_client = doc.static_config.unwrap().client.unwrap();
        let dynamic_client = doc.dynamic.unwrap().client.unwrap();

        // Static merge applies only the `startup` fields, converting seconds→ms.
        let mut cp = ClientPolicy::default();
        static_client.clone().merge_static_into(&mut cp);
        assert_eq!(cp.config_interval, 5_000, "config_interval is seconds in YAML");
        assert_eq!(cp.max_conns_per_node, 512);
        assert_eq!(cp.min_conns_per_node, 10);
        // The dynamic merge must NOT touch static fields.
        let mut cp_dyn = ClientPolicy::default();
        static_client.merge_into(&mut cp_dyn);
        assert_eq!(cp_dyn.max_conns_per_node, ClientPolicy::default().max_conns_per_node);

        // Dynamic merge applies dynamic fields, converting max_socket_idle seconds→ms.
        let mut cp = ClientPolicy::default();
        dynamic_client.merge_into(&mut cp);
        assert_eq!(cp.timeout, 2_500);
        assert_eq!(cp.idle_timeout, 55_000, "max_socket_idle is seconds in YAML");
        assert!(cp.use_services_alternate);
        assert_eq!(cp.application_id.as_deref(), Some("billing"));
        // config_interval is a startup field — untouched by the dynamic merge.
        assert_eq!(cp.config_interval, ClientPolicy::default().config_interval);
    }

    #[test]
    fn metrics_section_carries_enable_and_policy_overrides() {
        let metrics = document().dynamic.unwrap().metrics.unwrap();
        assert_eq!(metrics.enable, Some(true));
        let mut mp = crate::metrics::MetricsPolicy::default();
        metrics.policy.merge_into(&mut mp);
        assert_eq!(mp.latency_columns, 9);
        assert_eq!(mp.latency_base, 3);
        // The SAMPLE has no labels key, so labels stays absent.
        assert!(metrics.enable.is_some());
        assert!(metrics.labels.is_none());
    }

    #[test]
    fn metrics_section_parses_labels_map_alongside_overrides() {
        // Labels are a flat key/value map (cross-client schema); the latency
        // overrides flattened into `policy` must still be captured next to it.
        let yaml = "\
enable: true
latency_columns: 11
labels:
  app_id: billing
  team: payments
";
        let metrics: MetricsConfig = parse(yaml).expect("metrics section should parse");
        assert_eq!(metrics.enable, Some(true));
        let labels = metrics.labels.as_ref().expect("labels should be present");
        assert_eq!(labels.get("app_id").map(String::as_str), Some("billing"));
        assert_eq!(labels.get("team").map(String::as_str), Some("payments"));

        let mut mp = crate::metrics::MetricsPolicy::default();
        metrics.policy.clone().merge_into(&mut mp);
        assert_eq!(mp.latency_columns, 11);
    }

    #[test]
    fn txn_verify_and_roll_sections_merge() {
        let doc: ConfigDocument = serde_yml::from_str(
            "version: \"1.0.0\"\n\
             dynamic:\n\
             \x20 txn_verify:\n    socket_timeout: 1500\n    max_retries: 9\n    read_mode_sc: LINEARIZE\n    replica: PREFER_RACK\n\
             \x20 txn_roll:\n    total_timeout: 7000\n    max_retries: 4\n    respond_all_keys: false\n",
        )
        .unwrap();
        let dynamic = doc.dynamic.unwrap();

        let mut vp = TxnVerifyPolicy::default();
        dynamic.txn_verify.unwrap().merge_into(&mut vp);
        assert_eq!(vp.batch_policy.base_policy.socket_timeout, 1500);
        assert_eq!(vp.batch_policy.base_policy.max_retries, 9);
        assert_eq!(vp.batch_policy.base_policy.read_mode_sc, ReadModeSC::Linearize);
        // Batch knob (replica) flows through — the whole point of wrapping BatchPolicy.
        assert_eq!(vp.batch_policy.replica, Replica::PreferRack);
        // total_timeout is absent → the TxnVerifyPolicy default (10s) is preserved.
        assert_eq!(vp.batch_policy.base_policy.total_timeout, 10_000);

        let mut rp = TxnRollPolicy::default();
        dynamic.txn_roll.unwrap().merge_into(&mut rp);
        assert_eq!(rp.batch_policy.base_policy.total_timeout, 7000);
        assert_eq!(rp.batch_policy.base_policy.max_retries, 4);
        assert!(!rp.batch_policy.respond_all_keys); // batch knob applied
        // socket_timeout absent → the TxnRollPolicy default (3s) is preserved.
        assert_eq!(rp.batch_policy.base_policy.socket_timeout, 3_000);
    }

    #[test]
    fn document_without_version_still_parses() {
        // The provider enforces `version` presence; the document model itself
        // tolerates its absence so parsing never hard-fails on it.
        let doc: ConfigDocument =
            serde_yml::from_str("dynamic:\n  read:\n    max_retries: 1\n").unwrap();
        assert!(doc.version.is_none());
        // The flattened base config still captured the key.
        let read = doc.dynamic.unwrap().read.unwrap();
        let mut p = ReadPolicy::default();
        read.merge_into(&mut p);
        assert_eq!(p.base_policy.max_retries, 1);
    }

    // ---- Enum YAML deserialization (mirrors Go's dynconfig_serialze_test.go) ----
    // Valid tokens parse case-insensitively; invalid / empty / numeric reject.

    #[test]
    fn read_mode_ap_enum_deserialization() {
        assert_eq!(parse::<ReadModeAP>("ONE").unwrap(), ReadModeAP::One);
        assert_eq!(parse::<ReadModeAP>("ALL").unwrap(), ReadModeAP::All);
        assert_eq!(parse::<ReadModeAP>("one").unwrap(), ReadModeAP::One); // case-insensitive
        assert!(parse::<ReadModeAP>("\"foo\"").is_err());
        assert!(parse::<ReadModeAP>("\"\"").is_err());
        assert!(parse::<ReadModeAP>("\"123\"").is_err());
    }

    #[test]
    fn read_mode_sc_enum_deserialization() {
        assert_eq!(parse::<ReadModeSC>("SESSION").unwrap(), ReadModeSC::Session);
        assert_eq!(parse::<ReadModeSC>("LINEARIZE").unwrap(), ReadModeSC::Linearize);
        assert_eq!(
            parse::<ReadModeSC>("ALLOW_REPLICA").unwrap(),
            ReadModeSC::AllowReplica
        );
        assert_eq!(
            parse::<ReadModeSC>("ALLOW_UNAVAILABLE").unwrap(),
            ReadModeSC::AllowUnavailable
        );
        assert_eq!(parse::<ReadModeSC>("session").unwrap(), ReadModeSC::Session);
        assert!(parse::<ReadModeSC>("\"foo\"").is_err());
        assert!(parse::<ReadModeSC>("\"\"").is_err());
        assert!(parse::<ReadModeSC>("\"123\"").is_err());
    }

    #[test]
    fn replica_enum_deserialization() {
        assert_eq!(parse::<Replica>("MASTER").unwrap(), Replica::Master);
        assert_eq!(
            parse::<Replica>("MASTER_PROLES").unwrap(),
            Replica::MasterProles
        );
        assert_eq!(parse::<Replica>("SEQUENCE").unwrap(), Replica::Sequence);
        assert_eq!(parse::<Replica>("PREFER_RACK").unwrap(), Replica::PreferRack);
        assert_eq!(parse::<Replica>("master").unwrap(), Replica::Master);
        assert!(parse::<Replica>("\"foo\"").is_err());
        assert!(parse::<Replica>("\"\"").is_err());
        assert!(parse::<Replica>("\"123\"").is_err());
    }

    #[test]
    fn query_duration_enum_deserialization() {
        assert_eq!(parse::<QueryDuration>("LONG").unwrap(), QueryDuration::Long);
        assert_eq!(parse::<QueryDuration>("SHORT").unwrap(), QueryDuration::Short);
        assert_eq!(
            parse::<QueryDuration>("LONG_RELAX_AP").unwrap(),
            QueryDuration::LongRelaxAP
        );
        assert_eq!(parse::<QueryDuration>("long").unwrap(), QueryDuration::Long);
        assert!(parse::<QueryDuration>("\"foo\"").is_err());
        assert!(parse::<QueryDuration>("\"\"").is_err());
        assert!(parse::<QueryDuration>("\"123\"").is_err());
    }

    // ---- Per-policy full + partial merges (mirrors Go's *_policy_config_test.go) ----

    #[test]
    fn read_full_merge_covers_all_base_fields() {
        let cfg: ReadPolicyConfig = parse(
            "read_mode_ap: ALL\nread_mode_sc: LINEARIZE\nsocket_timeout: 3\ntotal_timeout: 5\n\
             max_retries: 3\nsleep_between_retries: 2\ntimeout_delay: 7\nreplica: PREFER_RACK\n",
        )
        .unwrap();
        let mut p = ReadPolicy::default();
        cfg.merge_into(&mut p);
        assert_eq!(p.base_policy.read_mode_ap, ReadModeAP::All);
        assert_eq!(p.base_policy.read_mode_sc, ReadModeSC::Linearize);
        assert_eq!(p.base_policy.socket_timeout, 3);
        assert_eq!(p.base_policy.total_timeout, 5);
        assert_eq!(p.base_policy.max_retries, 3);
        assert_eq!(p.base_policy.sleep_between_retries, 2);
        assert_eq!(p.base_policy.timeout_delay, 7);
        assert_eq!(p.replica, Replica::PreferRack);
    }

    #[test]
    fn read_partial_merge_preserves_defaults() {
        let default = ReadPolicy::default();
        let cfg: ReadPolicyConfig = parse("socket_timeout: 3\nreplica: PREFER_RACK\n").unwrap();
        let mut p = ReadPolicy::default();
        cfg.merge_into(&mut p);
        assert_eq!(p.base_policy.socket_timeout, 3); // overridden
        assert_eq!(p.replica, Replica::PreferRack); // overridden
        assert_eq!(p.base_policy.total_timeout, default.base_policy.total_timeout); // kept
        assert_eq!(p.base_policy.max_retries, default.base_policy.max_retries); // kept
        assert_eq!(p.base_policy.read_mode_ap, default.base_policy.read_mode_ap); // kept
    }

    #[test]
    fn write_full_merge() {
        let cfg: WritePolicyConfig = parse(
            "socket_timeout: 3\ntotal_timeout: 5000\nmax_retries: 3\nsleep_between_retries: 2\n\
             send_key: true\ndurable_delete: true\n",
        )
        .unwrap();
        let mut p = WritePolicy::default();
        cfg.merge_into(&mut p);
        assert_eq!(p.base_policy.socket_timeout, 3);
        assert_eq!(p.base_policy.total_timeout, 5000);
        assert_eq!(p.base_policy.max_retries, 3);
        assert_eq!(p.base_policy.sleep_between_retries, 2);
        assert!(p.send_key);
        assert!(p.durable_delete);
    }

    #[test]
    fn query_full_merge() {
        let cfg: QueryPolicyConfig = parse(
            "socket_timeout: 3\ntotal_timeout: 3000\nmax_retries: 3\nsleep_between_retries: 2\n\
             replica: PREFER_RACK\ninclude_bin_data: false\nrecord_queue_size: 50\n\
             expected_duration: SHORT\n",
        )
        .unwrap();
        let mut p = QueryPolicy::default();
        cfg.merge_into(&mut p);
        assert_eq!(p.base_policy.socket_timeout, 3);
        assert_eq!(p.base_policy.total_timeout, 3000);
        assert_eq!(p.base_policy.max_retries, 3);
        assert_eq!(p.base_policy.sleep_between_retries, 2);
        assert_eq!(p.replica, Replica::PreferRack);
        assert!(!p.include_bin_data);
        assert_eq!(p.record_queue_size, 50);
        assert_eq!(p.expected_duration, QueryDuration::Short);
    }

    #[test]
    fn query_partial_merge_preserves_defaults() {
        let default = QueryPolicy::default();
        let cfg: QueryPolicyConfig = parse("socket_timeout: 3\nreplica: PREFER_RACK\n").unwrap();
        let mut p = QueryPolicy::default();
        cfg.merge_into(&mut p);
        assert_eq!(p.base_policy.socket_timeout, 3);
        assert_eq!(p.replica, Replica::PreferRack);
        assert_eq!(p.base_policy.max_retries, default.base_policy.max_retries); // kept (5)
        assert_eq!(p.expected_duration, default.expected_duration); // kept (Long)
        assert_eq!(p.include_bin_data, default.include_bin_data); // kept (true)
    }

    #[test]
    fn batch_full_merge() {
        let cfg: BatchPolicyConfig = parse(
            "socket_timeout: 3\ntotal_timeout: 15\nmax_retries: 5\nsleep_between_retries: 1\n\
             replica: MASTER\nallow_inline: false\nallow_inline_ssd: true\nrespond_all_keys: false\n",
        )
        .unwrap();
        let mut p = BatchPolicy::default();
        cfg.merge_into(&mut p);
        assert_eq!(p.base_policy.socket_timeout, 3);
        assert_eq!(p.base_policy.total_timeout, 15);
        assert_eq!(p.base_policy.max_retries, 5);
        assert_eq!(p.replica, Replica::Master);
        assert!(!p.allow_inline);
        assert!(p.allow_inline_ssd);
        assert!(!p.respond_all_keys);
    }

    #[test]
    fn batch_partial_merge_preserves_defaults() {
        let default = BatchPolicy::default();
        let cfg: BatchPolicyConfig = parse("allow_inline: false\n").unwrap();
        let mut p = BatchPolicy::default();
        cfg.merge_into(&mut p);
        assert!(!p.allow_inline); // overridden
        assert_eq!(p.allow_inline_ssd, default.allow_inline_ssd); // kept
        assert_eq!(p.respond_all_keys, default.respond_all_keys); // kept
        assert_eq!(p.replica, default.replica); // kept
    }

    #[test]
    fn client_dynamic_merge_covers_all_fields() {
        let cfg: ClientPolicyConfig = parse(
            "timeout: 2500\nmax_socket_idle: 55\nmax_error_rate: 42\nerror_rate_window: 3\n\
             tend_interval: 250\nuse_service_alternate: true\napp_id: \"svc\"\nrack_ids: [1, 2, 3]\n",
        )
        .unwrap();
        let mut cp = ClientPolicy::default();
        cfg.merge_into(&mut cp);
        assert_eq!(cp.timeout, 2500);
        assert_eq!(cp.idle_timeout, 55_000); // seconds -> ms
        assert_eq!(cp.max_error_rate, 42);
        assert_eq!(cp.error_rate_window, 3);
        assert_eq!(cp.tend_interval, 250);
        assert!(cp.use_services_alternate);
        assert_eq!(cp.application_id.as_deref(), Some("svc"));
        let racks = cp.rack_ids.expect("rack_ids set");
        assert!(racks.contains(&1) && racks.contains(&2) && racks.contains(&3));
    }

    #[test]
    fn metrics_partial_merge_preserves_latency_base() {
        let default = MetricsPolicy::default();
        let cfg: MetricsPolicyConfig = parse("latency_columns: 9\n").unwrap();
        let mut mp = MetricsPolicy::default();
        cfg.merge_into(&mut mp);
        assert_eq!(mp.latency_columns, 9); // overridden
        assert_eq!(mp.latency_base, default.latency_base); // kept
    }

    #[test]
    fn metrics_latency_base_overrides_directly() {
        // `latency_base` is a direct multiplier (matching the Go client), not a
        // power-of-two exponent — the YAML value lands verbatim on the policy.
        let cfg: MetricsPolicyConfig = parse("latency_base: 8\n").unwrap();
        let mut mp = MetricsPolicy::default();
        cfg.merge_into(&mut mp);
        assert_eq!(mp.latency_base, 8);
    }

    // ---- Per-record batch sub-sections (mirror the batch_*_policy config tests) ----

    #[test]
    fn all_batch_sub_sections_parse() {
        let doc: ConfigDocument = serde_yml::from_str(
            "version: \"1.0.0\"\n\
             dynamic:\n\
             \x20 batch_read:\n    read_mode_ap: ALL\n    socket_timeout: 3\n    replica: MASTER\n\
             \x20 batch_write:\n    socket_timeout: 3\n    durable_delete: true\n    send_key: true\n\
             \x20 batch_delete:\n    durable_delete: true\n    send_key: true\n\
             \x20 batch_udf:\n    durable_delete: true\n    send_key: true\n",
        )
        .unwrap();
        let dynamic = doc.dynamic.unwrap();
        assert!(dynamic.batch_read.is_some());
        assert!(dynamic.batch_write.is_some());
        assert!(dynamic.batch_delete.is_some());
        assert!(dynamic.batch_udf.is_some());
    }

    #[test]
    fn batch_read_section_merges_read_and_batch_flags() {
        // The read fields flatten into a ReadPolicyConfig; the batch wire flags
        // (allow_inline/allow_inline_ssd/respond_all_keys) sit alongside them.
        let cfg: BatchReadSectionConfig = parse(
            "read_mode_ap: ALL\nsocket_timeout: 3\ntotal_timeout: 15\nreplica: MASTER\n\
             allow_inline: false\nrespond_all_keys: false\n",
        )
        .unwrap();
        let mut p = ReadPolicy::default();
        cfg.read.merge_into(&mut p);
        assert_eq!(p.base_policy.read_mode_ap, ReadModeAP::All);
        assert_eq!(p.base_policy.socket_timeout, 3);
        assert_eq!(p.base_policy.total_timeout, 15);
        assert_eq!(p.replica, Replica::Master);
        // Batch wire flags parse into the section (applied to the parent policy).
        assert_eq!(cfg.allow_inline, Some(false));
        assert_eq!(cfg.respond_all_keys, Some(false));
        assert_eq!(cfg.allow_inline_ssd, None);
    }

    #[test]
    fn sleep_multiplier_merges_into_base_policy() {
        // sleep_multiplier is a BasePolicy field, so it flows through every
        // section that flattens the base config (read/write/query/batch/…).
        let cfg: ReadPolicyConfig = parse("sleep_multiplier: 2.5\n").unwrap();
        let mut p = ReadPolicy::default();
        assert!((p.base_policy.sleep_multiplier - 1.0).abs() < f64::EPSILON); // default
        cfg.merge_into(&mut p);
        assert!((p.base_policy.sleep_multiplier - 2.5).abs() < f64::EPSILON);
    }

    #[test]
    fn batch_write_section_merges_like_write() {
        // batch_write reuses WritePolicyConfig (timeouts + send_key + durable_delete).
        let cfg: WritePolicyConfig =
            parse("socket_timeout: 3\ntotal_timeout: 15\ndurable_delete: true\nsend_key: true\n")
                .unwrap();
        let mut p = WritePolicy::default();
        cfg.merge_into(&mut p);
        assert_eq!(p.base_policy.socket_timeout, 3);
        assert_eq!(p.base_policy.total_timeout, 15);
        assert!(p.durable_delete);
        assert!(p.send_key);
    }

    #[test]
    fn batch_delete_section_merges_send_key_and_durable_delete() {
        let default = BatchDeletePolicy::default();
        let cfg: BatchDeletePolicyConfig = parse("durable_delete: true\nsend_key: true\n").unwrap();
        let mut p = BatchDeletePolicy::default();
        cfg.merge_into(&mut p);
        assert!(p.durable_delete);
        assert!(p.send_key);
        // Non-spec fields are untouched.
        assert_eq!(p.commit_level, default.commit_level);

        // Partial: only send_key present.
        let cfg: BatchDeletePolicyConfig = parse("send_key: true\n").unwrap();
        let mut p = BatchDeletePolicy::default();
        cfg.merge_into(&mut p);
        assert!(p.send_key);
        assert_eq!(p.durable_delete, default.durable_delete); // kept
    }

    #[test]
    fn batch_udf_section_merges_send_key_and_durable_delete() {
        let cfg: BatchUDFPolicyConfig = parse("durable_delete: true\nsend_key: true\n").unwrap();
        let mut p = BatchUDFPolicy::default();
        cfg.merge_into(&mut p);
        assert!(p.durable_delete);
        assert!(p.send_key);
    }
}
