// Copyright 2015-2026 Aerospike, Inc.
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

//! The `static.client.discovery` and `static.client.translation` blocks, the
//! `ip_map` and `use_services_alternate` keys next to them, and the registry
//! of named factories a file can select.
//!
//! These keys are read as raw YAML values and validated by hand, so that an
//! invalid setting is reported with the key and value that caused it, as the
//! other Aerospike clients report it. An invalid setting makes the whole file
//! invalid: nothing from the file is applied and the client starts on the
//! configuration set in code.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use indexmap::IndexMap;
use serde::Deserialize;

use crate::discovery::dns::{DnsSeedCandidateProvider, RecordType};
use crate::discovery::{
    AddressTranslator, PassThroughAddressTranslator, PortMapAddressTranslator,
    SeedCandidateProvider, SeedMergePolicy, StaticMapAddressTranslator, TranslationMissMode,
    AWS_TAGS_PROVIDER, BUILT_IN_PROVIDERS, BUILT_IN_TRANSLATORS, DNS_PROVIDER, PASS_THROUGH_TRANSLATOR,
    PORT_MAP_TRANSLATOR, STATIC_MAP_TRANSLATOR, STATIC_PROVIDER,
};
use crate::errors::{Error, Result};
use crate::policy::ClientPolicy;

const DISCOVERY: &str = "static.client.discovery";
const TRANSLATION: &str = "static.client.translation";

/// A value as the YAML loader read it: the options handed to a registered
/// factory.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum ConfigValue {
    /// `~` / `null`.
    Null,
    /// `true` / `false`.
    Bool(bool),
    /// An integer.
    Integer(i64),
    /// A floating-point number.
    Float(f64),
    /// A string.
    String(String),
    /// A sequence.
    List(Vec<ConfigValue>),
    /// A mapping, in file order.
    Map(ConfigMap),
}

/// A YAML mapping, in file order.
pub type ConfigMap = IndexMap<String, ConfigValue>;

impl ConfigValue {
    /// The string, when this is one.
    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            ConfigValue::String(s) => Some(s),
            _ => None,
        }
    }

    /// The integer, when this is one.
    #[must_use]
    pub const fn as_i64(&self) -> Option<i64> {
        match self {
            ConfigValue::Integer(i) => Some(*i),
            _ => None,
        }
    }

    /// The boolean, when this is one.
    #[must_use]
    pub const fn as_bool(&self) -> Option<bool> {
        match self {
            ConfigValue::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// The list, when this is one.
    #[must_use]
    pub fn as_list(&self) -> Option<&[ConfigValue]> {
        match self {
            ConfigValue::List(l) => Some(l),
            _ => None,
        }
    }

    /// The map, when this is one.
    #[must_use]
    pub const fn as_map(&self) -> Option<&ConfigMap> {
        match self {
            ConfigValue::Map(m) => Some(m),
            _ => None,
        }
    }
}

impl fmt::Display for ConfigValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigValue::Null => f.write_str("null"),
            ConfigValue::Bool(b) => write!(f, "{b}"),
            ConfigValue::Integer(i) => write!(f, "{i}"),
            ConfigValue::Float(x) => write!(f, "{x}"),
            ConfigValue::String(s) => f.write_str(s),
            ConfigValue::List(l) => {
                f.write_str("[")?;
                for (i, v) in l.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{v}")?;
                }
                f.write_str("]")
            }
            ConfigValue::Map(m) => {
                f.write_str("{")?;
                for (i, (k, v)) in m.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{k}={v}")?;
                }
                f.write_str("}")
            }
        }
    }
}

/// Builds a seed candidate provider from the keys of the
/// `static.client.discovery` block other than `provider`.
pub type SeedCandidateProviderFactory =
    Arc<dyn Fn(&ConfigMap) -> Result<Arc<dyn SeedCandidateProvider>> + Send + Sync>;
/// Builds an address translator from the keys of the
/// `static.client.translation` block other than `provider`.
pub type AddressTranslatorFactory =
    Arc<dyn Fn(&ConfigMap) -> Result<Arc<dyn AddressTranslator>> + Send + Sync>;

/// The named factories a YAML file can select.
///
/// `static.client.discovery.provider` and
/// `static.client.translation.provider` name them. Lives on the
/// [`ClientPolicy`]; see
/// [`ClientPolicy::register_seed_candidate_provider`].
#[derive(Clone, Default)]
pub struct DiscoveryRegistry {
    providers: Vec<(String, SeedCandidateProviderFactory)>,
    translators: Vec<(String, AddressTranslatorFactory)>,
}

impl fmt::Debug for DiscoveryRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DiscoveryRegistry")
            .field("providers", &self.provider_names())
            .field("translators", &self.translator_names())
            .finish()
    }
}

/// Two registries are equal when they hold the same factories under the
/// same names.
impl PartialEq for DiscoveryRegistry {
    fn eq(&self, other: &Self) -> bool {
        self.providers.len() == other.providers.len()
            && self.translators.len() == other.translators.len()
            && self
                .providers
                .iter()
                .zip(&other.providers)
                .all(|((a, fa), (b, fb))| a == b && Arc::ptr_eq(fa, fb))
            && self
                .translators
                .iter()
                .zip(&other.translators)
                .all(|((a, fa), (b, fb))| a == b && Arc::ptr_eq(fa, fb))
    }
}

impl DiscoveryRegistry {
    /// The registered provider names, in registration order.
    #[must_use]
    pub fn provider_names(&self) -> Vec<&str> {
        self.providers.iter().map(|(n, _)| n.as_str()).collect()
    }

    /// The registered translator names, in registration order.
    #[must_use]
    pub fn translator_names(&self) -> Vec<&str> {
        self.translators.iter().map(|(n, _)| n.as_str()).collect()
    }

    pub(crate) fn register_provider(
        &mut self,
        name: String,
        factory: SeedCandidateProviderFactory,
    ) -> Result<()> {
        check_name("seed candidate provider", &name, &BUILT_IN_PROVIDERS, &self.provider_names())?;
        self.providers.push((name, factory));
        Ok(())
    }

    pub(crate) fn register_translator(
        &mut self,
        name: String,
        factory: AddressTranslatorFactory,
    ) -> Result<()> {
        check_name("address translator", &name, &BUILT_IN_TRANSLATORS, &self.translator_names())?;
        self.translators.push((name, factory));
        Ok(())
    }

    fn provider(&self, name: &str) -> Option<&SeedCandidateProviderFactory> {
        self.providers.iter().find(|(n, _)| n == name).map(|(_, f)| f)
    }

    fn translator(&self, name: &str) -> Option<&AddressTranslatorFactory> {
        self.translators.iter().find(|(n, _)| n == name).map(|(_, f)| f)
    }
}

fn check_name(kind: &str, name: &str, built_in: &[&str], registered: &[&str]) -> Result<()> {
    if name.is_empty() {
        return Err(Error::invalid_argument(format!("Invalid {kind} name: ''")));
    }
    if built_in.contains(&name) {
        return Err(Error::invalid_argument(format!(
            "Invalid {kind} name '{name}': built-in name"
        )));
    }
    if registered.contains(&name) {
        return Err(Error::invalid_argument(format!(
            "Invalid {kind} name '{name}': already registered"
        )));
    }
    Ok(())
}

/// The `static.client` keys that are not plain policy fields, as raw values.
///
/// They are `ip_map`, `use_services_alternate`, `discovery` and
/// `translation`. [`resolve`](Self::resolve) validates them and
/// [`Resolved::apply`] copies them into a [`ClientPolicy`].
#[derive(Debug, Default, Clone, PartialEq, Deserialize)]
pub struct ClientDiscoveryConfig {
    /// `static.client.ip_map`.
    pub ip_map: Option<ConfigValue>,
    /// `static.client.use_services_alternate`.
    pub use_services_alternate: Option<ConfigValue>,
    /// `static.client.discovery`.
    pub discovery: Option<ConfigValue>,
    /// `static.client.translation`.
    pub translation: Option<ConfigValue>,
}

/// What a file says about an object-valued setting.
#[derive(Debug, Default)]
enum Setting<T> {
    /// The key is absent: the value set in code is kept.
    #[default]
    Unchanged,
    /// The key names the default: the value set in code is dropped.
    Clear,
    /// The key builds this value.
    Set(T),
}

impl<T> Setting<T> {
    fn apply(self, target: &mut Option<T>) {
        match self {
            Setting::Unchanged => {}
            Setting::Clear => *target = None,
            Setting::Set(value) => *target = Some(value),
        }
    }
}

/// What a validated [`ClientDiscoveryConfig`] sets on the policy.
///
/// Built before anything is applied, so an invalid setting leaves the policy
/// untouched.
#[derive(Debug, Default)]
pub struct Resolved {
    ip_map: Option<HashMap<String, String>>,
    use_services_alternate: Option<bool>,
    /// `Clear` resets the provider to the default (`provider: static`).
    provider: Setting<Arc<dyn SeedCandidateProvider>>,
    merge_policy: Option<SeedMergePolicy>,
    refresh_interval: Option<u32>,
    tend_refresh_deadline: Option<u32>,
    /// `Clear` resets the translator to the default (`static-map` without a
    /// `static_map`).
    translator: Setting<Arc<dyn AddressTranslator>>,
    on_miss: Option<TranslationMissMode>,
    /// Log lines describing each setting applied, for the info log.
    applied: Vec<String>,
}

impl Resolved {
    /// Copies the settings into `policy`.
    pub fn apply(self, policy: &mut ClientPolicy) {
        if let Some(map) = self.ip_map {
            policy.ip_map = Some(map);
        }
        if let Some(alt) = self.use_services_alternate {
            policy.use_services_alternate = alt;
        }
        self.provider.apply(&mut policy.seed_candidate_provider);
        if let Some(merge_policy) = self.merge_policy {
            policy.discovery_merge_policy = merge_policy;
        }
        if let Some(interval) = self.refresh_interval {
            policy.discovery_refresh_interval = interval;
        }
        if let Some(deadline) = self.tend_refresh_deadline {
            policy.discovery_tend_refresh_deadline = deadline;
        }
        self.translator.apply(&mut policy.address_translator);
        if let Some(on_miss) = self.on_miss {
            policy.translation_on_miss = on_miss;
        }
        for line in self.applied {
            info!("Set ClientPolicy.{line}");
        }
    }

    /// Whether anything would be set.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.applied.is_empty()
    }
}

impl ClientDiscoveryConfig {
    /// Validates every key against `policy` (its registrations) and builds
    /// the providers and translators the file selects. With `build` false the
    /// factories and built-in providers are not constructed, only checked:
    /// for a reload, which keeps the instances the client was constructed
    /// with.
    ///
    /// # Errors
    ///
    /// An invalid setting, with a message naming the key or value.
    pub fn resolve(&self, policy: &ClientPolicy, build: bool) -> Result<Resolved> {
        let mut out = Resolved::default();

        if let Some(value) = &self.ip_map {
            let map = string_map("static.client.ip_map", value)?;
            out.applied.push(format!("ip_map = {map:?}"));
            out.ip_map = Some(map);
        }
        if let Some(value) = &self.use_services_alternate {
            let alt = value
                .as_bool()
                .ok_or_else(|| invalid("static.client.use_services_alternate", value, "expected true or false"))?;
            out.applied.push(format!("use_services_alternate = {alt}"));
            out.use_services_alternate = Some(alt);
        }
        if let Some(value) = &self.discovery {
            self.resolve_discovery(value, policy, build, &mut out)?;
        }
        if let Some(value) = &self.translation {
            self.resolve_translation(value, policy, build, &mut out)?;
        }
        if out.ip_map.is_some() && self.static_map_set() {
            return Err(Error::invalid_argument(
                "static.client.translation.static_map and static.client.ip_map are both set. \
                 Set one of them",
            ));
        }
        Ok(out)
    }

    fn static_map_set(&self) -> bool {
        self.translation
            .as_ref()
            .and_then(ConfigValue::as_map)
            .is_some_and(|m| m.contains_key("static_map"))
    }

    fn resolve_discovery(
        &self,
        value: &ConfigValue,
        policy: &ClientPolicy,
        build: bool,
        out: &mut Resolved,
    ) -> Result<()> {
        let map = block(DISCOVERY, value)?;
        for key in ["execution_mode", "refresh_timeout_ms"] {
            if map.contains_key(key) {
                return Err(Error::invalid_argument(format!(
                    "{DISCOVERY}.{key} is not supported: the Rust client runs discovery on the \
                     tend task only"
                )));
            }
        }

        let registry = &policy.discovery_registry;
        let provider_name = match map.get("provider") {
            None => None,
            Some(ConfigValue::String(name)) if BUILT_IN_PROVIDERS.contains(&name.as_str()) => {
                Some(name.clone())
            }
            Some(ConfigValue::String(name)) if registry.provider(name).is_some() => Some(name.clone()),
            Some(other) => {
                let mut names: Vec<&str> = BUILT_IN_PROVIDERS.to_vec();
                names.extend(registry.provider_names());
                return Err(invalid(
                    &format!("{DISCOVERY}.provider"),
                    other,
                    &format!("valid values: {}", names.join(", ")),
                ));
            }
        };

        let dns = map.get("dns");
        if provider_name.as_deref() == Some(DNS_PROVIDER) {
            if dns.is_none() {
                return Err(Error::invalid_argument(format!(
                    "{DISCOVERY}.dns is required with provider {DNS_PROVIDER}"
                )));
            }
        } else if dns.is_some() {
            return Err(Error::invalid_argument(format!(
                "{DISCOVERY}.dns is only valid with provider {DNS_PROVIDER}"
            )));
        }
        let aws = map.get("aws");
        if provider_name.as_deref() == Some(AWS_TAGS_PROVIDER) {
            if aws.is_none() {
                return Err(Error::invalid_argument(format!(
                    "{DISCOVERY}.aws is required with provider {AWS_TAGS_PROVIDER}"
                )));
            }
        } else if aws.is_some() {
            return Err(Error::invalid_argument(format!(
                "{DISCOVERY}.aws is only valid with provider {AWS_TAGS_PROVIDER}"
            )));
        }

        if let Some(name) = &provider_name {
            let provider: Option<Arc<dyn SeedCandidateProvider>> = match name.as_str() {
                STATIC_PROVIDER => None,
                DNS_PROVIDER => {
                    let builder = dns_builder(dns.expect("checked above"))?;
                    if build {
                        Some(Arc::new(builder.build().map_err(|e| {
                            Error::invalid_argument(format!("Invalid {DISCOVERY}.dns: {}", e.base_message()))
                        })?))
                    } else {
                        None
                    }
                }
                AWS_TAGS_PROVIDER => aws_provider(aws.expect("checked above"), build)?,
                registered => {
                    let factory = registry.provider(registered).expect("checked above");
                    if build {
                        Some(call_factory(&format!("{DISCOVERY}.provider"), registered, &options(map), |o| {
                            factory(o)
                        })?)
                    } else {
                        None
                    }
                }
            };
            if build || name == STATIC_PROVIDER {
                out.provider = provider.map_or(Setting::Clear, Setting::Set);
            }
            out.applied.push(format!("seed_candidate_provider = {name}"));
        }

        if let Some(value) = map.get("merge_policy") {
            let name = enum_value(&format!("{DISCOVERY}.merge_policy"), value, &SeedMergePolicy::VALUES)?;
            let merge_policy: SeedMergePolicy = name.parse()?;
            out.applied.push(format!("discovery_merge_policy = {merge_policy}"));
            out.merge_policy = Some(merge_policy);
        }
        if let Some(value) = map.get("refresh_interval_ms") {
            let interval = int_value(&format!("{DISCOVERY}.refresh_interval_ms"), value, 0, i64::from(u32::MAX))?;
            out.applied.push(format!("discovery_refresh_interval = {interval}"));
            out.refresh_interval = Some(interval);
        }
        if let Some(value) = map.get("tend_refresh_deadline_ms") {
            let deadline = int_value(&format!("{DISCOVERY}.tend_refresh_deadline_ms"), value, 0, i64::from(u32::MAX))?;
            out.applied.push(format!("discovery_tend_refresh_deadline = {deadline}"));
            out.tend_refresh_deadline = Some(deadline);
        }
        Ok(())
    }

    fn resolve_translation(
        &self,
        value: &ConfigValue,
        policy: &ClientPolicy,
        build: bool,
        out: &mut Resolved,
    ) -> Result<()> {
        let map = block(TRANSLATION, value)?;
        if let Some(value) = map.get("on_miss") {
            let name = enum_value(&format!("{TRANSLATION}.on_miss"), value, &TranslationMissMode::VALUES)?;
            let on_miss: TranslationMissMode = name.parse()?;
            out.applied.push(format!("translation_on_miss = {on_miss}"));
            out.on_miss = Some(on_miss);
        }
        let static_map = match map.get("static_map") {
            Some(value) => Some(string_map(&format!("{TRANSLATION}.static_map"), value)?),
            None => None,
        };
        let port_map = match map.get("port_map") {
            Some(value) => Some(string_map(&format!("{TRANSLATION}.port_map"), value)?),
            None => None,
        };

        let registry = &policy.discovery_registry;
        let provider = map.get("provider");
        let name = match provider {
            None => STATIC_MAP_TRANSLATOR.to_string(),
            Some(ConfigValue::String(name)) if BUILT_IN_TRANSLATORS.contains(&name.as_str()) => {
                name.clone()
            }
            Some(ConfigValue::String(name)) if registry.translator(name).is_some() => name.clone(),
            Some(other) => {
                let mut names: Vec<&str> = BUILT_IN_TRANSLATORS.to_vec();
                names.extend(registry.translator_names());
                return Err(invalid(
                    &format!("{TRANSLATION}.provider"),
                    other,
                    &format!("valid values: {}", names.join(", ")),
                ));
            }
        };
        if static_map.is_some() && name != STATIC_MAP_TRANSLATOR {
            return Err(Error::invalid_argument(format!(
                "{TRANSLATION}.static_map is only valid with provider static-map, not '{name}'"
            )));
        }
        if port_map.is_some() && name != PORT_MAP_TRANSLATOR {
            return Err(Error::invalid_argument(format!(
                "{TRANSLATION}.port_map is only valid with provider port-map, not '{name}'"
            )));
        }
        if name == PORT_MAP_TRANSLATOR && port_map.is_none() {
            return Err(Error::invalid_argument(format!(
                "{TRANSLATION}.port_map is required with provider port-map"
            )));
        }
        if provider.is_none() && static_map.is_none() {
            // Neither `provider` nor `static_map`: the translator set in code is kept.
            return Ok(());
        }

        let translator: Option<Arc<dyn AddressTranslator>> = match name.as_str() {
            PORT_MAP_TRANSLATOR => {
                let translator = PortMapAddressTranslator::new(port_map.expect("checked above"))
                    .map_err(|e| Error::invalid_argument(format!("{TRANSLATION}.port_map: {}", e.base_message())))?;
                Some(Arc::new(translator))
            }
            PASS_THROUGH_TRANSLATOR => Some(Arc::new(PassThroughAddressTranslator)),
            STATIC_MAP_TRANSLATOR => static_map
                .map(|m| Arc::new(StaticMapAddressTranslator::new(Some(m))) as Arc<dyn AddressTranslator>),
            registered => {
                let factory = registry.translator(registered).expect("checked above");
                if build {
                    Some(call_factory(&format!("{TRANSLATION}.provider"), registered, &options(map), |o| {
                        factory(o)
                    })?)
                } else {
                    None
                }
            }
        };
        if build || BUILT_IN_TRANSLATORS.contains(&name.as_str()) {
            out.translator = translator.map_or(Setting::Clear, Setting::Set);
        }
        out.applied.push(format!("address_translator = {name}"));
        Ok(())
    }
}

/// Calls a registered factory, turning its failure into an invalid setting.
fn call_factory<T>(
    path: &str,
    name: &str,
    options: &ConfigMap,
    factory: impl FnOnce(&ConfigMap) -> Result<T>,
) -> Result<T> {
    factory(options).map_err(|e| {
        Error::invalid_argument(format!("Invalid {path} value '{name}': factory failed: {}", e.base_message()))
    })
}

/// Every key of `map` except `provider`.
fn options(map: &ConfigMap) -> ConfigMap {
    map.iter()
        .filter(|(k, _)| k.as_str() != "provider")
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

fn dns_builder(value: &ConfigValue) -> Result<crate::discovery::dns::Builder> {
    let path = format!("{DISCOVERY}.dns");
    let map = block(&path, value)?;
    let names = map
        .get("names")
        .ok_or_else(|| Error::invalid_argument(format!("{path}.names is required")))?;
    let mut builder = DnsSeedCandidateProvider::builder().names(string_list(&format!("{path}.names"), names)?);
    if let Some(value) = map.get("record_type") {
        let name = enum_value(&format!("{path}.record_type"), value, &RecordType::VALUES)?;
        builder = builder.record_type(name.parse()?);
    }
    if let Some(value) = map.get("port") {
        builder = builder.port(int_value(&format!("{path}.port"), value, 1, 65535)? as u16);
    }
    if let Some(value) = map.get("tls_name") {
        builder = builder.tls_name(string_value(&format!("{path}.tls_name"), value)?);
    }
    if let Some(value) = map.get("timeout_ms") {
        builder = builder.timeout(int_value(&format!("{path}.timeout_ms"), value, 1, i64::from(u32::MAX))?);
    }
    if let Some(value) = map.get("servers") {
        builder = builder.servers(string_list(&format!("{path}.servers"), value)?);
    }
    Ok(builder)
}

#[cfg(feature = "discovery-aws")]
fn aws_provider(value: &ConfigValue, build: bool) -> Result<Option<Arc<dyn SeedCandidateProvider>>> {
    use crate::discovery::aws::{AddressField, AwsEc2TagSeedCandidateProvider};

    let path = format!("{DISCOVERY}.aws");
    let map = block(&path, value)?;
    let region = map
        .get("region")
        .ok_or_else(|| Error::invalid_argument(format!("{path}.region is required")))?;
    let mut builder =
        AwsEc2TagSeedCandidateProvider::builder().region(string_value(&format!("{path}.region"), region)?);
    let filters = map
        .get("filters")
        .ok_or_else(|| Error::invalid_argument(format!("{path}.filters is required")))?;
    let list = match filters.as_list() {
        Some(list) if !list.is_empty() => list,
        _ => {
            return Err(invalid(
                &format!("{path}.filters"),
                filters,
                "expected a non-empty list of name and values",
            ))
        }
    };
    for (i, entry) in list.iter().enumerate() {
        let filter = format!("{path}.filters[{i}]");
        let entry = block(&filter, entry)?;
        let name = entry
            .get("name")
            .ok_or_else(|| Error::invalid_argument(format!("{filter}.name is required")))?;
        let values = entry
            .get("values")
            .ok_or_else(|| Error::invalid_argument(format!("{filter}.values is required")))?;
        builder = builder.filter(
            string_value(&format!("{filter}.name"), name)?,
            string_list(&format!("{filter}.values"), values)?,
        );
    }
    if let Some(value) = map.get("address_field") {
        let name = enum_value(&format!("{path}.address_field"), value, &AddressField::VALUES)?;
        builder = builder.address_field(name.parse()?);
    }
    if let Some(value) = map.get("port") {
        builder = builder.port(int_value(&format!("{path}.port"), value, 1, 65535)? as u16);
    }
    if let Some(value) = map.get("tls_name") {
        builder = builder.tls_name(string_value(&format!("{path}.tls_name"), value)?);
    }
    if let Some(value) = map.get("timeout_ms") {
        builder = builder.timeout(int_value(&format!("{path}.timeout_ms"), value, 1, i64::from(u32::MAX))?);
    }
    if let Some(value) = map.get("init_timeout_ms") {
        builder = builder.init_timeout(int_value(&format!("{path}.init_timeout_ms"), value, 1, i64::from(u32::MAX))?);
    }
    if let Some(value) = map.get("endpoint") {
        builder = builder.endpoint(string_value(&format!("{path}.endpoint"), value)?);
    }
    if !build {
        return Ok(None);
    }
    let provider = builder
        .build()
        .map_err(|e| Error::invalid_argument(format!("Invalid {path}: {}", e.base_message())))?;
    Ok(Some(Arc::new(provider)))
}

#[cfg(not(feature = "discovery-aws"))]
fn aws_provider(value: &ConfigValue, _build: bool) -> Result<Option<Arc<dyn SeedCandidateProvider>>> {
    block(&format!("{DISCOVERY}.aws"), value)?;
    Err(Error::invalid_argument(format!(
        "{DISCOVERY}.provider: aws-tags requires the discovery-aws cargo feature"
    )))
}

fn invalid(path: &str, value: &ConfigValue, reason: &str) -> Error {
    Error::invalid_argument(format!("Invalid {path} value '{value}': {reason}"))
}

fn block<'a>(path: &str, value: &'a ConfigValue) -> Result<&'a ConfigMap> {
    value.as_map().ok_or_else(|| invalid(path, value, "expected a map"))
}

fn string_map(path: &str, value: &ConfigValue) -> Result<HashMap<String, String>> {
    let mut map = HashMap::new();
    for (k, v) in block(path, value)? {
        let s = v
            .as_str()
            .ok_or_else(|| invalid(&format!("{path}.{k}"), v, "expected a string"))?;
        map.insert(k.clone(), s.to_string());
    }
    Ok(map)
}

fn string_list(path: &str, value: &ConfigValue) -> Result<Vec<String>> {
    let reason = "expected a non-empty list of strings";
    let list = match value.as_list() {
        Some(list) if !list.is_empty() => list,
        _ => return Err(invalid(path, value, reason)),
    };
    list.iter()
        .map(|item| {
            item.as_str()
                .map(str::to_string)
                .ok_or_else(|| invalid(path, value, reason))
        })
        .collect()
}

fn string_value(path: &str, value: &ConfigValue) -> Result<String> {
    value
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| invalid(path, value, "expected a string"))
}

fn int_value(path: &str, value: &ConfigValue, min: i64, max: i64) -> Result<u32> {
    let i = value
        .as_i64()
        .ok_or_else(|| invalid(path, value, "expected an integer"))?;
    if i < min || i > max {
        return Err(invalid(
            path,
            value,
            &format!("expected an integer from {min} to {max}"),
        ));
    }
    Ok(i as u32)
}

fn enum_value<'a>(path: &str, value: &'a ConfigValue, values: &[&str]) -> Result<&'a str> {
    match value.as_str() {
        Some(s) if values.contains(&s) => Ok(s),
        _ => Err(invalid(path, value, &format!("valid values: {}", values.join(", ")))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use crate::config::ConfigDocument;
    use crate::discovery::{Endpoint, SourceList, StaticSeedCandidateProvider};
    use crate::Host;

    fn doc(yaml: &str) -> ConfigDocument {
        serde_norway::from_str(yaml).expect("yaml parses")
    }

    fn client_block(yaml: &str) -> ClientDiscoveryConfig {
        doc(yaml)
            .static_config
            .expect("static section")
            .client_discovery
            .expect("client block")
    }

    fn resolve(yaml: &str) -> Result<ClientPolicy> {
        resolve_with(yaml, ClientPolicy::default())
    }

    fn resolve_with(yaml: &str, mut policy: ClientPolicy) -> Result<ClientPolicy> {
        client_block(yaml).resolve(&policy, true)?.apply(&mut policy);
        Ok(policy)
    }

    fn translate(policy: &ClientPolicy, node: &str, host: &str, port: u16) -> Option<Host> {
        policy
            .address_translator
            .as_ref()
            .expect("translator set")
            .translate(&Endpoint::new(node, None, host, port, SourceList::Standard))
    }

    const ALL: &str = "
version: 1.2.0
static:
  client:
    max_connections_per_node: 99
    ip_map:
      10.0.0.1: 192.168.1.1
      10.0.0.2: 192.168.1.2
    use_services_alternate: true
    discovery:
      provider: static
      merge_policy: replace
      refresh_interval_ms: 60000
      tend_refresh_deadline_ms: 200
    translation:
      provider: static-map
      on_miss: strict
";

    #[test]
    fn every_key_sets_its_field_and_the_policy_keys_still_parse() {
        let document = doc(ALL);
        let static_config = document.static_config.unwrap();
        let mut policy = ClientPolicy::default();
        static_config.client.unwrap().merge_static_into(&mut policy);
        assert_eq!(policy.max_conns_per_node, 99);

        let mut policy = ClientPolicy {
            seed_candidate_provider: Some(Arc::new(StaticSeedCandidateProvider::new(vec![]))),
            ..ClientPolicy::default()
        };
        static_config.client_discovery.unwrap().resolve(&policy, true).unwrap().apply(&mut policy);
        assert_eq!(policy.ip_map.as_ref().unwrap().get("10.0.0.1").map(String::as_str), Some("192.168.1.1"));
        assert!(policy.use_services_alternate);
        assert!(policy.seed_candidate_provider.is_none(), "provider: static resets the provider");
        assert_eq!(policy.discovery_merge_policy, SeedMergePolicy::Replace);
        assert_eq!(policy.discovery_refresh_interval, 60_000);
        assert_eq!(policy.discovery_tend_refresh_deadline, 200);
        assert_eq!(policy.translation_on_miss, TranslationMissMode::Strict);
        // static-map without static_map: the default translator over ip_map.
        assert!(policy.address_translator.is_none());
    }

    #[test]
    fn static_map_builds_a_static_map_translator_even_without_provider() {
        let policy = resolve("static:\n  client:\n    translation:\n      static_map:\n        10.0.0.1: 192.168.1.1\n").unwrap();
        assert_eq!(translate(&policy, "n", "10.0.0.1", 3000), Some(Host::new("192.168.1.1", 3000)));
        assert_eq!(translate(&policy, "n", "10.0.0.9", 3000), None);
        assert_eq!(policy.address_translator.as_ref().unwrap().name(), "static-map");
    }

    #[test]
    fn port_map_builds_a_port_map_translator() {
        let policy = resolve(
            "static:\n  client:\n    translation:\n      provider: port-map\n      port_map:\n        \"10.0.0.1:3000\": \"lb.example.com:30001\"\n        \"[2001:db8::1]:3000\": \"lb.example.com:30002\"\n        BB9020011AC4202: \"lb.example.com:30003\"\n",
        )
        .unwrap();
        assert_eq!(translate(&policy, "n", "10.0.0.1", 3000), Some(Host::new("lb.example.com", 30001)));
        assert_eq!(translate(&policy, "n", "2001:db8::1", 3000), Some(Host::new("lb.example.com", 30002)));
        assert_eq!(translate(&policy, "BB9020011AC4202", "10.0.0.7", 3000), Some(Host::new("lb.example.com", 30003)));
    }

    #[test]
    fn pass_through_and_on_miss_only() {
        let policy = resolve(
            "static:\n  client:\n    use_services_alternate: true\n    discovery:\n      merge_policy: discovery_only\n    translation:\n      provider: pass-through\n      on_miss: pass_through\n",
        )
        .unwrap();
        assert_eq!(policy.address_translator.as_ref().unwrap().name(), "pass-through");
        assert_eq!(policy.discovery_merge_policy, SeedMergePolicy::DiscoveryOnly);
        assert!(policy.use_services_alternate);

        let code_translator: Arc<dyn AddressTranslator> = Arc::new(PassThroughAddressTranslator);
        let policy = resolve_with(
            "static:\n  client:\n    translation:\n      on_miss: strict\n",
            ClientPolicy {
                address_translator: Some(code_translator.clone()),
                ..ClientPolicy::default()
            },
        )
        .unwrap();
        assert_eq!(policy.translation_on_miss, TranslationMissMode::Strict);
        assert!(policy.address_translator == Some(code_translator), "a block without provider keeps the code translator");
    }

    #[test]
    fn defaults_apply_when_blocks_are_absent_and_unknown_keys_are_ignored() {
        let policy = resolve(
            "static:\n  unknown_static_block:\n    foo: bar\n  client:\n    max_connections_per_node: 99\n    unknown_client_key: 5\n    discovery:\n      merge_policy: replace\n      refresh_intervl_ms: 1\n      provider_class: com.example.MyProvider\n    translation:\n      provider: pass-through\n      onmiss: strict\n      provider_class: com.example.MyTranslator\n",
        )
        .unwrap();
        assert_eq!(policy.discovery_merge_policy, SeedMergePolicy::Replace);
        assert_eq!(policy.discovery_refresh_interval, 30_000);
        assert_eq!(policy.translation_on_miss, TranslationMissMode::PassThrough);
        assert_eq!(policy.address_translator.as_ref().unwrap().name(), "pass-through");

        let policy = resolve("static:\n  client:\n    max_connections_per_node: 1\n").unwrap();
        assert_eq!(policy, ClientPolicy::default());
        assert!(doc("version: 1.2.0\n").static_config.is_none());
    }

    #[test]
    fn dns_block_builds_the_dns_provider() {
        let policy = resolve(
            "static:\n  client:\n    discovery:\n      provider: dns\n      merge_policy: replace\n      dns:\n        names:\n          - seeds-a.example.com\n          - seeds-b.example.com.\n        record_type: srv\n        port: 3100\n        tls_name: tls-seed\n        timeout_ms: 50\n        servers:\n          - 127.0.0.1:5353\n          - \"[::1]:5353\"\n        unknown_key: ignored\n",
        )
        .unwrap();
        let provider = policy.seed_candidate_provider.unwrap();
        assert_eq!(provider.name(), "dns");
        assert_eq!(provider.refresh_timeout(), Some(std::time::Duration::from_millis(50)));
        assert_eq!(policy.discovery_merge_policy, SeedMergePolicy::Replace);
    }

    #[cfg(feature = "discovery-aws")]
    #[test]
    fn aws_block_builds_the_aws_provider() {
        let policy = resolve(
            "static:\n  client:\n    discovery:\n      provider: aws-tags\n      merge_policy: merge\n      tend_refresh_deadline_ms: 100\n      aws:\n        region: eu-west-1\n        filters:\n          - name: tag:aerospike-cluster\n            values:\n              - prod-a\n              - prod-b\n          - name: subnet-id\n            values:\n              - subnet-0123\n        address_field: public_dns\n        port: 3100\n        tls_name: tls-seed\n        timeout_ms: 60\n        init_timeout_ms: 5000\n        endpoint: https://vpce-0123.ec2.eu-west-1.vpce.amazonaws.com\n        access_key_id: ignored-unknown-key\n",
        )
        .unwrap();
        let provider = policy.seed_candidate_provider.unwrap();
        assert_eq!(provider.name(), "aws-tags");
        assert_eq!(provider.refresh_timeout(), Some(std::time::Duration::from_millis(60)));
    }

    #[test]
    fn registered_provider_and_translator_are_built_from_yaml_with_options() {
        let seen: Arc<Mutex<Vec<ConfigMap>>> = Arc::new(Mutex::new(Vec::new()));
        let mut policy = ClientPolicy::default();
        let s1 = seen.clone();
        policy
            .register_seed_candidate_provider(
                "consul",
                Arc::new(move |options| {
                    s1.lock().unwrap().push(options.clone());
                    Ok(Arc::new(StaticSeedCandidateProvider::new(vec![Host::new("127.0.0.1", 1)])))
                }),
            )
            .unwrap();
        let s2 = seen.clone();
        policy
            .register_address_translator(
                "nat-table",
                Arc::new(move |options| {
                    s2.lock().unwrap().push(options.clone());
                    Ok(Arc::new(PassThroughAddressTranslator))
                }),
            )
            .unwrap();
        let policy = resolve_with(
            "static:\n  client:\n    max_connections_per_node: 99\n    discovery:\n      provider: consul\n      merge_policy: merge\n      service: aerospike\n      endpoints:\n        - 127.0.0.1:1\n    translation:\n      provider: nat-table\n      on_miss: strict\n      table:\n        \"10.0.0.1:3000\": \"127.0.0.1:1\"\n",
            policy,
        )
        .unwrap();
        assert_eq!(policy.seed_candidate_provider.unwrap().name(), "static");
        assert_eq!(policy.address_translator.unwrap().name(), "pass-through");
        assert_eq!(policy.translation_on_miss, TranslationMissMode::Strict);
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].get("merge_policy"), Some(&ConfigValue::String("merge".into())));
        assert_eq!(seen[0].get("service"), Some(&ConfigValue::String("aerospike".into())));
        assert_eq!(seen[0].get("endpoints"), Some(&ConfigValue::List(vec![ConfigValue::String("127.0.0.1:1".into())])));
        assert!(!seen[0].contains_key("provider"));
        assert_eq!(seen[1].get("on_miss"), Some(&ConfigValue::String("strict".into())));
        assert!(matches!(seen[1].get("table"), Some(ConfigValue::Map(m)) if m.len() == 1));

        drop(seen);

        // A block naming only the provider gives an empty options map.
        let only: Arc<Mutex<Option<ConfigMap>>> = Arc::new(Mutex::new(None));
        let o = only.clone();
        let mut policy = ClientPolicy::default();
        policy
            .register_seed_candidate_provider(
                "consul",
                Arc::new(move |options| {
                    *o.lock().unwrap() = Some(options.clone());
                    Ok(Arc::new(StaticSeedCandidateProvider::new(vec![])))
                }),
            )
            .unwrap();
        resolve_with("static:\n  client:\n    discovery:\n      provider: consul\n", policy).unwrap();
        assert_eq!(only.lock().unwrap().as_ref().map(ConfigMap::len), Some(0));
    }

    #[test]
    fn registration_rejects_built_in_duplicate_and_empty_names_and_clone_copies_them() {
        let mut policy = ClientPolicy::default();
        let factory: SeedCandidateProviderFactory =
            Arc::new(|_| Ok(Arc::new(StaticSeedCandidateProvider::new(vec![]))));
        let err = policy.register_seed_candidate_provider("dns", factory.clone()).unwrap_err();
        assert_eq!(err.base_message(), "Invalid argument: Invalid seed candidate provider name 'dns': built-in name");
        assert!(policy.register_seed_candidate_provider("", factory.clone()).unwrap_err().to_string().contains("name: ''"));
        policy.register_seed_candidate_provider("consul", factory.clone()).unwrap();
        let err = policy.register_seed_candidate_provider("consul", factory).unwrap_err();
        assert!(err.to_string().contains("'consul': already registered"), "{err}");

        let translator: AddressTranslatorFactory = Arc::new(|_| Ok(Arc::new(PassThroughAddressTranslator)));
        let err = policy.register_address_translator("port-map", translator.clone()).unwrap_err();
        assert!(err.to_string().contains("'port-map': built-in name"), "{err}");
        policy.register_address_translator("nat", translator).unwrap();

        let copy = policy.clone();
        assert_eq!(copy.discovery_registry.provider_names(), vec!["consul"]);
        assert_eq!(copy.discovery_registry.translator_names(), vec!["nat"]);
        assert_eq!(copy, policy);
    }

    #[test]
    fn unregistered_name_and_factory_failure_are_invalid_settings() {
        let mut policy = ClientPolicy::default();
        policy
            .register_address_translator("nat-table", Arc::new(|_| Err(Error::client_error("boom"))))
            .unwrap();
        let err = client_block("static:\n  client:\n    translation:\n      provider: nat\n")
            .resolve(&policy, true)
            .unwrap_err();
        assert_eq!(
            err.base_message(),
            "Invalid argument: Invalid static.client.translation.provider value 'nat': valid values: static-map, port-map, pass-through, nat-table"
        );
        let err = client_block("static:\n  client:\n    translation:\n      provider: nat-table\n")
            .resolve(&policy, true)
            .unwrap_err();
        assert!(err.to_string().contains("value 'nat-table': factory failed: "), "{err}");
        let err = client_block("static:\n  client:\n    discovery:\n      provider: consul\n")
            .resolve(&policy, true)
            .unwrap_err();
        assert_eq!(
            err.base_message(),
            "Invalid argument: Invalid static.client.discovery.provider value 'consul': valid values: static, dns, aws-tags"
        );
        // A reload (build = false) checks the name but does not call the factory.
        client_block("static:\n  client:\n    translation:\n      provider: nat-table\n")
            .resolve(&policy, false)
            .unwrap();
    }

    #[test]
    fn invalid_settings_name_the_key_or_value() {
        let cases: &[(&str, &str)] = &[
            ("discovery:\n      execution_mode: background\n", "static.client.discovery.execution_mode is not supported"),
            ("discovery:\n      refresh_timeout_ms: 5\n", "static.client.discovery.refresh_timeout_ms is not supported"),
            ("discovery:\n      provider: consul\n", "Invalid static.client.discovery.provider value 'consul'"),
            ("discovery:\n      provider: dns\n", "static.client.discovery.dns is required with provider dns"),
            ("discovery:\n      provider: static\n      dns:\n        names: [a]\n", "static.client.discovery.dns is only valid with provider dns"),
            ("discovery:\n      dns:\n        names: [a]\n", "static.client.discovery.dns is only valid with provider dns"),
            ("discovery:\n      provider: aws-tags\n", "static.client.discovery.aws is required with provider aws-tags"),
            ("discovery:\n      provider: dns\n      aws:\n        region: x\n      dns:\n        names: [a]\n", "static.client.discovery.aws is only valid with provider aws-tags"),
            ("discovery:\n      merge_policy: union\n", "Invalid static.client.discovery.merge_policy value 'union': valid values: merge, replace, discovery_only"),
            ("discovery:\n      refresh_interval_ms: soon\n", "Invalid static.client.discovery.refresh_interval_ms value 'soon': expected an integer"),
            ("discovery:\n      tend_refresh_deadline_ms: \"100\"\n", "Invalid static.client.discovery.tend_refresh_deadline_ms value '100': expected an integer"),
            ("discovery: 5\n", "Invalid static.client.discovery value '5': expected a map"),
            ("discovery:\n      provider: dns\n      dns:\n        record_type: a\n", "static.client.discovery.dns.names is required"),
            ("discovery:\n      provider: dns\n      dns:\n        names: []\n", "Invalid static.client.discovery.dns.names value '[]': expected a non-empty list of strings"),
            ("discovery:\n      provider: dns\n      dns:\n        names: [a]\n        record_type: mx\n", "Invalid static.client.discovery.dns.record_type value 'mx': valid values: a, srv"),
            ("discovery:\n      provider: dns\n      dns:\n        names: [a]\n        port: 70000\n", "Invalid static.client.discovery.dns.port value '70000': expected an integer from 1 to 65535"),
            ("discovery:\n      provider: dns\n      dns:\n        names: [a]\n        port: \"3000\"\n", "expected an integer"),
            ("discovery:\n      provider: dns\n      dns:\n        names: [a]\n        timeout_ms: 0\n", "Invalid static.client.discovery.dns.timeout_ms value '0': expected an integer from 1"),
            ("discovery:\n      provider: dns\n      dns:\n        names: [a]\n        tls_name: 1\n", "Invalid static.client.discovery.dns.tls_name value '1': expected a string"),
            ("discovery:\n      provider: dns\n      dns:\n        names: [a]\n        servers: [dns.example.com]\n", "Invalid static.client.discovery.dns: Invalid argument: Invalid DNS seed provider server: 'dns.example.com': expected an IP address"),
            ("discovery:\n      provider: dns\n      dns:\n        names: [a, A.]\n        servers: [127.0.0.1]\n", "duplicate name"),
            ("discovery:\n      provider: dns\n      dns: 7\n", "Invalid static.client.discovery.dns value '7': expected a map"),
            ("translation:\n      on_miss: drop\n", "Invalid static.client.translation.on_miss value 'drop': valid values: pass_through, strict"),
            ("translation:\n      provider: nat\n", "Invalid static.client.translation.provider value 'nat': valid values: static-map, port-map, pass-through"),
            ("translation:\n      provider: pass-through\n      static_map:\n        a: b\n", "static.client.translation.static_map is only valid with provider static-map, not 'pass-through'"),
            ("translation:\n      provider: static-map\n      port_map:\n        \"a:1\": \"b:2\"\n", "static.client.translation.port_map is only valid with provider port-map, not 'static-map'"),
            ("translation:\n      port_map:\n        \"a:1\": \"b:2\"\n", "port_map is only valid with provider port-map, not 'static-map'"),
            ("translation:\n      provider: port-map\n", "static.client.translation.port_map is required with provider port-map"),
            ("translation:\n      provider: port-map\n      port_map:\n        \"a:1\": \"b\"\n", "static.client.translation.port_map: Invalid argument: Invalid port-map entry a:1: b: b is not host:port"),
            ("translation:\n      static_map:\n        a: 1\n", "Invalid static.client.translation.static_map.a value '1': expected a string"),
            ("ip_map: [a]\n", "Invalid static.client.ip_map value '[a]': expected a map"),
            ("ip_map:\n      a: true\n", "Invalid static.client.ip_map.a value 'true': expected a string"),
            ("use_services_alternate: yes\n", "Invalid static.client.use_services_alternate value 'yes': expected true or false"),
            ("ip_map:\n      a: b\n    translation:\n      static_map:\n        a: b\n", "static.client.translation.static_map and static.client.ip_map are both set. Set one of them"),
        ];
        for (body, expected) in cases {
            let yaml = format!("static:\n  client:\n    {body}");
            let Err(err) = client_block(&yaml).resolve(&ClientPolicy::default(), true) else {
                panic!("{yaml} should be invalid");
            };
            assert!(err.to_string().contains(expected), "{yaml}\n  got: {err}\n  expected: {expected}");
        }
    }

    #[cfg(feature = "discovery-aws")]
    #[test]
    fn invalid_aws_settings_name_the_key_or_value() {
        let cases: &[(&str, &str)] = &[
            ("filters:\n          - name: a\n            values: [b]\n", "static.client.discovery.aws.region is required"),
            ("region: us-east-1\n", "static.client.discovery.aws.filters is required"),
            ("region: us-east-1\n        filters: []\n", "Invalid static.client.discovery.aws.filters value '[]': expected a non-empty list of name and values"),
            ("region: us-east-1\n        filters:\n          - values: [b]\n", "static.client.discovery.aws.filters[0].name is required"),
            ("region: us-east-1\n        filters:\n          - name: a\n", "static.client.discovery.aws.filters[0].values is required"),
            ("region: us-east-1\n        filters:\n          - name: a\n            values: []\n", "Invalid static.client.discovery.aws.filters[0].values value '[]': expected a non-empty list of strings"),
            ("region: us-east-1\n        filters:\n          - name: a\n            values: [b]\n        address_field: ip\n", "Invalid static.client.discovery.aws.address_field value 'ip': valid values: private_ip, public_ip, private_dns, public_dns"),
            ("region: us-east-1\n        filters:\n          - name: a\n            values: [b]\n        port: 0\n", "Invalid static.client.discovery.aws.port value '0': expected an integer from 1 to 65535"),
            ("region: us-east-1\n        filters:\n          - name: a\n            values: [b]\n        init_timeout_ms: x\n", "Invalid static.client.discovery.aws.init_timeout_ms value 'x': expected an integer"),
            ("region: us-east-1\n        filters:\n          - name: a\n            values: [b]\n        endpoint: ftp://x\n", "Invalid static.client.discovery.aws: Invalid argument: Invalid AWS EC2 seed provider endpoint: 'ftp://x'"),
            ("region: US\n        filters:\n          - name: a\n            values: [b]\n", "Invalid static.client.discovery.aws: Invalid argument: Invalid AWS EC2 seed provider region: 'US'"),
            ("region: us-east-1\n        filters:\n          - name: instance-state-name\n            values: [running]\n", "the provider adds instance-state-name=running"),
        ];
        for (body, expected) in cases {
            let yaml = format!("static:\n  client:\n    discovery:\n      provider: aws-tags\n      aws:\n        {body}");
            let Err(err) = client_block(&yaml).resolve(&ClientPolicy::default(), true) else {
                panic!("{yaml} should be invalid");
            };
            assert!(err.to_string().contains(expected), "{yaml}\n  got: {err}\n  expected: {expected}");
        }
    }

    #[test]
    fn config_value_display_and_accessors() {
        let v: ConfigValue = serde_norway::from_str("{a: 1, b: [x, true, 2.5], c: ~}").unwrap();
        assert_eq!(v.to_string(), "{a=1, b=[x, true, 2.5], c=null}");
        let m = v.as_map().unwrap();
        assert_eq!(m.get("a").unwrap().as_i64(), Some(1));
        assert_eq!(m.get("b").unwrap().as_list().unwrap()[1].as_bool(), Some(true));
        assert_eq!(m.get("b").unwrap().as_list().unwrap()[0].as_str(), Some("x"));
        assert_eq!(m.get("c"), Some(&ConfigValue::Null));
    }
}
