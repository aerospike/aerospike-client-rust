// Copyright 2015-2026 Aerospike, Inc.
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

//! The AWS EC2 tag seed candidate provider (`discovery-aws` feature).

mod credentials;
mod env;
mod http;
mod sigv4;
mod xml;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use aerospike_rt::time::Instant;

pub use credentials::{CredentialSource, Credentials, CredentialsHook, REFRESH_MARGIN_MS};

use self::credentials::Chain;
use self::env::{AwsEnvironment, SystemEnvironment};
use self::http::{HttpError, Url};
use super::{SeedCandidateContext, SeedCandidateProvider, AWS_TAGS_PROVIDER};
use crate::errors::{Error, Result};
use crate::net::Host;

/// Instance field returned as the seed address.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum AddressField {
    /// The primary private IPv4 address (`privateIpAddress`). The default.
    #[default]
    PrivateIp,
    /// The public IPv4 address (`ipAddress`).
    PublicIp,
    /// The private DNS name (`privateDnsName`).
    PrivateDns,
    /// The public DNS name (`dnsName`).
    PublicDns,
}

impl AddressField {
    const fn element(self) -> &'static str {
        match self {
            AddressField::PrivateIp => "privateIpAddress",
            AddressField::PublicIp => "ipAddress",
            AddressField::PrivateDns => "privateDnsName",
            AddressField::PublicDns => "dnsName",
        }
    }

    /// The YAML spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            AddressField::PrivateIp => "private_ip",
            AddressField::PublicIp => "public_ip",
            AddressField::PrivateDns => "private_dns",
            AddressField::PublicDns => "public_dns",
        }
    }

    /// The YAML spellings, for error messages.
    pub const VALUES: [&'static str; 4] = ["private_ip", "public_ip", "private_dns", "public_dns"];
}

impl FromStr for AddressField {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "private_ip" => Ok(AddressField::PrivateIp),
            "public_ip" => Ok(AddressField::PublicIp),
            "private_dns" => Ok(AddressField::PrivateDns),
            "public_dns" => Ok(AddressField::PublicDns),
            other => Err(Error::invalid_argument(format!(
                "Invalid AWS EC2 seed provider address_field value '{other}': valid values: {}",
                Self::VALUES.join(", ")
            ))),
        }
    }
}

/// Default port set on every host.
pub const DEFAULT_PORT: u16 = 3000;
/// Default total time for one tend refresh, in milliseconds.
pub const DEFAULT_TIMEOUT_MS: u32 = 80;
/// Default total time for the first refresh, in milliseconds.
pub const DEFAULT_INIT_TIMEOUT_MS: u32 = 10_000;

const API_VERSION: &str = "2016-11-15";
const STATE_FILTER: &str = "instance-state-name";
const FORM: &str = "application/x-www-form-urlencoded; charset=utf-8";
const SERVICE: &str = "ec2";

/// Seed candidate provider that returns one host per running EC2 instance
/// matching a set of filters, usually a tag.
///
/// The instances are listed through the EC2 `DescribeInstances` API. For a
/// self-managed Aerospike cluster on EC2 whose nodes carry a known tag.
/// YAML: `provider: aws-tags` with the `aws` block.
///
/// Each refresh sends one SigV4-signed EC2 Query API `DescribeInstances`
/// request (API version 2016-11-15) with the filters plus
/// `instance-state-name=running`, and follows `NextToken` pagination. It
/// returns one host per instance with a non-empty
/// [`address_field`](Builder::address_field), with
/// [`port`](Builder::port) and [`tls_name`](Builder::tls_name), deduplicated
/// and sorted by address. An instance without that field is skipped and the
/// count is logged at debug level. Any API, credential or timeout failure is
/// an error carrying the AWS error code and request id when there is one, so
/// the client keeps the previous seed list. No access key secret, session
/// token, web identity token or signature is logged or put in a message.
///
/// Minimum IAM policy of the credentials the provider uses:
///
/// ```json
/// {
///   "Version": "2012-10-17",
///   "Statement": [
///     {"Effect": "Allow", "Action": ["ec2:DescribeInstances"], "Resource": "*"}
///   ]
/// }
/// ```
///
/// Credentials: [`credentials_hook`](Builder::credentials_hook), when set,
/// replaces the chain below. Otherwise the first source found, in this order,
/// is used:
///
/// 1. [`credentials`](Builder::credentials) keys;
/// 2. environment variables `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and
///    `AWS_SESSION_TOKEN`;
/// 3. static keys of profile `AWS_PROFILE` (default `default`) in the shared
///    credentials file `AWS_SHARED_CREDENTIALS_FILE` (default
///    `~/.aws/credentials`);
/// 4. EKS web identity: `AWS_WEB_IDENTITY_TOKEN_FILE` and `AWS_ROLE_ARN`,
///    through STS `AssumeRoleWithWebIdentity` on the regional STS endpoint;
/// 5. ECS container credentials: `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` or
///    `AWS_CONTAINER_CREDENTIALS_FULL_URI`, with
///    `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE` or
///    `AWS_CONTAINER_AUTHORIZATION_TOKEN`;
/// 6. the EC2 instance profile through `IMDSv2` (session token, then the
///    role credentials), unless `AWS_EC2_METADATA_DISABLED` is `true`.
///
/// SSO, `credential_process`, `role_arn` profiles and cross-account roles are
/// not in the chain; use the hook. The chain resolves once, at the first
/// refresh; later refreshes reuse the source that answered. Credentials are
/// cached with their expiry and reloaded on the refreshing task when they are
/// within five minutes of expiry, never on a timer.
///
/// Deadlines: the provider needs periodic refresh and runs on the tend task,
/// so [`timeout`](Builder::timeout) must not exceed
/// [`ClientPolicy::discovery_tend_refresh_deadline`](crate::ClientPolicy::discovery_tend_refresh_deadline);
/// client construction fails otherwise. Every connect and read of one tend
/// refresh, credentials included, is bounded by the time left of the timeout,
/// and the first refresh, which runs in the client constructor, by
/// [`init_timeout`](Builder::init_timeout). The endpoint host name is
/// resolved by the system resolver, which the provider cannot bound. The
/// provider starts no task and uses no HTTP proxy. For an in-region endpoint
/// the defaults (timeout 80 ms, deadline 100 ms) leave room for one call; an
/// out-of-region endpoint, or a slow credential reload on tend, overruns the
/// deadline, and after three consecutive overruns the client disables the
/// provider and restores the initial seeds.
///
/// The provider sees only the instances of the credentials' own account, so
/// it serves self-managed clusters on EC2, not Aerospike Cloud.
///
/// ```rust,edition2021
/// use aerospike::discovery::aws::{AddressField, AwsEc2TagSeedCandidateProvider};
/// use aerospike::discovery::SeedMergePolicy;
/// use aerospike::ClientPolicy;
/// use std::sync::Arc;
///
/// # fn main() -> aerospike::Result<()> {
/// let provider = AwsEc2TagSeedCandidateProvider::builder()
///     .region("us-east-1")
///     .tag_filter("aerospike-cluster", ["prod-a"])
///     .address_field(AddressField::PrivateIp)
///     .port(3000)
///     .build()?;
/// let mut policy = ClientPolicy::default();
/// policy.seed_candidate_provider = Some(Arc::new(provider));
/// // The seed host passed to the client stays a fallback.
/// policy.discovery_merge_policy = SeedMergePolicy::Merge;
/// # Ok(())
/// # }
/// ```
pub struct AwsEc2TagSeedCandidateProvider {
    region: String,
    filters: Vec<(String, Vec<String>)>,
    address_field: AddressField,
    port: u16,
    tls_name: Option<String>,
    timeout: u32,
    init_timeout: u32,
    endpoint: Option<String>,
    url: Url,
    host_header: String,
    query: String,
    env: Arc<dyn AwsEnvironment>,
    chain: Chain,
}

impl fmt::Debug for AwsEc2TagSeedCandidateProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AwsEc2TagSeedCandidateProvider")
            .field("region", &self.region)
            .field("filters", &self.filters)
            .field("address_field", &self.address_field)
            .field("port", &self.port)
            .field("tls_name", &self.tls_name)
            .field("timeout", &self.timeout)
            .field("init_timeout", &self.init_timeout)
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for AwsEc2TagSeedCandidateProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{AWS_TAGS_PROVIDER} {} {:?}", self.region, self.filters)
    }
}

impl AwsEc2TagSeedCandidateProvider {
    /// Returns a builder.
    #[must_use]
    pub fn builder() -> Builder {
        Builder::default()
    }

    /// The AWS region.
    #[must_use]
    pub fn region(&self) -> &str {
        &self.region
    }

    /// The filters, by name, in configured order.
    #[must_use]
    pub fn filters(&self) -> &[(String, Vec<String>)] {
        &self.filters
    }

    /// The instance field returned as the seed address.
    #[must_use]
    pub const fn address_field(&self) -> AddressField {
        self.address_field
    }

    /// The port set on every host.
    #[must_use]
    pub const fn port(&self) -> u16 {
        self.port
    }

    /// The TLS name set on every host.
    #[must_use]
    pub fn tls_name(&self) -> Option<&str> {
        self.tls_name.as_deref()
    }

    /// Total time in milliseconds for one tend refresh, credentials included.
    #[must_use]
    pub const fn timeout(&self) -> u32 {
        self.timeout
    }

    /// Total time in milliseconds for the first refresh, credentials included.
    #[must_use]
    pub const fn init_timeout(&self) -> u32 {
        self.init_timeout
    }

    /// The configured EC2 endpoint, or `None` for the regional endpoint.
    #[must_use]
    pub fn endpoint(&self) -> Option<&str> {
        self.endpoint.as_deref()
    }

    /// The credential source that answered, or `None` before the first
    /// refresh.
    pub async fn credential_source(&self) -> Option<CredentialSource> {
        self.chain.source().await
    }

    async fn describe(
        &self,
        credentials: &Credentials,
        next_token: Option<&str>,
        deadline: Instant,
        budget: u32,
    ) -> Result<xml::Element> {
        let body = match next_token {
            Some(token) => format!("{}&NextToken={}", self.query, sigv4::encode(token)),
            None => self.query.clone(),
        };
        let amz_date = sigv4::amz_date(self.env.now_millis());
        let mut headers = BTreeMap::new();
        headers.insert("content-type".to_string(), FORM.to_string());
        headers.insert("host".to_string(), self.host_header.clone());
        headers.insert("x-amz-date".to_string(), amz_date.clone());
        if let Some(token) = credentials.session_token() {
            headers.insert("x-amz-security-token".to_string(), token.to_string());
        }
        let authorization = sigv4::authorization(
            credentials.access_key_id(),
            credentials.secret_access_key(),
            "POST",
            &self.url.path,
            "",
            &headers,
            body.as_bytes(),
            &self.region,
            SERVICE,
            &amz_date,
        );
        let signature = authorization
            .rsplit('=')
            .next()
            .unwrap_or_default()
            .to_string();
        headers.insert("authorization".to_string(), authorization);
        let secrets: Vec<&str> = [
            Some(credentials.secret_access_key()),
            credentials.session_token(),
            Some(signature.as_str()),
        ]
        .into_iter()
        .flatten()
        .collect();

        let response = match http::request("POST", &self.url, &headers, Some(body.as_bytes()), deadline).await
        {
            Ok(response) => response,
            Err(HttpError::Timeout) => {
                return Err(Error::client_error(format!(
                    "AWS EC2 seed provider: DescribeInstances timed out after {budget}ms"
                )))
            }
            Err(HttpError::Io(e)) => {
                return Err(Error::client_error(format!(
                    "AWS EC2 seed provider: DescribeInstances failed: {}",
                    xml::redact(&e, &secrets)
                )))
            }
        };
        if response.status != 200 {
            return Err(xml::error("DescribeInstances", &response, &secrets));
        }
        xml::parse(&response.body).map_err(|e| {
            Error::client_error(format!(
                "AWS EC2 seed provider: invalid DescribeInstances response: {e}"
            ))
        })
    }
}

#[async_trait::async_trait]
impl SeedCandidateProvider for AwsEc2TagSeedCandidateProvider {
    /// Lists the running instances matching the filters and returns one host
    /// per instance. The first call, at client initialization, is bounded by
    /// [`init_timeout`](Self::init_timeout); later calls by
    /// [`timeout`](Self::timeout).
    async fn refresh_seed_candidates(&self, ctx: &SeedCandidateContext) -> Result<Vec<Host>> {
        let budget = if ctx.tend_iteration == 0 {
            self.init_timeout
        } else {
            self.timeout
        };
        let deadline = Instant::now() + Duration::from_millis(u64::from(budget));
        let credentials = self.chain.get(deadline).await?;
        let mut addresses = BTreeSet::new();
        let mut next_token: Option<String> = None;
        let (mut instances, mut skipped, mut pages) = (0, 0, 0);
        loop {
            let root = self
                .describe(&credentials, next_token.as_deref(), deadline, budget)
                .await?;
            pages += 1;
            if let Some(reservations) = root.child("reservationSet") {
                for reservation in reservations.children("item") {
                    let Some(set) = reservation.child("instancesSet") else {
                        continue;
                    };
                    for instance in set.children("item") {
                        instances += 1;
                        match instance.text(self.address_field.element()) {
                            Some(address) if !address.is_empty() => {
                                addresses.insert(address);
                            }
                            _ => skipped += 1,
                        }
                    }
                }
            }
            next_token = root.text("nextToken").filter(|t| !t.is_empty());
            if next_token.is_none() {
                break;
            }
        }
        let hosts: Vec<Host> = addresses
            .into_iter()
            .map(|address| Host {
                name: address,
                tls_name: self.tls_name.clone(),
                port: self.port,
            })
            .collect();
        if skipped > 0 {
            debug!(
                "AWS EC2 seed provider skipped {skipped} instances without {}",
                self.address_field.element()
            );
        }
        debug!(
            "AWS EC2 seed provider {} returned {hosts:?} from {instances} instances in {pages} pages",
            self.region
        );
        Ok(hosts)
    }

    /// `true`: instances can change.
    fn needs_periodic_refresh(&self) -> bool {
        true
    }

    /// `true`: all I/O of one refresh is bounded by [`timeout`](Self::timeout),
    /// or [`init_timeout`](Self::init_timeout) for the first.
    fn supports_deadline(&self) -> bool {
        true
    }

    fn refresh_timeout(&self) -> Option<Duration> {
        Some(Duration::from_millis(u64::from(self.timeout)))
    }

    fn name(&self) -> String {
        AWS_TAGS_PROVIDER.to_string()
    }
}

/// Builder for [`AwsEc2TagSeedCandidateProvider`]. Values are validated by
/// [`build`](Self::build).
#[derive(Clone, Default)]
pub struct Builder {
    region: Option<String>,
    filters: Vec<(String, Vec<String>)>,
    address_field: Option<AddressField>,
    port: Option<u16>,
    tls_name: Option<String>,
    timeout: Option<u32>,
    init_timeout: Option<u32>,
    endpoint: Option<String>,
    credentials: Option<Credentials>,
    credentials_hook: Option<CredentialsHook>,
    env: Option<Arc<dyn AwsEnvironment>>,
}

impl fmt::Debug for Builder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Builder")
            .field("region", &self.region)
            .field("filters", &self.filters)
            .field("address_field", &self.address_field)
            .field("port", &self.port)
            .field("tls_name", &self.tls_name)
            .field("timeout", &self.timeout)
            .field("init_timeout", &self.init_timeout)
            .field("endpoint", &self.endpoint)
            .field("credentials", &self.credentials)
            .field("credentials_hook", &self.credentials_hook.is_some())
            .finish_non_exhaustive()
    }
}

impl Builder {
    /// AWS region of the instances and of the EC2 and STS endpoints, such as
    /// `us-east-1`. Required.
    #[must_use]
    pub fn region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    /// Adds a `DescribeInstances` filter, passed to EC2 as given: `tag:<key>`
    /// or any other EC2 filter name. An instance matches when it matches one
    /// of the values of every filter. Adding a name again replaces its
    /// values. `instance-state-name=running` is always added and cannot be
    /// set. At least one filter is required.
    #[must_use]
    pub fn filter<I, S>(mut self, name: impl Into<String>, values: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let name = name.into();
        let values: Vec<String> = values.into_iter().map(Into::into).collect();
        if let Some(entry) = self.filters.iter_mut().find(|(n, _)| *n == name) {
            entry.1 = values;
        } else {
            self.filters.push((name, values));
        }
        self
    }

    /// Adds filter `tag:<key>` with the given values.
    #[must_use]
    pub fn tag_filter<I, S>(self, key: &str, values: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.filter(format!("tag:{key}"), values)
    }

    /// Instance field returned as the seed address. Default:
    /// [`AddressField::PrivateIp`].
    #[must_use]
    pub const fn address_field(mut self, address_field: AddressField) -> Self {
        self.address_field = Some(address_field);
        self
    }

    /// Port set on every host. Default: [`DEFAULT_PORT`].
    #[must_use]
    pub const fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    /// TLS name set on every host. Default: none, in which case the TLS name
    /// is defaulted as for configured seeds.
    #[must_use]
    pub fn tls_name(mut self, tls_name: impl Into<String>) -> Self {
        self.tls_name = Some(tls_name.into());
        self
    }

    /// Total time in milliseconds for one tend refresh, credentials and every
    /// page included. Must be greater than zero and must not exceed the
    /// discovery tend refresh deadline, which is checked at client
    /// construction. Default: [`DEFAULT_TIMEOUT_MS`].
    #[must_use]
    pub const fn timeout(mut self, timeout_ms: u32) -> Self {
        self.timeout = Some(timeout_ms);
        self
    }

    /// Total time in milliseconds for the first refresh, which runs in the
    /// client constructor and resolves the credential chain. Must be greater
    /// than zero. Default: [`DEFAULT_INIT_TIMEOUT_MS`].
    #[must_use]
    pub const fn init_timeout(mut self, init_timeout_ms: u32) -> Self {
        self.init_timeout = Some(init_timeout_ms);
        self
    }

    /// EC2 endpoint URL, such as a VPC interface endpoint. Default: the
    /// regional endpoint `https://ec2.<region>.amazonaws.com`.
    #[must_use]
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Static credentials, the first source of the credential chain. Cannot
    /// be combined with [`credentials_hook`](Self::credentials_hook).
    #[must_use]
    pub fn credentials(mut self, credentials: Credentials) -> Self {
        self.credentials = Some(credentials);
        self
    }

    /// A credentials hook that replaces the built-in credential chain. Cannot
    /// be combined with [`credentials`](Self::credentials).
    #[must_use]
    pub fn credentials_hook(mut self, hook: CredentialsHook) -> Self {
        self.credentials_hook = Some(hook);
        self
    }

    #[cfg(test)]
    pub(crate) fn environment(mut self, env: Arc<dyn AwsEnvironment>) -> Self {
        self.env = Some(env);
        self
    }

    /// Validates the settings and builds the provider.
    ///
    /// # Errors
    ///
    /// An invalid setting fails with a message naming it.
    pub fn build(self) -> Result<AwsEc2TagSeedCandidateProvider> {
        let region = self.region.unwrap_or_default();
        if !valid_region(&region) {
            return Err(Error::invalid_argument(format!(
                "Invalid AWS EC2 seed provider region: '{region}'"
            )));
        }
        if self.filters.is_empty() {
            return Err(Error::invalid_argument(
                "Invalid AWS EC2 seed provider filters: at least one filter is required",
            ));
        }
        for (name, values) in &self.filters {
            if name.is_empty() {
                return Err(Error::invalid_argument(format!(
                    "Invalid AWS EC2 seed provider filter name: '{name}'"
                )));
            }
            if name == STATE_FILTER {
                return Err(Error::invalid_argument(format!(
                    "Invalid AWS EC2 seed provider filter name: '{name}': the provider adds \
                     {STATE_FILTER}=running"
                )));
            }
            if values.is_empty() {
                return Err(Error::invalid_argument(format!(
                    "Invalid AWS EC2 seed provider filter '{name}' values: empty"
                )));
            }
            if let Some(value) = values.iter().find(|v| v.is_empty()) {
                return Err(Error::invalid_argument(format!(
                    "Invalid AWS EC2 seed provider filter '{name}' value: '{value}'"
                )));
            }
        }
        let port = self.port.unwrap_or(DEFAULT_PORT);
        if port == 0 {
            return Err(Error::invalid_argument(
                "Invalid AWS EC2 seed provider port: 0",
            ));
        }
        if self.tls_name.as_deref() == Some("") {
            return Err(Error::invalid_argument(
                "Invalid AWS EC2 seed provider tls_name: empty",
            ));
        }
        let timeout = self.timeout.unwrap_or(DEFAULT_TIMEOUT_MS);
        if timeout == 0 {
            return Err(Error::invalid_argument(
                "Invalid AWS EC2 seed provider timeout: 0",
            ));
        }
        let init_timeout = self.init_timeout.unwrap_or(DEFAULT_INIT_TIMEOUT_MS);
        if init_timeout == 0 {
            return Err(Error::invalid_argument(
                "Invalid AWS EC2 seed provider init_timeout: 0",
            ));
        }
        if self.credentials.is_some() && self.credentials_hook.is_some() {
            return Err(Error::invalid_argument(
                "Invalid AWS EC2 seed provider credentials: credentials and credentials_hook \
                 cannot both be set",
            ));
        }
        let env: Arc<dyn AwsEnvironment> = self.env.unwrap_or_else(|| Arc::new(SystemEnvironment));
        let target = self
            .endpoint
            .clone()
            .unwrap_or_else(|| env.ec2_endpoint(&region));
        let url = Url::parse(&target).map_err(|_| {
            Error::invalid_argument(format!(
                "Invalid AWS EC2 seed provider endpoint: '{target}': expected http or https URL \
                 with a host and no query"
            ))
        })?;
        if url.query.is_some() {
            return Err(Error::invalid_argument(format!(
                "Invalid AWS EC2 seed provider endpoint: '{target}': expected http or https URL \
                 with a host and no query"
            )));
        }
        let host_header = url.host_header();
        let query = query(&self.filters);
        let chain = Chain::new(self.credentials, self.credentials_hook, env.clone(), region.clone());
        Ok(AwsEc2TagSeedCandidateProvider {
            region,
            filters: self.filters,
            address_field: self.address_field.unwrap_or_default(),
            port,
            tls_name: self.tls_name,
            timeout,
            init_timeout,
            endpoint: self.endpoint,
            url,
            host_header,
            query,
            env,
            chain,
        })
    }
}

/// `[a-z0-9]+(-[a-z0-9]+)+`
fn valid_region(region: &str) -> bool {
    let parts: Vec<&str> = region.split('-').collect();
    parts.len() >= 2
        && parts
            .iter()
            .all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit()))
}

fn query(filters: &[(String, Vec<String>)]) -> String {
    let mut out = format!("Action=DescribeInstances&Version={API_VERSION}");
    let mut n = 0;
    for (name, values) in filters {
        n += 1;
        append_filter(&mut out, n, name, values);
    }
    append_filter(&mut out, n + 1, STATE_FILTER, &["running".to_string()]);
    out
}

fn append_filter(out: &mut String, n: usize, name: &str, values: &[String]) {
    use std::fmt::Write as _;
    let _ = write!(out, "&Filter.{n}.Name={}", sigv4::encode(name));
    for (v, value) in values.iter().enumerate() {
        let _ = write!(out, "&Filter.{n}.Value.{}={}", v + 1, sigv4::encode(value));
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Write as _;
    use std::sync::Arc;

    use super::env::test_env::TestEnvironment;
    use super::http::stub::{ok, Handler, StubHttpServer};
    use super::*;

    fn ctx(tend_iteration: u64) -> SeedCandidateContext {
        SeedCandidateContext::new(None, Vec::new(), tend_iteration)
    }

    fn instance(fields: &[(&str, &str)]) -> String {
        let mut s = String::from("<item>");
        for (k, v) in fields {
            let _ = write!(s, "<{k}>{v}</{k}>");
        }
        s.push_str("</item>");
        s
    }

    fn describe_response(instances: &[String], next_token: Option<&str>) -> String {
        let mut s = String::from(
            "<?xml version=\"1.0\"?><DescribeInstancesResponse xmlns=\"http://ec2.amazonaws.com/doc/2016-11-15/\"><requestId>r</requestId><reservationSet>",
        );
        for i in instances {
            let _ = write!(s, "<item><instancesSet>{i}</instancesSet></item>");
        }
        s.push_str("</reservationSet>");
        if let Some(t) = next_token {
            let _ = write!(s, "<nextToken>{t}</nextToken>");
        }
        s.push_str("</DescribeInstancesResponse>");
        s
    }

    fn env_with_keys(ec2: &StubHttpServer) -> Arc<TestEnvironment> {
        let env = Arc::new(TestEnvironment {
            ec2: Some(ec2.url()),
            ..TestEnvironment::default()
        });
        env.set("AWS_ACCESS_KEY_ID", "AKIDEXAMPLE");
        env.set("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY");
        env.set_now(1_440_938_160_000);
        env
    }

    fn builder(env: Arc<TestEnvironment>) -> Builder {
        AwsEc2TagSeedCandidateProvider::builder()
            .region("us-east-1")
            .tag_filter("aerospike-cluster", ["prod-a"])
            .environment(env)
            .timeout(2000)
            .init_timeout(2000)
    }

    #[aerospike_macro::test]
    async fn each_address_field_and_a_missing_field_is_skipped() {
        let handler: Handler = Arc::new(|_| {
            ok(&describe_response(
                &[
                    instance(&[("privateIpAddress", "10.0.0.2"), ("ipAddress", "54.0.0.2"), ("privateDnsName", "ip-10-0-0-2.ec2.internal"), ("dnsName", "ec2-54.compute.amazonaws.com")]),
                    instance(&[("privateIpAddress", "10.0.0.1"), ("ipAddress", ""), ("privateDnsName", "ip-10-0-0-1.ec2.internal")]),
                ],
                None,
            ))
        });
        let ec2 = StubHttpServer::start(handler).await;
        let env = env_with_keys(&ec2);
        for (field, expected) in [
            (AddressField::PrivateIp, vec!["10.0.0.1", "10.0.0.2"]),
            (AddressField::PublicIp, vec!["54.0.0.2"]),
            (AddressField::PrivateDns, vec!["ip-10-0-0-1.ec2.internal", "ip-10-0-0-2.ec2.internal"]),
            (AddressField::PublicDns, vec!["ec2-54.compute.amazonaws.com"]),
        ] {
            let provider = builder(env.clone()).address_field(field).port(3100).tls_name("tls-seed").build().unwrap();
            let hosts = provider.refresh_seed_candidates(&ctx(0)).await.unwrap();
            let expected: Vec<Host> = expected.into_iter().map(|a| Host::new_tls(a, "tls-seed", 3100)).collect();
            assert_eq!(hosts, expected, "{field:?}");
            assert_eq!(provider.credential_source().await, Some(CredentialSource::Environment));
        }
    }

    #[aerospike_macro::test]
    async fn filters_are_sent_as_configured_with_running_added_and_the_request_signed() {
        let handler: Handler = Arc::new(|req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.target, "/");
            assert_eq!(req.header("content-type"), Some(FORM));
            assert_eq!(req.header("x-amz-date"), Some("20150830T123600Z"));
            assert_eq!(req.header("x-amz-security-token"), None);
            let auth = req.header("authorization").unwrap();
            assert!(auth.starts_with("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/ec2/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature="), "{auth}");
            assert_eq!(
                req.body,
                "Action=DescribeInstances&Version=2016-11-15\
                 &Filter.1.Name=tag%3Aaerospike-cluster&Filter.1.Value.1=prod-a&Filter.1.Value.2=prod-b\
                 &Filter.2.Name=subnet-id&Filter.2.Value.1=subnet-0123\
                 &Filter.3.Name=instance-state-name&Filter.3.Value.1=running"
            );
            ok(&describe_response(&[], None))
        });
        let ec2 = StubHttpServer::start(handler).await;
        let env = env_with_keys(&ec2);
        let provider = builder(env)
            .tag_filter("aerospike-cluster", ["prod-a", "prod-b"])
            .filter("subnet-id", ["subnet-0123"])
            .build()
            .unwrap();
        assert!(provider.refresh_seed_candidates(&ctx(0)).await.unwrap().is_empty());
        assert_eq!(ec2.requests().len(), 1);

        // Verify the signature against the canonical request the stub saw.
        let req = &ec2.requests()[0];
        let mut headers = BTreeMap::new();
        headers.insert("content-type".to_string(), FORM.to_string());
        headers.insert("host".to_string(), req.header("host").unwrap().to_string());
        headers.insert("x-amz-date".to_string(), "20150830T123600Z".to_string());
        let expected = sigv4::authorization(
            "AKIDEXAMPLE",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            "POST",
            "/",
            "",
            &headers,
            req.body.as_bytes(),
            "us-east-1",
            "ec2",
            "20150830T123600Z",
        );
        assert_eq!(req.header("authorization"), Some(expected.as_str()));
    }

    #[aerospike_macro::test]
    async fn pagination_follows_next_token_and_results_are_deduplicated_and_sorted() {
        let handler: Handler = Arc::new(|req| {
            if req.body.contains("NextToken=") {
                assert!(req.body.ends_with("&NextToken=page%202"), "{}", req.body);
                ok(&describe_response(&[instance(&[("privateIpAddress", "10.0.0.1")]), instance(&[("privateIpAddress", "10.0.0.3")])], None))
            } else {
                ok(&describe_response(&[instance(&[("privateIpAddress", "10.0.0.9")]), instance(&[("privateIpAddress", "10.0.0.1")])], Some("page 2")))
            }
        });
        let ec2 = StubHttpServer::start(handler).await;
        let provider = builder(env_with_keys(&ec2)).build().unwrap();
        let hosts = provider.refresh_seed_candidates(&ctx(1)).await.unwrap();
        assert_eq!(hosts, vec![Host::new("10.0.0.1", 3000), Host::new("10.0.0.3", 3000), Host::new("10.0.0.9", 3000)]);
        assert_eq!(ec2.requests().len(), 2);
    }

    #[aerospike_macro::test]
    async fn api_error_carries_code_and_request_id_without_the_secret() {
        let handler: Handler = Arc::new(|req| {
            let auth = req.header("authorization").unwrap().to_string();
            let signature = auth.rsplit('=').next().unwrap().to_string();
            (403, vec![], format!("<Response><Errors><Error><Code>UnauthorizedOperation</Code><Message>You are not authorized to perform this operation. sig={signature} secret=wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY</Message></Error></Errors><RequestID>ea966190-f9aa</RequestID></Response>").into_bytes())
        });
        let ec2 = StubHttpServer::start(handler).await;
        let provider = builder(env_with_keys(&ec2)).build().unwrap();
        let err = provider.refresh_seed_candidates(&ctx(0)).await.unwrap_err();
        let msg = err.base_message();
        assert!(msg.starts_with("AWS EC2 seed provider: DescribeInstances failed: HTTP 403 UnauthorizedOperation: You are not authorized"), "{msg}");
        assert!(msg.ends_with("(request id ea966190-f9aa)"), "{msg}");
        assert!(msg.contains("sig=<redacted> secret=<redacted>"), "{msg}");
    }

    #[aerospike_macro::test]
    async fn xml_with_doctype_is_rejected() {
        let handler: Handler = Arc::new(|_| ok("<!DOCTYPE x><DescribeInstancesResponse/>"));
        let ec2 = StubHttpServer::start(handler).await;
        let provider = builder(env_with_keys(&ec2)).build().unwrap();
        let err = provider.refresh_seed_candidates(&ctx(0)).await.unwrap_err();
        assert!(err.to_string().contains("invalid DescribeInstances response"), "{err}");
    }

    #[aerospike_macro::test]
    async fn unanswered_endpoint_fails_within_the_timeout_and_init_timeout() {
        let handler: Handler = Arc::new(|_| ok(""));
        let ec2 = StubHttpServer::start(handler).await;
        ec2.set_silent(true);
        let provider = builder(env_with_keys(&ec2)).timeout(100).init_timeout(300).build().unwrap();
        let started = Instant::now();
        let err = provider.refresh_seed_candidates(&ctx(0)).await.unwrap_err();
        assert_eq!(err.base_message(), "AWS EC2 seed provider: DescribeInstances timed out after 300ms");
        let elapsed = started.elapsed();
        assert!(elapsed >= Duration::from_millis(250) && elapsed < Duration::from_millis(800), "{elapsed:?}");
        let started = Instant::now();
        let err = provider.refresh_seed_candidates(&ctx(5)).await.unwrap_err();
        assert_eq!(err.base_message(), "AWS EC2 seed provider: DescribeInstances timed out after 100ms");
        assert!(started.elapsed() < Duration::from_millis(500));
    }

    #[aerospike_macro::test]
    async fn no_instance_matching_is_not_a_failure_and_the_hook_replaces_the_chain() {
        let handler: Handler = Arc::new(|req| {
            assert_eq!(req.header("x-amz-security-token"), Some("hooktok"));
            ok(&describe_response(&[], None))
        });
        let ec2 = StubHttpServer::start(handler).await;
        let env = Arc::new(TestEnvironment {
            ec2: Some(ec2.url()),
            ..TestEnvironment::default()
        });
        let hook: CredentialsHook = Arc::new(|| Credentials::new("HOOKKEY", "s", Some("hooktok".into())));
        let provider = builder(env).credentials_hook(hook).build().unwrap();
        assert!(provider.refresh_seed_candidates(&ctx(0)).await.unwrap().is_empty());
        assert_eq!(provider.credential_source().await, Some(CredentialSource::Hook));
        assert!(provider.needs_periodic_refresh());
        assert!(provider.supports_deadline());
        assert_eq!(provider.name(), "aws-tags");
        assert_eq!(provider.refresh_timeout(), Some(Duration::from_millis(2000)));
    }

    #[test]
    fn invalid_config_is_rejected_at_construction() {
        let b = || AwsEc2TagSeedCandidateProvider::builder().region("us-east-1").tag_filter("k", ["v"]);
        let cases: Vec<(Builder, &str)> = vec![
            (AwsEc2TagSeedCandidateProvider::builder().tag_filter("k", ["v"]), "region: ''"),
            (b().region("US-EAST-1"), "region: 'US-EAST-1'"),
            (b().region("useast1"), "region: 'useast1'"),
            (AwsEc2TagSeedCandidateProvider::builder().region("us-east-1"), "at least one filter"),
            (b().filter("", ["v"]), "filter name: ''"),
            (b().filter("instance-state-name", ["running"]), "the provider adds instance-state-name=running"),
            (b().filter("x", Vec::<String>::new()), "filter 'x' values: empty"),
            (b().filter("x", [""]), "filter 'x' value: ''"),
            (b().port(0), "port: 0"),
            (b().tls_name(""), "tls_name: empty"),
            (b().timeout(0), "timeout: 0"),
            (b().init_timeout(0), "init_timeout: 0"),
            (b().endpoint("ftp://x"), "endpoint: 'ftp://x'"),
            (b().endpoint("https://x/?q=1"), "no query"),
            (
                b().credentials(Credentials::new("k", "s", None).unwrap()).credentials_hook(Arc::new(|| Credentials::new("k", "s", None))),
                "cannot both be set",
            ),
        ];
        for (builder, expected) in cases {
            let err = builder.build().unwrap_err();
            assert!(err.to_string().contains(expected), "{err} should contain {expected}");
        }
    }

    #[test]
    fn getters_and_defaults() {
        let p = AwsEc2TagSeedCandidateProvider::builder()
            .region("eu-west-1")
            .tag_filter("a", ["1"])
            .filter("b", ["2", "3"])
            .tag_filter("a", ["9"])
            .build()
            .unwrap();
        assert_eq!(p.region(), "eu-west-1");
        assert_eq!(p.filters(), &[("tag:a".to_string(), vec!["9".to_string()]), ("b".to_string(), vec!["2".to_string(), "3".to_string()])]);
        assert_eq!(p.address_field(), AddressField::PrivateIp);
        assert_eq!(p.port(), DEFAULT_PORT);
        assert_eq!(p.tls_name(), None);
        assert_eq!(p.timeout(), DEFAULT_TIMEOUT_MS);
        assert_eq!(p.init_timeout(), DEFAULT_INIT_TIMEOUT_MS);
        assert_eq!(p.endpoint(), None);
        assert_eq!(p.url, Url::parse("https://ec2.eu-west-1.amazonaws.com").unwrap());
        assert_eq!(p.to_string(), "aws-tags eu-west-1 [(\"tag:a\", [\"9\"]), (\"b\", [\"2\", \"3\"])]");
        assert_eq!("public_dns".parse::<AddressField>().unwrap(), AddressField::PublicDns);
        assert!("x".parse::<AddressField>().is_err());
        let custom = AwsEc2TagSeedCandidateProvider::builder()
            .region("cn-north-1")
            .tag_filter("a", ["1"])
            .endpoint("http://vpce.local:8443/path")
            .build()
            .unwrap();
        assert_eq!(custom.endpoint(), Some("http://vpce.local:8443/path"));
        assert_eq!(custom.host_header, "vpce.local:8443");
        assert_eq!(custom.url.path, "/path");
    }
}
