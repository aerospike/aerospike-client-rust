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

//! The AWS credential chain of
//! [`AwsEc2TagSeedCandidateProvider`](super::AwsEc2TagSeedCandidateProvider):
//! configured keys, environment, shared credentials file, web identity, ECS
//! container credentials, EC2 `IMDSv2`, first found wins; a hook replaces the
//! chain. The chain resolves once and later calls reuse the source that
//! answered. Credentials are cached and reloaded on the calling task when
//! they are within [`REFRESH_MARGIN_MS`] of expiry.

#![allow(clippy::redundant_pub_crate)]

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use aerospike_rt::time::Instant;

use super::env::AwsEnvironment;
use super::http::{self, HttpError, Response, Url};
use super::{sigv4, xml};
use crate::errors::{Error, Result};

/// Credentials are reloaded this long before they expire.
pub const REFRESH_MARGIN_MS: i64 = 5 * 60 * 1000;

const FORM: &str = "application/x-www-form-urlencoded; charset=utf-8";
const IMDS_TOKEN_TTL: &str = "21600";
const IMDS_CREDENTIALS: &str = "/latest/meta-data/iam/security-credentials/";

/// AWS credentials used to sign requests. `Debug` shows only the access key
/// id.
#[derive(Clone, PartialEq, Eq)]
pub struct Credentials {
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
    expiration: i64,
}

impl Credentials {
    /// Credentials without expiry. The session token is that of temporary
    /// credentials and may be empty.
    ///
    /// # Errors
    ///
    /// An empty access key id or secret.
    pub fn new(
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        session_token: Option<String>,
    ) -> Result<Self> {
        Self::with_expiration(access_key_id, secret_access_key, session_token, 0)
    }

    /// Credentials with an expiry in milliseconds since the epoch, or 0 for
    /// none.
    ///
    /// # Errors
    ///
    /// An empty access key id or secret.
    pub fn with_expiration(
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        session_token: Option<String>,
        expiration: i64,
    ) -> Result<Self> {
        let access_key_id = access_key_id.into();
        let secret_access_key = secret_access_key.into();
        if access_key_id.is_empty() {
            return Err(Error::invalid_argument(
                "Invalid AWS EC2 seed provider credentials: access key id is empty",
            ));
        }
        if secret_access_key.is_empty() {
            return Err(Error::invalid_argument(
                "Invalid AWS EC2 seed provider credentials: secret access key is empty",
            ));
        }
        Ok(Credentials {
            access_key_id,
            secret_access_key,
            session_token: session_token.filter(|t| !t.is_empty()),
            expiration,
        })
    }

    /// The access key id.
    #[must_use]
    pub fn access_key_id(&self) -> &str {
        &self.access_key_id
    }

    /// The secret access key.
    #[must_use]
    pub fn secret_access_key(&self) -> &str {
        &self.secret_access_key
    }

    /// The session token of temporary credentials.
    #[must_use]
    pub fn session_token(&self) -> Option<&str> {
        self.session_token.as_deref()
    }

    /// The expiry in milliseconds since the epoch, or 0 for none.
    #[must_use]
    pub const fn expiration(&self) -> i64 {
        self.expiration
    }
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Credentials{{access_key_id={}, expiration={}}}",
            self.access_key_id, self.expiration
        )
    }
}

/// A source of credentials that replaces the built-in chain.
///
/// Called on the task that refreshes the provider, the tend task included,
/// so it must return quickly, well within the provider's timeout; an
/// implementation that does I/O should cache. Credentials returned with an
/// expiry are cached until five minutes before it; without one they are used
/// for the life of the provider.
pub type CredentialsHook = Arc<dyn Fn() -> Result<Credentials> + Send + Sync>;

/// Where the chain found its credentials.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialSource {
    /// Keys set on the builder.
    Config,
    /// `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.
    Environment,
    /// The shared credentials file.
    Profile,
    /// STS `AssumeRoleWithWebIdentity`.
    WebIdentity,
    /// ECS container credentials.
    Ecs,
    /// The EC2 instance profile through `IMDSv2`.
    Imds,
    /// The credentials hook.
    Hook,
}

impl CredentialSource {
    /// The label logged for this source.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            CredentialSource::Config => "config",
            CredentialSource::Environment => "environment",
            CredentialSource::Profile => "profile",
            CredentialSource::WebIdentity => "web-identity",
            CredentialSource::Ecs => "ecs",
            CredentialSource::Imds => "imds",
            CredentialSource::Hook => "hook",
        }
    }
}

#[derive(Default)]
struct State {
    source: Option<CredentialSource>,
    cached: Option<Credentials>,
    imds_role: Option<String>,
}

pub(crate) struct Chain {
    config: Option<Credentials>,
    hook: Option<CredentialsHook>,
    env: Arc<dyn AwsEnvironment>,
    region: String,
    state: aerospike_rt::Mutex<State>,
}

impl Chain {
    pub(crate) fn new(
        config: Option<Credentials>,
        hook: Option<CredentialsHook>,
        env: Arc<dyn AwsEnvironment>,
        region: String,
    ) -> Self {
        Chain {
            config,
            hook,
            env,
            region,
            state: aerospike_rt::Mutex::new(State::default()),
        }
    }

    /// The source that answered, or `None` before the chain has resolved.
    pub(crate) async fn source(&self) -> Option<CredentialSource> {
        self.state.lock().await.source
    }

    /// Returns credentials, loading them when none are cached or the cached
    /// ones are within [`REFRESH_MARGIN_MS`] of expiry.
    pub(crate) async fn get(&self, deadline: Instant) -> Result<Credentials> {
        let mut state = self.state.lock().await;
        let now = self.env.now_millis();
        if let Some(cached) = &state.cached {
            if cached.expiration == 0 || now < cached.expiration - REFRESH_MARGIN_MS {
                return Ok(cached.clone());
            }
        }
        let loaded = match state.source {
            None => self.resolve(&mut state, deadline).await,
            Some(source) => self.load(source, &mut state, deadline).await,
        };
        match loaded {
            Ok(credentials) => {
                state.cached = Some(credentials.clone());
                Ok(credentials)
            }
            Err(e) => {
                if let Some(cached) = &state.cached {
                    if now < cached.expiration {
                        warn!(
                            "AWS EC2 seed provider: credentials refresh from {} failed, using cached \
                             credentials until they expire: {e}",
                            state.source.map_or("?", CredentialSource::label)
                        );
                        return Ok(cached.clone());
                    }
                }
                Err(e)
            }
        }
    }

    async fn resolve(&self, state: &mut State, deadline: Instant) -> Result<Credentials> {
        let found = if self.hook.is_some() {
            CredentialSource::Hook
        } else if self.config.is_some() {
            CredentialSource::Config
        } else if self.environment()?.is_some() {
            CredentialSource::Environment
        } else if self.profile()?.is_some() {
            CredentialSource::Profile
        } else if self.present("AWS_WEB_IDENTITY_TOKEN_FILE") && self.present("AWS_ROLE_ARN") {
            CredentialSource::WebIdentity
        } else if self.present("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI")
            || self.present("AWS_CONTAINER_CREDENTIALS_FULL_URI")
        {
            CredentialSource::Ecs
        } else if !self
            .env
            .getenv("AWS_EC2_METADATA_DISABLED")
            .is_some_and(|v| v.eq_ignore_ascii_case("true"))
        {
            CredentialSource::Imds
        } else {
            return Err(Error::client_error(
                "AWS EC2 seed provider: no AWS credentials found in config, environment, shared \
                 credentials file, web identity or ECS, and IMDS is disabled",
            ));
        };

        let credentials = match self.load(found, state, deadline).await {
            Ok(c) => c,
            Err(e) if found == CredentialSource::Imds => {
                return Err(Error::client_error(format!(
                    "AWS EC2 seed provider: no AWS credentials found in config, environment, \
                     shared credentials file, web identity or ECS, and {}",
                    e.base_message().replace("AWS EC2 seed provider: ", "")
                )));
            }
            Err(e) => return Err(e),
        };
        state.source = Some(found);
        info!(
            "AWS EC2 seed provider credentials from {}: access key {}",
            found.label(),
            credentials.access_key_id
        );
        Ok(credentials)
    }

    async fn load(
        &self,
        from: CredentialSource,
        state: &mut State,
        deadline: Instant,
    ) -> Result<Credentials> {
        match from {
            CredentialSource::Hook => self.hook(),
            CredentialSource::Config => Ok(self.config.clone().expect("config source has keys")),
            CredentialSource::Environment => self
                .environment()?
                .ok_or_else(|| Error::client_error("AWS EC2 seed provider: environment credentials are no longer set")),
            CredentialSource::Profile => self
                .profile()?
                .ok_or_else(|| Error::client_error("AWS EC2 seed provider: profile credentials are no longer set")),
            CredentialSource::WebIdentity => self.web_identity(deadline).await,
            CredentialSource::Ecs => self.ecs(deadline).await,
            CredentialSource::Imds => self.imds(state, deadline).await,
        }
    }

    fn hook(&self) -> Result<Credentials> {
        let hook = self.hook.as_ref().expect("hook source has a hook");
        hook().map_err(|e| {
            Error::client_error(format!(
                "AWS EC2 seed provider: credentials hook failed: {}",
                e.base_message()
            ))
        })
    }

    fn environment(&self) -> Result<Option<Credentials>> {
        let key = self.env.getenv("AWS_ACCESS_KEY_ID").unwrap_or_default();
        let secret = self.env.getenv("AWS_SECRET_ACCESS_KEY").unwrap_or_default();
        if key.is_empty() || secret.is_empty() {
            return Ok(None);
        }
        Ok(Some(Credentials::new(key, secret, self.env.getenv("AWS_SESSION_TOKEN"))?))
    }

    fn profile(&self) -> Result<Option<Credentials>> {
        let file = self.env.getenv("AWS_SHARED_CREDENTIALS_FILE").unwrap_or_default();
        let path = if file.is_empty() {
            let Some(home) = self.env.user_home() else {
                return Ok(None);
            };
            PathBuf::from(home).join(".aws").join("credentials")
        } else if let Some(rest) = file.strip_prefix("~/") {
            let Some(home) = self.env.user_home() else {
                return Ok(None);
            };
            PathBuf::from(home).join(rest)
        } else {
            PathBuf::from(file)
        };
        let text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => return Ok(None),
            Err(e) => {
                return Err(Error::client_error(format!(
                    "AWS EC2 seed provider: failed to read {}: {e}",
                    path.display()
                )))
            }
        };
        let profile = self
            .env
            .getenv("AWS_PROFILE")
            .filter(|p| !p.is_empty())
            .unwrap_or_else(|| "default".to_string());
        let mut keys: HashMap<String, String> = HashMap::new();
        let mut in_profile = false;
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
                continue;
            }
            if let Some(section) = line.strip_prefix('[').and_then(|l| l.strip_suffix(']')) {
                in_profile = section.trim() == profile;
                continue;
            }
            if in_profile {
                if let Some((k, v)) = line.split_once('=') {
                    keys.insert(k.trim().to_string(), v.trim().to_string());
                }
            }
        }
        let key = keys.get("aws_access_key_id").cloned().unwrap_or_default();
        let secret = keys.get("aws_secret_access_key").cloned().unwrap_or_default();
        if key.is_empty() || secret.is_empty() {
            return Ok(None);
        }
        Ok(Some(Credentials::new(key, secret, keys.get("aws_session_token").cloned())?))
    }

    async fn web_identity(&self, deadline: Instant) -> Result<Credentials> {
        let file = self.env.getenv("AWS_WEB_IDENTITY_TOKEN_FILE").unwrap_or_default();
        let token = std::fs::read_to_string(&file)
            .map_err(|e| {
                Error::client_error(format!(
                    "AWS EC2 seed provider: failed to read AWS_WEB_IDENTITY_TOKEN_FILE: {e}"
                ))
            })?
            .trim()
            .to_string();
        let session = self
            .env
            .getenv("AWS_ROLE_SESSION_NAME")
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| format!("aerospike-client-{}", self.env.now_millis()));
        let body = format!(
            "Action=AssumeRoleWithWebIdentity&Version=2011-06-15&RoleArn={}&RoleSessionName={}&WebIdentityToken={}",
            sigv4::encode(&self.env.getenv("AWS_ROLE_ARN").unwrap_or_default()),
            sigv4::encode(&session),
            sigv4::encode(&token)
        );
        let mut headers = BTreeMap::new();
        headers.insert("content-type".to_string(), FORM.to_string());
        let response = self
            .send(
                "STS AssumeRoleWithWebIdentity",
                "POST",
                &self.env.sts_endpoint(&self.region),
                &headers,
                Some(body.as_bytes()),
                deadline,
                &[&token],
            )
            .await?;
        if response.status != 200 {
            return Err(xml::error("STS AssumeRoleWithWebIdentity", &response, &[&token]));
        }
        let root = xml::parse(&response.body).map_err(|_| {
            Error::client_error("AWS EC2 seed provider: invalid STS AssumeRoleWithWebIdentity response")
        })?;
        let credentials = root.find_element("Credentials").ok_or_else(|| {
            Error::client_error(
                "AWS EC2 seed provider: STS AssumeRoleWithWebIdentity response has no credentials",
            )
        })?;
        temporary(
            "STS AssumeRoleWithWebIdentity",
            credentials.text("AccessKeyId"),
            credentials.text("SecretAccessKey"),
            credentials.text("SessionToken"),
            credentials.text("Expiration"),
        )
    }

    async fn ecs(&self, deadline: Instant) -> Result<Credentials> {
        let relative = self.env.getenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI").unwrap_or_default();
        let uri = if relative.is_empty() {
            self.env.getenv("AWS_CONTAINER_CREDENTIALS_FULL_URI").unwrap_or_default()
        } else {
            format!("{}{relative}", self.env.ecs_endpoint())
        };
        let file = self.env.getenv("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE").unwrap_or_default();
        let authorization = if file.is_empty() {
            self.env.getenv("AWS_CONTAINER_AUTHORIZATION_TOKEN").filter(|t| !t.is_empty())
        } else {
            Some(
                std::fs::read_to_string(&file)
                    .map_err(|e| {
                        Error::client_error(format!(
                            "AWS EC2 seed provider: failed to read AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE: {e}"
                        ))
                    })?
                    .trim()
                    .to_string(),
            )
        };
        let mut headers = BTreeMap::new();
        if let Some(authorization) = &authorization {
            headers.insert("authorization".to_string(), authorization.clone());
        }
        let secrets: Vec<&str> = authorization.iter().map(String::as_str).collect();
        let response = self
            .send("ECS credentials", "GET", &uri, &headers, None, deadline, &secrets)
            .await?;
        if response.status != 200 {
            return Err(Error::client_error(format!(
                "AWS EC2 seed provider: ECS credentials failed: HTTP {}",
                response.status
            )));
        }
        let json = json_object(&response, "ECS credentials")?;
        temporary(
            "ECS credentials",
            json.get("AccessKeyId").cloned(),
            json.get("SecretAccessKey").cloned(),
            json.get("Token").cloned(),
            json.get("Expiration").cloned(),
        )
    }

    async fn imds(&self, state: &mut State, deadline: Instant) -> Result<Credentials> {
        let base = self.env.imds_endpoint();
        let mut headers = BTreeMap::new();
        headers.insert(
            "x-aws-ec2-metadata-token-ttl-seconds".to_string(),
            IMDS_TOKEN_TTL.to_string(),
        );
        let response = self
            .send(
                "IMDS token",
                "PUT",
                &format!("{base}/latest/api/token"),
                &headers,
                Some(&[]),
                deadline,
                &[],
            )
            .await?;
        if response.status != 200 {
            return Err(Error::client_error(format!(
                "AWS EC2 seed provider: IMDS token failed: HTTP {}",
                response.status
            )));
        }
        let token = response.text().trim().to_string();
        headers.clear();
        headers.insert("x-aws-ec2-metadata-token".to_string(), token.clone());

        if state.imds_role.is_none() {
            let response = self
                .send(
                    "IMDS role",
                    "GET",
                    &format!("{base}{IMDS_CREDENTIALS}"),
                    &headers,
                    None,
                    deadline,
                    &[&token],
                )
                .await?;
            if response.status != 200 {
                return Err(Error::client_error(format!(
                    "AWS EC2 seed provider: IMDS role failed: HTTP {} (no instance profile attached?)",
                    response.status
                )));
            }
            let text = response.text();
            let role = text.lines().next().unwrap_or_default().trim().to_string();
            if role.is_empty() {
                return Err(Error::client_error(
                    "AWS EC2 seed provider: IMDS returned no instance profile role",
                ));
            }
            state.imds_role = Some(role);
        }
        let role = state.imds_role.clone().expect("role set above");
        let response = self
            .send(
                "IMDS credentials",
                "GET",
                &format!("{base}{IMDS_CREDENTIALS}{role}"),
                &headers,
                None,
                deadline,
                &[&token],
            )
            .await?;
        if response.status != 200 {
            return Err(Error::client_error(format!(
                "AWS EC2 seed provider: IMDS credentials failed: HTTP {}",
                response.status
            )));
        }
        let json = json_object(&response, "IMDS credentials")?;
        if let Some(code) = json.get("Code") {
            if code != "Success" {
                return Err(Error::client_error(format!(
                    "AWS EC2 seed provider: IMDS credentials failed: {code}"
                )));
            }
        }
        temporary(
            "IMDS credentials",
            json.get("AccessKeyId").cloned(),
            json.get("SecretAccessKey").cloned(),
            json.get("Token").cloned(),
            json.get("Expiration").cloned(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    async fn send(
        &self,
        action: &str,
        method: &str,
        uri: &str,
        headers: &BTreeMap<String, String>,
        body: Option<&[u8]>,
        deadline: Instant,
        secrets: &[&str],
    ) -> Result<Response> {
        let url = Url::parse(uri).map_err(|e| {
            Error::client_error(format!("AWS EC2 seed provider: {action} failed: {}", xml::redact(&e, secrets)))
        })?;
        match http::request(method, &url, headers, body, deadline).await {
            Ok(response) => Ok(response),
            Err(HttpError::Timeout) => Err(Error::client_error(format!(
                "AWS EC2 seed provider: {action} timed out"
            ))),
            Err(HttpError::Io(e)) => Err(Error::client_error(format!(
                "AWS EC2 seed provider: {action} failed: {}",
                xml::redact(&e, secrets)
            ))),
        }
    }

    fn present(&self, name: &str) -> bool {
        self.env.getenv(name).is_some_and(|v| !v.is_empty())
    }
}

fn temporary(
    action: &str,
    key: Option<String>,
    secret: Option<String>,
    token: Option<String>,
    expiration: Option<String>,
) -> Result<Credentials> {
    let (Some(key), Some(secret), Some(expiration)) = (key, secret, expiration) else {
        return Err(Error::client_error(format!(
            "AWS EC2 seed provider: {action} response has no credentials"
        )));
    };
    if key.is_empty() || secret.is_empty() {
        return Err(Error::client_error(format!(
            "AWS EC2 seed provider: {action} response has no credentials"
        )));
    }
    let expires = sigv4::parse_iso8601(&expiration).ok_or_else(|| {
        Error::client_error(format!(
            "AWS EC2 seed provider: {action} returned an invalid expiration: {expiration}"
        ))
    })?;
    Credentials::with_expiration(key, secret, token, expires)
}

fn json_object(response: &Response, action: &str) -> Result<HashMap<String, String>> {
    Json::object(&response.text())
        .map_err(|_| Error::client_error(format!("AWS EC2 seed provider: invalid {action} response")))
}

/// Parser for the flat JSON objects returned by the ECS and IMDS credential
/// endpoints. Values are returned as strings; nested objects and arrays are
/// skipped.
pub(crate) struct Json<'a> {
    s: &'a [u8],
    i: usize,
}

impl<'a> Json<'a> {
    pub(crate) fn object(text: &'a str) -> std::result::Result<HashMap<String, String>, String> {
        let mut json = Json { s: text.as_bytes(), i: 0 };
        let mut map = HashMap::new();
        json.expect(b'{')?;
        if json.peek()? == b'}' {
            return Ok(map);
        }
        loop {
            let key = json.string()?;
            json.expect(b':')?;
            if let Some(value) = json.value()? {
                map.insert(key, value);
            }
            match json.next()? {
                b'}' => return Ok(map),
                b',' => {}
                _ => return Err("expected , or }".into()),
            }
        }
    }

    fn peek(&mut self) -> std::result::Result<u8, String> {
        while self.i < self.s.len() && self.s[self.i].is_ascii_whitespace() {
            self.i += 1;
        }
        self.s.get(self.i).copied().ok_or_else(|| "unexpected end".into())
    }

    fn next(&mut self) -> std::result::Result<u8, String> {
        let c = self.peek()?;
        self.i += 1;
        Ok(c)
    }

    fn expect(&mut self, c: u8) -> std::result::Result<(), String> {
        if self.next()? == c {
            Ok(())
        } else {
            Err(format!("expected {}", char::from(c)))
        }
    }

    fn value(&mut self) -> std::result::Result<Option<String>, String> {
        let c = self.peek()?;
        if c == b'"' {
            return self.string().map(Some);
        }
        if c == b'{' || c == b'[' {
            self.skip()?;
            return Ok(None);
        }
        let begin = self.i;
        while self.i < self.s.len() && !b",}] \t\r\n".contains(&self.s[self.i]) {
            self.i += 1;
        }
        let literal = String::from_utf8_lossy(&self.s[begin..self.i]).into_owned();
        Ok(if literal == "null" { None } else { Some(literal) })
    }

    fn skip(&mut self) -> std::result::Result<(), String> {
        let mut depth = 0;
        loop {
            let c = self.peek()?;
            if c == b'"' {
                self.string()?;
                continue;
            }
            if c == b'{' || c == b'[' {
                depth += 1;
            } else if c == b'}' || c == b']' {
                depth -= 1;
            }
            self.i += 1;
            if depth == 0 {
                return Ok(());
            }
        }
    }

    fn string(&mut self) -> std::result::Result<String, String> {
        self.expect(b'"')?;
        let mut out: Vec<u8> = Vec::new();
        loop {
            let c = *self.s.get(self.i).ok_or("unterminated string")?;
            self.i += 1;
            match c {
                b'"' => return String::from_utf8(out).map_err(|e| e.to_string()),
                b'\\' => {
                    let e = *self.s.get(self.i).ok_or("unterminated string")?;
                    self.i += 1;
                    match e {
                        b'b' => out.push(8),
                        b'f' => out.push(12),
                        b'n' => out.push(b'\n'),
                        b'r' => out.push(b'\r'),
                        b't' => out.push(b'\t'),
                        b'u' => {
                            let hex = self.s.get(self.i..self.i + 4).ok_or("bad escape")?;
                            let code = u32::from_str_radix(std::str::from_utf8(hex).map_err(|e| e.to_string())?, 16)
                                .map_err(|e| e.to_string())?;
                            self.i += 4;
                            let ch = char::from_u32(code).ok_or("bad escape")?;
                            let mut buf = [0u8; 4];
                            out.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
                        }
                        other => out.push(other),
                    }
                }
                other => out.push(other),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::super::env::test_env::TestEnvironment;
    use super::super::http::stub::{ok, StubHttpServer};
    use super::*;

    fn deadline() -> Instant {
        Instant::now() + Duration::from_secs(5)
    }

    fn chain(env: Arc<TestEnvironment>, config: Option<Credentials>, hook: Option<CredentialsHook>) -> Chain {
        Chain::new(config, hook, env, "us-east-1".to_string())
    }

    fn temp_file(tag: &str, contents: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("aerospike-aws-{}-{tag}", std::process::id()));
        std::fs::write(&path, contents).unwrap();
        path
    }

    #[aerospike_macro::test]
    async fn config_keys_come_first_and_never_expire() {
        let env = Arc::new(TestEnvironment::default());
        env.set("AWS_ACCESS_KEY_ID", "ENVKEY");
        env.set("AWS_SECRET_ACCESS_KEY", "envsecret");
        let c = chain(env, Some(Credentials::new("CFGKEY", "cfgsecret", None).unwrap()), None);
        let creds = c.get(deadline()).await.unwrap();
        assert_eq!(creds.access_key_id(), "CFGKEY");
        assert_eq!(c.source().await, Some(CredentialSource::Config));
        assert_eq!(format!("{creds:?}"), "Credentials{access_key_id=CFGKEY, expiration=0}");
    }

    #[aerospike_macro::test]
    async fn environment_with_optional_session_token() {
        let env = Arc::new(TestEnvironment::default());
        env.set("AWS_ACCESS_KEY_ID", "ENVKEY");
        env.set("AWS_SECRET_ACCESS_KEY", "envsecret");
        env.set("AWS_SESSION_TOKEN", "tok");
        let c = chain(env, None, None);
        let creds = c.get(deadline()).await.unwrap();
        assert_eq!(creds.access_key_id(), "ENVKEY");
        assert_eq!(creds.session_token(), Some("tok"));
        assert_eq!(c.source().await, Some(CredentialSource::Environment));
    }

    #[aerospike_macro::test]
    async fn profile_file_with_aws_profile_and_default_in_home() {
        let path = temp_file(
            "credentials",
            "# comment\n[default]\naws_access_key_id = DEF\naws_secret_access_key = defsecret\n\n[prod]\naws_access_key_id=PROD\naws_secret_access_key=prodsecret\naws_session_token=ptok\n[norole]\nrole_arn = x\n",
        );
        let env = Arc::new(TestEnvironment::default());
        env.set("AWS_SHARED_CREDENTIALS_FILE", path.to_str().unwrap());
        env.set("AWS_PROFILE", "prod");
        let creds = chain(env.clone(), None, None).get(deadline()).await.unwrap();
        assert_eq!(creds.access_key_id(), "PROD");
        assert_eq!(creds.session_token(), Some("ptok"));

        env.set("AWS_PROFILE", "");
        let creds = chain(env.clone(), None, None).get(deadline()).await.unwrap();
        assert_eq!(creds.access_key_id(), "DEF");

        // A profile without static keys is not in the chain: IMDS is disabled, so nothing is found.
        env.set("AWS_PROFILE", "norole");
        env.set("AWS_EC2_METADATA_DISABLED", "true");
        let err = chain(env.clone(), None, None).get(deadline()).await.unwrap_err();
        assert!(err.to_string().contains("no AWS credentials found"), "{err}");

        // Default location: $HOME/.aws/credentials.
        let home = std::env::temp_dir().join(format!("aerospike-aws-home-{}", std::process::id()));
        std::fs::create_dir_all(home.join(".aws")).unwrap();
        std::fs::write(home.join(".aws").join("credentials"), "[default]\naws_access_key_id=HOME\naws_secret_access_key=s\n").unwrap();
        let env = Arc::new(TestEnvironment {
            home: Some(home.to_str().unwrap().to_string()),
            ..TestEnvironment::default()
        });
        let c = chain(env, None, None);
        assert_eq!(c.get(deadline()).await.unwrap().access_key_id(), "HOME");
        assert_eq!(c.source().await, Some(CredentialSource::Profile));
        let _ = std::fs::remove_dir_all(home);
        let _ = std::fs::remove_file(path);
    }

    #[aerospike_macro::test]
    async fn web_identity_through_stub_sts_and_error_without_the_token() {
        let token_file = temp_file("web-token", "WEBTOKEN\n");
        let sts = StubHttpServer::start(Arc::new(|req| {
            assert_eq!(req.method, "POST");
            assert!(req.body.contains("Action=AssumeRoleWithWebIdentity"));
            assert!(req.body.contains("RoleArn=arn%3Aaws%3Aiam%3A%3A1%3Arole%2Fx"));
            assert!(req.body.contains("WebIdentityToken=WEBTOKEN"));
            if req.body.contains("RoleSessionName=fail") {
                return (403, vec![], b"<ErrorResponse><Error><Code>AccessDenied</Code><Message>denied WEBTOKEN</Message></Error><RequestId>rid</RequestId></ErrorResponse>".to_vec());
            }
            ok("<AssumeRoleWithWebIdentityResponse><AssumeRoleWithWebIdentityResult><Credentials><AccessKeyId>WEBKEY</AccessKeyId><SecretAccessKey>websecret</SecretAccessKey><SessionToken>webtok</SessionToken><Expiration>2030-01-01T00:00:00Z</Expiration></Credentials></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>")
        }))
        .await;
        let env = Arc::new(TestEnvironment {
            sts: Some(sts.url()),
            ..TestEnvironment::default()
        });
        env.set("AWS_WEB_IDENTITY_TOKEN_FILE", token_file.to_str().unwrap());
        env.set("AWS_ROLE_ARN", "arn:aws:iam::1:role/x");
        env.set("AWS_ROLE_SESSION_NAME", "sess");
        let c = chain(env.clone(), None, None);
        let creds = c.get(deadline()).await.unwrap();
        assert_eq!(creds.access_key_id(), "WEBKEY");
        assert_eq!(creds.session_token(), Some("webtok"));
        assert_eq!(creds.expiration(), sigv4::parse_iso8601("2030-01-01T00:00:00Z").unwrap());
        assert_eq!(c.source().await, Some(CredentialSource::WebIdentity));

        env.set("AWS_ROLE_SESSION_NAME", "fail");
        let err = chain(env, None, None).get(deadline()).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("HTTP 403 AccessDenied"), "{msg}");
        assert!(msg.contains("(request id rid)"), "{msg}");
        assert!(!msg.contains("WEBTOKEN"), "{msg}");
        let _ = std::fs::remove_file(token_file);
    }

    #[aerospike_macro::test]
    async fn ecs_with_authorization_token_and_full_uri_with_token_file() {
        let ecs = StubHttpServer::start(Arc::new(|req| {
            assert_eq!(req.method, "GET");
            if req.header("authorization") != Some("Bearer t") {
                return (401, vec![], vec![]);
            }
            ok(r#"{"AccessKeyId":"ECSKEY","SecretAccessKey":"ecssecret","Token":"ecstok","Expiration":"2030-01-01T00:00:00Z","Nested":{"a":[1,2]}}"#)
        }))
        .await;
        let env = Arc::new(TestEnvironment {
            ecs: Some(ecs.url()),
            ..TestEnvironment::default()
        });
        env.set("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/v2/credentials/abc");
        env.set("AWS_CONTAINER_AUTHORIZATION_TOKEN", "Bearer t");
        let c = chain(env, None, None);
        let creds = c.get(deadline()).await.unwrap();
        assert_eq!(creds.access_key_id(), "ECSKEY");
        assert_eq!(c.source().await, Some(CredentialSource::Ecs));
        assert_eq!(ecs.requests()[0].target, "/v2/credentials/abc");

        let token_file = temp_file("ecs-token", "Bearer t\n");
        let env = Arc::new(TestEnvironment::default());
        env.set("AWS_CONTAINER_CREDENTIALS_FULL_URI", &format!("{}/full", ecs.url()));
        env.set("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", token_file.to_str().unwrap());
        let creds = chain(env, None, None).get(deadline()).await.unwrap();
        assert_eq!(creds.access_key_id(), "ECSKEY");

        // Without authorization the stub answers 401.
        let env = Arc::new(TestEnvironment::default());
        env.set("AWS_CONTAINER_CREDENTIALS_FULL_URI", &format!("{}/full", ecs.url()));
        let err = chain(env, None, None).get(deadline()).await.unwrap_err();
        assert!(err.to_string().contains("ECS credentials failed: HTTP 401"), "{err}");
        let _ = std::fs::remove_file(token_file);
    }

    #[aerospike_macro::test]
    async fn imds_v2_token_handshake_then_role_then_credentials() {
        let imds = StubHttpServer::start(Arc::new(|req| {
            match (req.method.as_str(), req.target.as_str()) {
                ("PUT", "/latest/api/token") => {
                    assert_eq!(req.header("x-aws-ec2-metadata-token-ttl-seconds"), Some("21600"));
                    ok("IMDSTOKEN")
                }
                ("GET", "/latest/meta-data/iam/security-credentials/") => {
                    assert_eq!(req.header("x-aws-ec2-metadata-token"), Some("IMDSTOKEN"));
                    ok("my-role\n")
                }
                ("GET", "/latest/meta-data/iam/security-credentials/my-role") => ok(
                    r#"{"Code":"Success","AccessKeyId":"IMDSKEY","SecretAccessKey":"s","Token":"t","Expiration":"2030-01-01T00:00:00Z"}"#,
                ),
                _ => (404, vec![], vec![]),
            }
        }))
        .await;
        let env = Arc::new(TestEnvironment {
            imds: Some(imds.url()),
            ..TestEnvironment::default()
        });
        let c = chain(env, None, None);
        let creds = c.get(deadline()).await.unwrap();
        assert_eq!(creds.access_key_id(), "IMDSKEY");
        assert_eq!(c.source().await, Some(CredentialSource::Imds));
        assert_eq!(imds.requests().len(), 3);
    }

    #[aerospike_macro::test]
    async fn no_source_found_names_every_source_and_the_imds_failure() {
        let imds = StubHttpServer::start(Arc::new(|_| (403, vec![], vec![]))).await;
        let env = Arc::new(TestEnvironment {
            imds: Some(imds.url()),
            ..TestEnvironment::default()
        });
        let err = chain(env, None, None).get(deadline()).await.unwrap_err();
        assert_eq!(
            err.base_message(),
            "AWS EC2 seed provider: no AWS credentials found in config, environment, shared \
             credentials file, web identity or ECS, and IMDS token failed: HTTP 403"
        );
    }

    #[aerospike_macro::test]
    async fn expiry_refreshes_near_expiry_and_a_failed_reload_keeps_unexpired_credentials() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let c2 = calls.clone();
        let hook: CredentialsHook = Arc::new(move || {
            let n = c2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if n >= 2 {
                return Err(Error::client_error("boom"));
            }
            Credentials::with_expiration(format!("KEY{n}"), "s", None, 10 * 60 * 1000)
        });
        let env = Arc::new(TestEnvironment::default());
        env.set_now(0);
        let c = chain(env.clone(), None, Some(hook));
        assert_eq!(c.get(deadline()).await.unwrap().access_key_id(), "KEY0");
        assert_eq!(c.source().await, Some(CredentialSource::Hook));
        // Not within the margin yet: cached.
        env.set_now(4 * 60 * 1000);
        assert_eq!(c.get(deadline()).await.unwrap().access_key_id(), "KEY0");
        // Within five minutes of expiry: reloaded.
        env.set_now(6 * 60 * 1000);
        assert_eq!(c.get(deadline()).await.unwrap().access_key_id(), "KEY1");
        // The next reload fails: the unexpired cached credentials are kept.
        assert_eq!(c.get(deadline()).await.unwrap().access_key_id(), "KEY1");
        // Once expired, the failure surfaces.
        env.set_now(11 * 60 * 1000);
        let err = c.get(deadline()).await.unwrap_err();
        assert!(err.to_string().contains("credentials hook failed: boom"), "{err}");
    }

    #[test]
    fn json_parser_handles_strings_escapes_literals_and_nesting() {
        let map = Json::object(r#"{ "a": "x\"yA", "n": 12, "t": true, "z": null, "o": {"k": [1, {"q": "}"}]}, "last": "v" }"#).unwrap();
        assert_eq!(map.get("a").map(String::as_str), Some("x\"yA"));
        assert_eq!(map.get("n").map(String::as_str), Some("12"));
        assert_eq!(map.get("t").map(String::as_str), Some("true"));
        assert_eq!(map.get("z"), None);
        assert_eq!(map.get("o"), None);
        assert_eq!(map.get("last").map(String::as_str), Some("v"));
        assert!(Json::object("{}").unwrap().is_empty());
        assert!(Json::object("[1]").is_err());
        assert!(Json::object(r#"{"a": "unterminated"#).is_err());
    }

    #[test]
    fn credentials_reject_empty_keys() {
        assert!(Credentials::new("", "s", None).is_err());
        assert!(Credentials::new("k", "", None).is_err());
        let c = Credentials::new("k", "s", Some(String::new())).unwrap();
        assert_eq!(c.session_token(), None);
        assert_eq!(c.secret_access_key(), "s");
    }
}
