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

//! The environment the AWS credential chain reads and the default AWS
//! endpoints. Tests override it to point the chain at stubs; production reads
//! the process environment.

#![allow(clippy::redundant_pub_crate)]

use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) trait AwsEnvironment: Send + Sync {
    fn getenv(&self, name: &str) -> Option<String>;
    fn user_home(&self) -> Option<String>;
    fn ec2_endpoint(&self, region: &str) -> String {
        format!("https://ec2.{region}.{}", domain(region))
    }
    fn sts_endpoint(&self, region: &str) -> String {
        format!("https://sts.{region}.{}", domain(region))
    }
    fn ecs_endpoint(&self) -> String {
        "http://169.254.170.2".to_string()
    }
    fn imds_endpoint(&self) -> String {
        "http://169.254.169.254".to_string()
    }
    fn now_millis(&self) -> i64;
}

impl fmt::Debug for dyn AwsEnvironment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AwsEnvironment")
    }
}

fn domain(region: &str) -> &'static str {
    if region.starts_with("cn-") {
        "amazonaws.com.cn"
    } else {
        "amazonaws.com"
    }
}

/// The process environment.
#[derive(Debug, Default)]
pub(crate) struct SystemEnvironment;

impl AwsEnvironment for SystemEnvironment {
    fn getenv(&self, name: &str) -> Option<String> {
        std::env::var(name).ok()
    }

    fn user_home(&self) -> Option<String> {
        std::env::var("HOME")
            .ok()
            .or_else(|| std::env::var("USERPROFILE").ok())
    }

    fn now_millis(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as i64)
    }
}

#[cfg(test)]
pub(crate) mod test_env {
    //! An environment built from a map, with every endpoint pointed at a stub.

    use std::collections::HashMap;
    use std::sync::Mutex;

    use super::AwsEnvironment;
    use crate::locks::lock;

    #[derive(Debug, Default)]
    pub(crate) struct TestEnvironment {
        pub vars: Mutex<HashMap<String, String>>,
        pub home: Option<String>,
        pub ec2: Option<String>,
        pub sts: Option<String>,
        pub ecs: Option<String>,
        pub imds: Option<String>,
        pub now: Mutex<i64>,
    }

    impl TestEnvironment {
        pub(crate) fn set(&self, name: &str, value: &str) {
            lock(&self.vars).insert(name.to_string(), value.to_string());
        }

        pub(crate) fn set_now(&self, millis: i64) {
            *lock(&self.now) = millis;
        }
    }

    impl AwsEnvironment for TestEnvironment {
        fn getenv(&self, name: &str) -> Option<String> {
            lock(&self.vars).get(name).cloned()
        }

        fn user_home(&self) -> Option<String> {
            self.home.clone()
        }

        fn ec2_endpoint(&self, region: &str) -> String {
            self.ec2
                .clone()
                .unwrap_or_else(|| format!("https://ec2.{region}.amazonaws.com"))
        }

        fn sts_endpoint(&self, region: &str) -> String {
            self.sts
                .clone()
                .unwrap_or_else(|| format!("https://sts.{region}.amazonaws.com"))
        }

        fn ecs_endpoint(&self) -> String {
            self.ecs.clone().unwrap_or_else(|| "http://169.254.170.2".into())
        }

        fn imds_endpoint(&self) -> String {
            self.imds.clone().unwrap_or_else(|| "http://169.254.169.254".into())
        }

        fn now_millis(&self) -> i64 {
            *lock(&self.now)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_endpoints_follow_the_region_partition() {
        let env = SystemEnvironment;
        assert_eq!(env.ec2_endpoint("us-east-1"), "https://ec2.us-east-1.amazonaws.com");
        assert_eq!(env.sts_endpoint("cn-north-1"), "https://sts.cn-north-1.amazonaws.com.cn");
        assert_eq!(env.ecs_endpoint(), "http://169.254.170.2");
        assert_eq!(env.imds_endpoint(), "http://169.254.169.254");
        assert!(env.now_millis() > 1_600_000_000_000);
    }
}
