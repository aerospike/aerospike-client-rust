// Copyright 2015-2018 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
//
// Licensed under the Apache License version 2.0 (the "License"); you may not
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

use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::IndexMap;

use crate::Key;
use crate::Value;

/// The Aerospike epoch: Friday 1 January 2010, 00:00:00 UTC.
///
/// Record void-times cross the wire as a `u32` count of seconds since this
/// instant rather than since the Unix epoch, which is what keeps expirations
/// beyond 2106 representable. [`Record::time_to_live`] converts against it, and
/// it is public so callers doing their own void-time arithmetic agree with the
/// server on the origin.
pub static CITRUSLEAF_EPOCH: std::sync::LazyLock<SystemTime> =
    std::sync::LazyLock::new(|| UNIX_EPOCH + Duration::new(1_262_304_000, 0));

/// Container object for a database record.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serialization", derive(Serialize))]
pub struct Record {
    /// Record key. When reading a record from the database, the key is not set in the returned
    /// Record struct.
    pub key: Option<Key>,

    /// Map of named record bins, in the order the server returned them.
    pub bins: IndexMap<String, Value>,

    /// Positional op results in request order; `None` on non-operate paths.
    /// Nil-result ops carry `Value::Nil` so indices line up with the op list.
    pub results: Option<Vec<Value>>,

    /// Record modification count.
    pub generation: u32,

    /// Date record will expire, in seconds from Jan 01 2010, 00:00:00 UTC.
    expiration: u32,
}

impl Record {
    /// Construct a record.
    ///
    /// The client builds these while parsing a reply, and so does anything that
    /// forwards records on — a proxy, a cache, a test double. Every field but
    /// `expiration` is public, so this is the only way to set that one.
    ///
    /// `expiration` is the server's own encoding: **seconds since the Citrusleaf
    /// epoch** (2010-01-01 UTC, [`CITRUSLEAF_EPOCH`]) at which the record expires,
    /// with `0` meaning it never does. It is not a TTL, and it is not a Unix
    /// timestamp; [`Record::time_to_live`] converts it to the remaining duration.
    ///
    /// ```
    /// use aerospike::{IndexMap, Record};
    ///
    /// // A record that never expires.
    /// let record = Record::new(None, IndexMap::new(), None, 1, 0);
    /// assert_eq!(record.time_to_live(), None);
    /// ```
    #[must_use]
    pub const fn new(
        key: Option<Key>,
        bins: IndexMap<String, Value>,
        results: Option<Vec<Value>>,
        generation: u32,
        expiration: u32,
    ) -> Self {
        Record {
            key,
            bins,
            results,
            generation,
            expiration,
        }
    }

    /// `None` is returned both for not-populated and out-of-range — callers
    /// can't distinguish.
    #[must_use]
    pub fn operation_result(&self, i: usize) -> Option<&Value> {
        self.results.as_ref()?.get(i)
    }

    /// Returns the remaining time-to-live (TTL, a.k.a. expiration time) for the record or `None`
    /// if the record never expires.
    #[allow(clippy::option_if_let_else)]
    pub fn time_to_live(&self) -> Option<Duration> {
        match self.expiration {
            0 => None,
            secs_since_epoch => {
                let expiration = *CITRUSLEAF_EPOCH + Duration::new(u64::from(secs_since_epoch), 0);
                match expiration.duration_since(SystemTime::now()) {
                    Ok(d) => Some(d),
                    // Record was not expired at server but it looks expired at client
                    // because of delay or clock difference, present it as not-expired.
                    Err(_) => Some(Duration::new(1u64, 0)),
                }
            }
        }
    }
}

impl fmt::Display for Record {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "key: {:?}", self.key)?;
        write!(f, ", bins: {{")?;
        for (i, (k, v)) in self.bins.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{k}: {v}")?;
        }
        write!(f, "}}, generation: {}", self.generation)?;
        write!(f, ", ttl: ")?;
        match self.time_to_live() {
            None => "none".fmt(f),
            Some(duration) => duration.as_secs().fmt(f),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Record, CITRUSLEAF_EPOCH};
    use crate::IndexMap;
    use std::time::{Duration, SystemTime};

    #[test]
    fn ttl_expiration_future() {
        let expiration = SystemTime::now() + Duration::new(1000, 0);
        let secs_since_epoch = expiration
            .duration_since(*CITRUSLEAF_EPOCH)
            .unwrap()
            .as_secs();
        let record = Record::new(None, IndexMap::new(), None, 0, secs_since_epoch as u32);
        let ttl = record.time_to_live();
        assert!(ttl.is_some());
        assert!(1000 - ttl.unwrap().as_secs() <= 1);
    }

    #[test]
    fn ttl_expiration_past() {
        let record = Record::new(None, IndexMap::new(), None, 0, 0x0d00_d21c);
        assert_eq!(record.time_to_live(), Some(Duration::new(1u64, 0)));
    }

    #[test]
    fn ttl_never_expires() {
        let record = Record::new(None, IndexMap::new(), None, 0, 0);
        assert_eq!(record.time_to_live(), None);
    }

    #[test]
    fn operation_result_returns_positional_value() {
        use crate::Value;
        let results = vec![
            Value::Int(5),
            Value::String("ell".to_string()),
            Value::Nil,
            Value::Int(2),
        ];
        let record = Record::new(None, IndexMap::new(), Some(results), 0, 0);
        assert_eq!(record.operation_result(0), Some(&Value::Int(5)));
        assert_eq!(
            record.operation_result(1),
            Some(&Value::String("ell".to_string()))
        );
        assert_eq!(record.operation_result(2), Some(&Value::Nil));
        assert_eq!(record.operation_result(3), Some(&Value::Int(2)));
        assert_eq!(record.operation_result(4), None);
    }

    #[test]
    fn operation_result_returns_none_when_not_populated() {
        let record = Record::new(None, IndexMap::new(), None, 0, 0);
        assert_eq!(record.operation_result(0), None);
    }
}
